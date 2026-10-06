#!/usr/bin/env python3
"""
Fetch and scan GDELT's Web NGrams dataset (quadgram files, published every
minute) for mentions of student activism + repression terms, entirely
locally. No queries are sent to GDELT's search API, which has been
unreliable while GDELT migrates its infrastructure.

How it works:
  - GDELT publishes a pair of files every minute:
      https://data.gdeltproject.org/gdeltv5/weblegacy/ngrams/<TIMESTAMP>.ngrams.txt.gz
      https://data.gdeltproject.org/gdeltv5/weblegacy/ngrams/<TIMESTAMP>.toc.json.gz
    TIMESTAMP is YYYYMMDDHHMM00. Not every minute has a file, so a 404 is a
    normal gap, not an error.
  - data/ngrams_state.txt records the last minute attempted; each run walks
    forward from there (at most MAX_MINUTES_PER_RUN minutes per run).
  - For each minute: group the 4-word phrases by article, and record an
    article when its full text contains an identity phrase AND a repression
    term (see config/categories.yaml).
  - Each recorded article is tagged with: the category mentioned most, the
    focus country mentioned most, and scope tags (high_school, seah) that
    power the dashboard's "Hide ..." filters.
  - URLs that look like listing pages (author, tag, category, search,
    pagination) are skipped.

IMPORTANT: if a column is ever added to FIELDNAMES, add it at the END. On
startup, ensure_schema() upgrades an older data/hits.csv in place (header
plus padded rows) and refuses to touch a file whose header it doesn't
recognise, so a mismatch can never silently misalign columns.

Runs the same locally:   python scripts/fetch_ngrams.py
"""
import csv
import gzip
import json
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "categories.yaml"
DATA_PATH = ROOT / "data" / "hits.csv"
STATE_PATH = ROOT / "data" / "ngrams_state.txt"
BASE_URL = "https://data.gdeltproject.org/gdeltv5/weblegacy/ngrams"

FIELDNAMES = [
    "fetched_at",
    "category_id",
    "category_label",
    "title",
    "url",
    "seendate",
    "domain",
    "language",
    "sourcecountry",  # not available in this dataset (kept for older rows)
    "tone",  # not available in this dataset (kept for older rows)
    "watchlist_country",  # the focus country mentioned most in the article
    "relevance",  # reserved for optional AI triage
    "relevance_reason",
    "matched_term",
    "context_match",  # retired: no longer filled in or used
    "scope_tags",  # semicolon-separated: high_school, seah
]

MAX_MINUTES_PER_RUN = 60
PUBLISH_DELAY_MINUTES = 5
DEFAULT_LOOKBACK_MINUTES = 30
REQUEST_TIMEOUT = 20
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
}

# Thresholds for scope tags, counted over the article's 4-word phrases. A
# single mention of a word shows up in up to 4 overlapping phrases, so a
# threshold of 6-8 means roughly 2 mentions. These are starting guesses:
# check the tags in data/hits.csv after a few days and adjust.
HIGH_SCHOOL_MIN_HITS = 6  # and school words must be at least 2x university words
SEAH_MIN_HITS = 8  # a SEAH word in the headline always counts

# URL path segments that mark listing/archive pages rather than articles.
NON_ARTICLE_PATH_SEGMENTS = {
    "author", "authors", "tag", "tags", "category", "categories",
    "topic", "topics", "archive", "archives", "search", "page",
}


def looks_like_listing_page(url):
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return any(seg in NON_ARTICLE_PATH_SEGMENTS for seg in path.split("/") if seg)


# ---------------------------------------------------------------- matching

_NEVER = re.compile(r"(?!x)x")  # matches nothing (used for empty lists)


def _alternation(phrases):
    parts = []
    for p in sorted({p.strip().lower() for p in phrases if p and p.strip()}, key=len, reverse=True):
        words = [w for w in re.split(r"[\s\-]+", p) if w]
        parts.append(r"[\s\-]+".join(re.escape(w) for w in words))
    return "|".join(parts)


def prefix_re(phrases):
    """Match from the start of a word; the end stays open, so plurals and
    endings still match ('student activist' matches 'student activists')."""
    alt = _alternation(phrases)
    return re.compile(r"(?<!\w)(?:" + alt + r")") if alt else _NEVER


def word_re(phrases):
    """Match whole words only (optional plural 's'), so 'uk' cannot match
    inside 'ukraine' or 'mukherjee', and 'india' cannot match 'indiana'."""
    alt = _alternation(phrases)
    return re.compile(r"(?<!\w)(?:" + alt + r")s?(?!\w)") if alt else _NEVER


# Headline-only extras, used when re-tagging older rows where only the title
# is available. Plural "schools" almost always means K-12 ("500 schools
# closed") unless preceded by a university-type word ("law schools").
SCHOOL_TITLE_EXTRA = re.compile(
    r"(?<!\w)(?<!law )(?<!business )(?<!medical )(?<!journalism )(?<!graduate )"
    r"(?<!grad )(?<!nursing )(?<!art )(?<!film )schools(?!\w)|"
    r"(?<!\w)school (?:protests?|strikes?|closures?)(?!\w)"
)


# One-time rule used ONLY when upgrading older rows (headline is all we
# have): the French/German school-protest wave dominated the first week of
# data, and many of its headlines say "student protests in France" without
# the word "school". New rows never use this; they are read from full text.
LEGACY_SCHOOL_PROTEST_TITLE = re.compile(
    r"(?=.*\b(?:france|french|paris|germany|german)\b)"
    r"(?=.*\b(?:students?|schools?|teens?|teenagers?|pupils?|youth|teachers?)\b)"
    r"(?=.*\b(?:protests?|protesters?|unrest|riots?|blockades?|strikes?|demonstrations?|clashes)\b)"
)


# Same idea for the two SEAH-related cases that dominated the first week
# (Cornell and Lovely Professional University): their headlines often name
# the case rather than the offence. Older rows only; new rows use full text.
LEGACY_SEAH_TITLE = re.compile(r"jane doe|justice for survivors|lovely professional university|(?<!\w)lpu(?!\w)")


class Matchers:
    def __init__(self, cfg):
        self.identity = prefix_re(cfg["identity_phrases"])
        self.categories = []
        for c in cfg["categories"]:
            terms = [(t.lower(), prefix_re([t])) for t in c["terms"]]
            self.categories.append({"id": c["id"], "label": c["label"], "terms": terms})
        self.countries = [
            (c["name"], word_re([c["name"]] + list(c.get("aliases", []))))
            for c in cfg.get("focus_countries", [])
        ]
        scope = cfg.get("scope_terms", {})
        self.school = word_re(scope.get("high_school", []))
        self.tertiary = word_re(scope.get("tertiary", []))
        self.seah = word_re(scope.get("seah", []))


def load_matchers():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return Matchers(yaml.safe_load(f))


def best_category(text, m):
    best, best_score = (None, None, None), 0
    for c in m.categories:
        score, top_term, top_n = 0, None, 0
        for term, rx in c["terms"]:
            n = len(rx.findall(text))
            score += n
            if n > top_n:
                top_n, top_term = n, term
        if score > best_score:
            best, best_score = (c["id"], c["label"], top_term), score
    return best


def best_country(text, m):
    best, best_n = "", 0
    for name, rx in m.countries:
        n = len(rx.findall(text))
        if n > best_n:
            best, best_n = name, n
    return best


def classify_scope_text(text, title, m):
    """Scope tags from an article's full text (new rows)."""
    tags = []
    school = len(m.school.findall(text))
    tertiary = len(m.tertiary.findall(text))
    if school >= HIGH_SCHOOL_MIN_HITS and school >= 2 * tertiary:
        tags.append("high_school")
    if len(m.seah.findall(text)) >= SEAH_MIN_HITS or m.seah.search(title.lower()):
        tags.append("seah")
    return ";".join(tags)


def classify_scope_title(title, m):
    """Scope tags from the headline alone (older rows with no full text)."""
    t = title.lower()
    tags = []
    school_like = (
        m.school.search(t) or SCHOOL_TITLE_EXTRA.search(t) or LEGACY_SCHOOL_PROTEST_TITLE.search(t)
    )
    if school_like and not m.tertiary.search(t):
        tags.append("high_school")
    if m.seah.search(t) or LEGACY_SEAH_TITLE.search(t):
        tags.append("seah")
    return ";".join(tags)


# ------------------------------------------------------------ data upgrade

def ensure_schema(m):
    """Upgrade an older data/hits.csv in place: new header, padded rows, and
    headline-based scope tags and country for the older rows. Does nothing
    if the header is already current. Refuses to touch an unrecognised
    header rather than risk misaligning columns."""
    if not DATA_PATH.exists():
        return
    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        rows = [r for r in csv.reader(f) if r]
    if not rows:
        return
    header = rows[0]
    if header == FIELDNAMES:
        return
    if FIELDNAMES[: len(header)] != header:
        raise SystemExit(
            "ERROR: data/hits.csv has a header this script doesn't recognise, so it "
            "will not modify the file. Header found: " + ",".join(header)
        )

    idx = {name: i for i, name in enumerate(FIELDNAMES)}
    migrated = [FIELDNAMES]
    n_school = n_seah = 0
    for row in rows[1:]:
        if len(row) > len(header):
            raise SystemExit("ERROR: a row in data/hits.csv has more columns than its header; stopping.")
        row = row + [""] * (len(FIELDNAMES) - len(row))
        title = row[idx["title"]]
        row[idx["watchlist_country"]] = best_country(title.lower(), m)
        tags = classify_scope_title(title, m)
        row[idx["scope_tags"]] = tags
        n_school += "high_school" in tags
        n_seah += "seah" in tags
        migrated.append(row)

    fd, tmp = tempfile.mkstemp(dir=DATA_PATH.parent, suffix=".csv")
    with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(migrated)
    os.replace(tmp, DATA_PATH)
    print(
        f"Upgraded data/hits.csv: {len(migrated) - 1} existing rows re-tagged from "
        f"headlines ({n_school} high_school, {n_seah} seah); added column scope_tags."
    )


# --------------------------------------------------------------- pipeline

def load_existing_urls():
    if not DATA_PATH.exists():
        return set()
    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        return {row["url"] for row in csv.DictReader(f) if row.get("url")}


def load_state():
    if STATE_PATH.exists():
        text = STATE_PATH.read_text().strip()
        if text:
            return datetime.strptime(text, "%Y%m%d%H%M")
    return None


def save_state(dt):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(dt.strftime("%Y%m%d%H%M"))


def minute_range(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(minutes=1)


def fetch_minute_files(dt):
    """Download the ngrams + toc pair for one minute. Returns
    (ngrams_text, toc_lines), or (None, None) if there is no file."""
    ts = dt.strftime("%Y%m%d%H%M00")
    try:
        ngrams_resp = requests.get(f"{BASE_URL}/{ts}.ngrams.txt.gz", headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if ngrams_resp.status_code == 404:
            return None, None
        ngrams_resp.raise_for_status()

        toc_resp = requests.get(f"{BASE_URL}/{ts}.toc.json.gz", headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if toc_resp.status_code == 404:
            return None, None
        toc_resp.raise_for_status()

        ngrams_text = gzip.decompress(ngrams_resp.content).decode("utf-8", errors="replace")
        toc_text = gzip.decompress(toc_resp.content).decode("utf-8", errors="replace")
        return ngrams_text, toc_text.splitlines()
    except requests.exceptions.RequestException as e:
        print(f"    ! request error for {ts}: {e}")
        return None, None


def parse_toc(toc_lines):
    toc = {}
    for line in toc_lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            toc[rec["ID"]] = rec
        except (json.JSONDecodeError, KeyError):
            continue
    return toc


def build_doc_text(ngrams_text):
    """Group the 4-word phrases by article into one lowercased text each."""
    chunks = {}
    for line in ngrams_text.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        try:
            docid = int(parts[0])
        except ValueError:
            continue
        chunks.setdefault(docid, []).append(parts[1].lower())
    return {docid: " ".join(c) for docid, c in chunks.items()}


def reformat_date(iso_date):
    try:
        dt = datetime.strptime(iso_date[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%Y%m%dT%H%M%SZ")
    except (ValueError, TypeError):
        return ""


def domain_from_url(url):
    try:
        netloc = urlparse(url).netloc
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception:
        return ""


def process_minute(dt, m, existing_urls):
    """Returns (rows, file_existed)."""
    ngrams_text, toc_lines = fetch_minute_files(dt)
    if ngrams_text is None:
        return [], False

    toc = parse_toc(toc_lines)
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for docid, text in build_doc_text(ngrams_text).items():
        if not m.identity.search(text):
            continue
        cat_id, cat_label, matched_term = best_category(text, m)
        if cat_id is None:
            continue
        rec = toc.get(docid)
        if not rec:
            continue
        url = rec.get("url", "")
        if not url or url in existing_urls or looks_like_listing_page(url):
            continue
        existing_urls.add(url)
        title = rec.get("title", "")
        rows.append(
            {
                "fetched_at": fetched_at,
                "category_id": cat_id,
                "category_label": cat_label,
                "title": title,
                "url": url,
                "seendate": reformat_date(rec.get("date", "")),
                "domain": domain_from_url(url),
                "language": rec.get("lang", ""),
                "sourcecountry": "",
                "tone": "",
                "watchlist_country": best_country(text, m),
                "relevance": "",
                "relevance_reason": "",
                "matched_term": matched_term,
                "context_match": "",
                "scope_tags": classify_scope_text(text, title, m),
            }
        )
    return rows, True


def append_rows(rows):
    if not rows:
        return
    file_exists = DATA_PATH.exists()
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    m = load_matchers()
    ensure_schema(m)
    existing_urls = load_existing_urls()

    now = datetime.utcnow().replace(second=0, microsecond=0)
    end = now - timedelta(minutes=PUBLISH_DELAY_MINUTES)

    last = load_state()
    if last is None:
        start = end - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
        print(f"No state file found, starting {DEFAULT_LOOKBACK_MINUTES} minutes back.")
    else:
        start = last + timedelta(minutes=1)

    if start > end:
        print("Nothing new to process yet.")
        return

    minutes = list(minute_range(start, end))[:MAX_MINUTES_PER_RUN]
    print(
        f"Processing {len(minutes)} minute(s): "
        f"{minutes[0].strftime('%Y-%m-%d %H:%M')} to {minutes[-1].strftime('%Y-%m-%d %H:%M')} UTC"
    )

    total_new = files_found = 0
    for dt in minutes:
        rows, file_existed = process_minute(dt, m, existing_urls)
        files_found += file_existed
        if rows:
            append_rows(rows)
            total_new += len(rows)
            print(f"  {dt.strftime('%H:%M')}: {len(rows)} new match(es)")
        save_state(dt)

    print(
        f"\nDone. Checked {len(minutes)} minute(s); GDELT had published files for "
        f"{files_found} of them; added {total_new} new row(s)."
    )
    if files_found == 0:
        print(
            "Note: no published files in this whole window. One quiet run is normal; "
            "several in a row would be worth investigating."
        )


if __name__ == "__main__":
    main()
