#!/usr/bin/env python3
"""
Fetch and scan GDELT's Web NGrams dataset (quadgram files, published every
minute) for mentions of student activism + repression terms, entirely
locally — no queries sent to GDELT's strained search API at all.

Why this replaces fetch_gdelt.py: GDELT's DOC 2.0 search API has been
unreliable for weeks — GDELT's own maintainer confirmed their search/API
infrastructure is mid-migration to new infrastructure and temporarily
can't handle normal query volume, and specifically pointed us at this
ngrams dataset as the recommended alternative in the meantime. Instead of
querying GDELT, this script downloads small pre-built files GDELT already
publishes every minute — quadgram (4-word phrase) frequency tables per
article, plus a table-of-contents mapping each article to its URL/title —
and searches them locally. There is no query to rate-limit or reject,
since nothing is being asked of GDELT beyond a plain static file download.

Bonus: this dataset covers each article's FULL TEXT, not just the
headline (which is all the old DOC API gave us), so country and
noise-filter matching are both meaningfully more accurate now.

How it works:
  - GDELT publishes a pair of files every minute at a predictable URL:
      https://data.gdeltproject.org/gdeltv5/weblegacy/ngrams/<TIMESTAMP>.ngrams.txt.gz
      https://data.gdeltproject.org/gdeltv5/weblegacy/ngrams/<TIMESTAMP>.toc.json.gz
    TIMESTAMP is YYYYMMDDHHMM00. Files aren't published for every single
    minute (GDELT's own docs describe a "15 minute heartbeat" with gaps),
    so a missing file (404) is normal and expected, not an error.
  - This script keeps a small state file (data/ngrams_state.txt) recording
    the last minute it attempted, and each run walks forward from there,
    processing up to MAX_MINUTES_PER_RUN minutes so a single run can't take
    too long even after a gap (e.g. the very first run, or after an
    outage).
  - For each minute: download both files, group quadgrams by document ID,
    and for each document check whether an identity phrase AND a
    repression term both appear anywhere in its (reassembled) full text.
    Matches are tagged with category/term, country, and noise-context the
    same way the old script did, just checked against full text instead
    of headline-only.

Designed to run frequently via GitHub Actions (every 15 minutes; see
.github/workflows/gdelt-ngrams-monitor.yml) since each run is cheap and
GDELT describes this as a near-realtime feed, not something meant to be
queried for a big historical window in one go. Also runs the same
locally:

    python scripts/fetch_ngrams.py
"""
import csv
import gzip
import json
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
    "sourcecountry",
    "tone",
    "watchlist_country",
    "relevance",
    "relevance_reason",
    "matched_term",
    "context_match",
]

MAX_MINUTES_PER_RUN = 60      # bound runtime even after a gap/outage
PUBLISH_DELAY_MINUTES = 5     # GDELT recommends requesting "5 minutes ago" to be safe
DEFAULT_LOOKBACK_MINUTES = 30  # if no state file yet, start this far back
REQUEST_TIMEOUT = 20
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
}


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    identity_phrases = [p.lower() for p in cfg["identity_phrases"]]
    watchlist = cfg.get("focus_countries", cfg.get("watchlist_countries", []))
    context_terms = [t.lower() for t in cfg.get("context_terms", [])]

    categories = []
    for c in cfg["categories"]:
        categories.append(
            {"id": c["id"], "label": c["label"], "terms": [t.lower() for t in c["terms"]]}
        )

    return identity_phrases, categories, watchlist, context_terms


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
    """Try to download the ngrams + toc pair for one minute. Returns
    (ngrams_text, toc_lines) or (None, None) if this minute has no
    published file — a normal, expected gap, not an error."""
    ts = dt.strftime("%Y%m%d%H%M00")
    ngrams_url = f"{BASE_URL}/{ts}.ngrams.txt.gz"
    toc_url = f"{BASE_URL}/{ts}.toc.json.gz"

    try:
        ngrams_resp = requests.get(ngrams_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if ngrams_resp.status_code == 404:
            return None, None
        ngrams_resp.raise_for_status()

        toc_resp = requests.get(toc_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
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
    """Group quadgrams by document ID into one lowercased text blob per
    document, so phrase matches can be checked with simple substring
    tests (any phrase up to 4 words appears intact in at least one
    quadgram window, since windows slide by one word at a time)."""
    doc_chunks = {}
    for line in ngrams_text.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        docid_str, quadgram = parts[0], parts[1]
        try:
            docid = int(docid_str)
        except ValueError:
            continue
        doc_chunks.setdefault(docid, []).append(quadgram.lower())
    return {docid: " ".join(chunks) for docid, chunks in doc_chunks.items()}


def reformat_date(iso_date):
    """Convert GDELT's ISO date ("2026-06-30T20:16:00.000Z") into the
    compact format ("20260630T201600Z") the existing dashboard expects."""
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


def has_any(text, phrases):
    return any(p in text for p in phrases)


def match_category(text, categories):
    for cat in categories:
        for term in cat["terms"]:
            if term in text:
                return cat["id"], cat["label"], term
    return None, None, None


def match_watchlist_country(text, watchlist):
    for country in watchlist:
        names = [country["name"].lower()] + [a.lower() for a in country.get("aliases", [])]
        if any(n in text for n in names):
            return country["name"]
    return ""


def process_minute(dt, identity_phrases, categories, watchlist, context_terms, existing_urls):
    """Returns (rows, file_existed). file_existed distinguishes 'GDELT had
    no file published for this minute' (a normal gap) from 'a file
    existed but had no relevant matches' (also normal, but a genuinely
    different, useful thing to know when checking on the pipeline)."""
    ngrams_text, toc_lines = fetch_minute_files(dt)
    if ngrams_text is None:
        return [], False  # no file for this minute — normal gap

    toc = parse_toc(toc_lines)
    doc_text = build_doc_text(ngrams_text)
    fetched_at = datetime.now(timezone.utc).isoformat()

    rows = []
    for docid, text in doc_text.items():
        if not has_any(text, identity_phrases):
            continue
        cat_id, cat_label, matched_term = match_category(text, categories)
        if cat_id is None:
            continue

        rec = toc.get(docid)
        if not rec:
            continue
        url = rec.get("url", "")
        if not url or url in existing_urls:
            continue
        existing_urls.add(url)

        rows.append(
            {
                "fetched_at": fetched_at,
                "category_id": cat_id,
                "category_label": cat_label,
                "title": rec.get("title", ""),
                "url": url,
                "seendate": reformat_date(rec.get("date", "")),
                "domain": domain_from_url(url),
                "language": rec.get("lang", ""),
                "sourcecountry": "",  # not available in this dataset
                "tone": "",  # not available in this dataset
                "watchlist_country": match_watchlist_country(text, watchlist),
                "matched_term": matched_term,
                "context_match": "yes" if has_any(text, context_terms) else "no",
                "relevance": "",
                "relevance_reason": "",
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
    identity_phrases, categories, watchlist, context_terms = load_config()
    existing_urls = load_existing_urls()

    # Naive UTC throughout for date-range math (matches the state file's
    # naive format) — timezone.utc is used separately only for the
    # human-readable fetched_at timestamp stored on each row.
    now = datetime.utcnow().replace(second=0, microsecond=0)
    end = now - timedelta(minutes=PUBLISH_DELAY_MINUTES)

    last = load_state()
    if last is None:
        start = end - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
        print(f"No state file found — starting {DEFAULT_LOOKBACK_MINUTES} minutes back.")
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

    total_new = 0
    files_found = 0
    for dt in minutes:
        rows, file_existed = process_minute(
            dt, identity_phrases, categories, watchlist, context_terms, existing_urls
        )
        if file_existed:
            files_found += 1
        if rows:
            append_rows(rows)
            total_new += len(rows)
            print(f"  {dt.strftime('%H:%M')} — {len(rows)} new match(es)")
        save_state(dt)  # advance state even on a miss, so we never get stuck retrying

    print(
        f"\nDone. Checked {len(minutes)} minute(s); GDELT had published files for "
        f"{files_found} of them; added {total_new} new row(s)."
    )
    if files_found == 0:
        print(
            "Note: zero published files found across this entire window. A single "
            "quiet run is normal, but if this keeps happening across several "
            "consecutive runs, that would be worth investigating (unlike zero "
            "MATCHES, which is expected fairly often)."
        )


if __name__ == "__main__":
    main()
