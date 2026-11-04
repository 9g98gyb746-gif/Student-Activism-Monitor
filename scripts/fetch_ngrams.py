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
    article when its full text contains an identity phrase (or the name of
    one of SAIH's partners) AND a repression term (see
    config/categories.yaml).
  - Each recorded article is tagged with:
      * category: the repression category mentioned most
      * country and region: where the story is mainly set (see locate())
      * watchlist_country: the country again, but only if it is one of
        SAIH's partner countries (drives the partner badge and filter)
      * scope_tags: high_school, seah (drive the dashboard's "Hide" filters)
  - URLs that look like listing pages (author, tag, category, search,
    pagination), home pages, very short or generic titles, and the domains
    and web-address patterns listed under "skip" in the config (for example
    the film and TV sections of entertainment trade press) are not recorded
    at all.
  - Old articles that GDELT picked up again are skipped: when the web address
    contains a date (/2024/05/08/ or 2024-05-08) that is more than
    URL_MAX_AGE_DAYS older than the date GDELT gives for the article, it is
    not recorded. Addresses without a date cannot be checked.
  - A few ambiguous terms ("suspended", "charged", "killed" ...) only count
    when a protest/police word appears within a few words of them (the
    "second_signal" section of the config). Articles whose only repression
    terms fail that test are not recorded. "Suspended", "expelled",
    "disciplinary action", "killed", "arrested", "detained" and a few more
    need a student/school word (or a partner name) nearby instead of any
    protest word.
  - A repression term that is part of a phrase listed under "ignore_phrases"
    in the config ("assaulted" in "sexually assaulted") does not count where
    that phrase occurs, and an article tagged
    SEAH whose only terms are ambiguous ones (or "assaulted") is not
    recorded: those are almost always sexual-misconduct stories, not
    repression of activists.
  - The proximity check (PROXIMITY_WORDS) is still used to pick the category
    from the part of the article closest to the student-activism phrase, but
    articles with no term nearby are recorded like any other. (They used to be
    tagged "weak_match"; in practice they were as often real as not.)

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
import time
from collections import Counter, defaultdict, deque
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
    "watchlist_country",  # the main country, only if it is a partner country
    "relevance",  # reserved for optional AI triage
    "relevance_reason",
    "matched_term",
    "context_match",  # retired: no longer filled in or used
    "scope_tags",  # semicolon-separated: high_school, seah
    "country",  # the country the story is mainly about (may be blank)
    "region",  # region of that country, or "Unclear"
]

UNCLEAR = "Unclear"

MAX_MINUTES_PER_RUN = 1440  # at most a day of GDELT files per run (so a run can catch up after a gap)
MAX_RUN_SECONDS = 15 * 60  # stop starting new minutes after this long, so the run ends and commits
PUBLISH_DELAY_MINUTES = 5
DEFAULT_LOOKBACK_MINUTES = 30
URL_MAX_AGE_DAYS = 365  # skip articles whose web address carries a date this much older than GDELT's date
REQUEST_TIMEOUT = 20
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
}

# Thresholds, counted over the article's 4-word phrases. A single mention of
# a word shows up in up to 4 overlapping phrases, so a threshold of 6-8
# means roughly 2 mentions. These are starting guesses: check the tags in
# data/hits.csv after a few days and adjust.
HIGH_SCHOOL_MIN_HITS = 6  # and school words must be at least 2x university words
SEAH_MIN_HITS = 8  # a SEAH word in the headline always counts
MIN_REGION_HITS = 8  # a region needs this many country mentions to be chosen
REGION_DOMINANCE = 2  # ...and must have at least this many times the runner-up

# Proximity check. A repression term only "counts" if it appears within
# PROXIMITY_WORDS words of an identity phrase. GDELT stores each article as an
# unordered bag of overlapping 4-word phrases, so the word order is rebuilt by
# chaining phrases that overlap by three words. A 3-word overlap that occurs
# more than MAX_TRIGRAM_REPEATS times in an article (boilerplate such as
# "of the university") is not followed, because it would join unrelated parts
# of the page. Articles with no term near an identity phrase are still
# recorded too (the category is then taken from the whole article).
PROXIMITY_WORDS = 50
MAX_TRIGRAM_REPEATS = 5
DIAGNOSTIC_MAX_WORDS = 600  # how far to look when logging the distance of weak matches

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


def _norm(s):
    """Lowercase, with spaces and hyphens treated the same."""
    return re.sub(r"[\s\-]+", " ", s.strip().lower())


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


class _AnyOf:
    """Several compiled patterns used as one: search() is true if any matches."""
    def __init__(self, *patterns):
        self.patterns = [p for p in patterns if p is not _NEVER]

    def search(self, text):
        for p in self.patterns:
            hit = p.search(text)
            if hit:
                return hit
        return None


class Matchers:
    def __init__(self, cfg):
        # Identity = the general phrases (matched from the start of a word) plus
        # the names of SAIH's partners (whole words only, so "cric" cannot match
        # "cricket"). A partner name counts like an identity phrase: the article
        # still needs a repression term, with the same context rules.
        partner = cfg.get("partner_phrases") or []
        generic = cfg.get("partner_phrases_generic") or []
        self.identity = _AnyOf(prefix_re(cfg["identity_phrases"]), word_re(partner), word_re(generic))
        self.categories = []
        for c in cfg["categories"]:
            terms = [(t.lower(), prefix_re([t])) for t in c["terms"]]
            self.categories.append({"id": c["id"], "label": c["label"], "terms": terms})
        self.any_term = prefix_re([t for c in cfg["categories"] for t in c["terms"]])

        # Ignore phrases ("sexually assaulted"): a category term inside one of them
        # ("assaulted") does not count where the phrase occurs.
        ign = [str(x).lower() for x in cfg.get("ignore_phrases", [])]
        self.ignore = prefix_re(ign)
        self.ignore_terms = {
            t for c in cfg["categories"] for t, rx in ((t.lower(), prefix_re([t])) for t in c["terms"])
            if any(rx.search(x) for x in ign)
        }

        # Second signal: ambiguous terms only count with a context word nearby.
        ss = cfg.get("second_signal") or {}
        self.ambiguous = {str(t).strip().lower() for t in ss.get("terms", [])}
        known = {t.lower() for c in cfg["categories"] for t in c["terms"]}
        for t in self.ambiguous:
            if t not in known:
                raise ValueError(f"second_signal term '{t}' is not a term in any category")
        self.context = prefix_re(ss.get("context_words", []))
        self.second_distance = int(ss.get("distance", 12))
        # Stricter terms: these need a student/school word nearby, not just any
        # protest or police word (a "suspended" metro line in a riot story is not
        # a student suspension).
        self.strict_terms = {str(t).strip().lower() for t in ss.get("student_context_terms", [])}
        for t in self.strict_terms:
            if t not in self.ambiguous:
                raise ValueError(f"student_context_terms '{t}' must also be listed under second_signal terms")
        # A partner name also counts as a student word: "police arrested the ZINASU
        # secretary general" is about students even without the word "student".
        self.student_context = _AnyOf(prefix_re(ss.get("student_context_words", [])), word_re(partner))
        if self.strict_terms and not self.student_context.patterns:
            raise ValueError("student_context_terms has terms but no student_context_words")
        if self.ambiguous and self.context is _NEVER:
            raise ValueError("second_signal has terms but no context_words")

        # Records that are never recorded: junk domains and generic titles.
        sk = cfg.get("skip") or {}
        self.skip_domains = [str(d).strip().lower() for d in sk.get("domains", []) if str(d).strip()]
        self.skip_title_res = [re.compile(p, re.I) for p in sk.get("title_patterns", [])]
        self.skip_url_res = [re.compile(p, re.I) for p in sk.get("url_patterns", [])]
        self.min_title_words = int(sk.get("min_title_words", 0))
        self.min_title_chars = int(sk.get("min_title_chars", 0))

        if "regions" not in cfg or "partner_countries" not in cfg:
            raise SystemExit(
                "ERROR: config/categories.yaml has no 'regions' / 'partner_countries' section. "
                "Replace config/categories.yaml, scripts/fetch_ngrams.py and docs/index.html together."
            )

        # country name -> region, and every alias (and the name) -> country name
        self.country_region = {}
        self.alias_to_country = {}
        for region in cfg["regions"]:
            for entry in region["countries"]:
                parts = [p.strip() for p in str(entry).split("|") if p.strip()]
                name, aliases = parts[0], parts[1:]
                if name in self.country_region:
                    raise ValueError(f"country listed twice in regions: {name}")
                self.country_region[name] = region["name"]
                for alias in [name] + aliases:
                    key = _norm(alias)
                    if self.alias_to_country.get(key, name) != name:
                        raise ValueError(
                            f"alias '{alias}' is used by both {self.alias_to_country[key]} and {name}"
                        )
                    self.alias_to_country[key] = name

        self.partners = list(cfg["partner_countries"])
        for p in self.partners:
            if p not in self.country_region:
                raise ValueError(f"partner country '{p}' is not listed under any region")

        alt = _alternation(self.alias_to_country.keys())
        self.country_re = re.compile(r"(?<!\w)(" + alt + r")s?(?!\w)")

        scope = cfg.get("scope_terms", {})
        self.school = word_re(scope.get("high_school", []))
        self.tertiary = word_re(scope.get("tertiary", []))
        self.seah = word_re(scope.get("seah", []))


def load_matchers():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return Matchers(yaml.safe_load(f))


def term_hits(phrase, m):
    """Repression terms found in one phrase: [(category_index, term, count,
    is_ambiguous), ...]."""
    out = []
    if not m.any_term.search(phrase):
        return out
    for ci, c in enumerate(m.categories):
        for term, rx in c["terms"]:
            n = len(rx.findall(phrase))
            if n:
                out.append((ci, term, n, term in m.ambiguous))
    return out


def pick_category(hit_lists, m):
    """hit_lists: one list of term_hits() per phrase. Returns (category id,
    label, most-mentioned term), or (None, None, None) if there are no hits.
    The category mentioned most wins; ties go to the one listed first."""
    score = Counter()
    per_term = Counter()
    for hits in hit_lists:
        for ci, term, n, _ in hits:
            score[ci] += n
            per_term[(ci, term)] += n
    best_ci, best_score = None, 0
    for ci in range(len(m.categories)):
        if score[ci] > best_score:
            best_ci, best_score = ci, score[ci]
    if best_ci is None:
        return None, None, None
    top_term = max(
        (t for (ci, t) in per_term if ci == best_ci), key=lambda t: per_term[(best_ci, t)]
    )
    return m.categories[best_ci]["id"], m.categories[best_ci]["label"], top_term


# "Pro-Palestinian", "anti-Israel" and similar describe a stance, not a place:
# a "pro-Palestinian protest" is almost always somewhere else. They are
# ignored when working out where a story is set.
STANCE_RE = re.compile(
    r"(?<!\w)(?:pro|anti)[\s\-](?:palestin\w*|israel\w*|zionis\w*|gaza|hamas)(?!\w)"
    r"|(?<!\w)(?:solidarity with|support(?:ing)?(?: for)?|in support of|stand(?:ing)? with|free|for) palestin\w*"
)


def rank_countries(text, m):
    """Countries named in the text, most-mentioned first (ties: whichever
    appears first). Returns [(country, region, mentions), ...]."""
    text = STANCE_RE.sub(" ", text)
    counts, first = Counter(), {}
    for hit in m.country_re.finditer(text):
        name = m.alias_to_country.get(_norm(hit.group(1)))
        if name:
            counts[name] += 1
            first.setdefault(name, hit.start())
    ranked = sorted(counts, key=lambda n: (-counts[n], first[n]))
    return [(n, m.country_region[n], counts[n]) for n in ranked]


# Many headlines end with the outlet's name ("... - Dominican Republic Post",
# "... | Muscat Daily| Oman News"), which would otherwise count as a mention
# of that country. Trailing segments that start with a capital letter and are
# at most 45 characters long are ignored for location purposes.
_SUFFIX_RE = re.compile(r"(?:\s*\|\s*|\s[-\u2013\u2014]\s)([A-Z\u00C0-\u00DE][^|\u2013\u2014]{1,44})$")
_US_RE = re.compile(r"(?<![A-Za-z])(?:US|USA|U\.S\.)(?![A-Za-z])")


def headline_for_location(title):
    t = title
    for _ in range(5):
        hit = _SUFFIX_RE.search(t)
        if hit and len(t[: hit.start()].strip()) >= 20:
            t = t[: hit.start()]
        else:
            break
    # Upper-case "US" is the country; lower-case "us" is a pronoun, and the
    # matcher lowercases everything, so spot it here while case is intact.
    if _US_RE.search(t):
        t += " united states"
    return t


def locate(title, text, m):
    """Where is the story mainly set? Returns (country, region).

    1. If the headline names countries from a single region, use the one
       named most (that is what the story is about).
    2. Otherwise, if the full text is available, pick the region with the
       most country mentions, but only if it has at least MIN_REGION_HITS
       and REGION_DOMINANCE times the runner-up.
    3. Otherwise the region is "Unclear" (a wrong badge is worse than none).

    Palestine is only ever taken from the headline (step 1), never from the
    full text: campus solidarity stories in the US or UK mention Palestine
    dozens of times, which used to give them the Palestine (partner country)
    badge. A headline that names Gaza, the West Bank or Palestine as the
    place still gets it.
    """
    from_title = rank_countries(headline_for_location(title).lower(), m)
    if from_title and len({region for _, region, _ in from_title}) == 1:
        return from_title[0][0], from_title[0][1]

    if text:
        ranked = [x for x in rank_countries(text, m) if x[0] != "Palestine"]
        by_region = Counter()
        for _, region, n in ranked:
            by_region[region] += n
        top = by_region.most_common(2)
        if top:
            region, n = top[0]
            runner_up = top[1][1] if len(top) > 1 else 0
            if n >= MIN_REGION_HITS and n >= REGION_DOMINANCE * runner_up:
                country = next(c for c, r, _ in ranked if r == region)
                return country, region
    return "", UNCLEAR


def partner_of(country, m):
    return country if country in m.partners else ""


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


# --------------------------------------------------------------- proximity

def word_distances(items, sources, limit):
    """items: [(phrase, count), ...] for one article (phrases lowercased).
    sources: indexes of phrases to measure from. Returns {index: words away}
    for every phrase reachable within `limit` steps, following overlaps of
    three words in either direction."""
    words = [p.split(" ") for p, _ in items]
    first3 = defaultdict(list)
    repeats = Counter()
    for i, w in enumerate(words):
        if len(w) == 4:
            first3[tuple(w[:3])].append(i)
            repeats[tuple(w[:3])] += items[i][1]
    succ = [[] for _ in words]
    pred = [[] for _ in words]
    for i, w in enumerate(words):
        if len(w) != 4:
            continue
        tri = tuple(w[1:])
        if repeats.get(tri, 0) > MAX_TRIGRAM_REPEATS:
            continue
        for j in first3.get(tri, ()):
            if j != i:
                succ[i].append(j)
                pred[j].append(i)
    dist = {s: 0 for s in sources}
    queue = deque(sources)
    while queue:
        u = queue.popleft()
        if dist[u] >= limit:
            continue
        for nxt in succ[u] + pred[u]:
            if nxt not in dist:
                dist[nxt] = dist[u] + 1
                queue.append(nxt)
    return dist


def analyse_doc(items, m):
    """Decide whether an article is a match, and how good a match.

    items: [(phrase, count), ...] for one article (phrases lowercased).
    Returns None if the article has no usable repression term, otherwise a
    dict with:
      category - (id, label, term), the repression category mentioned most
      weak     - True if no usable term is within PROXIMITY_WORDS of an
                 identity phrase (the category is then taken from the whole
                 article)
      nearest  - words between an identity phrase and the closest usable
                 term (up to DIAGNOSTIC_MAX_WORDS), or None
    Returns {"skip": "needs_context"} if the article has repression terms but
    all of them are ambiguous ones with no context word nearby.

    A term counts if it is not ambiguous, or if a context word (protest,
    police, arrest ...) occurs within second_distance words of it. The terms
    listed under student_context_terms ("suspended", "expelled" ...) need a
    student word (student, school, university, activist ...) nearby instead."""
    hits = [term_hits(p, m) for p, _ in items]
    if not any(hits):
        return None

    confirmed = set()
    if any(h[3] for hl in hits for h in hl):
        ctx_sources = [i for i, (p, _) in enumerate(items) if m.context.search(p)]
        if ctx_sources:
            confirmed = set(word_distances(items, ctx_sources, m.second_distance))
    confirmed_strict = set()
    if m.strict_terms and any(h[1] in m.strict_terms for hl in hits for h in hl):
        st_sources = [i for i, (p, _) in enumerate(items) if m.student_context.search(p)]
        if st_sources:
            confirmed_strict = set(word_distances(items, st_sources, m.second_distance))
    # A term that is part of an ignore phrase ("sexually assaulted") does not count
    # in or within three words of that phrase (a 4-word phrase can show the term
    # without the word that comes before it).
    ignored = set()
    if m.ignore_terms:
        ign_sources = [i for i, (p, _) in enumerate(items) if m.ignore.search(p)]
        if ign_sources:
            ignored = set(word_distances(items, ign_sources, 3))
    usable = [
        [h for h in hl
         if (not h[3] or i in (confirmed_strict if h[1] in m.strict_terms else confirmed))
         and not (i in ignored and h[1] in m.ignore_terms)]
        for i, hl in enumerate(hits)
    ]
    if not any(usable):
        # Say which terms failed, so the run log shows why the article was skipped.
        failed = sorted({
            h[1] + (" [needs student word]" if h[1] in m.strict_terms else "")
            for hl in hits for h in hl
        })
        return {"skip": "needs_context", "terms": failed}

    overall = pick_category(usable, m)
    sources = [i for i, (p, _) in enumerate(items) if m.identity.search(p)]
    if not sources:
        return {"category": overall, "weak": True, "nearest": None}

    dist = word_distances(items, sources, DIAGNOSTIC_MAX_WORDS)
    term_dists = [d for i, d in dist.items() if usable[i]]
    nearest = min(term_dists) if term_dists else None
    limit = PROXIMITY_WORDS + 3  # a phrase is 4 words long
    near = [usable[i] if dist.get(i, DIAGNOSTIC_MAX_WORDS + 1) <= limit else [] for i in range(len(items))]
    near_cat = pick_category(near, m)
    if near_cat[0] is None:
        return {"category": overall, "weak": True, "nearest": nearest}
    return {"category": near_cat, "weak": False, "nearest": nearest}


# A date in the web address (WordPress-style /2024/05/08/ or /2024/05/, or a
# 2024-05-08 in the page name) tells us when the article was really published.
# GDELT's own date is when it saw the page, which can be much later for old
# pages that are updated, linked or re-shared.
_URL_DATE_PATH = re.compile(r"/((?:19|20)\d\d)/(0?[1-9]|1[0-2])(?:/(0?[1-9]|[12]\d|3[01]))?(?=/|$)")
_URL_DATE_SLUG = re.compile(r"(?<!\d)((?:19|20)\d\d)[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12]\d|3[01])(?!\d)")


def url_date(url):
    """The publication date written in a web address, or None. When only the
    month is given, the 28th is used (so a doubtful case is never judged older
    than it is)."""
    try:
        path = urlparse(url).path
    except Exception:
        return None
    hit = _URL_DATE_PATH.search(path) or _URL_DATE_SLUG.search(path)
    if not hit:
        return None
    y, mo, d = int(hit.group(1)), int(hit.group(2)), int(hit.group(3) or 28)
    try:
        return datetime(y, mo, d)
    except ValueError:
        return None


def url_is_old(url, seen_iso):
    """If the web address carries a date more than URL_MAX_AGE_DAYS before the
    date GDELT gives (an ISO string such as 2026-10-29T14:02:00), return that
    date as text, otherwise None."""
    ud = url_date(url)
    if ud is None:
        return None
    try:
        seen = datetime.strptime((seen_iso or "")[:10], "%Y-%m-%d")
    except ValueError:
        return None
    if (seen - ud).days > URL_MAX_AGE_DAYS:
        return ud.strftime("%Y-%m-%d")
    return None


def skip_reason(url, title, m):
    """Why a record should not be stored at all, or None."""
    try:
        parsed = urlparse(url)
    except Exception:
        return "bad url"
    domain = parsed.netloc.lower()
    domain = domain[4:] if domain.startswith("www.") else domain
    if any(domain == d or domain.endswith("." + d) for d in m.skip_domains):
        return "skipped domain"
    if any(rx.search(url) for rx in m.skip_url_res):
        return "skipped web address (entertainment section)"
    if parsed.path in ("", "/"):
        return "home page"
    t = (title or "").strip()
    if not t:
        return "no title"
    if len(t.split()) < m.min_title_words and len(t) < m.min_title_chars:
        return "title too short"
    if any(rx.search(t) for rx in m.skip_title_res):
        return "generic title"
    return None


# ------------------------------------------------------------ data upgrade

def ensure_schema(m):
    """Upgrade an older data/hits.csv in place: new header, padded rows, and
    headline-based country/region (and, for the very oldest rows, scope
    tags). Does nothing if the header is already current. Refuses to touch
    an unrecognised header rather than risk misaligning columns."""
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
    has_scope = "scope_tags" in header
    migrated = [FIELDNAMES]
    regions = Counter()
    for row in rows[1:]:
        if len(row) > len(header):
            raise SystemExit("ERROR: a row in data/hits.csv has more columns than its header; stopping.")
        row = row + [""] * (len(FIELDNAMES) - len(row))
        title = row[idx["title"]]
        country, region = locate(title, None,
