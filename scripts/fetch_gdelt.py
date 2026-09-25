#!/usr/bin/env python3
"""
Fetch GDELT DOC 2.0 API hits using MANY SIMPLE queries — one per
individual keyword — rather than fewer complex boolean queries.

Why: testing showed GDELT accepts a simple single-phrase query instantly
but blocks a compact 12-clause OR'd boolean query immediately, regardless
of request pacing. This matches other developers' reports that GDELT's
API doesn't handle compound boolean expressions reliably. So each query
here is just ("student" OR "students") AND <one term> — at most 3 total
clauses — traded off against needing ~47 requests instead of ~7.

Retries are deliberately minimal: GDELT's own documentation asks for at
most one request per 5 seconds, and other users have observed that
exceeding it can trigger a much longer (~15 minute) cooldown, not just a
brief one. So a term that fails just gets skipped and picked up by
tomorrow's overlapping 2-day window instead of retried aggressively today.

Designed to run daily via GitHub Actions (see
.github/workflows/gdelt-monitor.yml) but works the same run locally:

    python scripts/fetch_gdelt.py
"""
import csv
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "categories.yaml"
DATA_PATH = ROOT / "data" / "hits.csv"
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

FIELDNAMES = [
    "fetched_at",
    "category_id",
    "category_label",
    "matched_term",
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
]

TIMESPAN = "2d"
MAX_RECORDS = 100
REQUEST_DELAY_SECONDS = 9  # GDELT asks for 1 per 5s; this leaves margin
MAX_RETRIES = 1
RETRY_BACKOFF_SECONDS = 12

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def quote_if_needed(term):
    return f'"{term}"' if " " in term else term


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    identity_terms = cfg["identity_terms"]
    watchlist = cfg.get("watchlist_countries", [])

    # Flatten to one entry per individual (category, term) pair.
    tasks = []
    for category in cfg["categories"]:
        for term in category["terms"]:
            query = f"{identity_terms} AND {quote_if_needed(term)}"
            tasks.append(
                {
                    "category_id": category["id"],
                    "category_label": category["label"],
                    "term": term,
                    "query": query,
                }
            )

    return tasks, watchlist


def load_existing_urls():
    if not DATA_PATH.exists():
        return set()
    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        return {row["url"] for row in csv.DictReader(f)}


def fetch_query(label, query):
    """Fetch one simple query's articles. Never raises — returns [] on any
    unrecoverable failure so one bad query can't take down the run."""
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": MAX_RECORDS,
        "format": "json",
        "timespan": TIMESPAN,
        "sort": "datedesc",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(GDELT_URL, params=params, headers=HEADERS, timeout=60)

            if resp.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                print(f"    ! rate-limited (429) — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            try:
                payload = resp.json()
                return payload.get("articles", [])
            except ValueError:
                snippet = resp.text[:120].replace("\n", " ")
                print(f"    ! non-JSON response for '{label}' ({snippet!r}) — skipping")
                return []

        except requests.exceptions.RequestException as e:
            print(f"    ! request failed for '{label}': {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS)

    print(f"    ! giving up on '{label}' (will retry via tomorrow's overlapping window)")
    return []


def match_watchlist_country(article, watchlist):
    title = (article.get("title") or "").lower()
    for country in watchlist:
        names_to_check = [country["name"]] + country.get("aliases", [])
        for name in names_to_check:
            if name.lower() in title:
                return country["name"]
    return ""


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
    tasks, watchlist = load_config()
    existing_urls = load_existing_urls()
    fetched_at = datetime.now(timezone.utc).isoformat()
    total_new = 0
    total_found = 0

    for i, task in enumerate(tasks):
        label = f"{task['category_id']}:{task['term']}"
        print(f"[{i + 1}/{len(tasks)}] {label}")
        articles = fetch_query(label, task["query"])
        total_found += len(articles)

        rows = []
        for a in articles:
            url = a.get("url", "")
            if not url or url in existing_urls:
                continue
            existing_urls.add(url)
            rows.append(
                {
                    "fetched_at": fetched_at,
                    "category_id": task["category_id"],
                    "category_label": task["category_label"],
                    "matched_term": task["term"],
                    "title": a.get("title", ""),
                    "url": url,
                    "seendate": a.get("seendate", ""),
                    "domain": a.get("domain", ""),
                    "language": a.get("language", ""),
                    "sourcecountry": a.get("sourcecountry", ""),
                    "tone": a.get("tone", ""),
                    "watchlist_country": match_watchlist_country(a, watchlist),
                    "relevance": "",
                    "relevance_reason": "",
                }
            )

        append_rows(rows)  # save after every single term, not just at the end
        total_new += len(rows)
        if rows:
            print(f"    -> {len(articles)} found, {len(rows)} new, saved")
        else:
            print(f"    -> {len(articles)} found, 0 new")

        if i < len(tasks) - 1:
            time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 3))

    print(f"\nDone. {total_found} article(s) matched across all terms; {total_new} new row(s) added.")


if __name__ == "__main__":
    main()
