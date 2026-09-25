#!/usr/bin/env python3
"""
Fetch GDELT DOC 2.0 API hits using a small number of broad, grouped
queries (see config/categories.yaml), locally re-tag each result with its
specific category by matching the headline against that category's own
terms, deduplicate against the existing dataset, and append new rows to
data/hits.csv.

Retry policy is deliberately "fail fast": GDELT appears to rate-limit
GitHub Actions' shared IP ranges fairly persistently, and long exponential
backoffs just make every run take 40+ minutes without reliably getting
through. Since each run's 2-day search window overlaps the previous run's,
a category that fails today gets caught by tomorrow's run instead — so a
quick couple of retries followed by moving on is the better trade-off.

Progress is saved to data/hits.csv after EACH group, not just once at the
end, so a cancelled or timed-out run doesn't lose whatever did succeed.

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
MAX_RECORDS = 250
REQUEST_DELAY_SECONDS = 20
MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = 15

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
    categories_by_id = {c["id"]: c for c in cfg["categories"]}
    watchlist = cfg.get("watchlist_countries", [])

    groups = []
    for group in cfg["query_groups"]:
        cats = [categories_by_id[cid] for cid in group["category_ids"]]
        all_terms = [t for c in cats for t in c["terms"]]
        term_clause = " OR ".join(quote_if_needed(t) for t in all_terms)
        query = f"{identity_terms} AND ({term_clause})"
        groups.append({"id": group["id"], "query": query, "categories": cats})

    return groups, categories_by_id, watchlist


def load_existing_urls():
    if not DATA_PATH.exists():
        return set()
    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        return {row["url"] for row in csv.DictReader(f)}


def fetch_query(query_id, query):
    """Fetch one query's articles. Never raises — returns [] on any
    unrecoverable failure so one bad query can't take down the run.
    Deliberately fails fast (2 short retries) rather than fighting a
    possibly-persistent block for many minutes — see module docstring."""
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
                wait = RETRY_BACKOFF_SECONDS * attempt + random.uniform(0, 5)
                print(f"  ! rate-limited (429) — waiting {wait:.0f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            try:
                payload = resp.json()
                return payload.get("articles", [])
            except ValueError:
                snippet = resp.text[:150].replace("\n", " ")
                wait = RETRY_BACKOFF_SECONDS * attempt + random.uniform(0, 5)
                print(f"  ! non-JSON response for '{query_id}' ({snippet!r}) — retrying in {wait:.0f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

        except requests.exceptions.RequestException as e:
            print(f"  ! request failed for '{query_id}': {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS)

    print(f"  ! giving up on '{query_id}' after {MAX_RETRIES} attempts (will retry via tomorrow's overlapping window)")
    return []


def match_category(article, categories):
    title = (article.get("title") or "").lower()
    for category in categories:
        for term in category["terms"]:
            if term.lower() in title:
                return category
    return categories[0]


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
    groups, categories_by_id, watchlist = load_config()
    existing_urls = load_existing_urls()
    fetched_at = datetime.now(timezone.utc).isoformat()
    total_new = 0

    for i, group in enumerate(groups):
        print(f"[{i + 1}/{len(groups)}] Fetching group: {group['id']}")
        articles = fetch_query(group["id"], group["query"])
        print(f"  -> {len(articles)} articles returned")

        group_rows = []
        for a in articles:
            url = a.get("url", "")
            if not url or url in existing_urls:
                continue
            existing_urls.add(url)
            category = match_category(a, group["categories"])
            group_rows.append(
                {
                    "fetched_at": fetched_at,
                    "category_id": category["id"],
                    "category_label": category["label"],
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

        # Save this group's results immediately — a cancelled/timed-out run
        # still keeps whatever succeeded before that point.
        append_rows(group_rows)
        total_new += len(group_rows)
        if group_rows:
            print(f"  Saved {len(group_rows)} new row(s) from this group")

        if i < len(groups) - 1:
            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"Done. {total_new} new row(s) added this run.")


if __name__ == "__main__":
    main()
