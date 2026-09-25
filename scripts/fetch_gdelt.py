#!/usr/bin/env python3
"""
Fetch GDELT DOC 2.0 API hits for each repression category defined in
config/categories.yaml, deduplicate against the existing dataset, and
append new rows to data/hits.csv.

Designed to run daily via GitHub Actions (see
.github/workflows/gdelt-monitor.yml) but works the same run locally:

    python scripts/fetch_gdelt.py
"""
import csv
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
REQUEST_DELAY_SECONDS = 20  # was 12
MAX_RETRIES = 4              # was 3
RETRY_BACKOFF_SECONDS = 30   # was 20

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SAIH-student-activism-monitor/1.0)"}


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["categories"], set(cfg.get("watchlist_countries", []))


def load_existing_urls():
    if not DATA_PATH.exists():
        return set()
    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        return {row["url"] for row in csv.DictReader(f)}


import random  # add this to the imports at the top of the file

def fetch_category(category):
    """Fetch one category's articles. Never raises — returns [] on any
    unrecoverable failure so one bad category can't take down the run."""
    params = {
        "query": category["query"],
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
                wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 10)
                print(f"  ! rate-limited (429) — waiting {wait:.0f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            try:
                payload = resp.json()
            except ValueError:
                print(f"  ! non-JSON response for '{category['id']}' — skipping")
                return []

            return payload.get("articles", [])

        except requests.exceptions.RequestException as e:
            print(f"  ! request failed for '{category['id']}': {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS)

    print(f"  ! giving up on '{category['id']}' after {MAX_RETRIES} attempts")
    return []


def match_watchlist_country(article, watchlist):
    # GDELT's sourcecountry is a free-text country name (not ISO), so this
    # is a simple exact-match heuristic. Refine as needed once you see the
    # actual values GDELT returns for your regions of interest.
    source_country = (article.get("sourcecountry") or "").strip()
    return source_country if source_country in watchlist else ""


def main():
    categories, watchlist = load_config()
    existing_urls = load_existing_urls()
    new_rows = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for i, category in enumerate(categories):
        print(f"[{i + 1}/{len(categories)}] Fetching: {category['label']}")
        articles = fetch_category(category)
        print(f"  -> {len(articles)} articles returned")

        for a in articles:
            url = a.get("url", "")
            if not url or url in existing_urls:
                continue
            existing_urls.add(url)
            new_rows.append(
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

        if i < len(categories) - 1:
            time.sleep(REQUEST_DELAY_SECONDS)

    if not new_rows:
        print("No new articles found across any category.")
        return

    file_exists = DATA_PATH.exists()
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

    print(f"Appended {len(new_rows)} new rows to {DATA_PATH}")


if __name__ == "__main__":
    main()
