#!/usr/bin/env python3
"""
Fetch GDELT DOC 2.0 API hits using a small number of broad, grouped
queries (see config/categories.yaml), locally re-tag each result with its
specific category by matching the headline against that category's own
terms, deduplicate against the existing dataset, and append new rows to
data/hits.csv.

Fewer, broader queries (instead of one call per category) meaningfully
reduce how often we get caught by GDELT's rate limiting on shared/cloud
IPs like GitHub Actions runners.

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
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 30

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SAIH-student-activism-monitor/1.0)"}


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
                wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 10)
                print(f"  ! rate-limited (429) — waiting {wait:.0f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            try:
                payload = resp.json()
            except ValueError:
                print(f"  ! non-JSON response for '{query_id}' — skipping")
                return []

            return payload.get("articles", [])

        except requests.exceptions.RequestException as e:
            print(f"  ! request failed for '{query_id}': {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS)

    print(f"  ! giving up on '{query_id}' after {MAX_RETRIES} attempts")
    return []


def match_category(article, categories):
    """Find which category in this group actually matched, by checking the
    headline against each category's own terms. Falls back to the first
    category in the group if nothing matches directly (shouldn't normally
    happen, since the query already required one of these terms)."""
    title = (article.get("title") or "").lower()
    for category in categories:
        for term in category["terms"]:
            if term.lower() in title:
                return category
    return categories[0]


def match_watchlist_country(article, watchlist):
    """Best-effort proxy for 'where did this happen': check whether a
    watchlist country's name or a known alias appears in the article's
    headline. GDELT's free API doesn't expose true event-location tagging,
    and a headline-only check will miss cases where the country is only
    named in the article body — so treat this as a helpful signal, not a
    reliable filter."""
    title = (article.get("title") or "").lower()
    for country in watchlist:
        names_to_check = [country["name"]] + country.get("aliases", [])
        for name in names_to_check:
            if name.lower() in title:
                return country["name"]
    return ""


def main():
    groups, categories_by_id, watchlist = load_config()
    existing_urls = load_existing_urls()
    new_rows = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for i, group in enumerate(groups):
        print(f"[{i + 1}/{len(groups)}] Fetching group: {group['id']}")
        articles = fetch_query(group["id"], group["query"])
        print(f"  -> {len(articles)} articles returned")

        for a in articles:
            url = a.get("url", "")
            if not url or url in existing_urls:
                continue
            existing_urls.add(url)
            category = match_category(a, group["categories"])
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

        if i < len(groups) - 1:
            time.sleep(REQUEST_DELAY_SECONDS)

    if not new_rows:
        print("No new articles found across any group.")
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
