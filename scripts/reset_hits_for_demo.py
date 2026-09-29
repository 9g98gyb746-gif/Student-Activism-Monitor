#!/usr/bin/env python3
"""
ONE-TIME: archive the current data/hits.csv (which contains the old,
noisy DOC-API-era data collected before the switch to the ngrams
pipeline) to a timestamped backup file, then reset data/hits.csv to an
empty file with just the header — ready for the new pipeline to populate
cleanly.

Nothing is deleted: the archived copy stays in the repo (and everything
is in git history regardless), this just clears what the live dashboard
shows going forward.
"""
import csv
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "hits.csv"

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


def main():
    if not DATA_PATH.exists():
        print("No hits.csv found — nothing to archive.")
        return

    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        row_count = sum(1 for _ in csv.DictReader(f))

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    archive_path = DATA_PATH.parent / f"hits_archive_pre_ngrams_{stamp}.csv"
    shutil.copy(DATA_PATH, archive_path)
    print(f"Archived {row_count} row(s) to {archive_path}")

    with open(DATA_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
    print("data/hits.csv reset to an empty (header-only) file.")


if __name__ == "__main__":
    main()
