#!/usr/bin/env python3
"""
ONE-TIME repair for data/hits.csv: the file's header line never got updated
when matched_term (added first) and context_match (added later) were
introduced to fetch_gdelt.py, because the script only writes a header when
the file doesn't already exist. As a result, rows written by each version
have different actual column counts (13, 14, or 15) than the stale header
claims (still 13) — so every value after the point of insertion has been
silently mislabeled ever since.

This script knows the exact 3 historical row layouts that were actually
used and re-maps every row to the current, correct layout by column count,
regardless of what the stale header says. Run this ONCE, then use the
corrected fetch_gdelt.py going forward (which won't reintroduce this bug).
"""
import csv
from pathlib import Path

DATA_PATH = Path("data/hits.csv")

# The 3 layouts that were actually used historically, oldest to newest.
SCHEMA_V1 = [  # 13 columns — original script
    "fetched_at", "category_id", "category_label", "title", "url",
    "seendate", "domain", "language", "sourcecountry", "tone",
    "watchlist_country", "relevance", "relevance_reason",
]
SCHEMA_V2 = [  # 14 columns — after matched_term was added (inserted before title)
    "fetched_at", "category_id", "category_label", "matched_term", "title",
    "url", "seendate", "domain", "language", "sourcecountry", "tone",
    "watchlist_country", "relevance", "relevance_reason",
]
SCHEMA_V3 = [  # 15 columns — after context_match was also added
    "fetched_at", "category_id", "category_label", "matched_term", "title",
    "url", "seendate", "domain", "language", "sourcecountry", "tone",
    "watchlist_country", "context_match", "relevance", "relevance_reason",
]
SCHEMAS_BY_LENGTH = {13: SCHEMA_V1, 14: SCHEMA_V2, 15: SCHEMA_V3}

# The corrected, final layout going forward — same as SCHEMA_V1's original
# 13 fields in their original order (untouched), with the 2 newer fields
# appended at the very END instead of inserted in the middle. This matches
# the corrected fetch_gdelt.py's FIELDNAMES exactly.
FINAL_FIELDNAMES = [
    "fetched_at", "category_id", "category_label", "title", "url",
    "seendate", "domain", "language", "sourcecountry", "tone",
    "watchlist_country", "relevance", "relevance_reason",
    "matched_term", "context_match",
]


def main():
    if not DATA_PATH.exists():
        print("data/hits.csv doesn't exist — nothing to repair.")
        return

    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        raw_rows = list(csv.reader(f))

    if not raw_rows:
        print("data/hits.csv is empty — nothing to repair.")
        return

    data_rows = raw_rows[1:]  # skip the (stale) header line entirely
    print(f"Found {len(data_rows)} data row(s) to inspect.")

    repaired = []
    seen_urls = set()
    skipped_unknown = 0
    skipped_duplicate = 0

    for row in data_rows:
        schema = SCHEMAS_BY_LENGTH.get(len(row))
        if schema is None:
            skipped_unknown += 1
            print(f"  ! unrecognized column count ({len(row)}) — skipping a row: {row[:3]}")
            continue

        record = dict(zip(schema, row))
        url = record.get("url", "").strip()

        if not url or url in seen_urls:
            skipped_duplicate += 1
            continue
        seen_urls.add(url)

        repaired.append({field: record.get(field, "") for field in FINAL_FIELDNAMES})

    with open(DATA_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FINAL_FIELDNAMES)
        writer.writeheader()
        writer.writerows(repaired)

    print(f"\nRepaired {len(repaired)} row(s).")
    if skipped_duplicate:
        print(f"Skipped {skipped_duplicate} duplicate/blank-URL row(s).")
    if skipped_unknown:
        print(f"WARNING: skipped {skipped_unknown} row(s) with an unrecognized column count — please flag these for manual review.")


if __name__ == "__main__":
    main()
