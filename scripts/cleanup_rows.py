#!/usr/bin/env python3
"""
One-off cleanup of data/hits.csv. Removes rows that should never have been
recorded. Removed rows are NOT lost: they are saved to
data/removed_rows_<date>.csv so any of them can be put back.

Set DRY_RUN=1 (the default) to only list what would be removed.
Set DRY_RUN=0 to actually rewrite data/hits.csv.

Edit the rules below to change what is removed.
"""
import csv
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

CSV_PATH = "data/hits.csv"
DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"

# 1. Rows matched by search terms that were later removed from categories.yaml.
OLD_RULE_TERMS = {"lawsuit", "shot"}

# 2. Whole domains that only republish old stories (2024 campus-protest
#    stories re-crawled as if new).
BAD_DOMAINS = ["digbycourier.ca"]

# 3. Domains that are never news stories about this topic.
JUNK_DOMAINS = ["lolwot.com"]

# 4. URL patterns for listing / author / people pages.
BAD_URL_PATTERNS = [r"/author/", r"/people/", r"/authors/", r"/tag/", r"/topic/"]

# 5. Digest / briefing pages that mention many unrelated things.
BAD_TITLE_PATTERNS = [r"^morning briefing", r"intelligence brief", r"net worth$"]


def reason(row):
    term = (row.get("matched_term") or "").strip().lower()
    domain = (row.get("domain") or "").lower()
    url = row.get("url") or ""
    title = (row.get("title") or "").strip()
    if term in OLD_RULE_TERMS:
        return f"old search term '{term}'"
    if any(domain.endswith(d) for d in BAD_DOMAINS):
        return "re-crawled old stories domain"
    if any(domain.endswith(d) for d in JUNK_DOMAINS):
        return "junk domain"
    if any(re.search(p, url) for p in BAD_URL_PATTERNS):
        return "listing/author page"
    if any(re.search(p, title, re.I) for p in BAD_TITLE_PATTERNS):
        return "digest or junk title"
    return None


def main():
    if not os.path.exists(CSV_PATH):
        sys.exit(f"{CSV_PATH} not found")
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    if not fieldnames or "matched_term" not in fieldnames or "url" not in fieldnames:
        sys.exit("Unrecognised header in hits.csv; refusing to touch it.")

    keep, removed = [], []
    for row in rows:
        why = reason(row)
        (removed if why else keep).append((row, why))

    print(f"Rows in file: {len(rows)}")
    print(f"Would remove: {len(removed)}   Would keep: {len(keep)}")
    counts = {}
    for _, why in removed:
        counts[why] = counts.get(why, 0) + 1
    for why, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {n:4d}  {why}")
    print("\nRows to remove:")
    for row, why in removed:
        print(f"  [{why}] {row.get('domain','')} | {(row.get('title') or '')[:80]}")

    if DRY_RUN:
        print("\nDRY RUN: nothing was changed.")
        return

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    removed_path = f"data/removed_rows_{stamp}.csv"
    with open(removed_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames + ["removed_reason"])
        w.writeheader()
        for row, why in removed:
            w.writerow({**row, "removed_reason": why})

    fd, tmp = tempfile.mkstemp(dir="data", suffix=".csv")
    with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row, _ in keep:
            w.writerow(row)
    os.replace(tmp, CSV_PATH)
    print(f"\nRemoved {len(removed)} rows (saved to {removed_path}); kept {len(keep)}.")


if __name__ == "__main__":
    main()
