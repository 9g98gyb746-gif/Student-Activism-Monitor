#!/usr/bin/env python3
"""
One-off cleanup of data/hits.csv. Removes rows that the monitor would not
record today. Removed rows are NOT lost: they are saved to
data/removed_rows_<date>.csv, with the reason, so any of them can be put back.

Rules (the lists in config/categories.yaml under "skip" and "second_signal"
are shared with the monitor, so there is one place to edit them):
  1. rows found through search terms that were later removed
  2. skipped domains, listing/author pages, home pages, generic or very
     short titles
  3. rows whose only repression term is an ambiguous one ("suspended",
     "charged", "killed" ...) and whose headline has no protest/police word.
     Only the headline can be checked for old rows, so this rule is the most
     likely to remove something real. Read that part of the dry-run list.

Set DRY_RUN=1 (the default) to only list what would be removed.
Set DRY_RUN=0 to actually rewrite data/hits.csv.
"""
import csv
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "data" / "hits.csv"
CONFIG_PATH = ROOT / "config" / "categories.yaml"
DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"

# 1. Rows matched by search terms that were later removed from categories.yaml.
OLD_RULE_TERMS = {"lawsuit", "shot"}

# 2. URL patterns for listing / author / people pages.
BAD_URL_PATTERNS = [r"/author/", r"/people/", r"/authors/", r"/tag/", r"/topic/"]

# Rows to KEEP even if a rule above would remove them (real stories found
# through an old search term). Matched against the title.
KEEP_TITLE_PATTERNS = [r"rhodes university", r"saikat", r"mahmoud khalil"]


def load_rules():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    skip = cfg.get("skip") or {}
    ss = cfg.get("second_signal") or {}
    context = [w.strip().lower() for w in ss.get("context_words", [])]
    ctx_alt = "|".join(r"[\s\-]+".join(re.escape(x) for x in re.split(r"[\s\-]+", w)) for w in context)
    return {
        "domains": [d.strip().lower() for d in skip.get("domains", [])],
        "title_res": [re.compile(p, re.I) for p in skip.get("title_patterns", [])],
        "min_words": int(skip.get("min_title_words", 0)),
        "min_chars": int(skip.get("min_title_chars", 0)),
        "ambiguous": {str(t).strip().lower() for t in ss.get("terms", [])},
        "context_re": re.compile(r"(?<!\w)(?:" + ctx_alt + ")", re.I) if ctx_alt else None,
    }


def reason(row, rules):
    term = (row.get("matched_term") or "").strip().lower()
    domain = (row.get("domain") or "").lower()
    url = row.get("url") or ""
    title = (row.get("title") or "").strip()
    if any(re.search(p, title, re.I) for p in KEEP_TITLE_PATTERNS):
        return None
    if term in OLD_RULE_TERMS:
        return f"old search term '{term}'"
    if any(domain == d or domain.endswith("." + d) for d in rules["domains"]):
        return "skipped domain"
    if any(re.search(p, url) for p in BAD_URL_PATTERNS):
        return "listing/author page"
    try:
        if urlparse(url).path in ("", "/"):
            return "home page"
    except Exception:
        pass
    if not title:
        return "no title"
    if len(title.split()) < rules["min_words"] and len(title) < rules["min_chars"]:
        return "title too short"
    if any(rx.search(title) for rx in rules["title_res"]):
        return "generic title"
    if term in rules["ambiguous"] and rules["context_re"] and not rules["context_re"].search(title):
        return f"ambiguous term '{term}', no protest/police word in headline"
    return None


def main():
    if not CSV_PATH.exists():
        sys.exit(f"{CSV_PATH} not found")
    rules = load_rules()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    if not fieldnames or "matched_term" not in fieldnames or "url" not in fieldnames:
        sys.exit("Unrecognised header in hits.csv; refusing to touch it.")

    keep, removed = [], []
    for row in rows:
        why = reason(row, rules)
        (removed if why else keep).append((row, why))

    print(f"Rows in file: {len(rows)}")
    print(f"Would remove: {len(removed)}   Would keep: {len(keep)}")
    counts = {}
    for _, why in removed:
        key = re.sub(r"'[^']*'", "'...'", why)
        counts[key] = counts.get(key, 0) + 1
    for why, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {n:4d}  {why}")
    print("\nRows to remove:")
    for row, why in sorted(removed, key=lambda x: x[1]):
        print(f"  [{why}] {row.get('domain','')} | {(row.get('title') or '')[:80]}")

    if DRY_RUN:
        print("\nDRY RUN: nothing was changed.")
        return

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    removed_path = ROOT / "data" / f"removed_rows_{stamp}.csv"
    with open(removed_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames + ["removed_reason"])
        w.writeheader()
        for row, why in removed:
            w.writerow({**row, "removed_reason": why})

    fd, tmp = tempfile.mkstemp(dir=CSV_PATH.parent, suffix=".csv")
    with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row, _ in keep:
            w.writerow(row)
    os.replace(tmp, CSV_PATH)
    print(f"\nRemoved {len(removed)} rows (saved to {removed_path.name}); kept {len(keep)}.")


if __name__ == "__main__":
    main()
