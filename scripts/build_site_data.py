#!/usr/bin/env python3
"""Convert data/hits.csv into docs/data.json for the GitHub Pages dashboard."""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "hits.csv"
OUT_PATH = ROOT / "docs" / "data.json"


def main():
    if DATA_PATH.exists():
        with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    else:
        rows = []

    rows.sort(key=lambda r: r.get("seendate", ""), reverse=True)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(rows)} row(s) to {OUT_PATH}")


if __name__ == "__main__":
    main()
