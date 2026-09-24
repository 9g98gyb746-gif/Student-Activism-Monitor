#!/usr/bin/env python3
"""
Optional triage pass: ask Claude to label each un-classified row in
data/hits.csv as relevant/irrelevant to student-activist repression, with a
short reason, so a human reviewer can sort signal from noise faster.

This step is entirely optional. If ANTHROPIC_API_KEY isn't set, the script
exits without changing anything — the rest of the pipeline (and the
dashboard) works fine on raw, untriaged GDELT hits.

Run locally:
    pip install anthropic
    ANTHROPIC_API_KEY=sk-ant-... python scripts/classify_relevance.py
"""
import csv
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "hits.csv"

MODEL = "claude-sonnet-5"
BATCH_SIZE = 20

PROMPT_INSTRUCTIONS = (
    "For each numbered news item below, decide whether it plausibly describes "
    "repression, an attack, or retaliation against a student activist, "
    "student movement, or student union (e.g. arrest, violence, expulsion, "
    "legal action, surveillance, smear campaign, visa retaliation) as opposed "
    "to unrelated campus or student news (sports, general crime, routine "
    "politics, entertainment, admissions, etc.).\n\n"
    'Respond ONLY with a JSON array, one object per item in the same order, '
    'each with keys "relevant" (true/false) and "reason" (under 12 words, '
    "explaining the call).\n\n"
)


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set — skipping relevance triage.")
        return
    if not DATA_PATH.exists():
        print("No data file yet — nothing to classify.")
        return

    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    with open(DATA_PATH, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("Data file is empty — nothing to classify.")
        return
    fieldnames = list(rows[0].keys())

    to_classify = [r for r in rows if not r.get("relevance")]
    print(f"{len(to_classify)} row(s) need triage")

    for start in range(0, len(to_classify), BATCH_SIZE):
        batch = to_classify[start : start + BATCH_SIZE]
        listing = "\n".join(
            f"{i + 1}. [{r['category_label']}] {r['title']} ({r['domain']})"
            for i, r in enumerate(batch)
        )
        resp = client.messages.create(
            model=MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": PROMPT_INSTRUCTIONS + listing}],
        )
        text = "".join(getattr(block, "text", "") for block in resp.content)
        try:
            labels = json.loads(text)
        except json.JSONDecodeError:
            print("  ! could not parse model response for this batch — skipping")
            continue

        for row, label in zip(batch, labels):
            row["relevance"] = "relevant" if label.get("relevant") else "irrelevant"
            row["relevance_reason"] = label.get("reason", "")

        time.sleep(1)

    with open(DATA_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("Done.")


if __name__ == "__main__":
    main()
