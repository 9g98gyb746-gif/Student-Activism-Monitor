# Student Activism Monitor (GDELT early-warning feed)

A small, free, automated pipeline that scans world news via the [GDELT
Project](https://www.gdeltproject.org/) DOC 2.0 API for reports of repression
and attacks against student activists, tags each hit by category, and
publishes a browsable dashboard via GitHub Pages.

This is designed as an **early-signal tool**, not a source of truth. It is
meant to surface leads for staff to verify and, ideally, triangulate against
more rigorous sources (see [Limitations](#limitations-and-triangulation)
below) — not to be cited directly in reporting or advocacy.

## How it works

```
.github/workflows/gdelt-monitor.yml   → runs daily
        │
        ▼
scripts/fetch_gdelt.py                → queries GDELT per category, appends new
                                         rows to data/hits.csv (deduplicated by URL)
        │
        ▼
scripts/classify_relevance.py         → OPTIONAL: if ANTHROPIC_API_KEY is set,
                                         asks Claude to triage each new hit as
                                         relevant/irrelevant with a short reason
        │
        ▼
scripts/build_site_data.py            → writes docs/data.json for the dashboard
        │
        ▼
docs/index.html (GitHub Pages)        → filterable table of hits
        │
        ▼
git commit + push                     → data/hits.csv and docs/ are versioned,
                                         so you have a running history
```

## Categories

The search categories in `config/categories.yaml` are built around the forms
of repression documented in SAIH's own *Activism Under Attack* reports
(2023, 2024) — physical violence, arrest/detention, legal harassment
("lawfare"), institutional/academic sanctions, surveillance and
intimidation, delegitimization/smear campaigns, and visa or immigration
retaliation.

Two categories from the report — **co-option** and **factionalization** —
are deliberately left out. They describe subtle, relational tactics (e.g.
a university co-opting a movement's leadership, or state actors fostering
splits inside a student group) that essentially never appear as
keyword-matchable phrases in news text. A keyword feed cannot see these;
they're a good candidate for a note-taking / staff-input channel instead of
an automated one.

Edit `config/categories.yaml` to add, remove, or refine categories — this is
where most of your "am I drowning in noise" tuning will happen. Each
category is a self-contained GDELT boolean query, and you can add `NOT
(...)` exclusion terms to cut out recurring false-positive topics (sports
"clashes", movie "protests", etc.) once you see what noise your queries
attract.

`watchlist_countries` in the same file lists SAIH's current partner
countries/regions (Afghanistan, Bolivia, Colombia, Myanmar, Norway,
Palestine, South Africa, Zambia, Zimbabwe) purely so the dashboard can
highlight hits from those places — it does **not** restrict the search.
Repression of student activists elsewhere is still worth catching; the
watchlist is a highlighting aid, not a filter.

## Setup

1. **Create the repo.** Push this folder to a new GitHub repository (public
   or private — GitHub Pages works on both, though a private repo needs
   GitHub Pages enabled for private repos on your plan, or you can make the
   `docs/` output public separately).
2. **Enable GitHub Actions.** Nothing to configure — the workflow in
   `.github/workflows/gdelt-monitor.yml` runs on its own schedule (daily at
   06:00 UTC by default) and can also be triggered manually from the
   Actions tab (`workflow_dispatch`).
3. **Give the workflow write access.** In *Settings → Actions → General →
   Workflow permissions*, select "Read and write permissions" so the
   workflow can commit new data back to the repo.
4. **Enable GitHub Pages.** In *Settings → Pages*, set the source to the
   `docs/` folder on your default branch. Your dashboard will appear at
   `https://<org>.github.io/<repo>/`.
5. **(Optional) Add Claude-based triage.** If you want the relevance-triage
   step, add a repository secret named `ANTHROPIC_API_KEY` (*Settings →
   Secrets and variables → Actions*). Without it, the pipeline still runs
   fine — every hit just shows up untriaged, and a human reviews it
   directly instead.
6. **First run.** Trigger the workflow manually once (Actions tab → *GDELT
   student activism monitor* → *Run workflow*) rather than waiting for the
   schedule, so you can see data land in `data/hits.csv` and check the
   dashboard.

## Running locally

```bash
pip install -r requirements.txt
python scripts/fetch_gdelt.py
python scripts/classify_relevance.py   # no-op unless ANTHROPIC_API_KEY is set
python scripts/build_site_data.py
open docs/index.html                   # or just open the file in a browser
```

## Limitations and triangulation

- **GDELT indexes media coverage, not verified incidents.** A hit means a
  news article used matching language — it does not mean SAIH's definition
  of repression was met, or that the event is real. Every flagged hit needs
  human eyes before it goes anywhere near a report.
- **Language and source bias.** GDELT's non-English coverage and machine
  translation quality vary a lot by region; under-covered languages will
  under-report real events, and some regions (including several of SAIH's
  partner countries) get thinner wire coverage than others.
- **Duplication and lag.** The same event often generates several near
  identical articles; `fetch_gdelt.py` dedupes by URL only, so re-reporting
  of one event across outlets will still show as multiple rows tagged with
  the same category.
- **This is one source among several worth combining:**
  - **[ACLED](https://acleddata.com/)** (Armed Conflict Location & Event
    Data) tags actor roles more precisely, including student/protester
    categories, and is built for structured event analysis rather than raw
    text search. It requires a free registered API key.
  - **[Scholars at Risk's Free to Think / Academic Freedom Monitoring
    Project](https://www.scholarsatrisk.org/)** already tracks attacks on
    higher-education communities specifically, with human verification —
    arguably the closest existing "gold standard" dataset to cross-check
    against.
  - **[Front Line Defenders](https://www.frontlinedefenders.org/)** tracks
    attacks on human rights defenders generally, some of them students.

  None of these are wired into this repo yet — the GDELT feed was the
  priority for fast, free, early signal. The scripts are structured so a
  second source can be added as its own fetch script that writes into the
  same `data/hits.csv` schema (with a `source` column), so cross-referencing
  becomes a filter in the dashboard rather than a rebuild. That's a natural
  next step once the GDELT feed is tuned and staff have a sense of what
  "relevant" looks like in practice.
