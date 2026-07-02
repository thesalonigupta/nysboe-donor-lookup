# NYSBOE Donor Lookup

A second-pass donor research tool: it takes the donors marked **CONSIDER** in the output of a companion FEC lookup script (`fec_donor_lookup.py`) and checks their giving history with the New York State Board of Elections (NYSBOE), which covers state-level races and committees not present in the FEC's federal database. NYSBOE records contain only name and address (no employer/occupation), so matching relies on address: the script pulls each donor's known addresses from your spreadsheet and from FEC records, drives a real (headless) browser through NYSBOE's public search site — NYSBOE has no public API — and classifies each state contribution record as street-confirmed, needs-manual-review (city-level match only), or a different person with the same name.

## Prerequisites

- Python 3.9+
- A free FEC API key from https://api.data.gov/signup/ (use a **separate** key from the one your FEC lookup script uses, so the two scripts don't compete for the same hourly rate limit)
- Two input files in the working directory (or passed via flags — see Usage):
  - `donors.csv` — your donor spreadsheet (the same file you gave the FEC script), with First, Last, City, State, Address, Employer, and Occupation columns
  - `fec_results_summary.csv` — the output summary from the companion FEC lookup script

## Setup

1. Install Python dependencies:

   ```bash
   pip3 install -r requirements.txt
   ```

2. Install the browser Playwright drives:

   ```bash
   playwright install chromium
   ```

3. Set your FEC API key as an environment variable. Either export it directly:

   ```bash
   export FEC_API_KEY=your_key_here
   ```

   or copy `.env.example` to `.env`, fill in your key, and load it into your shell:

   ```bash
   cp .env.example .env
   # edit .env, then:
   set -a; source .env; set +a
   ```

## Usage

Basic run (expects `donors.csv` and `fec_results_summary.csv` in the current directory):

```bash
python3 nysboe_donor_lookup.py
```

Point at differently named/located input files:

```bash
python3 nysboe_donor_lookup.py --donors my_file.csv --fec-summary my_summary.csv
```

Retry only the donors that errored (NYSBOE's site can be slow/flaky — searches occasionally time out). This re-runs ONLY the rows whose status starts with `ERROR` in your existing `nysboe_results_summary.csv`, updating them in place and leaving every successful donor untouched. Safe to run multiple times:

```bash
python3 nysboe_donor_lookup.py --retry-failed
```

`--retry-failed` can be combined with `--donors` if your spreadsheet isn't named `donors.csv`. Donors that fail again on retry are escalated to `⚠ MANUAL LOOKUP REQUIRED` and skipped on future retries — check those manually at the [NYSBOE contributions search](https://publicreporting.elections.ny.gov/Contributions/Contributions).

The browser runs invisibly in the background by default; no windows will open.

## Output

- `nysboe_results_summary.csv` — one row per CONSIDER donor with counts of confirmed / needs-review records and an overall NY state match status
- `nysboe_results/nysboe_Last_First.txt` — one file per donor with their full NY state giving history (street-confirmed records and any records flagged for manual review), written incrementally as each donor finishes

## Companion script

This tool is designed to work alongside a companion FEC lookup script (`fec_donor_lookup.py`), which does the first pass against federal records and produces the `fec_results_summary.csv` this script consumes. This script is fully independent of it — it never imports or modifies the FEC script; it only reads two of its files (`donors.csv` and `fec_results_summary.csv`).
