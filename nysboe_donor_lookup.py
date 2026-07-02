"""
NYSBOE Donor Lookup Script — Batch Mode
=========================================
Second-pass tool: takes the "CONSIDER" donors from fec_donor_lookup.py's
output and checks their giving history with the New York State Board of
Elections (NYSBOE), which only covers state-level races/committees and is
NOT in the FEC's federal database.

This script is fully independent from fec_donor_lookup.py — it does not
import or modify it. It only reads two of its output/input files:
    1. donors.csv               (same input file you gave the FEC script —
                                  used here to get each donor's mailing address)
    2. fec_results_summary.csv  (FEC script's output — used here to find
                                  which donors were marked CONSIDER)

NYSBOE records do NOT include employer/occupation, only name + address.
So unlike the FEC script, there's no employer signal to confirm a match —
matching here relies on address alone (street, then city as a fallback).

SETUP (do this once):
    1. Get a free FEC API key at: https://api.data.gov/signup/
       (Yes, FEC — this script also queries the FEC API, but only to pull
       additional known addresses for each donor, not their giving history.
       Use a SEPARATE key from your FEC script's key so the two scripts
       don't compete for the same hourly rate limit.)

    2. Install dependencies (same as the FEC script, plus Playwright for
       the NYSBOE search — NYSBOE has no public API, so this drives a
       real (invisible/background) browser to search their site — and
       playwright-stealth, which patches a wide set of headless-browser
       fingerprints NYSBOE's site otherwise detects and blocks on):
       pip3 install requests pandas openpyxl playwright playwright-stealth
       playwright install chromium

    3. Set your (separate) FEC API key as the FEC_API_KEY environment
       variable (see .env.example):
       export FEC_API_KEY=your_key_here

    4. Make sure these two files are in the same folder as this script:
         donors.csv                  (your original donor spreadsheet)
         fec_results_summary.csv     (output from fec_donor_lookup.py)

    5. Run the script:
       python3 nysboe_donor_lookup.py

       The browser Playwright uses runs invisibly in the background by
       default (headless=True in run_nysboe_searches) — you won't see a
       window open. Set headless=False there if you ever want to watch
       it work for debugging.

    6. If some donors come back with an "ERROR" status (NYSBOE's site
       can be slow/flaky, especially on long runs — searches
       occasionally time out even after the built-in retry), you don't
       need to re-run the whole batch. Just run:
       python3 nysboe_donor_lookup.py --retry-failed

       This reads your existing nysboe_results_summary.csv, finds every
       donor whose status starts with "ERROR", and re-runs ONLY those —
       updating their row in the summary and their individual .txt file
       in place, leaving every already-successful donor untouched. Safe
       to run multiple times; it always retries whatever still says
       ERROR.

OUTPUT:
    - nysboe_results_summary.csv         → one row per CONSIDER donor with NY state match call
    - nysboe_results/nysboe_Last_First.txt → full NY state giving history per donor
"""

import sys
import re
import argparse
import requests

import pandas as pd
import os
import time
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import ssl
import certifi
import socket

# Fix for Mac SSL certificate verification error
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

# Global socket timeout — catches hangs that requests timeout can miss
socket.setdefaulttimeout(30)

# -------------------------------------------------------
# FEC API KEY — read from the FEC_API_KEY environment variable.
# Use a SEPARATE key from the one in fec_donor_lookup.py —
# this keeps the two scripts from competing for the same
# hourly rate limit if you run them back-to-back.
# Get one free at: https://api.data.gov/signup/
# See .env.example for setup.
# -------------------------------------------------------
FEC_API_KEY = os.environ.get("FEC_API_KEY", "")

# -------------------------------------------------------
# INPUT FILES
# Both must be in the same folder as this script (or give
# full paths). These are READ-ONLY inputs — this script
# never modifies either file.
# -------------------------------------------------------
DONORS_FILE       = "donors.csv"                # same file the FEC script used
FEC_SUMMARY_FILE  = "fec_results_summary.csv"    # output from fec_donor_lookup.py

# -------------------------------------------------------
# OUTPUT
# -------------------------------------------------------
OUTPUT_FOLDER = "nysboe_results"
SUMMARY_PATH  = "nysboe_results_summary.csv"

MIN_DATE = "2016-01-01"               # FEC API format: YYYY-MM-DD
MIN_DATE_NYSBOE = "01/01/2016"        # NYSBOE form format: MM/DD/YYYY — same 1/1/2016 floor as FEC
FEC_BASE_URL = "https://api.open.fec.gov/v1/schedules/schedule_a/"

# -------------------------------------------------------
# PARALLEL WORKERS — FEC address lookup phase
# Used by process_donors() to fetch each donor's FEC-known addresses
# concurrently rather than one at a time. The FEC tool's own
# fetch_contributions() runs at MAX_WORKERS=15; this phase pulls a much
# smaller, simpler request per donor (just addresses, no employer/
# occupation/ActBlue logic), and uses a separate API key from the FEC
# tool, so 10 was chosen as a reasonable middle ground — close to the
# FEC tool's own pace without assuming it can safely match it exactly.
# -------------------------------------------------------
FEC_ADDRESS_LOOKUP_WORKERS = 10

# -------------------------------------------------------
# COLUMN NAME MAPPING — for donors.csv
# Update these if your spreadsheet uses different headers.
# -------------------------------------------------------
COL_FIRST      = "First"
COL_LAST       = "Last"
COL_CITY       = "City"
COL_STATE      = "State"
COL_ADDRESS    = "Address"
COL_EMPLOYER   = "Employer"
COL_OCCUPATION = "Occupation"


def load_donor_addresses(filepath):
    """
    Loads donors.csv and returns a dict keyed by (last, first, city, state)
    -> {"address": ..., "employer": ...}. This is the SAME identity key the
    FEC script uses, so joins against fec_results_summary.csv line up
    cleanly even when names repeat.

    Employer is carried alongside address (rather than looked up
    separately) because it's needed downstream to confirm which FEC
    addresses actually belong to THIS donor, the same way
    fec_donor_lookup.py uses employer to confirm identity — see
    get_fec_known_addresses().
    """
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(filepath)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file type: {ext}. Use .csv or .xlsx")

    df = df.loc[:, ~df.columns.str.contains(r'^Unnamed', na=False)]
    df.columns = df.columns.str.strip()
    col_map = {c.lower(): c for c in df.columns}

    def get_col(name):
        return col_map.get(name.lower())

    first_col      = get_col(COL_FIRST)
    last_col       = get_col(COL_LAST)
    city_col       = get_col(COL_CITY)
    state_col      = get_col(COL_STATE)
    address_col    = get_col(COL_ADDRESS)
    employer_col   = get_col(COL_EMPLOYER)
    occupation_col = get_col(COL_OCCUPATION)

    if not first_col or not last_col:
        raise ValueError(
            f"Could not find First / Last columns in {filepath}.\n"
            f"Columns found: {list(df.columns)}"
        )

    addresses = {}
    for _, row in df.iterrows():
        first      = str(row[first_col]).strip()      if first_col      else ""
        last       = str(row[last_col]).strip()       if last_col       else ""
        city       = str(row[city_col]).strip()       if city_col       else ""
        state      = str(row[state_col]).strip()      if state_col      else ""
        address    = str(row[address_col]).strip()    if address_col    else ""
        employer   = str(row[employer_col]).strip()   if employer_col   else ""
        occupation = str(row[occupation_col]).strip() if occupation_col else ""

        if not first or not last or first.lower() == "nan" or last.lower() == "nan":
            continue
        if address.lower() == "nan":
            address = ""
        if employer.lower() == "nan":
            employer = ""
        if occupation.lower() == "nan":
            occupation = ""

        key = (last.lower(), first.lower(), city.lower(), state.lower())
        addresses[key] = {"address": address, "employer": employer, "occupation": occupation}

    return addresses


def load_consider_donors(summary_filepath, donors_filepath):
    """
    Reads fec_results_summary.csv, filters to rows where Consideration
    starts with "CONSIDER" (catches "CONSIDER", "CONSIDER (Governor-level)",
    "CONSIDER (medium quality)", etc. — but NOT "DO NOT CONSIDER").

    Joins each row against donors.csv (via load_donor_addresses) on
    Last+First+City+State to attach the donor's mailing address, since
    the summary CSV itself doesn't carry address.

    Returns a list of donor dicts ready for NYSBOE lookup.
    """
    if not os.path.exists(summary_filepath):
        raise FileNotFoundError(
            f"Could not find {summary_filepath}. Run fec_donor_lookup.py first, "
            f"or check the FEC_SUMMARY_FILE path at the top of this script."
        )
    if not os.path.exists(donors_filepath):
        raise FileNotFoundError(
            f"Could not find {donors_filepath}. This should be the same donor "
            f"spreadsheet you gave to fec_donor_lookup.py."
        )

    summary_df = pd.read_csv(summary_filepath)
    summary_df.columns = summary_df.columns.str.strip()

    required_cols = {"Last Name", "First Name", "City", "State", "Consideration"}
    missing = required_cols - set(summary_df.columns)
    if missing:
        raise ValueError(
            f"{summary_filepath} is missing expected column(s): {missing}. "
            f"Columns found: {list(summary_df.columns)}"
        )

    address_lookup = load_donor_addresses(donors_filepath)

    consider_donors = []
    skipped_no_address = 0

    for _, row in summary_df.iterrows():
        consideration = str(row["Consideration"]).strip()

        # Catches "CONSIDER", "CONSIDER (Governor-level)", "CONSIDER (medium quality)", etc.
        # but NOT "DO NOT CONSIDER" (which also starts with "DO NOT", not "CONSIDER").
        if not consideration.upper().startswith("CONSIDER"):
            continue

        last  = str(row["Last Name"]).strip()
        first = str(row["First Name"]).strip()
        city  = str(row["City"]).strip()
        state = str(row["State"]).strip()

        key = (last.lower(), first.lower(), city.lower(), state.lower())
        record = address_lookup.get(key, {})
        address    = record.get("address", "")
        employer   = record.get("employer", "")
        occupation = record.get("occupation", "")

        if not address:
            skipped_no_address += 1

        consider_donors.append({
            "first_name":        first,
            "last_name":         last,
            "city":              city,
            "state":             state,
            "address":           address,
            "employer":          employer,
            "occupation":        occupation,
            "fec_consideration": consideration,
        })

    print(f"Loaded {len(consider_donors)} CONSIDER donor(s) from {summary_filepath}")
    if skipped_no_address:
        print(
            f"  WARNING — {skipped_no_address} donor(s) had no matching address "
            f"found in {donors_filepath} (name/city/state didn't match any row). "
            f"These will still be searched by name, but address-based confirmation "
            f"won't be possible for them."
        )

    return consider_donors


# =========================================================
# FEC ADDRESS LOOKUP
# -------------------------------------------------------
# Deliberately minimal — this is NOT a copy of fec_donor_lookup.py's
# fetch_contributions(). It does not do employer matching, occupation
# confirmation, ActBlue dedup, or consideration scoring. Its only job
# is: "what street addresses does the FEC have on file for this donor?"
# so we have more than just the spreadsheet address to try against NYSBOE.
#
# Runs at FEC_ADDRESS_LOOKUP_WORKERS=10 concurrent workers (see that
# constant's definition for reasoning), on a separate API key from
# the main FEC script.
# =========================================================

_thread_local = threading.local()

def _get_fec_session():
    """Returns a thread-local requests.Session, creating one if needed."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def _fetch_fec_page(params, max_retries=3):
    """
    Single paginated-aware FEC fetch, simplified from fec_donor_lookup.py's
    _fetch_raw: same retry/backoff/429-handling pattern, but only pulls
    ONE page worth of results (per_page=100) since we just need a sample
    of addresses, not a full giving history. Most donors' addresses are
    consistent across their records, so one page is enough to find the
    distinct ones in practice.

    Raises RuntimeError on total retry exhaustion — fails loudly rather
    than silently returning an empty address list that looks like a
    legitimate "no records" result. The error message includes the
    actual underlying exception from the last attempt, so an
    intermittent failure (timeout vs. connection reset vs. DNS issue vs.
    something else) is distinguishable instead of a generic message.
    """
    session = _get_fec_session()
    response = None  # reset before every attempt — never reuse a stale response
    last_exception = None

    for attempt in range(max_retries):
        try:
            response = session.get(FEC_BASE_URL, params=params, timeout=30)
            last_exception = None
            break
        except requests.exceptions.Timeout as e:
            response = None
            last_exception = e
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            response = None
            last_exception = e
            time.sleep(2)

    if response is None:
        raise RuntimeError(
            f"FEC API request failed after {max_retries} retries while "
            f"gathering addresses ({type(last_exception).__name__}: "
            f"{last_exception}). Retry manually, or re-run the script — "
            f"this is usually transient."
        )

    if response.status_code == 429:
        print("    Rate limited by FEC API (address lookup) — waiting 60 seconds...", flush=True)
        time.sleep(60)
        return _fetch_fec_page(params, max_retries=max_retries)

    if response.status_code != 200:
        return []

    return response.json().get("results", [])


# =========================================================
# EMPLOYER COMPARISON (for confirming FEC address ownership)
# -------------------------------------------------------
# Ported from fec_donor_lookup.py's own employer-matching logic, used
# there to confirm a contribution record actually belongs to the donor
# being researched (not just someone with a matching name). This tool
# needs the exact same confirmation step for a different reason: when
# get_fec_known_addresses() queries FEC by Last+First(+State), FEC's
# name search is not unique-person-aware — it can and does return
# records belonging to a DIFFERENT person who happens to share that
# name and state. Feeding every one of those addresses into NYSBOE
# matching as if they all belonged to the donor would let a different
# person's address falsely confirm a NYSBOE record as a "street_match."
#
# Employer is the only available signal to filter that out (FEC has no
# stronger unique identifier in the public API). This mirrors
# fec_donor_lookup.py's own normalize_employer/employers_are_same logic
# exactly, including its no-employer and self-employed buckets and
# two-word partial matching, so the same employer string is judged
# identically by both tools.
# =========================================================

NO_EMPLOYER_BUCKET = {
    "retired", "not employed", "none", "n/a", "na", "homemaker",
    "home maker", "housewife", "unemployed", "student", "disabled",
    "volunteer", "not employed/retired", "none/retired",
    "not applicable", "not working",
}

SELF_EMPLOYED_BUCKET = {
    "self employed", "self-employed", "selfemployed", "freelance",
    "freelancer", "independent", "independent contractor", "contract",
    "own business", "own company", "private",
}

_EMPLOYER_SUFFIXES = [
    " llc", " inc", " corp", " co", " ltd", " lp", " llp",
    ", llc", ", inc", ", corp", ", co", ", ltd", " & co",
    " and co", " corporation", " company",
]


def normalize_employer(emp):
    """
    Normalizes an employer string for comparison: lowercases, maps
    no-employer / self-employed variants to single shared tokens, and
    strips common corporate suffixes that don't change identity (e.g.
    "Acme Inc" and "Acme" should compare equal).
    """
    if not emp:
        return "__no_employer__"
    cleaned = emp.lower().strip()
    if cleaned in NO_EMPLOYER_BUCKET:
        return "__no_employer__"
    if cleaned in SELF_EMPLOYED_BUCKET:
        return "__self_employed__"
    for suffix in _EMPLOYER_SUFFIXES:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)].strip()
            break
    return cleaned


def employer_partial_match(emp1, emp2):
    """
    Two-word partial match: employers are treated as the same if their
    first two words match (e.g. "Goldman Sachs" matches "Goldman Sachs
    & Co"), falling back to single-word match when an employer name has
    only one word (e.g. "Microsoft" matches "Microsoft").
    """
    words1 = emp1.split()
    words2 = emp2.split()
    if not words1 or not words2:
        return False
    n = min(2, len(words1), len(words2))
    return words1[:n] == words2[:n]


def employers_are_same(emp1, emp2):
    """
    Returns True if two employer strings should be treated as
    confirming the same person. Exact match after normalization first
    (this also catches both being blank/no-employer or both being
    self-employed), then falls back to two-word partial match.
    """
    norm1 = normalize_employer(emp1)
    norm2 = normalize_employer(emp2)
    if norm1 == norm2:
        return True
    return employer_partial_match(norm1, norm2)


def get_fec_known_addresses(last_name, first_name, state,
                            donor_employer="", donor_occupation="",
                            donor_address=""):
    """
    Queries the FEC API by Last+First (+ state, if available) and returns
    a list of distinct street addresses found in FEC records for this
    name, ALONG WITH the first name(s) FEC actually has on file for this
    donor and a confidence tier for each address.

    Three-tier identity confirmation hierarchy (mirrors fec_donor_lookup.py):

      Tier 1 — Street match: FEC record's street matches the donor's own
        spreadsheet address. Strongest signal — no further check needed.
        Tagged: tier="street"

      Tier 2 — Employer match: Street doesn't match spreadsheet address,
        but FEC record's employer matches donor's employer. Weaker signal
        — address is kept but flagged for review if it produces a NYSBOE
        match. Tagged: tier="employer"

      Tier 3 — Occupation match: Street AND employer both fail (or are
        blank), but FEC record's occupation matches donor's occupation.
        Weakest signal — address kept but flagged. Tagged: tier="occupation"

      Drop: All three fail with confirmed mismatches → address excluded.

    Blank fields: if the donor's own employer/occupation is blank, those
    tiers are skipped (can't confirm a mismatch). If the FEC record's
    employer/occupation is blank, that tier also can't confirm a mismatch
    and falls through to the next tier.

    Returns a list of dicts: [{"street": ..., "city": ..., "state": ...,
    "fec_first_name": ..., "tier": "street"|"employer"|"occupation"}, ...]
    Deduplicated by normalized (street, city, state) — when the same
    address appears under multiple tiers, the strongest tier wins.
    """
    params = {
        "contributor_name": f"{last_name}, {first_name}",
        "min_date": MIN_DATE,
        "per_page": 100,
        "sort": "-contribution_receipt_amount",
        "api_key": FEC_API_KEY,
    }
    if state:
        params["contributor_state"] = state

    results = _fetch_fec_page(params)

    # Only try flipped name format if first attempt returns zero results —
    # mirrors fec_donor_lookup.py's fallback for the same reason (FEC's
    # name search can be picky about "Last, First" vs "First Last" order).
    if not results:
        params_flip = dict(params)
        params_flip["contributor_name"] = f"{first_name} {last_name}"
        results = _fetch_fec_page(params_flip)

    def extract_fec_first_name(raw_contributor_name):
        """Parses 'LAST, FIRST MIDDLE' -> 'FIRST'. Same approach as
        fec_donor_lookup.py's fec_first_initial(), extended to return
        the full first name rather than just its initial."""
        raw = (raw_contributor_name or "").strip()
        if "," not in raw:
            return ""
        first_part = raw.split(",", 1)[1].strip()
        return first_part.split()[0].title() if first_part else ""

    # Pre-normalize the donor's own spreadsheet address for tier-1 comparison
    donor_addr_norm = normalize_address(donor_address, is_nysboe_format=False)
    donor_addr_key  = street_key(donor_addr_norm) if donor_addr_norm else None

    donor_employer_clean   = (donor_employer or "").strip()
    donor_occupation_clean = (donor_occupation or "").strip()

    # Tier ranking for dedup: lower number = stronger
    TIER_RANK = {"street": 0, "employer": 1, "occupation": 2}

    # seen maps dedup_key -> {"tier": ..., "fec_first_name": ..., index in addresses}
    seen = {}
    addresses = []
    dropped = 0

    for r in results:
        street    = (r.get("contributor_street_1") or "").strip()
        city      = (r.get("contributor_city") or "").strip()
        rstate    = (r.get("contributor_state") or "").strip()
        fec_first = extract_fec_first_name(r.get("contributor_name"))
        rec_emp   = (r.get("contributor_employer") or "").strip()
        rec_occ   = (r.get("contributor_occupation") or "").strip()

        if not street:
            continue

        # ── Tier 1: street matches donor's own spreadsheet address ──
        rec_norm = normalize_address(street, is_nysboe_format=False)
        rec_key  = street_key(rec_norm)
        if donor_addr_key and rec_key and donor_addr_key == rec_key:
            tier = "street"

        # ── Tier 2: employer match ──
        elif donor_employer_clean and rec_emp:
            if employers_are_same(donor_employer_clean, rec_emp):
                tier = "employer"
            else:
                # Confirmed employer mismatch — fall through to occupation
                if donor_occupation_clean and rec_occ:
                    if employers_are_same(donor_occupation_clean, rec_occ):
                        tier = "occupation"
                    else:
                        # All three confirmed mismatches — drop
                        dropped += 1
                        continue
                elif donor_occupation_clean and not rec_occ:
                    # Can't confirm occupation mismatch — keep at occupation tier
                    tier = "occupation"
                else:
                    # No occupation to check — drop on employer mismatch
                    dropped += 1
                    continue

        # ── Tier 2 fallback: donor has no employer on file ──
        elif not donor_employer_clean:
            # Can't run employer check — try occupation
            if donor_occupation_clean and rec_occ:
                if employers_are_same(donor_occupation_clean, rec_occ):
                    tier = "occupation"
                else:
                    # Occupation mismatch, no employer — drop
                    dropped += 1
                    continue
            else:
                # No employer or occupation to confirm — keep but weakest signal
                tier = "occupation"

        # ── Tier 2 fallback: FEC record has no employer ──
        else:
            # donor has employer but FEC record blank — can't confirm mismatch
            # fall through to occupation check
            if donor_occupation_clean and rec_occ:
                if employers_are_same(donor_occupation_clean, rec_occ):
                    tier = "occupation"
                else:
                    dropped += 1
                    continue
            else:
                tier = "occupation"

        dedup_key = (street.lower(), city.lower(), rstate.lower())
        if dedup_key in seen:
            # Keep the stronger tier if we've seen this address before
            existing_idx = seen[dedup_key]["idx"]
            if TIER_RANK[tier] < TIER_RANK[addresses[existing_idx]["tier"]]:
                addresses[existing_idx]["tier"] = tier
            continue

        idx = len(addresses)
        seen[dedup_key] = {"idx": idx}
        addresses.append({
            "street": street, "city": city, "state": rstate,
            "fec_first_name": fec_first, "tier": tier,
        })

    if dropped:
        print(
            f"    [{last_name}] Dropped {dropped} FEC record(s)/address(es) "
            f"where street, employer, and occupation all failed to confirm "
            f"identity — likely a different person with the same name.", flush=True
        )

    return addresses


# =========================================================
# NYSBOE SEARCH (Playwright — browser automation)
# -------------------------------------------------------
# NYSBOE's "Contributions by Contributor" page has no public API. The
# Search button triggers a server-side request that, after extensive
# testing, only succeeds when it comes from a REAL browser click — a
# script-constructed HTTP request with identical-looking headers, cookies,
# and payload still gets rejected with a generic error page. This appears
# to be server-side bot detection NYSBOE has in place, not a missing
# token or header (confirmed: no __VIEWSTATE/hidden fields exist on this
# page; cookies, Content-Type, Accept, and X-Requested-With were all
# tried and matched the real request).
#
# So this section uses Playwright to drive an ACTUAL browser: load the
# page, type into the real input field, click the real button, wait for
# real results to render, then read the rendered table directly (rather
# than the CSV export, which depends on the same session-priming search
# step and adds another moving part for no benefit once we're already
# driving a browser).
#
# NYSBOE's search is also genuinely slow/flaky under normal interactive
# use (observed multiple 5-10+ second searches, and one 503 mid-session)
# — so this waits for an actual completion signal (the "Total
# Contributions" text appearing) rather than a fixed short timeout.
#
# Confirmed-real selectors (verified directly against the live DOM):
#   input#txtNameCont      — contributor name field
#   button#btnCommonSearch — search button
#   button#btnCommonClear  — clear button
# =========================================================

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth


# =========================================================
# NICKNAME EXPANSION (for NYSBOE searches only)
# -------------------------------------------------------
# NYSBOE's giving records may list a donor under a nickname even if your
# spreadsheet has their full first name, or vice versa (e.g. spreadsheet
# says "William" but NYSBOE has "Bill", or spreadsheet says "Bill" but
# NYSBOE has "William"). This expands a donor's first name to include
# known nickname/full-name variants before searching.
#
# Source: curated from the well-established, high-frequency subset of
# the carltonnorthern/nicknames dataset (public domain, built from US
# census/genealogy records) — not the full ~2,600-row file, which
# includes many genealogically rare/archaic names not relevant here.
#
# Several nicknames are genuinely ambiguous between two root names with
# no single dominant modern usage (e.g. "Pat" -> Patrick or Patricia,
# "Chris" -> Christopher or Christine, "Hal"/"Harry" -> Harold or
# Henry). Rather than guess one, BOTH candidates are included for those
# — an extra NYSBOE search is cheap; a missed match is not. A few
# others have one clearly dominant modern mapping and were resolved to
# just that one (e.g. "Bill" -> William only, not the historically real
# but now-rare Bill -> Robert link).
# =========================================================

NICKNAME_TO_FULL = {
    'al': ['albert', 'alexander'],
    'alex': ['alexander'],
    'andy': ['andrew'],
    'barb': ['barbara'],
    'barbie': ['barbara'],
    'becca': ['rebecca'],
    'becky': ['rebecca'],
    'ben': ['benjamin'],
    'benny': ['benjamin'],
    'bert': ['albert'],
    'bess': ['elizabeth'],
    'bessie': ['elizabeth'],
    'beth': ['elizabeth'],
    'betsy': ['elizabeth'],
    'betty': ['elizabeth'],
    'bill': ['william'],
    'billy': ['william'],
    'bob': ['robert'],
    'bobby': ['robert'],
    'carl': ['charles'],
    'cathy': ['katherine'],
    'charlie': ['charles'],
    'chris': ['christopher', 'christine'],
    'chrissy': ['christine'],
    'christy': ['christine'],
    'chuck': ['charles'],
    'cindy': ['cynthia'],
    'daisy': ['margaret'],
    'dan': ['daniel'],
    'danny': ['daniel'],
    'dave': ['david'],
    'davey': ['david'],
    'deb': ['deborah'],
    'debbie': ['deborah'],
    'dick': ['richard'],
    'dickie': ['richard'],
    'dolly': ['dorothy'],
    'don': ['donald'],
    'donnie': ['donald'],
    'dot': ['dorothy'],
    'dottie': ['dorothy'],
    'drew': ['andrew'],
    'ed': ['edward'],
    'eddie': ['edward'],
    'eliza': ['elizabeth'],
    'ella': ['helen'],
    'ellen': ['helen'],
    'ellie': ['eleanor', 'helen'],
    'fanny': ['frances'],
    'fran': ['frances', 'francis'],
    'frank': ['francis'],
    'frankie': ['francis', 'frances'],
    'fred': ['frederick'],
    'freddie': ['frederick'],
    'freddy': ['frederick'],
    'gene': ['eugene'],
    'gerry': ['gerald'],
    'ginger': ['virginia'],
    'ginny': ['virginia'],
    'greg': ['gregory'],
    'hal': ['harold', 'henry'],
    'hank': ['henry'],
    'harry': ['harold', 'henry'],
    'jack': ['john'],
    'jackie': ['jacqueline'],
    'jamie': ['james'],
    'jeff': ['jeffrey'],
    'jen': ['jennifer'],
    'jennie': ['jennifer'],
    'jenny': ['jennifer'],
    'jerry': ['gerald'],
    'jim': ['james'],
    'jimmie': ['james'],
    'jimmy': ['james'],
    'joe': ['joseph'],
    'joey': ['joseph'],
    'johnny': ['john'],
    'kate': ['katherine'],
    'kathy': ['katherine'],
    'katie': ['katherine'],
    'kay': ['katherine'],
    'ken': ['kenneth'],
    'kenny': ['kenneth'],
    'larry': ['lawrence'],
    'libby': ['elizabeth'],
    'liz': ['elizabeth'],
    'liza': ['elizabeth'],
    'lizzie': ['elizabeth'],
    'lou': ['louis'],
    'louie': ['louis'],
    'maggie': ['margaret'],
    'mamie': ['mary'],
    'marge': ['margaret'],
    'margie': ['margaret'],
    'marty': ['martha'],
    'matt': ['matthew'],
    'mattie': ['martha'],
    'meg': ['margaret'],
    'mick': ['michael'],
    'mickey': ['michael'],
    'mike': ['michael'],
    'mikey': ['michael'],
    'molly': ['mary'],
    'ned': ['edward'],
    'nell': ['eleanor'],
    'nick': ['nicholas'],
    'nicky': ['nicholas'],
    'nora': ['eleanor'],
    'paddy': ['patrick'],
    'pat': ['patrick', 'patricia'],
    'patsy': ['patricia', 'martha'],
    'patty': ['patricia'],
    'peggy': ['margaret'],
    'pete': ['peter'],
    'polly': ['mary'],
    'ray': ['raymond'],
    'rich': ['richard'],
    'richie': ['richard'],
    'rick': ['richard'],
    'ricky': ['richard'],
    'rob': ['robert'],
    'robby': ['robert'],
    'sadie': ['sarah'],
    'sally': ['sarah'],
    'sam': ['samuel'],
    'sammy': ['samuel'],
    'steph': ['stephen', 'stephanie'],
    'steve': ['stephen'],
    'sue': ['susan'],
    'susie': ['susan'],
    'ted': ['edward', 'theodore'],
    'teddy': ['edward', 'theodore'],
    'terry': ['theresa'],
    'tess': ['theresa'],
    'tessa': ['theresa'],
    'theo': ['theodore'],
    'tim': ['timothy'],
    'timmy': ['timothy'],
    'tom': ['thomas'],
    'tommy': ['thomas'],
    'tony': ['anthony'],
    'tori': ['victoria'],
    'tricia': ['patricia'],
    'trish': ['patricia'],
    'vicki': ['victoria'],
    'vicky': ['victoria'],
    'vin': ['vincent'],
    'vince': ['vincent'],
    'vinny': ['vincent'],
    'wally': ['walter'],
    'walt': ['walter'],
    'wil': ['william'],
    'will': ['william'],
    'willie': ['william'],
    'willy': ['william'],
}

FULL_TO_NICKNAMES = {
    'albert': ['al', 'bert'],
    'alexander': ['al', 'alex'],
    'andrew': ['andy', 'drew'],
    'anthony': ['tony'],
    'barbara': ['barb', 'barbie'],
    'benjamin': ['ben', 'benny'],
    'charles': ['carl', 'charlie', 'chuck'],
    'christine': ['chris', 'chrissy', 'christy'],
    'christopher': ['chris'],
    'cynthia': ['cindy'],
    'daniel': ['dan', 'danny'],
    'david': ['dave', 'davey'],
    'deborah': ['deb', 'debbie'],
    'donald': ['don', 'donnie'],
    'dorothy': ['dolly', 'dot', 'dottie'],
    'edward': ['ed', 'eddie', 'ned', 'ted', 'teddy'],
    'eleanor': ['ellie', 'nell', 'nora'],
    'elizabeth': ['bess', 'bessie', 'beth', 'betsy', 'betty', 'eliza', 'libby', 'liz', 'liza', 'lizzie'],
    'eugene': ['gene'],
    'frances': ['fanny', 'fran', 'frankie'],
    'francis': ['fran', 'frank', 'frankie'],
    'frederick': ['fred', 'freddie', 'freddy'],
    'gerald': ['gerry', 'jerry'],
    'gregory': ['greg'],
    'harold': ['hal', 'harry'],
    'helen': ['ella', 'ellen', 'ellie'],
    'henry': ['hal', 'hank', 'harry'],
    'jacqueline': ['jackie'],
    'james': ['jamie', 'jim', 'jimmie', 'jimmy'],
    'jeffrey': ['jeff'],
    'jennifer': ['jen', 'jennie', 'jenny'],
    'john': ['jack', 'johnny'],
    'joseph': ['joe', 'joey'],
    'katherine': ['cathy', 'kate', 'kathy', 'katie', 'kay'],
    'kenneth': ['ken', 'kenny'],
    'lawrence': ['larry'],
    'louis': ['lou', 'louie'],
    'margaret': ['daisy', 'maggie', 'marge', 'margie', 'meg', 'peggy'],
    'martha': ['marty', 'mattie', 'patsy'],
    'mary': ['mamie', 'molly', 'polly'],
    'matthew': ['matt'],
    'michael': ['mick', 'mickey', 'mike', 'mikey'],
    'nicholas': ['nick', 'nicky'],
    'patricia': ['pat', 'patsy', 'patty', 'tricia', 'trish'],
    'patrick': ['paddy', 'pat'],
    'peter': ['pete'],
    'raymond': ['ray'],
    'rebecca': ['becca', 'becky'],
    'richard': ['dick', 'dickie', 'rich', 'richie', 'rick', 'ricky'],
    'robert': ['bob', 'bobby', 'rob', 'robby'],
    'samuel': ['sam', 'sammy'],
    'sarah': ['sadie', 'sally'],
    'stephanie': ['steph'],
    'stephen': ['steph', 'steve'],
    'susan': ['sue', 'susie'],
    'theodore': ['ted', 'teddy', 'theo'],
    'theresa': ['terry', 'tess', 'tessa'],
    'thomas': ['tom', 'tommy'],
    'timothy': ['tim', 'timmy'],
    'victoria': ['tori', 'vicki', 'vicky'],
    'vincent': ['vin', 'vince', 'vinny'],
    'virginia': ['ginger', 'ginny'],
    'walter': ['wally', 'walt'],
    'william': ['bill', 'billy', 'wil', 'will', 'willie', 'willy'],
}


def get_name_variants(first_name):
    """
    Given a donor's first name, returns a list of name variants to try
    when searching NYSBOE: the name as given, plus any known nickname
    or full-name equivalents.

    - If first_name is a known NICKNAME (e.g. "Bill"), returns the name
      itself plus its resolved full name(s) (e.g. ["Bill", "William"]).
    - If first_name is a known FULL NAME (e.g. "William"), returns the
      name itself plus its known nicknames (e.g. ["William", "Bill",
      "Billy", "Wil", "Will", "Willie", "Willy"]).
    - If first_name isn't in either table, returns just [first_name] —
      no expansion, search proceeds as normal.

    Always returns at least [first_name], and never returns duplicates.
    Capitalization of the input is preserved for the original name;
    expansions are returned Title-Cased for readability in logs/output,
    though NYSBOE's search is case-insensitive regardless.
    """
    if not first_name:
        return [first_name]

    key = first_name.strip().lower()
    variants = [first_name.strip()]
    seen = {key}

    for candidate in NICKNAME_TO_FULL.get(key, []):
        if candidate not in seen:
            variants.append(candidate.title())
            seen.add(candidate)

    for candidate in FULL_TO_NICKNAMES.get(key, []):
        if candidate not in seen:
            variants.append(candidate.title())
            seen.add(candidate)

    return variants


NYSBOE_SEARCH_URL = "https://publicreporting.elections.ny.gov/Contributions/Contributions"

# Deliberately gentle and sequential (not parallel like the FEC tool's
# Modest parallelism (3 workers) — confirmed real time data shows the
# 2-hour runtime for 169 donors was dominated by timeout failures, not
# successful searches (~40% of total time spent on the ~15% of donors
# that errored, since each failed attempt burned the full timeout
# twice). Lowering the timeout and dropping the in-loop retry (see
# below) address that directly. Parallelism is a SEPARATE, additional
# lever — kept conservative at 3 workers since NYSBOE has shown real
# flakiness under sequential load already; more aggressive parallelism
# risks triggering more failures than it saves in time, on a site that
# already shows signs of being rate-sensitive.
NYSBOE_MAX_WORKERS = 3
NYSBOE_SEARCH_TIMEOUT_MS = 20000   # lowered from 50s now that the wait condition
                                     # correctly detects both completion states:
                                     # results found ("Total Contributions") AND
                                     # zero results ("No data available in table"
                                     # with "Processing..." absent). Previously,
                                     # zero-result searches waited the full timeout
                                     # because "Total Contributions" never appears
                                     # on a zero-result page — so the old condition
                                     # always timed out for those. With the correct
                                     # condition, both states complete within seconds.
NYSBOE_BETWEEN_SEARCHES_DELAY = 2  # seconds, polite pacing between donors


_RE_RECIPIENT_ID_SUFFIX = re.compile(r'\s*-\s*ID#\s*\d*\s*$', re.IGNORECASE)


def clean_recipient_name(raw_recipient):
    """
    Strips NYSBOE's trailing committee ID suffix from a recipient name,
    e.g. "James for NY 2026 - ID# 308810" -> "James for NY 2026", and
    also "James for NY 2026 - ID#" (no number at all) -> "James for NY 2026".

    Only confirmed against real examples seen so far (the " - ID#" or
    " - ID# <digits>" pattern at the end of the string) — the regex is
    anchored to that specific shape so it won't accidentally strip
    anything from a recipient name that doesn't match it.
    """
    if not raw_recipient:
        return raw_recipient
    return _RE_RECIPIENT_ID_SUFFIX.sub('', raw_recipient).strip()


def _nysboe_extract_rows(page, max_pagination_seconds=20):
    """
    Reads ALL rendered results rows after a search completes, paging
    through as needed.

    NYSBOE's results table defaults to showing only 10 rows per page
    (confirmed via direct testing: a "Show [10/25/50/100] entries"
    dropdown exists at select[name='grdContributions_length']). An
    earlier version of this function only read whatever 10 rows were
    on the currently-displayed page — meaning any donor with more than
    10 NY State records was silently missing all the rest. This now
    selects the largest available page size (100) first, then clicks
    "Next" repeatedly if there are still more than 100 total results.

    IMPORTANT: this pagination loop used to have NO timeout protection
    at all — only a page-COUNT ceiling (max_pages), not a time ceiling.
    Confirmed via real test data that this was a real bug, not just a
    theoretical one: a "known slow names" stress-test batch found the
    exact same ~26 donors timing out consistently across multiple runs
    (not randomly, which is what you'd expect from generic site
    flakiness) — pointing at something structural rather than bad luck.
    The most likely explanation: those donors return enough NYSBOE
    records to need several "Next" clicks, and each click's fixed
    page.wait_for_timeout(2000) was running completely outside
    nysboe_search()'s outer NYSBOE_SEARCH_TIMEOUT_MS budget, since that
    timeout only wraps the INITIAL search-completion wait, not this
    pagination phase that runs after it. A donor needing many pages
    could exceed even a generous outer timeout with no protection.

    max_pagination_seconds bounds this loop independently — if exceeded,
    returns whatever records were already collected (partial results)
    rather than hanging indefinitely or raising an error. Partial
    results for a high-volume donor are far more useful than a hard
    failure; sort_and_cap_rows() downstream already caps at the top 30
    by dollar amount anyway, so a partial page set is often sufficient.

    Returns an empty list for a genuine "no records found" — NOT an
    error. Most donors will have no NY state giving history; that's the
    expected common case, not a failure.
    """
    pagination_start = time.time()

    # Bump the page size from the default 10 up to 100, if the dropdown
    # is present (it won't be if there are 0 results at all).
    length_select = page.query_selector("select[name='grdContributions_length']")
    if length_select:
        try:
            length_select.select_option("100")
            page.wait_for_timeout(1500)  # let the table re-render at the new page size
        except Exception:
            pass  # if this fails for any reason, fall back to whatever's already shown

    all_records = []
    seen_signatures = set()
    max_pages = 20  # safety ceiling (20 pages * 100 = 2,000 records) — a real
                     # donor needing more than this is essentially unheard of;
                     # this just prevents an infinite loop if "Next" misbehaves

    for _ in range(max_pages):
        if time.time() - pagination_start > max_pagination_seconds:
            print(f"    (pagination time budget of {max_pagination_seconds}s "
                  f"reached — returning {len(all_records)} record(s) collected "
                  f"so far rather than continuing to page through more)",
                  flush=True)
            break

        rows = page.query_selector_all("table tbody tr")
        page_had_new_row = False
        for row in rows:
            cells = row.query_selector_all("td")
            cell_text = [c.inner_text().strip() for c in cells]
            if len(cell_text) < 6:
                continue
            # Column order confirmed from the live table header:
            # 0 Expand | 1 Contribution Date | 2 Amount | 3 Contributor Name |
            # 4 Detail Original Name | 5 Contributor Address | 6 Transaction
            # Type | 7 Contributor Type | 8 Transfer Type | 9 Recipient | ...
            record = {
                "contribution_date":    cell_text[1] if len(cell_text) > 1 else "",
                "amount":               cell_text[2] if len(cell_text) > 2 else "",
                "contributor_name":     cell_text[3] if len(cell_text) > 3 else "",
                "detail_original_name": cell_text[4] if len(cell_text) > 4 else "",
                "contributor_address":  cell_text[5] if len(cell_text) > 5 else "",
                "recipient":            clean_recipient_name(cell_text[9]) if len(cell_text) > 9 else "",
            }
            # De-dupe by content signature, in case a "Next" click doesn't
            # actually advance the page (would otherwise re-read the same
            # rows and loop until max_pages for no reason).
            signature = tuple(record.values())
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            all_records.append(record)
            page_had_new_row = True

        if not page_had_new_row:
            break  # nothing new on this "page" — either done, or stuck; stop either way

        next_button = page.query_selector("a:has-text('Next'), button:has-text('Next')")
        if not next_button:
            break
        next_class = next_button.get_attribute("class") or ""
        if "disabled" in next_class.lower():
            break  # "Next" is greyed out — we're on the last page

        next_button.click()
        page.wait_for_timeout(2000)  # let the next page's rows render

    return all_records


def build_nysboe_search_queries(last_name, fec_first_names, spreadsheet_first_name=None):
    """
    Builds the list of "First Last" query strings to run against NYSBOE
    for one donor, per the agreed rule:

      - Start from the first name(s) FEC actually has on file for this
        donor (fec_first_names — may be more than one if FEC has
        slightly different spellings/forms across records, e.g. both
        "Bill" and "William" if the donor used both over time).
      - If a given FEC first name is a plain full name (not itself a
        known nickname), search ONLY "FullName Last" — no expansion.
        E.g. FEC has "William" -> search just "William Smith", not also
        "Bill Smith"/"Billy Smith"/etc.
      - If the FEC first name IS itself a known nickname (e.g. "Bill"),
        search BOTH the nickname form ("Bill Last") AND its resolved
        full name form ("William Last") — since NYSBOE might have the
        donor under either.
      - If FEC has no first name on file at all (e.g. the FEC lookup
        failed or returned nothing), falls back to the spreadsheet first
        name (spreadsheet_first_name) with the same nickname-expansion
        logic applied. If that's also unavailable, falls back to
        last-name-only as a last resort.

    IMPORTANT: last-name-only was the original fallback, but real test
    data showed it causes consistent 60-120s+ timeouts for common last
    names — "BANK" returns 16,001 NYSBOE entries and takes 60s to
    respond; "Jeffrey BANK" returns a handful and takes seconds. Always
    using first+last is the correct behavior even when FEC fails.

    Returns a deduplicated list of "First Last" (or just "Last") query
    strings, preserving a sensible order (each FEC-recorded name's
    variants grouped together).
    """
    # Use FEC first names if available, fall back to spreadsheet first
    # name, fall back to last-name-only as an absolute last resort.
    first_names_to_use = fec_first_names if fec_first_names else (
        [spreadsheet_first_name] if spreadsheet_first_name else []
    )

    if not first_names_to_use:
        return [last_name]

    queries = []
    seen = set()
    for first in first_names_to_use:
        if not first:
            continue

        key = first.strip().lower()
        if key in NICKNAME_TO_FULL:
            variants = [first.strip()] + [f.title() for f in NICKNAME_TO_FULL[key]]
        else:
            variants = [first.strip()]

        for variant in variants:
            query = f"{variant} {last_name}"
            qkey = query.lower()
            if qkey not in seen:
                seen.add(qkey)
                queries.append(query)

    return queries if queries else [last_name]


def nysboe_search(playwright_page, contributor_name):
    """
    Runs one NYSBOE "Contributions by Contributor" search using an
    already-open Playwright page, and returns the parsed result rows.

    contributor_name: searched as a substring (NYSBOE's search is
        "contains", not exact) — e.g. "ARZT" matches "George Arzt",
        "Amanda Arzt", and any business name containing "Arzt". Passing
        "LAST" alone (not "LAST, FIRST") matched real site behavior
        confirmed during testing; the donor-level filtering by full name
        and address happens afterward, in Python, against these rows.

    NOTE on dates: this does NOT use NYSBOE's own Date From field.
    Confirmed via direct testing (filling the field every way Playwright
    supports — .fill(), .type(), manual change/blur events, even jQuery
    UI's own datepicker API) that the value never actually reaches the
    real search request — the captured POST body showed txtDateFrom=
    empty every time, regardless of what was typed into the visible
    field. Rather than rely on an NYSBOE site mechanism we couldn't get
    working, this pulls ALL records for the name and the 1/1/2016 date
    floor is applied afterward in Python (see filter_rows_by_date),
    against the date already present in each returned row.

    Returns [] for a confirmed "zero records found" search — this is the
    expected, common case (most donors have no NY state giving history),
    not an error.

    Raises RuntimeError if the search itself doesn't complete normally
    (timeout, navigation failure, unexpected page state) — so a real
    site problem surfaces as a visible error rather than a misleading
    empty result.
    """
    try:
        playwright_page.goto(NYSBOE_SEARCH_URL, timeout=30000)

        name_field = playwright_page.locator("#txtNameCont")
        name_field.fill(contributor_name)

        playwright_page.click("#btnCommonSearch")

        # Wait for the search to complete. Two valid completion states:
        #
        # 1. Results found: "Total Contributions" appears in the page body
        #    (e.g. "Total Contributions: $7,593.00").
        #
        # 2. Zero results: "No data available in table" appears AND the
        #    "Processing..." overlay is gone. This is the case we were
        #    previously missing — NYSBOE shows "Showing 0 to 0 of 0 entries /
        #    No data available in table" as the table's INITIAL EMPTY STATE
        #    before the search even starts, so we can't just check for
        #    "No data" alone. We also need "Processing" to be absent,
        #    confirming the search has actually finished. Confirmed via
        #    direct testing: CANDAU, CAPLAN, HORING all showed this exact
        #    zero-result completed state within 2 seconds, but the old
        #    wait_for_function waited forever for "Total Contributions"
        #    which never appears on a zero-result page.
        playwright_page.wait_for_function(
            """() => {
                const body = document.body.innerText;
                const hasResults = body.includes('Total Contributions');
                const isZeroResult = (
                    body.includes('No data available in table') &&
                    !body.includes('Processing...')
                );
                return hasResults || isZeroResult;
            }""",
            timeout=NYSBOE_SEARCH_TIMEOUT_MS,
        )

        # A zero-result search is a genuine "no records found" — not a
        # failure. _nysboe_extract_rows() will return [] for it.
        return _nysboe_extract_rows(playwright_page)

    except PlaywrightTimeoutError:
        raise RuntimeError(
            f"NYSBOE search for '{contributor_name}' did not complete within "
            f"{NYSBOE_SEARCH_TIMEOUT_MS / 1000:.0f}s. NYSBOE's site can be slow "
            f"or flaky — retry this donor manually at {NYSBOE_SEARCH_URL}"
        )


def _process_one_donor_nysboe(donor):
    """
    Runs all NYSBOE search query variants for ONE donor, evaluates the
    results, and saves that donor's .txt file — ALL within this one
    function call, so the file is written the moment this donor
    finishes, not after the entire batch completes.

    This mirrors fec_donor_lookup.py's pattern exactly: its
    save_donor_file() call lives inside process_donor() (the per-donor
    worker), not after the whole ThreadPoolExecutor batch finishes —
    so files appear incrementally as donors complete, not all at once
    at the end. An earlier version of this script saved files only
    after run_nysboe_searches() returned for the whole batch, which
    meant files never appeared until every donor was done, even though
    search work was already happening concurrently per-donor.

    Uses a fully independent Stealth-wrapped Playwright context owned
    entirely by the calling thread — confirmed via direct testing that
    Playwright's sync API cannot share a browser/context across threads
    (it's bound to the thread that created it; attempting to share one
    raises "Cannot switch to a different thread" greenlet errors).

    Does NOT retry on timeout — a failed query is recorded as an error
    immediately rather than retried in-place. This was a deliberate
    change: the in-loop retry doubled the worst-case cost of a failing
    search (up to ~123s: a full timeout, a 3s pause, then another full
    timeout) for donors that often fail again anyway. Letting it fail
    once and handling retries afterward via `--retry-failed` is cheaper
    overall and keeps this function's per-donor cost predictable.

    Returns a summary row dict (same shape process_donors() collects
    into nysboe_results_summary.csv), not the raw (donor, rows, error)
    tuple the old version returned — since evaluation now happens here.
    """
    last = donor["last_name"]
    first = donor["first_name"]
    fec_first_names = donor.get("fec_first_names", [])
    queries = build_nysboe_search_queries(last, fec_first_names, spreadsheet_first_name=first)

    donor_rows = []
    seen_signatures = set()
    donor_error = None

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for query in queries:
            try:
                rows = nysboe_search(page, query)
                donor_error = None  # at least one query succeeded
                for row in rows:
                    signature = tuple(row.values())
                    if signature not in seen_signatures:
                        seen_signatures.add(signature)
                        donor_rows.append(row)
            except RuntimeError as e:
                donor_error = str(e)
                print(f"    [{last}] '{query}' -> ERROR: {donor_error}", flush=True)

            time.sleep(NYSBOE_BETWEEN_SEARCHES_DELAY)

        browser.close()

    if donor_error and not donor_rows:
        print(f"  -> [{last}] ERROR (all query variant(s) failed)", flush=True)
        status = f"ERROR — {donor_error}"
        confirmed_rows, review_rows = [], []
    else:
        print(f"  -> [{last}] {len(donor_rows)} record(s) found across all variant(s)", flush=True)
        # Build candidate list: spreadsheet address always tier="street",
        # FEC addresses carry their tier tag from the lookup phase.
        spreadsheet_addr = donor.get("address", "")
        candidate_addresses = []
        if spreadsheet_addr:
            candidate_addresses.append({"street": spreadsheet_addr, "tier": "street"})
        for fec_addr in donor.get("fec_addresses", []):
            # fec_addresses are now dicts: {"street": ..., "tier": ...}
            if isinstance(fec_addr, dict) and fec_addr.get("street"):
                candidate_addresses.append(fec_addr)
            elif isinstance(fec_addr, str) and fec_addr:
                # backwards compat if plain strings somehow remain
                candidate_addresses.append({"street": fec_addr, "tier": "occupation"})
        status, confirmed_rows, review_rows = evaluate_nysboe_results(
            donor_rows, candidate_addresses, donor["city"], donor["first_name"]
        )

    # Save the file HERE, immediately, rather than after the whole
    # batch returns — this is the actual fix for files only appearing
    # at the very end.
    save_nysboe_donor_file(donor, status, confirmed_rows, review_rows, OUTPUT_FOLDER)

    return {
        "Last Name":              donor["last_name"],
        "First Name":             donor["first_name"],
        "City":                   donor["city"],
        "State":                  donor["state"],
        "FEC Consideration":      donor.get("fec_consideration", ""),
        "NY Records Confirmed":   len(confirmed_rows),
        "NY Records For Review":  len(review_rows),
        "NYSBOE Status":          status,
    }


def run_nysboe_searches(donors):
    """
    Runs NYSBOE searches for a list of donor dicts (each needs at least
    last_name, first_name, city, state, address, fec_addresses,
    fec_first_names, fec_consideration — i.e. donors that have already
    been through the FEC address-lookup phase in process_donors()),
    processing up to NYSBOE_MAX_WORKERS donors concurrently.

    Each donor is fully processed END TO END — search, evaluate, AND
    save its .txt file — inside _process_one_donor_nysboe(), so files
    appear on disk as soon as that donor finishes, not after the whole
    batch completes. This matches fec_donor_lookup.py's behavior.

    Parallelism is deliberately modest (default 3) — NYSBOE has shown
    real flakiness under sequential load already (confirmed: a 503,
    inconsistent timeouts on the same name run-to-run), so this isn't
    pushed aggressively; more workers risks triggering more failures
    than it saves in time on an already rate-sensitive site.

    Returns a list of summary row dicts, in the SAME ORDER as the input
    donors list (collected by index, not completion order, so the
    final summary CSV stays deterministic even though processing
    happens concurrently).
    """
    summary_rows = [None] * len(donors)

    with ThreadPoolExecutor(max_workers=NYSBOE_MAX_WORKERS) as executor:
        future_to_index = {
            executor.submit(_process_one_donor_nysboe, donor): i
            for i, donor in enumerate(donors)
        }
        completed = 0
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            completed += 1
            try:
                summary_rows[index] = future.result()
            except Exception as e:
                # A worker thread crashed outright (not a normal
                # RuntimeError from a failed search, but something
                # unexpected) — record it as a donor-level error AND
                # still save a file for this donor, rather than letting
                # the whole batch crash or silently skipping the file.
                donor = donors[index]
                print(f"  -> [{donor['last_name']}] UNEXPECTED ERROR: {e}", flush=True)
                status = f"ERROR — Unexpected error: {e}"
                save_nysboe_donor_file(donor, status, [], [], OUTPUT_FOLDER)
                summary_rows[index] = {
                    "Last Name":              donor["last_name"],
                    "First Name":             donor["first_name"],
                    "City":                   donor["city"],
                    "State":                  donor["state"],
                    "FEC Consideration":      donor.get("fec_consideration", ""),
                    "NY Records Confirmed":   0,
                    "NY Records For Review":  0,
                    "NYSBOE Status":          status,
                }
            print(f"[{completed}/{len(donors)}] NYSBOE search complete: "
                  f"{donors[index]['last_name']}", flush=True)

    return summary_rows


# =========================================================
# ADDRESS NORMALIZATION & MATCHING
# -------------------------------------------------------
# NYSBOE returns one free-text address string per row (e.g.
# "Apt 2h, 745 E 31st St Brooklyn NY 11210 United States"), unlike FEC's
# clean separate street/city/state fields. Real testing against live
# NYSBOE data showed the SAME donor's address formatted differently
# across records — unit number sometimes before the street, sometimes
# after, with or without an "Apt" label, "East" vs "E", "Street" vs "St".
#
# The street_key() function below was verified against all 766 real
# addresses in this project's donors.csv with ZERO false collisions
# (two different addresses producing the same key) — but it intentionally
# does NOT try to compare unit/apartment numbers, since those appeared
# in too many different positions/formats to reliably extract. Matching
# is at the building level, same as the FEC tool's street-tier matching.
# =========================================================

_RE_PUNCT       = re.compile(r'[^a-z0-9 ]')
_RE_MULTI_SPACE = re.compile(r' +')
_RE_NYSBOE_TRAILING = re.compile(
    r'\b[A-Z]{2}\s+\d{5}(-\d{4})?\s+United States\s*$', re.IGNORECASE
)
_RE_UNIT_TOKEN = re.compile(
    r'\b(apt|ste|suite|unit|fl|floor|#)\s*[\w]*\b', re.IGNORECASE
)
_STREET_ABBREVS = [
    (re.compile(r'\bstreet\b'),    'st'),
    (re.compile(r'\bavenue\b'),    'ave'),
    (re.compile(r'\bboulevard\b'), 'blvd'),
    (re.compile(r'\bdrive\b'),     'dr'),
    (re.compile(r'\broad\b'),      'rd'),
    (re.compile(r'\blane\b'),      'ln'),
    (re.compile(r'\bcourt\b'),     'ct'),
    (re.compile(r'\bplace\b'),     'pl'),
    (re.compile(r'\bcircle\b'),    'cir'),
    (re.compile(r'\bterrace\b'),   'ter'),
    (re.compile(r'\bnorth\b'),     'n'),
    (re.compile(r'\bsouth\b'),     's'),
    (re.compile(r'\beast\b'),      'e'),
    (re.compile(r'\bwest\b'),      'w'),
]
_DIRECTIONS = {"n", "s", "e", "w"}


def normalize_address(raw, is_nysboe_format=False):
    """
    Normalizes an address string for street-level comparison.

    is_nysboe_format=True strips NYSBOE's trailing "CITY STATE ZIP United
    States" boilerplate and any unit/apt token (regardless of position)
    before normalizing — needed because NYSBOE's address field mixes
    street + city + state + unit into one string with no fixed order.

    is_nysboe_format=False is for the donor spreadsheet's own Address
    column, which is just the street portion already.
    """
    if not raw:
        return ""
    s = raw
    if is_nysboe_format:
        s = _RE_NYSBOE_TRAILING.sub('', s)
        s = _RE_UNIT_TOKEN.sub('', s)
    s = s.lower().strip().strip(', ')
    s = _RE_PUNCT.sub(' ', s)
    for pattern, repl in _STREET_ABBREVS:
        s = pattern.sub(repl, s)
    return _RE_MULTI_SPACE.sub(' ', s).strip()


_RE_ORDINAL = re.compile(r'^(\d+)(st|nd|rd|th)$')


def _strip_ordinal(token):
    """'87th' -> '87', '11st' -> '11', '3rd' -> '3'. Non-ordinals pass through."""
    m = _RE_ORDINAL.match(token)
    return m.group(1) if m else token


def street_key(normalized):
    """
    Extracts a (street_number, ...) tuple identifying the building,
    independent of unit number or word order around it.

    For numbered-street addresses (e.g. "21 E 87th St"), includes the
    house number AND the numbered-street identifier ("87th"), not just
    the bare direction letter — verified necessary against this
    project's real data: direction-only kept colliding "21 E 87th" with
    "21 E 61st" (13 real collisions out of 539 unique addresses before
    this fix; zero after).

    Ordinal suffixes are stripped from street name tokens so "11th St"
    and "11st St" (a real NYSBOE data entry variant) both produce the
    same key — e.g. ('590', '11') for 590 11th/11st St.

    Returns None if no street number can be found (e.g. PO Box entries).
    """
    if not normalized:
        return None
    tokens = normalized.split()
    num = next((t for t in tokens if t.isdigit()), None)
    if not num:
        return None
    idx = tokens.index(num)
    word1 = _strip_ordinal(tokens[idx + 1]) if idx + 1 < len(tokens) else ""
    if word1 in _DIRECTIONS and idx + 2 < len(tokens):
        word2 = _strip_ordinal(tokens[idx + 2])
        return (num, word1, word2)
    return (num, word1)


_RE_NYSBOE_FULL = re.compile(
    r'^(.*?)\s+([A-Z]{2})\s+\d{5}(-\d{4})?\s+United States\s*$', re.IGNORECASE
)


def nysboe_city_matches(raw_address, donor_city):
    """
    Checks whether the donor's KNOWN city appears as a whole word in
    NYSBOE's address string, anchored to the portion before the trailing
    state/zip/country.

    This does NOT try to extract "the city" in isolation — an earlier
    version did (grabbing the last 1-2 tokens before the state) and
    produced wrong results on real data: for "278 St. James Place
    Brooklyn NY 11238 United States" it extracted "Place Brooklyn"
    instead of "Brooklyn", because the street name itself ends in a
    word ("Place") that looks like it could be part of a city name.
    Searching for the already-known city as a substring sidesteps
    needing to know where the street name ends and the city begins.

    Returns False if the address doesn't match the expected trailing
    pattern at all, or if donor_city/raw_address is empty — callers
    should treat that as "can't confirm city," not as a hard mismatch
    on its own (see city_unknown tier in match_nysboe_record).
    """
    if not raw_address or not donor_city:
        return False
    match = _RE_NYSBOE_FULL.match(raw_address.strip())
    if not match:
        return False
    before_state = match.group(1)
    pattern = r'\b' + re.escape(donor_city.strip().lower()) + r'\b'
    return bool(re.search(pattern, before_state.lower()))


def first_name_matches(nysboe_contributor_name, donor_first_name):
    """
    Checks whether the first name in a NYSBOE result row's full name
    plausibly matches the donor's first name — either exactly, or via a
    known nickname/full-name variant (e.g. donor "Bill" matches NYSBOE
    row "William Smith").

    NYSBOE's search is last-name-only (it returns every donor sharing
    that surname — confirmed during testing: searching "ARZT" returned
    Amanda Arzt, George Arzt, and a business name containing "Arzt").
    Without checking the first name too, a street-address match could
    coincidentally apply to a different person with the same surname
    living at the same building (related, or just coincidence) — this
    check catches that before it's treated as a confirmed match.

    Handles hyphenated first names (e.g. "Yves-Andre"): NYSBOE sometimes
    stores these without the hyphen ("Yves Andre Istel"), so the match
    also succeeds if the NYSBOE name's first two tokens equal the two
    parts of the hyphenated name, or if the first token matches either
    half of it.

    Returns True if nysboe_contributor_name's first name is missing/
    unparseable — that's "can't confirm," handled as a pass-through
    rather than a hard fail, since some NYSBOE rows are business
    entities ("aaaa George Arzt Communications Inc") where there's no
    clean personal first name to compare at all.
    """
    if not nysboe_contributor_name or not donor_first_name:
        return True  # can't confirm either way -- don't penalize

    tokens = nysboe_contributor_name.strip().split()
    if not tokens:
        return True

    nysboe_first = tokens[0]

    donor_variants = {v.lower() for v in get_name_variants(donor_first_name)}

    # Standard single-token check
    if nysboe_first.lower() in donor_variants:
        return True

    # Hyphenated first name handling: "Yves-Andre" may appear in NYSBOE
    # as "Yves Andre" (two tokens, no hyphen). Check if the donor's name
    # is hyphenated and the first two NYSBOE tokens match the two parts.
    if "-" in donor_first_name and len(tokens) >= 2:
        parts = donor_first_name.lower().split("-")
        if tokens[0].lower() == parts[0] and tokens[1].lower() == parts[1]:
            return True
        # Also accept if only the pre-hyphen part matches (e.g. NYSBOE
        # has just "Yves Istel" — less common but possible)
        if tokens[0].lower() == parts[0]:
            return True

    return False


def match_nysboe_record(nysboe_row, candidate_addresses, donor_city, donor_first_name=None):
    """
    Compares one NYSBOE result row against a donor's known addresses
    (spreadsheet address + any FEC-derived addresses) and returns a
    confidence tier:

      "street_match"           — building-level match found among tier-1
                                  (spreadsheet) candidates, AND first name
                                  plausibly matches. High confidence.
      "street_match_weak"      — building-level match found, but against
                                  a tier-2 (employer-confirmed) or tier-3
                                  (occupation-confirmed) FEC address rather
                                  than the donor's own spreadsheet address.
                                  Goes to confirmed but gets a ⚠ verify note.
      "city_only"              — no street match, but NYSBOE's city matches
                                  the donor's known city. Flagged for review.
      "no_match"               — neither street nor city align. Excluded.
      "city_unknown"           — NYSBOE's address didn't parse into a
                                  recognizable city. Flagged for review.

    candidate_addresses is a list of dicts: [{"street": ..., "tier": ...}, ...]
    where tier is "street" | "employer" | "occupation".
    """
    nysboe_addr_norm = normalize_address(
        nysboe_row.get("contributor_address", ""), is_nysboe_format=True
    )
    nysboe_key = street_key(nysboe_addr_norm)

    if nysboe_key is not None:
        for candidate in candidate_addresses:
            cand_street = candidate["street"] if isinstance(candidate, dict) else candidate
            cand_tier   = candidate.get("tier", "street") if isinstance(candidate, dict) else "street"
            cand_norm = normalize_address(cand_street, is_nysboe_format=False)
            if street_key(cand_norm) == nysboe_key:
                if first_name_matches(nysboe_row.get("contributor_name", ""), donor_first_name):
                    if cand_tier == "street":
                        return "street_match"
                    else:
                        return "street_match_weak"
                # Street matched but first name clearly didn't — likely
                # a different person at the same address. Don't return
                # street_match; fall through to the city-level checks
                # below instead of immediately excluding, since the
                # SAME row might still legitimately register as a
                # city-only/manual-review candidate.
                break

    raw_address = nysboe_row.get("contributor_address", "")
    match = _RE_NYSBOE_FULL.match(raw_address.strip()) if raw_address else None
    if not match:
        return "city_unknown"

    if donor_city and nysboe_city_matches(raw_address, donor_city):
        return "city_only"

    return "no_match"


# =========================================================
# PER-DONOR EVALUATION
# =========================================================

MIN_AMOUNT_NYSBOE = 1000.0  # matches FEC tool's $1,000 minimum donation floor


def _parse_amount(amount_str):
    """Parses a '$1,234.56' style string into a float. Returns None if unparseable."""
    if not amount_str:
        return None
    cleaned = amount_str.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def filter_rows_by_date(rows, min_date_str=MIN_DATE_NYSBOE):
    """
    Filters NYSBOE rows to those on or after min_date_str (MM/DD/YYYY,
    matching the FEC tool's 1/1/2016 floor).

    This replaces trying to filter via NYSBOE's own Date From field —
    confirmed via direct testing that the field's value never actually
    reaches the search request (captured the real POST body and saw
    txtDateFrom= empty every time, regardless of how the field was
    filled). Filtering here instead, against the date already present
    in each row, is simpler and was verified working immediately.

    Rows with an unparseable date are KEPT rather than dropped — an
    unexpected date format is a sign something's off with that specific
    record, not grounds to silently exclude it from consideration.
    """
    if not rows:
        return rows

    try:
        min_date = datetime.strptime(min_date_str, "%m/%d/%Y").date()
    except ValueError:
        return rows  # malformed MIN_DATE_NYSBOE constant — don't filter blindly

    filtered = []
    for row in rows:
        date_str = row.get("contribution_date", "")
        try:
            row_date = datetime.strptime(date_str, "%m/%d/%Y").date()
        except ValueError:
            filtered.append(row)  # keep — can't confirm it's out of range
            continue
        if row_date >= min_date:
            filtered.append(row)

    return filtered


def filter_rows_by_amount(rows, min_amount=MIN_AMOUNT_NYSBOE):
    """
    Filters NYSBOE rows to those with an amount >= min_amount (matches
    the FEC tool's $1,000 minimum donation floor).

    Rows with an unparseable amount are DROPPED rather than kept — unlike
    the date filter, an amount we can't read can't be confirmed to meet
    the minimum, so it's excluded rather than assumed to qualify.
    """
    if not rows:
        return rows

    filtered = []
    for row in rows:
        amount = _parse_amount(row.get("amount", ""))
        if amount is not None and amount >= min_amount:
            filtered.append(row)

    return filtered


def sort_and_cap_rows(rows, max_rows=30):
    """
    Sorts rows by amount descending and caps at max_rows — matches the
    FEC tool's own approach (top 30 contributions over $1,000, highest
    to lowest), applied here per-bucket (confirmed vs. review) so each
    is independently capped at its own top 30 by dollar amount.

    A donor with fewer than max_rows qualifying records gets all of
    them, sorted — the cap only kicks in when there are more than 30.
    """
    if not rows:
        return rows
    sorted_rows = sorted(
        rows,
        key=lambda r: _parse_amount(r.get("amount", "")) or 0,
        reverse=True,
    )
    return sorted_rows[:max_rows]


def evaluate_nysboe_results(nysboe_rows, candidate_addresses, donor_city, donor_first_name=None):
    """
    Classifies every NYSBOE row for one donor by match tier, then rolls
    that up into a status string for the summary CSV/txt file.

    Returns (status, confirmed_rows, review_rows) where:
      status:          human-readable string. Reports what was found
                       plainly — no CONSIDER/DO NOT CONSIDER judgment
                       call, since BOE results aren't meant to gate
                       inclusion the way FEC's are. MANUAL REVIEW is
                       still flagged, since that genuinely needs a human
                       to resolve an ambiguous address match.
      confirmed_rows:  rows classified "street_match" — high confidence
                       (street address AND first name both confirm).
      review_rows:     rows classified "city_only" or "city_unknown" —
                       flagged, not auto-included or auto-excluded.

    Rows are filtered to >= $1,000 and on/after 1/1/2016 before
    classification — same floors as the FEC tool.

    "no_match" rows are dropped entirely — they're presumed to be a
    different person with a similar/same name.

    A genuinely empty nysboe_rows list (no qualifying records at all)
    is the common, expected case for most donors — reported plainly,
    not flagged as a problem.
    """
    nysboe_rows = filter_rows_by_date(nysboe_rows)

    if not nysboe_rows:
        # Either NYSBOE had nothing for this name at all, or everything
        # it had was before 1/1/2016 — both are folded into the same
        # "no state giving history" message per design (a donor whose
        # only NY records predate the relevant window reads the same
        # as a donor with no NY records at all, for this purpose).
        return (
            "*No state giving history",
            [], [],
        )

    rows_before_amount_filter = nysboe_rows
    nysboe_rows = filter_rows_by_amount(nysboe_rows)

    if not nysboe_rows:
        # There WAS giving history within the date window — it just
        # never crossed the $1,000 minimum. This is a meaningfully
        # different situation from "no giving history" (the donor does
        # give to NY state campaigns, just not significantly), so it
        # gets its own distinct label rather than reading as a blank.
        return (
            "*Insignificant state giving history",
            [], [],
        )

    confirmed_rows = []
    review_rows = []
    for row in nysboe_rows:
        tier = match_nysboe_record(row, candidate_addresses, donor_city, donor_first_name)
        if tier == "street_match":
            confirmed_rows.append(row)
        elif tier == "street_match_weak":
            # Confirmed via FEC employer/occupation-inferred address rather than
            # the donor's own spreadsheet address — goes to confirmed but gets
            # a note flagging it for a quick human sanity check.
            flagged = dict(row)
            flagged["recipient"] = (
                flagged.get("recipient", "") +
                " ⚠ verify — matched via employer/occupation-confirmed address"
            )
            confirmed_rows.append(flagged)
        elif tier in ("city_only", "city_unknown"):
            review_rows.append(row)
        # "no_match" rows are dropped — different person, presumably.

    # Sort by amount descending and cap at 30 — same approach as the FEC
    # tool, applied per-bucket so confirmed and review rows are each
    # independently capped at their own top 30 by dollar amount. This
    # also means the counts in the status string below always match
    # exactly what's written to the txt file.
    confirmed_rows = sort_and_cap_rows(confirmed_rows)
    review_rows = sort_and_cap_rows(review_rows)

    if confirmed_rows:
        status = f"{len(confirmed_rows)} NY state contribution record(s) found."
        if review_rows:
            status += (
                f" ⚠ MANUAL REVIEW — additionally, {len(review_rows)} record(s) "
                f"found under this name with only city-level address agreement "
                f"(NYSBOE has no employer field to confirm further)."
            )
        return status, confirmed_rows, review_rows

    if review_rows:
        return (
            f"⚠ MANUAL REVIEW — {len(review_rows)} NY state record(s) found "
            f"under this name with city-level address agreement only (no "
            f"street-level match, and NYSBOE has no employer field to "
            f"confirm identity further). Could be this donor or a different "
            f"person with a similar name.",
            [], review_rows,
        )

    # Every row that survived both filters turned out to be a "no_match"
    # (different person, same/similar name) — none confirmed, none even
    # flagged for review. Functionally the same outcome as having no
    # qualifying rows at all for this donor.
    return (
        "*No state giving history",
        [], [],
    )


def format_nysboe_date(date_str):
    """
    Converts NYSBOE's MM/DD/YYYY date format to MM-DD-YYYY, matching
    fec_donor_lookup.py's output style exactly (its format_date()
    converts FEC's YYYY-MM-DD to the same MM-DD-YYYY target format —
    this is the NYSBOE-side equivalent, since the input format differs).
    Falls back to the original string if it doesn't parse, rather than
    silently producing something wrong.
    """
    try:
        dt = datetime.strptime(str(date_str).strip(), "%m/%d/%Y")
        return dt.strftime("%m-%d-%Y")
    except (ValueError, TypeError):
        return str(date_str)


def format_nysboe_amount(amount_str):
    """
    Converts NYSBOE's "$1,234.00" style amount string to "$1,234" (no
    decimal places), matching fec_donor_lookup.py's format_amount()
    output style exactly. Falls back to "$0" if it doesn't parse, same
    as the FEC tool's equivalent function.
    """
    parsed = _parse_amount(amount_str)
    if parsed is None:
        return "$0"
    return f"${int(parsed):,}"


def save_nysboe_donor_file(donor, status, confirmed_rows, review_rows, folder):
    """
    Saves an individual .txt file per donor, mirroring fec_donor_lookup.py's
    output style and line format (date // amount // organization) —
    including matching its MM-DD-YYYY date format and no-decimal dollar
    amount format exactly.
    """
    first = donor["first_name"]
    last = donor["last_name"]
    city = donor["city"]
    state = donor["state"]

    filename = f"nysboe_{last}_{first}.txt".replace(" ", "_")
    filepath = os.path.join(folder, filename)

    with open(filepath, "w") as f:
        f.write(f"State Giving History: {first} {last}\n")
        f.write(f"Location: {city}, {state}\n")
        f.write(f"FEC Consideration: {donor.get('fec_consideration', '')}\n")
        f.write("\n")

        if confirmed_rows:
            f.write("State Giving History (street-address confirmed):\n")
            for r in confirmed_rows:
                f.write(
                    f"{format_nysboe_date(r.get('contribution_date', ''))} // "
                    f"{format_nysboe_amount(r.get('amount', ''))} // "
                    f"{r.get('recipient', '')}\n"
                )
            f.write("\n")

        if review_rows:
            f.write("State Records Needing Manual Review (city-level match only):\n")
            for r in review_rows:
                f.write(
                    f"{format_nysboe_date(r.get('contribution_date', ''))} // "
                    f"{format_nysboe_amount(r.get('amount', ''))} // "
                    f"{r.get('recipient', '')}\n"
                )
            f.write("\n")

        if not confirmed_rows and not review_rows:
            f.write(f"{status}\n\n")

        f.write(f"STATE GIVING HISTORY STATUS:\n{status}\n")

    return filepath


def process_donors(donors):
    """
    Runs the full pipeline (FEC address lookup -> NYSBOE search ->
    evaluate -> save per-donor txt file) for a given list of donor
    dicts, and returns a list of summary row dicts (one per donor) in
    the same shape main() writes to nysboe_results_summary.csv.

    Pulled out of main() so the same processing logic can be reused for
    both a normal full run and a retry-failed-donors-only run, without
    duplicating the pipeline steps in two places.

    Per-donor .txt files are written as each donor's NYSBOE search
    completes (inside run_nysboe_searches -> _process_one_donor_nysboe),
    not batched until the very end — matching fec_donor_lookup.py's
    behavior, where files appear incrementally as the run progresses.
    """
    print(f"\nGathering FEC-known addresses for {len(donors)} donor(s) "
          f"(up to {FEC_ADDRESS_LOOKUP_WORKERS} running concurrently)...")

    def _fetch_one_donor_fec_addresses(donor):
        try:
            fec_addrs = get_fec_known_addresses(
                donor["last_name"], donor["first_name"], donor["state"],
                donor_employer=donor.get("employer", ""),
                donor_occupation=donor.get("occupation", ""),
                donor_address=donor.get("address", ""),
            )
        except RuntimeError as e:
            print(f"    WARNING — [{donor['last_name']}] FEC address lookup "
                  f"failed: {e}", flush=True)
            fec_addrs = []

        # Store addresses as dicts carrying their tier tag so the NYSBOE
        # matching side can flag weaker-signal matches for review.
        donor["fec_addresses"] = [
            {"street": a["street"], "tier": a.get("tier", "occupation")}
            for a in fec_addrs
        ]
        seen_fn = set()
        fec_first_names = []
        for a in fec_addrs:
            fn = a.get("fec_first_name", "")
            if fn and fn.lower() not in seen_fn:
                seen_fn.add(fn.lower())
                fec_first_names.append(fn)
        donor["fec_first_names"] = fec_first_names
        return donor

    completed = 0
    with ThreadPoolExecutor(max_workers=FEC_ADDRESS_LOOKUP_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_one_donor_fec_addresses, donor): donor
            for donor in donors
        }
        for future in as_completed(futures):
            completed += 1
            donor = futures[future]
            try:
                future.result()
            except Exception as e:
                # Shouldn't normally happen (get_fec_known_addresses already
                # catches its own RuntimeErrors above) — but guard against
                # any unexpected exception crashing the whole batch.
                print(f"    UNEXPECTED ERROR — [{donor['last_name']}]: {e}", flush=True)
                donor["fec_addresses"] = []
                donor["fec_first_names"] = []
            print(f"  [{completed}/{len(donors)}] FEC lookup complete: "
                  f"{donor['last_name']}, {donor['first_name']}", flush=True)

    print(f"\nRunning NYSBOE searches for {len(donors)} donor(s)...")
    print(f"(Up to {NYSBOE_MAX_WORKERS} running concurrently. Background browsers — "
          f"no windows will appear. Each donor's .txt file is saved as soon as "
          f"that donor finishes, so you'll see files appear in {OUTPUT_FOLDER}/ "
          f"progressively, not all at once at the end.)\n")
    summary_rows = run_nysboe_searches(donors)

    return summary_rows


def retry_failed_donors(summary_path=None, donors_filepath=None):
    """
    Re-runs ONLY the donors whose NYSBOE Status starts with "ERROR" in
    an existing nysboe_results_summary.csv, then merges the new results
    back in — replacing just those rows, leaving every successful donor
    from the original run untouched.

    Use this after a run where some donors timed out/errored, instead
    of re-running the entire batch from scratch (which can take hours
    for a large donor list).

    If a donor fails AGAIN on retry, their status is upgraded from
    "ERROR" to "⚠ MANUAL LOOKUP REQUIRED" — indicating the failure is
    persistent rather than a one-off transient issue, and this donor
    should be checked manually at:
    https://publicreporting.elections.ny.gov/Contributions/Contributions

    Donors already marked "⚠ MANUAL LOOKUP REQUIRED" are SKIPPED on
    future --retry-failed runs — they've already been retried once and
    failed again; retrying a third time won't help.
    """
    summary_path = summary_path or SUMMARY_PATH
    donors_filepath = donors_filepath or DONORS_FILE

    if not os.path.exists(summary_path):
        print(f"ERROR: Could not find {summary_path}. Run the main script first.")
        return

    existing_df = pd.read_csv(summary_path)
    status_col = existing_df["NYSBOE Status"].astype(str)

    # Only retry first-time failures (ERROR), skip already-escalated ones
    failed_mask = status_col.str.startswith("ERROR")
    already_escalated = status_col.str.startswith("⚠ MANUAL LOOKUP REQUIRED")
    failed_rows = existing_df[failed_mask]

    if already_escalated.any():
        print(f"Skipping {already_escalated.sum()} donor(s) already marked "
              f"'MANUAL LOOKUP REQUIRED' — these have been retried once and "
              f"won't resolve automatically. Check them manually at "
              f"https://publicreporting.elections.ny.gov/Contributions/Contributions\n")

    if failed_rows.empty:
        print(f"No ERROR rows to retry in {summary_path}.")
        return

    print(f"Found {len(failed_rows)} donor(s) with ERROR status to retry "
          f"(donors that fail again will be escalated to MANUAL LOOKUP REQUIRED).\n")

    address_lookup = load_donor_addresses(donors_filepath)

    retry_donors = []
    for _, row in failed_rows.iterrows():
        last = str(row["Last Name"]).strip()
        first = str(row["First Name"]).strip()
        city = str(row["City"]).strip()
        state = str(row["State"]).strip()

        key = (last.lower(), first.lower(), city.lower(), state.lower())
        record = address_lookup.get(key, {})
        address    = record.get("address", "")
        employer   = record.get("employer", "")
        occupation = record.get("occupation", "")

        retry_donors.append({
            "first_name":        first,
            "last_name":         last,
            "city":              city,
            "state":             state,
            "address":           address,
            "employer":          employer,
            "occupation":        occupation,
            "fec_consideration": row["FEC Consideration"],
        })

    new_summary_rows = process_donors(retry_donors)

    # Escalate any donors that failed AGAIN to MANUAL LOOKUP REQUIRED
    still_failed = 0
    for row in new_summary_rows:
        if str(row["NYSBOE Status"]).startswith("ERROR"):
            still_failed += 1
            row["NYSBOE Status"] = (
                "⚠ MANUAL LOOKUP REQUIRED — timed out on both initial run and "
                f"retry. Check manually at "
                f"https://publicreporting.elections.ny.gov/Contributions/Contributions"
            )

    # Merge: replace each retried donor's row in the existing summary
    # with its new result, by (Last Name, First Name) key. Donors not
    # in the retry set are left completely untouched.
    new_by_key = {
        (r["Last Name"], r["First Name"]): r for r in new_summary_rows
    }

    merged_rows = []
    for _, row in existing_df.iterrows():
        key = (row["Last Name"], row["First Name"])
        if key in new_by_key:
            merged_rows.append(new_by_key[key])
        else:
            merged_rows.append(row.to_dict())

    pd.DataFrame(merged_rows).to_csv(summary_path, index=False)

    succeeded = len(retry_donors) - still_failed
    print("\n" + "=" * 60)
    print(f"Retry complete. {len(retry_donors)} donor(s) retried.")
    print(f"  Now succeeded:           {succeeded}")
    print(f"  Escalated to manual:     {still_failed}")
    if still_failed:
        print(f"  -> Check these manually at:")
        print(f"     https://publicreporting.elections.ny.gov/Contributions/Contributions")
    print(f"Summary updated: {summary_path}")
    print("=" * 60)


def main(donors_file=DONORS_FILE, fec_summary_file=FEC_SUMMARY_FILE):
    if not FEC_API_KEY:
        print("ERROR: The FEC_API_KEY environment variable is not set (or is empty).")
        print("Get a free key at: https://api.data.gov/signup/")
        print("Use a SEPARATE key from your fec_donor_lookup.py key.")
        print("Then set it before running, e.g.:")
        print("    export FEC_API_KEY=your_key_here")
        print("or copy .env.example to .env, fill in your key, and load it with:")
        print("    set -a; source .env; set +a")
        return

    print("Loading CONSIDER donors from FEC results...")
    donors = load_consider_donors(fec_summary_file, donors_file)

    if not donors:
        print("No CONSIDER donors found — nothing to do.")
        return

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    summary_rows = process_donors(donors)

    summary_path = SUMMARY_PATH
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)

    print("\n" + "=" * 60)
    print(f"Done! Processed {len(donors)} CONSIDER donor(s).")
    print(f"Summary saved to:          {summary_path}")
    print(f"Individual files saved to: {OUTPUT_FOLDER}/")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check CONSIDER donors from fec_donor_lookup.py against "
                    "NYSBOE state-level giving records."
    )
    parser.add_argument(
        "--donors", default=DONORS_FILE,
        help=f"Path to the donor spreadsheet (.csv or .xlsx) — the same file "
             f"given to fec_donor_lookup.py (default: {DONORS_FILE})",
    )
    parser.add_argument(
        "--fec-summary", default=FEC_SUMMARY_FILE,
        help=f"Path to fec_donor_lookup.py's output summary CSV "
             f"(default: {FEC_SUMMARY_FILE})",
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="Re-run ONLY donors whose status is ERROR in the existing "
             "nysboe_results_summary.csv, leaving successful donors untouched.",
    )
    args = parser.parse_args()

    try:
        if args.retry_failed:
            print("Running in RETRY-FAILED mode: only re-running donors with ERROR status.\n")
            retry_failed_donors(donors_filepath=args.donors)
        else:
            main(donors_file=args.donors, fec_summary_file=args.fec_summary)
    except Exception as e:
        import traceback
        print("FATAL ERROR:", e, flush=True)
        traceback.print_exc()
