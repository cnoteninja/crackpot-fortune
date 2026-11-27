#!/usr/bin/env python3
"""Crackpot Fortune — deterministic draw fetcher.

Pulls new lottery draws from machine-readable sources, validates them, and
merges them into data/draws.json. No AI transcription involved.

Sources:
  - Mega Millions / Powerball / Millionaire for Life: NY Lottery open data
    (Socrata JSON API on data.ny.gov) — the same official datasets the
    stored history was built from.
  - Mega Millions fast lane (Nov 2026): the official site's own results
    service, GET megamillions.com/cmspages/utilservice.asmx/GetLatestDrawData
    (JSON wrapped in an XML <string> envelope, no key). It publishes the
    latest draw ~8 minutes after the 11 PM draw; Socrata lags ~a day.
    Pipeline-only — the endpoint sends no CORS headers, so the in-app
    live refresh stays on Socrata. Socrata remains the history/backfill
    source; the fast lane only ever carries the LATEST draw and is
    merged with the same keyed-union + conflict rules as every source.
    It also carries the next jackpot, which populates DATA.jackpots.
  - Pick 3/4/5, Rolling Cash 5, Classic Lotto: LotteryUSA Ohio year-archive
    pages (structured HTML draw cards), parsed deterministically.
  - Phase 2 (Nov 2026): New York state games via the same NY Lottery
    Socrata API (Numbers, Win 4, Take 5, Pick 10, NY Lotto); Pennsylvania,
    Michigan, Indiana, Kentucky and West Virginia games via the matching
    LotteryUSA state year-archive pages (same parser, same merge rules).
    Michigan Fantasy 5 / Lotto 47 cards carry a second "Double Play" ball
    box — only the "Regular" box is the game's draw.

Data path override: CF_DATA=/path/to/draws.json (used for testing).

Output: one JSON summary line on stdout.
Exit code: 0 = updated, nochange, or locked; 1 = hard error
(draws.json unreadable or unwritable — the file is untouched).
A run may carry a 'warnings' list: per-game failures that are NOT data
conflicts (request errors, unparseable pages, empty results, bad
responses) no longer abort the run — that game simply doesn't update
while every game that fetched cleanly still publishes. A genuine
CONFLICT between stored and fetched draws for the same date is
quarantined the same way: that game is skipped (neither side is
published for it) and the conflict is listed in a 'conflicts' list in
the output, so the cron worker can flag it for a human decision — but
it no longer blocks the other 164 games. Newly added draws are also
cross-checked against a second view before they are written
(usalotteryresults.com draws.json, the PA official feed, the OH
official page, bitsandodds archives as fallback); a draw a second
source disagrees with is quarantined the same way and listed in the
'crosscheck' summary (confirmed / no_verdict / contested counts plus
the quarantined draws). A run that finds another run already holding
the lock prints {"status": "locked"} and exits 0.

  - Powerball fast lane (Nov 2026): MUSL's official Winning Numbers
    API, GET api.musl.com/v3/numbers (JSON, latest draw). Key-gated:
    needs an x-api-key (MUSL_API_KEY env var); with no key configured
    the lane is a silent no-op and Socrata remains the source of
    record. Mirrors the Mega Millions fast lane (same validation,
    same conflict rules, latest draw only).

Efficiency (Nov 2026): one shared requests.Session for connection
reuse; the LotteryUSA year-page parser stops once it reaches draws
already stored (newest-first cards + a 14-day lookback, so a draw
missed by a transient failure is still picked up later); and slot
pages (midday/evening/night) are skipped when the stored record for
the latest expected draw is already complete — a skipped fetch can
never lose a draw, because a missing slot always forces a fetch.
LotteryUSA sends no ETag/Last-Modified, so conditional requests are
not applicable there (checked Nov 27, 2026).
"""
import fcntl, html, json, os, re, sys, time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).parent
DATA_PATH = Path(os.environ.get('CF_DATA', ROOT / 'data' / 'draws.json'))
UA = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9'}
TIMEOUT = 30
# Pacing + retry (Nov 2026): the Phase 3 scale-up (8 -> 165 games, ~250
# requests per run) trips LotteryUSA's bot protection when the burst hits
# from a datacenter IP — the Actions runner's refresh died in ~35s on
# fast 403s. Pace every LotteryUSA page fetch and retry each game once.
# Both are module-level constants so tests can stub them to 0.
# Nov 27: the shared requests.Session removed the per-request TLS
# handshake, so at PACE_SECONDS = 0.25 the effective rate rose to
# ~0.5 s/page and LotteryUSA started serving empty 200 pages (no draw
# cards) after ~14 rapid requests — ~90 games failed one smoke run with
# 'no draw cards parsed'. 1.0 s restores the old ~1 s+/page effective
# rate (20/20 pages clean in probe); the block clears within ~60 s.
PACE_SECONDS = 1.0
RETRY_SECONDS = 5

# Efficiency (Nov 2026): one shared session for connection reuse across
# the ~250 requests per run (a fresh TLS handshake per request was the
# old behavior). _http_get is the single choke point for all HTTP.
SESSION = requests.Session()
ET = ZoneInfo('America/New_York')
# Year-page parse cutoff: newest stored draw for the page, minus this
# many days of overlap, so a draw missed by a transient failure while
# newer draws landed is still picked up on a later run.
LOOKBACK_DAYS = 14
# Slot draw times (ET) — after this time the slot's draw for today is
# expected to exist. Only used to decide whether a slot page can
# already be current; getting a time slightly wrong costs at most one
# wasted fetch, never a lost draw (see slot_page_current).
SLOT_DRAW_TIMES = {'midday': (12, 30), 'evening': (19, 30), 'night': (23, 0)}
# Run lock (see main()). Overridable for tests via CF_LOCK.
LOCK_PATH = Path(os.environ.get('CF_LOCK', ROOT / '.fetch.lock'))


def _http_get(url, headers=None, **kwargs):
    """Single choke point for all HTTP in this module: shared session
    (connection reuse) + the fuller browser headers + timeout."""
    return SESSION.get(url, headers=headers if headers is not None else UA,
                       timeout=TIMEOUT, **kwargs)

# game -> (count, max, has_bonus, bonus_max, distinct)
# count = numbers in one draw. For Pick 10 the draw has 20 numbers even
# though a slip plays 10 (see app-template.html drawMain).
RULES = {
    'pick3': (3, 9, False, None, False),
    'pick4': (4, 9, False, None, False),
    'pick5': (5, 9, False, None, False),
    'megamillions': (5, 70, True, 24, True),
    'powerball': (5, 69, True, 26, True),
    'rollingcash5': (5, 39, False, None, True),
    'classiclotto': (6, 49, False, None, True),
    'millionaireforlife': (5, 58, True, 5, True),
    # --- Phase 2: New York (Socrata) ---
    'ny_numbers': (3, 9, False, None, False),
    'ny_win4': (4, 9, False, None, False),
    'ny_take5': (5, 39, False, None, True),
    'ny_pick10': (20, 80, False, None, True),
    # NY Lotto bonus range 1-59 verified against the full Socrata archive
    # (2,623 draws, 2001-2026: min 1, max 59).
    'ny_lotto': (6, 59, True, 59, True),
    # --- Phase 2: Pennsylvania (LotteryUSA) ---
    'pa_pick2': (2, 9, False, None, False),
    'pa_pick3': (3, 9, False, None, False),
    'pa_pick4': (4, 9, False, None, False),
    'pa_pick5': (5, 9, False, None, False),
    'pa_cash5': (5, 43, False, None, True),
    'pa_match6': (6, 49, False, None, True),
    'pa_treasurehunt': (5, 30, False, None, True),
    # --- Phase 2: Michigan (LotteryUSA) ---
    'mi_daily3': (3, 9, False, None, False),
    'mi_daily4': (4, 9, False, None, False),
    'mi_daily5': (5, 9, False, None, False),
    'mi_fantasy5': (5, 39, False, None, True),
    'mi_lotto47': (6, 47, False, None, True),
    # --- Phase 2: Indiana (LotteryUSA) ---
    'in_daily3': (3, 9, False, None, False),
    'in_daily4': (4, 9, False, None, False),
    # --- Phase 2: Kentucky (LotteryUSA) ---
    'ky_pick3': (3, 9, False, None, False),
    'ky_pick4': (4, 9, False, None, False),
    # --- Phase 2: West Virginia (LotteryUSA) ---
    'wv_daily3': (3, 9, False, None, False),
    'wv_daily4': (4, 9, False, None, False),
    'wv_cash25': (6, 25, False, None, True),
    # --- Phase 3: Arizona (LotteryUSA) ---
    'az_fantasy5': (5, 41, False, None, True),
    'az_pick3': (3, 9, False, None, False),
    'az_pick4': (4, 9, False, None, False),
    'az_thepick': (6, 40, False, None, True),
    'az_tripletwist': (6, 42, False, None, True),
    # --- Phase 3: Arkansas (LotteryUSA) ---
    'ar_cash3': (3, 9, False, None, False),
    'ar_cash4': (4, 9, False, None, False),
    'ar_lotto': (6, 40, True, 40, True),
    'ar_naturalstate': (5, 39, False, None, True),
    # --- Phase 3: California (LotteryUSA) ---
    'ca_daily3': (3, 9, False, None, False),
    'ca_daily4': (4, 9, False, None, False),
    'ca_fantasy5': (5, 39, False, None, True),
    'ca_superlotto': (5, 47, True, 27, True),
    # --- Phase 3: Colorado (LotteryUSA) ---
    'co_cash5': (5, 32, False, None, True),
    'co_lotto': (6, 40, False, None, True),
    'co_pick3': (3, 9, False, None, False),
    # --- Phase 3: Connecticut (LotteryUSA) ---
    'ct_cash5': (5, 35, False, None, True),
    'ct_classiclotto': (6, 44, False, None, True),
    'ct_play3': (3, 9, False, None, False),
    'ct_play4': (4, 9, False, None, False),
    # --- Phase 3: Delaware (LotteryUSA) ---
    'de_lotto': (6, 35, False, None, True),
    'de_play3': (3, 9, False, None, False),
    'de_play4': (4, 9, False, None, False),
    'de_play5': (5, 9, False, None, False),
    # --- Phase 3: District of Columbia (LotteryUSA) ---
    'dc_3': (3, 9, False, None, False),
    'dc_4': (4, 9, False, None, False),
    'dc_5': (5, 9, False, None, False),
    # --- Phase 3: Florida (LotteryUSA) ---
    'fl_fantasy5': (5, 36, False, None, True),
    'fl_jackpottripleplay': (6, 46, False, None, True),
    'fl_lotto': (6, 53, False, None, True),
    'fl_pick2': (2, 9, False, None, False),
    'fl_pick3': (3, 9, False, None, False),
    'fl_pick4': (4, 9, False, None, False),
    'fl_pick5': (5, 9, False, None, False),
    # --- Phase 3: Georgia (LotteryUSA) ---
    'ga_cash3': (3, 9, False, None, False),
    'ga_cash4': (4, 9, False, None, False),
    'ga_fantasy5': (5, 42, False, None, True),
    'ga_georgiafive': (5, 9, False, None, False),
    # --- Phase 3: Idaho (LotteryUSA) ---
    'id_idahocash': (5, 45, False, None, True),
    'id_pick3': (3, 9, False, None, False),
    'id_pick4': (4, 9, False, None, False),
    # --- Phase 3: Illinois (LotteryUSA) ---
    'il_lotto': (6, 50, True, 25, True),
    'il_luckydaylotto': (5, 45, False, None, True),
    'il_pick3': (3, 9, False, None, False),
    'il_pick4': (4, 9, False, None, False),
    # --- Phase 3: Iowa (LotteryUSA) ---
    'ia_pick3': (3, 9, False, None, False),
    'ia_pick4': (4, 9, False, None, False),
    # --- Phase 3: Kansas (LotteryUSA) ---
    'ks_pick3': (3, 9, False, None, False),
    'ks_superkansas': (5, 32, True, 25, True),
    # --- Phase 3: Louisiana (LotteryUSA) ---
    'la_easy5': (5, 37, False, None, True),
    'la_lotto': (6, 42, False, None, True),
    'la_pick3': (3, 9, False, None, False),
    'la_pick4': (4, 9, False, None, False),
    'la_pick5': (5, 9, False, None, False),
    # --- Phase 3: Maine (LotteryUSA) ---
    'me_pick3': (3, 9, False, None, False),
    'me_pick4': (4, 9, False, None, False),
    # --- Phase 3: Maryland (LotteryUSA) ---
    'md_bonusmatch5': (5, 39, True, 39, True),
    'md_multimatch': (6, 43, False, None, True),
    'md_pick3': (3, 9, False, None, False),
    'md_pick4': (4, 9, False, None, False),
    'md_pick5': (5, 9, False, None, False),
    # --- Phase 3: Massachusetts (LotteryUSA) ---
    'ma_mass3': (3, 9, False, None, False),
    'ma_mass4': (4, 9, False, None, False),
    'ma_masscash': (5, 35, False, None, True),
    'ma_megabucks': (6, 44, False, None, True),
    'ma_numbers': (4, 9, False, None, False),
    # --- Phase 3: Minnesota (LotteryUSA) ---
    'mn_daily3': (3, 9, False, None, False),
    'mn_gopher5': (5, 47, False, None, True),
    'mn_north5': (5, 34, False, None, True),
    # --- Phase 3: Mississippi (LotteryUSA) ---
    'ms_cash3': (3, 9, False, None, False),
    'ms_cash4': (4, 9, False, None, False),
    'ms_match5': (5, 35, False, None, True),
    # --- Phase 3: Missouri (LotteryUSA) ---
    'mo_millions': (5, 42, True, 42, True),
    'mo_pick3': (3, 9, False, None, False),
    'mo_pick4': (4, 9, False, None, False),
    'mo_showme': (5, 39, False, None, True),
    # --- Phase 3: Montana (LotteryUSA) ---
    'mt_bigskyb': (4, 31, True, 16, True),
    'mt_cash': (5, 45, False, None, True),
    # --- Phase 3: Nebraska (LotteryUSA) ---
    'ne_pick3': (3, 9, False, None, False),
    'ne_pick5': (5, 40, False, None, True),
    # --- Phase 3: New Hampshire (LotteryUSA) ---
    'nh_pick3': (3, 9, False, None, False),
    'nh_pick4': (4, 9, False, None, False),
    # --- Phase 3: New Jersey (LotteryUSA) ---
    'nj_cash5': (5, 45, False, None, True),
    'nj_pick3': (3, 9, False, None, False),
    'nj_pick4': (4, 9, False, None, False),
    'nj_pick6': (6, 46, False, None, True),
    # --- Phase 3: New Mexico (LotteryUSA) ---
    'nm_pick3plus': (3, 9, False, None, False),
    'nm_pick4plus': (4, 9, False, None, False),
    'nm_roadrunner': (5, 37, False, None, True),
    # --- Phase 3: North Carolina (LotteryUSA) ---
    'nc_cash5': (5, 43, False, None, True),
    'nc_pick3': (3, 9, False, None, False),
    'nc_pick4': (4, 9, False, None, False),
    # --- Phase 3: Oklahoma (LotteryUSA) ---
    'ok_cash5': (5, 36, False, None, True),
    'ok_pick3': (3, 9, False, None, False),
    # --- Phase 3: Oregon (LotteryUSA) ---
    'or_megabucks': (6, 48, False, None, True),
    'or_winforlife': (4, 77, False, None, True),
    # --- Phase 3: Puerto Rico (LotteryUSA) ---
    'pr_lotoplus': (5, 35, True, 10, True),
    'pr_pega2': (2, 9, False, None, False),
    'pr_pega3': (3, 9, False, None, False),
    'pr_pega4': (4, 9, False, None, False),
    # --- Phase 3: Rhode Island (LotteryUSA) ---
    'ri_numbers': (4, 9, False, None, False),
    'ri_wildmoney': (5, 38, True, 38, True),
    # --- Phase 3: South Carolina (LotteryUSA) ---
    'sc_palmetto5': (5, 42, False, None, True),
    'sc_pick3': (3, 9, False, None, False),
    'sc_pick4': (4, 9, False, None, False),
    # --- Phase 3: South Dakota (LotteryUSA) ---
    'sd_dakotacash': (5, 35, False, None, True),
    # --- Phase 3: Tennessee (LotteryUSA) ---
    'tn_cash3': (3, 9, False, None, False),
    'tn_cash4': (4, 9, False, None, False),
    'tn_dailyjackpot': (5, 38, False, None, True),
    'tn_tncash': (5, 35, True, 5, True),
    # --- Phase 3: Texas (LotteryUSA) ---
    'tx_cashfive': (5, 35, False, None, True),
    'tx_lottotexas': (6, 54, False, None, True),
    'tx_twostep': (4, 35, True, 35, True),
    # --- Phase 3: Vermont (LotteryUSA) ---
    'vt_pick3': (3, 9, False, None, False),
    'vt_pick4': (4, 9, False, None, False),
    # --- Phase 3: Virginia (LotteryUSA) ---
    'va_bankamillion': (6, 40, True, 40, True),
    'va_cash5': (5, 45, False, None, True),
    'va_pick3': (3, 9, False, None, False),
    'va_pick4': (4, 9, False, None, False),
    'va_pick5': (5, 9, False, None, False),
    # --- Phase 3: Washington (LotteryUSA) ---
    'wa_daily': (3, 9, False, None, False),
    'wa_hit5': (5, 42, False, None, True),
    'wa_lotto': (6, 49, False, None, True),
    'wa_match4': (4, 24, False, None, True),
    # --- Phase 3: Wisconsin (LotteryUSA) ---
    'wi_badger5': (5, 31, False, None, True),
    'wi_megabucks': (6, 49, False, None, True),
    'wi_pick3': (3, 9, False, None, False),
    'wi_pick4': (4, 9, False, None, False),
    'wi_supercash': (6, 39, False, None, True),
    # --- Phase 3: Wyoming (LotteryUSA) ---
    'wy_cowboydraw': (5, 45, False, None, True),
    # --- Phase 3: Shared games (LotteryUSA) ---
    'lottoamerica': (5, 52, True, 10, True),
    'tristate_gimme5': (5, 39, False, None, True),
    'tristate_megabucks': (5, 41, True, 6, True),
}

# Digit games (positional, 0-9 per position). Everything else draws 1..max.
DIGITS = {'pick3', 'pick4', 'pick5', 'ny_numbers', 'ny_win4',
          'pa_pick2', 'pa_pick3', 'pa_pick4', 'pa_pick5',
          'mi_daily3', 'mi_daily4', 'mi_daily5',
          'in_daily3', 'in_daily4', 'ky_pick3', 'ky_pick4',
          'wv_daily3', 'wv_daily4',
          'ar_cash3', 'ar_cash4', 'az_pick3', 'az_pick4', 'ca_daily3', 'ca_daily4', 'co_pick3', 'ct_play3', 'ct_play4', 'dc_3', 'dc_4', 'dc_5', 'de_play3', 'de_play4', 'de_play5', 'fl_pick2', 'fl_pick3', 'fl_pick4', 'fl_pick5', 'ga_cash3', 'ga_cash4', 'ga_georgiafive', 'ia_pick3', 'ia_pick4', 'id_pick3', 'id_pick4', 'il_pick3', 'il_pick4', 'ks_pick3', 'la_pick3', 'la_pick4', 'la_pick5', 'ma_mass3', 'ma_mass4', 'ma_numbers', 'md_pick3', 'md_pick4', 'md_pick5', 'me_pick3', 'me_pick4', 'mn_daily3', 'mo_pick3', 'mo_pick4', 'ms_cash3', 'ms_cash4', 'nc_pick3', 'nc_pick4', 'ne_pick3', 'nh_pick3', 'nh_pick4', 'nj_pick3', 'nj_pick4', 'nm_pick3plus', 'nm_pick4plus', 'ok_pick3', 'pr_pega2', 'pr_pega3', 'pr_pega4', 'ri_numbers', 'sc_pick3', 'sc_pick4', 'tn_cash3', 'tn_cash4', 'va_pick3', 'va_pick4', 'va_pick5', 'vt_pick3', 'vt_pick4', 'wa_daily', 'wi_pick3', 'wi_pick4'}

SOCRATA = {
    'megamillions': ('5xaw-6ayf', 'mega_ball'),
    'powerball': ('d6yy-54nr', None),          # bonus is the 6th number in winning_numbers
    'millionaireforlife': ('a4w9-a3tp', 'mill_ball'),
    'ny_pick10': ('bycu-cw7c', None),
    'ny_lotto': ('6nbc-h7bj', 'bonus'),
}

# Socrata datasets with one row per date and separate midday/evening
# columns. game -> (dataset, {slot: numbers_column}, parse_kind)
# parse_kind 'digits': column is a digit string like '745' / '0863'.
# parse_kind 'balls': column is space-separated numbers like '03 11 23 32 37'.
SOCRATA_SLOTS = {
    'ny_numbers': ('d3rz-5njb', {'midday': 'midday_daily', 'evening': 'evening_daily'}, 'digits'),
    'ny_win4': ('d3rz-5njb', {'midday': 'midday_win_4', 'evening': 'evening_win_4'}, 'digits'),
    'ny_take5': ('dg63-4siq', {'midday': 'midday_winning_numbers', 'evening': 'evening_winning_numbers'}, 'balls'),
}

# Seed window start for Socrata games with no stored draws yet.
# (LotteryUSA games seed from whatever the year page returns, ~50 dates.)
SEED_SINCE = {
    'ny_numbers': '2026-08-01',
    'ny_win4': '2026-08-01',
    'ny_take5': '2026-08-01',
    'ny_pick10': '2026-01-01',
    'ny_lotto': '2025-01-01',
}

LUSA = {  # game -> list of (slot, url); slot None for single-draw games
    'pick3': [('midday', 'https://www.lotteryusa.com/ohio/midday-pick-3/year'),
              ('evening', 'https://www.lotteryusa.com/ohio/pick-3/year')],
    'pick4': [('midday', 'https://www.lotteryusa.com/ohio/midday-pick-4/year'),
              ('evening', 'https://www.lotteryusa.com/ohio/pick-4/year')],
    'pick5': [('midday', 'https://www.lotteryusa.com/ohio/midday-pick-5/year'),
              ('evening', 'https://www.lotteryusa.com/ohio/pick-5/year')],
    'rollingcash5': [(None, 'https://www.lotteryusa.com/ohio/rolling-cash-5/year')],
    'classiclotto': [(None, 'https://www.lotteryusa.com/ohio/classic-lotto/year')],
    # --- Phase 2: Pennsylvania (Day = midday slot) ---
    'pa_pick2': [('midday', 'https://www.lotteryusa.com/pennsylvania/midday-pick-2/year'),
                 ('evening', 'https://www.lotteryusa.com/pennsylvania/pick-2/year')],
    'pa_pick3': [('midday', 'https://www.lotteryusa.com/pennsylvania/midday-pick-3/year'),
                 ('evening', 'https://www.lotteryusa.com/pennsylvania/pick-3/year')],
    'pa_pick4': [('midday', 'https://www.lotteryusa.com/pennsylvania/midday-pick-4/year'),
                 ('evening', 'https://www.lotteryusa.com/pennsylvania/pick-4/year')],
    'pa_pick5': [('midday', 'https://www.lotteryusa.com/pennsylvania/midday-pick-5/year'),
                 ('evening', 'https://www.lotteryusa.com/pennsylvania/pick-5/year')],
    'pa_cash5': [(None, 'https://www.lotteryusa.com/pennsylvania/cash-5/year')],
    'pa_match6': [(None, 'https://www.lotteryusa.com/pennsylvania/match-6/year')],
    'pa_treasurehunt': [(None, 'https://www.lotteryusa.com/pennsylvania/treasure-hunt/year')],
    # --- Phase 2: Michigan ---
    'mi_daily3': [('midday', 'https://www.lotteryusa.com/michigan/midday-3/year'),
                  ('evening', 'https://www.lotteryusa.com/michigan/daily-3/year')],
    'mi_daily4': [('midday', 'https://www.lotteryusa.com/michigan/midday-4/year'),
                  ('evening', 'https://www.lotteryusa.com/michigan/daily-4/year')],
    'mi_daily5': [('midday', 'https://www.lotteryusa.com/michigan/midday-5/year'),
                  ('evening', 'https://www.lotteryusa.com/michigan/daily-5/year')],
    # Fantasy 5 and Lotto 47 cards carry a second "Double Play" ball box;
    # only the "Regular" box is the game's draw (see fetch_lusa_page).
    'mi_fantasy5': [(None, 'https://www.lotteryusa.com/michigan/fantasy-5/year')],
    'mi_lotto47': [(None, 'https://www.lotteryusa.com/michigan/classic-lotto-47/year')],
    # --- Phase 2: Indiana ---
    'in_daily3': [('midday', 'https://www.lotteryusa.com/indiana/midday-3/year'),
                  ('evening', 'https://www.lotteryusa.com/indiana/daily-3/year')],
    'in_daily4': [('midday', 'https://www.lotteryusa.com/indiana/midday-4/year'),
                  ('evening', 'https://www.lotteryusa.com/indiana/daily-4/year')],
    # --- Phase 2: Kentucky ---
    'ky_pick3': [('midday', 'https://www.lotteryusa.com/kentucky/midday-pick-3/year'),
                 ('evening', 'https://www.lotteryusa.com/kentucky/pick-3/year')],
    'ky_pick4': [('midday', 'https://www.lotteryusa.com/kentucky/midday-pick-4/year'),
                 ('evening', 'https://www.lotteryusa.com/kentucky/pick-4/year')],
    # --- Phase 2: West Virginia (one draw/day, Mon-Sat / scheduled days) ---
    'wv_daily3': [(None, 'https://www.lotteryusa.com/west-virginia/daily-3/year')],
    'wv_daily4': [(None, 'https://www.lotteryusa.com/west-virginia/daily-4/year')],
    'wv_cash25': [(None, 'https://www.lotteryusa.com/west-virginia/cash-25/year')],
    # --- Phase 3: Arizona ---
    'az_fantasy5': [(None, 'https://www.lotteryusa.com/arizona/fantasy-5/year')],
    'az_pick3': [('midday', 'https://www.lotteryusa.com/arizona/pick-3-midday/year'), ('evening', 'https://www.lotteryusa.com/arizona/pick-3/year')],
    'az_pick4': [('midday', 'https://www.lotteryusa.com/arizona/pick-4-midday/year'), ('evening', 'https://www.lotteryusa.com/arizona/pick-4/year')],
    'az_thepick': [(None, 'https://www.lotteryusa.com/arizona/the-pick/year')],
    'az_tripletwist': [(None, 'https://www.lotteryusa.com/arizona/triple-twist/year')],
    # --- Phase 3: Arkansas ---
    'ar_cash3': [('midday', 'https://www.lotteryusa.com/arkansas/midday-cash-3/year'), ('evening', 'https://www.lotteryusa.com/arkansas/cash-3/year')],
    'ar_cash4': [('midday', 'https://www.lotteryusa.com/arkansas/midday-cash-4/year'), ('evening', 'https://www.lotteryusa.com/arkansas/cash-4/year')],
    'ar_lotto': [(None, 'https://www.lotteryusa.com/arkansas/lotto/year')],
    'ar_naturalstate': [(None, 'https://www.lotteryusa.com/arkansas/natural-state-jackpot/year')],
    # --- Phase 3: California ---
    'ca_daily3': [('midday', 'https://www.lotteryusa.com/california/midday-3/year'), ('evening', 'https://www.lotteryusa.com/california/daily-3/year')],
    'ca_daily4': [(None, 'https://www.lotteryusa.com/california/daily-4/year')],
    'ca_fantasy5': [(None, 'https://www.lotteryusa.com/california/fantasy-5/year')],
    'ca_superlotto': [(None, 'https://www.lotteryusa.com/california/super-lotto-plus/year')],
    # --- Phase 3: Colorado ---
    'co_cash5': [(None, 'https://www.lotteryusa.com/colorado/cash-5/year')],
    'co_lotto': [(None, 'https://www.lotteryusa.com/colorado/lotto/year')],
    'co_pick3': [('midday', 'https://www.lotteryusa.com/colorado/pick-3-midday/year'), ('evening', 'https://www.lotteryusa.com/colorado/pick-3/year')],
    # --- Phase 3: Connecticut ---
    'ct_cash5': [(None, 'https://www.lotteryusa.com/connecticut/cash-5/year')],
    'ct_classiclotto': [(None, 'https://www.lotteryusa.com/connecticut/classic-lotto/year')],
    'ct_play3': [('midday', 'https://www.lotteryusa.com/connecticut/midday-3/year'), ('evening', 'https://www.lotteryusa.com/connecticut/play-3/year')],
    'ct_play4': [('midday', 'https://www.lotteryusa.com/connecticut/midday-4/year'), ('evening', 'https://www.lotteryusa.com/connecticut/play-4/year')],
    # --- Phase 3: Delaware ---
    'de_lotto': [(None, 'https://www.lotteryusa.com/delaware/lotto/year')],
    'de_play3': [('midday', 'https://www.lotteryusa.com/delaware/play-3-midday/year'), ('evening', 'https://www.lotteryusa.com/delaware/play-3/year')],
    'de_play4': [('midday', 'https://www.lotteryusa.com/delaware/play-4-midday/year'), ('evening', 'https://www.lotteryusa.com/delaware/play-4/year')],
    'de_play5': [('midday', 'https://www.lotteryusa.com/delaware/play-5-midday/year'), ('evening', 'https://www.lotteryusa.com/delaware/play-5/year')],
    # --- Phase 3: District of Columbia ---
    'dc_3': [('midday', 'https://www.lotteryusa.com/district-of-columbia/dc-lucky-midday/year'), ('evening', 'https://www.lotteryusa.com/district-of-columbia/dc-lucky-numbers/year'), ('night', 'https://www.lotteryusa.com/district-of-columbia/dc-3-night/year')],
    'dc_4': [('midday', 'https://www.lotteryusa.com/district-of-columbia/dc-4-midday/year'), ('evening', 'https://www.lotteryusa.com/district-of-columbia/dc-4/year'), ('night', 'https://www.lotteryusa.com/district-of-columbia/dc-4-night/year')],
    'dc_5': [('midday', 'https://www.lotteryusa.com/district-of-columbia/midday-dc5/year'), ('evening', 'https://www.lotteryusa.com/district-of-columbia/dc-5/year')],
    # --- Phase 3: Florida ---
    'fl_fantasy5': [('midday', 'https://www.lotteryusa.com/florida/fantasy-5-midday/year'), ('evening', 'https://www.lotteryusa.com/florida/fantasy-5/year')],
    'fl_jackpottripleplay': [(None, 'https://www.lotteryusa.com/florida/jackpot-triple-play/year')],
    'fl_lotto': [(None, 'https://www.lotteryusa.com/florida/lotto/year')],
    'fl_pick2': [('midday', 'https://www.lotteryusa.com/florida/pick-2-midday/year'), ('evening', 'https://www.lotteryusa.com/florida/pick-2/year')],
    'fl_pick3': [('midday', 'https://www.lotteryusa.com/florida/midday-pick-3/year'), ('evening', 'https://www.lotteryusa.com/florida/pick-3/year')],
    'fl_pick4': [('midday', 'https://www.lotteryusa.com/florida/midday-pick-4/year'), ('evening', 'https://www.lotteryusa.com/florida/pick-4/year')],
    'fl_pick5': [('midday', 'https://www.lotteryusa.com/florida/pick-5-midday/year'), ('evening', 'https://www.lotteryusa.com/florida/pick-5/year')],
    # --- Phase 3: Georgia ---
    'ga_cash3': [('midday', 'https://www.lotteryusa.com/georgia/midday-3/year'), ('evening', 'https://www.lotteryusa.com/georgia/cash-3-evening/year'), ('night', 'https://www.lotteryusa.com/georgia/cash-3/year')],
    'ga_cash4': [('midday', 'https://www.lotteryusa.com/georgia/midday-4/year'), ('evening', 'https://www.lotteryusa.com/georgia/cash-4-evening/year'), ('night', 'https://www.lotteryusa.com/georgia/cash-4/year')],
    'ga_fantasy5': [(None, 'https://www.lotteryusa.com/georgia/fantasy-5/year')],
    'ga_georgiafive': [('midday', 'https://www.lotteryusa.com/georgia/midday-georgia-five/year'), ('evening', 'https://www.lotteryusa.com/georgia/georgia-five/year')],
    # --- Phase 3: Idaho ---
    'id_idahocash': [(None, 'https://www.lotteryusa.com/idaho/idaho-cash/year')],
    'id_pick3': [('midday', 'https://www.lotteryusa.com/idaho/midday-pick-3/year'), ('evening', 'https://www.lotteryusa.com/idaho/pick-3/year')],
    'id_pick4': [('midday', 'https://www.lotteryusa.com/idaho/pick-4-day/year'), ('evening', 'https://www.lotteryusa.com/idaho/pick-4/year')],
    # --- Phase 3: Illinois ---
    'il_lotto': [(None, 'https://www.lotteryusa.com/illinois/lotto/year')],
    'il_luckydaylotto': [('midday', 'https://www.lotteryusa.com/illinois/midday-lucky-day-lotto/year'), ('evening', 'https://www.lotteryusa.com/illinois/lucky-day-lotto-evening/year')],
    'il_pick3': [('midday', 'https://www.lotteryusa.com/illinois/midday-3/year'), ('evening', 'https://www.lotteryusa.com/illinois/daily-3/year')],
    'il_pick4': [('midday', 'https://www.lotteryusa.com/illinois/midday-4/year'), ('evening', 'https://www.lotteryusa.com/illinois/daily-4/year')],
    # --- Phase 3: Iowa ---
    'ia_pick3': [('midday', 'https://www.lotteryusa.com/iowa/midday-3/year'), ('evening', 'https://www.lotteryusa.com/iowa/pick-3/year')],
    'ia_pick4': [('midday', 'https://www.lotteryusa.com/iowa/midday-4/year'), ('evening', 'https://www.lotteryusa.com/iowa/pick-4/year')],
    # --- Phase 3: Kansas ---
    'ks_pick3': [('midday', 'https://www.lotteryusa.com/kansas/midday-pick-3/year'), ('evening', 'https://www.lotteryusa.com/kansas/pick-3/year')],
    'ks_superkansas': [(None, 'https://www.lotteryusa.com/kansas/super-kansas-cash/year')],
    # --- Phase 3: Louisiana ---
    'la_easy5': [(None, 'https://www.lotteryusa.com/louisiana/easy-5/year')],
    'la_lotto': [(None, 'https://www.lotteryusa.com/louisiana/lotto/year')],
    'la_pick3': [(None, 'https://www.lotteryusa.com/louisiana/pick-3/year')],
    'la_pick4': [(None, 'https://www.lotteryusa.com/louisiana/pick-4/year')],
    'la_pick5': [(None, 'https://www.lotteryusa.com/louisiana/pick-5/year')],
    # --- Phase 3: Maine ---
    'me_pick3': [('midday', 'https://www.lotteryusa.com/maine/midday-3/year'), ('evening', 'https://www.lotteryusa.com/maine/pick-3/year')],
    'me_pick4': [('midday', 'https://www.lotteryusa.com/maine/midday-4/year'), ('evening', 'https://www.lotteryusa.com/maine/pick-4/year')],
    # --- Phase 3: Maryland ---
    'md_bonusmatch5': [(None, 'https://www.lotteryusa.com/maryland/bonus-match-5/year')],
    'md_multimatch': [(None, 'https://www.lotteryusa.com/maryland/multi-match/year')],
    'md_pick3': [('midday', 'https://www.lotteryusa.com/maryland/midday-pick-3/year'), ('evening', 'https://www.lotteryusa.com/maryland/pick-3/year')],
    'md_pick4': [('midday', 'https://www.lotteryusa.com/maryland/midday-pick-4/year'), ('evening', 'https://www.lotteryusa.com/maryland/pick-4/year')],
    'md_pick5': [('midday', 'https://www.lotteryusa.com/maryland/midday-pick-5/year'), ('evening', 'https://www.lotteryusa.com/maryland/pick-5/year')],
    # --- Phase 3: Massachusetts ---
    'ma_mass3': [(None, 'https://www.lotteryusa.com/massachusetts/mass-3/year')],
    'ma_mass4': [(None, 'https://www.lotteryusa.com/massachusetts/mass-4/year')],
    'ma_masscash': [('midday', 'https://www.lotteryusa.com/massachusetts/midday-mass-cash/year'), ('evening', 'https://www.lotteryusa.com/massachusetts/mass-cash/year')],
    'ma_megabucks': [(None, 'https://www.lotteryusa.com/massachusetts/megabucks/year')],
    'ma_numbers': [('midday', 'https://www.lotteryusa.com/massachusetts/midday-numbers/year'), ('evening', 'https://www.lotteryusa.com/massachusetts/numbers/year')],
    # --- Phase 3: Minnesota ---
    'mn_daily3': [(None, 'https://www.lotteryusa.com/minnesota/pick-3/year')],
    'mn_gopher5': [(None, 'https://www.lotteryusa.com/minnesota/gopher-5/year')],
    'mn_north5': [(None, 'https://www.lotteryusa.com/minnesota/north-5/year')],
    # --- Phase 3: Mississippi ---
    'ms_cash3': [('midday', 'https://www.lotteryusa.com/mississippi/cash-3-midday/year'), ('evening', 'https://www.lotteryusa.com/mississippi/cash-3/year')],
    'ms_cash4': [('midday', 'https://www.lotteryusa.com/mississippi/cash-4-midday/year'), ('evening', 'https://www.lotteryusa.com/mississippi/cash-4/year')],
    'ms_match5': [(None, 'https://www.lotteryusa.com/mississippi/match-5/year')],
    # --- Phase 3: Missouri ---
    'mo_millions': [(None, 'https://www.lotteryusa.com/missouri/mo-millions/year')],
    'mo_pick3': [('midday', 'https://www.lotteryusa.com/missouri/midday-pick-3/year'), ('evening', 'https://www.lotteryusa.com/missouri/pick-3/year')],
    'mo_pick4': [('midday', 'https://www.lotteryusa.com/missouri/midday-pick-4/year'), ('evening', 'https://www.lotteryusa.com/missouri/pick-4/year')],
    'mo_showme': [(None, 'https://www.lotteryusa.com/missouri/show-me-cash/year')],
    # --- Phase 3: Montana ---
    'mt_bigskyb': [(None, 'https://www.lotteryusa.com/montana/big-sky-bonus/year')],
    'mt_cash': [(None, 'https://www.lotteryusa.com/montana/montana-cash/year')],
    # --- Phase 3: Nebraska ---
    'ne_pick3': [(None, 'https://www.lotteryusa.com/nebraska/pick-3/year')],
    'ne_pick5': [(None, 'https://www.lotteryusa.com/nebraska/pick-5/year')],
    # --- Phase 3: New Hampshire ---
    'nh_pick3': [('midday', 'https://www.lotteryusa.com/new-hampshire/midday-3/year'), ('evening', 'https://www.lotteryusa.com/new-hampshire/pick-3/year')],
    'nh_pick4': [('midday', 'https://www.lotteryusa.com/new-hampshire/midday-4/year'), ('evening', 'https://www.lotteryusa.com/new-hampshire/pick-4/year')],
    # --- Phase 3: New Jersey ---
    'nj_cash5': [(None, 'https://www.lotteryusa.com/new-jersey/cash-5/year')],
    'nj_pick3': [('midday', 'https://www.lotteryusa.com/new-jersey/midday-pick-3/year'), ('evening', 'https://www.lotteryusa.com/new-jersey/pick-3/year')],
    'nj_pick4': [('midday', 'https://www.lotteryusa.com/new-jersey/midday-pick-4/year'), ('evening', 'https://www.lotteryusa.com/new-jersey/pick-4/year')],
    'nj_pick6': [(None, 'https://www.lotteryusa.com/new-jersey/pick-6/year')],
    # --- Phase 3: New Mexico ---
    'nm_pick3plus': [('midday', 'https://www.lotteryusa.com/new-mexico/midday-pick-3-plus/year'), ('evening', 'https://www.lotteryusa.com/new-mexico/pick-3-plus/year')],
    'nm_pick4plus': [('midday', 'https://www.lotteryusa.com/new-mexico/midday-pick-4-plus/year'), ('evening', 'https://www.lotteryusa.com/new-mexico/pick-4-plus/year')],
    'nm_roadrunner': [(None, 'https://www.lotteryusa.com/new-mexico/road-runner-cash/year')],
    # --- Phase 3: North Carolina ---
    'nc_cash5': [(None, 'https://www.lotteryusa.com/north-carolina/cash-5/year')],
    'nc_pick3': [('midday', 'https://www.lotteryusa.com/north-carolina/midday-3/year'), ('evening', 'https://www.lotteryusa.com/north-carolina/pick-3/year')],
    'nc_pick4': [('midday', 'https://www.lotteryusa.com/north-carolina/midday-pick-4/year'), ('evening', 'https://www.lotteryusa.com/north-carolina/pick-4/year')],
    # --- Phase 3: Oklahoma ---
    'ok_cash5': [(None, 'https://www.lotteryusa.com/oklahoma/cash-5/year')],
    'ok_pick3': [(None, 'https://www.lotteryusa.com/oklahoma/pick-3/year')],
    # --- Phase 3: Oregon ---
    'or_megabucks': [(None, 'https://www.lotteryusa.com/oregon/megabucks/year')],
    'or_winforlife': [(None, 'https://www.lotteryusa.com/oregon/win-for-life/year')],
    # --- Phase 3: Puerto Rico ---
    'pr_lotoplus': [(None, 'https://www.lotteryusa.com/puerto-rico/lotto/year')],
    'pr_pega2': [('midday', 'https://www.lotteryusa.com/puerto-rico/midday-pega-2/year'), ('evening', 'https://www.lotteryusa.com/puerto-rico/pega-2/year')],
    'pr_pega3': [('midday', 'https://www.lotteryusa.com/puerto-rico/midday-pega-3/year'), ('evening', 'https://www.lotteryusa.com/puerto-rico/pega-3/year')],
    'pr_pega4': [('midday', 'https://www.lotteryusa.com/puerto-rico/midday-pega-4/year'), ('evening', 'https://www.lotteryusa.com/puerto-rico/pega-4/year')],
    # --- Phase 3: Rhode Island ---
    'ri_numbers': [('midday', 'https://www.lotteryusa.com/rhode-island/midday-numbers/year'), ('evening', 'https://www.lotteryusa.com/rhode-island/numbers/year')],
    'ri_wildmoney': [(None, 'https://www.lotteryusa.com/rhode-island/wild-money/year')],
    # --- Phase 3: South Carolina ---
    'sc_palmetto5': [(None, 'https://www.lotteryusa.com/south-carolina/palmetto-cash-5/year')],
    'sc_pick3': [('midday', 'https://www.lotteryusa.com/south-carolina/midday-pick-3/year'), ('evening', 'https://www.lotteryusa.com/south-carolina/pick-3/year')],
    'sc_pick4': [('midday', 'https://www.lotteryusa.com/south-carolina/midday-pick-4/year'), ('evening', 'https://www.lotteryusa.com/south-carolina/pick-4/year')],
    # --- Phase 3: South Dakota ---
    'sd_dakotacash': [(None, 'https://www.lotteryusa.com/south-dakota/dakota-cash/year')],
    # --- Phase 3: Tennessee ---
    'tn_cash3': [('midday', 'https://www.lotteryusa.com/tennessee/morning-cash-3/year'), ('evening', 'https://www.lotteryusa.com/tennessee/midday-cash-3/year'), ('night', 'https://www.lotteryusa.com/tennessee/cash-3/year')],
    'tn_cash4': [('midday', 'https://www.lotteryusa.com/tennessee/morning-cash-4/year'), ('evening', 'https://www.lotteryusa.com/tennessee/midday-cash-4/year'), ('night', 'https://www.lotteryusa.com/tennessee/cash-4/year')],
    'tn_dailyjackpot': [(None, 'https://www.lotteryusa.com/tennessee/daily-tennessee-jackpot/year')],
    'tn_tncash': [(None, 'https://www.lotteryusa.com/tennessee/tennessee-cash/year')],
    # --- Phase 3: Texas ---
    'tx_cashfive': [(None, 'https://www.lotteryusa.com/texas/cash-5/year')],
    'tx_lottotexas': [(None, 'https://www.lotteryusa.com/texas/lotto-texas/year')],
    'tx_twostep': [(None, 'https://www.lotteryusa.com/texas/texas-two-step/year')],
    # --- Phase 3: Vermont ---
    'vt_pick3': [('midday', 'https://www.lotteryusa.com/vermont/midday-3/year'), ('evening', 'https://www.lotteryusa.com/vermont/pick-3/year')],
    'vt_pick4': [('midday', 'https://www.lotteryusa.com/vermont/midday-4/year'), ('evening', 'https://www.lotteryusa.com/vermont/pick-4/year')],
    # --- Phase 3: Virginia ---
    'va_bankamillion': [(None, 'https://www.lotteryusa.com/virginia/bank-a-million/year')],
    'va_cash5': [(None, 'https://www.lotteryusa.com/virginia/cash-5/year')],
    'va_pick3': [('midday', 'https://www.lotteryusa.com/virginia/midday-3/year'), ('evening', 'https://www.lotteryusa.com/virginia/pick-3/year')],
    'va_pick4': [('midday', 'https://www.lotteryusa.com/virginia/midday-4/year'), ('evening', 'https://www.lotteryusa.com/virginia/pick-4/year')],
    'va_pick5': [('midday', 'https://www.lotteryusa.com/virginia/pick-5-day/year'), ('evening', 'https://www.lotteryusa.com/virginia/pick-5/year')],
    # --- Phase 3: Washington ---
    'wa_daily': [(None, 'https://www.lotteryusa.com/washington/daily/year')],
    'wa_hit5': [(None, 'https://www.lotteryusa.com/washington/hit-5/year')],
    'wa_lotto': [(None, 'https://www.lotteryusa.com/washington/lotto/year')],
    'wa_match4': [(None, 'https://www.lotteryusa.com/washington/match-4/year')],
    # --- Phase 3: Wisconsin ---
    'wi_badger5': [(None, 'https://www.lotteryusa.com/wisconsin/badger-5/year')],
    'wi_megabucks': [(None, 'https://www.lotteryusa.com/wisconsin/very-own-megabucks/year')],
    'wi_pick3': [('midday', 'https://www.lotteryusa.com/wisconsin/daily-pick-3/year'), ('evening', 'https://www.lotteryusa.com/wisconsin/daily-pick-3-evening/year')],
    'wi_pick4': [('midday', 'https://www.lotteryusa.com/wisconsin/daily-pick-4/year'), ('evening', 'https://www.lotteryusa.com/wisconsin/daily-pick-4-evening/year')],
    'wi_supercash': [(None, 'https://www.lotteryusa.com/wisconsin/super-cash/year')],
    # --- Phase 3: Wyoming ---
    'wy_cowboydraw': [(None, 'https://www.lotteryusa.com/wyoming/cowboy-draw/year')],
    # --- Phase 3: Shared games ---
    'lottoamerica': [(None, 'https://www.lotteryusa.com/lotto-america/year')],
    'tristate_gimme5': [(None, 'https://www.lotteryusa.com/maine/gimme-5/year')],
    'tristate_megabucks': [(None, 'https://www.lotteryusa.com/maine/megabucks/year')],
}

# LUSA games whose cards carry a second ball box that is NOT the draw
# (e.g. Michigan Fantasy 5 / Lotto 47 "Double Play"). Only the named
# box title is parsed for these games.
# nj_pick3: NJ Lottery's Pick-3 Green Ball Promotion (Nov 2–29, 2026)
# adds a second "Double" ball box to evening cards when the green ball
# is drawn — Nov 5, 2026 evening was Main 3-4-1 + Double 5-3-2, which
# the parser used to concatenate into a 6-number "draw" and raise a
# CONFLICT on three straight refreshes (Nov 6 20:43 / 23:43, Nov 7
# 01:43 EST). Cards without a second box fall back to whole-card
# parsing (see fetch_lusa_page), so plain evenings are unaffected.
LUSA_ONLY_BOX = {'mi_fantasy5': 'Regular', 'mi_lotto47': 'Regular',
                 'co_lotto': 'Main numbers', 'fl_lotto': 'Main Draw', 'il_lotto': 'Jackpot', 'mo_millions': 'Main', 'nc_cash5': 'Main draw', 'nj_pick6': 'Main draw',
                 'nj_pick3': 'Main'}

# Games whose "bonus" ball is the last number of the main draw, not a
# separate ball — NJ Cash 5's Bullseye is the fifth number drawn
# (verified against the official NJ Lottery rules, Nov 3, 2026).
# merge_simple folds it into the numbers list; the game is stored as a
# plain main-only draw.
LUSA_BONUS_IN_MAIN = {'nj_cash5'}

# Games with two draws per day fetched via LotteryUSA slot pages.
SLOT_GAMES = {'pick3', 'pick4', 'pick5',
              'pa_pick2', 'pa_pick3', 'pa_pick4', 'pa_pick5',
              'mi_daily3', 'mi_daily4', 'mi_daily5',
              'in_daily3', 'in_daily4', 'ky_pick3', 'ky_pick4',
              'ar_cash3', 'ar_cash4', 'az_pick3', 'az_pick4', 'ca_daily3', 'co_pick3', 'ct_play3', 'ct_play4', 'dc_3', 'dc_4', 'dc_5', 'de_play3', 'de_play4', 'de_play5', 'fl_fantasy5', 'fl_pick2', 'fl_pick3', 'fl_pick4', 'fl_pick5', 'ga_cash3', 'ga_cash4', 'ga_georgiafive', 'ia_pick3', 'ia_pick4', 'id_pick3', 'id_pick4', 'il_luckydaylotto', 'il_pick3', 'il_pick4', 'ks_pick3', 'ma_masscash', 'ma_numbers', 'md_pick3', 'md_pick4', 'md_pick5', 'me_pick3', 'me_pick4', 'mo_pick3', 'mo_pick4', 'ms_cash3', 'ms_cash4', 'nc_pick3', 'nc_pick4', 'nh_pick3', 'nh_pick4', 'nj_pick3', 'nj_pick4', 'nm_pick3plus', 'nm_pick4plus', 'pr_pega2', 'pr_pega3', 'pr_pega4', 'ri_numbers', 'sc_pick3', 'sc_pick4', 'tn_cash3', 'tn_cash4', 'va_pick3', 'va_pick4', 'va_pick5', 'vt_pick3', 'vt_pick4', 'wi_pick3', 'wi_pick4'}


def fail(msg):
    print(json.dumps({'status': 'error', 'detail': msg}))
    sys.exit(1)


def validate(game, numbers, bonus):
    count, mx, has_bonus, bonus_max, distinct = RULES[game]
    if len(numbers) != count:
        return f'{game}: expected {count} numbers, got {numbers}'
    lo = 0 if game in DIGITS else 1
    if any(not (lo <= n <= mx) for n in numbers):
        return f'{game}: number out of range in {numbers}'
    if distinct and len(set(numbers)) != len(numbers):
        return f'{game}: duplicate numbers in {numbers}'
    if has_bonus:
        if bonus is None or not (1 <= bonus <= bonus_max):
            return f'{game}: bad bonus {bonus} for {numbers}'
    elif bonus is not None:
        return f'{game}: unexpected bonus {bonus}'
    return None


def fetch_socrata(game, last_date):
    """Returns (entries, row_warnings). A malformed row is skipped with
    a warning instead of aborting the whole game — one bad upstream row
    (e.g. a garbled draw in the Socrata dataset) must not stall the
    game for weeks while newer draws pile up behind it."""
    ds, bonus_field = SOCRATA[game]
    url = (f'https://data.ny.gov/resource/{ds}.json'
           f"?$where=draw_date>'{last_date}T00:00:00'&$order=draw_date ASC&$limit=500")
    try:
        rows = _http_get(url).json()
        if not isinstance(rows, list):
            raise ValueError(f'{game}: Socrata returned non-list: {str(rows)[:120]}')
    except (ValueError, requests.RequestException):
        # Socrata occasionally returns an empty/unparseable body for $where
        # queries (seen 2026-10-23 night run). Fall back to newest-first and
        # filter in Python instead of failing the whole run.
        fb = (f'https://data.ny.gov/resource/{ds}.json'
              f'?$order=draw_date DESC&$limit=500')
        rows = _http_get(fb).json()
        if not isinstance(rows, list):
            raise ValueError(f'{game}: Socrata fallback returned non-list')
        rows = [r for r in rows if r['draw_date'][:10] > last_date]
        rows.sort(key=lambda r: r['draw_date'])
    out = []
    warnings = []
    for r in rows:
        date = (r.get('draw_date') or '')[:10] or 'unknown'
        try:
            nums = [int(x) for x in r['winning_numbers'].split()]
            if bonus_field:
                bonus = int(r[bonus_field])
            elif game == 'powerball':
                bonus = nums.pop()  # last number is the Powerball
            else:
                bonus = None
            err = validate(game, nums, bonus)
            if err:
                raise ValueError(err)
        except Exception as e:
            warnings.append(f'{game}: skipping malformed row {date}: {e}')
            continue
        entry = {'date': date, 'numbers': nums}
        if bonus is not None:
            entry['bonus'] = bonus
        out.append(entry)
    return out, warnings


# Mega Millions fast lane (Nov 2026): the official site's own results
# service publishes the latest draw ~8 minutes after the 11 PM draw;
# the NY Socrata dataset lags ~a day. Pipeline-only — the endpoint
# sends no CORS headers, so the in-app live refresh stays on Socrata.
MM_FASTLANE_URL = 'https://www.megamillions.com/cmspages/utilservice.asmx/GetLatestDrawData'

def fetch_mm_fastlane():
    """Latest Mega Millions draw + next jackpot from megamillions.com.
    Returns (draw_entry|None, jackpot_entry|None). Any failure — HTTP
    error, timeout, malformed XML/JSON, missing or invalid fields —
    returns (None, None); the Socrata path remains the source of record.
    The draw is validated with the same validate() as every other source.
    Jackpot shape matches the template's DATA.jackpots reader:
    {game, date, amount} where amount is the NEXT draw's advertised
    jackpot (NextPrizePool) and date is the latest draw's date — the draw
    that set that jackpot. The date is deliberately NOT the next draw's
    date: every newest-date computation in the codebase (build.py, this
    file's summary, the Actions workflow's commit message, the app's
    newestDate()) takes the max over all dates in draws.json, so a
    future-dated jackpot entry would misreport the data as current
    through a draw that hasn't happened yet."""
    try:
        resp = _http_get(MM_FASTLANE_URL)
        resp.raise_for_status()
        m = re.search(r'<string[^>]*>(.*?)</string>', resp.text, re.S)
        if not m:
            return None, None
        payload = json.loads(m.group(1))
        drawing = payload.get('Drawing')
        if not isinstance(drawing, dict):
            return None, None
        play_date = drawing.get('PlayDate')
        if (not isinstance(play_date, str)
                or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', play_date)):
            return None, None
        date = play_date[:10]
        # Guard against a garbled or future-dated feed response.
        if date > datetime.now(ET).strftime('%Y-%m-%d'):
            return None, None
        nums = [drawing.get(f'N{i}') for i in range(1, 6)]
        bonus = drawing.get('MBall')
        if any(not isinstance(n, int) or isinstance(n, bool) for n in nums):
            return None, None
        if not isinstance(bonus, int) or isinstance(bonus, bool):
            return None, None
        err = validate('megamillions', nums, bonus)
        if err:
            return None, None
        draw = {'date': date, 'numbers': sorted(nums), 'bonus': bonus}
        jackpot = None
        jackpot_obj = payload.get('Jackpot')
        if isinstance(jackpot_obj, dict):
            amount = jackpot_obj.get('NextPrizePool')
            if isinstance(amount, (int, float)) and not isinstance(amount, bool) and amount > 0:
                jackpot = {'game': 'megamillions', 'date': date, 'amount': int(amount)}
        return draw, jackpot
    except Exception:
        return None, None


def apply_mm_fastlane(data, added, added_draws=None):
    """Merge the fast lane into data (in place). Returns
    (jackpot_changed, error). error is non-None only on a genuine draw
    conflict with stored data (hard error); every other fast-lane
    failure is swallowed here and returns (False, None)."""
    try:
        mm_draw, mm_jackpot = fetch_mm_fastlane()
    except Exception:
        return False, None
    jackpot_changed = False
    if mm_draw is not None:
        stored_mm = data.get('megamillions', [])
        latest_stored = stored_mm[-1]['date'] if stored_mm else ''
        try:
            merged, n = merge_simple('megamillions', stored_mm,
                                     {mm_draw['date']: {'numbers': mm_draw['numbers'],
                                                       'bonus': mm_draw.get('bonus')}})
        except ValueError as e:
            return False, str(e)
        if n:
            data['megamillions'] = merged
            added['megamillions'] = added.get('megamillions', 0) + n
            if added_draws is not None:
                added_draws.append({'game': 'megamillions',
                                    'date': mm_draw['date'], 'slot': None,
                                    'numbers': list(mm_draw['numbers']),
                                    'bonus': mm_draw.get('bonus')})
        # Jackpot: only trust the feed when it is at least as fresh as
        # our stored draws, so a stale feed can never overwrite a newer
        # jackpot entry.
        if mm_jackpot is not None and mm_draw['date'] >= latest_stored:
            current = [j for j in data.get('jackpots', []) if j.get('game') == 'megamillions']
            if not current or current[0] != mm_jackpot:
                data['jackpots'] = [j for j in data.get('jackpots', []) if j.get('game') != 'megamillions']
                data['jackpots'].append(mm_jackpot)
                jackpot_changed = True
    return jackpot_changed, None


# Powerball fast lane (Nov 2026): MUSL's official Winning Numbers API.
# Key-gated: needs an x-api-key (MUSL_API_KEY env var). As of Nov 2026
# there is no self-serve key portal — a key must be requested from MUSL
# directly, so until one exists this lane is a silent no-op and the NY
# Socrata feed remains the source of record. Docs:
# https://api.musl.com/numbers-api
PB_FASTLANE_URL = 'https://api.musl.com/v3/numbers'

def fetch_powerball_fastlane():
    """Latest Powerball draw from api.musl.com. Returns (draw|None, None)
    — no jackpot (the nextDrawing jackpot field is unverified). Any
    failure — no key, HTTP error, malformed JSON, invalid numbers —
    returns (None, None); the Socrata path remains the source of record.
    The draw is validated with the same validate() as every other source.
    Response shape (per MUSL docs): {"drawDate": "2026-02-16",
    "numbers": [{"ruleCode": "white-balls", "value": "16", ...} x5,
    {"ruleCode": "powerball", "value": "06", ...}, ...]}."""
    key = os.environ.get('MUSL_API_KEY')
    if not key:
        return None, None
    try:
        resp = _http_get(PB_FASTLANE_URL,
                         headers={**UA, 'Accept': 'application/json',
                                  'x-api-key': key})
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            return None, None
        date = payload.get('drawDate')
        if not isinstance(date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date):
            return None, None
        # Guard against a garbled or future-dated feed response.
        if date > datetime.now(ET).strftime('%Y-%m-%d'):
            return None, None
        numbers = payload.get('numbers')
        if not isinstance(numbers, list):
            return None, None
        nums = []
        bonus = None
        for entry in numbers:
            if not isinstance(entry, dict):
                continue
            try:
                v = int(entry.get('value'))
            except (TypeError, ValueError):
                return None, None
            if entry.get('ruleCode') == 'white-balls':
                nums.append(v)
            elif entry.get('ruleCode') == 'powerball':
                bonus = v
        if len(nums) != 5 or bonus is None:
            return None, None
        err = validate('powerball', sorted(nums), bonus)
        if err:
            return None, None
        return {'date': date, 'numbers': sorted(nums), 'bonus': bonus}, None
    except Exception:
        return None, None


def apply_powerball_fastlane(data, added, added_draws=None):
    """Merge the Powerball fast lane into data (in place). Returns an
    error string or None; only a genuine draw conflict is an error,
    every other fast-lane failure is swallowed here. Mirrors
    apply_mm_fastlane without the jackpot."""
    try:
        pb_draw, _ = fetch_powerball_fastlane()
    except Exception:
        return None
    if pb_draw is None:
        return None
    stored_pb = data.get('powerball', [])
    try:
        merged, n = merge_simple('powerball', stored_pb,
                                 {pb_draw['date']: {'numbers': pb_draw['numbers'],
                                                    'bonus': pb_draw.get('bonus')}})
    except ValueError as e:
        return str(e)
    if n:
        data['powerball'] = merged
        added['powerball'] = added.get('powerball', 0) + n
        if added_draws is not None:
            added_draws.append({'game': 'powerball',
                                'date': pb_draw['date'], 'slot': None,
                                'numbers': list(pb_draw['numbers']),
                                'bonus': pb_draw.get('bonus')})
    return None


def fetch_socrata_slots(game, since):
    """Socrata datasets with one row per date and separate midday/evening
    columns (NY Numbers / Win 4 / Take 5). Returns (slot_maps, warnings)
    like the LotteryUSA pick games: {'midday': {date: nums}, 'evening':
    {date: nums}}. A malformed row is skipped with a warning instead of
    aborting the whole game — one bad upstream row must not stall the
    game for weeks while newer draws pile up behind it."""
    dataset, columns, kind = SOCRATA_SLOTS[game]
    url = (f'https://data.ny.gov/resource/{dataset}.json'
           f"?$where=draw_date>'{since}T00:00:00'&$order=draw_date ASC&$limit=500")
    rows = _http_get(url).json()
    if not isinstance(rows, list):
        raise ValueError(f'{game}: Socrata returned non-list: {str(rows)[:120]}')
    maps = {'midday': {}, 'evening': {}}
    warnings = []
    for r in rows:
        date = (r.get('draw_date') or '')[:10] or 'unknown'
        for slot, col in columns.items():
            raw = (r.get(col) or '').strip()
            if not raw:
                continue
            try:
                if kind == 'digits':
                    nums = [int(c) for c in raw if c.isdigit()]
                else:
                    nums = [int(x) for x in raw.split()]
                err = validate(game, nums, None)
                if err:
                    raise ValueError(err)
            except Exception as e:
                warnings.append(f'{game}: skipping malformed row {date} {slot}: {e}')
                continue
            maps[slot][date] = nums
    return maps, warnings


def fetch_lusa_page(url, only_box=None, cutoff=None):
    """Parse one LotteryUSA year page ->
    {date: {'numbers': [...], 'bonus': n|None}} (newest first in page).
    A bonus ball (e.g. Texas Two Step's Bonus Ball, WA Lotto's bonus) renders
    as <li class="c-result__bonus"> with the number in a nested span, so it
    is captured separately from the main balls. Digit games' add-on balls
    (Fireball / Wild Ball) render the same way and are ignored by the digit
    merge path (the digit model has no bonus). only_box: if set, take balls
    only from the ball box with that title (e.g. Michigan Fantasy 5 /
    Lotto 47 'Regular' vs 'Double Play'; Florida Lotto 'Main Draw').
    Cards that carry no box with that title fall back to all balls in the
    card — LotteryUSA only renders titled ball boxes on cards that have a
    second box, so a plain card has no box to match (NJ Pick 3, Nov 2026:
    only Green Ball Double Draw evenings carry a 'Main' + 'Double' pair).
    cutoff: a 'YYYY-MM-DD' date; cards are newest-first, so parsing stops
    at the first card on or before the cutoff — everything below it is
    already stored (or older than the lookback window). Stopping early
    returns whatever newer cards were parsed, possibly {} (nothing new);
    the 'no draw cards parsed' error is raised only when the page yields
    no usable cards at all and parsing never stopped early."""
    # Pace the burst: this is the single choke point for all ~241
    # LotteryUSA page fetches per run. Hitting them in rapid succession
    # from one IP trips the source's bot protection (fast 403s).
    time.sleep(PACE_SECONDS)
    html = _http_get(url).text
    cards = re.split(r'c-results-table__item', html)[1:]
    found = {}
    stopped_early = False
    for card in cards:
        dm = re.search(r'c-draw-card__draw-date-sub">\s*([A-Z][a-z]{2} \d{1,2}, \d{4})', card)
        if not dm:
            continue
        date = datetime.strptime(dm.group(1), '%b %d, %Y').strftime('%Y-%m-%d')
        if cutoff is not None and date <= cutoff:
            stopped_early = True
            break
        scope = card
        if only_box:
            balls = []
            for part in re.split(r'c-draw-card__ball-box', card)[1:]:
                tm = re.search(r'c-draw-card__ball-title">\s*([^<]+?)\s*</p>', part)
                if tm and tm.group(1).strip() == only_box:
                    balls = [int(x) for x in re.findall(r'<li class="c-ball[^"]*">\s*(\d+)\s*</li>', part)]
                    scope = part
                    break
            if not balls:
                # No titled box on this card (the usual single-draw card):
                # parse the whole card as before.
                balls = [int(x) for x in re.findall(r'<li class="c-ball[^"]*">\s*(\d+)\s*</li>', card)]
        else:
            balls = [int(x) for x in re.findall(r'<li class="c-ball[^"]*">\s*(\d+)\s*</li>', card)]
        bonus = None
        bm = re.search(r'<li class="c-result__bonus">.*?<span class="c-ball[^"]*">\s*(\d+)\s*</span>', scope, re.S)
        if bm:
            bonus = int(bm.group(1))
        if balls:
            found[date] = {'numbers': balls, 'bonus': bonus}
    if not found and not stopped_early:
        raise ValueError(f'no draw cards parsed from {url}')
    return found


def merge_simple(game, stored, fetched_by_date, added_draws=None):
    """Non-pick games: keyed union by date within the stored window
    (no backfill earlier than the first stored draw), sorted ascending.
    fetched_by_date values are {'numbers': [...], 'bonus': n|None}.
    added_draws (when given) collects the newly added draws as stored
    ({'date', 'numbers', 'bonus'[, 'slot']}) for the cross-check."""
    first = stored[0]['date'] if stored else '0000-00-00'
    by_date = {d['date']: d for d in stored}
    added = 0
    for date, entry in fetched_by_date.items():
        if date < first:
            continue
        nums, bonus = entry['numbers'], entry.get('bonus')
        if game in DIGITS:
            # Digit games have no bonus in the model (add-on balls such as
            # Fireball, Wicked Bonus, Wild Ball are ignored — same as
            # merge_picks). MA Mass 3/4 cards carry a Wicked Bonus digit.
            bonus = None
        if game in LUSA_BONUS_IN_MAIN and bonus is not None:
            # The "bonus" is the last number of the main draw, not a
            # separate ball (NJ Cash 5's Bullseye is the fifth number
            # drawn). Fold it into the numbers list.
            nums = nums + [bonus]
            bonus = None
        if date not in by_date:
            err = validate(game, nums, bonus)
            if err:
                raise ValueError(err + f' (date {date})')
            e = {'date': date, 'numbers': nums}
            if bonus is not None:
                e['bonus'] = bonus
            by_date[date] = e
            added += 1
            if added_draws is not None:
                d = {'game': game, 'date': date, 'slot': None,
                     'numbers': list(nums), 'bonus': bonus}
                added_draws.append(d)
        elif by_date[date]['numbers'] != nums or by_date[date].get('bonus') != bonus:
            raise ValueError(f'{game}: CONFLICT on {date}: stored {by_date[date]} vs fetched {entry}')
    return sorted(by_date.values(), key=lambda d: d['date']), added


# Slot order within a date (Phase 3: DC-3/DC-4 and GA Cash 3/Cash 4 draw
# three times a day — midday, evening, night). Stored entries are always
# in time order, so the n-th entry of a date is the n-th slot.
SLOT_ORDER = {'midday': 0, 'evening': 1, 'night': 2}
_SLOT_BY_INDEX = ('midday', 'evening', 'night')


def _counts_by_date(stored):
    from collections import Counter
    return Counter(e['date'] for e in stored)


def slot_page_current(game, slot, stored, now_et):
    """True when the latest expected draw for this slot is already stored,
    so fetching the page cannot add anything. Conservative by design:
    stored entries are positional within a date (see merge_picks), so one
    entry alone can't be trusted to be a particular slot — the page is
    skipped only when the stored record for the latest expected date is
    COMPLETE (as many entries as slots that have happened by now). A
    missing slot therefore always forces a fetch, and a skipped fetch can
    never lose a draw."""
    n_slots = len(LUSA[game])
    today = now_et.date()
    h, m = SLOT_DRAW_TIMES[slot]
    if (now_et.hour, now_et.minute) >= (h, m):
        latest = today
        passed = sum(1 for s, _ in LUSA[game]
                     if SLOT_DRAW_TIMES[s] <= (now_et.hour, now_et.minute))
    else:
        latest = today - timedelta(days=1)
        passed = n_slots
    counts = _counts_by_date(stored)
    return counts.get(latest.isoformat(), 0) >= passed


def _slot_newest(stored):
    """Newest stored date per slot (positional mapping, same as
    merge_picks). Used only to place the parse cutoff; the lookback
    window absorbs any one-slot mislabeling."""
    from collections import Counter
    seen = Counter()
    newest = {}
    for entry in stored:
        idx = seen[entry['date']]
        slot = _SLOT_BY_INDEX[idx] if idx < len(_SLOT_BY_INDEX) else 'evening'
        seen[entry['date']] += 1
        if slot not in newest or entry['date'] > newest[slot]:
            newest[slot] = entry['date']
    return newest


def _cutoff_for(newest_date):
    """Parse cutoff for a page: newest stored draw for it, minus the
    lookback window (covers a draw missed by a transient failure while
    newer draws landed). None when nothing is stored yet (seed: parse
    the whole page)."""
    if not newest_date:
        return None
    d = datetime.strptime(newest_date, '%Y-%m-%d').date() - timedelta(days=LOOKBACK_DAYS)
    return d.isoformat()


def merge_picks(game, stored, slot_maps, added_draws=None):
    """Pick games: stored order within a date is midday, evening, then
    night (only games with three draws a day use the night slot).
    slot_maps: {'midday': {date: digits}, 'evening': {...}, 'night': {...}}
    An empty stored list seeds from whatever the source returns.
    added_draws (when given) collects the newly added draws as stored
    ({'game', 'date', 'slot', 'numbers', 'bonus': None})."""
    first = stored[0]['date'] if stored else '0000-00-00'
    by_key = {}
    from collections import Counter
    seen = Counter()
    for entry in stored:
        idx = seen[entry['date']]
        slot = _SLOT_BY_INDEX[idx] if idx < len(_SLOT_BY_INDEX) else 'evening'
        seen[entry['date']] += 1
        by_key[(entry['date'], slot)] = entry['numbers']
    added = 0
    for slot, fmap in slot_maps.items():
        for date, entry in fmap.items():
            # LotteryUSA slot maps carry {'numbers': [...], 'bonus': ...};
            # Socrata slot maps carry plain number lists. Digit games have
            # no bonus in the model either way (add-on balls ignored).
            nums = entry['numbers'] if isinstance(entry, dict) else entry
            if date < first:
                continue
            key = (date, slot)
            if key not in by_key:
                err = validate(game, nums, None)
                if err:
                    raise ValueError(err + f' (date {date} {slot})')
                by_key[key] = nums
                added += 1
                if added_draws is not None:
                    added_draws.append({'game': game, 'date': date,
                                        'slot': slot, 'numbers': list(nums),
                                        'bonus': None})
            elif by_key[key] != nums:
                raise ValueError(f'{game}: CONFLICT {date} {slot}: stored {by_key[key]} vs fetched {nums}')
    merged = [{'date': d, 'numbers': by_key[(d, s)]}
              for (d, s) in sorted(by_key.keys(), key=lambda k: (k[0], SLOT_ORDER.get(k[1], 1)))]
    return merged, added


def _is_retryable(err):
    """Whether a per-game failure is worth one retry. A merge conflict is
    a genuine data disagreement, not a transient failure — retrying it
    is pointless, so conflicts fail immediately. Everything in the
    transient set (request errors, parse failures, empty results,
    validation errors from a bad response) gets one retry."""
    if isinstance(err, requests.RequestException):
        return True
    return isinstance(err, ValueError) and 'CONFLICT' not in str(err)


def fetch_game(game, data, warnings=None, now_et=None, added_draws=None):
    """Fetch and merge one game in place; returns the number of draws
    added. Raises on any failure. Safe to retry: a failed attempt never
    mutates data (merges only assign after they succeed). warnings: a
    list the caller supplies; per-row non-fatal notes are appended to
    it. now_et: injectable clock (America/New_York) for the
    schedule-aware slot skip; defaults to now. added_draws: when given,
    collects the newly added draws (with slot) for the cross-check."""
    if warnings is None:
        warnings = []
    if now_et is None:
        now_et = datetime.now(ET)
    stored = data.get(game, [])
    if game in SOCRATA:
        since = stored[-1]['date'] if stored else SEED_SINCE.get(game, '2020-01-01')
        new, row_warnings = fetch_socrata(game, since)
        warnings.extend(row_warnings)
        if new:
            if stored:
                for e in new:
                    if e['date'] <= stored[-1]['date']:
                        raise ValueError(f'{game}: Socrata returned non-newer draw {e}')
            stored.extend(new)
            data[game] = stored
            if added_draws is not None:
                for e in new:
                    added_draws.append({'game': game, 'date': e['date'],
                                        'slot': None,
                                        'numbers': list(e['numbers']),
                                        'bonus': e.get('bonus')})
            return len(new)
    elif game in SOCRATA_SLOTS:
        # Fetch since the newest stored date (minus one day of overlap,
        # so a missing slot on the newest date is not skipped over).
        # Previously this re-fetched from the FIRST stored date, which
        # re-validated months of history on every run — and re-hit the
        # same bad upstream row, stalling the game (NY Numbers / Win 4,
        # Nov 2026).
        if stored:
            newest = datetime.strptime(stored[-1]['date'], '%Y-%m-%d').date()
            since = (newest - timedelta(days=1)).isoformat()
        else:
            since = SEED_SINCE.get(game, '2020-01-01')
        slot_maps, row_warnings = fetch_socrata_slots(game, since)
        warnings.extend(row_warnings)
        merged, n = merge_picks(game, stored, slot_maps, added_draws)
        if n:
            data[game] = merged
        return n
    elif game in SLOT_GAMES:
        slot_maps = {}
        newest_by_slot = _slot_newest(stored)
        for slot, url in LUSA[game]:
            if slot_page_current(game, slot, stored, now_et):
                continue
            slot_maps[slot] = fetch_lusa_page(
                url, LUSA_ONLY_BOX.get(game),
                cutoff=_cutoff_for(newest_by_slot.get(slot)))
        merged, n = merge_picks(game, stored, slot_maps, added_draws)
        if n:
            data[game] = merged
        return n
    else:
        cutoff = _cutoff_for(stored[-1]['date'] if stored else None)
        fmap = fetch_lusa_page(LUSA[game][0][1], LUSA_ONLY_BOX.get(game),
                               cutoff=cutoff)
        merged, n = merge_simple(game, stored, fmap, added_draws)
        if n:
            data[game] = merged
        return n
    return 0


def fetch_game_with_retry(game, data, warnings=None, now_et=None, added_draws=None):
    """One attempt, then one retry after RETRY_SECONDS for transient
    failures. Returns (added_count, error_str_or_None). Warnings
    collected by the first attempt are rolled back before the retry so
    they are not reported twice; added_draws collected by a failed
    attempt are rolled back the same way (a failed merge never mutates
    data, so its draws must not reach the cross-check)."""
    if warnings is None:
        warnings = []
    mark = len(warnings)
    dmark = len(added_draws) if added_draws is not None else 0
    try:
        return fetch_game(game, data, warnings, now_et, added_draws), None
    except Exception as e:
        if not _is_retryable(e):
            if added_draws is not None:
                del added_draws[dmark:]
            return 0, str(e)
        del warnings[mark:]
        if added_draws is not None:
            del added_draws[dmark:]
        time.sleep(RETRY_SECONDS)
        try:
            return fetch_game(game, data, warnings, now_et, added_draws), None
        except Exception as e2:
            if added_draws is not None:
                del added_draws[dmark:]
            return 0, str(e2)


XCHECK_GAMEIDS = {
    'ar_cash3': {'evening': 'ar-cash-3-evening', 'midday': 'ar-cash-3-midday'},
    'ar_cash4': {'evening': 'ar-cash-4-evening', 'midday': 'ar-cash-4-midday'},
    'ar_lotto': {None: 'ar-natural-state-jackpot'},
    'ar_naturalstate': {None: 'ar-natural-state-jackpot'},
    'az_fantasy5': {None: 'az-fantasy-5'},
    'az_thepick': {None: 'az-the-pick'},
    'az_tripletwist': {None: 'az-triple-twist'},
    'ca_daily3': {'evening': 'ca-daily-3-evening', 'midday': 'ca-daily-3-midday'},
    'ca_daily4': {None: 'ca-daily-4'},
    'ca_fantasy5': {None: 'ca-fantasy-5'},
    'ca_superlotto': {None: 'ca-superlotto-plus'},
    'classiclotto': {None: 'oh-classic-lotto'},
    'co_cash5': {None: 'co-cash-5'},
    'co_lotto': {None: 'co-lotto'},
    'co_pick3': {'evening': 'co-pick-3-evening', 'midday': 'co-pick-3-midday'},
    'ct_cash5': {None: 'ct-cash-5'},
    'ct_classiclotto': {None: 'ct-lotto'},
    'ct_play3': {'evening': 'ct-play3-night', 'midday': 'ct-play3-day'},
    'ct_play4': {'evening': 'ct-play4-night', 'midday': 'ct-play4-day'},
    'dc_3': {'evening': 'dc-dc-3-evening', 'midday': 'dc-dc-3-midday'},
    'dc_4': {'evening': 'dc-dc-4-evening', 'midday': 'dc-dc-4-midday'},
    'dc_5': {'evening': 'dc-dc-5-evening', 'midday': 'dc-dc-5-midday'},
    'de_play3': {'evening': 'de-play-3-night', 'midday': 'de-play-3-day'},
    'de_play4': {'evening': 'de-play-4-night', 'midday': 'de-play-4-day'},
    'fl_fantasy5': {'evening': 'fl-fantasy-5-evening', 'midday': 'fl-fantasy-5-midday'},
    'fl_jackpottripleplay': {None: 'fl-jackpot-triple-play'},
    'fl_lotto': {None: 'fl-lotto'},
    'fl_pick2': {'evening': 'fl-pick-2-evening', 'midday': 'fl-pick-2-midday'},
    'fl_pick3': {'evening': 'fl-pick-3-evening', 'midday': 'fl-pick-3-midday'},
    'fl_pick4': {'evening': 'fl-pick-4-evening', 'midday': 'fl-pick-4-midday'},
    'fl_pick5': {'evening': 'fl-pick-5-evening', 'midday': 'fl-pick-5-midday'},
    'ga_cash3': {'evening': 'ga-cash-3-evening', 'midday': 'ga-cash-3-midday', 'night': 'ga-cash-3-night'},
    'ga_cash4': {'evening': 'ga-cash-4-evening', 'midday': 'ga-cash-4-midday', 'night': 'ga-cash-4-night'},
    'ga_fantasy5': {None: 'ga-fantasy-5'},
    'ga_georgiafive': {'evening': 'ga-georgia-five-evening', 'midday': 'ga-georgia-five-midday'},
    'ia_pick3': {'evening': 'ia-pick-3-evening', 'midday': 'ia-pick-3-midday'},
    'ia_pick4': {'evening': 'ia-pick-4-evening', 'midday': 'ia-pick-4-midday'},
    'id_idahocash': {None: 'id-idaho-cash'},
    'id_pick3': {'evening': 'id-pick-3-night', 'midday': 'id-pick-3-day'},
    'id_pick4': {'evening': 'id-pick-4-night', 'midday': 'id-pick-4-day'},
    'il_lotto': {None: 'il-lotto'},
    'il_luckydaylotto': {'evening': 'il-luckyday-lotto-evening', 'midday': 'il-luckyday-lotto-midday'},
    'il_pick3': {'evening': 'il-pick-3-evening', 'midday': 'il-pick-3-midday'},
    'il_pick4': {'evening': 'il-pick-4-evening', 'midday': 'il-pick-4-midday'},
    'in_daily3': {'evening': 'in-daily-3-evening', 'midday': 'in-daily-3-midday'},
    'in_daily4': {'evening': 'in-daily-4-evening', 'midday': 'in-daily-4-midday'},
    'ks_pick3': {'evening': 'ks-pick-3-evening', 'midday': 'ks-pick-3-midday'},
    'ks_superkansas': {None: 'ks-super-cash'},
    'ky_pick3': {'evening': 'ky-pick-3-evening', 'midday': 'ky-pick-3-midday'},
    'ky_pick4': {'evening': 'ky-pick-4-evening', 'midday': 'ky-pick-4-midday'},
    'la_easy5': {None: 'la-easy-5'},
    'la_lotto': {None: 'la-lotto'},
    'la_pick3': {None: 'la-pick-3'},
    'la_pick4': {None: 'la-pick-4'},
    'la_pick5': {None: 'la-pick-5'},
    'lottoamerica': {None: 'lotto-america'},
    'ma_megabucks': {None: 'ma-megabucks-doubler'},
    'ma_numbers': {'evening': 'ma-numbers-evening', 'midday': 'ma-numbers-midday'},
    'md_bonusmatch5': {None: 'md-bonus-match-5'},
    'md_multimatch': {None: 'md-multimatch'},
    'md_pick3': {'evening': 'md-pick-3-evening', 'midday': 'md-pick-3-midday'},
    'md_pick4': {'evening': 'md-pick-4-evening', 'midday': 'md-pick-4-midday'},
    'md_pick5': {'evening': 'md-pick-5-evening', 'midday': 'md-pick-5-midday'},
    'me_pick3': {'evening': 'me-pick-3-evening', 'midday': 'me-pick-3-day'},
    'me_pick4': {'evening': 'me-pick-4-evening', 'midday': 'me-pick-4-day'},
    'megamillions': {None: 'mega-millions'},
    'mi_daily3': {'evening': 'mi-daily-3-evening', 'midday': 'mi-daily-3-midday'},
    'mi_daily4': {'evening': 'mi-daily-4-evening', 'midday': 'mi-daily-4-midday'},
    'mi_fantasy5': {None: 'mi-fantasy-5'},
    'mi_lotto47': {None: 'mi-classic-lotto-regular'},
    'mn_daily3': {None: 'mn-daily-3'},
    'mn_gopher5': {None: 'mn-gopher-5'},
    'mn_north5': {None: 'mn-northstar-cash'},
    'mo_pick3': {'evening': 'mo-pick-3-evening', 'midday': 'mo-pick-3-midday'},
    'mo_pick4': {'evening': 'mo-pick-4-evening'},
    'mo_showme': {None: 'mo-show-me-cash'},
    'ms_cash3': {'evening': 'ms-cash-3-evening', 'midday': 'ms-cash-3-midday'},
    'ms_cash4': {'evening': 'ms-cash-4-evening', 'midday': 'ms-cash-4-midday'},
    'ms_match5': {None: 'ms-match-5'},
    'mt_bigskyb': {None: 'mt-big-sky-bonus'},
    'mt_cash': {None: 'mt-montana-cash'},
    'nc_cash5': {None: 'nc-cash-5'},
    'nc_pick3': {'evening': 'nc-pick-3-evening', 'midday': 'nc-pick-3-day-midday'},
    'nc_pick4': {'evening': 'nc-pick-4-evening', 'midday': 'nc-pick-4-day-midday'},
    'ne_pick3': {None: 'ne-pick-3'},
    'ne_pick5': {None: 'ne-pick-5'},
    'nh_pick3': {'evening': 'nh-pick-3-evening', 'midday': 'nh-pick-3-day'},
    'nh_pick4': {'evening': 'nh-pick-4-evening', 'midday': 'nh-pick-4-day'},
    'nj_cash5': {None: 'nj-cash-5'},
    'nj_pick3': {'evening': 'nj-pick-3-evening', 'midday': 'nj-pick-3-midday'},
    'nj_pick4': {'evening': 'nj-pick-4-evening', 'midday': 'nj-pick-4-midday'},
    'nj_pick6': {None: 'nj-pick-6-lotto'},
    'nm_pick3plus': {'evening': 'nm-pick-3-evening', 'midday': 'nm-pick-3-day'},
    'nm_pick4plus': {'evening': 'nm-pick-4-evening', 'midday': 'nm-pick-4-day'},
    'nm_roadrunner': {None: 'nm-roadrunner-cash'},
    'ny_lotto': {None: 'ny-lotto'},
    'ny_numbers': {'evening': 'ny-numbers-evening', 'midday': 'ny-numbers-midday'},
    'ny_pick10': {None: 'ny-pick-10'},
    'ny_take5': {'evening': 'ny-take-5-evening', 'midday': 'ny-take-5-midday'},
    'ny_win4': {'evening': 'ny-win-4-evening', 'midday': 'ny-win-4-midday'},
    'ok_cash5': {None: 'ok-cash-5'},
    'ok_pick3': {None: 'ok-pick-3'},
    'or_megabucks': {None: 'or-megabucks'},
    'or_winforlife': {None: 'or-win-for-life'},
    'pa_cash5': {None: 'pa-cash-5'},
    'pa_match6': {None: 'pa-match-6-lotto'},
    'pa_pick2': {'evening': 'pa-pick-2-evening', 'midday': 'pa-pick-2-day'},
    'pa_pick3': {'evening': 'pa-pick-3-evening', 'midday': 'pa-pick-3-day'},
    'pa_pick4': {'evening': 'pa-pick-4-evening', 'midday': 'pa-pick-4-day'},
    'pa_pick5': {'evening': 'pa-pick-5-evening', 'midday': 'pa-pick-5-day'},
    'pa_treasurehunt': {None: 'pa-treasure-hunt'},
    'pick3': {'evening': 'oh-pick-3-evening', 'midday': 'oh-pick-3-midday'},
    'pick4': {'evening': 'oh-pick-4-evening', 'midday': 'oh-pick-4-midday'},
    'pick5': {'evening': 'oh-pick-5-evening', 'midday': 'oh-pick-5-midday'},
    'powerball': {None: 'powerball'},
    'pr_lotoplus': {None: 'pr-loto-plus'},
    'pr_pega2': {'evening': 'pr-pega-2-night', 'midday': 'pr-pega-2-day'},
    'pr_pega3': {'evening': 'pr-pega-3-night', 'midday': 'pr-pega-3-day'},
    'pr_pega4': {'evening': 'pr-pega-4-night', 'midday': 'pr-pega-4-day'},
    'ri_numbers': {'evening': 'ri-numbers-evening', 'midday': 'ri-numbers-midday'},
    'ri_wildmoney': {None: 'ri-wild-money'},
    'rollingcash5': {None: 'oh-rolling-cash-5'},
    'sc_palmetto5': {None: 'sc-palmetto-cash-5'},
    'sc_pick3': {'evening': 'sc-pick-3-evening', 'midday': 'sc-pick-3-midday'},
    'sc_pick4': {'evening': 'sc-pick-4-evening', 'midday': 'sc-pick-4-midday'},
    'sd_dakotacash': {None: 'sd-dakota-cash'},
    'tn_cash3': {'evening': 'tn-cash-3-midday', 'midday': 'tn-cash-3-morning', 'night': 'tn-cash-3-evening'},
    'tn_cash4': {'evening': 'tn-cash-4-midday', 'midday': 'tn-cash-4-morning', 'night': 'tn-cash-4-evening'},
    'tn_dailyjackpot': {None: 'tn-daily-tennessee-jackpot'},
    'tn_tncash': {None: 'tn-tennessee-cash'},
    'tristate_gimme5': {None: 'gimme-5'},
    'tristate_megabucks': {None: 'tri-state-megabucks'},
    'tx_cashfive': {None: 'tx-cash-5'},
    'tx_lottotexas': {None: 'tx-lotto-texas'},
    'tx_twostep': {None: 'tx-texas-two-step'},
    'va_bankamillion': {None: 'va-bank-a-million'},
    'va_cash5': {None: 'va-cash-5'},
    'va_pick3': {'evening': 'va-pick-3-night', 'midday': 'va-pick-3-day'},
    'va_pick4': {'evening': 'va-pick-4-night', 'midday': 'va-pick-4-day'},
    'va_pick5': {'evening': 'va-pick-5-night', 'midday': 'va-pick-5-day'},
    'vt_pick3': {'evening': 'vt-pick-3-evening', 'midday': 'vt-pick-3-day'},
    'vt_pick4': {'evening': 'vt-pick-4-evening', 'midday': 'vt-pick-4-day'},
    'wa_daily': {None: 'wa-daily-game'},
    'wa_hit5': {None: 'wa-hit-5'},
    'wa_lotto': {None: 'wa-lotto'},
    'wa_match4': {None: 'wa-match-4'},
    'wi_badger5': {None: 'wi-badger-5'},
    'wi_megabucks': {None: 'wi-megabucks'},
    'wi_pick3': {'evening': 'wi-pick-3-evening', 'midday': 'wi-pick-3-midday'},
    'wi_pick4': {'evening': 'wi-pick-4-evening', 'midday': 'wi-pick-4-midday'},
    'wi_supercash': {None: 'wi-super-cash'},
    'wv_cash25': {None: 'wv-cash-25'},
    'wv_daily3': {None: 'wv-daily-3'},
    'wv_daily4': {None: 'wv-daily-4'},
    'wy_cowboydraw': {None: 'wy-cowboy-draw'},
}


BO_SLUGS = {
    'ar_cash3': 'cash-3',
    'ar_cash4': 'cash-4',
    'ar_lotto': 'natural-state-jackpot',
    'ar_naturalstate': 'natural-state-jackpot',
    'az_fantasy5': 'fantasy-5',
    'az_pick3': 'pick-3',
    'az_pick4': 'pick-4',
    'az_thepick': 'the-pick',
    'az_tripletwist': 'triple-twist',
    'ca_daily3': 'daily-3',
    'ca_daily4': 'daily-4',
    'ca_fantasy5': 'fantasy-5',
    'ca_superlotto': 'superlotto-plus',
    'classiclotto': 'classic-lotto',
    'co_cash5': 'cash-5',
    'co_lotto': 'lotto',
    'co_pick3': 'pick-3',
    'ct_cash5': 'cash-5',
    'ct_classiclotto': 'lotto',
    'ct_play3': 'play3',
    'ct_play4': 'play4',
    'dc_3': 'dc-3',
    'dc_4': 'dc-4',
    'dc_5': 'dc-5',
    'de_lotto': 'lotto',
    'de_play3': 'play-3',
    'de_play4': 'play-4',
    'de_play5': 'play-5',
    'fl_fantasy5': 'fantasy-5',
    'fl_jackpottripleplay': 'jackpot-triple-play',
    'fl_lotto': 'lotto',
    'fl_pick2': 'pick-2',
    'fl_pick3': 'pick-3',
    'fl_pick4': 'pick-4',
    'fl_pick5': 'pick-5',
    'ga_cash3': 'cash-3',
    'ga_cash4': 'cash-4',
    'ga_fantasy5': 'fantasy-5',
    'ga_georgiafive': 'georgia-five',
    'ia_pick3': 'pick-3',
    'ia_pick4': 'pick-4',
    'id_idahocash': 'idaho-cash',
    'id_pick3': 'pick-3',
    'id_pick4': 'pick-4',
    'il_lotto': 'lotto',
    'il_luckydaylotto': 'luckyday-lotto',
    'il_pick3': 'pick-3',
    'il_pick4': 'pick-4',
    'in_daily3': 'daily-3',
    'in_daily4': 'daily-4',
    'ks_pick3': 'pick-3',
    'ks_superkansas': 'super-cash',
    'ky_pick3': 'pick-3',
    'ky_pick4': 'pick-4',
    'la_easy5': 'easy-5',
    'la_lotto': 'lotto',
    'la_pick3': 'pick-3',
    'la_pick4': 'pick-4',
    'la_pick5': 'pick-5',
    'lottoamerica': None,
    'ma_mass3': 'mass-3',
    'ma_mass4': 'mass-4',
    'ma_masscash': 'masscash',
    'ma_megabucks': 'megabucks-doubler',
    'ma_numbers': 'numbers',
    'md_bonusmatch5': 'bonus-match-5',
    'md_multimatch': 'multimatch',
    'md_pick3': 'pick-3',
    'md_pick4': 'pick-4',
    'md_pick5': 'pick-5',
    'me_pick3': 'pick-3',
    'me_pick4': 'pick-4',
    'megamillions': None,
    'mi_daily3': 'daily-3',
    'mi_daily4': 'daily-4',
    'mi_daily5': 'daily-5',
    'mi_fantasy5': 'fantasy-5',
    'mi_lotto47': 'lotto-47',
    'mn_daily3': 'daily-3',
    'mn_gopher5': 'gopher-5',
    'mn_north5': 'northstar-cash',
    'mo_millions': 'mo-millions',
    'mo_pick3': 'pick-3',
    'mo_pick4': 'pick-4',
    'mo_showme': 'show-me-cash',
    'ms_cash3': 'cash-3',
    'ms_cash4': 'cash-4',
    'ms_match5': 'match-5',
    'mt_bigskyb': 'big-sky-bonus',
    'mt_cash': 'montana-cash',
    'nc_cash5': 'cash-5',
    'nc_pick3': 'pick-3',
    'nc_pick4': 'pick-4',
    'ne_pick3': 'pick-3',
    'ne_pick5': 'pick-5',
    'nh_pick3': 'pick-3',
    'nh_pick4': 'pick-4',
    'nj_cash5': 'cash-5',
    'nj_pick3': 'pick-3',
    'nj_pick4': 'pick-4',
    'nj_pick6': 'pick-6-lotto',
    'nm_pick3plus': 'pick-3',
    'nm_pick4plus': 'pick-4',
    'nm_roadrunner': 'roadrunner-cash',
    'ny_lotto': None,
    'ny_numbers': None,
    'ny_pick10': None,
    'ny_take5': None,
    'ny_win4': None,
    'ok_cash5': 'cash-5',
    'ok_pick3': 'pick-3',
    'or_megabucks': 'megabucks',
    'or_winforlife': 'win-for-life',
    'pa_cash5': 'cash-5',
    'pa_match6': 'match-6-lotto',
    'pa_pick2': 'pick-2',
    'pa_pick3': 'pick-3',
    'pa_pick4': 'pick-4',
    'pa_pick5': 'pick-5',
    'pa_treasurehunt': 'treasure-hunt',
    'pick3': 'pick-3',
    'pick4': 'pick-4',
    'pick5': 'pick-5',
    'powerball': None,
    'pr_lotoplus': 'loto-plus',
    'pr_pega2': 'pega-2',
    'pr_pega3': 'pega-3',
    'pr_pega4': 'pega-4',
    'ri_numbers': 'numbers',
    'ri_wildmoney': 'wild-money',
    'rollingcash5': 'rolling-cash-5',
    'sc_palmetto5': 'palmetto-cash-5',
    'sc_pick3': 'pick-3',
    'sc_pick4': 'pick-4',
    'sd_dakotacash': 'dakota-cash',
    'tn_cash3': 'cash-3',
    'tn_cash4': 'cash-4',
    'tn_dailyjackpot': 'daily-tennessee-jackpot',
    'tn_tncash': 'tennessee-cash',
    'tristate_gimme5': 'tri-state-gimme-5',
    'tristate_megabucks': 'tri-state-megabucks',
    'tx_cashfive': 'cash-5',
    'tx_lottotexas': 'lotto-texas',
    'tx_twostep': 'texas-two-step',
    'va_bankamillion': 'bank-a-million',
    'va_cash5': 'cash-5',
    'va_pick3': 'pick-3',
    'va_pick4': 'pick-4',
    'va_pick5': 'pick-5',
    'vt_pick3': 'pick-3',
    'vt_pick4': 'pick-4',
    'wa_daily': 'daily-game',
    'wa_hit5': 'hit-5',
    'wa_lotto': 'lotto',
    'wa_match4': 'match-4',
    'wi_badger5': 'badger-5',
    'wi_megabucks': 'megabucks',
    'wi_pick3': 'pick-3',
    'wi_pick4': 'pick-4',
    'wi_supercash': 'supercash',
    'wv_cash25': 'cash-25',
    'wv_daily3': 'daily-3',
    'wv_daily4': 'daily-4',
    'wy_cowboydraw': 'cowboy-draw',
}

# bitsandodds state slugs that differ from the LUSA state segment
# (Tri-State games live under Vermont there).
BO_STATE_OVERRIDES = {
    'tristate_gimme5': 'vermont',
    'tristate_megabucks': 'vermont',
}


# ------------------------------------------------------------------
# Cross-check (item 5, Nov 2026): verify newly added draws against a
# second view before they are written to the published file.
#
# Design (from the two research passes, Nov 27 2026):
# - Primary second view: usalotteryresults.com/api/draws.json — one
#   free keyless JSON fetch, latest draw per game/slot, all 47
#   jurisdictions. It is an aggregator, not the operator, and whole
#   feeds go stale for weeks (AZ, LA, WA Lotto, NY Lotto, OH games,
#   DC midday, KS Super Kansas, FL Jackpot Triple Play were stale on
#   Nov 27) — so the gate is per-row: a row only counts when its
#   drawDate EQUALS the new draw's date. A stale/missing row is
#   "no verdict", never a disagreement.
# - Official lanes (higher trust): PA's DrawingsData JSON feed (one
#   request, all PA games) and OH's winning-numbers page (embedded
#   JSON in the #cmsNumbers input). NJ/NC/FL/MI/WI have no usable
#   plain-GET official source — aggregators only.
# - Fallback: bitsandodds.com per-game archive pages (Date|Draw|
#   Numbers table, ~120 draws) for games the other sources can't
#   confirm. Bounded: at most XC_BO_MAX_FETCHES pages per run.
# - NOT built, deliberately: a LotteryUSA-vs-LotteryUSA check (same
#   backend — cannot catch an upstream data error, the exact failure
#   a second view is for) and any paid/keyed API.
#
# Verdicts per new draw: 'confirm' (some source agrees) publishes;
# 'mismatch' (a source disagrees AND no source confirms) quarantines
# the draw — it is removed from the in-memory data, never written,
# and listed in the run output for human review, exactly like a
# CONFLICT. A second source can itself be wrong (LotteryValley still
# showed the wrong NC Cash 5 value days after the correction), so a
# disagreement never silently overwrites stored data, and one
# confirmation suffices when sources disagree with each other
# (counted as 'contested'). A missing/stale second view never blocks
# the run: the draw publishes on format validation alone.
#
# Slot mapping in XCHECK_GAMEIDS is by the draw time encoded in the
# LUSA URL slug (see the generator in /tmp/cf-test/gen_xcheck.py) —
# our slot labels are shifted for TN Cash 3/4. A wrong slot mapping
# would false-confirm or false-quarantine, so the table was verified
# against the live aggregator payload (same-date draws matched for
# every override) before it was baked in. Regenerate the table when
# the aggregator renames a gameId or a game is added; a missing entry
# is safe (no verdict).
# ------------------------------------------------------------------
XC_AGG_URL = 'https://www.usalotteryresults.com/api/draws.json'
XC_PA_URL = ('https://www.palottery.pa.gov/Custom/feeds/'
             'DrawingsData.aspx?game=pick%203')
XC_OH_URL = ('https://www.ohiolottery.com/winningnumbers/'
             'index?aliaspath=/WinningNumbers/Index')
XC_BO_BASE = 'https://bitsandodds.com'
XC_BO_MAX_FETCHES = 25
XC_PACE_SECONDS = 0.5  # politeness pause between cross-check fetches

# Our PA game -> PA feed GameName.
PA_GAME_NAMES = {
    'pa_pick2': 'PICK 2', 'pa_pick3': 'PICK 3', 'pa_pick4': 'PICK 4',
    'pa_pick5': 'PICK 5', 'pa_cash5': 'CASH 5', 'pa_match6': 'MATCH 6',
    'pa_treasurehunt': 'TREASURE HUNT',
}
# Our OH game -> OH DrawGameId (verified against the live page Nov 27).
# 13=Pick 3, 14=Pick 4, 25=Pick 5, 24=Rolling Cash 5, 26=Classic Lotto,
# 38=Millionaire for Life, 12=Mega Millions, 15=Powerball.
# (11=Lucky for Life is retired — deliberately absent.)
OH_GAME_IDS = {
    'pick3': 13, 'pick4': 14, 'pick5': 25, 'rollingcash5': 24,
    'classiclotto': 26, 'millionaireforlife': 38,
    'megamillions': 12, 'powerball': 15,
}
# OH evening draw time (Pick 3/4/5): used only to disambiguate a lone
# same-date row in the OH blob (see fetch_xcheck_oh).
OH_EVENING_HOUR = 19
OH_EVENING_MINUTE = 30

# bitsandodds Draw-column labels -> our slot labels.
BO_SLOT_LABELS = {'midday': 'midday', 'day': 'midday', 'evening': 'evening',
                  'night': 'night', 'morning': 'morning',
                  'afternoon': 'afternoon'}


def _draws_match(game, numbers, bonus, ref_numbers, ref_bonus):
    """Compare a primary draw against a second-source draw. Digit games
    (max 9): exact sequence — order IS the result. Ball games: sorted
    comparison — draw order is not the result. A bonus is compared only
    when both sides have one (a source that doesn't report it can't
    contradict it). Add-on balls (Fireball, Wild Ball, Double Play,
    multipliers) are never compared — neither model carries them."""
    if game in DIGITS:
        if list(numbers) != list(ref_numbers):
            return False
    else:
        if sorted(numbers) != sorted(ref_numbers):
            return False
    if bonus is not None and ref_bonus is not None and bonus != ref_bonus:
        return False
    return True


def _ref_bonus(game, ref_numbers, ref_bonus):
    """Normalise a second source's bonus for comparison: digit games
    have none; a bonus list (aggregator) yields its single element."""
    if game in DIGITS:
        return None
    if isinstance(ref_bonus, list):
        return ref_bonus[0] if ref_bonus else None
    return ref_bonus


def fetch_xcheck_aggregator():
    """usalotteryresults draws.json -> {gameId: row} or None when the
    fetch or parse fails (the run then skips this source silently).
    No global freshness gate: the payload as a whole can be fresh
    while individual feeds are weeks stale, so staleness is judged
    per row at compare time (row.drawDate must equal the draw's date)."""
    try:
        time.sleep(XC_PACE_SECONDS)
        payload = _http_get(XC_AGG_URL).json()
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    rows = {}
    for grp in payload.get('multiState', []) or []:
        if not isinstance(grp, dict):
            continue
        for row in grp.get('draws', []) or []:
            if isinstance(row, dict) and row.get('gameId'):
                rows[row['gameId']] = row
    for j in payload.get('jurisdictions', []) or []:
        if not isinstance(j, dict):
            continue
        for row in j.get('draws', []) or []:
            if isinstance(row, dict) and row.get('gameId'):
                rows[row['gameId']] = row
    return rows or None


def _pa_date(s):
    """'11/27/2026' -> '2026-11-27'; None on anything else."""
    if not isinstance(s, str):
        return None
    m = re.fullmatch(r'(\d{1,2})/(\d{1,2})/(\d{4})', s.strip())
    if not m:
        return None
    mo, d, y = (int(x) for x in m.groups())
    try:
        return date(y, mo, d).isoformat()
    except ValueError:
        return None


def fetch_xcheck_pa():
    """PA DrawingsData feed -> {(GameName, date, is_midday): [numbers]}
    or None on failure. Takes the first GameBallCount elements of
    DrawingNumbersAsList — the Wild Ball is appended as the last
    element for digit games and must not be compared. Each row's
    RelatedEveningDrawing is included as the previous evening's draw."""
    try:
        time.sleep(XC_PACE_SECONDS)
        feed = _http_get(XC_PA_URL).json()
    except Exception:
        return None
    if not isinstance(feed, list):
        return None
    out = {}

    def _add(name, date_s, is_midday, nums_list, n):
        date = _pa_date(date_s)
        if not name or not date or not n:
            return
        try:
            nums = [int(x) for x in list(nums_list or [])[:n]]
        except (TypeError, ValueError):
            return
        if len(nums) == n:
            out[(name, date, bool(is_midday))] = nums

    for row in feed:
        if not isinstance(row, dict):
            continue
        name = row.get('GameName')
        n = row.get('GameBallCount')
        _add(name, row.get('DrawingDateAsString'),
             row.get('IsMidDayDrawing'), row.get('DrawingNumbersAsList'), n)
        rel = row.get('RelatedEveningDrawing') or {}
        if isinstance(rel, dict):
            _add(name, rel.get('DrawingDateAsString'), False,
                 rel.get('DrawingNumbersAsList'), n)
    return out or None


def fetch_xcheck_oh():
    """OH winning-numbers page -> {DrawGameId: [(date, [numbers])]}
    newest-first, or None on failure. Parses the #cmsNumbers hidden
    input's JSON blob. Draws carry no slot label: Pick 3/4/5 hold the
    ~2 latest draws, so the slot is resolved positionally at compare
    time (same date: first row = evening, last row = midday). A stale
    Feb 21 Lucky for Life row is in the blob — it is ignored because
    Lucky for Life is not in OH_GAME_IDS."""
    try:
        time.sleep(XC_PACE_SECONDS)
        html_text = _http_get(XC_OH_URL).text
    except Exception:
        return None
    m = re.search(r'id="cmsNumbers" value=\'([^\']+)\'', html_text)
    if not m:
        return None
    try:
        blob = json.loads(html.unescape(m.group(1)))
    except Exception:
        return None
    if not isinstance(blob, list):
        return None
    out = {}
    for g in blob:
        if not isinstance(g, dict):
            continue
        gid = g.get('DrawGameId')
        draws = []
        for dr in g.get('Draws') or []:
            if not isinstance(dr, dict):
                continue
            nums = dr.get('Numbers') or []
            vals = [n.get('Value') for n in nums
                    if isinstance(n, dict) and n.get('Value') is not None]
            d = (dr.get('DrawDate') or '')[:10]
            if d and vals:
                draws.append((d, vals))
        if gid is not None and draws:
            out[gid] = draws
    return out or None


def fetch_xcheck_bo(game):
    """One bitsandodds archive page -> {(date, slot): [numbers]} or None.
    Only the table headed Date|Draw|Numbers is parsed; a missing table,
    a 404, or any fetch failure is None (no verdict, never an error).
    Numbers are space-separated ints in draw order."""
    slug = BO_SLUGS.get(game)
    if not slug:
        return None
    state = BO_STATE_OVERRIDES.get(game)
    if state is None:
        urls = LUSA.get(game) or []
        if not urls:
            return None
        state = urls[0][1].split('/')[3]
    url = f'{XC_BO_BASE}/{state}/lottery/{slug}/numbers/'
    try:
        time.sleep(XC_PACE_SECONDS)
        html_text = _http_get(url).text
    except Exception:
        return None
    for table in re.findall(r'<table.*?</table>', html_text, re.S):
        if '<th>Date</th>' not in table or '<th>Numbers</th>' not in table:
            continue
        out = {}
        for date_s, draw_s, nums_s in re.findall(
                r'<td[^>]*>\s*([^<]+?)\s*</td>\s*<td[^>]*>\s*([^<]+?)\s*</td>'
                r'\s*<td[^>]*>\s*([0-9 ]+?)\s*</td>', table):
            try:
                d = datetime.strptime(date_s.strip(), '%B %d, %Y').strftime('%Y-%m-%d')
                nums = [int(x) for x in nums_s.split()]
            except (TypeError, ValueError):
                continue
            slot = BO_SLOT_LABELS.get(draw_s.strip().lower())
            if slot is None or not nums:
                continue
            out[(d, slot)] = nums
        if out:
            return out
    return None


def _oh_numbers_for(game, rows, date, slot, now_et):
    """Resolve one OH draw from the blob's rows for a game. Returns
    (numbers, bonus) or None when the blob can't pin the draw down.
    Single-draw games match by date alone. For Pick 3/4/5 the blob
    holds the ~2 latest draws newest-first: with two rows on the date,
    the first is the evening draw and the last is the midday draw;
    with one row, it is the latest draw of that date, so the slot is
    the one implied by the fetch time (before 19:30 -> midday)."""
    count, _, has_bonus, _, _ = RULES[game]
    same = [nums for (d, nums) in rows if d == date]
    if not same:
        return None
    if slot is None:
        nums = same[0]
    elif len(same) >= 2:
        nums = same[0] if slot == 'evening' else same[-1]
    else:
        evening_yet = (now_et.hour, now_et.minute) >= (OH_EVENING_HOUR, OH_EVENING_MINUTE)
        if slot == 'evening' and not evening_yet:
            return None
        if slot == 'midday' and evening_yet:
            return None
        nums = same[0]
    mains = list(nums[:count])
    bonus = None
    if has_bonus and len(nums) > count and nums[count] is not None and nums[count] > 0:
        bonus = nums[count]
    return mains, bonus


def crosscheck_new_draws(new_draws, data, added, now_et):
    """Verify newly added draws against second views; quarantine on
    disagreement. new_draws: [{'game','date','slot','numbers','bonus'}]
    as stored (already merged into data). Quarantined draws are removed
    from data and from added (a game whose count hits 0 drops out of
    added). Returns {'confirmed', 'no_verdict', 'contested',
    'quarantined': [{game, date, slot, numbers, bonus, second, source}]}
    where 'second' is the disagreeing second-source numbers. Never
    raises for a source failure — a missing source is no verdict. The
    caller wraps this in try/except as well, so a bug here can never
    block a run."""
    summary = {'confirmed': 0, 'no_verdict': 0, 'contested': 0,
               'quarantined': []}
    if not new_draws:
        return summary
    agg = fetch_xcheck_aggregator()
    pa = fetch_xcheck_pa()
    oh = fetch_xcheck_oh()
    bo_cache = {}
    bo_fetches = [0]
    for draw in list(new_draws):
        game, date = draw['game'], draw['date']
        slot, nums, bonus = draw['slot'], draw['numbers'], draw['bonus']
        confirms, mismatches = [], []

        def _check(ref_nums, ref_bonus, source):
            if ref_nums is None:
                return
            rb = _ref_bonus(game, ref_nums, ref_bonus)
            if _draws_match(game, nums, bonus, ref_nums, rb):
                confirms.append(source)
            else:
                mismatches.append((source, list(ref_nums), rb))

        # Aggregator lane (per-row date gate: stale row = no verdict).
        gid = XCHECK_GAMEIDS.get(game, {}).get(slot)
        if agg is not None and gid:
            row = agg.get(gid)
            if row is not None and row.get('drawDate') == date:
                _check(row.get('numbers'), row.get('bonus'), 'usalotteryresults')
        # PA official lane.
        if pa is not None and game in PA_GAME_NAMES:
            ref = pa.get((PA_GAME_NAMES[game], date, slot == 'midday'))
            if ref is not None:
                _check(ref, None, 'palottery')
        # OH official lane.
        if oh is not None and game in OH_GAME_IDS:
            got = _oh_numbers_for(game, oh.get(OH_GAME_IDS[game], []),
                                  date, slot, now_et)
            if got is not None:
                _check(got[0], got[1], 'ohiolottery')
        # bitsandodds fallback, only when nothing confirmed or
        # mismatched yet (a confirm or a mismatch already decides it,
        # and the fallback is capped per run).
        if not confirms and not mismatches and bo_fetches[0] < XC_BO_MAX_FETCHES:
            if game not in bo_cache:
                bo_cache[game] = fetch_xcheck_bo(game)
                bo_fetches[0] += 1
            table = bo_cache.get(game)
            if table is not None:
                ref = table.get((date, slot)) if slot is not None else None
                if ref is None and slot is None:
                    # Single-draw games: the table's rows for the date.
                    dated = [v for (d, _s), v in table.items() if d == date]
                    ref = dated[0] if len(dated) == 1 else None
                if ref is not None:
                    _check(ref, None, 'bitsandodds')
        if confirms:
            summary['confirmed'] += 1
            if mismatches:
                summary['contested'] += 1
            continue
        if mismatches:
            source, ref_nums, ref_bonus = mismatches[0]
            summary['quarantined'].append({
                'game': game, 'date': date, 'slot': slot,
                'numbers': list(nums), 'bonus': bonus,
                'second': ref_nums, 'second_bonus': ref_bonus,
                'source': source})
            # Remove the draw from the in-memory data (it is never
            # written). Note: for slot games the stored order within a
            # date is positional, so removing a midday draw while the
            # evening stays can mislabel the evening row on the next
            # run — that degrades to a CONFLICT (safe: human review),
            # never to a wrong publish.
            stored = data.get(game, [])
            for i, e in enumerate(stored):
                if e['date'] == date and list(e['numbers']) == list(nums) \
                        and e.get('bonus') == bonus:
                    del stored[i]
                    break
            if game in added:
                added[game] -= 1
                if added[game] <= 0:
                    del added[game]
            continue
        summary['no_verdict'] += 1
    return summary


def main():
    # Serialize runs: a second concurrent run prints {"status": "locked"}
    # and exits 0 instead of racing the first run's data write. flock is
    # released automatically if this process dies, so locks can't go
    # stale. If the lockfile itself can't be created, proceed without
    # the lock rather than failing the refresh. The lock is closed
    # explicitly in a finally (not left to frame teardown), so it is
    # released even if a traceback keeps this frame alive.
    lock_fp = None
    try:
        lock_fp = open(LOCK_PATH, 'w')
        fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(json.dumps({'status': 'locked'}))
        return
    except OSError:
        lock_fp = None
    try:
        _run()
    finally:
        if lock_fp is not None:
            lock_fp.close()


def _run():
    try:
        data = json.loads(DATA_PATH.read_text())
    except (OSError, ValueError) as e:
        fail(f'cannot load draws.json: {e}')

    added = {}
    warnings = []
    conflicts = []
    errors = []
    now_et = datetime.now(ET)
    new_draws = []

    for game in RULES:
        n, err = fetch_game_with_retry(game, data, warnings, now_et, new_draws)
        if err:
            (conflicts if 'CONFLICT' in err else errors).append(err)
        elif n:
            added[game] = n

    # Mega Millions fast lane (official site, minutes after the draw).
    # Only a genuine conflict with stored data is reported; every other
    # fast-lane failure is swallowed inside apply_mm_fastlane.
    jackpot_changed, mm_err = apply_mm_fastlane(data, added, new_draws)
    if mm_err:
        (conflicts if 'CONFLICT' in mm_err else errors).append(mm_err)

    # Powerball fast lane (MUSL API, key-gated; silent no-op with no key).
    pb_err = apply_powerball_fastlane(data, added, new_draws)
    if pb_err:
        (conflicts if 'CONFLICT' in pb_err else errors).append(pb_err)

    # Quarantine rule (Nov 2026): a genuine data CONFLICT no longer
    # aborts the run. The conflicting game is skipped — neither side is
    # published for it — and the conflict is listed in 'conflicts' so
    # the cron worker can flag it for a human decision. Every other
    # per-game failure (request error, unparseable page, empty result,
    # bad response) is a warning: that game simply doesn't update this
    # run, while every game that fetched cleanly still publishes. A
    # failed game never mutates data (fetch_game merges only after it
    # succeeds), so partial publishing can't write a half-merged game.
    warnings.extend(errors)

    # Cross-check (item 5): verify the new draws against second views
    # before writing. A mismatch quarantines the draw (removed from
    # data/added, listed in 'crosscheck'), never silently overwrites.
    # The check itself can never block the run: on any exception it
    # warns and publishes as today.
    xc = None
    if new_draws:
        try:
            xc = crosscheck_new_draws(new_draws, data, added, now_et)
        except Exception as e:
            warnings.append(f'cross-check skipped: {e}')

    if added or jackpot_changed:
        tmp = DATA_PATH.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, separators=(',', ':')))
        tmp.replace(DATA_PATH)

    through = max(d['date'] for draws in data.values() for d in draws)
    out = {'status': 'updated' if (added or jackpot_changed) else 'nochange',
           'added': added, 'through': through}
    if warnings:
        out['warnings'] = warnings
    if conflicts:
        out['conflicts'] = conflicts
    if xc is not None:
        out['crosscheck'] = xc
    print(json.dumps(out))


if __name__ == '__main__':
    main()
