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
Exit code: 0 = updated or nochange, 1 = hard error (draws.json untouched).
A run may carry a 'warnings' list: per-game failures that are NOT data
conflicts (request errors, unparseable pages, empty results, bad
responses) no longer abort the run — that game simply doesn't update
while every game that fetched cleanly still publishes. Only a genuine
CONFLICT between stored and fetched draws for the same date is a hard
error, because publishing either side blindly would be wrong.
"""
import json, os, re, sys, time
from datetime import datetime
from pathlib import Path

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
PACE_SECONDS = 0.25
RETRY_SECONDS = 5

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
    ds, bonus_field = SOCRATA[game]
    url = (f'https://data.ny.gov/resource/{ds}.json'
           f"?$where=draw_date>'{last_date}T00:00:00'&$order=draw_date ASC&$limit=500")
    try:
        rows = requests.get(url, headers=UA, timeout=TIMEOUT).json()
        if not isinstance(rows, list):
            raise ValueError(f'{game}: Socrata returned non-list: {str(rows)[:120]}')
    except (ValueError, requests.RequestException):
        # Socrata occasionally returns an empty/unparseable body for $where
        # queries (seen 2026-10-23 night run). Fall back to newest-first and
        # filter in Python instead of failing the whole run.
        fb = (f'https://data.ny.gov/resource/{ds}.json'
              f'?$order=draw_date DESC&$limit=500')
        rows = requests.get(fb, headers=UA, timeout=TIMEOUT).json()
        if not isinstance(rows, list):
            raise ValueError(f'{game}: Socrata fallback returned non-list')
        rows = [r for r in rows if r['draw_date'][:10] > last_date]
        rows.sort(key=lambda r: r['draw_date'])
    out = []
    for r in rows:
        date = r['draw_date'][:10]
        nums = [int(x) for x in r['winning_numbers'].split()]
        if bonus_field:
            bonus = int(r[bonus_field])
        elif game == 'powerball':
            bonus = nums.pop()  # last number is the Powerball
        else:
            bonus = None
        err = validate(game, nums, bonus)
        if err:
            raise ValueError(err + f' (date {date})')
        entry = {'date': date, 'numbers': nums}
        if bonus is not None:
            entry['bonus'] = bonus
        out.append(entry)
    return out


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
        resp = requests.get(MM_FASTLANE_URL, headers=UA, timeout=TIMEOUT)
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
        if date > datetime.now().strftime('%Y-%m-%d'):
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


def apply_mm_fastlane(data, added):
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


def fetch_socrata_slots(game, since):
    """Socrata datasets with one row per date and separate midday/evening
    columns (NY Numbers / Win 4 / Take 5). Returns slot maps like the
    LotteryUSA pick games: {'midday': {date: nums}, 'evening': {date: nums}}."""
    dataset, columns, kind = SOCRATA_SLOTS[game]
    url = (f'https://data.ny.gov/resource/{dataset}.json'
           f"?$where=draw_date>'{since}T00:00:00'&$order=draw_date ASC&$limit=500")
    rows = requests.get(url, headers=UA, timeout=TIMEOUT).json()
    if not isinstance(rows, list):
        raise ValueError(f'{game}: Socrata returned non-list: {str(rows)[:120]}')
    maps = {'midday': {}, 'evening': {}}
    for r in rows:
        for slot, col in columns.items():
            raw = (r.get(col) or '').strip()
            if not raw:
                continue
            if kind == 'digits':
                nums = [int(c) for c in raw if c.isdigit()]
            else:
                nums = [int(x) for x in raw.split()]
            err = validate(game, nums, None)
            if err:
                raise ValueError(err + f' (date {r["draw_date"][:10]} {slot})')
            maps[slot][r['draw_date'][:10]] = nums
    return maps


def fetch_lusa_page(url, only_box=None):
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
    only Green Ball Double Draw evenings carry a 'Main' + 'Double' pair)."""
    # Pace the burst: this is the single choke point for all ~241
    # LotteryUSA page fetches per run. Hitting them in rapid succession
    # from one IP trips the source's bot protection (fast 403s).
    time.sleep(PACE_SECONDS)
    html = requests.get(url, headers=UA, timeout=TIMEOUT).text
    cards = re.split(r'c-results-table__item', html)[1:]
    found = {}
    for card in cards:
        dm = re.search(r'c-draw-card__draw-date-sub">\s*([A-Z][a-z]{2} \d{1,2}, \d{4})', card)
        if not dm:
            continue
        date = datetime.strptime(dm.group(1), '%b %d, %Y').strftime('%Y-%m-%d')
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
    if not found:
        raise ValueError(f'no draw cards parsed from {url}')
    return found


def merge_simple(game, stored, fetched_by_date):
    """Non-pick games: keyed union by date within the stored window
    (no backfill earlier than the first stored draw), sorted ascending.
    fetched_by_date values are {'numbers': [...], 'bonus': n|None}."""
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
        elif by_date[date]['numbers'] != nums or by_date[date].get('bonus') != bonus:
            raise ValueError(f'{game}: CONFLICT on {date}: stored {by_date[date]} vs fetched {entry}')
    return sorted(by_date.values(), key=lambda d: d['date']), added


# Slot order within a date (Phase 3: DC-3/DC-4 and GA Cash 3/Cash 4 draw
# three times a day — midday, evening, night). Stored entries are always
# in time order, so the n-th entry of a date is the n-th slot.
SLOT_ORDER = {'midday': 0, 'evening': 1, 'night': 2}
_SLOT_BY_INDEX = ('midday', 'evening', 'night')


def merge_picks(game, stored, slot_maps):
    """Pick games: stored order within a date is midday, evening, then
    night (only games with three draws a day use the night slot).
    slot_maps: {'midday': {date: digits}, 'evening': {...}, 'night': {...}}
    An empty stored list seeds from whatever the source returns."""
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


def fetch_game(game, data):
    """Fetch and merge one game in place; returns the number of draws
    added. Raises on any failure. Safe to retry: a failed attempt never
    mutates data (merges only assign after they succeed)."""
    stored = data.get(game, [])
    if game in SOCRATA:
        since = stored[-1]['date'] if stored else SEED_SINCE.get(game, '2020-01-01')
        new = fetch_socrata(game, since)
        if new:
            if stored:
                for e in new:
                    if e['date'] <= stored[-1]['date']:
                        raise ValueError(f'{game}: Socrata returned non-newer draw {e}')
            stored.extend(new)
            data[game] = stored
            return len(new)
    elif game in SOCRATA_SLOTS:
        since = stored[0]['date'] if stored else SEED_SINCE.get(game, '2020-01-01')
        slot_maps = fetch_socrata_slots(game, since)
        merged, n = merge_picks(game, stored, slot_maps)
        if n:
            data[game] = merged
        return n
    elif game in SLOT_GAMES:
        slot_maps = {slot: fetch_lusa_page(url, LUSA_ONLY_BOX.get(game))
                     for slot, url in LUSA[game]}
        merged, n = merge_picks(game, stored, slot_maps)
        if n:
            data[game] = merged
        return n
    else:
        fmap = fetch_lusa_page(LUSA[game][0][1], LUSA_ONLY_BOX.get(game))
        merged, n = merge_simple(game, stored, fmap)
        if n:
            data[game] = merged
        return n
    return 0


def fetch_game_with_retry(game, data):
    """One attempt, then one retry after RETRY_SECONDS for transient
    failures. Returns (added_count, error_str_or_None)."""
    try:
        return fetch_game(game, data), None
    except Exception as e:
        if not _is_retryable(e):
            return 0, str(e)
        time.sleep(RETRY_SECONDS)
        try:
            return fetch_game(game, data), None
        except Exception as e2:
            return 0, str(e2)


def main():
    data = json.loads(DATA_PATH.read_text())
    added = {}
    errors = []

    for game in RULES:
        n, err = fetch_game_with_retry(game, data)
        if err:
            errors.append(err)
        elif n:
            added[game] = n

    # Mega Millions fast lane (official site, minutes after the draw).
    # Only a genuine conflict with stored data is a hard error; every
    # other fast-lane failure is swallowed inside apply_mm_fastlane.
    jackpot_changed, mm_err = apply_mm_fastlane(data, added)
    if mm_err:
        errors.append(mm_err)

    # Partial-failure rule (Nov 2026 — LotteryUSA's Maine pages stopped
    # parsing): a genuine data CONFLICT is still a hard error (fail,
    # write nothing), because stored and fetched draws disagree and
    # publishing either side blindly would be wrong. Every other
    # per-game failure (request error, unparseable page, empty result,
    # bad response) is a warning: that game simply doesn't update this
    # run, while every game that fetched cleanly still publishes. A
    # failed game never mutates data (fetch_game merges only after it
    # succeeds), so partial publishing can't write a half-merged game.
    hard = [e for e in errors if 'CONFLICT' in e]
    if hard:
        fail('; '.join(hard))

    if added or jackpot_changed:
        tmp = DATA_PATH.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, separators=(',', ':')))
        tmp.replace(DATA_PATH)

    through = max(d['date'] for draws in data.values() for d in draws)
    out = {'status': 'updated' if (added or jackpot_changed) else 'nochange',
           'added': added, 'through': through}
    if errors:
        out['warnings'] = errors
    print(json.dumps(out))


if __name__ == '__main__':
    main()
