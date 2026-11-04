#!/usr/bin/env python3
"""Crackpot Fortune — deterministic draw fetcher.

Pulls new lottery draws from machine-readable sources, validates them, and
merges them into data/draws.json. No AI transcription involved.

Sources:
  - Mega Millions / Powerball / Millionaire for Life: NY Lottery open data
    (Socrata JSON API on data.ny.gov) — the same official datasets the
    stored history was built from.
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
Exit code: 0 = updated or nochange, 1 = error (draws.json untouched).
"""
import json, os, re, sys
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).parent
DATA_PATH = Path(os.environ.get('CF_DATA', ROOT / 'data' / 'draws.json'))
UA = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36'}
TIMEOUT = 30

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
}

# Digit games (positional, 0-9 per position). Everything else draws 1..max.
DIGITS = {'pick3', 'pick4', 'pick5', 'ny_numbers', 'ny_win4',
          'pa_pick2', 'pa_pick3', 'pa_pick4', 'pa_pick5',
          'mi_daily3', 'mi_daily4', 'mi_daily5',
          'in_daily3', 'in_daily4', 'ky_pick3', 'ky_pick4',
          'wv_daily3', 'wv_daily4'}

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
}

# LUSA games whose cards carry a second ball box that is NOT the draw
# (e.g. Michigan Fantasy 5 / Lotto 47 "Double Play"). Only the named
# box title is parsed for these games.
LUSA_ONLY_BOX = {'mi_fantasy5': 'Regular', 'mi_lotto47': 'Regular'}

# Games with two draws per day fetched via LotteryUSA slot pages.
SLOT_GAMES = {'pick3', 'pick4', 'pick5',
              'pa_pick2', 'pa_pick3', 'pa_pick4', 'pa_pick5',
              'mi_daily3', 'mi_daily4', 'mi_daily5',
              'in_daily3', 'in_daily4', 'ky_pick3', 'ky_pick4'}


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
    """Parse one LotteryUSA year page -> {date: [numbers]} (newest first in page).
    only_box: if set, take balls only from the ball box with that title
    (e.g. Michigan Fantasy 5 / Lotto 47 'Regular' vs 'Double Play')."""
    html = requests.get(url, headers=UA, timeout=TIMEOUT).text
    cards = re.split(r'c-results-table__item', html)[1:]
    found = {}
    for card in cards:
        dm = re.search(r'c-draw-card__draw-date-sub">\s*([A-Z][a-z]{2} \d{1,2}, \d{4})', card)
        if not dm:
            continue
        date = datetime.strptime(dm.group(1), '%b %d, %Y').strftime('%Y-%m-%d')
        if only_box:
            balls = []
            for part in re.split(r'c-draw-card__ball-box', card)[1:]:
                tm = re.search(r'c-draw-card__ball-title">\s*([^<]+?)\s*</p>', part)
                if tm and tm.group(1).strip() == only_box:
                    balls = [int(x) for x in re.findall(r'<li class="c-ball[^"]*">\s*(\d+)\s*</li>', part)]
                    break
        else:
            balls = [int(x) for x in re.findall(r'<li class="c-ball[^"]*">\s*(\d+)\s*</li>', card)]
        if balls:
            found[date] = balls
    if not found:
        raise ValueError(f'no draw cards parsed from {url}')
    return found


def merge_simple(game, stored, fetched_by_date):
    """Non-pick games: keyed union by date within the stored window
    (no backfill earlier than the first stored draw), sorted ascending."""
    first = stored[0]['date'] if stored else '0000-00-00'
    by_date = {d['date']: d for d in stored}
    added = 0
    for date, nums in fetched_by_date.items():
        if date < first:
            continue
        if date not in by_date:
            err = validate(game, nums, None)
            if err:
                raise ValueError(err + f' (date {date})')
            by_date[date] = {'date': date, 'numbers': nums}
            added += 1
        elif by_date[date]['numbers'] != nums:
            raise ValueError(f'{game}: CONFLICT on {date}: stored {by_date[date]["numbers"]} vs fetched {nums}')
    return sorted(by_date.values(), key=lambda d: d['date']), added


def merge_picks(game, stored, slot_maps):
    """Pick games: stored order within a date is midday then evening.
    slot_maps: {'midday': {date: digits}, 'evening': {date: digits}}
    An empty stored list seeds from whatever the source returns."""
    first = stored[0]['date'] if stored else '0000-00-00'
    by_key = {}
    from collections import Counter
    seen = Counter()
    for entry in stored:
        slot = 'midday' if seen[entry['date']] == 0 else 'evening'
        seen[entry['date']] += 1
        by_key[(entry['date'], slot)] = entry['numbers']
    added = 0
    for slot, fmap in slot_maps.items():
        for date, nums in fmap.items():
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
              for (d, s) in sorted(by_key.keys(), key=lambda k: (k[0], 0 if k[1] == 'midday' else 1))]
    return merged, added


def main():
    data = json.loads(DATA_PATH.read_text())
    added = {}
    errors = []

    for game in RULES:
        stored = data.get(game, [])
        try:
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
                    added[game] = len(new)
            elif game in SOCRATA_SLOTS:
                since = stored[0]['date'] if stored else SEED_SINCE.get(game, '2020-01-01')
                slot_maps = fetch_socrata_slots(game, since)
                merged, n = merge_picks(game, stored, slot_maps)
                if n:
                    data[game] = merged
                    added[game] = n
            elif game in SLOT_GAMES:
                slot_maps = {slot: fetch_lusa_page(url, LUSA_ONLY_BOX.get(game))
                             for slot, url in LUSA[game]}
                merged, n = merge_picks(game, stored, slot_maps)
                if n:
                    data[game] = merged
                    added[game] = n
            else:
                fmap = fetch_lusa_page(LUSA[game][0][1], LUSA_ONLY_BOX.get(game))
                merged, n = merge_simple(game, stored, fmap)
                if n:
                    data[game] = merged
                    added[game] = n
        except Exception as e:
            errors.append(str(e))

    if errors:
        fail('; '.join(errors))

    if added:
        tmp = DATA_PATH.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, separators=(',', ':')))
        tmp.replace(DATA_PATH)

    through = max(d['date'] for draws in data.values() for d in draws)
    print(json.dumps({'status': 'updated' if added else 'nochange',
                      'added': added, 'through': through}))


if __name__ == '__main__':
    main()
