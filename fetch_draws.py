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
RULES = {
    'pick3': (3, 9, False, None, False),
    'pick4': (4, 9, False, None, False),
    'pick5': (5, 9, False, None, False),
    'megamillions': (5, 70, True, 24, True),
    'powerball': (5, 69, True, 26, True),
    'rollingcash5': (5, 39, False, None, True),
    'classiclotto': (6, 49, False, None, True),
    'millionaireforlife': (5, 58, True, 5, True),
}

SOCRATA = {
    'megamillions': ('5xaw-6ayf', 'mega_ball'),
    'powerball': ('d6yy-54nr', None),          # bonus is the 6th number in winning_numbers
    'millionaireforlife': ('a4w9-a3tp', 'mill_ball'),
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
}


def fail(msg):
    print(json.dumps({'status': 'error', 'detail': msg}))
    sys.exit(1)


def validate(game, numbers, bonus):
    count, mx, has_bonus, bonus_max, distinct = RULES[game]
    if len(numbers) != count:
        return f'{game}: expected {count} numbers, got {numbers}'
    lo = 0 if game.startswith('pick') else 1
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


def fetch_lusa_page(url):
    """Parse one LotteryUSA year page -> {date: [numbers]} (newest first in page)."""
    html = requests.get(url, headers=UA, timeout=TIMEOUT).text
    cards = re.split(r'c-results-table__item', html)[1:]
    found = {}
    for card in cards:
        dm = re.search(r'c-draw-card__draw-date-sub">\s*([A-Z][a-z]{2} \d{1,2}, \d{4})', card)
        if not dm:
            continue
        date = datetime.strptime(dm.group(1), '%b %d, %Y').strftime('%Y-%m-%d')
        balls = [int(x) for x in re.findall(r'<li class="c-ball[^"]*">\s*(\d+)\s*</li>', card)]
        if balls:
            found[date] = balls
    if not found:
        raise ValueError(f'no draw cards parsed from {url}')
    return found


def merge_simple(game, stored, fetched_by_date):
    """Non-pick games: keyed union by date within the stored window
    (no backfill earlier than the first stored draw), sorted ascending."""
    first = stored[0]['date']
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
    slot_maps: {'midday': {date: digits}, 'evening': {date: digits}}"""
    first = stored[0]['date']
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
        stored = data[game]
        try:
            if game in SOCRATA:
                new = fetch_socrata(game, stored[-1]['date'])
                if new:
                    for e in new:
                        if e['date'] <= stored[-1]['date']:
                            raise ValueError(f'{game}: Socrata returned non-newer draw {e}')
                    stored.extend(new)
                    added[game] = len(new)
            elif game.startswith('pick'):
                slot_maps = {slot: fetch_lusa_page(url) for slot, url in LUSA[game]}
                merged, n = merge_picks(game, stored, slot_maps)
                if n:
                    data[game] = merged
                    added[game] = n
            else:
                fmap = fetch_lusa_page(LUSA[game][0][1])
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
