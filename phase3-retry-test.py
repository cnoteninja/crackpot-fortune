#!/usr/bin/env python3
"""Retry + pacing tests for fetch_draws.py (Nov 2026).

Stubs the shared SESSION.get and time.sleep; no network use, no real sleeps.
Covers: per-game retry on transient failure, no retry on CONFLICT,
quarantine rule (a CONFLICT skips just that game and is listed in
'conflicts' while the rest still publishes), partial-failure rule,
pacing invoked per LotteryUSA page fetch, the fuller browser headers
flowing to every request path, the run lock, per-row Socrata skips,
early-exit parsing, schedule-aware slot skipping, and the cross-check
of new draws against second views (aggregator, PA/OH official lanes,
bitsandodds fallback) with quarantine on disagreement.
Run: python3 phase3-retry-test.py
"""
import copy, io, json, os, sys, tempfile, time
from contextlib import redirect_stdout
from datetime import datetime as _dt
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
_TMP = tempfile.mkdtemp(prefix='cf-retry-')
_DATA = Path(_TMP) / 'draws.json'
os.environ['CF_DATA'] = str(_DATA)
# The Powerball fast lane must stay a silent no-op in these tests: if a
# MUSL key leaked into the environment, main() would hit the live API.
os.environ.pop('MUSL_API_KEY', None)

import fetch_draws
import requests

pass_n = 0
fail_n = 0


def ok(cond, label):
    global pass_n, fail_n
    if cond:
        pass_n += 1
    else:
        fail_n += 1
        print('FAIL:', label)


def eq(a, b, label):
    ok(a == b, f'{label} (got {json.dumps(a)}, want {json.dumps(b)})')


def lusa_page(date_str, balls):
    """Minimal LotteryUSA year-page fragment with one draw card."""
    lis = ''.join(f'<li class="c-ball">{n}</li>' for n in balls)
    return (f'<div class="c-results-table__item">'
            f'<span class="c-draw-card__draw-date-sub">{date_str}</span>'
            f'<ul>{lis}</ul></div>')


class FakeResp:
    def __init__(self, text='', rows=None, status=200):
        self.text = text
        self._rows = rows
        self.status_code = status

    def json(self):
        return self._rows

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.RequestException(f'HTTP {self.status_code}')


class StubGet:
    """Scripted requests.get. Each action is ('raise', exc), ('page', html)
    or ('json', rows); when the script runs out, the last action repeats.
    Records every call's URL and asserts the fuller browser headers."""
    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def __call__(self, url, headers=None, timeout=None):
        self.calls.append(url)
        assert headers is not None and headers.get('Accept', '').startswith('text/html'), \
            f'Accept header missing on {url}'
        assert headers.get('Accept-Language') == 'en-US,en;q=0.9', \
            f'Accept-Language header missing on {url}'
        idx = min(len(self.calls) - 1, len(self.script) - 1)
        kind, val = self.script[idx]
        if kind == 'raise':
            # Fresh exception each time: re-raising the stored instance
            # would attach a traceback that pins the caller's frame
            # (and, via f_back, main()'s frame and its lock) for as long
            # as this stub lives. Production raises a fresh exception
            # per failed request, so the stub must too.
            raise type(val)(*val.args)
        if kind == 'page':
            return FakeResp(text=val)
        return FakeResp(rows=val)


def fixture():
    return {
        'rollingcash5': [{'date': '2026-11-03', 'numbers': [1, 2, 3, 4, 5]}],
        'pick3': [{'date': '2026-11-03', 'numbers': [1, 2, 3]}],
        'megamillions': [{'date': '2026-11-03', 'numbers': [1, 2, 3, 4, 5], 'bonus': 6}],
    }


orig_get = fetch_draws.SESSION.get
orig_sleep = time.sleep
sleeps = []


def record_sleep(s):
    sleeps.append(s)


fetch_draws.time.sleep = record_sleep

try:
    # ---------- 0. constants + headers ----------
    eq(fetch_draws.PACE_SECONDS, 1.0, 'PACE_SECONDS is 1.0 (Nov 27 bot-protection probe)')
    eq(fetch_draws.RETRY_SECONDS, 5, 'RETRY_SECONDS is 5')
    eq(fetch_draws.UA['Accept'],
       'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'UA carries full Accept header')
    eq(fetch_draws.UA['Accept-Language'], 'en-US,en;q=0.9',
       'UA carries Accept-Language header')

    # ---------- 1. pacing invoked once per LotteryUSA page fetch ----------
    sleeps.clear()
    fetch_draws.SESSION.get = StubGet([('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    found = fetch_draws.fetch_lusa_page('https://example.invalid/x')
    eq(found, {'2026-11-04': {'numbers': [6, 7, 8, 9, 10], 'bonus': None}},
       'fetch_lusa_page parses a minimal card')
    eq(sleeps, [fetch_draws.PACE_SECONDS], 'one page fetch paces once at PACE_SECONDS')

    # slot game fetches two pages -> two paces
    sleeps.clear()
    stub = StubGet([('page', lusa_page('Nov 4, 2026', [4, 5, 6]))])
    fetch_draws.SESSION.get = stub
    data = fixture()
    n = fetch_draws.fetch_game('pick3', data)
    eq(n, 2, 'slot game adds midday + evening')
    eq(len(stub.calls), 2, 'slot game fetches two pages')
    eq(sleeps, [fetch_draws.PACE_SECONDS] * 2, 'two page fetches pace twice')

    # ---------- 2a. transient RequestException -> retry succeeds ----------
    sleeps.clear()
    stub = StubGet([('raise', requests.RequestException('403')),
                    ('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    fetch_draws.SESSION.get = stub
    data = fixture()
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(err, None, 'retry: no error after transient failure')
    eq(n, 1, 'retry: one draw added on second attempt')
    eq(data['rollingcash5'][-1], {'date': '2026-11-04', 'numbers': [6, 7, 8, 9, 10]},
       'retry: new draw stored')
    eq(len(stub.calls), 2, 'retry: fetched twice')
    eq(sleeps, [fetch_draws.PACE_SECONDS, 5, fetch_draws.PACE_SECONDS], 'retry: pace, retry pause, pace')

    # ---------- 2b. ValueError without CONFLICT is also retried ----------
    sleeps.clear()
    stub = StubGet([('page', '<html>nothing here</html>'),
                    ('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    fetch_draws.SESSION.get = stub
    data = fixture()
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(err, None, 'empty-page ValueError retried: no error')
    eq(n, 1, 'empty-page ValueError retried: draw added')
    eq(len(stub.calls), 2, 'empty-page ValueError retried: fetched twice')

    # ---------- 3. fails twice -> error, data untouched ----------
    sleeps.clear()
    stub = StubGet([('raise', requests.RequestException('403'))])
    fetch_draws.SESSION.get = stub
    data = fixture()
    before = copy.deepcopy(data)
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(n, 0, 'fail-twice: nothing added')
    ok(err is not None and err != '', 'fail-twice: error returned')
    eq(data, before, 'fail-twice: data untouched')
    eq(len(stub.calls), 2, 'fail-twice: fetched exactly twice')
    eq(sleeps, [fetch_draws.PACE_SECONDS, 5, fetch_draws.PACE_SECONDS], 'fail-twice: pace, retry pause, pace')

    # ---------- 4. CONFLICT is NOT retried ----------
    sleeps.clear()
    stub = StubGet([('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    fetch_draws.SESSION.get = stub
    data = {'rollingcash5': [{'date': '2026-11-04', 'numbers': [1, 2, 3, 4, 5]}]}
    before = copy.deepcopy(data)
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(n, 0, 'conflict: nothing added')
    ok(err is not None and 'CONFLICT' in err, 'conflict: hard error returned')
    eq(data, before, 'conflict: data untouched')
    eq(len(stub.calls), 1, 'conflict: fetched exactly once (no retry)')
    eq(sleeps, [fetch_draws.PACE_SECONDS], 'conflict: pace only, no retry pause')

    # ---------- 5. _is_retryable classification ----------
    ok(fetch_draws._is_retryable(requests.RequestException('x')),
       'retryable: RequestException')
    ok(fetch_draws._is_retryable(ValueError('no draw cards parsed')),
       'retryable: plain ValueError')
    ok(not fetch_draws._is_retryable(ValueError('x: CONFLICT on 2026-11-04')),
       'not retryable: CONFLICT ValueError')
    ok(not fetch_draws._is_retryable(KeyError('x')), 'not retryable: KeyError')
    ok(not fetch_draws._is_retryable(TypeError('x')), 'not retryable: TypeError')

    # ---------- 6. Socrata path: headers flow, no pacing ----------
    sleeps.clear()
    ds, bf = fetch_draws.SOCRATA['megamillions']
    rows = [{'draw_date': '2026-11-04T00:00:00.000',
             'winning_numbers': '1 2 3 4 5', bf: '6'}]
    stub = StubGet([('json', rows)])
    fetch_draws.SESSION.get = stub
    data = fixture()
    n = fetch_draws.fetch_game('megamillions', data)
    eq(n, 1, 'socrata: one newer draw added')
    eq(sleeps, [], 'socrata: no pacing sleep on the Socrata path')
    ok(any('data.ny.gov' in u for u in stub.calls), 'socrata: NY feed called')

    # ---------- 7. main(): all games failing transiently -> nochange +
    # warnings, exit 0 (partial-failure rule, Nov 2026). Nothing to
    # write, so the file is untouched, but a transient failure no
    # longer aborts the run. ----------
    fetch_draws.SESSION.get = StubGet([('raise', requests.RequestException('403'))])
    data = fixture()
    _DATA.write_text(json.dumps(data))
    before_bytes = _DATA.read_bytes()
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            fetch_draws.main()
            code = None
        except SystemExit as e:
            code = e.code
    eq(code, None, 'main: no exit when every game fails transiently')
    out = json.loads(buf.getvalue().strip().splitlines()[-1])
    eq(out['status'], 'nochange', 'main: status nochange when nothing added')
    ok(bool(out.get('warnings')), 'main: warnings listed when games fail transiently')
    eq(_DATA.read_bytes() == before_bytes, True, 'main: data file untouched when nothing added')

    # ---------- 8. main(): partial success publishes, warnings listed ----------
    # First call raises (pick3's first attempt), every later call returns
    # a 3-ball card: the 3-number games succeed, everything needing
    # another count fails validation, Socrata gets HTML instead of JSON.
    # So some games publish while the rest warn.
    fetch_draws.SESSION.get = StubGet([('raise', requests.RequestException('403')),
                                        ('page', lusa_page('Nov 4, 2026', [6, 7, 8]))])
    data = fixture()
    _DATA.write_text(json.dumps(data))
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            fetch_draws.main()
            code = None
        except SystemExit as e:
            code = e.code
    eq(code, None, 'main partial: no exit on partial success')
    out = json.loads(buf.getvalue().strip().splitlines()[-1])
    eq(out['status'], 'updated', 'main partial: status updated')
    eq(out['added'].get('pick3'), 2, 'main partial: pick3 published 2 draws')
    ok(bool(out.get('warnings')), 'main partial: warnings listed for failed games')
    written = json.loads(_DATA.read_text())
    eq(written['pick3'][-1], {'date': '2026-11-04', 'numbers': [6, 7, 8]},
       'main partial: pick3 new draw in file')

    # ---------- 9. main(): a CONFLICT quarantines just that game — the
    # run still exits 0, the conflict is listed, the conflicted game's
    # stored draws are untouched, and unrelated games still publish.
    # (The stub's 5-ball page also conflicts pick3's stored Nov 3, and
    # legitimately adds Nov 3 to the empty 5-ball games.) ----------
    fetch_draws.SESSION.get = StubGet([('page', lusa_page('Nov 3, 2026', [6, 7, 8, 9, 10]))])
    data = fixture()
    _DATA.write_text(json.dumps(data))
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            fetch_draws.main()
            code = None
        except SystemExit as e:
            code = e.code
    eq(code, None, 'main conflict: no exit on quarantined CONFLICT')
    out = json.loads(buf.getvalue().strip().splitlines()[-1])
    eq(out['status'], 'updated', 'main conflict: status updated (clean games still publish)')
    ok(any('rollingcash5' in c for c in out.get('conflicts', [])),
       'main conflict: conflict listed for rollingcash5')
    written = json.loads(_DATA.read_text())
    eq(written['rollingcash5'], [{'date': '2026-11-03', 'numbers': [1, 2, 3, 4, 5]}],
       'main conflict: conflicted game untouched in file')
    eq(written['pick3'], [{'date': '2026-11-03', 'numbers': [1, 2, 3]}],
       'main conflict: pick3 stored draw untouched in file')
    # ---------- 10. quarantine: a CONFLICT skips just that game while
    # the rest publishes. URL-aware stub: pick-3 pages good, rolling
    # cash 5 conflicts, everything else 403s. ----------
    class URLMapGet:
        def __init__(self):
            self.calls = []
        def __call__(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if 'rolling-cash-5' in url:
                return FakeResp(text=lusa_page('Nov 3, 2026', [6, 7, 8, 9, 10]))
            if 'pick-3' in url:
                return FakeResp(text=lusa_page('Nov 4, 2026', [6, 7, 8]))
            raise requests.RequestException('403')
    fetch_draws.SESSION.get = URLMapGet()
    data = fixture()
    _DATA.write_text(json.dumps(data))
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            fetch_draws.main()
            code = None
        except SystemExit as e:
            code = e.code
    eq(code, None, 'quarantine: no exit')
    out = json.loads(buf.getvalue().strip().splitlines()[-1])
    eq(out['status'], 'updated', 'quarantine: status updated (clean games published)')
    eq(out['added'].get('pick3'), 2, 'quarantine: pick3 published 2 draws')
    ok(bool(out.get('conflicts')) and 'rollingcash5' in out['conflicts'][0],
       'quarantine: conflict listed for rollingcash5')
    written = json.loads(_DATA.read_text())
    eq(written['rollingcash5'], [{'date': '2026-11-03', 'numbers': [1, 2, 3, 4, 5]}],
       'quarantine: conflicted game untouched in file')
    eq(written['pick3'][-1], {'date': '2026-11-04', 'numbers': [6, 7, 8]},
       'quarantine: clean game published in file')

    # ---------- 11. run lock: a held lock prints {"status": "locked"}
    # and exits 0 without touching data; after release the run proceeds.
    import fcntl
    lock_path = Path(_TMP) / 'test.lock'
    _orig_lock = fetch_draws.LOCK_PATH
    fetch_draws.LOCK_PATH = lock_path
    data = fixture()
    _DATA.write_text(json.dumps(data))
    before_bytes = _DATA.read_bytes()
    with open(lock_path, 'w') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        buf = io.StringIO()
        with redirect_stdout(buf):
            try:
                fetch_draws.main()
                code = None
            except SystemExit as e:
                code = e.code
        eq(code, None, 'lock: no exit when locked')
        out = json.loads(buf.getvalue().strip().splitlines()[-1])
        eq(out['status'], 'locked', 'lock: status locked')
        eq(_DATA.read_bytes() == before_bytes, True, 'lock: data file untouched')
    fetch_draws.SESSION.get = StubGet([('raise', requests.RequestException('403'))])
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            fetch_draws.main()
            code = None
        except SystemExit as e:
            code = e.code
    out = json.loads(buf.getvalue().strip().splitlines()[-1])
    eq(out['status'], 'nochange', 'lock: run proceeds after release')
    fetch_draws.LOCK_PATH = _orig_lock

    # ---------- 12. Socrata slots: one malformed row is skipped with a
    # warning while newer draws still land; the query starts from the
    # NEWEST stored date (minus one day of overlap), not the first. ----------
    rows = [
        {'draw_date': '2026-11-14T00:00:00.000', 'midday_daily': '123', 'evening_daily': '456'},
        {'draw_date': '2026-11-15T00:00:00.000', 'midday_daily': '5', 'evening_daily': '789'},
        {'draw_date': '2026-11-16T00:00:00.000', 'midday_daily': '012', 'evening_daily': '345'},
    ]
    stub = StubGet([('json', rows)])
    fetch_draws.SESSION.get = stub
    data = {'ny_numbers': [{'date': '2026-11-14', 'numbers': [1, 2, 3]},
                           {'date': '2026-11-14', 'numbers': [4, 5, 6]}]}
    warnings = []
    n = fetch_draws.fetch_game('ny_numbers', data, warnings)
    eq(n, 3, 'slots: 3 good draws added past the bad row')
    ok(any('skipping malformed row 2026-11-15 midday' in w for w in warnings),
       'slots: bad row warned, not fatal')
    eq(data['ny_numbers'][-1], {'date': '2026-11-16', 'numbers': [3, 4, 5]},
       'slots: newest draw stored')
    ok("draw_date>'2026-11-13" in stub.calls[0],
       'slots: query starts at newest stored minus one day')

    # ---------- 13. early-exit parse: a cutoff returns exactly the
    # full parse filtered to newer cards; all-old returns {} (no raise);
    # a cardless page still raises. ----------
    def multi_page(dates_balls):
        return ''.join(lusa_page(d, b) for d, b in dates_balls)
    html = multi_page([('Nov 10, 2026', [1, 2, 3]), ('Nov 9, 2026', [4, 5, 6]),
                       ('Nov 8, 2026', [7, 8, 9]), ('Nov 7, 2026', [1, 1, 1]),
                       ('Nov 6, 2026', [2, 2, 2])])
    fetch_draws.SESSION.get = StubGet([('page', html)])
    full = fetch_draws.fetch_lusa_page('https://example.invalid/x')
    eq(len(full), 5, 'early-exit: full parse sees all 5 cards')
    fetch_draws.SESSION.get = StubGet([('page', html)])
    cut = fetch_draws.fetch_lusa_page('https://example.invalid/x', cutoff='2026-11-07')
    eq(cut, {d: v for d, v in full.items() if d > '2026-11-07'},
       'early-exit: cutoff result equals full parse filtered to newer cards')
    fetch_draws.SESSION.get = StubGet([('page', html)])
    cut2 = fetch_draws.fetch_lusa_page('https://example.invalid/x', cutoff='2026-11-10')
    eq(cut2, {}, 'early-exit: all-old page returns {} without raising')
    fetch_draws.SESSION.get = StubGet([('page', '<html>nothing here</html>')])
    try:
        fetch_draws.fetch_lusa_page('https://example.invalid/x')
        raised = False
    except ValueError:
        raised = True
    ok(raised, 'early-exit: cardless page still raises')

    # ---------- 14. schedule-aware slot skip: a complete record skips
    # the page; a missing slot always forces a fetch. ----------
    from zoneinfo import ZoneInfo as _ZI
    ET = _ZI('America/New_York')
    # complete through today (Nov 27) at 14:00 ET: both slots skipped
    data = {'pick3': [{'date': '2026-11-26', 'numbers': [1, 1, 1]},
                      {'date': '2026-11-26', 'numbers': [2, 2, 2]},
                      {'date': '2026-11-27', 'numbers': [3, 3, 3]},
                      {'date': '2026-11-27', 'numbers': [4, 4, 4]}]}
    stub = StubGet([('page', lusa_page('Nov 27, 2026', [9, 9, 9]))])
    fetch_draws.SESSION.get = stub
    now = _dt(2026, 11, 27, 14, 0, tzinfo=ET)
    n = fetch_draws.fetch_game('pick3', data, [], now)
    eq(n, 0, 'schedule: complete record adds nothing')
    eq(len(stub.calls), 0, 'schedule: complete record fetches no pages')
    # today has only midday at 20:00 ET: both pages fetched (ambiguous)
    class TwoPageGet:
        def __init__(self):
            self.calls = []
        def __call__(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if 'midday-pick-3' in url:
                return FakeResp(text=lusa_page('Nov 27, 2026', [3, 3, 3]))
            return FakeResp(text=lusa_page('Nov 27, 2026', [5, 5, 5]))
    data = {'pick3': [{'date': '2026-11-26', 'numbers': [1, 1, 1]},
                      {'date': '2026-11-26', 'numbers': [2, 2, 2]},
                      {'date': '2026-11-27', 'numbers': [3, 3, 3]}]}
    stub = TwoPageGet()
    fetch_draws.SESSION.get = stub
    now = _dt(2026, 11, 27, 20, 0, tzinfo=ET)
    n = fetch_draws.fetch_game('pick3', data, [], now)
    eq(len(stub.calls), 2, 'schedule: incomplete day fetches both pages')
    eq(n, 1, 'schedule: incomplete day picks up the missing evening')
    # complete through yesterday at 13:43 ET: midday fetched, evening skipped
    data = {'pick3': [{'date': '2026-11-26', 'numbers': [1, 1, 1]},
                      {'date': '2026-11-26', 'numbers': [2, 2, 2]}]}
    stub = StubGet([('page', lusa_page('Nov 27, 2026', [6, 6, 6]))])
    fetch_draws.SESSION.get = stub
    now = _dt(2026, 11, 27, 13, 43, tzinfo=ET)
    n = fetch_draws.fetch_game('pick3', data, [], now)
    eq(len(stub.calls), 1, 'schedule: midday run fetches midday only')
    ok('midday-pick-3' in stub.calls[0], 'schedule: the one fetch is the midday page')
    eq(n, 1, 'schedule: midday run adds today midday')

    # ---------- 15. one shared session carries every request. ----------
    ok(isinstance(fetch_draws.SESSION, requests.Session),
       'session: SESSION is a shared requests.Session')
    stub = StubGet([('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    fetch_draws.SESSION.get = stub
    fetch_draws.fetch_lusa_page('https://example.invalid/x')
    eq(stub.calls, ['https://example.invalid/x'], 'session: LUSA fetch goes through SESSION.get')

    # ---------- 16. cross-check (item 5): new draws are verified against
    # a second view; a disagreement quarantines the draw, a stale or
    # missing second view is no verdict, and a dead source never blocks
    # the run. ----------
    AGG = {
        'multiState': [],
        'jurisdictions': [
            {'code': 'OH', 'name': 'Ohio', 'slug': 'ohio', 'draws': [
                {'gameId': 'oh-pick-3-midday', 'game': 'Pick 3',
                 'label': 'Pick 3 (Midday)', 'multiState': False,
                 'drawDate': '2026-11-27', 'numbers': [3, 3, 3],
                 'bonus': None, 'multiplier': None, 'jackpot': None,
                 'nextDrawDate': '2026-11-27', 'nextJackpot': None},
                {'gameId': 'oh-pick-3-evening', 'game': 'Pick 3',
                 'label': 'Pick 3 (Evening)', 'multiState': False,
                 'drawDate': '2026-11-26', 'numbers': [1, 2, 3],
                 'bonus': None, 'multiplier': None, 'jackpot': None,
                 'nextDrawDate': '2026-11-27', 'nextJackpot': None},
                {'gameId': 'oh-rolling-cash-5', 'game': 'Rolling Cash 5',
                 'label': 'Rolling Cash 5', 'multiState': False,
                 'drawDate': '2026-11-27', 'numbers': [1, 2, 3, 4, 5],
                 'bonus': None, 'multiplier': None, 'jackpot': None,
                 'nextDrawDate': '2026-11-27', 'nextJackpot': None},
            ]},
            {'code': 'PA', 'name': 'Pennsylvania', 'slug': 'pennsylvania', 'draws': [
                {'gameId': 'pa-pick-3-day', 'game': 'Pick 3',
                 'label': 'Pick 3 (Midday)', 'multiState': False,
                 'drawDate': '2026-11-27', 'numbers': [0, 4, 7],
                 'bonus': None, 'multiplier': None, 'jackpot': None,
                 'nextDrawDate': '2026-11-27', 'nextJackpot': None},
            ]},
            {'code': 'NC', 'name': 'North Carolina', 'slug': 'north-carolina', 'draws': [
                {'gameId': 'nc-pick-3-day-midday', 'game': 'Pick 3',
                 'label': 'Pick 3 (Midday)', 'multiState': False,
                 'drawDate': '2026-11-27', 'numbers': [4, 8, 0],
                 'bonus': None, 'multiplier': None, 'jackpot': None,
                 'nextDrawDate': '2026-11-27', 'nextJackpot': None},
            ]},
        ],
    }
    PA_FEED = [
        {'DrawingName': 'PICK 3 (DAY)', 'GameName': 'PICK 3', 'GameBallCount': 3,
         'DrawingNumbersAsList': ['0', '4', '6', '4'], 'IsMidDayDrawing': True,
         'DrawingDateAsString': '11/27/2026',
         'RelatedEveningDrawing': {'DrawingDateAsString': '11/26/2026',
                                   'DrawingNumbersAsList': ['2', '9', '4', '3']}},
        {'DrawingName': 'CASH 5', 'GameName': 'CASH 5', 'GameBallCount': 5,
         'DrawingNumbersAsList': ['1', '2', '3', '4', '5'], 'IsMidDayDrawing': False,
         'DrawingDateAsString': '11/27/2026', 'RelatedEveningDrawing': {}},
    ]
    OH_BLOB_2ROW = [
        {'DrawGameId': 13, 'Name': 'Pick 3', 'Draws': [
            {'DrawDate': '2026-11-27T00:00:00',
             'Numbers': [{'Value': 4, 'Position': 1}, {'Value': 2, 'Position': 2},
                         {'Value': 6, 'Position': 3}]},
            {'DrawDate': '2026-11-27T00:00:00',
             'Numbers': [{'Value': 1, 'Position': 1}, {'Value': 1, 'Position': 2},
                         {'Value': 1, 'Position': 3}]},
        ]},
        {'DrawGameId': 11, 'Name': 'Lucky for Life', 'Draws': [
            {'DrawDate': '2026-02-21T00:00:00',
             'Numbers': [{'Value': 1, 'Position': 1}]},
        ]},
    ]
    OH_HTML_2ROW = ('<input type="hidden" id="cmsNumbers" value=\''
                    + json.dumps(OH_BLOB_2ROW) + '\'>')
    OH_BLOB_1ROW = [
        {'DrawGameId': 13, 'Name': 'Pick 3', 'Draws': [
            {'DrawDate': '2026-11-27T00:00:00',
             'Numbers': [{'Value': 4, 'Position': 1}, {'Value': 2, 'Position': 2},
                         {'Value': 6, 'Position': 3}]},
        ]},
    ]
    OH_HTML_1ROW = ('<input type="hidden" id="cmsNumbers" value=\''
                    + json.dumps(OH_BLOB_1ROW) + '\'>')
    BO_HTML = ('<html><body>'
               '<table><tr><th>Other</th></tr><tr><td>x</td></tr></table>'
               '<table><tr><th>Date</th><th>Draw</th><th>Numbers</th></tr>'
               '<tr><td>November 27, 2026</td><td>Midday</td>'
               '<td class="num nums">4 0 7</td></tr>'
               '<tr><td>November 27, 2026</td><td>Evening</td>'
               '<td class="num nums">1 2 3</td></tr>'
               '<tr><td>November 26, 2026</td><td>Midday</td>'
               '<td class="num nums">9 9 9</td></tr>'
               '</table></body></html>')

    class XcStub:
        """URL-keyed cross-check stub. Each source defaults to an empty
        (no-verdict) answer; 'fail' names the sources that raise."""
        def __init__(self, agg=None, pa=None, oh=None, bo=None, fail=()):
            self.agg, self.pa, self.oh, self.bo = agg, pa, oh, bo
            self.fail = set(fail)
            self.calls = []
        def __call__(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if 'usalotteryresults' in url:
                if 'agg' in self.fail:
                    raise requests.RequestException('503')
                return FakeResp(rows=self.agg if self.agg is not None else {})
            if 'palottery' in url:
                if 'pa' in self.fail:
                    raise requests.RequestException('503')
                return FakeResp(rows=self.pa if self.pa is not None else [])
            if 'ohiolottery' in url:
                if 'oh' in self.fail:
                    raise requests.RequestException('503')
                return FakeResp(text=self.oh if self.oh is not None else '<html></html>')
            if 'bitsandodds' in url:
                if 'bo' in self.fail:
                    raise requests.RequestException('503')
                return FakeResp(text=self.bo if self.bo is not None else '<html></html>')
            raise AssertionError(f'unexpected cross-check URL {url}')

    def xc_run(new_draws, data, added, stub, now=None):
        fetch_draws.SESSION.get = stub
        if now is None:
            now = _dt(2026, 11, 27, 14, 0, tzinfo=ET)
        return fetch_draws.crosscheck_new_draws(new_draws, data, added, now)

    # aggregator: confirm / stale row / missing game
    data = {'pick3': [{'date': '2026-11-26', 'numbers': [1, 2, 3]},
                      {'date': '2026-11-26', 'numbers': [2, 2, 2]},
                      {'date': '2026-11-27', 'numbers': [3, 3, 3]},
                      {'date': '2026-11-27', 'numbers': [5, 5, 5]}],
            'classiclotto': [{'date': '2026-11-27', 'numbers': [1, 2, 3, 4, 5, 6]}]}
    added = {'pick3': 2, 'classiclotto': 1}
    draws = [{'game': 'pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [3, 3, 3], 'bonus': None},
             {'game': 'pick3', 'date': '2026-11-27', 'slot': 'evening',
              'numbers': [5, 5, 5], 'bonus': None},
             {'game': 'classiclotto', 'date': '2026-11-27', 'slot': None,
              'numbers': [1, 2, 3, 4, 5, 6], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(agg=AGG, fail=('pa', 'oh')))
    eq(s['confirmed'], 1, 'xc: aggregator confirms the matching draw')
    eq(s['no_verdict'], 2, 'xc: stale row + missing game are no verdict')
    eq(s['quarantined'], [], 'xc: no quarantine on confirm/no-verdict')
    eq(added, {'pick3': 2, 'classiclotto': 1}, 'xc: confirmed draws stay in added')

    # aggregator mismatch quarantines (removed from data, added decremented)
    data = {'pick3': [{'date': '2026-11-26', 'numbers': [1, 2, 3]},
                      {'date': '2026-11-27', 'numbers': [3, 3, 4]}]}
    added = {'pick3': 1}
    draws = [{'game': 'pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [3, 3, 4], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(agg=AGG, fail=('pa', 'oh')))
    eq(len(s['quarantined']), 1, 'xc: aggregator mismatch quarantines')
    q = s['quarantined'][0]
    eq(q['numbers'], [3, 3, 4], 'xc: quarantine lists the primary numbers')
    eq(q['second'], [3, 3, 3], 'xc: quarantine lists the second-source numbers')
    eq(q['source'], 'usalotteryresults', 'xc: quarantine names the source')
    eq(data['pick3'], [{'date': '2026-11-26', 'numbers': [1, 2, 3]}],
       'xc: quarantined draw removed from data')
    eq(added, {}, 'xc: quarantined draw decrements added to zero')

    # digit order is the result; ball games compare sorted
    data = {'nc_pick3': [{'date': '2026-11-27', 'numbers': [0, 8, 4]}]}
    added = {'nc_pick3': 1}
    draws = [{'game': 'nc_pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [0, 8, 4], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(agg=AGG, fail=('pa', 'oh')))
    eq(len(s['quarantined']), 1, 'xc: digit order mismatch quarantines')
    agg2 = copy.deepcopy(AGG)
    for j in agg2['jurisdictions']:
        for r in j['draws']:
            if r['gameId'] == 'nc-pick-3-day-midday':
                r['numbers'] = [0, 8, 4]
    data = {'nc_pick3': [{'date': '2026-11-27', 'numbers': [0, 8, 4]}]}
    added = {'nc_pick3': 1}
    draws = [{'game': 'nc_pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [0, 8, 4], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(agg=agg2, fail=('pa', 'oh')))
    eq(s['confirmed'], 1, 'xc: digit exact order confirms')
    data = {'rollingcash5': [{'date': '2026-11-27', 'numbers': [5, 4, 3, 2, 1]}]}
    added = {'rollingcash5': 1}
    draws = [{'game': 'rollingcash5', 'date': '2026-11-27', 'slot': None,
              'numbers': [5, 4, 3, 2, 1], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(agg=AGG, fail=('pa', 'oh')))
    eq(s['confirmed'], 1, 'xc: ball games compare sorted')

    # PA: the Wild Ball (last element) is dropped; the related evening
    # drawing confirms the previous evening's draw
    data = {'pa_pick3': [{'date': '2026-11-27', 'numbers': [0, 4, 6]},
                         {'date': '2026-11-26', 'numbers': [2, 9, 4]}]}
    added = {'pa_pick3': 2}
    draws = [{'game': 'pa_pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [0, 4, 6], 'bonus': None},
             {'game': 'pa_pick3', 'date': '2026-11-26', 'slot': 'evening',
              'numbers': [2, 9, 4], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(pa=PA_FEED, fail=('agg', 'oh')))
    eq(s['confirmed'], 2, 'xc: PA Wild Ball dropped, day + related evening confirm')
    eq(s['quarantined'], [], 'xc: PA draws not quarantined')

    # OH: two same-date rows — first = evening, last = midday
    data = {'pick3': [{'date': '2026-11-27', 'numbers': [4, 2, 6]},
                      {'date': '2026-11-27', 'numbers': [1, 1, 1]}]}
    added = {'pick3': 2}
    draws = [{'game': 'pick3', 'date': '2026-11-27', 'slot': 'evening',
              'numbers': [4, 2, 6], 'bonus': None},
             {'game': 'pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [1, 1, 1], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(oh=OH_HTML_2ROW, fail=('agg', 'pa')))
    eq(s['confirmed'], 2, 'xc: OH two-row slot positions confirm')
    data = {'pick3': [{'date': '2026-11-27', 'numbers': [4, 2, 6]}]}
    added = {'pick3': 1}
    draws = [{'game': 'pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [4, 2, 6], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(oh=OH_HTML_2ROW, fail=('agg', 'pa')))
    eq(len(s['quarantined']), 1, 'xc: OH midday draw matching the evening row quarantines')
    eq(data['pick3'], [], 'xc: quarantined OH draw removed from data')
    # OH: a single row is disambiguated by the clock (evening 19:30)
    data = {'pick3': [{'date': '2026-11-27', 'numbers': [4, 2, 6]}]}
    added = {'pick3': 1}
    draws = [{'game': 'pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [4, 2, 6], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(oh=OH_HTML_1ROW, fail=('agg', 'pa')),
               now=_dt(2026, 11, 27, 13, 0, tzinfo=ET))
    eq(s['confirmed'], 1, 'xc: OH single row at 13:00 confirms midday')
    draws = [{'game': 'pick3', 'date': '2026-11-27', 'slot': 'evening',
              'numbers': [4, 2, 6], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(oh=OH_HTML_1ROW, fail=('agg', 'pa')),
               now=_dt(2026, 11, 27, 13, 0, tzinfo=ET))
    eq(s['no_verdict'], 1, 'xc: OH single row at 13:00 is no verdict for evening')
    s = xc_run(draws, data, added, XcStub(oh=OH_HTML_1ROW, fail=('agg', 'pa')),
               now=_dt(2026, 11, 27, 20, 0, tzinfo=ET))
    eq(s['confirmed'], 1, 'xc: OH single row at 20:00 confirms evening')

    # bitsandodds fallback: finds the Date|Draw|Numbers table (skipping
    # other tables) and confirms / mismatches on it
    data = {'nc_pick3': [{'date': '2026-11-27', 'numbers': [4, 0, 7]}]}
    added = {'nc_pick3': 1}
    draws = [{'game': 'nc_pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [4, 0, 7], 'bonus': None}]
    stub = XcStub(bo=BO_HTML, fail=('agg', 'pa', 'oh'))
    s = xc_run(draws, data, added, stub)
    eq(s['confirmed'], 1, 'xc: bitsandodds table confirms')
    ok(any('bitsandodds.com/north-carolina/lottery/pick-3/numbers/' in c
           for c in stub.calls), 'xc: bitsandodds page fetched for the right game')
    data = {'nc_pick3': [{'date': '2026-11-27', 'numbers': [4, 0, 8]}]}
    added = {'nc_pick3': 1}
    draws = [{'game': 'nc_pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [4, 0, 8], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(bo=BO_HTML, fail=('agg', 'pa', 'oh')))
    eq(len(s['quarantined']), 1, 'xc: bitsandodds mismatch quarantines')
    eq(s['quarantined'][0]['source'], 'bitsandodds',
       'xc: quarantine names bitsandodds')

    # every second source dead: no verdict, never a failure
    data = {'pick3': [{'date': '2026-11-27', 'numbers': [3, 3, 3]}]}
    added = {'pick3': 1}
    draws = [{'game': 'pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [3, 3, 3], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(fail=('agg', 'pa', 'oh', 'bo')))
    eq(s, {'confirmed': 0, 'no_verdict': 1, 'contested': 0, 'quarantined': []},
       'xc: dead second sources are no verdict, never a failure')
    eq(data['pick3'], [{'date': '2026-11-27', 'numbers': [3, 3, 3]}],
       'xc: dead sources leave the draw published')

    # an official confirm beats an aggregator mismatch (contested)
    data = {'pa_pick3': [{'date': '2026-11-27', 'numbers': [0, 4, 6]}]}
    added = {'pa_pick3': 1}
    draws = [{'game': 'pa_pick3', 'date': '2026-11-27', 'slot': 'midday',
              'numbers': [0, 4, 6], 'bonus': None}]
    s = xc_run(draws, data, added, XcStub(agg=AGG, pa=PA_FEED, fail=('oh',)))
    eq(s['confirmed'], 1, 'xc: official confirm wins over aggregator mismatch')
    eq(s['contested'], 1, 'xc: contested counted when sources disagree')
    eq(data['pa_pick3'], [{'date': '2026-11-27', 'numbers': [0, 4, 6]}],
       'xc: contested draw stays published')

    # main(): the run cross-checks its new draws and reports the summary
    def full_fixture():
        data = {}
        for game, (count, _mx, has_bonus, _bm, _dist) in fetch_draws.RULES.items():
            nums = list(range(1, count + 1))
            entry = {'date': '2026-11-03', 'numbers': nums}
            if has_bonus:
                entry['bonus'] = 1
            if game in fetch_draws.SLOT_GAMES:
                data[game] = [dict(entry) for _ in fetch_draws.LUSA[game]]
            else:
                data[game] = [entry]
        return data

    class MainXcGet:
        def __init__(self):
            self.calls = []
        def __call__(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if 'usalotteryresults' in url:
                return FakeResp(rows=AGG)
            if 'palottery' in url or 'ohiolottery' in url or 'bitsandodds' in url:
                raise requests.RequestException('503')
            if 'megamillions.com' in url:
                raise requests.RequestException('503')
            if 'data.ny.gov' in url:
                return FakeResp(rows=[])
            if '/ohio/midday-pick-3/' in url:
                return FakeResp(text=lusa_page('Nov 27, 2026', [3, 3, 3]))
            if 'lotteryusa.com' in url:
                return FakeResp(text=lusa_page('Nov 3, 2026', [1, 2, 3]))
            raise AssertionError(f'unexpected URL {url}')
    fetch_draws.SESSION.get = MainXcGet()
    _DATA.write_text(json.dumps(full_fixture()))
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            fetch_draws.main()
            code = None
        except SystemExit as e:
            code = e.code
    eq(code, None, 'main xc: no exit')
    out = json.loads(buf.getvalue().strip().splitlines()[-1])
    eq(out['status'], 'updated', 'main xc: status updated')
    eq(out['added'], {'pick3': 1}, 'main xc: only the new pick3 draw added')
    xc = out.get('crosscheck', {})
    eq(xc.get('confirmed'), 1, 'main xc: crosscheck confirmed the new draw')
    eq(xc.get('quarantined'), [], 'main xc: nothing quarantined')
    written = json.loads(_DATA.read_text())
    eq(written['pick3'][-1], {'date': '2026-11-27', 'numbers': [3, 3, 3]},
       'main xc: new draw in file')

finally:
    fetch_draws.SESSION.get = orig_get
    fetch_draws.time.sleep = orig_sleep

print(f'\n{pass_n} passed, {fail_n} failed')
sys.exit(1 if fail_n else 0)
