#!/usr/bin/env python3
"""Retry + pacing tests for fetch_draws.py (Nov 2026).

Stubs requests.get and time.sleep; no network use, no real sleeps.
Covers: per-game retry on transient failure, no retry on CONFLICT,
partial-failure rule (a transient failure warns and the rest still
publish; only a CONFLICT blocks the write and leaves the data file
untouched), pacing invoked per LotteryUSA page fetch, and the fuller
browser headers flowing to every request path. Run: python3 phase3-retry-test.py
"""
import copy, io, json, os, sys, tempfile, time
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
_TMP = tempfile.mkdtemp(prefix='cf-retry-')
_DATA = Path(_TMP) / 'draws.json'
os.environ['CF_DATA'] = str(_DATA)

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
            raise val
        if kind == 'page':
            return FakeResp(text=val)
        return FakeResp(rows=val)


def fixture():
    return {
        'rollingcash5': [{'date': '2026-11-03', 'numbers': [1, 2, 3, 4, 5]}],
        'pick3': [{'date': '2026-11-03', 'numbers': [1, 2, 3]}],
        'megamillions': [{'date': '2026-11-03', 'numbers': [1, 2, 3, 4, 5], 'bonus': 6}],
    }


orig_get = fetch_draws.requests.get
orig_sleep = time.sleep
sleeps = []


def record_sleep(s):
    sleeps.append(s)


fetch_draws.time.sleep = record_sleep

try:
    # ---------- 0. constants + headers ----------
    eq(fetch_draws.PACE_SECONDS, 0.25, 'PACE_SECONDS is 0.25')
    eq(fetch_draws.RETRY_SECONDS, 5, 'RETRY_SECONDS is 5')
    eq(fetch_draws.UA['Accept'],
       'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'UA carries full Accept header')
    eq(fetch_draws.UA['Accept-Language'], 'en-US,en;q=0.9',
       'UA carries Accept-Language header')

    # ---------- 1. pacing invoked once per LotteryUSA page fetch ----------
    sleeps.clear()
    fetch_draws.requests.get = StubGet([('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    found = fetch_draws.fetch_lusa_page('https://example.invalid/x')
    eq(found, {'2026-11-04': {'numbers': [6, 7, 8, 9, 10], 'bonus': None}},
       'fetch_lusa_page parses a minimal card')
    eq(sleeps, [0.25], 'one page fetch paces once at PACE_SECONDS')

    # slot game fetches two pages -> two paces
    sleeps.clear()
    stub = StubGet([('page', lusa_page('Nov 4, 2026', [4, 5, 6]))])
    fetch_draws.requests.get = stub
    data = fixture()
    n = fetch_draws.fetch_game('pick3', data)
    eq(n, 2, 'slot game adds midday + evening')
    eq(len(stub.calls), 2, 'slot game fetches two pages')
    eq(sleeps, [0.25, 0.25], 'two page fetches pace twice')

    # ---------- 2a. transient RequestException -> retry succeeds ----------
    sleeps.clear()
    stub = StubGet([('raise', requests.RequestException('403')),
                    ('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    fetch_draws.requests.get = stub
    data = fixture()
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(err, None, 'retry: no error after transient failure')
    eq(n, 1, 'retry: one draw added on second attempt')
    eq(data['rollingcash5'][-1], {'date': '2026-11-04', 'numbers': [6, 7, 8, 9, 10]},
       'retry: new draw stored')
    eq(len(stub.calls), 2, 'retry: fetched twice')
    eq(sleeps, [0.25, 5, 0.25], 'retry: pace, retry pause, pace')

    # ---------- 2b. ValueError without CONFLICT is also retried ----------
    sleeps.clear()
    stub = StubGet([('page', '<html>nothing here</html>'),
                    ('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    fetch_draws.requests.get = stub
    data = fixture()
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(err, None, 'empty-page ValueError retried: no error')
    eq(n, 1, 'empty-page ValueError retried: draw added')
    eq(len(stub.calls), 2, 'empty-page ValueError retried: fetched twice')

    # ---------- 3. fails twice -> error, data untouched ----------
    sleeps.clear()
    stub = StubGet([('raise', requests.RequestException('403'))])
    fetch_draws.requests.get = stub
    data = fixture()
    before = copy.deepcopy(data)
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(n, 0, 'fail-twice: nothing added')
    ok(err is not None and err != '', 'fail-twice: error returned')
    eq(data, before, 'fail-twice: data untouched')
    eq(len(stub.calls), 2, 'fail-twice: fetched exactly twice')
    eq(sleeps, [0.25, 5, 0.25], 'fail-twice: pace, retry pause, pace')

    # ---------- 4. CONFLICT is NOT retried ----------
    sleeps.clear()
    stub = StubGet([('page', lusa_page('Nov 4, 2026', [6, 7, 8, 9, 10]))])
    fetch_draws.requests.get = stub
    data = {'rollingcash5': [{'date': '2026-11-04', 'numbers': [1, 2, 3, 4, 5]}]}
    before = copy.deepcopy(data)
    n, err = fetch_draws.fetch_game_with_retry('rollingcash5', data)
    eq(n, 0, 'conflict: nothing added')
    ok(err is not None and 'CONFLICT' in err, 'conflict: hard error returned')
    eq(data, before, 'conflict: data untouched')
    eq(len(stub.calls), 1, 'conflict: fetched exactly once (no retry)')
    eq(sleeps, [0.25], 'conflict: pace only, no retry pause')

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
    fetch_draws.requests.get = stub
    data = fixture()
    n = fetch_draws.fetch_game('megamillions', data)
    eq(n, 1, 'socrata: one newer draw added')
    eq(sleeps, [], 'socrata: no pacing sleep on the Socrata path')
    ok(any('data.ny.gov' in u for u in stub.calls), 'socrata: NY feed called')

    # ---------- 7. main(): all games failing transiently -> nochange +
    # warnings, exit 0 (partial-failure rule, Nov 2026). Nothing to
    # write, so the file is untouched, but a transient failure no
    # longer aborts the run. ----------
    fetch_draws.requests.get = StubGet([('raise', requests.RequestException('403'))])
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
    fetch_draws.requests.get = StubGet([('raise', requests.RequestException('403')),
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

    # ---------- 9. main(): a CONFLICT anywhere still exits 1, file untouched ----------
    fetch_draws.requests.get = StubGet([('page', lusa_page('Nov 3, 2026', [6, 7, 8, 9, 10]))])
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
    eq(code, 1, 'main conflict: exits 1 on CONFLICT')
    out = json.loads(buf.getvalue().strip().splitlines()[-1])
    eq(out['status'], 'error', 'main conflict: status error')
    eq(_DATA.read_bytes() == before_bytes, True, 'main conflict: data file untouched on error')
finally:
    fetch_draws.requests.get = orig_get
    fetch_draws.time.sleep = orig_sleep

print(f'\n{pass_n} passed, {fail_n} failed')
sys.exit(1 if fail_n else 0)
