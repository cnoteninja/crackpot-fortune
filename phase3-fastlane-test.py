#!/usr/bin/env python3
"""Fast-lane tests for the Mega Millions official-site feed (Nov 2026).

Stubs requests.get for the GetLatestDrawData endpoint; no network use.
Covers: add-missing-draw, no-op, conflict (hard error), endpoint down,
malformed responses, out-of-range numbers, stale-feed jackpot guard,
jackpot shape, and build.py's newest-date computation with jackpots
present. Run: python3 phase3-fastlane-test.py
"""
import copy, json, os, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
_TMP = tempfile.mkdtemp(prefix='cf-fastlane-')
os.environ['CF_DATA'] = str(Path(_TMP) / 'draws.json')

import build
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


def mm_payload(n=(4, 24, 31, 62, 70), mb=3, play_date='2026-11-03T00:00:00',
               next_pool=505000000.0, next_draw='2026-11-06T23:00:00',
               drop=(), raw=None):
    """Canned GetLatestDrawData body. drop: top-level keys to remove from
    the Drawing dict (e.g. ('N3',)). raw: if set, returned verbatim."""
    if raw is not None:
        return raw
    drawing = {"PlayDate": play_date, "N1": n[0], "N2": n[1], "N3": n[2],
               "N4": n[3], "N5": n[4], "MBall": mb, "Megaplier": -1,
               "UpdatedBy": "SERVICE", "UpdatedTime": "2026-11-03T23:08:02",
               "PlayDateTicks": 639292608000000000, "IgnoreServiceUntil": None}
    for k in drop:
        drawing.pop(k, None)
    p = {"Drawing": drawing,
         "Jackpot": {"PlayDate": play_date, "CurrentPrizePool": 482000000.0,
                     "NextPrizePool": next_pool, "CurrentCashValue": 203600000.0,
                     "NextCashValue": 213300000.0, "Winners": 0, "Verified": True},
         "NextDrawingDate": next_draw, "PrizeTiers": [], "MatchWinners": {},
         "MatchWinnersLocation": "[]", "PrizeMatrix": {}}
    return ('<?xml version="1.0" encoding="utf-8"?>\n'
            '<string xmlns="http://tempuri.org/">' + json.dumps(p) + '</string>')


class FakeResp:
    def __init__(self, text, status=200, payload=None):
        self.text = text
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.RequestException(f'HTTP {self.status_code}')


def stub_get_factory(body=None, exc=None):
    def stub(url, headers=None, timeout=None):
        if exc is not None:
            raise exc
        assert 'megamillions.com' in url, f'unexpected URL in stub: {url}'
        return FakeResp(body)
    return stub


def fixture(mm_last='2026-10-30', mm_numbers=None, jackpot=None):
    """Minimal draws.json: MM ending mm_last, plus two other games."""
    mm = [{'date': '2026-10-27', 'numbers': [14, 19, 21, 45, 67], 'bonus': 3},
          {'date': '2026-10-30', 'numbers': [5, 10, 38, 49, 68], 'bonus': 17}]
    if mm_numbers is not None:
        mm.append({'date': mm_last, 'numbers': mm_numbers[0], 'bonus': mm_numbers[1]})
    elif mm_last != '2026-10-30':
        mm.append({'date': mm_last, 'numbers': [5, 10, 38, 49, 68], 'bonus': 17})
    data = {'megamillions': mm,
            'powerball': [{'date': '2026-11-02', 'numbers': [7, 23, 29, 64, 66], 'bonus': 23}],
            'pick3': [{'date': '2026-11-03', 'numbers': [2, 7, 2]}]}
    if jackpot is not None:
        data['jackpots'] = jackpot
    return data


REAL_DRAW = {'date': '2026-11-03', 'numbers': [4, 24, 31, 62, 70], 'bonus': 3}
REAL_JACKPOT = {'game': 'megamillions', 'date': '2026-11-03', 'amount': 505000000}
orig_get = fetch_draws.SESSION.get

try:
    # ---------- 1. happy path: real Nov 3 numbers ----------
    fetch_draws.SESSION.get = stub_get_factory(mm_payload())
    draw, jack = fetch_draws.fetch_mm_fastlane()
    eq(draw, REAL_DRAW, 'fast lane returns real Nov 3 draw')
    eq(jack, REAL_JACKPOT, 'fast lane returns real jackpot entry')
    ok(set(jack.keys()) == {'game', 'date', 'amount'}, 'jackpot entry has exactly game/date/amount')

    # unsorted numbers from the feed are sorted before merging
    fetch_draws.SESSION.get = stub_get_factory(mm_payload(n=(70, 4, 62, 24, 31)))
    draw2, _ = fetch_draws.fetch_mm_fastlane()
    eq(draw2['numbers'], [4, 24, 31, 62, 70], 'unsorted feed numbers sorted')

    # ---------- 2. apply adds a missing draw + jackpot ----------
    fetch_draws.SESSION.get = stub_get_factory(mm_payload())
    data = fixture()
    added = {}
    changed, err = fetch_draws.apply_mm_fastlane(data, added)
    eq(err, None, 'apply: no error on happy path')
    eq(added, {'megamillions': 1}, 'apply: one MM draw added')
    ok(changed is True, 'apply: jackpot_changed True')
    eq(data['megamillions'][-1], REAL_DRAW, 'apply: Nov 3 draw stored')
    eq(data['jackpots'], [REAL_JACKPOT], 'apply: jackpot stored')

    # ---------- 3. no-op when Socrata already has the same draw ----------
    fetch_draws.SESSION.get = stub_get_factory(mm_payload())
    data = fixture(mm_last='2026-11-03', mm_numbers=([4, 24, 31, 62, 70], 3))
    added = {}
    changed, err = fetch_draws.apply_mm_fastlane(data, added)
    eq(err, None, 'no-op: no error')
    eq(added, {}, 'no-op: nothing added')
    ok(changed is True, 'no-op: jackpot still recorded when absent')
    eq(data['jackpots'], [REAL_JACKPOT], 'no-op: jackpot recorded')
    # identical jackpot already stored -> no change at all
    changed2, err2 = fetch_draws.apply_mm_fastlane(data, {})
    ok(changed2 is False and err2 is None, 'no-op: identical jackpot is a clean no-op')

    # ---------- 4. conflict -> hard error, data untouched ----------
    fetch_draws.SESSION.get = stub_get_factory(mm_payload())
    data = fixture(mm_last='2026-11-03', mm_numbers=([1, 2, 3, 4, 5], 6))
    before = copy.deepcopy(data)
    changed, err = fetch_draws.apply_mm_fastlane(data, {})
    ok(err is not None and 'CONFLICT' in err, 'conflict: hard error returned')
    eq(data, before, 'conflict: data untouched')
    ok(changed is False, 'conflict: jackpot not applied')

    # ---------- 5. endpoint down / malformed -> skip silently ----------
    fetch_draws.SESSION.get = stub_get_factory(exc=requests.RequestException('down'))
    draw, jack = fetch_draws.fetch_mm_fastlane()
    ok(draw is None and jack is None, 'endpoint down: (None, None)')
    bad_bodies = [
        ('not xml at all', mm_payload(raw='not xml at all')),
        ('xml without string', mm_payload(raw='<html></html>')),
        ('bad json', mm_payload(raw='<string>not json</string>')),
        ('no Drawing', mm_payload(raw='<string>{}</string>')),
        ('missing N3', mm_payload(drop=('N3',))),
        ('bad PlayDate', mm_payload(play_date='Nov 3 2026')),
        ('future PlayDate', mm_payload(play_date='2999-01-01T00:00:00')),
        ('Mega Ball 25', mm_payload(mb=25)),
        ('duplicate mains', mm_payload(n=(4, 4, 31, 62, 70))),
        ('main out of range', mm_payload(n=(4, 24, 31, 62, 71))),
        ('HTTP 500', None),
    ]
    for label, body in bad_bodies:
        if label == 'HTTP 500':
            fetch_draws.SESSION.get = lambda *a, **k: FakeResp('err', status=500)
        else:
            fetch_draws.SESSION.get = stub_get_factory(body)
        draw, jack = fetch_draws.fetch_mm_fastlane()
        ok(draw is None and jack is None, f'malformed: {label} -> (None, None)')
    # a failed fast lane leaves the rest of the data alone
    fetch_draws.SESSION.get = stub_get_factory(exc=requests.RequestException('down'))
    data = fixture()
    before = copy.deepcopy(data)
    changed, err = fetch_draws.apply_mm_fastlane(data, {})
    ok(changed is False and err is None, 'failed fast lane: apply is a silent no-op')
    eq(data, before, 'failed fast lane: data untouched')

    # ---------- 6. stale feed never overwrites a newer jackpot ----------
    fetch_draws.SESSION.get = stub_get_factory(
        mm_payload(n=(5, 10, 38, 49, 68), mb=17, play_date='2026-10-30T00:00:00',
                   next_pool=482000000.0))
    data = fixture(mm_last='2026-11-03', mm_numbers=([4, 24, 31, 62, 70], 3),
                   jackpot=[REAL_JACKPOT])
    changed, err = fetch_draws.apply_mm_fastlane(data, {})
    ok(changed is False and err is None, 'stale feed: jackpot not regressed')
    eq(data['jackpots'], [REAL_JACKPOT], 'stale feed: stored jackpot kept')

    # ---------- 7. build.py newest date with jackpots present ----------
    data = fixture(mm_last='2026-11-03', mm_numbers=([4, 24, 31, 62, 70], 3),
                   jackpot=[REAL_JACKPOT])
    eq(build.newest_draw_date(data), '2026-11-03', 'newest date ignores jackpot entry')
    # a jackpot entry can never exceed the newest real draw (its date is
    # the latest MM draw's date, which is itself a stored draw)
    ok(all(j['date'] <= build.newest_draw_date(data) for j in data['jackpots']),
       'jackpot date never exceeds newest draw date')

    # ---------- 8. Powerball fast lane (MUSL API, Nov 2026) ----------
    def pb_payload(date='2026-11-25', whites=(16, 18, 19, 56, 58), pb=6,
                   drop=(), raw=None):
        """Canned MUSL Winning Numbers API body. drop: ruleCodes to remove."""
        if raw is not None:
            return raw
        nums = [{'itemCode': f'white-ball-{i + 1}', 'ruleCode': 'white-balls',
                 'value': f'{n:02d}', 'orderDrawn': i + 1}
                for i, n in enumerate(whites)]
        nums.append({'itemCode': 'powerball', 'ruleCode': 'powerball',
                     'value': f'{pb:02d}', 'orderDrawn': 6})
        nums.append({'itemCode': 'power-play', 'ruleCode': 'power-play',
                     'value': '03', 'orderDrawn': 7})
        if drop:
            nums = [e for e in nums if e['ruleCode'] not in drop]
        return {'numbers': nums, 'drawId': 12345,
                'game': {'code': 'powerball', 'name': 'Powerball'},
                'statusCode': 'complete', 'drawDate': date,
                'drawDateUtc': date + 'T03:59:00Z'}

    def pb_stub_factory(payload=None, exc=None):
        def stub(url, headers=None, timeout=None):
            assert 'api.musl.com' in url, f'unexpected URL in PB stub: {url}'
            assert headers and headers.get('x-api-key') == 'test-key-123', \
                'x-api-key header missing/wrong'
            if exc is not None:
                raise exc
            return FakeResp('', payload=payload)
        return stub

    os.environ['MUSL_API_KEY'] = 'test-key-123'
    PB_REAL = {'date': '2026-11-25', 'numbers': [16, 18, 19, 56, 58], 'bonus': 6}
    # happy path
    fetch_draws.SESSION.get = pb_stub_factory(pb_payload())
    draw, jack = fetch_draws.fetch_powerball_fastlane()
    eq(draw, PB_REAL, 'pb fast lane returns real Nov 25 draw')
    eq(jack, None, 'pb fast lane returns no jackpot')
    # unsorted white balls sorted
    fetch_draws.SESSION.get = pb_stub_factory(pb_payload(whites=(58, 16, 56, 18, 19)))
    draw2, _ = fetch_draws.fetch_powerball_fastlane()
    eq(draw2['numbers'], [16, 18, 19, 56, 58], 'pb unsorted feed numbers sorted')
    # no key -> silent no-op, no HTTP attempted
    os.environ.pop('MUSL_API_KEY', None)
    _calls = []
    def _nocall(url, headers=None, timeout=None):
        _calls.append(url)
        raise AssertionError('no HTTP should be attempted without a key')
    fetch_draws.SESSION.get = _nocall
    draw, jack = fetch_draws.fetch_powerball_fastlane()
    ok(draw is None and jack is None and _calls == [], 'pb no key: (None, None), no HTTP')
    os.environ['MUSL_API_KEY'] = 'test-key-123'
    # malformed -> (None, None)
    bad = [
        ('endpoint down', None, requests.RequestException('down')),
        ('non-dict payload', 'not a dict', None),
        ('no numbers', pb_payload(drop=('white-balls', 'powerball')), None),
        ('4 white balls', pb_payload(whites=(16, 18, 19, 56)), None),
        ('missing powerball', pb_payload(drop=('powerball',)), None),
        ('bad bonus', pb_payload(pb=27), None),
        ('duplicate mains', pb_payload(whites=(16, 16, 19, 56, 58)), None),
        ('main out of range', pb_payload(whites=(16, 18, 19, 56, 70)), None),
        ('bad drawDate', pb_payload(date='Nov 25 2026'), None),
        ('future drawDate', pb_payload(date='2999-01-01'), None),
    ]
    for label, payload, exc in bad:
        fetch_draws.SESSION.get = pb_stub_factory(payload, exc)
        draw, jack = fetch_draws.fetch_powerball_fastlane()
        ok(draw is None and jack is None, f'pb malformed: {label} -> (None, None)')
    fetch_draws.SESSION.get = lambda *a, **k: FakeResp('err', status=500)
    draw, jack = fetch_draws.fetch_powerball_fastlane()
    ok(draw is None and jack is None, 'pb malformed: HTTP 500 -> (None, None)')
    # apply: adds, no-op, conflict, failed fetch
    fetch_draws.SESSION.get = pb_stub_factory(pb_payload())
    data = fixture()
    added = {}
    err = fetch_draws.apply_powerball_fastlane(data, added)
    eq(err, None, 'pb apply: no error on happy path')
    eq(added, {'powerball': 1}, 'pb apply: one PB draw added')
    eq(data['powerball'][-1], PB_REAL, 'pb apply: Nov 25 draw stored')
    fetch_draws.SESSION.get = pb_stub_factory(pb_payload())
    data = fixture()
    data['powerball'].append(dict(PB_REAL))
    added = {}
    err = fetch_draws.apply_powerball_fastlane(data, added)
    eq(err, None, 'pb no-op: no error')
    eq(added, {}, 'pb no-op: nothing added')
    fetch_draws.SESSION.get = pb_stub_factory(pb_payload())
    data = fixture()
    data['powerball'].append({'date': '2026-11-25', 'numbers': [1, 2, 3, 4, 5], 'bonus': 6})
    before = copy.deepcopy(data)
    err = fetch_draws.apply_powerball_fastlane(data, {})
    ok(err is not None and 'CONFLICT' in err, 'pb conflict: error returned')
    eq(data, before, 'pb conflict: data untouched')
    fetch_draws.SESSION.get = pb_stub_factory(None, requests.RequestException('down'))
    data = fixture()
    before = copy.deepcopy(data)
    err = fetch_draws.apply_powerball_fastlane(data, {})
    ok(err is None, 'pb failed fetch: silent no-op')
    eq(data, before, 'pb failed fetch: data untouched')
    os.environ.pop('MUSL_API_KEY', None)

    # ---------- 9. build.py base64 cache (Nov 2026) ----------
    try:
        build._CACHE_PATH.unlink()
    except OSError:
        pass
    art1 = build._asset_b64('art.jpg', 'art.jpg.b64')
    ok(build._CACHE_PATH.exists(), 'cache: cache file created on first encode')
    art2 = build._asset_b64('art.jpg', 'art.jpg.b64')
    eq(art1, art2, 'cache: second call returns identical base64')
    cache = json.loads(build._CACHE_PATH.read_text())
    ok(any('art.jpg' in k for k in cache), 'cache: art.jpg entry keyed in cache')
    # prove the cache is READ, not just written: poison the entry
    key = [k for k in cache if 'art.jpg' in k][0]
    cache[key] = 'SENTINEL'
    build._CACHE_PATH.write_text(json.dumps(cache))
    eq(build._asset_b64('art.jpg', 'art.jpg.b64'), 'SENTINEL',
       'cache: cached value is returned')
    # restore: delete the cache file so the next real build recomputes
    build._CACHE_PATH.unlink()
    art3 = build._asset_b64('art.jpg', 'art.jpg.b64')
    eq(art3, art1, 'cache: recompute after cache removal matches original')
    build._CACHE_PATH.unlink(missing_ok=True)
finally:
    fetch_draws.SESSION.get = orig_get

print(f'\n{pass_n} passed, {fail_n} failed')
sys.exit(1 if fail_n else 0)
