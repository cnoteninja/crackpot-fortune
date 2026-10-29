#!/usr/bin/env python3
"""Crackpot Fortune build: template + draws.json + art + cookie -> outputs.

Outputs:
  app/www/index.html            (Capacitor web root, with PWA metas)
  repo/www/index.html           (GitHub staging copy, identical to app/www)
  ~/workspace/your_files/crackpot-fortune.html  (standalone, no PWA metas)

Also stamps the footer 'Draw history through <date>' from the newest
draw date found in draws.json.
"""
import json, base64, re, sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent

def _asset_b64(bin_name, b64_name):
    """Base64 for an embedded image. Prefers the binary file; falls back
    to the committed base64 text twin (*.b64) on runners, where the
    snapshot-push route carries text only."""
    p = ROOT / bin_name
    if p.exists():
        return base64.b64encode(p.read_bytes()).decode()
    return ''.join((ROOT / b64_name).read_text().split())


def main():
    t = (ROOT / 'app-template.html').read_text()
    data = json.loads((ROOT / 'data' / 'draws.json').read_text())

    newest = max(d['date'] for draws in data.values() for d in draws)
    y, m, dd = newest.split('-')
    pretty = date(int(y), int(m), int(dd)).strftime('%b %-d, %Y')
    t, n = re.subn(r'Draw history through <b>[^<]*</b>',
                   f'Draw history through <b>{pretty}</b>', t)
    assert n == 1, 'footer date pattern not found'

    data_js = 'const DATA = /*__DATA__*/' + json.dumps(data, separators=(',', ':')) + ';'
    t, n = re.subn(r'const DATA = /\*__DATA__\*/[^;]*;', data_js, t, count=1)
    assert n == 1, 'DATA placeholder not found'

    art_b64 = _asset_b64('art.jpg', 'art.jpg.b64')
    assert '__ART__' in t
    t = t.replace('__ART__', 'data:image/jpeg;base64,' + art_b64)

    cookie_b64 = _asset_b64('assets/cookie-q.png', 'assets/cookie-q.png.b64')
    assert '__COOKIE__' in t
    t = t.replace('__COOKIE__', 'data:image/png;base64,' + cookie_b64)

    pwa = ('<meta name="theme-color" content="#120e2e">\n'
           '<meta name="mobile-web-app-capable" content="yes">\n'
           '<meta name="apple-mobile-web-app-capable" content="yes">\n'
           '<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">\n'
           '<meta name="apple-mobile-web-app-title" content="Crackpot Fortune">\n'
           '<link rel="apple-touch-icon" href="icon-180.png">\n'
           '<link rel="icon" type="image/png" href="icon-180.png">\n')
    viewport = '<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">\n'
    assert viewport in t
    www = t.replace(viewport, viewport + pwa)

    # Web-root outputs are layout-aware: the GitHub Actions runner checks
    # the repo out flat, so its web root is www/ at ROOT; the local
    # layout keeps repo/ and app/ mirrors. Optional outputs are written
    # only where their parent dir exists, so this script runs unchanged
    # in both layouts.
    for out in (ROOT / 'www' / 'index.html',
                ROOT / 'repo' / 'www' / 'index.html',
                ROOT / 'app' / 'www' / 'index.html'):
        if out.parent.exists():
            out.write_text(www)
    standalone = Path.home() / 'workspace' / 'your_files' / 'crackpot-fortune.html'
    if standalone.parent.exists():
        standalone.write_text(t)
    counts = {k: len(v) for k, v in data.items()}
    print(f'built OK; data through {newest}; draws per game: {counts}')

if __name__ == '__main__':
    main()
