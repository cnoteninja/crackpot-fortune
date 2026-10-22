# 🥠 Crackpot Fortune

**A whimsical little lottery oracle.** Real draw history underneath,
pure fortune-cookie magic on top.

Tap the cookie. It cracks. A paper slip unfurls with your numbers —
and a fortune that tells the *story* of the pick.

## How it works

Crackpot Fortune studies real draw history for **Ohio Pick 3, Pick 4,
Pick 5, Mega Millions, and Powerball**, then picks numbers three ways:

- ✨ **Destiny Blend** — hot numbers mixed with long-overdue sleepers
- 🔥 **Hot Streak** — the most frequently drawn numbers lately
- 🌙 **Long Sleep** — the numbers that haven't appeared in the longest time

…over the last 30 draws, 100 draws, or the cookie's whole memory.
Slips you like go in the **Cookie Jar** for later.

## The honest print

Every lottery draw is independent. No app — however charming — can
predict one. This is a for-fun pattern toy: it finds patterns because
patterns are delightful, not because they're destiny. It sells
nothing, takes no wagers, and awards no prizes. See
[docs/PRIVACY_POLICY.md](docs/PRIVACY_POLICY.md) (short version:
the app collects nothing at all).

## Project layout

| Path | What it is |
| --- | --- |
| `www/index.html` | The entire app — one self-contained file |
| `capacitor.config.json` | Capacitor app config (ID: `com.cnoteninja.crackpotfortune`) |
| `resources/` | Icon + splash source art |
| `.github/workflows/` | Build recipes — see below |
| `WORKFLOW.md` | The living checklist: what's done, what's next |
| `docs/` | Store listing + privacy policy drafts |

The native `android/` and `ios/` projects are **generated in CI**,
not committed — the workflows create them fresh on every build.

## Building

Pushes to `main` (and manual runs) trigger GitHub Actions:

- **Build Android (debug APK)** — produces an installable APK under
  the run's *Artifacts*. No Play Store account needed to test.
- **Build iOS (unsigned)** — proves the app compiles on a macOS
  runner. Installing on a device or submitting to the App Store
  needs an Apple Developer account; publishing is parked until
  then (see `WORKFLOW.md`).

Local development: `npm ci`, edit `www/index.html`, then
`npx cap add android` / `npx cap add ios` + `npx cap sync` to
run the native shells.

## Data

Draw history is bundled in the app (snapshot through Oct 21, 2026):
Ohio Pick games via LotteryUSA's results archive; Mega Millions and
Powerball via New York Lottery open data (data.ny.gov).

---

*Built by Christopher McMickens (@cnoteninja) with an AI assist —
an idea at 10:45 PM, a working app the next morning.*
