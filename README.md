# herald-watcher
Free, keyword-based breaking-news watcher (RSS/Google News/price feeds) for @JhMpologoma.
Runs every 5 minutes on GitHub Actions (`.github/workflows/herald-watcher.yml`) and sends qualifying
headlines to a private webhook. No AI calls, no secrets in the code.

Required repository secrets (Settings > Secrets and variables > Actions):
- `HERALD_WEBHOOK_URL`
- `HERALD_WEBHOOK_KEY`

First run (empty cache) runs `watcher.py --prime`: marks current items seen and sends nothing.

## FinancialJuice (added 2026-10-26)
`https://www.financialjuice.com/feed.ashx?xy=rss` (public RSS), polled every 75 s. Leads only, never a source to cite:
each hit carries `lead_only: true` and must be confirmed at the primary source. `score_fj()` drops routine squawk
(minor data prints, speaker soundbites, "sources"/poll/estimate chatter, rate-probability/vol/correlation tables,
analyst ratings, small % ticks, intercepts/unconfirmed claims) and only passes major events (central-bank decisions,
big US data surprises, record/crash market moves, war shocks on capitals or nuclear sites, leader deaths/resignations,
$10B+ deals, tariffs/sanctions/executive orders) scoring >= 12 (normal news minimum is 9). Not fast-lane unless
wire-attributed and score >= 14. First fetch only primes the feed. Turn off with `HERALD_FJ=0`.

## Latency pass (2026-10-26)
Parallel fetching (one pass of 90 feeds in ~7-14 s), 30-60 s poll intervals on wires/official feeds when run continuously,
new central-bank/official feeds (Fed monetary, White House actions, ECB, BoE, BoJ, BoC, BEA) and Google News Reuters
business/markets, central-bank and megacap-deal queries. Urgent hits are sent at once on their own; batches every 3 min; cap 60/day.
The Reuters direct sitemap is behind a bot wall (HTTP 401) and was removed. Cron moved to minute 2,7,12,... to dodge
GitHub's top-of-hour load.

## Scoring pass (2026-10-26)

- **Wire-grade world events** (`HARD_EVENT`): labour strikes/walkouts, outbreaks (Ebola, cholera, mpox...), deadly attacks and arrests over them, crisis diplomacy (delegations/security chiefs meet, crisis/peace talks, envoy expulsions), disasters (oil spills, beach closures, floods, quakes). +3, and +4 more from a wire/top-desk source (Reuters, AP, BBC, Bloomberg, CNBC, WSJ, Nikkei, official). Feature/reaction pieces about them ("describes", "funeral", "what you need to know") are dropped.
- **More world entities**: Jordan, England/Scotland/Wales/Ireland, NHS/BMA, Africa CDC/WHO, Gulf/Mideast, capitals (Tehran, Riyadh, Kyiv...), ~60 more countries.
- **Megacap capital actions** (`MEGACAP_CAPITAL`): $bn stock/equity/debt offerings, share sales, buybacks, "public/proposed offering", "offering of ... shares" count as a big event for big companies.
- **Company releases are not "press release" spam** when they come from a megacap's IR site/newsroom or a PR Newswire/Business Wire/GlobeNewswire release whose issuer is the megacap (title starts with it). These count as official and fast-lane. New feeds: `GN: megacap newsrooms`, `GN: megacap press wires`.
- **Junk filter** (`PREVIEW_JUNK`, `ANALYST_NOTE`): previews ("expected to", "ahead of", "braces", "awaits", "looms", "odds"), analyst notes (Goldman expects/forecasts, "..., says Morgan Stanley"), market wraps (pre-bell, "stock market today", futures), FX commentary (softens/poises/hovers), buy-lists, first-person columns, op-eds, "in maps", "- business live". A rate move is "big" only when a central bank acts, not for the phrase "rate hike". Non-megacap quarterly results are capped below threshold.
- **Cap**: 100 webhooks per EAT day, the last 20 for urgent hits only; ordinary batches at most every 5 min (measured from the last ordinary batch); urgent sends keep the 30 s floor.
- **Loop mode**: `python watcher.py --run-for SECONDS` polls continuously and exits cleanly (state saved) after SECONDS. The GitHub Actions copy is now one ~5h42m job (6 x 57-min chunks, cache save after each chunk and always at the end), concurrency group `herald-watcher`, cron every 5 h plus a self-dispatch at the end of each run.
