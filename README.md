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
