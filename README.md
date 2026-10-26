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
