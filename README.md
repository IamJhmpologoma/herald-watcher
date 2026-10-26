# herald-watcher
Free, keyword-based breaking-news watcher (RSS/Google News/price feeds) for @JhMpologoma.
Runs every 5 minutes on GitHub Actions (`.github/workflows/herald-watcher.yml`) and sends qualifying
headlines to a private webhook. No AI calls, no secrets in the code.

Required repository secrets (Settings > Secrets and variables > Actions):
- `HERALD_WEBHOOK_URL`
- `HERALD_WEBHOOK_KEY`

First run (empty cache) runs `watcher.py --prime`: marks current items seen and sends nothing.
