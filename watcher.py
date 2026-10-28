#!/usr/bin/env python3
"""
Herald breaking-news watcher for @JhMpologoma ("Mr Update").
Since 2026-10-25 ~20:00 EAT: a broad Watcher-Guru-style breaking-news watcher
(markets, macro, crypto, AI, big tech/business, world headlines). Style guide: ../watcher-guru-style.md

No AI calls. Each feed has its own poll interval (fast wires/markets/crypto every 60-90s,
Google News searches every 150-240s, slow blogs every 3-9 min); conditional GETs (ETag /
Last-Modified) where the server supports them. Items are scored with keyword rules, deduped,
appended to ../breaking-queue.jsonl and escalated in batches to the webhook routine.
Big-mover price alerts: BTC/ETH/SOL/XRP (CoinGecko) and gold, S&P 500, Nasdaq 100, WTI oil
futures (Yahoo Finance chart API).

Usage:
  watcher.py                   run forever
  watcher.py --once            one pass over every feed (writes state/queue, may escalate)
  watcher.py --once --dry-run  one pass, prints hits, writes/escalates nothing

Latency pass 2026-10-26 ~14:40 EAT: 5 s tick, parallel fetch (16 workers), 30-60 s polls on wires/official feeds,
urgent hits dispatched alone at once (30 s floor), batches every 3 min, cap 60/day (last 10 reserved for urgent).
"""
from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import html
import json
import logging
import logging.handlers
import os
import random
import re
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
HERALD = os.path.dirname(HERE)
STATE_PATH = os.environ.get("HERALD_STATE_PATH") or os.path.join(HERE, "state.json")
LOG_PATH = os.environ.get("HERALD_LOG_PATH") or os.path.join(HERE, "watcher.log")
QUEUE_PATH = os.environ.get("HERALD_QUEUE_PATH") or os.path.join(HERALD, "breaking-queue.jsonl")
WEBHOOK_ENV = os.environ.get("HERALD_WEBHOOK_ENV") or os.path.join(HERE, "webhook.env")
INSTANCE = os.environ.get("HERALD_INSTANCE", "box")   # "box" or e.g. "gha" (always-on copy)
TZ = ZoneInfo("Africa/Kampala")
TICK_SECONDS = 5                    # main loop tick (latency pass 2026-10-26: was 15); each feed has its own interval
MIN_FEED_INTERVAL_S = 30            # politeness floor: no feed is fetched more often than every 30 s
FETCH_WORKERS = 16                  # feeds due in a tick are fetched in parallel (was serial: 30-130 s cycles)
HOST_BACKOFF_S = (300, 900, 1800)   # 401/403/429/503 from a host: back off that host 5 -> 15 -> 30 min
MAX_AGE_MIN = 45                    # breaking: drop items published more than 45 min ago
FIRST_RUN_MAX_AGE_MIN = 45
UA = "Mozilla/5.0 (X11; Linux x86_64) HeraldWatcher/2.0 (+news monitor for @JhMpologoma)"
TIMEOUT = 12

# ---- topic switches (2026-10-25 ~20:00 EAT: everything on again; env can still turn parts off)
CRYPTO_ENABLED = os.environ.get("HERALD_CRYPTO", "1") == "1"
PRICE_ALERTS_ENABLED = os.environ.get("HERALD_PRICES", "1") == "1"
NEWS_ENABLED = os.environ.get("HERALD_NEWS", "1") == "1"

# ---- webhook limits (breaking-news level)
ESC_MIN_GAP_S = 5 * 60              # normal batches: at most one every 5 min (scoring pass; latency pass had 3, before that 8)
ESC_MAX_PER_DAY = 140               # per EAT day (speed research 2026-10-28: GHA hit reserve-only at 95/100 ~23:20 EAT and held
                                    # non-urgent hits 28 min until midnight; was 100, before that 60, 40)
ESC_RESERVE_FOR_URGENT = 20         # the last 20 of the daily cap are kept for urgent (fast-lane) hits only
MAX_HITS_PER_WEBHOOK = 6            # several hits batched per webhook, best first
AI_MAX_HITS_PER_POST = 3            # AI can't crowd out the rest of a batch
AI_HIT_THRESHOLD = 9
AI_MAX_AGE_H = MAX_AGE_MIN / 60
NEWS_HIT_THRESHOLD = 9
CRYPTO_HIT_THRESHOLD = 9
STORY_DEDUPE_S = 3 * 3600           # same entity+event+number story: one hit per 3h
# ---- fast lane (2026-10-26): top wires/official sources with score >= FAST_LANE_MIN_SCORE skip the
# 8-minute gap (daily cap still applies) and are flagged priority:true in the webhook payload.
FAST_LANE_MIN_SCORE = 10
FAST_LANE_MIN_GAP_S = 30            # anti-burst floor between two fast-lane webhooks (latency pass: was 60)
URGENT_ANY_SOURCE_MIN_SCORE = 13    # non-wire items this strong are also sent at once, on their own
URGENT_MAX_PER_WEBHOOK = 2          # urgent hits go out alone (or in a pair), never wait for a batch
FAST_LANE_DOMAINS = re.compile(r"(^|\.)(reuters\.com|apnews\.com|bloomberg\.com|bbc\.co\.uk|bbc\.com|nikkei\.com|cnbc\.com|wsj\.com|"
                               r"ecb\.europa\.eu|bankofengland\.co\.uk|boj\.or\.jp|bankofcanada\.ca|"
                               r"federalreserve\.gov|sec\.gov|whitehouse\.gov|treasury\.gov|bls\.gov|[a-z0-9-]+\.gov)$", re.I)
FAST_LANE_PUBLISHERS = re.compile(r"^(reuters|associated press|ap news|bloomberg|bbc|bbc news|nikkei asia|nikkei|"
                                  r"cnbc|the wall street journal|wsj|financial times|"
                                  r"the white house|federal reserve|u\.s\. department of the treasury|bureau of labor statistics)\b", re.I)
FAST_LANE_FEEDS = re.compile(r"^(Reuters|BBC|Nikkei|CNBC|WSJ|MarketWatch Bulletins|Federal Reserve|SEC press|White House|ECB|Bank of (England|Japan|Canada)|BEA|"
                             r"GN: (Reuters|AP|Bloomberg|treasury\.gov|bls\.gov))", re.I)
AI_STORY_DEDUPE_S = 12 * 3600

def gnews(q: str) -> str:
    return ("https://news.google.com/rss/search?q=" + urllib.parse.quote(q)
            + "&hl=en-US&gl=US&ceid=US:en")
# name, url, tier (official | reputable | aggregator), poll interval (s)
# All checked with curl on 2026-10-25 ~19:50 EAT (HTTP 200, parseable, fresh items).
NEWS_FEEDS = [
    # Latency pass 2026-10-26 ~14:40 EAT: fastest feeds polled every 30-60 s (floor 30 s), conditional GET where supported.
    # wires / markets (fast)
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html", "reputable", 30),
    ("CNBC World", "https://www.cnbc.com/id/100727362/device/rss/rss.html", "reputable", 45),
    ("CNBC Markets", "https://www.cnbc.com/id/15839069/device/rss/rss.html", "reputable", 45),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html", "reputable", 60),
    ("CNBC Investing", "https://www.cnbc.com/id/15839135/device/rss/rss.html", "reputable", 90),
    ("MarketWatch Bulletins", "https://feeds.content.dowjones.io/public/rss/mw_bulletins", "reputable", 30),
    ("MarketWatch Top", "https://feeds.content.dowjones.io/public/rss/mw_topstories", "reputable", 60),
    ("MarketWatch Pulse", "https://feeds.content.dowjones.io/public/rss/mw_marketpulse", "reputable", 60),
    ("WSJ Markets", "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain", "reputable", 45),
    ("WSJ World", "https://feeds.content.dowjones.io/public/rss/RSSWorldNews", "reputable", 45),
    ("Yahoo Finance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC,%5EIXIC,BTC-USD,NVDA,TSLA&region=US&lang=en-US", "reputable", 90),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml", "reputable", 45),
    ("BBC Business", "https://feeds.bbci.co.uk/news/business/rss.xml", "reputable", 60),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml", "reputable", 90),
    ("Guardian Business", "https://www.theguardian.com/business/rss", "reputable", 120),
    ("NYT World", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "reputable", 90),
    ("NYT Business", "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "reputable", 120),
    # official
    # press_all.xml is the right URL (200 + ETag); its CDN returns a stray 404 now and then -> fetch retries once.
    ("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml", "official", 30),
    ("Federal Reserve monetary", "https://www.federalreserve.gov/feeds/press_monetary.xml", "official", 30),
    ("SEC press releases", "https://www.sec.gov/news/pressreleases.rss", "official", 45),
    ("White House", "https://www.whitehouse.gov/news/feed/", "official", 45),
    ("White House actions", "https://www.whitehouse.gov/presidential-actions/feed/", "official", 45),
    ("ECB press", "https://www.ecb.europa.eu/rss/press.html", "official", 45),
    ("Bank of England news", "https://www.bankofengland.co.uk/rss/news", "official", 60),
    ("Bank of Japan", "https://www.boj.or.jp/en/rss/whatsnew.xml", "official", 90),
    ("Bank of Canada press", "https://www.bankofcanada.ca/content_type/press-releases/feed/", "official", 45),
    ("BEA releases", "https://apps.bea.gov/rss/rss.xml", "official", 60),
    # treasury.gov has no working RSS (404) and bls.gov returns 403 to scripts: Google News site: fallbacks
    ("GN: treasury.gov", gnews("site:home.treasury.gov when:1d"), "official", 120),
    ("GN: bls.gov", gnews("site:bls.gov when:1d"), "official", 120),
    # Google News top-story searches (aggregator tier; publisher shown in item)
    ("GN: Trump", gnews("Trump (announces OR signs OR tariff OR \"executive order\" OR says) when:1h"), "aggregator", 90),
    ("GN: Fed/rates", gnews("(\"Federal Reserve\" OR Powell OR \"rate cut\" OR \"rate hike\" OR FOMC) when:1h"), "aggregator", 90),
    ("GN: central banks", gnews('("rate decision" OR "cuts rates" OR "raises rates" OR "holds rates" OR "leaves rates" OR "keeps rates") (ECB OR "Bank of England" OR "Bank of Japan" OR "Bank of Canada" OR Fed OR RBA OR SNB OR PBOC) when:1h'), "aggregator", 90),
    ("GN: tariffs/trade", gnews("(tariff OR tariffs OR sanctions OR \"trade deal\") when:1h"), "aggregator", 120),
    ("GN: US data", gnews("(CPI OR inflation OR \"jobs report\" OR payrolls OR GDP OR PPI OR unemployment) report when:1h"), "aggregator", 120),
    ("GN: Nvidia/Tesla/Musk", gnews("(Nvidia OR Tesla OR SpaceX OR \"Elon Musk\") when:1h"), "aggregator", 120),
    ("GN: Big Tech", gnews("(Apple OR Microsoft OR Amazon OR Alphabet OR Meta) (earnings OR revenue OR announces OR lawsuit OR fined OR acquire) when:1h"), "aggregator", 150),
    ("GN: megacap deals", gnews('(Meta OR Apple OR Microsoft OR Nvidia OR Amazon OR Alphabet OR Tesla OR Berkshire OR OpenAI OR Broadcom OR Oracle) ("stock sale" OR offering OR acquire OR acquisition OR buyback OR "billion deal" OR merger) when:1h'), "aggregator", 90),
    ("GN: Wall St banks", gnews("(JPMorgan OR BlackRock OR Goldman OR \"Bank of America\" OR \"Wells Fargo\" OR Citi) when:1h"), "aggregator", 180),
    ("GN: markets moves", gnews("(\"stock market\" OR \"S&P 500\" OR Nasdaq OR Dow OR gold OR oil) (record OR plunges OR surges OR crash OR wiped OR soars) when:1h"), "aggregator", 120),
    ("GN: world breaking", gnews("(war OR missile OR ceasefire OR attack OR earthquake OR emergency OR coup) (Russia OR Ukraine OR China OR Taiwan OR Israel OR Iran OR NATO OR \"North Korea\") when:1h"), "aggregator", 120),
    # Reuters' direct sitemap is behind a DataDome bot wall (HTTP 401 since 12:47 EAT, browser UA too) -> removed;
    # Reuters/AP/Bloomberg now come through four Google News site: queries polled every 60 s.
    ("GN: Reuters", gnews("site:reuters.com when:1h"), "reputable", 60),
    ("GN: Reuters world", gnews("site:reuters.com/world when:1h"), "reputable", 60),
    ("GN: Reuters business", gnews("site:reuters.com/business when:1h"), "reputable", 60),
    ("GN: Reuters markets", gnews("site:reuters.com/markets when:1h"), "reputable", 60),
    ("GN: AP", gnews("site:apnews.com when:1h"), "reputable", 60),
    ("GN: Bloomberg", gnews("site:bloomberg.com when:1h"), "reputable", 60),
    # scoring pass 2026-10-26: megacap company releases (IR sites/newsrooms and their PR/Business Wire copies)
    ("GN: megacap newsrooms", gnews("(site:investor.atmeta.com OR site:about.fb.com OR site:nvidianews.nvidia.com OR site:news.microsoft.com "
                                    "OR site:apple.com/newsroom OR site:aboutamazon.com OR site:abc.xyz OR site:ir.tesla.com OR site:openai.com/index) when:1d"), "official", 120),
    ("GN: megacap press wires", gnews("(site:prnewswire.com OR site:businesswire.com OR site:globenewswire.com) "
                                      "(Meta OR Apple OR Microsoft OR Nvidia OR Amazon OR Alphabet OR Tesla OR Broadcom OR Oracle OR Netflix OR JPMorgan) when:1d"), "official", 120),
    # ---- speed fixes 2026-10-26: direct wires + world/politics (all curl-checked 2026-10-26 ~09:00 EAT)
    ("BBC Top Stories", "https://feeds.bbci.co.uk/news/rss.xml", "reputable", 30),
    ("BBC Africa", "https://feeds.bbci.co.uk/news/world/africa/rss.xml", "reputable", 180),
    ("Nikkei Asia", "https://asia.nikkei.com/rss/feed/nar", "reputable", 90),       # undated items: first-seen time used
    ("Japan Times", "https://www.japantimes.co.jp/feed/", "reputable", 180),
    ("Yonhap", "https://en.yna.co.kr/RSS/news.xml", "reputable", 120),   # speed research 2026-10-27: Korea markets/N. Korea first-hand
    ("Guardian World", "https://www.theguardian.com/world/rss", "reputable", 90),
    ("DW", "https://rss.dw.com/rdf/rss-en-all", "reputable", 120),
    ("France 24", "https://www.france24.com/en/rss", "reputable", 120),
    ("Sky News World", "https://feeds.skynews.com/feeds/rss/world.xml", "reputable", 120),
    # apnews.com RSS returns 403 to scripts; NHK World English feed 404 / Japanese feed stale: Google News fallbacks
    ("GN: NHK World", gnews("site:www3.nhk.or.jp/nhkworld when:1h"), "reputable", 180),
    ("GN: election results", gnews('("election results" OR concedes OR "wins election" OR "wins presidency" OR "sworn in" OR runoff OR "re-elected" OR "wins re-election" OR landslide) when:1h'), "aggregator", 90),
    ("GN: leaders", gnews('("prime minister" OR president OR king OR pope) (resigns OR "steps down" OR dies OR died OR impeached OR ousted OR assassinated OR arrested) when:1h'), "aggregator", 90),
    ("GN: world breaking 2", gnews('(coup OR invasion OR assassination OR earthquake OR tsunami OR "state of emergency" OR "martial law" OR ceasefire) when:1h'), "aggregator", 90),
    ("GN: Africa", gnews('(Uganda OR Kenya OR Nigeria OR "South Africa" OR Ethiopia OR Ghana OR Tanzania OR Rwanda) (president OR election OR protest OR central bank OR dies OR coup) when:2h'), "aggregator", 180),
    # speed research 2026-10-28: IMF Argentina deal posted ~3h late (imf.org RSS is 403 to scripts, not bypassed);
    # OpenAI-Google chips (The Information/Reuters brief) never hit; Putin-Zelensky invite / Binance Brazil licence missed
    ("GN: IMF deals", gnews('("staff-level agreement" OR "IMF board" OR "IMF approves" OR "IMF bailout" OR "IMF loan") when:1h'), "aggregator", 120),
    ("GN: The Information", gnews("site:theinformation.com when:1h"), "reputable", 120),
    ("GN: leader talks", gnews('(Putin OR Zelensky OR Trump OR "Xi Jinping" OR Netanyahu OR Khamenei) (invites OR "direct talks" OR "to meet" OR summit OR "phone call") when:1h'), "aggregator", 120),
    ("GN: crypto licences", gnews('(Binance OR Coinbase OR Kraken OR Ripple OR Tether OR Circle OR OKX OR Bybit OR Robinhood) (license OR licence OR charter OR "partnership") when:2h'), "aggregator", 180),
]
# feeds added in the latency pass: their first fetch only marks items seen (no backlog burst)
LATENCY_NEW_FEEDS = {"GN: big tech x crypto", "GN: crypto law/courts", "GN: IMF deals", "GN: The Information", "GN: leader talks", "GN: crypto licences", "Federal Reserve monetary", "White House actions", "ECB press", "Bank of England news", "Bank of Japan",
                     "Bank of Canada press", "BEA releases", "GN: central banks", "GN: megacap deals",
                     "GN: Reuters business", "GN: Reuters markets",
                     "GN: megacap newsrooms", "GN: megacap press wires", "Yonhap"}

CRYPTO_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/", "reputable", 45),
    ("The Block", "https://www.theblock.co/rss.xml", "reputable", 45),
    ("Cointelegraph", "https://cointelegraph.com/rss", "reputable", 45),
    ("Decrypt", "https://decrypt.co/feed", "reputable", 60),
    ("Bitcoin Magazine", "https://bitcoinmagazine.com/feed", "reputable", 180),
    ("DL News", "https://www.dlnews.com/arc/outboundfeeds/rss/", "reputable", 180),
    ("Coinbase status", "https://status.coinbase.com/history.rss", "official", 180),
    ("GN: ripple.com", gnews("site:ripple.com when:1d"), "official", 300),
    ("GN: Bitcoin", gnews("(Bitcoin OR BTC) (ETF OR buys OR reserve OR falls OR surges OR record OR liquidated) when:1h"), "aggregator", 150),
    ("GN: crypto policy", gnews("(crypto OR stablecoin OR \"digital asset\") (SEC OR Congress OR Senate OR Treasury OR bill OR \"executive order\" OR ban OR approves) when:1h"), "aggregator", 180),
    ("GN: crypto hack", gnews("(crypto OR exchange OR DeFi OR bridge) (hack OR exploit OR drained OR insolvency OR \"withdrawals halted\") when:2h"), "aggregator", 180),
    # WG-speed audit 2026-10-28: WG crypto scoops we missed (Google MPC custody for banks, Thailand Supreme Court
    # rules Bitcoin is property) came via CoinDesk / Crypto Briefing and no query matched them
    ("GN: big tech x crypto", gnews('(Google OR Apple OR Microsoft OR Amazon OR Meta OR BlackRock OR JPMorgan OR Visa OR Mastercard OR PayPal OR Stripe OR Fidelity OR Nasdaq) '
                                    '(crypto OR bitcoin OR stablecoin OR tokenized OR "digital assets" OR custody OR blockchain) when:1h'), "aggregator", 120),
    ("GN: crypto law/courts", gnews('(Bitcoin OR crypto OR cryptocurrency) ("supreme court" OR court OR "legal tender" OR "legal property" OR parliament '
                                    'OR "central bank" OR legalizes OR legalises OR recognizes OR recognises) when:1h'), "aggregator", 150),
    ("GN: ETH/SOL/XRP", gnews("(Ethereum OR Solana OR XRP OR Ripple) (ETF OR approves OR launches OR partnership OR record) when:1h"), "aggregator", 240),
]

# AI feeds (all checked with curl on 2026-10-25). Same tuple shape.
AI_FEEDS = [
    ("OpenAI news", "https://openai.com/news/rss.xml", "official"),
    ("Google AI blog", "https://blog.google/technology/ai/rss/", "official"),
    ("Google DeepMind blog", "https://deepmind.google/blog/rss.xml", "official"),
    ("Meta newsroom", "https://about.fb.com/news/feed/", "official"),
    ("NVIDIA newsroom", "https://nvidianews.nvidia.com/releases.xml", "official"),
    ("Mistral AI news", "https://mistral.ai/rss.xml", "official"),
    # anthropic.com and x.ai have no RSS (404); Google News site: queries are the fallback
    ("GN: anthropic.com", gnews("site:anthropic.com when:1d"), "official"),
    # speed research 2026-10-28: direct poll of anthropic.com/sitemap.xml (robots: Allow /) - see check_anthropic_site()
    ("GN: x.ai", gnews("site:x.ai when:1d"), "official"),
    ("TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/", "reputable"),
    ("The Verge AI", "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml", "reputable"),
    ("Ars Technica AI", "https://arstechnica.com/ai/feed/", "reputable"),
    ("CNBC Technology", "https://www.cnbc.com/id/19854910/device/rss/rss.html", "reputable"),
    # venturebeat.com feeds return 429 to scripts
    ("GN: venturebeat.com", gnews("site:venturebeat.com when:1d"), "reputable"),
    ("GN: AI top-5 events", gnews('(OpenAI OR Anthropic OR xAI OR Gemini OR "Meta AI" OR ChatGPT OR Claude OR Grok) '
                                  '(launches OR unveils OR raises OR funding OR valuation OR lawsuit OR outage '
                                  'OR acquires OR releases) when:1d'), "aggregator"),
]
# AI security sources (checked with curl on 2026-10-25). General security feeds must mention AI.
AI_SEC_FEEDS_LIST = [
    ("The Hacker News", "https://feeds.feedburner.com/TheHackersNews", "reputable"),
    ("BleepingComputer", "https://www.bleepingcomputer.com/feed/", "reputable"),
    ("SecurityWeek AI", "https://www.securityweek.com/category/artificial-intelligence/feed/", "reputable"),
    ("GN: AI security", gnews('("prompt injection" OR jailbreak OR jailbroken OR deepfake OR "data leak" OR '
                              '"AI security" OR vulnerability OR malware) (ChatGPT OR OpenAI OR Claude OR Anthropic '
                              'OR Gemini OR Grok OR Copilot OR "AI agent" OR LLM) when:1d'), "aggregator"),
]
AI_FEEDS = AI_FEEDS + AI_SEC_FEEDS_LIST
AI_SEC_FEEDS = {n for n, _, _ in AI_SEC_FEEDS_LIST}
AI_FEED_NAMES = {n for n, _, _ in AI_FEEDS}
# AI feed intervals (s): official blogs are slow-moving; OpenAI's feed is 800 KB with no ETag.
AI_FEED_INTERVAL = {"OpenAI news": 9 * 60, "Mistral AI news": 6 * 60, "Google DeepMind blog": 5 * 60,
                    "Google AI blog": 4 * 60, "Meta newsroom": 4 * 60}
AI_FEEDS4 = [(n, u, t, AI_FEED_INTERVAL.get(n, 180)) for n, u, t in AI_FEEDS]

FEEDS = ((NEWS_FEEDS if NEWS_ENABLED else []) + (CRYPTO_FEEDS if CRYPTO_ENABLED else []) + AI_FEEDS4)

# ---- FinancialJuice (added 2026-10-26 at Jjuko's request): LEADS ONLY, headline-worthy items only.
# Public RSS (curl-checked 2026-10-26 ~14:20 EAT: HTTP 200, 100 items, fresh within minutes, no ETag).
# FJ is a very noisy squawk: data prints, speaker soundbites, "sources" chatter, vol/probability tables.
# score_fj() drops routine items, needs an FJ_MAJOR signal AND score >= FJ_MIN_SCORE (normal news min is 9).
# Never cite FinancialJuice: each hit carries lead_only/verify_note; confirm at the primary source first.
# Not fast-lane unless the line is attributed to a top wire (Reuters/Bloomberg/AP/...) and scores high.
FJ_NAME = "FinancialJuice"
FJ_URL = "https://www.financialjuice.com/feed.ashx?xy=rss"
FJ_POLL_S = 75
FJ_MIN_SCORE = 12
FJ_FEEDS = [(FJ_NAME, FJ_URL, "aggregator", FJ_POLL_S)] if os.environ.get("HERALD_FJ", "1") == "1" else []
FEEDS = FEEDS + FJ_FEEDS
PRIME_ON_FIRST_FETCH = {FJ_NAME} | LATENCY_NEW_FEEDS   # new feed: first fetch only marks items seen (no backlog burst)
FJ_PREFIX = re.compile(r"^\s*financialjuice:\s*", re.I)
# routine noise: always dropped
FJ_NOISE = re.compile(
    r"fjelite|interest rate probabilities|implied volatility|correlation matrix|currency strength|"
    r"\bcot\b|positioning|options? expir|\bfx options\b|\bpoll:|\bpoll shows\b|\bsurvey\b(?!.*\b(record|crash)\b)|"
    r"target price|price target|\b(upgrades?|downgrades?|raises|cuts|lowers) (to|target)|overweight|underweight|equal-weight|"
    r"\bwatch ?list\b|\bestimate\b|midpoint|reverse repos?|\binjects?\b|\bliquidity operation|\bauction\b|"
    r"\bsells? \d+-day\b|stabilisation bonds|\bfutures (rise|fall|edge|gain|slip)\b|early (asian )?trad|"
    r"\b(rises?|falls?|edges?|gains?|slips?|dips?|up|down) \d(\.\d+)?%(?! in a day)|"
    r"\bwhat to expect\b|\bpreview\b|\breview\b|\bsentiment\b|\bcalendar\b|\bexpected to\b|\bexpects?\b|\bhopes?\b|"
    r"\bsources?\b|\bsource says\b|\bpeople familiar\b|\breportedly\b|\brumou?r|\bunconfirmed\b|"
    r"files (mixed )?(securities )?shelf|\b(q[1-4]|fy) \d{4} earnings$|^\$?[a-z]{1,5} .{0,40} earnings$|"
    r"no immediate (\w+ )?confirmation|\bintercept\w*|\btargeted\b|\bclaims? responsibility\b|\bwe will not\b|\bnot proposing\b|\bseeks public feedback\b|\bconsidering\b|\bplans? to compile\b", re.I)
# a "Speaker: quote" line. Kept only if the quote itself is a FJ_MAJOR event.
FJ_SPEAKER = re.compile(r"^[^:]{2,70}:\s+\S")
# economic data prints: "<Indicator> Actual A (Forecast F, Previous P)"
FJ_DATA = re.compile(r"^(?P<ind>.+?)\s+actual\s+(?P<a>-?[\d.,]+)\s*(?P<u>%|k|m|b)?\s*\(forecast\s+(?P<f>-?[\d.,]+|-)\s*(%|k|m|b)?", re.I)
# only these prints can ever pass, and only with a big surprise vs forecast (abs difference)
FJ_DATA_MAJOR = [
    (re.compile(r"^(us )?(fed|fomc).{0,30}(interest )?rate decision", re.I), 0.0),     # any Fed decision
    (re.compile(r"^(ecb|boe|boj|bank of (england|japan)).{0,30}rate decision", re.I), 0.0),
    (re.compile(r"^(us )?non-?farm payrolls", re.I), 75.0),                             # thousands
    (re.compile(r"^(us )?(core )?cpi (yoy|y/y|mom|m/m)", re.I), 0.2),
    (re.compile(r"^(us )?unemployment rate", re.I), 0.2),
    (re.compile(r"^(us )?(gdp|gdp growth rate).{0,20}(qoq|annuali[sz]ed|advance)", re.I), 1.0),
]
# headline-worthy signal required for every non-data FJ item
FJ_MAJOR = re.compile(
    # central-bank decisions / emergency action
    r"\b(fed|fomc|federal reserve|ecb|boe|boj|bank of (england|japan)|pboc|snb)\b.{0,40}\b(cuts?|raises?|hikes?|holds?|leaves|keeps|unexpectedly)\b.{0,30}\b(rates?|basis points|bps|bp)\b|"
    r"\bemergency (rate|meeting|cut)\b|\bintervene[sd]? in (the )?(fx|currency|yen)|\bintervention\b|"
    # big market moves
    r"\bcircuit breaker|\btrading (halted|suspended)\b|\bflash crash\b|"
    r"\b(s&p 500|nasdaq|dow|stocks|gold|silver|oil|brent|wti|bitcoin|btc|yen|dollar|yuan|treasury yields?)\b.{0,40}\b(record high|all[- ]time high|crash\w*|plunges?|plummets?|soars?|biggest (drop|fall|gain|rise))\b|"
    r"\b(1\d|[2-9]\d)(\.\d+)?% in a day\b|"
    # war / geopolitical shock
    r"\bdeclares? war\b|\binva(des?|sion)\b|\bnuclear (test|strike|attack|weapon)\b|\bmartial law\b|\bcoup\b|\bassassinat|"
    r"\bstate of emergency\b|\bceasefire\b.{0,30}\b(agreed|reached|signed|collapses|ends|announced|takes effect|starts|begins)\b|"
    r"\b(attacks?|strikes?|missiles?|drones?|bomb\w*|explosions?)\b.{0,50}\b(riyadh|tehran|tel aviv|jerusalem|taipei|beijing|washington|seoul|abu dhabi|dubai|doha|pyongyang)\b|"
    r"\b(strikes?|attacks?)\b.{0,30}\bnuclear (plant|site|facilit\w*)\b|"
    r"\bstrait of hormuz\b.{0,40}\b(closed?|clos(es|ing)|block\w*|mine\w*)\b|"
    # leaders
    r"\b(president|prime minister|pm|chancellor|king|queen|pope|supreme leader|crown prince)\b.{0,40}\b(dies|died|dead|resigns|resigned|steps down|ousted|impeached|assassinated|arrested|killed)\b|"
    r"\b(dies|died) at \d+\b.{0,0}|"
    r"\b(wins?|won|concedes?)\b.{0,30}\b(election|presidency|runoff|referendum)\b|\bcalls? (snap|early) election\b|"
    # big company / government announcements
    r"\b(acquire|acquires|acquisition|merger|takeover|buy|buys|to buy|offer|stock sale|stake|invest\w*|deal|bid)\b.{0,60}\$\s?(1\d|[2-9]\d|\d{3,})(\.\d+)?\s?(b|bn|billion)\b|"
    r"\$\s?(1\d|[2-9]\d|\d{3,})(\.\d+)?\s?(b|bn|billion)\b.{0,60}\b(acquire|acquisition|merger|takeover|stock sale|offering|deal|bid)\b|"
    r"\$\s?\d+(\.\d+)?\s?(t|tn|trillion)\b|"
    r"\b(bankrupt\w*|chapter 11|defaults? on)\b|"
    r"\b(trump|white house|u\.?s\.?|china|eu|beijing)\b.{0,40}\b(imposes?|announces?|raises?|slaps?|to impose)\b.{0,30}\b(tariffs?|sanctions|export (ban|controls?))\b|"
    r"\bexecutive order\b.{0,40}\b(signs?|signed)\b|\bsigns?\b.{0,30}\bexecutive order\b", re.I)
# speed research 2026-10-27: headline-grade company / AI / legal / regulatory events. These hit FJ first but were dropped
# (Anthropic S-1, MSFT 60k buyouts, GPT-6.1, Comey charged, Ripple OCC approval). Matched on the full line; floor at FJ_MIN_SCORE.
FJ_HEADLINE = re.compile(
    r"\b(files?|filed|filing) (publicly )?for (an? )?(ipo|initial public offering|nasdaq listing|nyse listing)\b|\bapplied to list\b.{0,40}\b(nasdaq|nyse)\b|"
    r"\b(lay ?offs?|lays? off|job cuts?|cuts?|cutting|eliminat\w+|buyouts?)\b.{0,40}\b(\d{2,3},?\d{3}|\d+(\.\d+)?\s?(k|thousand))\s+(jobs|workers|employees|staff|roles|positions)\b|"
    r"\b(gpt|claude|gemini|grok|llama|qwen|deepseek)[- ]?\d+(\.\d+)?\b.{0,40}\b(is )?(rolling out|released?|launch(es|ed)?|unveil\w*|now available)\b|"
    r"\b(releases?|launch(es)?|unveils?|rolls? out)\b.{0,20}\b(gpt|claude|gemini|grok|llama)[- ]?\d+(\.\d+)?\b|"
    r"\b(charges?|charged|indicts?|indicted|arrests?|arrested)\b.{0,30}\b(former|ex-?)\s?(fbi director|cia director|president|prime minister|attorney general|national security adviser|secretary of \w+)\b|"
    r"\b(former|ex-?)\s?(fbi director|cia director|president|prime minister)\b.{0,40}\b(charged|indicted|arrested|convicted|sentenced)\b|"
    r"\b(final|full) (us |federal |regulatory |occ |sec )*approval\b|\bnational trust bank\b.{0,30}\b(approv\w+|charter)\b|\bapproves? (the )?(first )?spot\b.{0,20}\betfs?\b", re.I)
# top-tier FJ_MAJOR events (central-bank decisions, war on a capital/nuclear site, declared war/invasion/coup): floor at FJ_MIN_SCORE
FJ_TOP = re.compile(
    r"\b(fed|fomc|federal reserve|ecb|boe|boj|bank of (england|japan))\b.{0,40}\b(cuts?|raises?|hikes?|holds?|leaves|keeps|unexpectedly)\b.{0,30}\b(rates?|basis points|bps|bp)\b|"
    r"\bemergency (rate|meeting|cut)\b|\bdeclares? war\b|\binva(des?|sion)\b|\bcoup\b|\bmartial law\b|\bnuclear (test|strike|attack)\b|"
    r"\b(attacks?|strikes?|missiles?|drones?|bomb\w*|explosions?)\b.{0,50}\b(riyadh|tehran|tel aviv|jerusalem|taipei|beijing|washington|seoul|abu dhabi|dubai|doha|pyongyang)\b|"
    r"\b(strikes?|attacks?)\b.{0,30}\bnuclear (plant|site|facilit\w*)\b", re.I)
FJ_WIRE_ATTRIB = re.compile(r"[:\-–]\s*(reuters|bloomberg|ap|associated press|nikkei|bbc|wsj)\.?\s*$", re.I)
FJ_ATTRIB_TAIL = re.compile(r"\s*(?::|-|–)\s*(ifx|tass|ria|cctv|state tv|statement|governor|nikkei|axios|reuters|bloomberg|"
                            r"the times|kcna|tasnim|sepah news|govt data|government data|notice|[a-z]{2,12})\.?\s*$", re.I)
NEWS_FEED_NAMES = {f[0] for f in NEWS_FEEDS}
CRYPTO_FEED_NAMES = {f[0] for f in CRYPTO_FEEDS}

# general-purpose AI feeds: item must mention AI
AI_REQUIRED = {"Meta newsroom", "CNBC Technology", "NVIDIA newsroom", "The Hacker News", "BleepingComputer"}
AI_OFFICIAL_TOP5 = {"OpenAI news", "Google AI blog", "Google DeepMind blog", "GN: anthropic.com", "GN: x.ai"}

CRYPTO_TERM = re.compile(r"crypto|bitcoin|digital asset|stablecoin|blockchain|token|"
                         r"\bxrp\b|ethereum|\bbtc\b|virtual currenc", re.I)
IMPACT = re.compile(
    r"\betfs?\b|\bsec\b|approv|lawsuit|\bsues?\b|settle|\bhack|exploit|drain|stolen|"
    r"partnership|partners with|\blaunch|upgrade|amendment|mainnet|\bbanks?\b|\bfed\b|"
    r"federal reserve|\bban(s|ned)?\b|liquidat|record|all-time high|\bath\b|inflows?|outflows?|"
    r"acquir|acquisition|listing|delist|charter|license|regulat|bill\b|senate|congress|"
    r"treasury|reserve|crash|plunge|surge|soar|tumble|halt|outage|bankrupt|insolven|"
    r"slips?|drops?|falls?|jumps?|rall(y|ies)|breaks?|longs|shorts|wiped|freez|seiz|arrest|"
    r"tokeniz|stablecoin|custody|integrat|adopt",
    re.I)
BIG_EVENT = re.compile(
    r"(exchange|bridge|protocol|wallet).{0,40}(hack|exploit|drain|breach)|"
    r"(hack|exploit|drain|breach).{0,40}(exchange|bridge|protocol)|"
    r"insolven|bankrupt|withdrawals? (halt|paus|suspend)|halts? withdrawals|"
    r"(sec|cftc|congress|senate|white house|treasury).{0,60}(crypto|stablecoin|digital asset)|"
    r"(crypto|stablecoin|digital asset).{0,60}(bill|law|executive order|framework).{0,30}(pass|sign|approv|vote)",
    re.I)
MONEY = re.compile(r"\$\s?(\d+(?:\.\d+)?)\s?(million|m\b|billion|bn|b\b)", re.I)
CLICKBAIT = re.compile(
    r"price prediction|predicts?|could (reach|hit|soar|explode)|will (hit|reach|soar)|"
    r"to the moon|here'?s why|here is why|top \d+|best crypto|\bpresale\b|\d+x\b|"
    r"millionaire|should you buy|is it too late|price analysis|technical analysis|"
    r"what'?s next for|next \d+ (days|hours)|expert says|analyst says|"
    r"\bwhale\b.*\b(buys?|sells?|moves?)\b|sentiment|chart pattern|\beyes\b|breakout|"
    r"\btargets?\b|price faces|downside risk|bullish|bearish|^live:|\bprice today\b", re.I)
SPAM = re.compile(r"sponsored|press release|partner content|\bpr\b:|advertorial|"
                  r"globenewswire|prnewswire|accesswire|business wire|newsfile|"
                  r"benzinga pro|coinpedia|crypto-economy|zycrypto|coingape|"
                  r"thecryptobasic|times tabloid|bitcoinist|newsbtc|captainaltcoin|"
                  r"crypto news flash|finbold|watcher\.guru|ambcrypto|pluang|traders union|"
                  r"hokanews|cryptopolitan|cryptoticker|coin gabbar|techbullion|streetinsider|"
                  r"cryptonewsz|coincodex|blockchain reporter|crypto daily|bitget|kucoin|"
                  r"binance square|mexc|tradingview|investinglive|economies\.com", re.I)
QUESTION_BAIT = re.compile(r"\?\s*$|worth buying|buy now|should you|is it time", re.I)
# ---- AI scoring
AI_TOP5 = {
    "openai": r"\bopenai\b|chatgpt|\bgpt-?\d|\bsora\b|sam altman|\baltman\b|\bo\d(-mini|-pro)?\b(?=.*model)",
    "google": r"\bgemini\b|deepmind|\bgoogle\b.{0,40}\b(ai|model|chatbot)\b|\bsundar pichai\b.{0,60}\bai\b|\bveo\b|\bimagen\b",
    "anthropic": r"anthropic|\bclaude\b|dario amodei",
    "xai": r"\bxai\b|\bgrok\b|colossus",
    "meta": r"\bllama\b|meta ai\b|meta superintelligence|\bmeta\b.{0,40}\b(ai|model|superintelligence)\b|zuckerberg.{0,60}\bai\b",
}
AI_OTHER = re.compile(
    r"\bnvidia\b|jensen huang|copilot|apple intelligence|\bsiri\b.{0,30}\b(ai|gemini|llm)\b|mistral|deepseek|"
    r"perplexity|\bqwen\b|alibaba.{0,30}\bai\b|microsoft.{0,40}\b(ai|openai)\b|amazon.{0,30}\b(ai|anthropic|nova)\b|"
    r"hugging ?face|safe superintelligence|\bssi\b|thinking machines|ilya sutskever|moonshot|\bkimi\b|"
    r"\bagi\b|superintelligence", re.I)
AI_TERM = re.compile(r"\bai\b|artificial intelligence|machine learning|\bllms?\b|chatbot|"
                     r"language model|generative|\bagents?\b|gpu|data center|openai|gemini|claude|grok|llama|"
                     r"copilot|superintelligence|nvidia|anthropic|\bxai\b|deepseek|mistral|\bchips?\b", re.I)
AI_EVENT = re.compile(
    r"\blaunch(es|ed)?\b|\breleas(e|es|ed)\b|\bunveil|\bintroduc(es|ing|ed)\b|\bdebuts?\b|\bannounc|"
    r"\brais(es|ed|ing)\b|\bfunding\b|\bvaluation\b|\bvalued\b|\bipo\b|\bacquir|\bacquisition\b|\bbuys\b|"
    r"\bmerger\b|\bdeal\b|\bpartnership\b|\blawsuit\b|\bsues?\b|\bsued\b|\bcourt\b|\bruling\b|\bsettle|"
    r"\bbans?\b|\bbanned\b|\bregulat|\bantitrust\b|\bprobe\b|\binvestigat|\bfines\b|\bfined\b|executive order|"
    r"\bai act\b|\bsenate\b|\bcongress\b|\boutage\b|\bdown for\b|\bbreach\b|\bleak(s|ed)?\b|"
    r"\bresigns?\b|\bsteps down\b|\bfired\b|\bousted\b|\bhires?\b|\blayoffs?\b|\bexport\b|\bchips? (ban|curbs?|rules)\b|"
    r"\brecord\b|\bopen[- ]sources?\b|\bopen[- ]weights?\b|\bbenchmark\b|\bblock(s|ed)\b",
    re.I)
AI_MODEL_VER = re.compile(
    r"\bgpt-?\d(\.\d+)?\b|\bgemini \d(\.\d+)?\b|\bclaude (\w+ )?\d(\.\d+)?\b|"
    r"\b(opus|sonnet|haiku) \d|\bgrok \d(\.\d+)?\b|\bllama \d|\bdeepseek[- ]?(v|r)\d|\bsora \d|\bveo \d|"
    r"\bmistral (large|medium) \d|\bqwen ?\d|\bdeepseek\b.{0,30}\b(v|r)\d", re.I)
AI_BIG_EVENT = re.compile(
    r"(openai|anthropic|xai|deepmind|mistral|deepseek|perplexity|nvidia).{0,60}"
    r"(raises|valuation|valued|ipo|acquires|to acquire|buys|sued|lawsuit|ban|antitrust|outage)|"
    r"(chatgpt|claude|gemini|grok|copilot).{0,30}(outage|is down|goes down|banned)|"
    r"(ban|bans|banned|restrict).{0,40}(chatgpt|deepseek|openai|nvidia chips|ai chips)|"
    r"(ai act|executive order|ai bill|ai law).{0,40}(sign|pass|vote|take effect|delay)|"
    r"(ceo|cto|chief scientist|co-?founder).{0,40}(resigns|steps down|fired|ousted|leaves)|"
    r"(sues?|sued|lawsuit against|bans?|blocks|\bfines\b|fined|probe into|antitrust).{0,40}"
    r"(openai|anthropic|xai|google|meta|nvidia|deepseek|chatgpt|microsoft|apple)|"
    r"(eu|ftc|doj|sec|regulators?|court|judge).{0,40}(\bfines\b|sues|blocks|bans|orders|rules against)",
    re.I)
AI_CLICKBAIT = re.compile(
    r"you should know|you need to|should change|how to\b|\btips\b|\bguide\b|explained|\breview\b|hands[- ]on|"
    r"\bi tried\b|\bi asked\b|best ai|\bdeals?\b|% off|\bsale\b|podcast|newsletter|this week in|roundup|"
    r"\bopinion\b|op-ed|why you|everything we know|what is\b|\bvs\.?\b|\bcheat sheet\b|\bprompts?\b to|"
    r"\bstocks? to buy\b|price target|\bshares of\b|stake in|\bholdings?\b|\bquiz\b|\bgadgets\b|"
    r"^watch:|^video:|^live:|\d+ questions|need answered|what (it|this) means for (you|investors)|"
    r"\bis [\w ]{1,25} down today\b|\bhere'?s (why|what|how)\b|top \d+|\d+ (things|ways|reasons)", re.I)
# ---- AI security (strict: needs a named AI player, or on a security feed a generic AI target, plus a security event)
AI_SEC_EVENT = re.compile(
    r"prompt[- ]injection|jailbr(eak|oken)|deepfake|data leak|\bleak(s|ed)?\b|\bbreach(es|ed)?\b|vulnerab|\bflaws?\b|"
    r"\bexploit(s|ed)?\b|zero[- ]day|\bcve-\d|malware|backdoor|supply[- ]chain attack|\bhijack|exfiltrat|"
    r"sandbox escape|\bescapes?\b|\bcompromis|\bst(ole|eal|olen)\b|phishing|ransomware|\bexposed?\b|"
    r"\bscams?\b|impersonat|voice clon|poison", re.I)
AI_SEC_SEVERE = re.compile(
    r"zero[- ]click|actively exploited|\bcritical\b|\bmillions?\b|\bbillions?\b|"
    r"\d[\d,]* (users|accounts|customers|organi[sz]ations|companies)|supply[- ]chain|\bst(ole|eal|olen)\b|"
    r"\bhijack|\bescapes?\b|exfiltrat|malware|backdoor|ransomware|zero[- ]day", re.I)
AI_SEC_GENERIC = re.compile(r"\bai agents?\b|\bagentic\b|\bllms?\b|\bai models?\b|\bmcp\b|litellm|deepfake|"
                            r"\bai (tools?|platforms?|assistants?|chatbots?|coding|browsers?)\b", re.I)
AI_SPAM = re.compile(r"marketbeat|fello ai|neoteo|ssbcrack|metroindia|indianweb2|tech-insider\.org|"
                     r"techstory|analytics insight|aitoolsclub|geeky gadgets|digital journal|stocktwits|"
                     r"tradingkey|stockstotrade|klse screener|xenospect|pasquale pillitteri|"
                     r"\bwebinar\b|threatsday|weekly recap", re.I)
log = logging.getLogger("herald-watcher")
_stop = False


# ------------------------------------------------------------------ utils
def now_ts() -> float:
    return time.time()

def eat(ts: float | None = None) -> str:
    return dt.datetime.fromtimestamp(ts or now_ts(), TZ).strftime("%Y-%m-%d %H:%M:%S EAT")

def setup_logging(verbose: bool):
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=1_000_000, backupCount=3)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    if verbose:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        log.addHandler(sh)

def load_state() -> dict:
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(st: dict):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f)
    os.replace(tmp, STATE_PATH)

def http_get(url: str, headers: dict | None = None):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, r.read(3_000_000), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, b"", dict(e.headers or {})

def norm_url(u: str) -> str:
    try:
        p = urllib.parse.urlsplit(u.strip())
        q = urllib.parse.parse_qsl(p.query)
        q = [(k, v) for k, v in q if not k.lower().startswith(("utm_", "ref", "fbclid"))]
        return urllib.parse.urlunsplit((p.scheme.lower(), p.netloc.lower().removeprefix("www."),
                                        p.path.rstrip("/"), urllib.parse.urlencode(q), ""))
    except Exception:
        return u

STOP = set("a an the of to in on for and or is are as at by with from its it this that be has have "
           "after over into new says said report reports amid could will".split())

def title_tokens(t: str) -> set:
    t = re.sub(r"[^a-z0-9$.% ]", " ", t.lower())
    return {w for w in t.split() if w not in STOP and len(w) > 1}

def similar(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

def strip_tags(s: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", s or "")).strip()


# ------------------------------------------------------------------ feeds
def parse_date(s: str | None) -> float | None:
    if not s:
        return None
    s = s.strip()
    try:
        d = email.utils.parsedate_to_datetime(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.timestamp()
    except Exception:
        pass
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.timestamp()
    except Exception:
        return None

def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()

def parse_feed(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    items = []
    for el in root.iter():
        if local(el.tag) not in ("item", "entry"):
            continue
        it = {"title": "", "link": "", "date": None, "summary": "", "src": ""}
        for c in el:
            n = local(c.tag)
            if n == "title":
                it["title"] = strip_tags(c.text or "")
            elif n == "link":
                href = c.get("href")
                if href and (c.get("rel") in (None, "alternate")):
                    it["link"] = href
                elif c.text and not it["link"]:
                    it["link"] = c.text.strip()
            elif n in ("pubdate", "published", "updated", "date") and not it["date"]:
                it["date"] = parse_date(c.text)
            elif n in ("description", "summary") and not it["summary"]:
                it["summary"] = strip_tags(c.text or "")[:500]
            elif n == "source":
                it["src"] = (c.text or "").strip()
        if it["title"]:
            items.append(it)
    # Google News sitemap (Reuters): <url><loc/><news:news><news:publication_date/><news:title/></news:news></url>
    if not items and local(root.tag) == "urlset":
        for el in root:
            if local(el.tag) != "url":
                continue
            it = {"title": "", "link": "", "date": None, "summary": "", "src": ""}
            for c in el.iter():
                n = local(c.tag)
                if n == "loc" and not it["link"] and local(c.tag) == "loc" and "/images" not in (c.text or "")[:60]:
                    it["link"] = (c.text or "").strip()
                elif n == "title":
                    it["title"] = strip_tags(c.text or "")
                elif n == "publication_date":
                    it["date"] = parse_date(c.text)
            if it["title"] and it["link"]:
                items.append(it)
    return items

class FeedHTTPError(RuntimeError):
    def __init__(self, code: int):
        super().__init__(f"HTTP {code}")
        self.code = code


def fetch_prepare(url: str, st: dict) -> dict:
    """Conditional-GET headers (ETag / Last-Modified) for this URL. Main thread only."""
    cache = st.setdefault("http_cache", {}).setdefault(url, {})
    hdr = {}
    if cache.get("etag"):
        hdr["If-None-Match"] = cache["etag"]
    if cache.get("lm"):
        hdr["If-Modified-Since"] = cache["lm"]
    return hdr


def fetch_http(url: str, hdr: dict):
    """Worker-thread HTTP fetch. Retries once after 1.5 s on a stray 404/5xx/timeout (federalreserve.gov's CDN
    returns an occasional 404 for a URL that is fine on the next request). Returns (code, body, headers)."""
    for attempt in (0, 1):
        try:
            code, body, headers = http_get(url, hdr)
        except Exception:
            if attempt:
                raise
            time.sleep(1.5)
            continue
        if attempt == 0 and (code == 404 or code in (500, 502, 503, 504)):
            time.sleep(1.5)
            continue
        return code, body, headers


def fetch_finish(url: str, st: dict, code: int, body: bytes, headers: dict) -> list[dict]:
    cache = st.setdefault("http_cache", {}).setdefault(url, {})
    if code == 304:
        return []
    if code != 200:
        raise FeedHTTPError(code)
    h = {k.lower(): v for k, v in headers.items()}
    cache["etag"] = h.get("etag")
    cache["lm"] = h.get("last-modified")
    return parse_feed(body)


def fetch_feed(name: str, url: str, st: dict) -> list[dict]:
    return fetch_finish(url, st, *fetch_http(url, fetch_prepare(url, st)))


def host_of(url: str) -> str:
    try:
        return urllib.parse.urlsplit(url).netloc.lower()
    except Exception:
        return ""

# ------------------------------------------------------------------ general (Watcher Guru style) scoring
# Entity groups (title). Specific match text is used for the story key.
ENT = {
    "crypto": re.compile(r"\bbitcoin\b|\bbtc\b|\bethereum\b|\bether\b|\beth\b|\bsolana\b|\bsol\b|\bxrp\b|\bripple\b|"
                         r"\bstablecoins?\b|\bcrypto(currency|currencies)?\b|\bbinance\b|\bcoinbase\b|\btether\b|\busdt\b|\busdc\b|"
                         r"\bcircle\b|\bmicrostrategy\b|\bsaylor\b|\bstrategy\b(?=.{0,40}\b(bitcoin|btc)\b)|\bdogecoin\b|\bdoge\b|"
                         r"\btokeni[sz]|\bdigital assets?\b|\bmemecoin|\bdefi\b|\bblockchain\b|\bhyperliquid\b|\bbnb\b|\bkraken\b|"
                         r"\bbitmine\b|\bmetaplanet\b|\bgenius act\b|\bclarity act\b", re.I),
    "macro": re.compile(r"\bfed\b|federal reserve|\bpowell\b|\bfomc\b|interest rates?|rate (cut|hike)s?|\bcpi\b|\bppi\b|"
                        r"inflation|consumer price index|producer price index|retail sales|\bpmi\b|jobs report|non-?farm|payrolls|unemployment|jobless|\bgdp\b|recession|\btreasury\b|"
                        r"\bbessent\b|national debt|debt ceiling|\byields?\b|\btariffs?\b|government shutdown|\bimf\b|"
                        r"central banks?|\becb\b|bank of (japan|england)|\bpboc\b", re.I),
    "markets": re.compile(r"s&p 500|\bs&p\b|\bnasdaq\b|\bdow\b|stock market|wall street|\bstocks\b|\bgold\b|\bsilver\b|"
                          r"\boil\b|\bbrent\b|\bcrude\b|\bbonds?\b|\bkospi\b|\bnikkei\b(?! (asia|reports?|says|reported))|hang seng|\bftse\b|market cap|"
                          r"\bipo\b|\bshares\b|\betfs?\b|\bvix\b|bear market|bull market|\$[A-Z]{2,5}\b", re.I),
    "bigco": re.compile(r"\bnvidia\b|\btesla\b|\bspacex\b|\bstarlink\b|\bapple\b|\bmeta\b|\bmicrosoft\b|\bamazon\b|"
                        r"\bgoogle\b|\balphabet\b|\bjpmorgan\b|\bgoldman\b|bank of america|wells fargo|morgan stanley|"
                        r"\bciti(group)?\b|\bvisa\b|\bmastercard\b|\bblackrock\b|\brobinhood\b|\bberkshire\b|\bnetflix\b|"
                        r"\bopenai\b|\banthropic\b|\bxai\b|\bpaypal\b|\bschwab\b|\bfidelity\b|\bintel\b|\bamd\b|\btsmc\b|"
                        r"\bboeing\b|\bsamsung\b|\bbroadcom\b|\boracle\b|\bpalantir\b|\bx\.com\b|\btwitter\b|\bgta\b", re.I),
    "people": re.compile(r"\btrump\b|\bmusk\b|\bpowell\b|\bbessent\b|\bdimon\b|larry fink|\bbuffett\b|\bzuckerberg\b|"
                         r"\baltman\b|jensen huang|\bxi( jinping)?\b|\bputin\b|\bzelensky[iy]?\b|\bnetanyahu\b|\bvance\b|"
                         r"\bmilei\b|\bmodi\b|\bcz\b|changpeng zhao|\bsaylor\b|\bgensler\b|\batkins\b|\blutnick\b|"
                         r"\bcathie wood\b|\bpeter schiff\b|\bbezos\b|\bcook\b(?=.{0,20}apple)", re.I),
    "world": re.compile(r"\bchina\b|\bchinese\b|\brussia\b|\bukraine\b|\bisrael\b|\biran\b|\bgaza\b|\bhamas\b|\bhezbollah\b|"
                        r"\beu\b|european union|\beurope\b|\buk\b|\bbritain\b|\bjapan\b|\bindia\b|north korea|\bsaudi\b|"
                        r"\buae\b|\bdubai\b|\bvenezuela\b|\btaiwan\b|\bcanada\b|\bmexico\b|\bnato\b|\bg7\b|\bg20\b|"
                        r"\bswitzerland\b|\bgermany\b|\bfrance\b|\bsouth korea\b|\bargentina\b|\bbrazil\b|\bturkey\b|"
                        r"\bpentagon\b|\bwhite house\b|\bcongress\b|\bsenate\b|\bsupreme court\b|\bkremlin\b|"
                        # 2026-10-26 world expansion
                        r"\bserbia\b|\bserbian\b|\bbrazil(ian)?\b|\bargentin(a|e|ian)\b|\bmexic(o|an)\b|\bt(u|ü)rk(ey|iye|ish)\b|"
                        r"\biranian\b|\bisraeli\b|\bukrainian\b|\brussian\b|\btaiwanese\b|\bindian\b|\bpakistan(i)?\b|"
                        r"\bnorth korean\b|\bsaudi arabia\b|\bunited kingdom\b|\bbritish\b|\bfrench\b|\bgerman\b|"
                        r"\bjapanese\b|\bvenezuelan\b|\buganda(n)?\b|\bkenya(n)?\b|\bnigeria(n)?\b|south africa(n)?\b|"
                        r"\bethiopia(n)?\b|\bghana(ian)?\b|\btanzania(n)?\b|\brwanda(n)?\b|\begypt(ian)?\b|\bsyria(n)?\b|"
                        r"\blebanon\b|\biraq(i)?\b|\byemen(i)?\b|\bafghanistan\b|\bpoland\b|\bpolish\b|\bhungary\b|"
                        r"\bitaly\b|\bitalian\b|\bspain\b|\bspanish\b|\bnetherlands\b|\bdutch\b|\bcolombia(n)?\b|"
                        r"\bchile(an)?\b|\bperu(vian)?\b|\bcuba(n)?\b|\bphilippines\b|\bindonesia(n)?\b|\bvietnam(ese)?\b|"
                        r"\bthailand\b|\bmyanmar\b|\bbangladesh\b|\bsri lanka\b|\baustralia(n)?\b|\bnew zealand\b|"
                        r"\bgeorgia\b(?! state)|\bmoldova\b|\bromania(n)?\b|\bbelarus\b|\bqatar\b|\bsudan\b|\bniger\b|"
                        r"\bmali\b|\bdrc\b|\bcongo\b|"
                        # 2026-10-26 scoring pass: countries/capitals/bodies seen in missed stories
                        r"\bjordan(ian)?\b(?! (peterson|spieth|love|poole|clarkson|henderson|chiles|brand))|\bengland\b|\bscotland\b|\bwales\b|\bwelsh\b|\bscottish\b|\bireland\b|\birish\b|"
                        r"\bnhs\b|\bbma\b|\bafrica cdc\b|\bafrica\b|\bafrican union\b|\bwho\b(?= (says|declares|warns|chief|director))|world health organi[sz]ation|"
                        r"\btehran\b|\briyadh\b|\bmoscow\b|\bkyiv\b|\bbeijing\b|\bjerusalem\b|\bislamabad\b|\bankara\b|\bcairo\b|\bdamascus\b|\bbeirut\b|\bbaghdad\b|"
                        r"\blondon\b|\bparis\b|\bberlin\b|\btokyo\b|\bseoul\b|\bnew delhi\b|\bwest bank\b|\bpalestin\w*\b|\bkuwait\w*\b|\boman\b|\bbahrain\w*\b|"
                        r"\blibya\w*\b|\btunisia\w*\b|\balgeria\w*\b|\bmoroc\w*\b|\bkazakh\w*\b|\barmenia\w*\b|\bazerbaijan\w*\b|\bnepal\w*\b|\bcambodia\w*\b|"
                        r"\bmalaysia\w*\b|\bsingapore\w*\b|\bhong kong\b|\bgreece\b|\bgreek\b|\bportug\w*\b|\bswed(en|ish)\b|\bnorw(ay|egian)\b|\bfin(land|nish)\b|"
                        r"\bdenmark\b|\bdanish\b|\bbelgi\w*\b|\baustria\w*\b|\bczech\b|\bcroatia\w*\b|\bbosnia\w*\b|\bkosovo\b|\bcyprus\b|\bzimbabwe\w*\b|\bzambia\w*\b|"
                        r"\bmozambique\b|\bangola\w*\b|\bsenegal\w*\b|\bcameroon\w*\b|\bchad\b|\bburkina\b|\bsouth sudan\b|\beritrea\w*\b|\bburundi\w*\b|\bmalawi\w*\b|"
                        r"\bmadagascar\b|\bnicaragua\w*\b|\bguatemala\w*\b|\bhonduras\b|\bel salvador\b|\bpanama\w*\b|\bgulf\b(?= (states|nations|of|countries))|\bmideast\b|\bmiddle east\b|"
                        r"\bmecca pact\b|\bemirati\b|\bqatari\b|\bsaudis?\b|\bsomali\b|\bethiopian\b|\bcongolese\b|"
                        r"\bsomalia\b|\bhaiti\b|\bbolivia\b|\becuador\b|\bunited nations\b|\bu\.?n\.? (security council|chief)\b", re.I),
    # 2026-10-26: politics/leaders (elections, resignations, deaths of leaders)
    "politics": re.compile(r"\belections?\b|\bpresiden(t|cy|tial)\b|prime minister|\bpremier\b|\b(ex-|former )?pm\b|\bchancellor\b|"
                           r"\bparliament(ary)?\b|\breferendum\b|\brunoff\b|\bopposition leader\b|\bking\b|\bpope\b|\bmonarch\b|"
                           r"\blula\b|\bvu(c|č)i(c|ć)\b|\bmuseveni\b|\bbobi wine\b|\bruto\b|\btinubu\b|\bramaphosa\b|\bstarmer\b|"
                           r"\bmacron\b|\bmerz\b|\berdo(g|ğ)an\b|\bkhamenei\b|\bsheinbaum\b|\bmaduro\b|\bmachado\b|\btakaichi\b|"
                           r"\bishiba\b|\bcarney\b|\balbanese\b|\bmeloni\b|\bsanchez\b|\borb(a|á)n\b|\bkim jong un\b|\bmbs\b|"
                           r"\bpezeshkian\b|\bsharif\b|\bprabowo\b|\bmarcos\b|\bvon der leyen\b|\bguterres\b|\blee jae[- ]myung\b", re.I),
    "crisis": re.compile(r"\bearthquake\b|\btsunami\b|\bcoup\b|\binvasion\b|\binvades?\b|\bceasefire\b|\bassassinat|"
                         r"\bmartial law\b|state of emergency|\bhurricane\b|\btyphoon\b|\bcyclone\b|\bvolcan|\bnuclear\b|"
                         r"\bterror(ist)? attack\b|\bmass shooting\b|\bplane crash\b|\bwar\b", re.I),
}
HARD_EVENT = re.compile(
    # labour action
    r"\b(to|will|on|set to|begin|begins|began|announce[sd]?|call(s|ed)?|stage[sd]?|vote[sd]? (to|for))\s+(an?\s+)?(\w+-day\s+|\w+-hour\s+|national\s+|general\s+|indefinite\s+|new\s+)?(strikes?|walkouts?)\b|"
    r"\bgo on (an? )?(\w+-day |\w+-hour |national |general |indefinite )?strike\b|\bgeneral strike\b|\bwalk-?outs?\b|\bstrike action\b|\bstrike (over|for|by|at)\b(?! (iran|israel|gaza|ukraine|russia))|"
    # outbreaks
    r"\bebola\b|\bcholera\b|\bmpox\b|\bmarburg\b|\bbird flu\b|\bh5n1\b|\boutbreak\b|\bepidemic\b|\bpandemic\b|\bpublic health emergency\b|"
    # attacks, arrests in major incidents
    r"\b(deadly|fatal|terror|terrorist|suicide|mass|church|mosque|synagogue|school)\b.{0,25}\b(attack|shooting|bombing|blast|explosion|stabbing|massacre)\b|"
    r"\b(shooting|bombing|blast|explosion|gunmen)\b.{0,30}\b(kills?|killed|dead|deaths)\b|"
    r"\bkill(s|ed)? (at least |more than |over )?(\d{2,}|dozens|scores|hundreds)\b|\b(\d{2,}|dozens|scores|hundreds) (people |civilians )?(killed|dead)\b|"
    r"\bhostages?\b|\bshootout\b|\bgunmen\b|"
    r"\barrest(s|ed)?\b.{0,50}\b(attack|bombing|shooting|assassination|plot|massacre|explosion)\b|\bsuspects?\b.{0,40}\b(attack|bombing|shooting)\b|"
    # crisis diplomacy
    r"\b(officials|security chiefs|chiefs|ministers|delegations?|envoys?|leaders|mediators|negotiators)\b.{0,50}\b(meet|meets|met|visit|visits|arrive|arrives|hold talks|holds talks|for talks|in talks)\b|"
    r"\bdelegations? meet\b|\bcrisis talks\b|\bpeace talks\b|\bemergency (summit|session|meeting|talks)\b|\brenewed fighting\b|"
    r"\bexpels?\b.{0,25}\b(ambassador|diplomats?|envoy)\b|\bsever(s|ed)?\b.{0,20}\bties\b|\brecalls?\b.{0,10}\bambassador\b|"
    # disasters
    r"\boil (spill|slick)\b|\bspill\b.{0,25}\b(widens|spreads|grows)\b|\bwildfires?\b|\bfloods?\b.{0,25}\b(kill|dead|displace|evacuat)|\blandslide kills?\b|"
    r"\bplane crash\b|\btrain crash\b|\bderail\w*\b|\b(building|bridge|dam|mine)\b.{0,15}\bcollapse|\bbeach(es)? (clos|shut)\w*|\bshuts?\b.{0,20}\bbeaches\b|"
    r"\bmass evacuation\b|\bevacuat\w+ (thousands|residents|city|town)\b|\bstate of emergency\b|\bcyclone\b|\bhurricane\b|\btyphoon\b|\bearthquake\b|\bvolcan\w* erupt", re.I)
HARD_EVENT_WIRE_BONUS = 4    # extra when a hard event comes from a wire / top-desk source (Reuters, AP, BBC, Bloomberg, ...)
# Features, reactions and look-backs around a hard event: never the event itself
HARD_EVENT_FEATURE = re.compile(r"\bdescribes?\b|\brecounts?\b|\bfuneral\b|\bvigil\b|\bremember\w*\b|\btributes?\b|\binside\b|\bwhat (we|you) (know|need)|"
                                r"\bneed to know\b|\btimeline\b|\bin (maps|charts|pictures|photos)\b|\bphotos show\b", re.I)
# Megacap company capital actions (stock/equity/debt offerings, giant buybacks) count as a big event
MEGACAP_CAPITAL = re.compile(
    r"\$\s?\d+(\.\d+)?\s?(billion|bn)\b.{0,40}\b(stock|equity|share|debt|bond|convertible)s?\b.{0,12}\b(offering|sale|raise|issuance|buyback|repurchase)|"
    r"\b(stock|equity|share|debt|bond|convertible)s? (offering|sale|issuance|buyback|repurchase)\b.{0,40}\$\s?\d+(\.\d+)?\s?(billion|bn)|"
    r"\b(raise|raises|raising|seeks|plans|launches|announces)\b.{0,30}\$\s?\d+(\.\d+)?\s?(billion|bn)\b.{0,30}\b(in )?(equity|stock|debt|bonds?|financing|share sale)\b|"
    r"\b(public|proposed|registered|secondary|underwritten) offering\b|\boffering of\b.{0,40}\b(shares|stock|notes|depositary|preferred)\b", re.I)
# Big-company newsrooms / IR sites (GN publisher name or link host) and their wire-distributed releases.
MEGACAP_ISSUER = re.compile(r"^(meta( platforms)?|apple|microsoft|nvidia|amazon(\.com)?|alphabet|google|tesla|berkshire hathaway inc|broadcom|oracle|"
                            r"jpmorgan( chase)?|netflix|openai|anthropic|spacex|xai|visa|mastercard|walmart|exxon ?mobil|eli lilly|tsmc|samsung( electronics)?)\b(?! \w+ (specialty|reinsurance|energy|homeservices))", re.I)
MEGACAP_NEWSROOM_SRC = re.compile(r"^(meta( investor relations| newsroom)?|apple( newsroom)?|microsoft( news| investor relations)?|nvidia( newsroom)?|amazon( news)?|"
                                  r"about amazon|alphabet( investor relations)?|tesla( investor relations)?|investor\.atmeta\.com|nvidianews\.nvidia\.com)$", re.I)
PRESS_WIRES = re.compile(r"pr newswire|prnewswire|business wire|businesswire|globenewswire", re.I)
# Previews, analyst notes, market wraps and FX commentary: not breaking news (a decision or print is)
PREVIEW_JUNK = re.compile(
    r"\bexpected to\b|\bexpects?\b|\bforecasts?\b|\bpredicts?\b|\bahead of\b|\bbraces?\b|\bawaits?\b|\bawaiting\b|\blooms?\b|\bodds\b|"
    r"\bon the line\b|\bpre-?bell\b|\bpre-?market\b|\bstock market today\b|\bfutures (rise|fall|edge|climb|slip|dip|gain)|\bearnings call transcript\b|"
    r"\bmaybe\b|(?<!to )(?<!\bto )\bpreview\b(?!\s+(a|an|its|new|crypto|security|custody)\b)|\bwhat to watch\b|\blooks (beyond|ahead|to)\b|\bsoftens\b|\bpoises\b|\bhovers\b|\bdrifts\b|\bsteadies\b|\bfirms\b|"
    r"\banalysts? (divided|say|see|warn)\b|\bwill (surge|soar|jump|rally|rise|climb|fall|drop|plunge|crash|double|triple)\b|\b(says|according to|per) (morgan stanley|goldman( sachs)?|jpmorgan|bofa|bank of america|citi(group)?|ubs|barclays|bernstein|jefferies|wedbush|analysts?)\s*$|\binvestors weigh\b|\bweighs? (in|up)\b|\breverses? (losses|gains)\b|\baccording to (one|an?) (wall street )?analyst\b|\bto buy (before|now)\b|"
    r"\b(cryptocurrenc(y|ies)|coins?|tokens?|altcoins?|stocks?|shares?)\b.{0,15}\bto buy\b|\bdays away\b|,\s*more$|\bop-?ed\b|^i[’']?m\b|^i\b|^my\b|\bin (maps|charts|pictures)\b|[-–—]\s*(business|markets?|politics|world|news) live\s*$|\blive blog\b|\blatest (news|company updates|updates)\b|\byour questions,? answered\b", re.I)
PREVIEW_OVERRIDE = re.compile(r"\b(crash\w*|plunge\w*|tumble\w*|record high|all[- ]time high|halt\w*|circuit breaker|emergency)\b", re.I)
ANALYST_NOTE = re.compile(r"^(goldman( sachs)?|morgan stanley|jpmorgan|j\.p\. morgan|bofa|bank of america|citi(group)?|ubs|barclays|hsbc|wells fargo|"
                          r"bernstein|jefferies|deutsche bank|nomura|bnp paribas|societe generale|rbc|td|capital economics|oxford economics)\b.{0,50}"
                          r"\b(expects?|forecasts?|sees|predicts?|upgrades?|downgrades?|price target|target|recommends?|overweight|underweight|strategists?|economists?)\b", re.I)
# Non-megacap quarterly results are not breaking news for us
SMALL_EARNINGS = re.compile(r"\b(q[1-4]|quarterly|first-quarter|second-quarter|third-quarter|fourth-quarter|half-year|annual)\b.{0,25}\b(profit|revenue|earnings|net income|loss)\b|"
                            r"\b(profit|net income|revenue)\b.{0,10}\b(falls|rises|jumps|drops|slumps|surges|climbs|declines)\b", re.I)
ENT_WEIGHT = {"crypto": 4, "macro": 4, "markets": 3, "bigco": 3, "people": 3, "politics": 3, "crisis": 3, "world": 2}

NEWS_EVENT = re.compile(
    r"\bannounc|\bsigns?\b|\bsigned\b|executive order|\bimpos|\bsanction|\bbans?\b|\bbanned\b|"
    r"\bapprov|\brejects?\b|\blaunch(es|ed)?\b|\bunveil|\bcuts?\b|\bhikes?\b|\braises?\b|\bholds?\b(?= rates| interest)|"
    r"record high|all[- ]time high|\brecord\b|\bcrash|\bplung|\btumbl|\bsoar|\bsurg|\bjumps?\b|\bdump|\bsinks?\b|\bslump|"
    r"wiped|\bhalt|circuit breaker|\bbankrupt|\bdefault|\bacquir|\bmerger\b|\bbuys?\b|\bbought\b|\bsells?\b|\bsold\b|"
    r"\bipo\b|\bearnings\b|\brevenue\b|\bbeats?\b|\bmiss(es)?\b|\blayoffs?\b|\bcuts? \d[\d,]* jobs|\bresign|\bsteps down\b|"
    r"\bfired\b|\barrest|\bindict|\bcharged\b|\bsues?\b|\bsued\b|\blawsuit\b|\bruling\b|\brules\b|\bfined?\b|\bfines\b|"
    r"\bceasefire\b|\battacks?\b|\bstrikes?\b|\bmissiles?\b|\binvad|\bemergency\b|\boutage\b|\bhack|\bexploit|"
    r"\bliquidat|\bunlock|\breclaims?\b|\bfalls? (below|under)\b|\bdrops? (below|under)\b|\btops?\b|\bcrosses\b|\bhits?\b|"
    r"\bwarns?\b|\bthreatens?\b|\bconfirms?\b|\bofficially\b|\bpasses\b|\bpassed\b|\bvotes?\b|\bdeal\b|\bagreement\b|"
    r"\bpartners?\b|\bpartnership\b|\binvests?\b|\bdelist|\blists\b|\bfreez|\bseiz|\bdies\b|\bdead\b|\bkilled\b|"
    r"\bshutdown\b|\bexpected\b|\bexpectations\b|\breports?\b|\bstatement\b|\bminutes\b|\bdecision\b|"
    r"\burges?\b|\bstockpil|\borders?\b|\bto pay\b|\brolls? out\b|"
    # WG-speed audit 2026-10-28: "Supreme Court recognizes Bitcoin as legal property", "offers buyouts to 60,000", "Google to preview ... system"
    r"\brecogni[sz](es|ed)\b|\blegal (tender|property)\b|\blegali[sz](es|ed)\b|\bbuyouts?\b|\bto preview\b|\bpreviews\b(?= (a|an|its|new)\b)|"
    # 2026-10-26 world-politics events
    r"\bconcedes?\b|\bconceded\b|\bdefeat(s|ed)?\b|\bwins?\b|\bwon\b|\bre-?elected\b|\belected\b|\bsworn in\b|"
    r"\bdied\b|\bpasses away\b|\bimpeach|\bousted\b|\btopples?\b|\boverthrow|\bcoup\b|\bassassinat|"
    r"\binvasion\b|\bearthquake\b|\btsunami\b|\bresults?\b|\blandslide\b|\bmajority\b|\bdetained\b|"
    r"\bdeclares?\b|\bdissolves?\b|\bcalls? (snap|early) election", re.I)
NEWS_BIG = re.compile(
    r"\btrump\b.{0,50}\b(signs|announces|imposes|executive order|tariffs?|fires|pardons)|"
    r"\b(fed|federal reserve|fomc|ecb|bank of (japan|england|canada|korea)|rbnz|rba|reserve bank|snb|riksbank|central bank|pboc|rbi|boj|boe|boc)\b.{0,40}\b(cuts|raises|hikes|holds|leaves|lowers|keeps|delivers)\b|"
    r"\b(surprise|unexpected|emergency|delivers?|announces?|approves?)\b.{0,12}\brate (cut|hike)|emergency (rate|meeting)|"
    r"\b(cpi|inflation|ppi|gdp|payrolls|jobs report|unemployment( rate)?|jobless claims|consumer price index|producer price index|retail sales)\b.{0,50}"
    r"(\d|than expected|expectations|rises|falls|jumps|slows|accelerat)|"
    r"wiped out|circuit breaker|record high|all[- ]time high|bear market|\bbankrupt|"
    r"\b(stock market|s&p 500|nasdaq|dow|stocks|gold|silver|oil)\b.{0,40}\b(crash|plunge|tumble|surge|soar|record|dump|sinks?)|"
    r"\b(declares?|launch(es)?|begins?|enters?) war\b|\bwar (begins|breaks out)\b|\binvasion\b|"
    r"\bceasefire\b.{0,20}\b(agreed|reached|deal|signed|collapses|ends)\b|\bmissile strikes?\b|\bnuclear (test|strike|attack)\b|"
    r"\b(earnings|revenue|eps)\b.{0,40}\b(beat|beats|beating|miss|misses|tops)|"
    r"\b(acquire|buy|buys|merger|takeover)\b.{0,40}\$\s?\d+(\.\d+)?\s?(billion|bn|trillion)|"
    r"\b(bitcoin|btc|ether|eth|solana|xrp)\b.{0,40}\b(etf|reserve|treasury|buys|bought|approv|falls (below|under)|reclaims|tops)|"
    r"\b(hack|exploit|drain)\w*\b.{0,40}\$\s?\d|\$\s?\d[\d.,]*\s?(million|billion|m|bn)\b.{0,40}\b(liquidat|hack|stolen|drained)|"
    r"\b(sec|cftc|congress|senate|white house|treasury)\b.{0,60}\b(crypto|stablecoin|bitcoin|digital asset)|"
    r"\b(arrest|indict|charged)\w*\b.{0,40}\b(ceo|founder|president|minister)|"
    r"\b(ceo|founder|chairman)\b.{0,30}\b(resigns|steps down|fired|ousted)|"
    r"\b(buys|bought|acquires|acquired|adds|added)\b.{0,12}[\d,.]+\s?(k\s)?(bitcoin|btc|ether|eth|sol|xrp)\b|"
    r"\b(visa|mastercard|jpmorgan|blackrock|schwab|paypal|fidelity|goldman|citi|bank)\b.{0,40}\b(launch\w*|rolls? out)\b.{0,40}\b(crypto|stablecoins?|bitcoin|tokeni\w*)|"
    r"\b(surges?|soars?|plunges?|crash(es)?|tumbles?|jumps?|sinks?|drops?|falls?)\b.{0,15}\b(1\d|[2-9]\d)(\.\d+)?%|"
    r"\b(judge|court)\b.{0,30}\b(orders|rules|blocks)\b|"
    r"\bofficially\b|\bjust in\b|\bbreaking\b|"
    # 2026-10-26 world-politics big events
    r"\b(wins?|won|concedes?|conceded|loses|lost|claims victory in)\b.{0,40}\b(elections?|vote|presidency|presidential|runoff|referendum|term|majority|landslide)\b|"
    r"\b(elections?|vote|runoff|presidency)\b.{0,40}\b(won|wins|concedes?|conceded|defeat|victory|results?)\b|"
    r"\b(wins?|won|secures?|sworn in for|begins?|starts?) (a )?(second|third|fourth|fifth|sixth|seventh|eighth|new|another) (term|mandate)\b|"
    r"\b(elected|re-?elected|sworn in)\b.{0,30}\b(president|prime minister|pm|chancellor|leader|pope)\b|"
    r"\b(president|prime minister|pm|chancellor|king|queen|pope|leader|premier|minister)\b.{0,40}\b(dies|died|dead|resigns|resigned|steps down|ousted|impeached|assassinated|arrested|detained|killed)\b|"
    r"\b(coup|assassinat\w*|impeach\w*|martial law|state of emergency)\b|"
    r"\b(magnitude|m)\s?[6-9](\.\d)?\b.{0,30}\bearthquake\b|\bearthquake\b.{0,40}\b[6-9]\.\d\b|\btsunami (warning|alert|hits)\b|"
    r"\binvasion of\b|\binvades?\b", re.I)
NEWS_FLUFF = re.compile(
    r"\bopinion\b|op-ed|\banalysis\b|\bcommentary\b|\bcolumn\b|\bexplainer\b|\bexplained\b|what to know|"
    r"what we know|\btakeaways?\b|\bkey moments\b|\bwhat happened\b|\bhow to\b|\bhow (the|a|an|this|it)\b|^why\b|\bwhy (the|a|an|this|you|it|investors|stocks)\b|"
    r"^live news\b|^the (morning|evening|daily|weekly) \w+( \w+)? (report|brief|briefing|download|wrap)\b|"  # latency pass: live blogs/newsletters never urgent
    r"\bpodcast\b|\bnewsletter\b|^watch:|^video:|^live:|\blive updates?\b|\blive:|\bas it happened\b|week ahead|"
    r"\bmorning (brief|briefing|bid)\b|\bevening briefing\b|\bstocks? to (buy|watch)\b|\bbest stocks\b|"
    r"\bstocks making the biggest moves\b|\bthings to know\b|\b\d+ (things|ways|reasons|stocks)\b|\btop \d+\b|"
    r"\bpersonal finance\b|\bretirement\b|\bmortgage rates? today\b|\bsavings? rates?\b|\bcd rates?\b|"
    r"\bquiz\b|\bcrossword\b|\breview\b|\bhands[- ]on\b|\binterview\b|\bobituary\b|\bphotos?:|\bin pictures\b|"
    r"\bsport|\bfootball\b|\bsoccer\b|\bnba\b|\bnfl\b|\bcricket\b|\btennis\b|\bmovie\b|\bcelebrity\b|"
    r"\brecipe\b|\btravel\b|\bhoroscope\b|\bwordle\b|\bdeals? of the day\b|% off|\bcoupon\b|"
    r"\bforecast\b|\bprice prediction\b|\boutlook\b|\bmocks?\b|\bslams?\b|\bblasts?\b|\brips\b|\bfears?\b|"
    r"\bcould\b|\bmight\b|\bmay\b(?! \d)|\bpay off\b|\bwin for\b|\bheres?\b|\bhere's\b|\bsees\b|\bbets?\b", re.I)
# Google News: only queue items from publishers we can verify against
TRUSTED_PUBLISHERS = re.compile(
    r"^(reuters|associated press|ap news|bloomberg|cnbc|the wall street journal|wsj|financial times|ft\.com|"
    r"bbc|the new york times|nytimes|the washington post|axios|politico|the guardian|cnn|fox business|"
    r"marketwatch|barron's|yahoo finance|fortune|business insider|the hill|nbc news|cbs news|abc news|"
    r"al jazeera|nikkei asia|south china morning post|forbes|semafor|the economist|bloomberg law|"
    r"coindesk|the block|decrypt|cointelegraph|dl news|bitcoin magazine|blockworks|the defiant|"
    r"techcrunch|the verge|wired|ars technica|the information|engadget|cnet|zdnet|"
    r"u\.s\. department of the treasury|treasury|bureau of labor statistics|bls|federal reserve|"
    r"sec\.gov|ripple|the white house|whitehouse\.gov|anthropic|openai|x\.ai|google|venturebeat|"
    r"crypto briefing|cryptoslate|the cryptonomist|tipranks|"
    r"investopedia|kiplinger|morningstar|investing\.com|fxstreet|kitco|deutsche welle|dw|france 24|"
    r"times of india|the times|the telegraph|sky news|euronews|cbc|globe and mail|abc \(australia\))", re.I)
OFFICIAL_RELEVANT = {
    "Federal Reserve": re.compile(r"fomc|monetary policy|federal funds|discount rate|statement|minutes|"
                                  r"crypto|stablecoin|digital asset|enforcement action|stress test|chair", re.I),
    "SEC press releases": re.compile(r"crypto|bitcoin|ether|stablecoin|digital asset|token|etf|charges|"
                                     r"chairman|atkins|fraud|ponzi", re.I),
    "White House": re.compile(r"executive order|proclamation|tariff|sanction|fact sheet|announces|signs|"
                              r"trade|deal|agreement|crypto|digital asset|ai\b|artificial intelligence|"
                              r"economy|tax|energy|oil|china|russia|ukraine|israel|iran", re.I),
    "Federal Reserve monetary": re.compile(r"fomc|monetary policy|federal funds|discount rate|statement|minutes|emergency|facility", re.I),
    "White House actions": re.compile(r"executive order|proclamation|tariff|sanction|duties|import|export|emergency|crypto|digital asset|"
                                      r"artificial intelligence|china|russia|iran|canada|mexico|trade", re.I),
    "ECB press": re.compile(r"monetary policy|interest rates?|decision|statement|press conference|lagarde|digital euro|emergency", re.I),
    "Bank of England news": re.compile(r"bank rate|monetary policy|interest rate|financial stability|governor|bailey|stablecoin|emergency", re.I),
    "Bank of Japan": re.compile(r"monetary policy|statement|policy rate|outlook for economic activity|ueda|governor|intervention", re.I),
    "Bank of Canada press": re.compile(r"policy rate|interest rate|monetary policy|governor|macklem|emergency", re.I),
    "BEA releases": re.compile(r"gross domestic product|\bgdp\b|personal income|outlays|\bpce\b|trade in goods", re.I),
    "GN: bls.gov": re.compile(r"consumer price index|producer price index|employment situation|"
                              r"jobs|payroll|unemployment|cpi|ppi|inflation", re.I),
    "GN: treasury.gov": re.compile(r"sanction|tariff|debt|refunding|buyback|stablecoin|genius|crypto|"
                                   r"secretary|bessent|ofac", re.I),
}
# Official headline releases: a matching title is lifted to at least this score (it often lacks our entity words,
# e.g. ECB "Monetary policy decisions") so a rate decision or a GDP/PCE print is never gated out.
OFFICIAL_MAJOR = {
    "Federal Reserve": (re.compile(r"fomc statement|issues fomc|discount rate|emergency|federal funds|monetary policy (statement|action)", re.I), 13),
    "Federal Reserve monetary": (re.compile(r"fomc statement|issues fomc|discount rate|emergency|federal funds|monetary policy (statement|action)", re.I), 13),
    "ECB press": (re.compile(r"monetary policy decisions?|emergency", re.I), 13),
    "Bank of England news": (re.compile(r"bank rate|monetary policy summary|emergency", re.I), 13),
    "Bank of Japan": (re.compile(r"statement on monetary policy|monetary policy decision|change in (the )?(guideline|policy)", re.I), 13),
    "Bank of Canada press": (re.compile(r"(maintains|holds|lowers|raises|cuts|increases|reduces) (its )?(target for the overnight rate|policy rate)", re.I), 13),
    "BEA releases": (re.compile(r"gross domestic product|\bgdp\b|personal income and outlays", re.I), 12),
    "White House actions": (re.compile(r"executive order|proclamation|tariff|sanction", re.I), 11),
}

TOPIC_ORDER = [("Crypto", "crypto"), ("Macro", "macro"), ("Markets", "markets"),
               ("Tech", "bigco"), ("World", "politics"), ("World", "crisis"), ("World", "world"), ("World", "people")]


def _clean_title(name: str, it: dict) -> str:
    title, gsrc = it["title"], it.get("src", "")
    if name.startswith("GN:"):
        if gsrc and title.endswith(" - " + gsrc):
            title = title[: -(len(gsrc) + 3)]
        else:
            title = re.sub(r"\s+-\s+[^-]{2,40}$", "", title)
    if name == "Yonhap":   # strip wire version tags so (URGENT)/(LEAD)/(2nd LD) copies dedupe; mark features as opinion
        title = re.sub(r"^\((EDITORIAL|News Focus|Yonhap Feature|Yonhap Interview|Feature|Interview|Column)\)\s*", "Opinion: ", title, flags=re.I)
        title = re.sub(r"^\((URGENT|LEAD|\d+(st|nd|rd|th) LD|Yonhap Exclusive)\)\s*", "", title, flags=re.I)
        title = re.sub(r"\bN\. Korea", "North Korea", re.sub(r"\bS\. Korea", "South Korea", title))
    return title.strip()


def score_news(name: str, tier: str, it: dict, feed_topic: str, event_hint: bool = False) -> tuple[int, list[str], str, str]:
    """General breaking-news scoring (markets, macro, crypto, big tech, world). Returns score, why, title, topic."""
    title = _clean_title(name, it)
    gsrc = it.get("src", "")
    summary = it.get("summary", "")[:300]
    why = []
    # scoring pass 2026-10-26: a megacap's own release (IR site/newsroom, or a PR/Business Wire release whose
    # issuer is the megacap itself) is an official source, not "press release" noise
    try:
        _host = urllib.parse.urlsplit(it.get("link", "")).netloc.lower()
    except Exception:
        _host = ""
    company_release = bool(MEGACAP_NEWSROOM_SRC.search(gsrc.strip())
                           or re.search(r"(^|\.)(investor\.atmeta\.com|about\.fb\.com|nvidianews\.nvidia\.com|news\.microsoft\.com|ir\.tesla\.com|abc\.xyz)$", _host)
                           or (MEGACAP_ISSUER.search(title) and PRESS_WIRES.search(f"{gsrc} {_host}")))
    if company_release:
        tier = "official"; why.append("company-release")
    elif SPAM.search(f"{title} {gsrc} {it['link']}") or SPAM.search(summary[:200]) or AI_SPAM.search(f"{title} {gsrc}"):
        return -99, ["spam/sponsored/low-trust"], title, ""
    if (PREVIEW_JUNK.search(title) and not PREVIEW_OVERRIDE.search(title)) or ANALYST_NOTE.search(title):
        return -40, ["preview/analyst-note/market-wrap"], title, ""
    if HARD_EVENT.search(title) and HARD_EVENT_FEATURE.search(title):
        return -40, ["feature/reaction piece"], title, ""
    if NEWS_FLUFF.search(title):
        return -50, ["fluff/opinion/explainer/live-blog"], title, ""
    if CLICKBAIT.search(title) and not NEWS_BIG.search(title) and not MEGACAP_CAPITAL.search(title):
        return -50, ["clickbait/prediction"], title, ""
    if QUESTION_BAIT.search(title):
        return -20, ["question headline"], title, ""
    if name.startswith("GN:") and gsrc and not company_release and not TRUSTED_PUBLISHERS.search(gsrc.strip()):
        return -30, ["untrusted publisher:" + gsrc[:30]], title, ""
    if name in OFFICIAL_RELEVANT and not OFFICIAL_RELEVANT[name].search(f"{title} {summary}"):
        return -10, ["routine official release"], title, ""
    groups = {g: (m.group(0) if (m := rx.search(title)) else None) for g, rx in ENT.items()}
    groups = {g: m for g, m in groups.items() if m}
    if feed_topic == "Crypto" and "crypto" not in groups and CRYPTO_TERM.search(summary):
        groups["crypto"] = "(feed)"
    s = 0
    for i, g in enumerate(sorted(groups, key=lambda g: -ENT_WEIGHT[g])[:2]):
        s += ENT_WEIGHT[g] if i == 0 else ENT_WEIGHT[g] // 2
    if groups:
        why.append("ent:" + ",".join(f"{g}={str(m).lower()[:14]}" for g, m in groups.items()))
    ev = {m.group(0).lower() for m in NEWS_EVENT.finditer(title)}
    if ev:
        s += min(2, len(ev)) * 2; why.append("event:" + ",".join(sorted(ev))[:50])
    big = NEWS_BIG.search(title)
    if not big and "bigco" in groups:
        big = MEGACAP_CAPITAL.search(title)
    if big:
        s += 4; why.append("big:" + big.group(0).lower()[:30])
    hard = HARD_EVENT.search(title)
    if hard:
        s += 3; why.append("hard:" + hard.group(0).lower()[:30])
        if _wire_grade(name, it):
            s += HARD_EVENT_WIRE_BONUS; why.append("wire")
    for m in MONEY.finditer(title):
        v = float(m.group(1)) * (1000 if m.group(2).lower()[0] == "b" else 1)
        if v >= 1000:
            s += 2; why.append(f"${v/1000:.0f}B"); break
    if re.search(r"\$\s?\d[\d.,]*\s?trillion|\btrillion\b", title, re.I):
        s += 2; why.append("$T")
    if re.search(r"\d+(\.\d+)?%", title):
        s += 1; why.append("pct")
    s += {"official": 3, "reputable": 1}.get(tier, -1)
    # hard gates: needs a named entity AND an event
    if not groups:
        s = min(s, NEWS_HIT_THRESHOLD - 1); why.append("gate:no-entity")
    if SMALL_EARNINGS.search(title) and "bigco" not in groups:
        s = min(s, NEWS_HIT_THRESHOLD - 1); why.append("gate:non-megacap-earnings")
    if not (ev or big or hard or event_hint):       # event_hint: caller (FinancialJuice) already matched a major event
        s = min(s, NEWS_HIT_THRESHOLD - 1); why.append("gate:no-event")
    topic = next((t for t, g in TOPIC_ORDER if g in groups), feed_topic or "World")
    if feed_topic == "Crypto" and "crypto" in groups:
        topic = "Crypto"
    return s, why, title, topic


def _num(x: str) -> float | None:
    try:
        return float(x.replace(",", ""))
    except Exception:
        return None


def score_fj(name: str, tier: str, it: dict) -> tuple[int, list[str], str, str]:
    """FinancialJuice: strict. Drop routine squawk; pass only FJ_MAJOR events / big data surprises, score >= FJ_MIN_SCORE."""
    title = FJ_PREFIX.sub("", it["title"]).strip().rstrip(".")
    why = ["fj-lead"]
    m = FJ_DATA.search(title)
    if m:                                    # economic data print
        ind = m.group("ind").strip()
        rule = next((d for rx, d in FJ_DATA_MAJOR if rx.search(ind)), None)
        if rule is None:
            return -40, why + ["fj:minor data print"], title, ""
        a, f = _num(m.group("a")), _num(m.group("f"))
        if rule > 0 and (a is None or f is None or abs(a - f) < rule):
            return -30, why + [f"fj:in-line print ({ind})"], title, ""
        return FJ_MIN_SCORE + 1, why + [f"fj:major data {ind} actual {m.group('a')} vs {m.group('f')}", "big:data-surprise"], title, "Macro"
    core = FJ_ATTRIB_TAIL.sub("", title)     # drop trailing attribution (": governor", "- RIA", ": Nikkei")
    body = core
    sp = FJ_SPEAKER.match(core)
    if sp:
        body = core.split(":", 1)[1].strip()
    if FJ_NOISE.search(title):
        return -40, why + ["fj:routine/noise"], title, ""
    major = FJ_MAJOR.search(body) or FJ_HEADLINE.search(core)
    if not major:
        return -20, why + (["fj:speaker soundbite"] if sp else ["fj:not headline-worthy"]), title, ""
    sc, w2, clean, topic = score_news(name, tier, {**it, "title": core, "src": ""}, "", event_hint=True)
    if sc < -5:                               # spam/fluff/clickbait gates from the main scorer still apply
        return sc, why + w2, title, ""
    if not any(x.startswith("big:") for x in w2):
        sc += 4                               # the FJ_MAJOR match counts as a NEWS_BIG event
    sc += 3                                   # FJ_MAJOR bonus (aggregator tier already -1)
    if FJ_TOP.search(body) and sc < FJ_MIN_SCORE:
        sc = FJ_MIN_SCORE; why.append("fj-top-floor")
    elif FJ_HEADLINE.search(core) and sc < FJ_MIN_SCORE:
        sc = FJ_MIN_SCORE; why.append("fj-headline-floor")
    why += w2 + ["fj-major:" + major.group(0).lower()[:30]]
    return sc, why, title, topic or "World"


_NEWS_EVENT_STEMS = [("rates", r"rate|fomc|cuts|hikes|holds"), ("data", r"cpi|ppi|inflation|payroll|jobs|gdp|unemploy"),
                     ("tariff", r"tariff|sanction|trade"), ("earnings", r"earnings|revenue|eps|quarter|q[1-4]"),
                     ("move-up", r"surg|soar|jump|record|high|reclaim|tops|rall"),
                     ("move-down", r"crash|plung|tumbl|dump|sink|slump|wiped|falls|drops|below|under"),
                     ("deal", r"acqui|merger|buy|bought|deal|agreement|partner|invest"),
                     ("legal", r"sue|lawsuit|charged|indict|arrest|fine|ruling|court|probe"),
                     ("policy", r"executive order|signs|bill|law|approv|ban|vote|pass"),
                     ("conflict", r"war|attack|strike|missile|ceasefire|invad"),
                     ("hack", r"hack|exploit|drain|stolen|breach|outage"), ("people", r"resign|steps down|fired|ousted"),
                     ("election", r"elect|concede|wins?\b|won\b|runoff|sworn in|term\b|vote"),
                     ("death", r"dies|died|dead|killed|passes away|assassinat"), ("disaster", r"earthquake|tsunami|hurricane|typhoon")]


def story_key(title: str) -> str | None:
    """entity+event(+first number) key, so several outlets on one story within 3h give one hit."""
    ents = []
    for g in ("people", "politics", "bigco", "crypto", "macro", "markets", "crisis", "world"):
        m = ENT[g].search(title)
        if m:
            ents.append(m.group(0).lower().strip())
    if not ents:
        return None
    ev = next((n for n, rx in _NEWS_EVENT_STEMS if re.search(rx, title, re.I)), "other")
    num = re.search(r"\d[\d,.]*\s?(%|billion|million|trillion|bn)?", title)
    return "|".join(sorted(list(dict.fromkeys(ents))[:2])) + ":" + ev + (":" + num.group(0).replace(",", "").strip() if num else "")


def kind_for(why: list[str], topic: str) -> str:
    j = " ".join(why)
    if "ai-security" in j:
        return "ai-security"
    if "big-event" in j or "big:" in j:
        return "big-event"
    if topic == "AI":
        return "ai-top5-news" if "ai-top5" in j else "ai-news"
    return topic.lower() + "-news"


def score_ai(name: str, tier: str, it: dict) -> tuple[int, list[str], str]:
    """AI scoring. Needs an AI company/model name AND a strong-event word; threshold AI_HIT_THRESHOLD."""
    title = it["title"]
    gsrc = it.get("src", "")
    if name.startswith("GN:"):
        if gsrc and title.endswith(" - " + gsrc):
            title = title[: -(len(gsrc) + 3)]
        else:
            title = re.sub(r"\s+-\s+[^-]{2,40}$", "", title)
    summary = it.get("summary", "")[:300]
    text = f"{title} {summary}"
    why = []
    if (SPAM.search(f"{title} {gsrc} {it['link']}") or AI_SPAM.search(f"{title} {gsrc} {it['link']}")
            or SPAM.search(summary[:200])):
        return -99, ["spam/sponsored"], title
    if AI_CLICKBAIT.search(title) or CLICKBAIT.search(title):
        return -50, ["clickbait/explainer/review"], title
    if QUESTION_BAIT.search(title):
        return -20, ["question/bait headline"], title
    if name in AI_REQUIRED and not AI_TERM.search(text):
        return -10, ["non-AI item in general feed"], title
    s = 0
    top = [k for k, rx in AI_TOP5.items() if re.search(rx, title, re.I)]
    if not top and name in AI_OFFICIAL_TOP5:
        top = ["(official:" + name + ")"]
    other = AI_OTHER.search(title)
    generic = AI_SEC_GENERIC.search(title) if name in AI_SEC_FEEDS else None
    if top:
        s += 4; why.append("ai-top5:" + ",".join(top))
    elif other:
        s += 3; why.append("ai-other:" + other.group(0).lower()[:20])
    elif generic:
        s += 2; why.append("ai-generic:" + generic.group(0).lower()[:20])
    elif any(re.search(rx, summary, re.I) for rx in AI_TOP5.values()) or AI_OTHER.search(summary):
        s += 1; why.append("ai-name-body")
    ev = set(m.group(0).lower() for m in AI_EVENT.finditer(title))
    if ev:
        s += min(2, len(ev)) * 2; why.append("ai-event:" + ",".join(sorted(ev))[:60])
    mv = AI_MODEL_VER.search(title)
    if mv:
        s += 2; why.append("model:" + mv.group(0))
    big = AI_BIG_EVENT.search(title)
    if big:
        s += 4; why.append("ai-big-event")
    sec = AI_SEC_EVENT.search(title)
    if sec:
        s += 3; why.append("ai-security:" + sec.group(0).lower()[:20])
        sev = AI_SEC_SEVERE.search(title)
        if sev:
            s += 2; why.append("severe:" + sev.group(0).lower()[:20])
    for m in MONEY.finditer(title):
        v = float(m.group(1)) * (1000 if m.group(2).lower()[0] == "b" else 1)
        if v >= 1000 and re.search(r"rais|fund|valu|acqui|buy|deal|invest|ipo|fine|settle|contract", title, re.I):
            s += 3; why.append(f"${v/1000:.0f}B")
            break
    if tier == "official":
        s += 2; why.append("official")
    elif tier == "reputable":
        s += 1
    else:
        s -= 1
    # hard gates: must be about a named AI player AND be an event
    if not (top or other or (generic and sec)):
        s = min(s, AI_HIT_THRESHOLD - 1); why.append("gate:no-ai-name")
    if not (ev or mv or big or sec):
        s = min(s, AI_HIT_THRESHOLD - 1); why.append("gate:no-event")
    return s, why, title


AI_STORY_DEDUPE_S = 12 * 3600
_EVENT_STEMS = [("sue", r"\bsue|lawsuit|sued"), ("raise", r"rais|funding|valuation|valued|ipo"),
                ("acquire", r"acqui|buys|merger"), ("outage", r"outage|down"), ("launch", r"launch|releas|unveil|introduc|debut|announc"),
                ("ban", r"\bban|block|restrict"), ("regulate", r"regulat|fine|probe|investigat|antitrust|ai act|executive order"),
                ("exec", r"resign|steps down|fired|ousted|hires"),
                ("security", r"prompt[- ]injection|jailbr|flaw|vulnerab|exploit|leak|breach|malware|hijack|escape|"
                             r"deepfake|backdoor|steal|stole|compromis")]

def ai_story_key(why: list[str], title: str) -> str | None:
    """company+event(+model) key, so 8 outlets covering one story within 12h give one hit."""
    comp = [k for k, rx in AI_TOP5.items() if re.search(rx, title, re.I)]
    o = AI_OTHER.search(title)
    if not comp and o:
        comp = [o.group(0).lower().split()[0]]
    if not comp:
        g = AI_SEC_GENERIC.search(title)
        if g and AI_SEC_EVENT.search(title):
            comp = ["generic-" + g.group(0).lower().split()[-1]]
    if not comp:
        return None
    ev = next((n for n, rx in _EVENT_STEMS if re.search(rx, title, re.I)), "other")
    mv = AI_MODEL_VER.search(title)
    return f"{comp[0]}:{ev}" + (f":{mv.group(0).lower()}" if mv else "")

# ------------------------------------------------------------------ prices
CRYPTO_ASSETS = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "ripple"}
CRYPTO_NAMES = {"btc": "Bitcoin $BTC", "eth": "Ethereum $ETH", "sol": "Solana $SOL", "xrp": "$XRP"}
# Yahoo Finance chart API (free, no key; checked 2026-10-25). Futures trade ~23h on weekdays.
MARKET_ASSETS = {"gold": ("GC=F", "Gold"), "spx": ("ES=F", "S&P 500 futures"),
                 "ndx": ("NQ=F", "Nasdaq 100 futures"), "oil": ("CL=F", "WTI crude oil"),
                 # speed research 2026-10-27: Brazil's crash (B3 circuit breaker) and the KOSPI rally were posted 44 min / ~3 h late
                 "bvsp": ("^BVSP", "Brazil's Ibovespa"), "kospi": ("^KS11", "South Korea's KOSPI"),
                 "nikkei": ("^N225", "Japan's Nikkei 225"), "hsi": ("^HSI", "Hong Kong's Hang Seng"),
                 "usdbrl": ("BRL=X", "US dollar vs Brazilian real")}
NON_USD_ASSETS = {"bvsp", "kospi", "nikkei", "hsi", "usdbrl"}   # quoted in points / BRL, not $
# thresholds: (1h move %, move vs previous close %)
MOVE_PCT = {"btc": (3.0, 5.0), "eth": (3.0, 6.0), "sol": (3.0, 7.0), "xrp": (3.0, 7.0),
            "gold": (1.5, 2.5), "spx": (1.2, 2.0), "ndx": (1.5, 2.5), "oil": (3.0, 5.0),
            "bvsp": (3.0, 4.0), "kospi": (3.0, 3.5), "nikkei": (3.0, 3.5), "hsi": (3.0, 3.5), "usdbrl": (2.0, 3.0)}
# round-number levels Watcher Guru-style ("Bitcoin falls under $80,000", "$2,500 $ETH")
LEVEL_STEP = {"btc": 2500, "eth": 100, "sol": 10, "xrp": 0.10, "gold": 100}
CRYPTO_POLL_S = 60
MARKET_POLL_S = 120
COOLDOWN_1H_S = 90 * 60
COOLDOWN_DAY_S = 6 * 3600
COOLDOWN_LEVEL_S = 6 * 3600
LEVEL_HYST = 0.003                  # 0.3% band around a level to avoid flapping


def _fmt_price(p: float, sym: str = "") -> str:
    if sym in NON_USD_ASSETS:                       # indices / FX: points, no $ sign
        return f"{p:,.2f}" if p < 100 else f"{p:,.0f}"
    return f"${p:,.0f}" if p >= 100 else (f"${p:,.2f}" if p >= 1 else f"${p:,.4f}")


def _price_alerts(st: dict, cur: dict, daych: dict, topic: str, names: dict, urls: dict) -> list[dict]:
    t = now_ts()
    hist = st.setdefault("price_hist2", [])
    cd = st.setdefault("cooldowns", {})
    prev = next((h for h in reversed(hist) if any(k in h["p"] for k in cur)), None)
    hits = []

    def fire(key, cool, title, sym, pct=None, score=10):
        if t - cd.get(key, 0) < cool:
            return
        cd[key] = t
        hits.append({"kind": "price", "topic": topic, "title": title, "asset": sym, "pct": pct,
                     "url": urls[sym], "source": "CoinGecko" if topic == "Crypto" else "Yahoo Finance",
                     "score": score, "published_ts": t, "published_eat": eat(t), "age_min": 0})

    for sym, p in cur.items():
        m1, md = MOVE_PCT[sym]
        old = [h for h in hist if 45 * 60 <= t - h["t"] <= 75 * 60 and sym in h["p"]]
        if old:
            ref = min(old, key=lambda h: abs(t - h["t"] - 3600))["p"][sym]
            pct = (p / ref - 1) * 100
            if abs(pct) >= m1:
                d = "up" if pct > 0 else "down"
                fire(f"1h:{sym}:{d}", COOLDOWN_1H_S,
                     f"{names[sym]} {'surges' if pct > 0 else 'drops'} {abs(pct):.1f}% in the past hour to {_fmt_price(p, sym)}",
                     sym, round(pct, 2), 11)
        c = daych.get(sym)
        if c is not None and abs(c) >= md:
            d = "up" if c > 0 else "down"
            fire(f"day:{sym}:{d}", COOLDOWN_DAY_S,
                 f"{names[sym]} {'up' if c > 0 else 'down'} {abs(c):.1f}% {'in 24h' if topic == 'Crypto' else 'on the day'}, now {_fmt_price(p, sym)}",
                 sym, round(c, 2), 9)
        step = LEVEL_STEP.get(sym)
        if step and prev and sym in prev["p"]:
            q = prev["p"][sym]
            lo, hi = sorted((q, p))
            k = int(lo // step) + 1
            while k * step <= hi:
                lv = round(k * step, 6)
                up = q < lv and p >= lv * (1 + LEVEL_HYST)
                dn = q > lv and p <= lv * (1 - LEVEL_HYST)
                if up or dn:
                    word = "reclaims" if up else "falls under"
                    fire(f"lvl:{sym}:{lv}", COOLDOWN_LEVEL_S, f"{names[sym]} {word} {_fmt_price(lv)}", sym, score=9)
                k += 1
    return hits


# ---- speed research 2026-10-28: US national debt record (WatcherGuru posted $40.5T at 23:53 EAT Oct 27, we were
# 24 min behind). Official Treasury Fiscal Data API (public, no key). New Debt to the Penny rows land ~16:00-17:00 ET.
DEBT_POLL_S = 300
DEBT_API = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
            "?sort=-record_date&page%5Bsize%5D=1&fields=record_date,tot_pub_debt_out_amt")
DEBT_STEP = 0.1e12


def check_us_debt(st: dict) -> list[dict]:
    code, body, _ = http_get(DEBT_API)
    if code != 200:
        raise RuntimeError(f"Treasury API HTTP {code}")
    row = json.loads(body)["data"][0]
    day, amt = row["record_date"], float(row["tot_pub_debt_out_amt"])
    ds = st.setdefault("us_debt", {})
    if not ds:                                   # first run: baseline only
        ds.update(day=day, max=amt)
        return []
    if day == ds.get("day"):
        return []
    prev_max = ds.get("max", amt)
    ds["day"] = day
    hits = []
    if amt > prev_max:
        ds["max"] = amt
        if int(amt // DEBT_STEP) > int(prev_max // DEBT_STEP):
            lvl = int(amt // DEBT_STEP) * DEBT_STEP / 1e12
            t = now_ts()
            whole = int(amt // 1e12) > int(prev_max // 1e12)
            hits.append({"kind": "data", "topic": "Macro",
                         "title": f"US national debt hits new record high of ${amt/1e12:.2f} trillion (passes ${lvl:.1f}T), Treasury data for {day}",
                         "url": "https://fiscaldata.treasury.gov/datasets/debt-to-the-penny/debt-to-the-penny",
                         "source": "Treasury Debt to the Penny", "score": 13 if whole else 11, "priority": True,
                         "why": ["official", "record", f"debt:{amt:.0f}"],
                         "published_ts": t, "published_eat": eat(t), "age_min": 0, "detect_lag_s": 0, "instance": INSTANCE})
    return hits


# ---- speed research 2026-10-28: anthropic.com has no RSS; its sitemap.xml (robots: Allow /) lists every /news and
# /research post with lastmod. The S-1 post and the Mithril Medicine acquisition (inside /research/early-progress-
# drug-development, 23:26 EAT) only reached us via Google News 2.5 h later. New URLs -> og:title/description -> hit.
ANTHROPIC_POLL_S = 180
ANTHROPIC_SITEMAP = "https://www.anthropic.com/sitemap.xml"


def _og(html_s: str, key: str) -> str:
    m = (re.search(r'<meta[^>]+(?:property|name)="' + key + r'"[^>]+content="([^"]*)"', html_s)
         or re.search(r'<meta[^>]+content="([^"]*)"[^>]+(?:property|name)="' + key + '"', html_s))
    return html.unescape(m.group(1)).strip() if m else ""


def check_anthropic_site(st: dict) -> list[dict]:
    code, body, _ = http_get(ANTHROPIC_SITEMAP)
    if code != 200:
        raise RuntimeError(f"anthropic sitemap HTTP {code}")
    x = body.decode("utf-8", "replace")
    urls = {u: lm for u, lm in re.findall(r"<loc>(https://www\.anthropic\.com/(?:news|research)/[^<]+)</loc>\s*<lastmod>([^<]+)</lastmod>", x)}
    a = st.setdefault("anthropic_seen", {})
    if not a:                                    # first run: baseline only
        st["anthropic_seen"] = {u: 1 for u in urls}
        return []
    hits, t = [], now_ts()
    for u, lm in urls.items():
        if u in a:
            continue
        a[u] = 1
        pub = parse_date(lm) or t
        if t - pub > 6 * 3600:                   # old page newly added to the sitemap
            continue
        c2, b2, _ = http_get(u)
        page = b2.decode("utf-8", "replace") if c2 == 200 else ""
        title = _og(page, "og:title") or u.rsplit("/", 1)[-1].replace("-", " ")
        desc = _og(page, "og:description")
        if desc.startswith("Anthropic is an AI safety and research company"):
            desc = ""
        it = {"title": f"Anthropic: {title}", "src": "", "link": u, "summary": desc}
        sc, why, _ = score_ai("Anthropic site", "official", it)
        sc = max(sc, FAST_LANE_MIN_SCORE); why = list(why) + ["anthropic-official-new-post"]   # official, 1-3/day: fast lane
        hits.append({"kind": "official", "topic": "AI", "title": f"Anthropic: {title}" + (f" | {desc[:220]}" if desc else ""),
                     "url": u, "source": "anthropic.com (official)", "score": sc, "priority": sc >= FAST_LANE_MIN_SCORE,
                     "why": why, "published_ts": pub, "published_eat": eat(pub), "age_min": round((t - pub) / 60),
                     "detect_lag_s": round(t - pub), "instance": INSTANCE})
    return hits


def check_crypto_prices(st: dict) -> list[dict]:
    ids = ",".join(CRYPTO_ASSETS.values())
    code, body, _ = http_get(f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
                             f"&include_24hr_change=true")
    if code != 200:
        raise RuntimeError(f"CoinGecko HTTP {code}")
    data = json.loads(body)
    cur = {s: float(data[c]["usd"]) for s, c in CRYPTO_ASSETS.items() if c in data}
    ch = {s: float(data[c].get("usd_24h_change") or 0) for s, c in CRYPTO_ASSETS.items() if c in data}
    urls = {s: f"https://www.coingecko.com/en/coins/{c}" for s, c in CRYPTO_ASSETS.items()}
    hits = _price_alerts(st, cur, ch, "Crypto", CRYPTO_NAMES, urls)
    _record(st, cur, ch)
    return hits


def check_market_prices(st: dict) -> list[dict]:
    cur, ch, urls, names = {}, {}, {}, {}
    for sym, (tick, nm) in MARKET_ASSETS.items():
        q = urllib.parse.quote(tick)
        code, body, _ = http_get(f"https://query1.finance.yahoo.com/v8/finance/chart/{q}?interval=5m&range=1d")
        if code != 200:
            log.warning("Yahoo %s HTTP %s", tick, code)
            continue
        m = json.loads(body)["chart"]["result"][0]["meta"]
        p, pc, mt = m.get("regularMarketPrice"), m.get("chartPreviousClose") or m.get("previousClose"), m.get("regularMarketTime")
        if not p or not mt or now_ts() - mt > (40 if sym in NON_USD_ASSETS else 20) * 60:   # closed/stale (Asian index quotes lag ~20-30 min)
            continue
        cur[sym] = float(p)
        if pc:
            ch[sym] = (float(p) / float(pc) - 1) * 100
        urls[sym] = f"https://finance.yahoo.com/quote/{q}"
        names[sym] = nm
    if not cur:
        return []
    hits = _price_alerts(st, cur, ch, "Markets", names, urls)
    _record(st, cur, ch)
    return hits


def _record(st: dict, cur: dict, ch: dict):
    t = now_ts()
    hist = st.setdefault("price_hist2", [])
    hist.append({"t": t, "p": cur})
    st["price_hist2"] = [h for h in hist if t - h["t"] <= 3 * 3600]
    lp = st.get("last_prices")
    if not isinstance(lp, dict) or "t" in lp:          # old (AI-only era) format
        lp = st["last_prices"] = {}
    for s, p in cur.items():
        lp[s] = {"t": t, "eat": eat(t), "p": p, "ch": round(ch.get(s, 0), 2)}

# ------------------------------------------------------------------ escalation
# ------------------------------------------------------------------ escalation
def load_webhook_cfg() -> dict:
    cfg = {}
    if os.path.exists(WEBHOOK_ENV):
        with open(WEBHOOK_ENV) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                cfg[k.strip().removeprefix("export ").strip()] = v.strip().strip('"').strip("'")
    for k in ("HERALD_WEBHOOK_URL", "HERALD_WEBHOOK_KEY", "HERALD_WEBHOOK_AUTH_HEADER",
              "HERALD_WEBHOOK_AUTH_SCHEME"):
        if os.environ.get(k) and not cfg.get(k):
            cfg[k] = os.environ[k]
    return cfg

def post_webhook(cfg: dict, payload: dict) -> bool:
    url, key = cfg["HERALD_WEBHOOK_URL"], cfg.get("HERALD_WEBHOOK_KEY", "")
    mode = (cfg.get("HERALD_WEBHOOK_AUTH_HEADER") or "both").strip()
    scheme = cfg.get("HERALD_WEBHOOK_AUTH_SCHEME", "Bearer")
    headers = {"Content-Type": "application/json", "User-Agent": UA}
    if key:
        if mode.lower() == "both":
            headers["Authorization"] = f"{scheme} {key}".strip()
            headers["X-Webhook-Key"] = key
        elif mode.lower() == "authorization":
            headers["Authorization"] = f"{scheme} {key}".strip()
        else:
            headers[mode] = key
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            log.info("webhook POST -> %s", r.status)
            ok = 200 <= r.status < 300
            _WEBHOOK_LAST["ok" if ok else "fail"] = now_ts()
            return ok
    except urllib.error.HTTPError as e:
        log.warning("webhook HTTP %s: %s", e.code, e.read(300))
    except Exception as e:
        log.warning("webhook error: %s", e)
    _WEBHOOK_LAST["fail"] = now_ts()
    return False

# ------------------------------------------------------------------ box/GHA dedupe (WG speed pass 2026-10-28)
# Both copies used to escalate the same story (~half of all breaking runs were duplicates). Now the box copy
# publishes a small heartbeat (no secrets) to the orphan branch "heartbeat" of HB_REPO every HB_EVERY_S; the GHA
# copy stays on standby (polls, marks seen, sends nothing) while that heartbeat is fresh, and takes over at once
# when it goes stale, can't be read, or the box reports a failing webhook (fail-open).
HB_REPO = os.environ.get("HERALD_HB_REPO", "IamJhmpologoma/herald-watcher")
HB_EVERY_S = 90
HB_STALE_S = 300
_WEBHOOK_LAST = {"ok": 0.0, "fail": 0.0}
_HB_CACHE = {"t": 0.0, "alive": False, "why": ""}


def _hb_payload(st: dict) -> dict:
    t = now_ts()
    lc = st.get("last_cycle", {})
    ok = sum(1 for v in st.get("feed_health", {}).values() if v.get("ok"))
    return {"instance": INSTANCE, "t": round(t), "eat": eat(t), "last_cycle_t": round(lc.get("t", 0)),
            "feeds_ok": ok, "feeds": len(st.get("feed_health", {})),
            "escalations_today": st.get("escalations", {}).get("count", 0),
            "webhook_last_ok": round(_WEBHOOK_LAST["ok"]), "webhook_last_fail": round(_WEBHOOK_LAST["fail"])}


def _gh_api(args: list[str], body: dict | None = None) -> dict | None:
    import subprocess
    cmd = ["gh", "api", *args]
    if body is not None:
        cmd += ["--input", "-"]
    r = subprocess.run(cmd, input=json.dumps(body) if body is not None else None, capture_output=True,
                       text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout)[:200])
    return json.loads(r.stdout) if r.stdout.strip() else {}


def heartbeat_once(st: dict):
    """Box only: parentless commit with box.json, force-moved onto refs/heads/heartbeat (keeps the branch 1 commit)."""
    content = json.dumps(_hb_payload(st), indent=1)
    tree = _gh_api([f"repos/{HB_REPO}/git/trees", "-X", "POST"],
                   {"tree": [{"path": "box.json", "mode": "100644", "type": "blob", "content": content}]})
    com = _gh_api([f"repos/{HB_REPO}/git/commits", "-X", "POST"],
                  {"message": "box heartbeat " + eat(), "tree": tree["sha"], "parents": []})
    try:
        _gh_api([f"repos/{HB_REPO}/git/refs/heads/heartbeat", "-X", "PATCH"], {"sha": com["sha"], "force": True})
    except RuntimeError:
        _gh_api([f"repos/{HB_REPO}/git/refs", "-X", "POST"], {"ref": "refs/heads/heartbeat", "sha": com["sha"]})


def heartbeat_thread(st: dict):
    import threading

    def run():
        fails = 0
        while not _stop:
            lc_t = st.get("last_cycle", {}).get("t", 0)
            if now_ts() - lc_t < 120:            # only claim "alive" while the poll loop really is cycling
                try:
                    heartbeat_once(st)
                    if fails:
                        log.info("heartbeat ok again after %d failure(s)", fails)
                    fails = 0
                except Exception as e:
                    fails += 1
                    if fails in (1, 5) or fails % 40 == 0:
                        log.warning("heartbeat failed (%d): %s", fails, str(e)[:160])
            for _ in range(HB_EVERY_S):
                if _stop:
                    break
                time.sleep(1)
    threading.Thread(target=run, name="heartbeat", daemon=True).start()
    log.info("heartbeat -> %s@heartbeat every %ss (GHA copy stands by while fresh < %ss)", HB_REPO, HB_EVERY_S, HB_STALE_S)


def box_alive() -> tuple[bool, str]:
    """GHA copy: is the box watcher alive and escalating? Cached 60 s. Fail-open (False) on any read error."""
    t = now_ts()
    if t - _HB_CACHE["t"] < 60:
        return _HB_CACHE["alive"], _HB_CACHE["why"]
    alive, why = False, ""
    try:
        hdr = {"Accept": "application/vnd.github.raw+json", "User-Agent": UA, "Cache-Control": "no-cache"}
        tok = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if tok:
            hdr["Authorization"] = "Bearer " + tok
        url = f"https://api.github.com/repos/{HB_REPO}/contents/box.json?ref=heartbeat&_={int(t)}"
        with urllib.request.urlopen(urllib.request.Request(url, headers=hdr), timeout=10) as r:
            hb = json.loads(r.read())
        age = t - hb.get("t", 0)
        cyc = t - hb.get("last_cycle_t", 0)
        bad_hook = hb.get("webhook_last_fail", 0) > hb.get("webhook_last_ok", 0) and t - hb.get("webhook_last_fail", 0) < 900
        alive = age < HB_STALE_S and cyc < HB_STALE_S + 120 and not bad_hook
        why = f"heartbeat {round(age)}s old, box cycle {round(cyc)}s ago" + (", box webhook failing" if bad_hook else "")
    except Exception as e:
        why = f"heartbeat unreadable ({str(e)[:80]})"
    _HB_CACHE.update(t=t, alive=alive, why=why)
    return alive, why


def is_urgent(h: dict) -> bool:
    """Sent at once, on its own: wire/official fast-lane hits (priority) and any very strong non-lead hit."""
    sc = h.get("score", 0)
    if h.get("priority") and sc >= FAST_LANE_MIN_SCORE:
        return True
    return sc >= URGENT_ANY_SOURCE_MIN_SCORE and not h.get("lead_only") and h.get("kind") != "price"


def maybe_escalate(st: dict, dry: bool):
    """Latency pass 2026-10-26: urgent hits go out immediately on their own (30 s anti-burst floor);
    the rest are batched at most every ESC_MIN_GAP_S. The last ESC_RESERVE_FOR_URGENT of the daily cap
    are kept for urgent hits."""
    pend = st.setdefault("pending_escalation", [])
    t = now_ts()
    # breaking only: drop anything published more than MAX_AGE_MIN ago (it's still in the queue file)
    pend[:] = [h for h in pend if (t - h.get("published_ts", t)) / 60 <= MAX_AGE_MIN]
    if not pend:
        return
    if INSTANCE != "box" and not dry:
        alive, why = box_alive()
        st.setdefault("_standby", {})
        if alive:
            if not st["_standby"].get("on"):
                log.info("standby: box watcher alive (%s); not escalating", why)
            st["_standby"] = {"on": True, "dropped": st["_standby"].get("dropped", 0) + len(pend)}
            log.info("standby: dropped %d hit(s) the box copy will handle", len(pend))
            st["pending_escalation"] = []
            return
        if st["_standby"].get("on"):
            log.info("TAKEOVER: %s; escalating from this copy", why)
        st["_standby"] = {"on": False, "dropped": st["_standby"].get("dropped", 0)}
    cfg = load_webhook_cfg()
    if not cfg.get("HERALD_WEBHOOK_URL"):
        if not st.get("_warned_no_webhook_day") == dt.date.today().isoformat():
            log.info("escalation disabled (no HERALD_WEBHOOK_URL in %s or env); %d hit(s) queued only",
                     WEBHOOK_ENV, len(pend))
            st["_warned_no_webhook_day"] = dt.date.today().isoformat()
        st["pending_escalation"] = []
        return
    day = dt.datetime.now(TZ).date().isoformat()
    esc = st.setdefault("escalations", {"day": day, "count": 0, "last": 0})
    if esc.get("day") != day:
        esc.update(day=day, count=0)
    if esc["count"] >= ESC_MAX_PER_DAY:
        return
    reserve_only = esc["count"] >= ESC_MAX_PER_DAY - ESC_RESERVE_FOR_URGENT
    gap = t - esc.get("last", 0)
    batch_gap = t - esc.get("last_batch", esc.get("last", 0))
    rank = lambda h: (not h.get("priority"), -h.get("score", 0), -h.get("published_ts", 0))
    urgent = sorted([h for h in pend if is_urgent(h)], key=rank)
    fast_lane = False
    if urgent and gap >= FAST_LANE_MIN_GAP_S:
        send, fast_lane = urgent[:URGENT_MAX_PER_WEBHOOK], True
    elif batch_gap >= ESC_MIN_GAP_S and gap >= FAST_LANE_MIN_GAP_S and not reserve_only:
        # priority hits first, then by score
        send, n_ai = [], 0
        for h in sorted(pend, key=rank):
            if h.get("topic") == "AI":
                if n_ai >= AI_MAX_HITS_PER_POST:
                    continue
                n_ai += 1
            send.append(h)
            if len(send) >= MAX_HITS_PER_WEBHOOK:
                break
    else:
        return
    for h in send:
        if h.get("detected_ts"):
            h["dispatch_lag_s"] = round(t - h["detected_ts"])
    payload = {
        "event": "herald_breaking_news",
        "account": "@JhMpologoma",
        "ts_eat": eat(t),
        "count": len(send),
        "priority": any(h.get("priority") for h in send),
        "fast_lane": fast_lane,
        "urgent": fast_lane,
        "instance": INSTANCE,
        "topics": sorted({h.get("topic", "?") for h in send}),
        "hits": send,
        "queue_file": QUEUE_PATH,
        "mode": "breaking-news (markets, macro, crypto, AI, tech, world)",
        "instructions": ("URGENT: post this first if it verifies; speed matters. " if fast_lane else "") +
                        "Verify each hit at the official or a reputable source before posting (never post from "
                        "another account's post alone). Write in our own words, Watcher-Guru-style 'JUST IN:' format "
                        "per ../watcher-guru-style.md. Mark handled in breaking-queue.jsonl.",
    }
    if dry:
        log.info("[dry-run] would escalate %d hit(s) (topics %s)%s", len(send), payload["topics"], " FAST-LANE" if fast_lane else "")
        sent = {id(h) for h in send}
        st["pending_escalation"] = [h for h in pend if id(h) not in sent]
        return
    if post_webhook(cfg, payload):
        esc["count"] += 1
        esc["last"] = t
        if not fast_lane:
            esc["last_batch"] = t
        sent = {id(h) for h in send}
        # unsent lower-ranked hits stay pending for the next window while still fresh
        st["pending_escalation"] = [h for h in pend if id(h) not in sent]
        log.info("escalated %d hit(s) [%s]%s; dispatch lag %ss; %d still pending; %d/%d today",
                 len(send), ",".join(payload["topics"]), " FAST-LANE" if fast_lane else "",
                 ",".join(str(h.get("dispatch_lag_s", "?")) for h in send),
                 len(st["pending_escalation"]), esc["count"], ESC_MAX_PER_DAY)
    else:
        esc["last"] = t      # retry: urgent after 30 s, normal batch after ESC_MIN_GAP_S
        if not fast_lane:
            esc["last_batch"] = t


# ------------------------------------------------------------------ cycle
UNDATED_FEEDS = {"Nikkei Asia"}


def _wire_grade(name: str, it: dict) -> bool:
    """Wire / top-desk source (Reuters, AP, BBC, Bloomberg, Nikkei, CNBC, WSJ, official feeds), any score."""
    return name != FJ_NAME and is_fast_lane(name, it, FAST_LANE_MIN_SCORE)


def is_fast_lane(name: str, it: dict, score: int) -> bool:
    if score < FAST_LANE_MIN_SCORE:
        return False
    src = (it.get("src") or "").strip()
    if name == FJ_NAME:
        return score >= FJ_MIN_SCORE + 2 and bool(FJ_WIRE_ATTRIB.search(it.get("title", "")))
    if name.startswith("GN:"):
        # aggregator results only count when the publisher itself is a top wire / official source
        if src:
            # scoring pass: a megacap's own release (IR/newsroom, or PR/Business Wire with the megacap as issuer) is first-hand
            return bool(FAST_LANE_PUBLISHERS.search(src) or MEGACAP_NEWSROOM_SRC.search(src)
                        or (PRESS_WIRES.search(src) and MEGACAP_ISSUER.search(_clean_title(name, it))))
    elif FAST_LANE_FEEDS.search(name):
        return True
    try:
        host = urllib.parse.urlsplit(it.get("link", "")).netloc.lower().split(":")[0]
    except Exception:
        host = ""
    return bool(FAST_LANE_DOMAINS.search(host))


# ---- headline floor (speed research 2026-10-28): headline shapes that are always WatcherGuru-tier but scored 5-8.
# Replayed misses: "OpenAI plans to buy Google AI chips" (8), "IMF reaches staff-level agreement with Argentina" (8),
# "Putin invites Zelensky to Moscow" (5), "Binance files for crypto license in Brazil" (6), "Trump ... ban ... AI" (6).
# Applied only to items the junk filters did not already drop (score > -20), floor = FAST_LANE_MIN_SCORE.
_BIG_AI = r"(openai|anthropic|xai|google|alphabet|microsoft|meta|apple|amazon|nvidia|amd|broadcom|tsmc|oracle|softbank|tesla|spacex)"
HEADLINE_FLOOR = re.compile(
    r"\b" + _BIG_AI + r"\b.{0,50}\b(plans to buy|to buy|buys|will buy|to acquire|acquires|agrees to buy|in talks to (buy|acquire)|"
    r"signs?|strikes?|inks?)\b.{0,50}(\b" + _BIG_AI + r"\b|\bai chips?\b|\btpus?\b|\bgpus?\b)"
    r"|\bstaff[- ]level agreement\b|\bimf (board )?(approves|reaches|agrees)\b|\bimf bailout\b"
    r"|\b(putin|zelensky[iy]?|trump|xi jinping|xi|netanyahu|khamenei|kim jong un)\b.{0,40}\b(invites?|agrees? to meet|will meet|to meet|"
    r"direct talks|summit with)\b.{0,40}\b(putin|zelensky[iy]?|trump|xi|netanyahu|khamenei|kim|moscow|kyiv|washington|beijing)\b"
    r"|\b(binance|coinbase|kraken|ripple|tether|circle|okx|bybit|robinhood|gemini|crypto\.com)\b.{0,40}\b(files for|applies for|wins|secures|"
    r"granted|obtains|receives|gets)\b.{0,30}\b(licen[cs]e|charter|approval|registration)"
    r"|\b(ripple|xrp|stellar|xlm)\b.{0,40}\b(partners? with|partnership with|strikes deal|deal with|teams up)\b"
    r"|\b(trump|white house)\b.{0,60}\b(bans?|banning|executive order)\b.{0,50}\b(ai|artificial intelligence|crypto|bitcoin|stablecoins?|chips?)\b"
    # WG-speed audit 2026-10-28: megacap/TradFi giant x crypto product; courts/states recognising bitcoin; mass buyouts
    r"|\b(google|alphabet|apple|microsoft|amazon|meta|blackrock|jpmorgan|visa|mastercard|paypal|stripe|fidelity|nasdaq|goldman)\b.{0,60}"
    r"\b(launch\w*|unveil\w*|previews?|to preview|rolls? out|introduc\w*|enters?|files?|buys?|bought)\b.{0,60}"
    r"\b(crypto\w*|bitcoin|btc|stablecoins?|tokeni[sz]ed|digital assets?|custody|blockchain)\b"
    r"|\b(supreme court|high court|court|parliament|government|central bank)\b.{0,60}\b(recogni[sz]\w*|rules?|declares?|legali[sz]\w*|approves?|bans?)\b"
    r".{0,40}\b(bitcoin|btc|crypto\w*)\b.{0,30}\b(legal|property|tender|currency|asset)"
    r"|\b(buyouts?|layoffs?|job cuts)\b.{0,30}\b\d{2,3},?\d{3}\b|\b\d{2,3},?\d{3}\b.{0,30}\b(buyouts?|layoffs?|jobs)\b",
    re.I)


def poll_feed(name: str, url: str, tier: str, topic: str, st: dict, t: float, first_run: bool, dry: bool,
              raw=None) -> list[dict]:
    seen = st.setdefault("seen_urls", {})
    recent = st.setdefault("recent_hits", [])
    health = st.setdefault("feed_health", {})
    try:
        if raw is None:
            items = fetch_feed(name, url, st)
        elif isinstance(raw, BaseException):
            raise raw
        else:
            items = fetch_finish(url, st, *raw)
        prev = health.get(name, {})
        health[name] = {"ok": True, "t": t, "n": len(items) if items else prev.get("n", 0)}
    except Exception as e:
        health[name] = {"ok": False, "t": t, "err": str(e)[:120]}
        log.warning("feed %s failed: %s", name, e)
        code = getattr(e, "code", None)
        if code in (401, 403, 429):         # blocked / rate-limited: back off the whole host politely
            hb = st.setdefault("host_backoff", {}).setdefault(host_of(url), {"n": 0, "until": 0})
            secs = HOST_BACKOFF_S[min(hb["n"], len(HOST_BACKOFF_S) - 1)]
            hb.update(n=hb["n"] + 1, until=t + secs)
            log.warning("host %s backed off %ds after HTTP %s", host_of(url), secs, code)
        return []
    st.get("host_backoff", {}).pop(host_of(url), None)
    hits = []
    primed = st.setdefault("primed_feeds", {})
    was_primed = bool(primed.get(name))
    if items and name in PRIME_ON_FIRST_FETCH and not was_primed:
        for it in items:            # new feed: mark the backlog seen, send nothing this time
            if it.get("link"):
                seen[norm_url(it["link"])] = t
        primed[name] = t
        log.info("feed %s primed (%d items marked seen)", name, len(items))
        return []
    if items:
        primed[name] = t
    for it in items:
        if not it["link"]:
            continue
        nu = norm_url(it["link"])
        if nu in seen:
            continue
        seen[nu] = t
        if not it["date"]:
            if name in UNDATED_FEEDS and was_primed:
                it["date"] = t          # first time we see it after the feed was primed = (near) publish time
            else:
                continue
        age_min = (t - it["date"]) / 60
        if age_min > MAX_AGE_MIN or age_min < -30:
            continue
        if first_run and age_min > FIRST_RUN_MAX_AGE_MIN and not dry:
            continue
        if topic == "AI":
            sc, why, clean = score_ai(name, tier, it)
            item_topic, thr = "AI", AI_HIT_THRESHOLD
        else:
            if name == FJ_NAME:
                sc, why, clean, item_topic = score_fj(name, tier, it)
                thr = FJ_MIN_SCORE
            else:
                sc, why, clean, item_topic = score_news(name, tier, it, topic)
                thr = CRYPTO_HIT_THRESHOLD if item_topic == "Crypto" else NEWS_HIT_THRESHOLD
        if name in OFFICIAL_MAJOR and sc > -50:
            rx, floor = OFFICIAL_MAJOR[name]
            if rx.search(clean if topic != "AI" else it["title"]):
                sc = max(sc, floor); why = list(why) + ["official-major"]
        if -20 < sc < FAST_LANE_MIN_SCORE and name != FJ_NAME and HEADLINE_FLOOR.search(clean if topic != "AI" else it["title"]):
            sc = FAST_LANE_MIN_SCORE; why = list(why) + ["headline-floor"]
        if sc < thr:
            continue
        if item_topic == "AI":
            skey, win, stories = ai_story_key(why, clean), AI_STORY_DEDUPE_S, st.setdefault("ai_stories", {})
        else:
            skey, win, stories = story_key(FJ_ATTRIB_TAIL.sub("", clean) if name == FJ_NAME else clean), STORY_DEDUPE_S, st.setdefault("stories", {})
        if skey and t - stories.get(skey, 0) < win:
            continue
        toks = title_tokens(clean)
        dup = next((r for r in recent if similar(toks, set(r["tokens"])) >= 0.5), None)
        if dup:
            dup.setdefault("also", []).append(name)
            continue
        recent.append({"t": t, "tokens": sorted(toks), "title": clean})
        if skey:
            stories[skey] = t
        hits.append({
            "ts_eat": eat(t), "topic": item_topic,
            "source": name + (f" ({it['src']})" if it.get("src") else ""),
            "title": clean, "url": it["link"], "score": sc, "kind": kind_for(why, item_topic),
            "why": why, "published_eat": eat(it["date"]), "published_ts": it["date"], "age_min": round(age_min),
            "detect_lag_s": round(t - it["date"]), "detected_ts": round(t, 1), "handled": False,
            "priority": is_fast_lane(name, it, sc), "instance": INSTANCE,
        })
        if name == FJ_NAME:
            hits[-1].update(lead_only=True, verify_note="FinancialJuice squawk = lead only. Confirm at the primary "
                            "source (company/central bank/government/wire) and cite that, never FinancialJuice.")
    return hits


_POOL: ThreadPoolExecutor | None = None


def _record_hits(st: dict, hits: list[dict], dry: bool):
    if hits and not dry:
        with open(QUEUE_PATH, "a") as f:
            for h in hits:
                f.write(json.dumps(h, ensure_ascii=False) + "\n")
    st.setdefault("pending_escalation", []).extend(hits)
    for h in hits:
        log.info("HIT [%s %s %s] %s | %s | lag %ss | %s", h.get("topic"), h["kind"], h["score"], h["title"],
                 h["source"], h.get("detect_lag_s", 0), h["url"])


def cycle(st: dict, dry: bool, force_all: bool = False) -> list[dict]:
    """Fetch every feed whose interval has elapsed (all of them if force_all) in parallel; process each
    feed as soon as it returns and dispatch urgent hits at once (latency pass 2026-10-26)."""
    global _POOL
    t = t0 = now_ts()
    first_run = not st.get("seen_urls")
    last_fetch = st.setdefault("feed_last_fetch", {})
    backoff = st.setdefault("host_backoff", {})
    due = []
    for name, url, tier, interval in FEEDS:
        if not force_all and t - last_fetch.get(name, 0) < max(interval, MIN_FEED_INTERVAL_S):
            continue
        if backoff.get(host_of(url), {}).get("until", 0) > t:
            continue
        last_fetch[name] = t
        topic = "AI" if name in AI_FEED_NAMES else ("Crypto" if name in CRYPTO_FEED_NAMES else "")
        due.append((name, url, tier, topic, fetch_prepare(url, st)))
    hits = []
    if due:
        if _POOL is None:
            _POOL = ThreadPoolExecutor(max_workers=FETCH_WORKERS, thread_name_prefix="fetch")
        futs = {_POOL.submit(fetch_http, url, hdr): (name, url, tier, topic) for name, url, tier, topic, hdr in due}
        for fut in as_completed(futs):
            name, url, tier, topic = futs[fut]
            try:
                raw = fut.result()
            except Exception as e:
                raw = e
            new = poll_feed(name, url, tier, topic, st, now_ts(), first_run, dry, raw=raw)
            if new:
                _record_hits(st, new, dry)
                hits += new
                if any(is_urgent(h) for h in new):
                    maybe_escalate(st, dry)          # don't wait for the slowest feed in this tick
    polled = len(due)
    if PRICE_ALERTS_ENABLED:
        for key, every, fn in (("_crypto_px", CRYPTO_POLL_S, check_crypto_prices),
                               ("_market_px", MARKET_POLL_S, check_market_prices),
                               ("_us_debt", DEBT_POLL_S, check_us_debt),
                               ("_anthropic", ANTHROPIC_POLL_S, check_anthropic_site)):
            if force_all or t - last_fetch.get(key, 0) >= every:
                last_fetch[key] = t
                try:
                    px = fn(st)
                    for h in px:
                        h.update(ts_eat=eat(t), handled=False, detected_ts=round(now_ts(), 1))
                    _record_hits(st, px, dry)
                    hits += px
                except Exception as e:
                    log.warning("price check %s failed: %s", key, e)

    # prune state
    t = now_ts()
    st["seen_urls"] = {u: ts for u, ts in st.get("seen_urls", {}).items() if t - ts < 4 * 86400}
    st["recent_hits"] = [r for r in st.get("recent_hits", []) if t - r["t"] < 48 * 3600]
    st["ai_stories"] = {k: v for k, v in st.get("ai_stories", {}).items() if t - v < AI_STORY_DEDUPE_S}
    st["stories"] = {k: v for k, v in st.get("stories", {}).items() if t - v < STORY_DEDUPE_S}
    st["host_backoff"] = {h: v for h, v in backoff.items() if v.get("until", 0) > t - 3600}
    st.pop("price_hist", None)
    names = {f[0] for f in FEEDS}            # drop health rows of removed feeds (e.g. the Reuters sitemap)
    st["feed_health"] = {k: v for k, v in st.get("feed_health", {}).items() if k in names}

    maybe_escalate(st, dry)
    if polled or hits:
        st["last_cycle"] = {"t": t, "eat": eat(t), "feeds_polled": polled, "hits": len(hits),
                            "secs": round(now_ts() - t0, 1)}
    return hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--max-age-min", type=float, help="override freshness window (testing)")
    ap.add_argument("--prime", action="store_true",
                    help="one pass that only marks every current item as seen (sends nothing); for a fresh state file")
    ap.add_argument("--run-for", type=float, default=0, metavar="SECONDS",
                    help="loop mode with a time limit: poll continuously, then save state and exit 0 after SECONDS (GitHub Actions chunks)")
    a = ap.parse_args()
    global MAX_AGE_MIN, PRICE_ALERTS_ENABLED
    if a.max_age_min:
        MAX_AGE_MIN = a.max_age_min
    if a.prime:
        a.once, MAX_AGE_MIN, PRICE_ALERTS_ENABLED = True, -1, False
    setup_logging(a.verbose or (a.once and not a.dry_run))

    def _sig(*_):
        global _stop
        _stop = True
    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    log.info("watcher start (once=%s dry=%s) pid=%s feeds=%d (news %d, crypto %d, AI %d) prices=%s "
             "webhook: 1/%dmin, %d/day, %d hits/batch, max age %dmin",
             a.once, a.dry_run, os.getpid(), len(FEEDS), len(NEWS_FEEDS) if NEWS_ENABLED else 0,
             len(CRYPTO_FEEDS) if CRYPTO_ENABLED else 0, len(AI_FEEDS4), PRICE_ALERTS_ENABLED,
             ESC_MIN_GAP_S // 60, ESC_MAX_PER_DAY, MAX_HITS_PER_WEBHOOK, MAX_AGE_MIN)
    st = load_state()
    if a.dry_run:
        for k in ("seen_urls", "recent_hits", "http_cache", "stories", "ai_stories", "feed_last_fetch"):
            st[k] = {} if k != "recent_hits" else []
    if INSTANCE == "box" and not a.once and not a.dry_run and os.environ.get("HERALD_HEARTBEAT", "1") != "0":
        heartbeat_thread(st)
    n_cycles = 0
    deadline = time.time() + a.run_for if a.run_for and not a.once else 0
    while True:
        try:
            hits = cycle(st, a.dry_run, force_all=a.once)
            lc = st.get("last_cycle", {})
            n_cycles += 1
            if hits or n_cycles % 60 == 0:      # summary every ~5 min, or when something hit
                ok = sum(1 for v in st.get("feed_health", {}).values() if v.get("ok"))
                log.info("tick: %d hit(s) | feeds ok %d/%d | last poll %s", len(hits), ok,
                         len(st.get("feed_health", {})), lc.get("eat"))
        except Exception as e:
            log.exception("cycle crashed: %s", e)
            hits = []
        if not a.dry_run and (a.once or st.get("last_cycle", {}).get("t", 0) >= time.time() - TICK_SECONDS - 60):
            save_state(st)
        if a.once:
            if a.dry_run:
                print(json.dumps(hits, indent=1, ensure_ascii=False))
            return
        end = time.time() + TICK_SECONDS
        while time.time() < end and not _stop:
            time.sleep(1)
        if deadline and time.time() >= deadline and not _stop:
            ok = sum(1 for v in st.get("feed_health", {}).values() if v.get("ok"))
            log.info("run-for limit reached after %d cycles | feeds ok %d/%d | escalations today %s/%d",
                     n_cycles, ok, len(st.get("feed_health", {})), st.get("escalations", {}).get("count", 0), ESC_MAX_PER_DAY)
            _stop_now = True
        else:
            _stop_now = False
        if _stop or _stop_now:
            log.info("watcher stop")
            if not a.dry_run:
                save_state(st)
            return


if __name__ == "__main__":
    main()
