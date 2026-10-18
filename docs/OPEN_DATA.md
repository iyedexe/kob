# Open datasets — `kob --get-data`

`kob --get-data` downloads real, public data and writes it as Hive-partitioned Parquet into
the data root kob serves (`KOB_DATA_ROOT`, default `./data`), then exits. Serve it straight
after with `uv run kob --mode rest` (or plain `uv run kob` for Flight). No extra
dependencies — just pyarrow and the standard library.

```bash
uv run kob --get-data list            # every dataset, group, size
uv run kob --get-data                 # = core: 8 small sets, ~25 MB, ~5 s
uv run kob --get-data heavy           # uk_house_prices + crypto_klines_1m + forex_ticks
uv run kob --get-data all             # core + heavy
uv run kob --get-data fx_daily,forex_ticks --from 2015-01 --to 2015-03
KOB_DATA_ROOT=/big/disk uv run kob --get-data all
```

Make targets: `make get-data` (core) and `make get-data-heavy` (all, defaults below).

---

## Options

| Option | Applies to | Meaning |
|---|---|---|
| `--get-data SETS` | — | comma-separated dataset names and/or groups `core`, `heavy`, `all`; `list` prints the catalogue. No value = `core`. |
| `--from YYYY[-MM]` | heavy sets | first year/month to fetch, inclusive |
| `--to YYYY[-MM]` | heavy sets | last year/month to fetch, inclusive |
| `--symbols A,B,…` | `crypto_klines_1m` | Binance symbols (default: BTC, ETH, BNB, SOL, XRP, ADA, DOGE, TRX, LTC, LINK vs USDT) |

Defaults when `--from` / `--to` are omitted:

| dataset | default range |
|---|---|
| `uk_house_prices` | every year (1995 → 2023-02) |
| `crypto_klines_1m` | the last 12 published months |
| `forex_ticks` | one month: 2022-08 (the last in the archive) |

## How it writes data

- **Partitioned and sorted for kob.** Time-series are partitioned by `year` (plus `symbol`,
  `pair` or `index` where it helps) and sorted by the columns you range on, so partition
  pruning and Parquet row-group statistics skip what a query doesn't need.
- **Resumable.** Heavy sets skip years/months already on disk — rerun the same command after
  an interruption and it picks up where it stopped. Core sets are small and always rebuilt.
- **Never half-written.** Each heavy piece is written to a `.kob-staging-*` folder *next to*
  the data root (outside it) or to a `.tmp` file, and moved into place only when complete, so a
  running server never serves a partial partition. In REST mode press **Reload datasets**
  (or `POST /reload`) to pick up new data.
- **Retries.** Downloads retry 4 times with exponential backoff on dropped connections / TLS
  errors; HTTP 404s (a symbol not listed yet, a month not published) are skipped and counted.
- **Provenance.** `<root>/SOURCES.md` lists every downloaded dataset with its source URL and
  licence. Check it before redistributing anything.
- Downloaded data and `.kob-staging-*` are git-ignored (`data/`, `data-*/`).

---

## Core datasets (public CSVs on GitHub)

### `prices_daily` — S&P 500 daily prices
619,040 rows · 2013-02-08 → 2018-02-07 · 505 tickers · partitioned by `year`, sorted `ticker, date`

| column | type | |
|---|---|---|
| date | date | trading day |
| ticker | string | e.g. `AAPL` |
| open, high, low, close | double | USD |
| volume | int64 | shares |

Source: Kaggle "S&P 500 stock data" (camnugent/sandp500) via `plotly/datasets` — **CC0**.

### `fx_daily` — FX rates vs USD
262,587 rows · 1971-01-04 → today · 22 currencies · partitioned by `year`, sorted `currency, date`

| column | type | |
|---|---|---|
| date | date | |
| currency | string | ISO code: EUR, GBP, JPY, CHF, AUD, CAD, CNY, … |
| country | string | as named by the Fed (`Euro`, `United Kingdom`, …) |
| units_per_usd | double | currency per 1 USD (EUR ≈ 0.92) |
| usd_per_unit | double | USD per 1 unit (1 EUR ≈ 1.09 USD) |

Source: US Federal Reserve H.10 via FRED, packaged by datahub.io — **ODC-PDDL**. Note: the
package README claims some currencies are quoted the other way; the actual file quotes all
of them per USD, which is what the loader assumes (checked against known 2024 rates).

### `index_membership` — S&P 500 members over time
1,644,638 rows · 1996-01-02 → 2025-08 · partitioned by `index` (`SPX`) and `year`, sorted `date, ticker`

| column | type | |
|---|---|---|
| date | date | a day on which membership changed |
| ticker | string | a member on that date |

There is a snapshot **each time membership changed** (3,480 dates), not one per trading day:
the members on day *D* are those in the latest snapshot on or before *D* (see the as-of
example below). Early years list ~470 names (the Wikipedia-derived history is incomplete
there). Source: `hanshof/sp500_constituents`, built from Wikipedia.

### `index_membership_periods` — membership start/end per ticker
1,262 rows · columns: `index`, `ticker`, `start_date`, `end_date` (**null = still a member**).
Source: `fja05680/sp500`.

### `sp500_constituents` — current members
503 rows · `ticker, name, sector, sub_industry, headquarters, date_added, cik, founded`.
Source: datahub.io (from Wikipedia).

### `sp500_monthly` — Shiller's S&P 500 data
1,869 rows · 1871 → today · `date, sp500, dividend, earnings, cpi, long_interest_rate,
real_price, real_dividend, real_earnings, pe10`. Source: Robert Shiller via datahub.io —
**ODC-PDDL**. (Index-level dividends; there is no per-stock dividend set.)

### `vix_daily`, `brent_daily`
9,278 / 9,108 rows · VIX `date, open, high, low, close` (1990 →) and Brent `date, price`
(1987 →). Sources: CBOE and US EIA via datahub.io.

---

## Heavy datasets (public S3 buckets, streamed)

### `uk_house_prices` — every property sale in England & Wales
28,113,076 rows · 1995-01-01 → 2023-02-27 · ~0.7 GB download, 636 MB Parquet, ~1 min ·
partitioned by `year`, sorted `date, postcode`

| column | type | |
|---|---|---|
| date | date | completion date |
| price | int64 | GBP |
| postcode | string | e.g. `CB2 1TN` |
| type | string | `detached`, `semi-detached`, `terraced`, `flat`, `other` |
| is_new | bool | new build |
| duration | string | `freehold`, `leasehold`, `unknown` |
| addr1, addr2, street, locality, town, district, county | string | address (towns upper-case: `LONDON`) |

Source: HM Land Registry Price Paid Data, as hosted by ClickHouse
(`s3://datasets-documentation/uk-house-prices/`). **Open Government Licence v3.0** —
attribution required: *Contains HM Land Registry data © Crown copyright and database right.*
The hosted copy ends in February 2023.

### `crypto_klines_1m` — Binance 1-minute candles
~44,640 rows per symbol-month (525,600 per symbol-year) · from 2017-08 (BTCUSDT) to last month ·
default 10 symbols × 12 months = 5.26M rows, 307 MB · partitioned by `symbol` and `year`, one
file per month, sorted by `open_time`

| column | type | |
|---|---|---|
| open_time, close_time | timestamp (UTC, ms) | candle open/close |
| open, high, low, close | double | quote currency (USDT) |
| volume | double | base asset volume |
| quote_volume | double | USDT volume |
| trades | int64 | number of trades |
| taker_buy_base_volume, taker_buy_quote_volume | double | |

Source: Binance public market data (`data.binance.vision`, spot monthly klines). Binance
switched these files to **microsecond** timestamps from 2025-01-01; the loader detects the
unit per file and stores milliseconds throughout. Publicly downloadable — see Binance's terms
(`github.com/binance/binance-public-data`) before redistributing.

### `forex_ticks` — FX bid/ask ticks
70–145M rows per month (2022-08: 144,529,181 ticks, 1.4 GB Parquet, ~30 s) · 2000-05 → 2022-08 ·
66 pairs in total (61 in 2022-08) · the full archive is ~11.5 billion ticks / ~550 GB ·
partitioned by `pair` and `year`, one file per month per pair

| column | type | |
|---|---|---|
| datetime | timestamp (UTC, ms) | tick time |
| bid, ask | double | |
| base, quote | string | e.g. `EUR`, `USD` |
| pair (partition) | string | e.g. `EURUSD` |

Source: histdata.com tick data, cleaned and hosted by ClickHouse
(`s3://datasets-documentation/forex/`). Tick collection was best-effort, so tick counts are
not a measure of market activity. Check histdata.com's terms before redistributing.

---

## Example queries

```python
from kob.tools.client import KobClient
import duckdb

kob = KobClient("http://127.0.0.1:8000")            # or "grpc://127.0.0.1:8815"

# --- stocks / FX / index -------------------------------------------------
px, fx, mem = kob["prices_daily"], kob["fx_daily"], kob["index_membership"]
px.query(px.date["2017-01-01":"2017-03-31"], ticker=["AAPL", "MSFT"])
px.query(px.volume[100_000_000:], columns=["date", "ticker", "volume"])
fx.query(fx.date["2024-01-01":"2024-12-31"], currency=["EUR", "GBP"])

# S&P 500 basket on 2017-06-30, priced in EUR (as-of membership + client-side join)
day = "2017-06-30"
asof = max(mem.query(mem.date[:day], columns=["date"])["date"].to_pylist())
members = mem.query(mem.date[asof:asof], columns=["ticker"])["ticker"].to_pylist()
closes = px.query(px.date[day:day], ticker=members, columns=["ticker", "close"])
eur = fx.query(fx.date[day:day], currency="EUR", columns=["usd_per_unit"])
duckdb.sql("SELECT c.ticker, c.close / e.usd_per_unit AS close_eur FROM closes c, eur e").df()

# --- UK house prices ------------------------------------------------------
uk = kob["uk_house_prices"]
uk.query(uk.date["2020-01-01":"2020-12-31"], town="LONDON", type="detached")
uk.query(uk.price[1_000_000:], uk.year[2015:2023], columns=["date", "price", "town", "type"])

# --- crypto ---------------------------------------------------------------
k = kob["crypto_klines_1m"]
k.query(k.open_time["2026-03-01":"2026-03-07"], symbol="BTCUSDT")   # one week, 10,080 candles
k.query(k.quote_volume[50_000_000:], columns=["open_time", "close", "quote_volume"])

# --- FX ticks -------------------------------------------------------------
t = kob["forex_ticks"]
t.query(t.datetime["2022-08-15":"2022-08-15"], pair="EURUSD")        # one day
t.query(t.datetime["2022-08-10T14:00:00":"2022-08-10T15:00:00"])      # one hour, all pairs
```

REST GET equivalents (what the Swagger form sends):

```bash
curl "localhost:8000/data/uk_house_prices?date.start=2020-01-01&date.end=2020-12-31&town=LONDON&type=detached&format=json&limit=5"
curl "localhost:8000/data/forex_ticks?pair=EURUSD&datetime.start=2022-08-15&datetime.end=2022-08-15" -o eurusd.arrow
```

## Measured

`kob --get-data all` followed by `kob --mode rest` and `kob` (Flight) on the same data; each
query run once per transport from one Python client on the same machine, uncompressed Arrow.
Both transports returned identical results for every query.

| query | rows | in memory | REST | Flight |
|---|---:|---:|---:|---:|
| UK: detached houses sold in London, 2020 | 1,039 | <0.1 MB | 285 ms | 287 ms |
| UK: every sale ≥ £1M, 2015–2023 | 232,552 | 8 MB | 286 ms | 268 ms |
| UK: every sale in 2022, all columns | 812,532 | 112 MB | 438 ms | 421 ms |
| Crypto: BTCUSDT 1m candles, one week | 10,080 | 1 MB | 25 ms | 35 ms |
| Crypto: candles ≥ $50M quote volume, all symbols | 107 | <0.1 MB | 29 ms | 31 ms |
| Crypto: all symbols × 12 months, 3 columns | 5,256,000 | 126 MB | 337 ms | 328 ms |
| FX ticks: EURUSD on one day | 129,503 | 7 MB | 56 ms | 78 ms |
| FX ticks: one hour, all pairs | 382,710 | 21 MB | 151 ms | 175 ms |
| FX ticks: EURUSD, whole month, 3 columns | 2,987,572 | 72 MB | 235 ms | 179 ms |
| FX ticks: every tick of the month, all pairs, 4 columns | 144,529,181 | 4.9 GB | 16.9 s | 22.9 s |

Download times for the defaults: core ~5 s, `uk_house_prices` 67 s, `crypto_klines_1m` (120
files) a few seconds once listed, `forex_ticks` one month ~30 s. Single runs on one machine —
treat them as orders of magnitude, not a benchmark (see `docs/PERFORMANCE_REPORT.md` for that).

## Adding a dataset

Datasets live in `src/kob/tools/open_data.py`:

- **small CSV** — add a `Source` to `SOURCES`, a `build_<name>(raw: bytes) -> (table,
  partition_cols)` function to `BUILDERS`, and a `Spec("core", …, _core_fetch(name))` to
  `DATASETS`;
- **large / streamed** — write a `fetch_<name>(out: Path, opts: Options) -> rows` that skips
  what is already on disk and writes atomically (see `fetch_uk_house_prices` /
  `fetch_forex_ticks`), and register it as `Spec("heavy", …)`.

Add an offline test with inline sample data to `tests/test_kob.py` (see
`test_open_data_builders`, `test_get_data_helpers_and_cli`).
