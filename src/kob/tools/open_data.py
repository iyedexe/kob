"""Real, open datasets for trying kob — ``kob --get-data`` (no extra dependencies).

Downloads public data and writes Hive-partitioned, range-sorted Parquet into the data root
kob serves (``KOB_DATA_ROOT``, default ``./data``), so ``kob --mode rest`` works right after.

**core** (default, ~25 MB, seconds) — public CSVs hosted on GitHub::

    prices_daily/year=2017/              daily OHLCV, 505 S&P 500 stocks, 2013-2018
    fx_daily/year=2024/                  Fed H.10 FX vs USD, 22 currencies, 1971-today
    index_membership/index=SPX/year=…/   S&P 500 member snapshots, 1996-2025 (one per change)
    index_membership_periods/            ticker, start_date, end_date (null = still in)
    sp500_constituents/                  today's members: sector, date added, CIK
    sp500_monthly/                       Shiller: index level, dividend, earnings, CPI, 1871-
    vix_daily/, brent_daily/             CBOE VIX, Brent crude spot

**heavy** — public S3 buckets, streamed and re-partitioned::

    uk_house_prices/year=2020/           HM Land Registry Price Paid: ~30M sales, 1995-2023
    crypto_klines_1m/symbol=BTCUSDT/year=2024/   Binance 1-minute candles (pick symbols/months)
    forex_ticks/pair=EURUSD/year=2015/   histdata.com bid/ask ticks, 70-145M rows per month

Sources and licences are written to ``<root>/SOURCES.md``. Heavy sets are resumable: months
/ years already on disk are skipped, and each one is staged outside the data root and moved
into place only when complete, so a running server never sees a half-written partition.

    kob --get-data                                   # core
    kob --get-data list                              # what's available, with sizes
    kob --get-data uk_house_prices,crypto_klines_1m --symbols BTCUSDT,ETHUSDT --from 2023-01
    kob --get-data forex_ticks --from 2015-01 --to 2015-03
    KOB_DATA_ROOT=/big/disk kob --get-data all
"""

from __future__ import annotations

import io
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq

RAW = "https://raw.githubusercontent.com"


@dataclass(frozen=True)
class Source:
    url: str
    origin: str
    licence: str


SOURCES: dict[str, Source] = {
    "prices_daily": Source(
        f"{RAW}/plotly/datasets/master/all_stocks_5yr.csv",
        "Kaggle 'S&P 500 stock data' (camnugent/sandp500), mirrored by plotly/datasets",
        "CC0 1.0 (public domain)"),
    "fx_daily": Source(
        f"{RAW}/datasets/exchange-rates/main/data/daily.csv",
        "US Federal Reserve H.10 via FRED, packaged by datahub.io (datasets/exchange-rates)",
        "ODC-PDDL (public domain); underlying data is US-government public data"),
    "index_membership": Source(
        f"{RAW}/hanshof/sp500_constituents/main/sp_500_historical_components.csv",
        "hanshof/sp500_constituents — S&P 500 member list each day membership changed "
        "(as-of: members on day D = latest snapshot <= D), built from Wikipedia",
        "see the repository; derived from Wikipedia (CC BY-SA)"),
    "index_membership_periods": Source(
        f"{RAW}/fja05680/sp500/master/sp500_ticker_start_end.csv",
        "fja05680/sp500 — S&P 500 membership start/end per ticker",
        "see the repository"),
    "sp500_constituents": Source(
        f"{RAW}/datasets/s-and-p-500-companies/main/data/constituents.csv",
        "datahub.io (datasets/s-and-p-500-companies), from Wikipedia",
        "ODC-PDDL; derived from Wikipedia (CC BY-SA)"),
    "sp500_monthly": Source(
        f"{RAW}/datasets/s-and-p-500/main/data/data.csv",
        "Robert Shiller's U.S. stock market data, packaged by datahub.io (datasets/s-and-p-500)",
        "ODC-PDDL"),
    "vix_daily": Source(
        f"{RAW}/datasets/finance-vix/main/data/vix-daily.csv",
        "CBOE VIX history, packaged by datahub.io (datasets/finance-vix)",
        "ODC-PDDL packaging; CBOE terms apply to the underlying data"),
    "brent_daily": Source(
        f"{RAW}/datasets/oil-prices/main/data/brent-daily.csv",
        "US EIA Brent spot price, packaged by datahub.io (datasets/oil-prices)",
        "ODC-PDDL; EIA data is US-government public data"),
}

# FRED H.10 country name → ISO currency. The packaged file quotes every currency as units
# of that currency per 1 USD (its README's "inverted" list is out of date — checked: EUR
# 2024-06-03 = 0.9183, i.e. EUR per USD).
_FX = {
    "Australia": "AUD", "Brazil": "BRL", "Canada": "CAD", "China": "CNY", "Denmark": "DKK",
    "Euro": "EUR", "Hong Kong": "HKD", "India": "INR", "Japan": "JPY", "Malaysia": "MYR",
    "Mexico": "MXN", "New Zealand": "NZD", "Norway": "NOK", "Singapore": "SGD",
    "South Africa": "ZAR", "South Korea": "KRW", "Sweden": "SEK", "Switzerland": "CHF",
    "Taiwan": "TWD", "Thailand": "THB", "United Kingdom": "GBP", "Venezuela": "VES",
    "Ireland": "IEP",
}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _download(url: str, attempts: int = 5) -> bytes:
    """GET with retries + exponential backoff: big pulls hit transient TLS/connection drops.
    HTTP errors (404 = not published) are returned to the caller immediately."""
    req = urllib.request.Request(url, headers={"User-Agent": "kob-get-data"})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                return resp.read()
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, OSError) as exc:  # incl. ssl.SSLError, resets, timeouts
            if attempt == attempts - 1:
                raise
            wait = 2 ** attempt
            print(f"    retry {attempt + 1}/{attempts - 1} in {wait}s: {url.rsplit('/', 1)[-1]} ({exc})")
            time.sleep(wait)
    raise AssertionError("unreachable")


def _csv(data: bytes, types: dict[str, pa.DataType] | None = None,
         names: list[str] | None = None) -> pa.Table:
    read = pacsv.ReadOptions(column_names=names, skip_rows=1 if names else 0)
    conv = pacsv.ConvertOptions(column_types=types or {}, strings_can_be_null=True)
    return pacsv.read_csv(io.BytesIO(data), read_options=read, convert_options=conv)


def _date(col: pa.Array) -> pa.Array:
    """ISO 'YYYY-MM-DD' (or longer) strings → date32; anything unparsable → null."""
    col = pc.utf8_slice_codeunits(col.cast(pa.string()), 0, 10)
    return pc.strptime(col, format="%Y-%m-%d", unit="s", error_is_null=True).cast(pa.date32())


def _with_year(t: pa.Table, date_col: str = "date") -> pa.Table:
    return t.append_column("year", pc.year(t[date_col]).cast(pa.int16()))


def _write(t: pa.Table, out: Path, name: str, partition_by: list[str] | None = None) -> int:
    target = out / name
    if target.exists():
        shutil.rmtree(target)
    if partition_by:
        part = pads.partitioning(pa.schema([t.schema.field(c) for c in partition_by]), flavor="hive")
        pads.write_dataset(t, target, format="parquet", partitioning=part,
                           basename_template="part-{i}.parquet",
                           existing_data_behavior="overwrite_or_ignore")
    else:
        target.mkdir(parents=True)
        pq.write_table(t, target / "part-0.parquet")
    return t.num_rows


# --------------------------------------------------------------------------- #
# Builders: raw CSV bytes → Arrow table (+ partition columns)
# --------------------------------------------------------------------------- #
def build_prices_daily(raw: bytes) -> tuple[pa.Table, list[str]]:
    t = _csv(raw, {"open": pa.float64(), "high": pa.float64(), "low": pa.float64(),
                   "close": pa.float64(), "volume": pa.int64()})
    t = pa.table({"date": _date(t["date"]), "ticker": t["Name"], "open": t["open"],
                  "high": t["high"], "low": t["low"], "close": t["close"], "volume": t["volume"]})
    return _with_year(t.sort_by([("ticker", "ascending"), ("date", "ascending")])), ["year"]


def build_fx_daily(raw: bytes) -> tuple[pa.Table, list[str]]:
    t = _csv(raw, {"Exchange rate": pa.float64()})
    t = t.filter(pc.is_valid(t["Exchange rate"]))
    countries = t["Country"].to_pylist()
    unknown = sorted({c for c in countries if c not in _FX})
    if unknown:
        raise ValueError(f"fx_daily: unmapped countries {unknown}; extend _FX")
    units_per_usd = t["Exchange rate"]  # currency per 1 USD, for every currency
    t = pa.table({
        "date": _date(t["Date"]),
        "currency": pa.array([_FX[c] for c in countries]),
        "country": t["Country"],
        "units_per_usd": units_per_usd,
        "usd_per_unit": pc.divide(1.0, units_per_usd),
    })
    return _with_year(t.sort_by([("currency", "ascending"), ("date", "ascending")])), ["year"]


def build_index_membership(raw: bytes) -> tuple[pa.Table, list[str]]:
    t = _csv(raw)
    lists = pc.split_pattern(t["tickers"], ",")
    t = pa.table({
        "date": _date(t["date"]).take(pc.list_parent_indices(lists)),
        "ticker": pc.utf8_trim_whitespace(pc.list_flatten(lists)),
    })
    t = t.filter(pc.not_equal(t["ticker"], ""))
    t = t.group_by(["date", "ticker"]).aggregate([])  # the source repeats a couple of dates
    t = t.append_column("index", pa.array(["SPX"] * t.num_rows))
    return _with_year(t.sort_by([("date", "ascending"), ("ticker", "ascending")])), ["index", "year"]


def build_index_membership_periods(raw: bytes) -> tuple[pa.Table, list[str]]:
    t = _csv(raw)
    t = pa.table({"index": pa.array(["SPX"] * t.num_rows), "ticker": t["ticker"],
                  "start_date": _date(t["start_date"]), "end_date": _date(t["end_date"])})
    return t.sort_by([("ticker", "ascending"), ("start_date", "ascending")]), []


def build_sp500_constituents(raw: bytes) -> tuple[pa.Table, list[str]]:
    t = _csv(raw, {"CIK": pa.int64(), "Founded": pa.string()})
    t = pa.table({
        "ticker": t["Symbol"], "name": t["Security"], "sector": t["GICS Sector"],
        "sub_industry": t["GICS Sub-Industry"], "headquarters": t["Headquarters Location"],
        "date_added": _date(t["Date added"]), "cik": t["CIK"], "founded": t["Founded"],
    })
    return t.sort_by("ticker"), []


def build_sp500_monthly(raw: bytes) -> tuple[pa.Table, list[str]]:
    names = ["date", "sp500", "dividend", "earnings", "cpi", "long_interest_rate",
             "real_price", "real_dividend", "real_earnings", "pe10"]
    t = _csv(raw, {n: pa.float64() for n in names[1:]}, names=names)
    return t.set_column(0, "date", _date(t["date"])).sort_by("date"), []


def build_vix_daily(raw: bytes) -> tuple[pa.Table, list[str]]:
    names = ["date", "open", "high", "low", "close"]
    t = _csv(raw, {n: pa.float64() for n in names[1:]}, names=names)
    t = t.set_column(0, "date", _date(t["date"]))
    return t.filter(pc.is_valid(t["date"])).sort_by("date"), []


def build_brent_daily(raw: bytes) -> tuple[pa.Table, list[str]]:
    t = _csv(raw, {"price": pa.float64()}, names=["date", "price"])
    return t.set_column(0, "date", _date(t["date"])).sort_by("date"), []


BUILDERS: dict[str, Callable[[bytes], tuple[pa.Table, list[str]]]] = {
    "prices_daily": build_prices_daily,
    "fx_daily": build_fx_daily,
    "index_membership": build_index_membership,
    "index_membership_periods": build_index_membership_periods,
    "sp500_constituents": build_sp500_constituents,
    "sp500_monthly": build_sp500_monthly,
    "vix_daily": build_vix_daily,
    "brent_daily": build_brent_daily,
}


# --------------------------------------------------------------------------- #
# Heavy datasets (public S3, streamed)
# --------------------------------------------------------------------------- #
CH_BUCKET, CH_REGION = "datasets-documentation", "eu-west-3"   # ClickHouse example datasets
BINANCE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
                   "ADAUSDT", "DOGEUSDT", "TRXUSDT", "LTCUSDT", "LINKUSDT"]

SOURCES.update({
    "uk_house_prices": Source(
        f"s3://{CH_BUCKET}/uk-house-prices/parquet/house_prices_YYYY.parquet",
        "HM Land Registry Price Paid Data (England & Wales), as hosted by ClickHouse",
        "Open Government Licence v3.0 — attribution: Contains HM Land Registry data "
        "© Crown copyright and database right"),
    "crypto_klines_1m": Source(
        f"{BINANCE}/data/spot/monthly/klines/SYMBOL/1m/SYMBOL-1m-YYYY-MM.zip",
        "Binance public market data (data.binance.vision), spot 1-minute klines",
        "publicly downloadable; see Binance's terms for the data (github.com/binance/binance-public-data)"),
    "forex_ticks": Source(
        f"s3://{CH_BUCKET}/forex/parquet/year_month/YYYYMM-tick.parquet",
        "histdata.com FX tick data, cleaned and hosted by ClickHouse (66 pairs, 2000-2022)",
        "free download from histdata.com; check histdata.com's terms before redistributing"),
})

# ClickHouse's Price Paid enum codes → labels, indexed by code (verified against prices:
# detached has the highest median, flats are overwhelmingly leasehold).
_HOUSE_TYPE = pa.array(["other", "terraced", "semi-detached", "detached", "flat"])
_HOUSE_DURATION = pa.array(["unknown", "freehold", "leasehold"])
_KLINE_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
               "quote_volume", "trades", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"]


@dataclass
class Options:
    start: str | None = None          # "YYYY-MM" (or "YYYY"), inclusive
    end: str | None = None            # "YYYY-MM" (or "YYYY"), inclusive
    symbols: list[str] | None = None  # crypto_klines_1m
    workers: int = 8


def _s3(region: str):
    import pyarrow.fs as pafs

    return pafs.S3FileSystem(anonymous=True, region=region)


def _months(start: str, end: str) -> list[tuple[int, int]]:
    y, m = int(start[:4]), int(start[5:7] or 1) if len(start) > 4 else 1
    ey, em = int(end[:4]), int(end[5:7]) if len(end) > 4 else 12
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _last_full_month() -> str:
    t = time.gmtime()
    y, m = (t.tm_year - 1, 12) if t.tm_mon == 1 else (t.tm_year, t.tm_mon - 1)
    return f"{y:04d}-{m:02d}"


def _shift(month: str, delta: int) -> str:
    y, m = int(month[:4]), int(month[5:7])
    i = y * 12 + (m - 1) + delta
    return f"{i // 12:04d}-{i % 12 + 1:02d}"


def _staged_write(final: Path, write: Callable[[Path], None]) -> None:
    """Write into a staging dir *outside* the data root, then move into place atomically-ish."""
    staging = Path(tempfile.mkdtemp(prefix=".kob-staging-", dir=final.parents[1]))  # sibling of the data root
    try:
        write(staging)
        for f in staging.rglob("*.parquet"):
            dest = final / f.relative_to(staging)
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(f, dest)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def fetch_uk_house_prices(out: Path, opts: Options) -> int:
    import pyarrow.parquet as pq_

    s3 = _s3(CH_REGION)
    from pyarrow.fs import FileSelector

    years = sorted(int(m.group(1)) for f in s3.get_file_info(
        FileSelector(f"{CH_BUCKET}/uk-house-prices/parquet/"))
        if (m := re.search(r"/house_prices_(\d{4})\.parquet$", f.path)))
    lo = int(opts.start[:4]) if opts.start else years[0]
    hi = int(opts.end[:4]) if opts.end else years[-1]
    root, rows, skipped = out / "uk_house_prices", 0, 0
    for y in [y for y in years if lo <= y <= hi]:
        target = root / f"year={y}" / "part-0.parquet"
        if target.exists():
            skipped += 1
            continue
        t = pq_.read_table(s3.open_input_file(f"{CH_BUCKET}/uk-house-prices/parquet/house_prices_{y}.parquet"))
        out_t = pa.table({
            "date": t["date"].cast(pa.int32()).cast(pa.date32()),
            "price": t["price"].cast(pa.int64()),
            "postcode": pc.utf8_trim_whitespace(pc.binary_join_element_wise(
                t["postcode1"].cast(pa.string()), t["postcode2"].cast(pa.string()), " ")),
            "type": _HOUSE_TYPE.take(t["type"].cast(pa.int32())),
            "is_new": t["is_new"].cast(pa.bool_()),
            "duration": _HOUSE_DURATION.take(t["duration"].cast(pa.int32())),
            **{c: t[c].cast(pa.string()) for c in
               ("addr1", "addr2", "street", "locality", "town", "district", "county")},
        }).sort_by([("date", "ascending"), ("postcode", "ascending")])
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        pq_.write_table(out_t, tmp, row_group_size=256_000)
        os.replace(tmp, target)
        rows += out_t.num_rows
        print(f"    uk_house_prices {y}: {out_t.num_rows:,} sales")
    if skipped:
        print(f"    uk_house_prices: {skipped} year(s) already present, skipped")
    return rows


def parse_klines(data: bytes) -> pa.Table:
    """Binance kline CSV → Arrow, sorted by open_time. Handles both timestamp units
    (ms until 2024, µs from 2025-01-01 on) and files with or without a header row."""
    has_header = not data[:1].isdigit()
    t = pacsv.read_csv(io.BytesIO(data),
                       read_options=pacsv.ReadOptions(column_names=_KLINE_COLS,
                                                      skip_rows=1 if has_header else 0),
                       convert_options=pacsv.ConvertOptions(column_types={
                           "open_time": pa.int64(), "close_time": pa.int64(), "trades": pa.int64(),
                           **{c: pa.float64() for c in _KLINE_COLS[1:6] + _KLINE_COLS[7:8] + _KLINE_COLS[9:11]}}))

    def ts(col: pa.ChunkedArray) -> pa.Array:
        unit = "us" if pc.max(col).as_py() > 10**14 else "ms"
        # close_time is "…999" / "…999999": truncating µs → ms is intended, not data loss.
        return pc.cast(col.cast(pa.timestamp(unit)), pa.timestamp("ms"), safe=False)

    return pa.table({"open_time": ts(t["open_time"]), "close_time": ts(t["close_time"]),
                     **{c: t[c] for c in ("open", "high", "low", "close", "volume", "quote_volume",
                                          "trades", "taker_buy_base_volume", "taker_buy_quote_volume")}}
                    ).sort_by("open_time")


def _kline_month(out: Path, symbol: str, y: int, m: int) -> int:
    target = out / "crypto_klines_1m" / f"symbol={symbol}" / f"year={y}" / f"{y}-{m:02d}.parquet"
    if target.exists():
        return 0  # already present
    url = f"{BINANCE}/data/spot/monthly/klines/{symbol}/1m/{symbol}-1m-{y}-{m:02d}.zip"
    try:
        raw = _download(url)
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):
            return -1  # symbol not listed yet / month not published
        raise
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        data = z.read(z.namelist()[0])
    t = parse_klines(data)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    pq.write_table(t, tmp)
    os.replace(tmp, target)
    return t.num_rows


def fetch_crypto_klines_1m(out: Path, opts: Options) -> int:
    from concurrent.futures import ThreadPoolExecutor

    end = opts.end or _last_full_month()
    start = opts.start or _shift(end, -11)  # default: the last 12 published months
    symbols = opts.symbols or DEFAULT_SYMBOLS
    jobs = [(s, y, m) for s in symbols for (y, m) in _months(start, end)]
    print(f"    crypto_klines_1m: {len(symbols)} symbol(s) x {len(jobs) // len(symbols)} month(s), "
          f"{start} -> {end}")
    rows = missing = present = 0
    with ThreadPoolExecutor(opts.workers) as pool:
        for n in pool.map(lambda j: _kline_month(out, *j), jobs):
            if n < 0:
                missing += 1
            elif n == 0:
                present += 1
            else:
                rows += n
    if present:
        print(f"    crypto_klines_1m: {present} symbol-month(s) already present, skipped")
    if missing:
        print(f"    crypto_klines_1m: {missing} symbol-month(s) not published, skipped")
    return rows


def fetch_forex_ticks(out: Path, opts: Options) -> int:
    s3 = _s3(CH_REGION)
    end = opts.end or "2022-08"             # last month in the bucket
    start = opts.start or end               # default: one month (2022-08: 144.5M ticks)
    root, rows, skipped = out / "forex_ticks", 0, 0
    for y, m in _months(start, end):
        tag = f"{y}{m:02d}"
        if any(root.glob(f"pair=*/year={y}/{tag}-*.parquet")):
            skipped += 1
            continue
        src = pads.dataset(f"{CH_BUCKET}/forex/parquet/year_month/{tag}-tick.parquet",
                           filesystem=s3, format="parquet")
        f = pads.field
        base, quote = f("base").cast(pa.string()), f("quote").cast(pa.string())
        scanner = src.scanner(columns={
            "datetime": f("datetime").cast(pa.timestamp("ms")),  # UTC, stored tz-naive
            "bid": f("bid"), "ask": f("ask"), "base": base, "quote": quote,
            "pair": pc.Expression._call("binary_join_element_wise", [base, quote, pads.scalar("")]),
            "year": pads.scalar(y).cast(pa.int16()),
        }, batch_size=1 << 20)
        count = [0]

        def write(staging: Path, scanner=scanner, tag=tag, count=count) -> None:
            def batches():
                for b in scanner.to_batches():
                    count[0] += b.num_rows
                    yield b
            pads.write_dataset(batches(), staging, schema=scanner.projected_schema, format="parquet",
                               partitioning=pads.partitioning(
                                   pa.schema([("pair", pa.string()), ("year", pa.int16())]), flavor="hive"),
                               basename_template=f"{tag}-{{i}}.parquet",
                               max_rows_per_group=1 << 20, existing_data_behavior="overwrite_or_ignore")

        _staged_write(root, write)
        rows += count[0]
        print(f"    forex_ticks {y}-{m:02d}: {count[0]:,} ticks")
    if skipped:
        print(f"    forex_ticks: {skipped} month(s) already present, skipped")
    return rows


# --------------------------------------------------------------------------- #
# Registry + entry point
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Spec:
    group: str
    size: str
    about: str
    fetch: Callable[[Path, Options], int]


def _core_fetch(name: str) -> Callable[[Path, Options], int]:
    def fetch(out: Path, opts: Options) -> int:
        table, partition_by = BUILDERS[name](_download(SOURCES[name].url))
        return _write(table, out, name, partition_by)
    return fetch


DATASETS: dict[str, Spec] = {
    "prices_daily": Spec("core", "30 MB csv", "daily OHLCV, 505 S&P 500 stocks, 2013-2018", _core_fetch("prices_daily")),
    "fx_daily": Spec("core", "8 MB csv", "FX vs USD, 22 currencies, 1971-today", _core_fetch("fx_daily")),
    "index_membership": Spec("core", "7 MB csv", "S&P 500 member snapshots, 1996-2025", _core_fetch("index_membership")),
    "index_membership_periods": Spec("core", "<1 MB", "S&P 500 membership start/end per ticker", _core_fetch("index_membership_periods")),
    "sp500_constituents": Spec("core", "<1 MB", "current S&P 500 members, sectors", _core_fetch("sp500_constituents")),
    "sp500_monthly": Spec("core", "<1 MB", "Shiller S&P level/dividend/earnings, 1871-", _core_fetch("sp500_monthly")),
    "vix_daily": Spec("core", "<1 MB", "VIX daily OHLC, 1990-today", _core_fetch("vix_daily")),
    "brent_daily": Spec("core", "<1 MB", "Brent crude daily, 1987-today", _core_fetch("brent_daily")),
    "uk_house_prices": Spec("heavy", "0.7 GB, ~30M rows (all years)", "UK property sales, 1995-2023 (--from/--to years)", fetch_uk_house_prices),
    "crypto_klines_1m": Spec("heavy", "~44k rows per symbol-month", "Binance 1m candles (--symbols, --from/--to; default 10 symbols x last 12 months)", fetch_crypto_klines_1m),
    "forex_ticks": Spec("heavy", "~0.6-1.2 GB, 70-145M rows per month", "FX bid/ask ticks, 66 pairs, 2000-05..2022-08 (--from/--to; default 2022-08)", fetch_forex_ticks),
}
GROUPS = {
    "core": [n for n, s in DATASETS.items() if s.group == "core"],
    "heavy": [n for n, s in DATASETS.items() if s.group == "heavy"],
    "all": list(DATASETS),
}


def resolve(names: str) -> list[str]:
    out: list[str] = []
    for n in (x.strip() for x in names.split(",") if x.strip()):
        for name in GROUPS.get(n, [n]):
            if name not in DATASETS:
                raise ValueError(f"unknown dataset {name!r}; choose from {list(GROUPS)} or {list(DATASETS)}")
            if name not in out:
                out.append(name)
    return out


def describe() -> str:
    lines = [f"{'dataset':<26} {'group':<6} {'size':<32} about"]
    lines += [f"{n:<26} {s.group:<6} {s.size:<32} {s.about}" for n, s in DATASETS.items()]
    lines += ["", "groups: " + ", ".join(f"{g} = {', '.join(v) if g != 'all' else 'everything'}"
                                          for g, v in GROUPS.items())]
    return "\n".join(lines)


def _sources_md(out: Path) -> str:
    present = [n for n in DATASETS if (out / n).is_dir()]
    lines = ["# Open datasets for kob", "",
             "Written by `kob --get-data`. Check each source's licence before redistributing.", "",
             "| dataset | source | licence | url |", "|---|---|---|---|"]
    lines += [f"| `{n}` | {SOURCES[n].origin} | {SOURCES[n].licence} | {SOURCES[n].url} |" for n in present]
    return "\n".join(lines) + "\n"


def get_data(names: str, out: Path, opts: Options | None = None) -> None:
    """Download ``names`` (datasets and/or groups, comma-separated) into the data root ``out``."""
    opts = opts or Options()
    selected = resolve(names)
    out.mkdir(parents=True, exist_ok=True)
    t_all = time.perf_counter()
    print(f"  kob --get-data → {out.resolve()}")
    for name in selected:
        t0 = time.perf_counter()
        rows = DATASETS[name].fetch(out, opts)
        print(f"  {name:<26} {rows:>13,} rows   {time.perf_counter() - t0:7.1f}s")
    (out / "SOURCES.md").write_text(_sources_md(out))
    print(f"\nDone in {time.perf_counter() - t_all:.1f}s. Serve it:  uv run kob --mode rest"
          + ("" if out.resolve() == Path("data").resolve() else f"   (with KOB_DATA_ROOT={out})"))
