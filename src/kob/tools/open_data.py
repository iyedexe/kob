"""Real, open financial datasets for trying kob (``kob-open-data``) — no extra dependencies.

Downloads public CSVs (all hosted on GitHub) and writes them as Hive-partitioned Parquet,
in the layout kob is designed for::

    <out>/prices_daily/year=2017/part-0.parquet           daily OHLCV, 505 S&P 500 stocks, 2013-2018
    <out>/fx_daily/year=2024/part-0.parquet               FRED H.10 FX vs USD, 22 currencies, 1971-today
    <out>/index_membership/index=SPX/year=2024/...        S&P 500 member snapshots, 1996-2025 (one per change)
    <out>/index_membership_periods/part-0.parquet         ticker, start_date, end_date (null = still in)
    <out>/sp500_constituents/part-0.parquet               today's members: sector, date added, CIK
    <out>/sp500_monthly/part-0.parquet                    Shiller: index level, dividend, earnings, CPI, 1871-
    <out>/vix_daily/part-0.parquet                        CBOE VIX OHLC, 1990-today
    <out>/brent_daily/part-0.parquet                      Brent crude spot, 1987-today

Sources and licences are written to ``<out>/SOURCES.md``. Files are sorted by the columns
you would range on (e.g. ``ticker, date``) so Parquet row-group statistics prune well.

    kob-open-data                         # all datasets → ./data-open
    kob-open-data --only prices_daily,fx_daily --out /tmp/kob-open
    KOB_DATA_ROOT=data-open kob --mode rest
"""

from __future__ import annotations

import argparse
import io
import shutil
import time
import urllib.request
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
def _download(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "kob-open-data"})
    with urllib.request.urlopen(req, timeout=300) as resp:
        return resp.read()


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


def _sources_md(names: list[str]) -> str:
    lines = ["# Open datasets for kob", "",
             "Generated by `kob-open-data`. Check each source's licence before redistributing.", "",
             "| dataset | source | licence | url |", "|---|---|---|---|"]
    for n in names:
        s = SOURCES[n]
        lines.append(f"| `{n}` | {s.origin} | {s.licence} | {s.url} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Download open financial datasets as Parquet for kob.")
    p.add_argument("--out", default="data-open", help="Output data root (default: ./data-open).")
    p.add_argument("--only", help=f"Comma-separated subset of: {', '.join(BUILDERS)}.")
    args = p.parse_args(argv)

    names = [n.strip() for n in args.only.split(",")] if args.only else list(BUILDERS)
    bad = [n for n in names if n not in BUILDERS]
    if bad:
        p.error(f"unknown dataset(s) {bad}; choose from {list(BUILDERS)}")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    t_all = time.perf_counter()
    for name in names:
        t0 = time.perf_counter()
        raw = _download(SOURCES[name].url)
        table, partition_by = BUILDERS[name](raw)
        rows = _write(table, out, name, partition_by)
        print(f"  {name:<26} {rows:>10,} rows   {len(raw) / 1e6:6.1f} MB csv   "
              f"{time.perf_counter() - t0:5.1f}s")
    (out / "SOURCES.md").write_text(_sources_md(names))
    print(f"\nDone in {time.perf_counter() - t_all:.1f}s → {out.resolve()}\n"
          f"Serve it:  KOB_DATA_ROOT={out} uv run kob --mode rest")


if __name__ == "__main__":
    main()
