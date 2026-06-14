"""Synthetic dataset generator.

Real FactSet GeoRev and OptionMetrics (IvyDB) are proprietary, licensed datasets and
cannot be redistributed. This module fabricates data with the *same schema and shape*
so the server and benchmarks are realistic and the result is freely shareable:

* ``georev``        — company x fiscal period x region/country x segment revenue.
* ``optionmetrics`` — per underlying & date, full strike/expiry option chains with
                      Black-Scholes implied vol and greeks.

Everything is vectorised with NumPy and written one Hive partition at a time, so even
the multi-GB ``--scale large`` preset stays within a small memory budget.

Usage::

    uv run --extra gen kob-gen --scale small
    uv run --extra gen kob-gen --scale large --out data
    uv run --extra gen kob-gen --datasets optionmetrics --underlyings 80 --years 2020 2024
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.stats import norm

from .catalog import DATA_ROOT

# --------------------------------------------------------------------------- #
# Reference dimensions
# --------------------------------------------------------------------------- #
REGIONS = {
    "North America": ["US", "CA", "MX"],
    "EMEA": ["GB", "DE", "FR", "NL", "CH"],
    "APAC": ["JP", "CN", "AU", "SG", "IN"],
    "Latin America": ["BR", "AR", "CL"],
}
REGION_NAMES = list(REGIONS.keys())
SEGMENTS = ["Products", "Services", "Subscriptions", "Licensing", "Other"]
SECTORS = [
    "Technology", "Financials", "Health Care", "Industrials", "Energy",
    "Consumer Discretionary", "Consumer Staples", "Materials", "Utilities",
]
TICKER_POOL = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "BRKB", "JPM", "V",
    "UNH", "HD", "PG", "MA", "XOM", "JNJ", "COST", "ABBV", "WMT", "CRM",
    "BAC", "KO", "PEP", "ADBE", "NFLX", "AMD", "CSCO", "TMO", "ACN", "MCD",
    "ABT", "DHR", "INTC", "QCOM", "TXN", "NKE", "WFC", "PM", "ORCL", "IBM",
    "GE", "CAT", "BA", "GS", "MS", "AXP", "BLK", "C", "SPGI", "NOW",
    "UBER", "SHOP", "PLTR", "SNOW", "COIN", "SQ", "PYPL", "DIS", "T", "VZ",
    "F", "GM", "DAL", "UAL", "AAL", "MAR", "BKNG", "ABNB", "DASH", "RBLX",
]

# --------------------------------------------------------------------------- #
# Scale presets
# --------------------------------------------------------------------------- #
PRESETS = {
    "small": {
        "georev": {"n_companies": 1200, "years": [2022, 2023]},
        "optionmetrics": {"n_underlyings": 6, "years": [2023], "n_expiries": 6, "n_strikes": 13},
    },
    "medium": {
        "georev": {"n_companies": 4000, "years": [2020, 2021, 2022, 2023]},
        "optionmetrics": {"n_underlyings": 20, "years": [2022, 2023], "n_expiries": 8, "n_strikes": 21},
    },
    "large": {
        "georev": {"n_companies": 8000, "years": [2019, 2020, 2021, 2022, 2023, 2024]},
        "optionmetrics": {"n_underlyings": 60, "years": [2021, 2022, 2023, 2024], "n_expiries": 12, "n_strikes": 31},
    },
}


def _du(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*.parquet"))


def _fmt_bytes(n: int) -> str:
    x = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if x < 1024 or unit == "TB":
            return f"{x:.1f} {unit}"
        x /= 1024
    return f"{x:.1f} TB"


# --------------------------------------------------------------------------- #
# GeoRev
# --------------------------------------------------------------------------- #
def _make_companies(n: int, rng: np.random.Generator) -> dict:
    ids = np.array([f"C{ i:06d}" for i in range(n)])
    tickers = np.array([
        TICKER_POOL[i] if i < len(TICKER_POOL)
        else "".join(rng.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), size=4))
        for i in range(n)
    ])
    names = np.array([f"{t} Corp" for t in tickers])
    sectors = rng.choice(SECTORS, size=n)
    total_rev = rng.lognormal(mean=21.0, sigma=1.4, size=n)  # ~ hundreds of millions to billions USD
    region_w = rng.dirichlet(np.ones(len(REGION_NAMES)) * 1.3, size=n)
    seg_w = rng.dirichlet(np.ones(len(SEGMENTS)) * 1.2, size=n)
    return dict(ids=ids, tickers=tickers, names=names, sectors=sectors,
                total_rev=total_rev, region_w=region_w, seg_w=seg_w)


def generate_georev(out_root: Path, n_companies: int, years: list[int], rng: np.random.Generator) -> dict:
    ds_root = out_root / "georev"
    if ds_root.exists():
        shutil.rmtree(ds_root)
    comp = _make_companies(n_companies, rng)
    n_reg, n_seg = len(REGION_NAMES), len(SEGMENTS)
    # Map each (region, segment) pair to a representative country (first of region).
    region_country = np.array([REGIONS[r][0] for r in REGION_NAMES])

    total_rows = 0
    for year in years:
        # Cross product: company x quarter x region x segment
        ci, qi, ri, si = np.meshgrid(
            np.arange(n_companies), np.arange(4), np.arange(n_reg), np.arange(n_seg), indexing="ij"
        )
        ci, qi, ri, si = ci.ravel(), qi.ravel(), ri.ravel(), si.ravel()

        season = np.array([0.94, 0.99, 1.01, 1.06])  # quarterly seasonality, sums ~4
        growth = rng.normal(1.0 + 0.04 * (year - years[0]), 0.05, size=n_companies)
        frac = comp["region_w"][ci, ri] * comp["seg_w"][ci, si]
        quarter_total = comp["total_rev"][ci] * growth[ci] / 4.0
        revenue = quarter_total * frac * season[qi]

        period_end = pd.to_datetime(
            {"year": np.full(ci.shape, year), "month": (qi + 1) * 3,
             "day": np.where((qi + 1) * 3 == 6, 30, np.where((qi + 1) * 3 == 9, 30, np.where((qi + 1) * 3 == 12, 31, 31)))}
        ).values.astype("datetime64[D]")

        table = pa.table({
            "company_id": comp["ids"][ci],
            "ticker": comp["tickers"][ci],
            "company_name": comp["names"][ci],
            "sector": comp["sectors"][ci],
            "fiscal_year": np.full(ci.shape, year, dtype=np.int32),
            "fiscal_quarter": (qi + 1).astype(np.int8),
            "period_end": pa.array(period_end, type=pa.date32()),
            "region": np.array(REGION_NAMES)[ri],
            "country": region_country[ri],
            "segment": np.array(SEGMENTS)[si],
            "revenue_usd": np.round(revenue, 2),
            "revenue_pct_of_total": np.round(frac, 6),
            "yoy_growth": np.round(rng.normal(0.06, 0.12, size=ci.shape), 4),
            "is_estimate": rng.random(ci.shape) < 0.25,
        })
        part_dir = ds_root / f"year={year}"
        part_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, part_dir / "part-0.parquet", compression="zstd")
        total_rows += table.num_rows
        print(f"  georev year={year}: {table.num_rows:,} rows")

    size = _du(ds_root)
    return {"rows": total_rows, "bytes": size, "partitions": len(years)}


# --------------------------------------------------------------------------- #
# OptionMetrics (IvyDB-style EOD chains)
# --------------------------------------------------------------------------- #
def _black_scholes(S, K, T, vol, r, cp_is_call):
    """Vectorised Black-Scholes price + greeks (q=0). Arrays broadcast together."""
    sqrtT = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * vol * vol) * T) / (vol * sqrtT)
    d2 = d1 - vol * sqrtT
    Nd1, Nd2 = norm.cdf(d1), norm.cdf(d2)
    nd1 = norm.pdf(d1)
    disc = np.exp(-r * T)

    call = S * Nd1 - K * disc * Nd2
    put = K * disc * norm.cdf(-d2) - S * norm.cdf(-d1)
    price = np.where(cp_is_call, call, put)

    delta = np.where(cp_is_call, Nd1, Nd1 - 1.0)
    gamma = nd1 / (S * vol * sqrtT)
    vega = S * nd1 * sqrtT / 100.0  # per 1 vol-point
    theta_call = (-S * nd1 * vol / (2 * sqrtT) - r * K * disc * Nd2) / 365.0
    theta_put = (-S * nd1 * vol / (2 * sqrtT) + r * K * disc * norm.cdf(-d2)) / 365.0
    theta = np.where(cp_is_call, theta_call, theta_put)
    rho_call = K * T * disc * Nd2 / 100.0
    rho_put = -K * T * disc * norm.cdf(-d2) / 100.0
    rho = np.where(cp_is_call, rho_call, rho_put)
    return price, delta, gamma, vega, theta, rho


def generate_optionmetrics(
    out_root: Path, n_underlyings: int, years: list[int],
    n_expiries: int, n_strikes: int, rng: np.random.Generator,
) -> dict:
    ds_root = out_root / "optionmetrics"
    if ds_root.exists():
        shutil.rmtree(ds_root)

    underlyings = [
        TICKER_POOL[i] if i < len(TICKER_POOL)
        else "".join(rng.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), size=4))
        for i in range(n_underlyings)
    ]
    moneyness = np.linspace(0.80, 1.20, n_strikes)  # strike / spot
    r = 0.04
    total_rows = 0
    n_parts = 0

    for u_idx, sym in enumerate(underlyings):
        start_price = float(rng.uniform(25, 500))
        base_vol = float(rng.uniform(0.18, 0.55))
        for year in years:
            days = pd.bdate_range(f"{year}-01-01", f"{year}-12-31").values.astype("datetime64[D]")
            n_days = len(days)
            # Daily GBM spot path for the year.
            drift, dvol = 0.06 / 252, base_vol / np.sqrt(252)
            shocks = rng.normal(drift - 0.5 * dvol**2, dvol, size=n_days)
            spot = start_price * np.exp(np.cumsum(shocks))
            start_price = float(spot[-1])  # carry into next year

            di, ei, ki, ci = np.meshgrid(
                np.arange(n_days), np.arange(n_expiries), np.arange(n_strikes), np.arange(2), indexing="ij"
            )
            di, ei, ki, ci = di.ravel(), ei.ravel(), ki.ravel(), ci.ravel()

            S = spot[di]
            dte = (ei + 1) * 30
            T = dte / 365.0
            mny = moneyness[ki]
            K = np.round(S * mny, 2)
            cp_is_call = ci == 0

            # Vol smile/skew + term structure + idiosyncratic noise.
            vol = (base_vol
                   + 0.12 * (1.0 - mny)          # downside skew
                   + 0.03 * np.sqrt(T)           # term structure
                   + rng.normal(0, 0.01, size=mny.shape))
            vol = np.clip(vol, 0.05, 2.5)

            price, delta, gamma, vega, theta, rho = _black_scholes(S, K, T, vol, r, cp_is_call)
            price = np.clip(price, 0.01, None)
            spread = np.clip(price * 0.01, 0.05, None)
            bid = np.round(np.clip(price - spread / 2, 0.0, None), 2)
            ask = np.round(price + spread / 2, 2)
            mid = np.round(price, 2)
            last = np.round(np.clip(price + rng.normal(0, spread, size=price.shape), 0.0, None), 2)
            volume = rng.poisson(np.clip(2000 * np.exp(-((mny - 1.0) ** 2) / 0.02), 1, None)).astype(np.int64)
            oi = (volume * rng.integers(2, 12, size=volume.shape)).astype(np.int64)

            date = days[di]
            expiry = (date.astype("datetime64[D]") + dte.astype("timedelta64[D]"))

            table = pa.table({
                "date": pa.array(date, type=pa.date32()),
                "expiry": pa.array(expiry, type=pa.date32()),
                "cp_flag": np.where(cp_is_call, "C", "P"),
                "strike": K,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "last_price": last,
                "volume": volume,
                "open_interest": oi,
                "impl_vol": np.round(vol, 6),
                "delta": np.round(delta, 6),
                "gamma": np.round(gamma, 8),
                "vega": np.round(vega, 6),
                "theta": np.round(theta, 6),
                "rho": np.round(rho, 6),
                "und_price": np.round(S, 2),
                "days_to_exp": dte.astype(np.int32),
            })
            part_dir = ds_root / f"underlying={sym}" / f"year={year}"
            part_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, part_dir / "part-0.parquet", compression="zstd")
            total_rows += table.num_rows
            n_parts += 1
        print(f"  optionmetrics {sym}: {len(years)} year(s) done "
              f"(running total {total_rows:,} rows)")

    size = _du(ds_root)
    return {"rows": total_rows, "bytes": size, "partitions": n_parts}


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate synthetic GeoRev / OptionMetrics Parquet datasets.")
    p.add_argument("--scale", choices=list(PRESETS), default="small", help="Size preset (default: small).")
    p.add_argument("--datasets", default="georev,optionmetrics",
                   help="Comma-separated subset to generate.")
    p.add_argument("--out", default=str(DATA_ROOT), help="Output data root.")
    p.add_argument("--seed", type=int, default=7, help="RNG seed for reproducibility.")
    # Granular overrides (optional; default to preset values).
    p.add_argument("--n-companies", type=int)
    p.add_argument("--underlyings", type=int)
    p.add_argument("--years", type=int, nargs="+")
    p.add_argument("--n-expiries", type=int)
    p.add_argument("--n-strikes", type=int)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    preset = PRESETS[args.scale]
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    wanted = {d.strip() for d in args.datasets.split(",") if d.strip()}

    manifest: dict = {"scale": args.scale, "seed": args.seed, "datasets": {}}
    t0 = time.time()

    if "georev" in wanted:
        cfg = preset["georev"]
        print(f"Generating georev (scale={args.scale}) ...")
        stats = generate_georev(
            out_root,
            n_companies=args.n_companies or cfg["n_companies"],
            years=args.years or cfg["years"],
            rng=rng,
        )
        print(f"  -> {stats['rows']:,} rows, {_fmt_bytes(stats['bytes'])}")
        manifest["datasets"]["georev"] = stats

    if "optionmetrics" in wanted:
        cfg = preset["optionmetrics"]
        print(f"Generating optionmetrics (scale={args.scale}) ...")
        stats = generate_optionmetrics(
            out_root,
            n_underlyings=args.underlyings or cfg["n_underlyings"],
            years=args.years or cfg["years"],
            n_expiries=args.n_expiries or cfg["n_expiries"],
            n_strikes=args.n_strikes or cfg["n_strikes"],
            rng=rng,
        )
        print(f"  -> {stats['rows']:,} rows, {_fmt_bytes(stats['bytes'])}")
        manifest["datasets"]["optionmetrics"] = stats

    total_bytes = sum(d["bytes"] for d in manifest["datasets"].values())
    total_rows = sum(d["rows"] for d in manifest["datasets"].values())
    manifest["total_rows"] = total_rows
    manifest["total_bytes"] = total_bytes
    manifest["elapsed_sec"] = round(time.time() - t0, 1)
    (out_root / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))

    print(f"\nDone in {manifest['elapsed_sec']}s — {total_rows:,} rows, "
          f"{_fmt_bytes(total_bytes)} total. Manifest: {out_root / 'MANIFEST.json'}")


if __name__ == "__main__":
    main()
