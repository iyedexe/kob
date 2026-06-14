"""Excel bridge via Python — the most robust local option.

Excel can't speak Arrow, but Python can: this pulls data over the *fast* Arrow Flight
path (into a pandas DataFrame) and then hands it to Excel.

Run it with the project env so `kob` is importable::

    uv run --extra client python clients/excel/xlwings_client.py --underlying AAPL --year 2023

Behaviour:
  * If ``xlwings`` + Excel are available  -> writes into the active workbook (live).
  * elif ``openpyxl`` is available        -> saves an .xlsx next to this script.
  * else                                  -> saves a .csv (always works).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from kob.core.contract import Filter, QueryRequest
from kob.tools.client import query_flight


def fetch_dataframe(flight_location: str, args: argparse.Namespace):
    filters: list[Filter] = []
    if args.underlying:
        filters.append(Filter("underlying", "=", args.underlying))
    if args.year:
        filters.append(Filter("year", "=", args.year))
    if args.cp:
        filters.append(Filter("cp_flag", "=", args.cp))
    cols = [c.strip() for c in args.columns.split(",")] if args.columns else None
    req = QueryRequest(dataset=args.dataset, columns=cols, filters=filters, limit=args.limit)
    table = query_flight(flight_location, req)  # Arrow over Flight (the fast path)
    return table.to_pandas()                     # zero-copy where dtypes allow


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Pull server data into Excel via Python.")
    p.add_argument("--flight", default="grpc://127.0.0.1:8815", help="kob Flight location.")
    p.add_argument("--dataset", default="optionmetrics")
    p.add_argument("--underlying", default="AAPL")
    p.add_argument("--year", type=int, default=2023)
    p.add_argument("--cp", default="C")
    p.add_argument("--columns", default="date,strike,cp_flag,impl_vol,delta,und_price")
    p.add_argument("--limit", type=int, default=20000)
    args = p.parse_args(argv)

    df = fetch_dataframe(args.flight, args)
    print(f"Fetched {len(df):,} rows x {df.shape[1]} cols")

    # 1) Live Excel via xlwings
    try:
        import xlwings as xw  # type: ignore

        book = xw.Book()  # opens a new workbook in Excel
        book.sheets[0]["A1"].value = df
        print("Wrote DataFrame into the active Excel workbook via xlwings.")
        return
    except Exception:
        pass

    # 2) Save .xlsx via openpyxl
    out_xlsx = Path(__file__).with_name("excel_export.xlsx")
    try:
        df.to_excel(out_xlsx, index=False)
        print(f"xlwings/Excel not available — wrote {out_xlsx}")
        return
    except Exception:
        pass

    # 3) Fallback: CSV (always works)
    out_csv = Path(__file__).with_name("excel_export.csv")
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv} (install openpyxl or xlwings for native Excel output)")


if __name__ == "__main__":
    main()
