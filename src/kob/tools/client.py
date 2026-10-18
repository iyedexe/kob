"""Reference Python client (install extra: ``kob[client]``).

Three transports:

* **Flight** — the product's fast, default path (``kob``, on :8815);
* **REST** — ``kob --mode rest``: Arrow IPC over plain HTTP (:8000), with per-dataset
  Swagger and a manual rescan (:func:`reload_rest`);
* **HTTP-Arrow** — the Arrow-over-HTTP demo server (``kob-demo-arrow``, on :8001).

Slicing API (Flight or REST, picked from the URL)::

    from kob.tools.client import KobClient

    kob = KobClient("http://127.0.0.1:8000")          # or KobClient("grpc://127.0.0.1:8815")
    om = kob["optionmetrics"]
    tbl = om.query(om.date["2023-03-01":"2023-03-31"],  # inclusive at BOTH ends
                   om.strike[90:],                      # open end
                   underlying=["AAPL", "MSFT"], cp_flag="C", limit=1000)
    kob.reload()                                        # REST: rescan Parquet on the server

Slices work on sliceable columns (dates, timestamps, numbers) and mean start/end: unlike
Python lists, ``col[a:b]`` *includes* ``b``.

Low-level functions (take a :class:`~kob.core.contract.QueryRequest`)::

    from kob.tools.client import query_flight, query_rest, query_http, reload_rest
    tbl = query_flight("grpc://127.0.0.1:8815", QueryRequest(dataset="events"))
    tbl = query_rest("http://127.0.0.1:8000", QueryRequest(dataset="events", limit=10))

CLI::

    kob-client --dataset events --filter 'region:=:eu' --filter 'amount:>=:100'
    kob-client --transport rest --dataset events --columns user_id,amount --limit 10
    kob-client --transport rest --reload --dataset events      # rescan first, then query
    kob-client --transport rest --dataset prices --range date=2024-01-01..2024-03-31
    kob-client --dataset dividends --range ex_date=2024-01-01..2024-03-31 --range pay_date=..2024-06-30
    kob-client --transport http --dataset events --columns user_id,amount --limit 10
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import requests

from ..core.contract import Filter, QueryRequest


def query_flight(location: str, req: QueryRequest) -> pa.Table:
    """Discover the flight then DoGet the Arrow stream (the fast, recommended path)."""
    import pyarrow.flight as flight  # lazy: not needed for HTTP-only use

    client = flight.connect(location)
    descriptor = flight.FlightDescriptor.for_command(json.dumps(req.to_dict()).encode())
    info = client.get_flight_info(descriptor)
    return client.do_get(info.endpoints[0].ticket).read_all()


def query_http(base_url: str, req: QueryRequest, compression: str = "zstd") -> pa.Table:
    """POST to the Arrow-over-HTTP demo server and read back the Arrow IPC stream."""
    resp = requests.post(
        f"{base_url.rstrip('/')}/query",
        params={"compression": compression},
        json=req.to_dict(),
        stream=True,
        timeout=600,
    )
    resp.raise_for_status()
    resp.raw.decode_content = True
    return pa.ipc.open_stream(resp.raw).read_all()


def _read_arrow(resp: requests.Response) -> pa.Table:
    if not resp.ok:
        try:
            detail = resp.json().get("detail", resp.text)
        except ValueError:
            detail = resp.text
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}: {detail}", response=resp)
    resp.raw.decode_content = True
    return pa.ipc.open_stream(resp.raw).read_all()


def query_rest(base_url: str, req: QueryRequest, compression: str = "zstd",
               timeout: float = 600) -> pa.Table:
    """POST the JSON contract to ``kob --mode rest`` and read back the Arrow IPC stream.

    ``compression`` is the Arrow IPC buffer codec the server should use
    (``zstd`` / ``lz4`` / ``none``); pyarrow decodes any of them transparently.
    """
    body = req.to_dict()
    body.pop("dataset")  # the dataset is in the path
    resp = requests.post(
        f"{base_url.rstrip('/')}/data/{req.dataset}",
        params={"format": "arrow", "compression": compression},
        json=body,
        stream=True,
        timeout=timeout,
    )
    return _read_arrow(resp)


def reload_rest(base_url: str, timeout: float = 600) -> dict:
    """Ask ``kob --mode rest`` to rescan its data root (same as the Swagger Reload button)."""
    resp = requests.post(f"{base_url.rstrip('/')}/reload", timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def list_datasets_http(base_url: str) -> dict:
    """List datasets from the Swagger control plane / REST server (default :8000)."""
    return requests.get(f"{base_url.rstrip('/')}/datasets", timeout=30).json()


# --------------------------------------------------------------------------- #
# Slicing API: ds.column[start:end]
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Range:
    """An inclusive ``start``..``end`` range on one column (either side may be None)."""
    column: str
    start: Any = None
    end: Any = None


class Column:
    """A named column; slice it to get a :class:`Range`: ``col["2024-01-01":"2024-03-31"]``.

    Slices are **inclusive at both ends** (they mean start/end, not Python's half-open
    ``[a, b)``) and take no step. Works on sliceable columns: dates, timestamps, numbers.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __getitem__(self, key: slice) -> Range:
        if not isinstance(key, slice):
            raise TypeError(f"use a slice on a column: {self.name}[start:end], got {key!r}")
        if key.step is not None:
            raise ValueError(f"kob slices take no step: {self.name}[start:end]")
        return Range(self.name, key.start, key.stop)

    def __repr__(self) -> str:
        return f"Column({self.name!r})"


class Dataset:
    """A dataset on a kob server. Columns are attributes (``ds.date``) or items (``ds["date"]``).

    ::

        om = KobClient("http://127.0.0.1:8000")["optionmetrics"]
        om.query(om.date["2023-03-01":"2023-03-31"],     # inclusive date range
                 om.expiry[:"2023-04-30"],                # open start
                 om.strike[90:110],                       # numeric range
                 underlying=["AAPL", "MSFT"],             # IN
                 cp_flag="C",                             # =
                 columns=["date", "strike", "mid"], limit=1000)
    """

    def __init__(self, client: "KobClient", name: str) -> None:
        self._client = client
        self._name = name

    def __getattr__(self, column: str) -> Column:
        if column.startswith("_"):
            raise AttributeError(column)
        return Column(column)

    def __getitem__(self, column: str) -> Column:
        return Column(column)

    def __repr__(self) -> str:
        return f"Dataset({self._name!r})"

    def request(self, *conditions: Range | Filter, columns: list[str] | None = None,
                limit: int | None = None, **where: Any) -> QueryRequest:
        """Build the :class:`QueryRequest` that :meth:`query` would send.

        ``conditions`` are column slices (``ds.date["a":"b"]``) or :class:`Filter` objects.
        Keyword arguments filter by column: a list/tuple → ``in``, a ``slice`` → range,
        anything else → ``=``.
        """
        filters: list[Filter] = []
        ranges: dict[str, dict[str, Any]] = {}

        def add_range(col: str, start: Any, end: Any) -> None:
            if col in ranges:
                raise ValueError(f"two ranges on {col!r}; combine them into one slice")
            ranges[col] = {"start": start, "end": end}

        for cond in conditions:
            if isinstance(cond, Range):
                add_range(cond.column, cond.start, cond.end)
            elif isinstance(cond, Filter):
                filters.append(cond)
            else:
                raise TypeError(f"expected a column slice or a Filter, got {cond!r}")
        for col, value in where.items():
            if isinstance(value, slice):
                r = Column(col)[value]  # validates: no step
                add_range(r.column, r.start, r.end)
            elif isinstance(value, (list, tuple, set)):
                filters.append(Filter(col, "in", list(value)))
            else:
                filters.append(Filter(col, "=", value))
        return QueryRequest(dataset=self._name, columns=columns, filters=filters, limit=limit,
                            ranges=ranges)

    def query(self, *conditions: Range | Filter, columns: list[str] | None = None,
              limit: int | None = None, **where: Any) -> pa.Table:
        """Run a query and return an Arrow table. See :meth:`request` for the arguments."""
        return self._client.query(self.request(*conditions, columns=columns, limit=limit, **where))


class KobClient:
    """One client for both transports, picked from the URL scheme.

    ``KobClient("http://host:8000")`` → ``kob --mode rest`` (Arrow over HTTP);
    ``KobClient("grpc://host:8815")`` → ``kob`` (Arrow Flight).
    """

    def __init__(self, url: str, compression: str = "zstd", timeout: float = 600) -> None:
        self.url = url.rstrip("/")
        self.compression = compression
        self.timeout = timeout
        self.transport = "flight" if self.url.startswith(("grpc://", "grpc+tls://")) else "rest"

    def __getitem__(self, dataset: str) -> Dataset:
        return Dataset(self, dataset)

    def dataset(self, name: str) -> Dataset:
        return Dataset(self, name)

    def query(self, req: QueryRequest) -> pa.Table:
        if self.transport == "flight":
            return query_flight(self.url, req)
        return query_rest(self.url, req, compression=self.compression, timeout=self.timeout)

    def datasets(self) -> dict:
        """REST only: the server's datasets (``GET /datasets``)."""
        self._rest_only("datasets")
        return list_datasets_http(self.url)

    def reload(self) -> dict:
        """REST only: rescan the server's Parquet (the Swagger Reload button)."""
        self._rest_only("reload")
        return reload_rest(self.url, timeout=self.timeout)

    def _rest_only(self, what: str) -> None:
        if self.transport != "rest":
            raise ValueError(f"{what}() needs a REST URL (http://...), not {self.url}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _coerce(val: str) -> object:
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val


def _req_from_args(args: argparse.Namespace) -> QueryRequest:
    filters: list[Filter] = []
    for raw in args.filter or []:
        col, op, val = raw.split(":", 2)
        filters.append(Filter(col, op, _coerce(val)))
    columns = [c.strip() for c in args.columns.split(",")] if args.columns else None
    ranges: dict[str, dict[str, str | None]] = {}
    for raw in args.range or []:
        col, _, span = raw.partition("=")
        start, sep, end = span.partition("..")
        if not col or not sep:
            raise SystemExit(f"--range must be 'column=start..end' (either side optional), got {raw!r}")
        ranges[col] = {"start": start or None, "end": end or None}
    return QueryRequest(dataset=args.dataset, columns=columns, filters=filters, limit=args.limit,
                        ranges=ranges)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Query a kob server.")
    p.add_argument("--transport", choices=["flight", "rest", "http"], default="flight",
                   help="flight = the fast default (:8815); rest = `kob --mode rest` (:8000); "
                        "http = the Arrow-over-HTTP demo (:8001).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int)
    p.add_argument("--dataset", required=True)
    p.add_argument("--columns", help="Comma-separated projection.")
    p.add_argument("--filter", action="append",
                   help="Filter 'column:op:value' (repeatable), e.g. 'region:=:eu' or 'amount:>=:100'.")
    p.add_argument("--limit", type=int)
    p.add_argument("--range", action="append",
                   help="Inclusive range on a date/timestamp/numeric column, 'column=start..end' "
                        "(repeatable; either side optional), e.g. 'ex_date=2024-01-01..2024-03-31', "
                        "'pay_date=..2024-06-30', 'strike=90..110'.")
    p.add_argument("--compression", default="zstd", choices=["zstd", "lz4", "none"])
    p.add_argument("--head", type=int, default=5, help="Rows to preview.")
    p.add_argument("--reload", action="store_true",
                   help="rest only: rescan the server's Parquet before querying.")
    args = p.parse_args(argv)

    req = _req_from_args(args)
    if args.reload:
        if args.transport != "rest":
            p.error("--reload only applies to --transport rest")
        info = reload_rest(f"http://{args.host}:{args.port or 8000}")
        print(f"reloaded: {len(info['datasets'])} dataset(s) in {info['elapsed_ms']} ms")
    t0 = time.perf_counter()
    if args.transport == "flight":
        table = query_flight(f"grpc://{args.host}:{args.port or 8815}", req)
    elif args.transport == "rest":
        table = query_rest(f"http://{args.host}:{args.port or 8000}", req, compression=args.compression)
    else:
        table = query_http(f"http://{args.host}:{args.port or 8001}", req, compression=args.compression)
    dt = time.perf_counter() - t0

    print(f"transport={args.transport}  rows={table.num_rows:,}  cols={table.num_columns}  "
          f"in-memory={table.nbytes / 1e6:.1f} MB  wall={dt * 1e3:.1f} ms  "
          f"({table.num_rows / dt / 1e6:.2f} M rows/s)")
    if args.head:
        try:
            print(table.slice(0, args.head).to_pandas().to_string(index=False))
        except ModuleNotFoundError:  # pandas not installed — raw Arrow preview
            print(table.slice(0, args.head))


if __name__ == "__main__":
    main()
