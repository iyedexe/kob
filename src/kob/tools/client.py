"""Reference Python client (install extra: ``kob[client]``).

Three transports:

* **Flight** — the product's fast, default path (``kob``, on :8815);
* **REST** — ``kob --mode rest``: Arrow IPC over plain HTTP (:8000), with per-dataset
  Swagger and a manual rescan (:func:`reload_rest`);
* **HTTP-Arrow** — the Arrow-over-HTTP demo server (``kob-demo-arrow``, on :8001).

Importable API::

    from kob.tools.client import query_flight, query_rest, query_http, reload_rest
    from kob.core.contract import QueryRequest, Filter

    tbl = query_flight("grpc://127.0.0.1:8815",
                       QueryRequest(dataset="events",
                                    filters=[Filter("region", "=", "eu")]))
    tbl = query_rest("http://127.0.0.1:8000", QueryRequest(dataset="events", limit=10))
    tbl = query_rest_params("http://127.0.0.1:8000", "events", region=["eu", "us"], limit=10)
    reload_rest("http://127.0.0.1:8000")      # rescan Parquet on the server

CLI::

    kob-client --dataset events --filter 'region:=:eu' --filter 'amount:>=:100'
    kob-client --transport rest --dataset events --columns user_id,amount --limit 10
    kob-client --transport rest --reload --dataset events      # rescan first, then query
    kob-client --transport http --dataset events --columns user_id,amount --limit 10
"""

from __future__ import annotations

import argparse
import json
import time

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


def query_rest_params(base_url: str, dataset: str, *, compression: str = "zstd",
                      timeout: float = 600, **params: object) -> pa.Table:
    """GET ``/data/{dataset}`` with the typed query parameters shown in the REST Swagger.

    Keyword args map to query parameters: ``region=["eu", "us"]`` (IN),
    ``amount__min=100`` / ``amount__max=500`` (→ ``amount.min`` / ``amount.max``),
    ``columns=["a", "b"]``, ``filter=["strike:>:100"]``, ``limit=10``.
    """
    query: list[tuple[str, object]] = [("format", "arrow"), ("compression", compression)]
    for k, v in params.items():
        if k.endswith("__min") or k.endswith("__max"):
            k = f"{k[:-5]}.{k[-3:]}"
        if k == "columns" and isinstance(v, (list, tuple)):
            v = ",".join(map(str, v))
        for item in v if isinstance(v, (list, tuple)) else [v]:
            query.append((k, str(item).lower() if isinstance(item, bool) else item))
    resp = requests.get(f"{base_url.rstrip('/')}/data/{dataset}", params=query,
                        stream=True, timeout=timeout)
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
    return QueryRequest(dataset=args.dataset, columns=columns, filters=filters, limit=args.limit)


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
