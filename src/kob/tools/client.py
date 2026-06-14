"""Reference Python client (install extra: ``kob[client]``).

Two transports: **Flight** (the product's fast, default path, on :8815) and **HTTP-Arrow**
(the Arrow-over-HTTP demo server, on :8001).

Importable API::

    from kob.tools.client import query_flight, query_http
    from kob.core.contract import QueryRequest, Filter

    tbl = query_flight("grpc://127.0.0.1:8815",
                       QueryRequest(dataset="events",
                                    filters=[Filter("region", "=", "eu")]))

CLI::

    kob-client --dataset events --filter 'region:=:eu' --filter 'amount:>=:100'
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


def list_datasets_http(base_url: str) -> dict:
    """List datasets from the Swagger control plane (default :8000)."""
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
    p.add_argument("--transport", choices=["flight", "http"], default="flight",
                   help="flight = the fast default (:8815); http = the Arrow-over-HTTP demo (:8001).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int)
    p.add_argument("--dataset", required=True)
    p.add_argument("--columns", help="Comma-separated projection.")
    p.add_argument("--filter", action="append",
                   help="Filter 'column:op:value' (repeatable), e.g. 'region:=:eu' or 'amount:>=:100'.")
    p.add_argument("--limit", type=int)
    p.add_argument("--compression", default="zstd", choices=["zstd", "lz4", "none"])
    p.add_argument("--head", type=int, default=5, help="Rows to preview.")
    args = p.parse_args(argv)

    req = _req_from_args(args)
    t0 = time.perf_counter()
    if args.transport == "flight":
        table = query_flight(f"grpc://{args.host}:{args.port or 8815}", req)
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
