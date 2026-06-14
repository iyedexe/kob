"""kob — the primary server entry point. ``kob`` starts **Flight + Swagger** together.

* **Arrow Flight** (gRPC) is the fast, default data path. It runs on a background thread.
* A small **HTTP/Swagger** control plane (:mod:`kob.server.api`) runs in the foreground for
  discovery and interactive exploration at ``/docs``.

Both share one process and one in-memory DuckDB catalog, so discovery is consistent across
the two and there is nothing to keep in sync. The data path stays pure Flight — the HTTP
control plane never touches it.

    kob                                   # Flight :8815 + Swagger http://127.0.0.1:8000/docs
    kob --host 0.0.0.0 --flight-port 8815 --http-port 8000 --threads 0
"""

from __future__ import annotations

import argparse
import os
import threading

import uvicorn

from .api import create_app
from .flight import build_server


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description="kob — self-discovering Parquet server: Arrow Flight (fast data) + Swagger (docs).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--flight-port", type=int, default=8815, help="Arrow Flight (gRPC) data port.")
    p.add_argument("--http-port", type=int, default=8000, help="HTTP/Swagger control-plane port.")
    p.add_argument("--threads", type=int, default=0, help="DuckDB threads (0 = all cores).")
    args = p.parse_args(argv)

    if args.threads:
        os.environ["KOB_DUCKDB_THREADS"] = str(args.threads)

    # Build the Flight server first: FlightServerBase binds the port on construction, so a
    # port clash fails loudly here (main thread) rather than silently in the worker thread.
    flight_server, flight_location = build_server(args.host, args.flight_port)
    threading.Thread(target=flight_server.serve, name="kob-flight", daemon=True).start()

    app = create_app(flight_location=flight_location)
    docs_url = f"http://{args.host}:{args.http_port}/docs"

    print(
        "\n  kob — Parquet → Apache Arrow, self-discovering\n"
        f"    data  (fast)     Arrow Flight   {flight_location}\n"
        f"    docs  (Swagger)  HTTP           {docs_url}\n"
        f"    data root        {os.environ.get('KOB_DATA_ROOT', './data')}\n"
    )
    # uvicorn blocks in the foreground; Ctrl-C stops it and the daemon Flight thread exits.
    uvicorn.run(app, host=args.host, port=args.http_port, log_level="info")


if __name__ == "__main__":
    main()
