"""Demo transport — **Apache Arrow IPC over plain HTTP**.

A standalone, single-purpose server that streams query results as an Arrow IPC stream over
HTTP. It is here to *demonstrate* the Arrow-over-HTTP option and to give the benchmark a
target; the product's recommended fast path is Arrow Flight (:mod:`kob.server.flight`),
which avoids HTTP framing entirely.

Streaming is shared with ``kob --mode rest`` (:func:`kob.server.rest.stream_arrow_ipc`):
a producer thread pipelines DuckDB batches onto the wire through a bounded queue.

    python -m kob.demos.arrow_http.server --port 8001
"""

from __future__ import annotations

import argparse
import os

import uvicorn
from fastapi import FastAPI, HTTPException, Query

from ...core import engine
from ...core.catalog import DATA_ROOT
from ...core.contract import QueryRequest
from ...server.rest import ARROW_STREAM_MIME, stream_arrow_ipc


def create_app() -> FastAPI:
    from fastapi.responses import StreamingResponse

    app = FastAPI(title="kob demo — Arrow IPC over HTTP", version="0.4.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "data_root": str(DATA_ROOT), "transport": "arrow-ipc/http"}

    @app.post("/query")
    async def query(body: dict, compression: str = Query("zstd", pattern="^(zstd|lz4|none)$")):
        try:
            reader = engine.execute_reader(QueryRequest.from_dict(body))
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from None
        return StreamingResponse(
            stream_arrow_ipc(reader, compression),
            media_type=ARROW_STREAM_MIME,
            headers={"X-KOB-Compression": compression},
        )

    return app


app = create_app()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="kob demo — Arrow IPC over HTTP.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8001)
    p.add_argument("--threads", type=int, default=0, help="DuckDB threads (0 = all cores).")
    args = p.parse_args(argv)
    if args.threads:
        os.environ["KOB_DUCKDB_THREADS"] = str(args.threads)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
