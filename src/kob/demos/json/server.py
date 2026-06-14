"""Demo transport — **REST/JSON** (row objects over HTTP); the ubiquitous, slowest baseline.

A standalone server that streams query results as a JSON array of row objects. It is here
to *demonstrate* the cost of a row-oriented text format: the server must encode every value
and the client must decode every value, so latency scales with ``rows × cols``. The
benchmark uses it as the REST/JSON baseline that Arrow beats by hundreds of × — see
``docs/PERFORMANCE_REPORT.md``.

    python -m kob.demos.json.server --port 8002
"""

from __future__ import annotations

import argparse
import json
import os

import pyarrow as pa
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from ...core import engine
from ...core.catalog import DATA_ROOT
from ...core.contract import QueryRequest


def _stream_json(reader: pa.RecordBatchReader):
    yield b"["
    first = True
    for batch in reader:
        for row in batch.to_pylist():
            yield (b"" if first else b",") + json.dumps(row, default=str).encode()
            first = False
    yield b"]"


def create_app() -> FastAPI:
    app = FastAPI(title="kob demo — REST/JSON", version="0.4.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "data_root": str(DATA_ROOT), "transport": "rest/json"}

    @app.post("/query")
    async def query(body: dict):
        try:
            reader = engine.execute_reader(QueryRequest.from_dict(body))
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from None
        return StreamingResponse(_stream_json(reader), media_type="application/json")

    return app


app = create_app()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="kob demo — REST/JSON baseline.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8002)
    p.add_argument("--threads", type=int, default=0, help="DuckDB threads (0 = all cores).")
    args = p.parse_args(argv)
    if args.threads:
        os.environ["KOB_DUCKDB_THREADS"] = str(args.threads)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
