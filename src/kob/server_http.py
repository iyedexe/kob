"""kob — minimal, self-discovering Parquet read server (Arrow IPC over HTTP).

A producer drops Parquet under ``KOB_DATA_ROOT``; kob discovers it and serves it.
Tiny dependency surface (DuckDB + PyArrow + FastAPI), zero hardcoded schema.

Discovery
    ``GET  /health``                O(1) liveness (never walks the data tree).
    ``GET  /datasets``              list discovered datasets (folders of Parquet).
    ``GET  /datasets/{name}``       partition columns (+values, the "filter per folder"
                                    options) and data columns (+types).
    ``GET  /datasets/{name}/facets``  per-column min/max (+ optional distinct) — the
                                    "filter per column" options. ``?columns=a,b&distinct=true``

Query
    ``POST /query``                 run a query; body is the JSON contract. Streams Arrow
                                    IPC. ``?format=arrow|csv|json`` (default arrow),
                                    ``?compression=zstd|lz4|none`` (default zstd).

Everything is read-only. Column/operator names are validated against the discovered
schema and all values are bound parameters, so untrusted clients cannot inject SQL.

Interactive docs: Swagger UI at ``/docs``, ReDoc at ``/redoc``, schema at ``/openapi.json``.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import queue
import threading
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from . import engine
from .catalog import DATA_ROOT, get_dataset, list_datasets
from .contract import QueryRequest

ARROW_STREAM_MIME = "application/vnd.apache.arrow.stream"


# --------------------------------------------------------------------------- #
# Arrow IPC streaming.
#
# A background producer thread writes batches into a small queue while the response
# generator drains it — so DuckDB scans batch N+1 while batch N is on the wire
# (pipelining), with the bounded queue providing backpressure.
# --------------------------------------------------------------------------- #
class _QueueFile(io.RawIOBase):
    def __init__(self, q: "queue.Queue[bytes | None]") -> None:
        self._q = q

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:  # type: ignore[override]
        self._q.put(bytes(b))
        return len(b)


def _stream_arrow(reader: pa.RecordBatchReader, compression: str | None):
    comp = None if compression in (None, "none", "") else compression
    opts = pa.ipc.IpcWriteOptions(compression=comp)
    q: "queue.Queue[bytes | None]" = queue.Queue(maxsize=16)

    def produce() -> None:
        try:
            sink = pa.output_stream(_QueueFile(q))
            writer = pa.ipc.new_stream(sink, reader.schema, options=opts)
            for batch in reader:
                writer.write_batch(batch)
                sink.flush()
            writer.close()
            sink.close()
        except Exception:  # noqa: BLE001 — a truncated stream signals the error to the client
            pass
        q.put(None)

    threading.Thread(target=produce, daemon=True).start()

    def consume():
        while True:
            item = q.get()
            if item is None:
                break
            yield item

    return consume()


def _stream_json(reader: pa.RecordBatchReader):
    """Row-JSON debug/interop path — intentionally simple, never the fast path."""
    yield b"["
    first = True
    for batch in reader:
        for row in batch.to_pylist():
            yield (b"" if first else b",") + json.dumps(row, default=str).encode()
            first = False
    yield b"]"


def _csv_bytes(reader: pa.RecordBatchReader) -> bytes:
    """CSV interop path (Excel etc.) — materialises the result; fine for its use case."""
    buf = pa.BufferOutputStream()
    pacsv.write_csv(reader.read_all(), buf)
    return buf.getvalue().to_pybytes()


# --------------------------------------------------------------------------- #
# Request models — typed so Swagger documents the body and "Try it out" works
# --------------------------------------------------------------------------- #
class FilterModel(BaseModel):
    column: str = Field(..., description="A data column or a partition (folder) key.", examples=["region"])
    op: str = Field("=", description="One of: = != < <= > >= in 'not in' like ilike", examples=["in"])
    value: Any = Field(..., description="A scalar, or a list for in / not in.", examples=[["eu", "us"]])


class QueryModel(BaseModel):
    dataset: str = Field(..., description="A discovered dataset (see GET /datasets).", examples=["events"])
    columns: list[str] | None = Field(None, description="Projection; omit for all columns.")
    filters: list[FilterModel] = Field(
        default_factory=list, description="AND-ed; may target data columns or partition (folder) keys.")
    limit: int | None = Field(None, ge=1, description="Optional row cap.")

    model_config = {
        "json_schema_extra": {
            "examples": [{
                "dataset": "events",
                "columns": ["dt", "region", "user_id", "amount"],
                "filters": [
                    {"column": "region", "op": "in", "value": ["eu", "us"]},
                    {"column": "amount", "op": ">=", "value": 100.0},
                ],
                "limit": 100000,
            }]
        }
    }


_API_DESCRIPTION = """\
**kob** — a tiny, self-discovering, read-only Parquet query service.

A producer drops Parquet under `KOB_DATA_ROOT`; kob discovers datasets, partitions
and columns automatically and serves results as **Apache Arrow** (or CSV/JSON).

- **Discovery** — list datasets, inspect partitions (filter *per folder*) and columns (filter *per column*).
- **Query** — `POST /query` with the JSON contract; streams Arrow IPC.
"""

_OPENAPI_TAGS = [
    {"name": "Discovery", "description": "Find datasets and the filters available per folder and per column."},
    {"name": "Query", "description": "Run a read-only query and stream the result as Arrow / CSV / JSON."},
]


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
def create_app() -> FastAPI:
    app = FastAPI(
        title="kob",
        version="0.3.0",
        description=_API_DESCRIPTION,
        openapi_tags=_OPENAPI_TAGS,
    )

    @app.get("/health", tags=["Discovery"], summary="O(1) liveness")
    def health() -> dict:
        # Deliberately no discovery here: health probes fire constantly and must
        # never pay a tree walk. /datasets is the discovery endpoint.
        return {"status": "ok", "data_root": str(DATA_ROOT), "data_root_exists": DATA_ROOT.is_dir()}

    @app.get("/datasets", tags=["Discovery"], summary="List discovered datasets")
    def datasets(refresh: bool = False) -> dict:
        return {
            "data_root": str(DATA_ROOT),
            "datasets": [ds.to_public_dict(with_values=False) for ds in list_datasets(refresh=refresh)],
        }

    @app.get("/datasets/{name}", tags=["Discovery"],
             summary="Partition values (filter per folder) + data columns")
    def dataset(name: str, refresh: bool = False) -> dict:
        try:
            ds = get_dataset(name, refresh=refresh)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        return ds.to_public_dict(with_values=True)

    @app.get("/datasets/{name}/facets", tags=["Discovery"],
             summary="Per-column min/max & distinct values (filter per column)")
    def facets(
        name: str,
        columns: str = Query(..., description="Comma-separated columns."),
        distinct: bool = False,
    ) -> dict:
        try:
            ds = get_dataset(name)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        wanted = [c.strip() for c in columns.split(",") if c.strip()]
        out: dict[str, dict] = {}
        data_cols = []
        for c in wanted:
            if c in ds.partition_cols:
                # Partition filters come straight from the folder names — free, no scan.
                out[c] = {"type": "partition", "values": ds.partition_values.get(c, [])}
            elif c in ds.allowed_columns:
                data_cols.append(c)
            else:
                raise HTTPException(400, f"Unknown column '{c}' for dataset '{name}'")
        if data_cols:
            out.update(engine.column_facets(name, data_cols, distinct=distinct))
        return {"dataset": name, "facets": out}

    @app.post(
        "/query",
        tags=["Query"],
        summary="Run a query; stream Arrow (or CSV/JSON)",
        responses={200: {"description": "Result rows in the requested format.",
                         "content": {ARROW_STREAM_MIME: {}, "text/csv": {}, "application/json": {}}}},
    )
    async def query(
        body: QueryModel,
        format: str = Query("arrow", pattern="^(arrow|json|csv)$",
                            description="arrow streams Arrow IPC; csv/json for interop/debug."),
        compression: str = Query("zstd", pattern="^(zstd|lz4|none)$",
                                 description="Arrow IPC buffer compression (zstd over a network, none on a LAN)."),
    ):
        try:
            req = QueryRequest.from_dict(body.model_dump())
            reader = engine.execute_reader(req)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None

        if format == "arrow":
            return StreamingResponse(
                _stream_arrow(reader, compression),
                media_type=ARROW_STREAM_MIME,
                headers={"X-KOB-Compression": compression},
            )
        if format == "json":
            return StreamingResponse(_stream_json(reader), media_type="application/json")
        return Response(content=_csv_bytes(reader), media_type="text/csv")

    return app


app = create_app()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="kob — minimal self-discovering Parquet read server (Arrow/HTTP).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--workers", type=int, default=1,
                   help="Uvicorn worker processes (each gets its own DuckDB).")
    p.add_argument("--threads", type=int, default=0,
                   help="DuckDB threads per worker (0 = all cores).")
    args = p.parse_args(argv)
    if args.threads:
        os.environ["KOB_DUCKDB_THREADS"] = str(args.threads)
    if args.workers > 1:
        # Multi-process serving needs the app as an import string.
        uvicorn.run("kob.server_http:app", host=args.host, port=args.port,
                    workers=args.workers, log_level="info")
    else:
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
