"""kob — the Swagger HTTP control plane that runs alongside the Arrow Flight server.

This small FastAPI app is the **human-facing** surface of kob. It is *not* the fast data
path — that is Arrow Flight (see :mod:`kob.server.flight`). It exists so you can, from a
browser at ``/docs``:

* **discover** datasets and the filters available *per folder* (partitions) and *per
  column* (min/max/distinct);
* read **how to pull data over Flight** (``GET /flight``);
* **try a query interactively** (``POST /query``), which returns JSON or CSV — handy for
  exploration, debugging and spreadsheet/interop, never the throughput path.

Everything is read-only. Column/operator names are validated against the discovered
schema and all values are bound parameters, so untrusted clients cannot inject SQL.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..core import engine
from ..core.catalog import DATA_ROOT, get_dataset, list_datasets
from ..core.contract import QueryRequest

DEFAULT_FLIGHT_LOCATION = "grpc://127.0.0.1:8815"


# --------------------------------------------------------------------------- #
# Interop result encoders (JSON / CSV). The binary fast path is Flight, not this.
# --------------------------------------------------------------------------- #
def _stream_json(reader: pa.RecordBatchReader):
    """Row-JSON for browsers/interop — intentionally simple, never the fast path."""
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
    op: str = Field("=", description="One of: = != <> < <= > >= in 'not in' like 'not like' ilike", examples=["in"])
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

A producer drops Parquet under `KOB_DATA_ROOT`; kob discovers datasets, partitions and
columns automatically. The **fast, default data path is Apache Arrow Flight** (gRPC) —
see `GET /flight`. This HTTP API is for **discovery and interactive exploration**:

- **Discovery** — list datasets, inspect partitions (filter *per folder*) and columns (filter *per column*).
- **Flight** — `GET /flight` documents how to pull results over Arrow Flight (the throughput path).
- **Query (interactive)** — `POST /query` runs a query and returns **JSON or CSV** for browsing/interop.
  For large or latency-sensitive pulls, use Flight instead.
"""

_OPENAPI_TAGS = [
    {"name": "Discovery", "description": "Find datasets and the filters available per folder and per column."},
    {"name": "Flight", "description": "How to pull results over the fast Arrow Flight (gRPC) transport."},
    {"name": "Query", "description": "Run a read-only query interactively (JSON / CSV). Flight is the fast path."},
]


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
def create_app(flight_location: str = DEFAULT_FLIGHT_LOCATION) -> FastAPI:
    app = FastAPI(
        title="kob",
        version="0.4.0",
        description=_API_DESCRIPTION,
        openapi_tags=_OPENAPI_TAGS,
    )

    @app.get("/health", tags=["Discovery"], summary="O(1) liveness")
    def health() -> dict:
        # Deliberately no discovery here: health probes fire constantly and must
        # never pay a tree walk. /datasets is the discovery endpoint.
        return {"status": "ok", "data_root": str(DATA_ROOT), "data_root_exists": DATA_ROOT.is_dir(),
                "flight": flight_location}

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

    @app.get("/flight", tags=["Flight"], summary="How to pull results over Arrow Flight (the fast path)")
    def flight_info() -> dict:
        example = QueryModel.model_config["json_schema_extra"]["examples"][0]
        return {
            "location": flight_location,
            "transport": "Apache Arrow Flight (gRPC + Arrow IPC) — the recommended, fastest data path.",
            "how_to": [
                "1. Connect a Flight client to `location`.",
                "2. Build a FlightDescriptor.for_command(json) where json is the same query "
                "contract as POST /query (see the example below).",
                "3. get_flight_info(descriptor) → endpoints[0].ticket, then do_get(ticket) to "
                "stream Arrow RecordBatches. Or build a Ticket from the JSON directly.",
            ],
            "example_command": example,
            "python": (
                "from kob.tools.client import query_flight; "
                "from kob.core.contract import QueryRequest, Filter; "
                f"query_flight('{flight_location}', QueryRequest(dataset='events'))"
            ),
        }

    @app.post(
        "/query",
        tags=["Query"],
        summary="Run a query interactively; return JSON (default) or CSV",
        responses={200: {"description": "Result rows in the requested format.",
                         "content": {"application/json": {}, "text/csv": {}}}},
    )
    async def query(
        body: QueryModel,
        format: str = Query("json", pattern="^(json|csv)$",
                            description="json (default) or csv. For binary/throughput, use Arrow Flight."),
    ):
        try:
            req = QueryRequest.from_dict(body.model_dump())
            reader = engine.execute_reader(req)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None

        if format == "csv":
            return Response(content=_csv_bytes(reader), media_type="text/csv")
        return StreamingResponse(_stream_json(reader), media_type="application/json")

    return app


# A default app (Flight location at its default port) so `uvicorn kob.server.api:app`
# works; the `kob` entry point (kob.server.app) injects the real Flight location.
app = create_app()
