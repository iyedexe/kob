"""kob REST mode — ``kob --mode rest``: a single HTTP server, data served as **Apache Arrow**.

No Flight, no gRPC: one port, plain HTTP. Query results stream back as an Arrow IPC
stream (``application/vnd.apache.arrow.stream``), so clients still get the columnar
layout with nothing to decode row by row.

The Swagger UI is generated **from the discovered datasets**: every dataset gets its own
section with typed endpoints —

* ``GET  /data/{dataset}`` — one query parameter per column: partition keys and string
  columns are enum/multi-value pickers (``IN``), numeric/temporal columns get exact
  (``col``) and range (``col.min`` / ``col.max``) parameters, ``columns`` is an enum of
  the dataset's own columns;
* ``POST /data/{dataset}`` — the JSON query contract, with column names as enums.

Discovery is **manual** in this mode: the tree is walked once at startup and again only
when someone presses **Reload** in the Swagger UI (or calls ``POST /reload``). Requests
never trigger a tree walk, so a large data root costs nothing on the hot path. Reload
rebuilds the OpenAPI spec, so new datasets/columns/partition values appear in Swagger.

    kob --mode rest --port 8000            # → http://127.0.0.1:8000/docs
"""

from __future__ import annotations

import datetime as _dt
import io
import queue
import threading
import time
from typing import Any

import duckdb
import pyarrow as pa
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

from ..core import catalog, engine
from ..core.catalog import DATA_ROOT, Dataset
from ..core.contract import ALLOWED_OPS, Filter, QueryRequest
from .api import _csv_bytes, _stream_json, dataset_facets

ARROW_STREAM_MIME = "application/vnd.apache.arrow.stream"

# Query-parameter names with a fixed meaning on GET /data/{dataset}. A column with one of
# these names gets no typed parameter (it is still filterable via `filter` or POST).
_RESERVED = {"columns", "filter", "limit", "format", "compression"}
_LIST_OPS = {"in", "not in"}
_RANGE_TYPES = {"int64", "double", "decimal", "date", "timestamp"}


# --------------------------------------------------------------------------- #
# Arrow IPC streaming
# --------------------------------------------------------------------------- #
class _QueueFile(io.RawIOBase):
    def __init__(self, q: "queue.Queue[bytes | None]") -> None:
        self._q = q

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:  # type: ignore[override]
        self._q.put(bytes(b))
        return len(b)


def stream_arrow_ipc(reader: pa.RecordBatchReader, compression: str | None = None):
    """Encode a RecordBatchReader as an Arrow IPC stream, pipelined through a thread.

    A producer thread writes batches into a small bounded queue while the response drains
    it, so DuckDB scans batch N+1 while batch N is on the wire; the queue gives
    backpressure. An error mid-stream truncates the stream, which the client sees.
    """
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


# --------------------------------------------------------------------------- #
# GET query-string → QueryRequest
# --------------------------------------------------------------------------- #
def _coerce(ds: Dataset, column: str, raw: str) -> Any:
    """Turn a query-string value into the column's Python type (DuckDB casts the rest)."""
    t = ds.columns.get(column)
    try:
        if t == "int64":
            return int(raw)
        if t == "double":
            return float(raw)
    except ValueError:
        raise ValueError(f"Invalid {t} value for {column!r}: {raw!r}") from None
    if t == "bool":
        low = raw.strip().lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
        raise ValueError(f"Invalid bool value for {column!r}: {raw!r}")
    return raw  # strings, dates, timestamps, partition values


def _split_csv(values: list[str]) -> list[str]:
    return [v.strip() for raw in values for v in raw.split(",") if v.strip()]


def request_from_query_params(ds: Dataset, items: list[tuple[str, str]]) -> QueryRequest:
    """Build a QueryRequest from GET /data/{dataset} query parameters (see module doc)."""
    grouped: dict[str, list[str]] = {}
    for k, v in items:
        grouped.setdefault(k, []).append(v)

    columns = _split_csv(grouped.pop("columns", [])) or None
    limit_raw = grouped.pop("limit", [None])[-1]
    try:
        limit = int(limit_raw) if limit_raw not in (None, "") else None
    except ValueError:
        raise ValueError(f"limit must be an integer, got {limit_raw!r}") from None
    grouped.pop("format", None)
    grouped.pop("compression", None)

    ranges: dict[str, dict[str, str | None]] = {}
    for key in [k for k in grouped if k.endswith((".start", ".end"))]:
        col, _, side = key.rpartition(".")
        if col not in ds.date_columns:
            continue  # not a date column: falls through to "unknown parameter" below
        val = next((v for v in reversed(grouped.pop(key)) if v), None)
        if val is not None:
            ranges.setdefault(col, {"start": None, "end": None})[side] = val

    filters: list[Filter] = []
    for raw in grouped.pop("filter", []):
        try:
            col, op, val = raw.split(":", 2)
        except ValueError:
            raise ValueError(f"filter must be 'column:op:value', got {raw!r}") from None
        if col not in ds.allowed_columns:
            raise ValueError(f"Unknown filter column for '{ds.name}': {col!r}")
        if op.lower() in _LIST_OPS:
            value: Any = [_coerce(ds, col, v) for v in _split_csv([val])]
        else:
            value = _coerce(ds, col, val)
        filters.append(Filter(col, op, value))

    for key, vals in grouped.items():
        vals = [v for v in vals if v != ""]  # Swagger UI sends empty optional params
        if not vals:
            continue
        if key in ds.allowed_columns:
            col, op = key, "in"
        elif key.endswith(".min") and key[:-4] in ds.allowed_columns:
            col, op = key[:-4], ">="
        elif key.endswith(".max") and key[:-4] in ds.allowed_columns:
            col, op = key[:-4], "<="
        else:
            raise ValueError(f"Unknown query parameter for '{ds.name}': {key!r}")
        if op == "in":
            filters.append(Filter(col, op, [_coerce(ds, col, v) for v in _split_csv(vals)]))
        else:
            filters.append(Filter(col, op, _coerce(ds, col, vals[-1])))

    return QueryRequest(dataset=ds.name, columns=columns, filters=filters, limit=limit,
                        ranges=ranges)


# --------------------------------------------------------------------------- #
# Per-dataset OpenAPI generation
# --------------------------------------------------------------------------- #
def _json_type(t: str) -> dict:
    return {
        "int64": {"type": "integer"},
        "double": {"type": "number"},
        "decimal": {"type": "number"},
        "bool": {"type": "boolean"},
        "date": {"type": "string", "format": "date"},
        "timestamp": {"type": "string", "format": "date-time"},
    }.get(t, {"type": "string"})


def _format_params() -> list[dict]:
    return [
        {"name": "format", "in": "query", "required": False,
         "description": "`arrow` (default) streams an Arrow IPC stream. `json` / `csv` are for "
                        "eyeballing results in this page — not the throughput path.",
         "schema": {"type": "string", "enum": ["arrow", "json", "csv"], "default": "arrow"}},
        {"name": "compression", "in": "query", "required": False,
         "description": "Arrow IPC buffer compression. `none` is readable by every Arrow "
                        "implementation; `zstd`/`lz4` shrink the wire size.",
         "schema": {"type": "string", "enum": ["none", "zstd", "lz4"], "default": "none"}},
    ]


def _responses(ds: Dataset) -> dict:
    return {
        "200": {
            "description": f"Rows of `{ds.name}` — an Arrow IPC stream (or JSON/CSV if asked).",
            "content": {ARROW_STREAM_MIME: {"schema": {"type": "string", "format": "binary"}},
                        "application/json": {}, "text/csv": {}},
        },
        "400": {"description": "Invalid column, operator or value."},
        "404": {"description": "Dataset not in the last scan (press Reload)."},
    }


def _range_params(ds: Dataset, col: str) -> list[dict]:
    t = ds.columns.get(col, "date")
    whole_day = " A date-only end covers that whole day." if t == "timestamp" else ""
    return [
        {"name": f"{col}.start", "in": "query", "required": False,
         "description": f"`{col}` from this date, inclusive ({t}).",
         "schema": {"type": "string", "format": "date"}},
        {"name": f"{col}.end", "in": "query", "required": False,
         "description": f"`{col}` up to this date, inclusive ({t}).{whole_day}",
         "schema": {"type": "string", "format": "date"}},
    ]


def _get_operation(ds: Dataset) -> dict:
    col_names = list(ds.columns)
    params: list[dict] = [
        {"name": "columns", "in": "query", "required": False, "style": "form", "explode": False,
         "description": "Projection (comma-separated); omit for all columns.",
         "schema": {"type": "array", "items": {"type": "string", "enum": col_names}}},
    ]
    for k in ds.partition_cols:
        if k in _RESERVED:
            continue
        params.append({
            "name": k, "in": "query", "required": False, "explode": True,
            "description": f"Partition (folder) filter on `{k}` — matches any selected value.",
            "schema": {"type": "array", "items": {"type": "string",
                                                  "enum": ds.partition_values.get(k, [])}},
        })
        if k in ds.date_columns:
            params += _range_params(ds, k)
    for c, t in ds.data_columns.items():
        if c in _RESERVED:
            continue
        js = _json_type(t)
        if t == "bool":
            params.append({"name": c, "in": "query", "required": False,
                           "description": f"`{c}` = value (bool).", "schema": js})
            continue
        if t != "double":  # exact match on floats is rarely what anyone wants
            params.append({"name": c, "in": "query", "required": False, "explode": True,
                           "description": f"`{c}` IN values ({t}); repeat or comma-separate.",
                           "schema": {"type": "array", "items": js}})
        if c in ds.date_columns:
            params += _range_params(ds, c)
        elif t in _RANGE_TYPES:
            params.append({"name": f"{c}.min", "in": "query", "required": False,
                           "description": f"`{c}` >= value ({t}).", "schema": js})
            params.append({"name": f"{c}.max", "in": "query", "required": False,
                           "description": f"`{c}` <= value ({t}).", "schema": js})
    params += [
        {"name": "filter", "in": "query", "required": False, "explode": True,
         "description": "Extra filters as `column:op:value` (repeatable), e.g. `strike:>:100` or "
                        "`cp_flag:in:C,P`. Ops: " + ", ".join(sorted(ALLOWED_OPS)) + ".",
         "schema": {"type": "array", "items": {"type": "string"}}},
        {"name": "limit", "in": "query", "required": False,
         "description": "Row cap.", "schema": {"type": "integer", "minimum": 1}},
        *_format_params(),
    ]
    return {
        "tags": [ds.name],
        "summary": f"Query {ds.name} (typed parameters)",
        "description": (f"All filters are AND-ed. {ds.file_count} Parquet file(s); partitioned by "
                        f"{', '.join(f'`{k}`' for k in ds.partition_cols) or 'nothing'}."
                        + (" Date ranges: each date column has its own `<column>.start` / "
                           "`<column>.end` (inclusive)." if ds.date_columns else "")),
        "operationId": f"get_data_{ds.name}",
        "parameters": params,
        "responses": _responses(ds),
    }


def _post_operation(ds: Dataset) -> dict:
    col_enum = {"type": "string", "enum": list(ds.columns)}
    example_cols = list(ds.columns)[:4]
    example_filters = []
    if ds.partition_cols and ds.partition_values.get(ds.partition_cols[0]):
        k = ds.partition_cols[0]
        example_filters.append({"column": k, "op": "in", "value": ds.partition_values[k][:2]})
    body_schema = {
        "type": "object",
        "properties": {
            "columns": {"type": "array", "items": col_enum, "description": "Projection; omit for all."},
            "filters": {"type": "array", "description": "AND-ed filters.", "items": {
                "type": "object", "required": ["column", "op", "value"],
                "properties": {
                    "column": col_enum,
                    "op": {"type": "string", "enum": sorted(ALLOWED_OPS)},
                    "value": {"description": "A scalar, or a list for in / not in."},
                },
            }},
            "limit": {"type": "integer", "minimum": 1},
            **({
                "ranges": {
                    "type": "object",
                    "description": "Inclusive start/end per date column (AND-ed); either side optional.",
                    "properties": {c: {"type": "object", "properties": {
                        "start": {"type": "string", "format": "date"},
                        "end": {"type": "string", "format": "date"}}} for c in ds.date_columns},
                    "additionalProperties": False,
                },
            } if ds.date_columns else {}),
        },
        "example": {"columns": example_cols, "filters": example_filters, "limit": 1000},
    }
    return {
        "tags": [ds.name],
        "summary": f"Query {ds.name} (JSON contract)",
        "description": "The same JSON query contract every kob client speaks; `dataset` comes "
                       "from the path.",
        "operationId": f"post_data_{ds.name}",
        "parameters": _format_params(),
        "requestBody": {"required": False,
                        "content": {"application/json": {"schema": body_schema}}},
        "responses": _responses(ds),
    }


def _description(datasets: list[Dataset]) -> str:
    scanned = catalog.last_scan_time()
    when = (_dt.datetime.fromtimestamp(scanned).strftime("%Y-%m-%d %H:%M:%S")
            if scanned else "never")
    lines = [
        "**kob** (REST mode) — self-discovering, read-only Parquet query API. Results stream as "
        f"**Apache Arrow IPC** (`{ARROW_STREAM_MIME}`).",
        "",
        f"Data root `{DATA_ROOT}` — last scanned **{when}**. Discovery is manual: press "
        "**Reload datasets** (top right) or `POST /reload` after adding Parquet.",
        "",
        "| dataset | files | partitions | columns |",
        "|---|---|---|---|",
    ]
    for ds in datasets:
        lines.append(f"| `{ds.name}` | {ds.file_count} | {', '.join(ds.partition_cols) or '—'} "
                     f"| {len(ds.data_columns)} |")
    if not datasets:
        lines.append("| *(none found)* | | | |")
    return "\n".join(lines)


_STATIC_TAGS = [
    {"name": "Catalog", "description": "Datasets, their filters, and the manual rescan."},
]


# --------------------------------------------------------------------------- #
# Swagger UI with a Reload button
# --------------------------------------------------------------------------- #
_RELOAD_WIDGET = """
<div id="kob-reload-bar" style="position:fixed;top:12px;right:16px;z-index:1000;
     font-family:sans-serif;font-size:13px;display:flex;gap:8px;align-items:center;">
  <span id="kob-reload-status" style="color:#3b4151;"></span>
  <button id="kob-reload-btn" type="button" style="cursor:pointer;padding:6px 14px;border-radius:4px;
          border:1px solid #49cc90;background:#49cc90;color:#fff;font-weight:600;">
    &#x21bb; Reload datasets</button>
</div>
<script>
document.getElementById("kob-reload-btn").addEventListener("click", async () => {
  const btn = document.getElementById("kob-reload-btn");
  const status = document.getElementById("kob-reload-status");
  btn.disabled = true; status.textContent = "Rescanning Parquet…";
  try {
    const r = await fetch("reload", {method: "POST"});
    if (!r.ok) throw new Error(r.status + " " + r.statusText);
    const j = await r.json();
    status.textContent = j.datasets.length + " dataset(s) in " + j.elapsed_ms + " ms — reloading…";
    window.location.reload();
  } catch (e) {
    status.textContent = "Reload failed: " + e.message; btn.disabled = false;
  }
});
</script>
"""


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
def create_rest_app() -> FastAPI:
    """Build the REST-mode app. Puts the catalog in manual mode and scans once."""
    catalog.rescan()

    app = FastAPI(title="kob", version="0.4.0", docs_url=None, redoc_url=None)

    def openapi() -> dict:
        if app.openapi_schema:
            return app.openapi_schema
        datasets = catalog.list_datasets()
        schema = get_openapi(title=app.title, version=app.version, description=_description(datasets),
                             routes=app.routes, tags=_STATIC_TAGS)
        paths = schema.setdefault("paths", {})
        for ds in datasets:
            paths[f"/data/{ds.name}"] = {"get": _get_operation(ds), "post": _post_operation(ds)}
        schema["tags"] = _STATIC_TAGS + [
            {"name": ds.name, "description": f"Dataset `{ds.name}` — {ds.file_count} file(s), "
                                             f"{len(ds.columns)} column(s)."}
            for ds in datasets
        ]
        app.openapi_schema = schema
        return schema

    app.openapi = openapi  # type: ignore[method-assign]

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse("docs")

    @app.get("/docs", include_in_schema=False)
    def docs() -> HTMLResponse:
        page = get_swagger_ui_html(openapi_url="openapi.json", title="kob — REST / Arrow")
        html = page.body.decode().replace("</body>", _RELOAD_WIDGET + "</body>")
        return HTMLResponse(html)

    @app.get("/health", tags=["Catalog"], summary="O(1) liveness")
    def health() -> dict:
        return {"status": "ok", "mode": "rest", "data_root": str(DATA_ROOT),
                "data_root_exists": DATA_ROOT.is_dir(), "last_scan": catalog.last_scan_time()}

    @app.post("/reload", tags=["Catalog"], summary="Rescan the data root and rebuild this Swagger spec")
    def reload() -> dict:
        t0 = time.perf_counter()
        datasets = catalog.rescan()
        app.openapi_schema = None  # rebuilt from the new snapshot on the next /openapi.json
        return {"datasets": [ds.name for ds in datasets],
                "elapsed_ms": round((time.perf_counter() - t0) * 1e3, 1),
                "scanned_at": catalog.last_scan_time()}

    @app.get("/datasets", tags=["Catalog"], summary="List datasets from the last scan")
    def datasets() -> dict:
        return {"data_root": str(DATA_ROOT), "last_scan": catalog.last_scan_time(),
                "datasets": [ds.to_public_dict(with_values=False) for ds in catalog.list_datasets()]}

    @app.get("/datasets/{name}", tags=["Catalog"],
             summary="Partition values (filter per folder) + data columns")
    def dataset(name: str) -> dict:
        try:
            return catalog.get_dataset(name).to_public_dict(with_values=True)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None

    @app.get("/datasets/{name}/facets", tags=["Catalog"],
             summary="Per-column min/max & distinct values (filter per column)")
    def facets(name: str, columns: str = Query(..., description="Comma-separated columns."),
               distinct: bool = False) -> dict:
        return dataset_facets(name, columns, distinct)

    def _respond(req: QueryRequest, fmt: str, compression: str):
        if fmt not in ("arrow", "json", "csv"):
            raise HTTPException(400, f"format must be arrow, json or csv, got {fmt!r}")
        if compression not in ("none", "zstd", "lz4"):
            raise HTTPException(400, f"compression must be none, zstd or lz4, got {compression!r}")
        try:
            reader = engine.execute_reader(req)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        except duckdb.Error as exc:
            raise HTTPException(400, f"Query failed: {exc}") from None
        if fmt == "csv":
            return Response(content=_csv_bytes(reader), media_type="text/csv")
        if fmt == "json":
            return StreamingResponse(_stream_json(reader), media_type="application/json")
        return StreamingResponse(stream_arrow_ipc(reader, compression), media_type=ARROW_STREAM_MIME,
                                 headers={"X-KOB-Compression": compression})

    def _dataset_or_404(name: str) -> Dataset:
        try:
            return catalog.get_dataset(name)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None

    # The per-dataset paths are documented by openapi() above; these two generic routes
    # serve them. Sync handlers on purpose: FastAPI runs them on the threadpool, so the
    # blocking DuckDB execute never stalls the event loop.
    @app.get("/data/{name}", include_in_schema=False)
    def data_get(name: str, request: Request):
        ds = _dataset_or_404(name)
        qp = request.query_params
        try:
            req = request_from_query_params(ds, list(qp.multi_items()))
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        return _respond(req, qp.get("format") or "arrow", qp.get("compression") or "none")

    @app.post("/data/{name}", include_in_schema=False)
    def data_post(name: str, body: dict | None = None, format: str = "arrow",
                  compression: str = "none"):
        ds = _dataset_or_404(name)
        body = dict(body or {})
        body["dataset"] = ds.name
        try:
            req = QueryRequest.from_dict(body)
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(400, f"Invalid query body: {exc}") from None
        return _respond(req, format, compression)

    return app
