"""DuckDB-backed execution engine.

DuckDB reads the Hive-partitioned Parquet directly and pushes projection, predicate
(row-group zonemap) and partition pruning into the scan, then streams results out as
Apache Arrow ``RecordBatch`` objects with no row-by-row conversion. Both servers call
:func:`execute_reader`; the wire transport is the only thing that differs between them.

Speed notes
-----------
* Queries scan an **explicit file list** planned from the catalog's inventory (see
  ``core/inventory.py``): DuckDB never lists or globs the tree, and only opens files whose
  partition values / footer min-max can match.
* ``parquet_metadata_cache`` keeps Parquet footers cached across queries, so repeated
  queries over the same files skip the footer read (a network round trip each).
* ``preserve_insertion_order = false`` lets DuckDB parallelise scans freely.
* DuckDB worker threads default to all cores; override with ``KOB_DUCKDB_THREADS``.
"""

from __future__ import annotations

import os
import threading

import duckdb
import pyarrow as pa

from .contract import QueryRequest, build_sql

# Rows per Arrow batch streamed from DuckDB. A few 100k rows keeps each batch in the
# low-MB range for typical widths — good for streaming + backpressure without tiny chunks.
DEFAULT_BATCH_ROWS = 122_880

_conn_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Process-wide DuckDB connection (lazily created). Use :func:`cursor` per request."""
    global _conn
    with _conn_lock:
        if _conn is None:
            conn = duckdb.connect(database=":memory:")
            threads = os.environ.get("KOB_DUCKDB_THREADS")
            if threads:
                conn.execute(f"SET threads TO {int(threads)}")
            # Parallel scans don't need to preserve row order for analytical reads.
            conn.execute("SET preserve_insertion_order = false")
            # Cache Parquet footers across queries (big win on network drives). The
            # setting's name changed across DuckDB versions; try the current one first.
            for setting in ("parquet_metadata_cache", "enable_object_cache"):
                try:
                    conn.execute(f"SET {setting} = true")
                    break
                except duckdb.Error:
                    continue  # not known to this DuckDB version — never fatal
            # Publish only once fully configured, so a failed SET above can't
            # leave a half-configured connection behind for every later query.
            _conn = conn
        return _conn


def cursor() -> duckdb.DuckDBPyConnection:
    """A fresh cursor sharing the in-memory catalog — safe to use from many threads."""
    return get_connection().cursor()


def _arrow_reader(cur: duckdb.DuckDBPyConnection, batch_rows: int):
    """DuckDB result -> streaming Arrow reader; prefers the current API name."""
    if hasattr(cur, "to_arrow_reader"):           # DuckDB >= 1.5
        return cur.to_arrow_reader(batch_rows)
    return cur.fetch_record_batch(batch_rows)     # older DuckDB (deprecated alias)


def execute_reader(req: QueryRequest, batch_rows: int = DEFAULT_BATCH_ROWS) -> pa.RecordBatchReader:
    """Run a query and return a streaming :class:`pyarrow.RecordBatchReader`.

    The reader pulls batches lazily from DuckDB, so memory stays bounded even for
    multi-GB result sets. We re-wrap DuckDB's reader in a generator-backed
    ``RecordBatchReader`` whose closure keeps the cursor alive until the stream is
    fully consumed — Flight consumes the reader lazily in C++, long after this
    function returns, so the cursor must not be garbage-collected early.
    """
    sql, params = build_sql(req)
    cur = cursor()
    try:
        cur.execute(sql, params)
        duck_reader = _arrow_reader(cur, batch_rows)
        schema = duck_reader.schema
    except Exception:
        cur.close()  # the generator below never runs, so close here
        raise

    def _batches():
        # `cur` and `duck_reader` are read (never reassigned) here, so they are
        # captured as closure cells and stay alive for the whole stream. Closing the
        # cursor in `finally` also references `cur`, reinforcing the capture.
        try:
            for batch in duck_reader:
                yield batch
        finally:
            cur.close()

    return pa.RecordBatchReader.from_batches(schema, _batches())


def fetch_schema(req: QueryRequest) -> pa.Schema:
    """Cheaply resolve the Arrow output schema (used by Flight ``get_flight_info``)."""
    sql, params = build_sql(req)
    cur = cursor()
    try:
        cur.execute(f"SELECT * FROM ({sql}) AS _schema_probe LIMIT 0", params)
        return _arrow_reader(cur, 1).schema  # Schema is materialised; safe to close
    finally:
        cur.close()


def column_facets(dataset: str, columns: list[str], *, distinct: bool = False) -> dict[str, dict]:
    """Per-column min/max (and optional approx distinct count) for filter introspection.

    min/max come straight from the inventory (the footers read at discovery) when every
    file has statistics — no file is touched. Otherwise DuckDB resolves them from Parquet
    row-group statistics. ``approx_count_distinct`` (HLL) is opt-in as it does scan.
    """
    from . import inventory  # local imports avoid a cycle
    from .catalog import get_dataset
    from .contract import _ident, scan_sql

    ds = get_dataset(dataset)
    cols = [c for c in columns if c in ds.allowed_columns]
    if not cols:
        return {}
    out: dict[str, dict] = {}
    if not distinct and ds.inventory is not None:
        for c in cols:
            mm = inventory.column_min_max(ds.inventory, c)
            if mm is not None:
                out[c] = {"type": ds.columns[c], "min": mm[0], "max": mm[1]}
        cols = [c for c in cols if c not in out]
        if not cols:
            return out
    selects = []
    for i, c in enumerate(cols):
        q = _ident(c)
        selects.append(f"min({q}) AS mn{i}, max({q}) AS mx{i}")
        if distinct:
            selects.append(f"approx_count_distinct({q}) AS nd{i}")
    sql = f"SELECT {', '.join(selects)} FROM {scan_sql(ds)}"
    cur = cursor()
    try:
        row = cur.execute(sql, [ds.inventory.files.to_pylist()]).fetchone()
    finally:
        cur.close()
    idx = 0
    for c in cols:
        entry = {"type": ds.columns[c], "min": row[idx], "max": row[idx + 1]}
        idx += 2
        if distinct:
            entry["approx_distinct"] = int(row[idx])
            idx += 1
        out[c] = entry
    return out
