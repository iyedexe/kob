"""DuckDB-backed execution engine.

DuckDB reads the Hive-partitioned Parquet directly and pushes projection, predicate
(row-group zonemap) and partition pruning into the scan, then streams results out as
Apache Arrow ``RecordBatch`` objects with no row-by-row conversion. Both servers call
:func:`execute_reader`; the wire transport is the only thing that differs between them.

Speed notes
-----------
* ``enable_object_cache`` keeps Parquet footers/metadata cached across queries, so
  repeated queries over the same tree skip the metadata read entirely.
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
            _conn = duckdb.connect(database=":memory:")
            threads = os.environ.get("KOB_DUCKDB_THREADS")
            if threads:
                _conn.execute(f"SET threads TO {int(threads)}")
            # Parallel scans don't need to preserve row order for analytical reads.
            _conn.execute("SET preserve_insertion_order = false")
            # Cache Parquet footers/metadata across queries (big win on repeated queries).
            try:
                _conn.execute("SET enable_object_cache = true")
            except duckdb.Error:
                pass  # setting renamed/removed in some versions — never fatal
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
    cur.execute(sql, params)
    duck_reader = _arrow_reader(cur, batch_rows)
    schema = duck_reader.schema

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
    cur.execute(f"SELECT * FROM ({sql}) AS _schema_probe LIMIT 0", params)
    return _arrow_reader(cur, 1).schema


def column_facets(dataset: str, columns: list[str], *, distinct: bool = False) -> dict[str, dict]:
    """Per-column min/max (and optional approx distinct count) for filter introspection.

    DuckDB resolves min/max from Parquet row-group statistics, so this is cheap even
    over a large tree. ``approx_count_distinct`` (HLL) is opt-in as it does scan.
    """
    from .catalog import get_dataset  # local import avoids a cycle

    ds = get_dataset(dataset)
    cols = [c for c in columns if c in ds.allowed_columns]
    if not cols:
        return {}
    selects = []
    for i, c in enumerate(cols):
        selects.append(f'min("{c}") AS mn{i}, max("{c}") AS mx{i}')
        if distinct:
            selects.append(f'approx_count_distinct("{c}") AS nd{i}')
    glob = ds.glob.replace("'", "''")
    sql = (f"SELECT {', '.join(selects)} "
           f"FROM read_parquet('{glob}', hive_partitioning = true, union_by_name = true)")
    cur = cursor()
    row = cur.execute(sql).fetchone()
    out: dict[str, dict] = {}
    idx = 0
    for c in cols:
        entry = {"type": ds.columns[c], "min": row[idx], "max": row[idx + 1]}
        idx += 2
        if distinct:
            entry["approx_distinct"] = int(row[idx])
            idx += 1
        out[c] = entry
    return out
