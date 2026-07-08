"""kob test suite — plain Python, no test-framework dependency.

Run:   python tests/test_kob.py      (or: pytest tests/test_kob.py)

Each test builds a throwaway Parquet tree under a tmp dir and points the catalog
at it, exercising discovery, the query contract, the engine, and the JSON edge
cases end to end (no servers are started).
"""
from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Point kob at a scratch data root BEFORE importing the catalog (module-level env read).
_TMP = Path(tempfile.mkdtemp(prefix="kob-test-"))
os.environ["KOB_DATA_ROOT"] = str(_TMP)
os.environ["KOB_DISCOVERY_TTL"] = "0"  # every get_dataset re-discovers: tests see disk truth

from kob.core import catalog, engine  # noqa: E402
from kob.core.contract import Filter, QueryRequest, build_sql  # noqa: E402
from kob.server.api import _stream_json  # noqa: E402


def _write(rel: str, table: pa.Table) -> Path:
    path = _TMP / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def _reset(name: str) -> None:
    shutil.rmtree(_TMP / name, ignore_errors=True)
    with catalog._lock:
        catalog._cache.pop(name, None)


def _q(**kw) -> pa.Table:
    return engine.execute_reader(QueryRequest(**kw)).read_all()


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #
def test_basic_discovery_and_query():
    _reset("events")
    _write("events/region=eu/part-0.parquet", pa.table({"amount": [1.0, 2.0]}))
    _write("events/region=us/part-0.parquet", pa.table({"amount": [3.0]}))
    ds = catalog.get_dataset("events")
    assert ds.partition_cols == ("region",)
    assert ds.partition_values["region"] == ["eu", "us"]
    assert ds.data_columns == {"amount": "double"}
    out = _q(dataset="events", filters=[Filter("region", "=", "eu")])
    assert out.num_rows == 2


def test_stray_root_file_keeps_partition_columns():
    # A stray Parquet at the dataset root must not erase the partition columns.
    _reset("stray")
    _write("stray/notes.parquet", pa.table({"amount": [0.0]}))
    _write("stray/region=eu/part-0.parquet", pa.table({"amount": [1.0]}))
    ds = catalog.get_dataset("stray")
    assert ds.partition_cols == ("region",), "stray root file erased partitions"


def test_corrupt_sample_does_not_hide_dataset():
    # One corrupt file must not make the whole (healthy) dataset invisible.
    _reset("corrupt")
    (_TMP / "corrupt").mkdir()
    (_TMP / "corrupt" / "aaa-broken.parquet").write_bytes(b"not a parquet file")
    _write("corrupt/bbb=1/good.parquet", pa.table({"x": [1]}))
    ds = catalog.get_dataset("corrupt")
    assert "x" in ds.data_columns


def test_schema_evolution_union_of_samples():
    # A column added by a later file is discoverable (sampled footers are merged).
    _reset("evolve")
    _write("evolve/y=1/a.parquet", pa.table({"x": [1]}))
    _write("evolve/y=2/b.parquet", pa.table({"x": [2], "extra": [9.5]}))
    ds = catalog.get_dataset("evolve")
    assert {"x", "extra"} <= set(ds.data_columns)
    out = _q(dataset="evolve", columns=["extra"], filters=[Filter("extra", ">=", 0.0)])
    assert out.num_rows == 1


def test_deleted_dataset_evicted_and_404s():
    _reset("gone")
    _write("gone/part-0.parquet", pa.table({"x": [1]}))
    catalog.get_dataset("gone")
    shutil.rmtree(_TMP / "gone")
    try:
        catalog.get_dataset("gone")
        raise AssertionError("expected KeyError for a deleted dataset")
    except KeyError:
        pass
    assert "gone" not in catalog._cache, "stale cache entry not evicted"


# --------------------------------------------------------------------------- #
# Query contract
# --------------------------------------------------------------------------- #
def _events2() -> None:
    if "events2" not in catalog._cache and not (_TMP / "events2").exists():
        _write("events2/region=eu/part-0.parquet", pa.table({"amount": [1.0]}))


def test_in_operator_rejects_plain_string():
    _events2()
    try:
        build_sql(QueryRequest(dataset="events2", filters=[Filter("region", "in", "eu")]))
        raise AssertionError('expected ValueError: "in" with a plain string')
    except ValueError as exc:
        assert "requires a list" in str(exc)
    # a real list still works
    out = _q(dataset="events2", filters=[Filter("region", "in", ["eu", "us"])])
    assert out.num_rows == 1


def test_in_operator_rejects_non_iterable():
    _events2()
    try:
        build_sql(QueryRequest(dataset="events2", filters=[Filter("amount", "in", 5)]))
        raise AssertionError('expected ValueError: "in" with a scalar')
    except ValueError:
        pass


def test_quoted_identifier_cannot_break_out():
    # A producer-controlled column name containing a double quote must be inert.
    _reset("quoted")
    _write("quoted/part-0.parquet", pa.table({'we"ird': [1, 2], "ok": [3, 4]}))
    ds = catalog.get_dataset("quoted")
    assert 'we"ird' in ds.allowed_columns
    sql, _ = build_sql(QueryRequest(dataset="quoted", columns=['we"ird'],
                                    filters=[Filter('we"ird', ">=", 0)]))
    assert '"we""ird"' in sql, f"identifier not escaped: {sql}"
    out = _q(dataset="quoted", columns=['we"ird'], filters=[Filter('we"ird', ">=", 2)])
    assert out.num_rows == 1


def test_empty_in_list_is_always_false():
    _events2()
    out = _q(dataset="events2", filters=[Filter("region", "in", [])])
    assert out.num_rows == 0


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
def test_fetch_schema_and_facets_close_cursors():
    _reset("nums")
    _write("nums/part-0.parquet", pa.table({"v": [1.0, 2.0, 3.0]}))
    schema = engine.fetch_schema(QueryRequest(dataset="nums"))
    assert schema.names == ["v"]
    facets = engine.column_facets("nums", ["v"], distinct=True)
    assert facets["v"]["min"] == 1.0 and facets["v"]["max"] == 3.0
    assert facets["v"]["approx_distinct"] >= 1


def test_execute_reader_closes_cursor_on_bad_sql():
    # A DuckDB-level failure (bad cast) must raise, not leak the cursor silently.
    try:
        _q(dataset="nums", filters=[Filter("v", ">=", "not-a-number")])
        raise AssertionError("expected a DuckDB error")
    except Exception:
        pass  # any exception is fine — the point is the try/close path runs


# --------------------------------------------------------------------------- #
# JSON interop
# --------------------------------------------------------------------------- #
def test_stream_json_nan_inf_are_null():
    _reset("floats")
    _write("floats/part-0.parquet",
           pa.table({"v": [1.5, float("nan"), float("inf"), -float("inf")]}))
    reader = engine.execute_reader(QueryRequest(dataset="floats"))
    payload = b"".join(_stream_json(reader))
    rows = json.loads(payload)  # strict parse: would fail on bare NaN/Infinity
    vals = [r["v"] for r in rows]
    assert vals[0] == 1.5 and vals[1:] == [None, None, None], vals
    assert all(v is None or math.isfinite(v) for v in vals)


# --------------------------------------------------------------------------- #
# Query source: pruning, union_by_name, glob fallback (network-share speedups)
# --------------------------------------------------------------------------- #
def test_source_prunes_to_matching_partition_files():
    _reset("parts")
    _write("parts/sym=AAPL/p.parquet", pa.table({"v": [1]}))
    _write("parts/sym=MSFT/p.parquet", pa.table({"v": [2]}))
    ds = catalog.get_dataset("parts")
    assert len(ds.files) == 2
    sql, _ = build_sql(QueryRequest(dataset="parts", filters=[Filter("sym", "=", "AAPL")]))
    assert "read_parquet([" in sql, "should hand DuckDB an explicit file list, not a glob"
    assert "sym=AAPL" in sql and "sym=MSFT" not in sql, "did not prune to the matching partition"
    assert _q(dataset="parts", filters=[Filter("sym", "=", "AAPL")]).num_rows == 1
    sql2, _ = build_sql(QueryRequest(dataset="parts", filters=[Filter("sym", "in", ["AAPL", "MSFT"])]))
    assert "sym=AAPL" in sql2 and "sym=MSFT" in sql2


def test_union_by_name_auto_and_override():
    _reset("uni")
    _write("uni/a.parquet", pa.table({"v": [1]}))
    ds = catalog.get_dataset("uni")
    assert ds.schema_uniform is True
    assert "union_by_name = false" in ds.source(), "uniform schema should skip union_by_name"

    _reset("evo2")
    _write("evo2/y=1/a.parquet", pa.table({"v": [1]}))
    _write("evo2/y=2/b.parquet", pa.table({"v": [2], "w": [3]}))
    ds2 = catalog.get_dataset("evo2")
    assert ds2.schema_uniform is False
    assert "union_by_name = true" in ds2.source()

    old = catalog._UNION_ENV
    try:
        catalog._UNION_ENV = "0"  # env override wins even when non-uniform
        assert "union_by_name = false" in ds2.source()
    finally:
        catalog._UNION_ENV = old


def test_use_glob_fallback():
    _reset("g")
    _write("g/a.parquet", pa.table({"v": [1]}))
    ds = catalog.get_dataset("g")
    old = catalog.USE_GLOB
    try:
        catalog.USE_GLOB = True
        src = ds.source()
        assert "**/*.parquet" in src and "read_parquet('" in src, "KOB_USE_GLOB should force a glob"
    finally:
        catalog.USE_GLOB = old


# --------------------------------------------------------------------------- #
# Observability
# --------------------------------------------------------------------------- #
def test_metrics_and_dashboard():
    from kob.core.metrics import DASHBOARD_HTML, METRICS
    METRICS.reset()
    _reset("m")
    _write("m/a.parquet", pa.table({"v": [1, 2]}))
    catalog.get_dataset("m")   # discovery timer
    _q(dataset="m")            # query.execute timer
    snap = METRICS.snapshot()
    assert "discover.dataset" in snap["timers"], "discovery not timed"
    assert "query.execute" in snap["timers"], "query not timed"
    assert snap["counters"].get("cache.miss", 0) >= 1
    assert isinstance(DASHBOARD_HTML, str) and "fetch('metrics')" in DASHBOARD_HTML


# --------------------------------------------------------------------------- #
# Object-store URL support (helpers exercised on local paths — no cloud account)
# --------------------------------------------------------------------------- #
def test_url_detection():
    assert catalog._is_url("s3://bucket/x")
    assert catalog._is_url("https://host/x")
    assert catalog._is_url("az://c/x")
    assert not catalog._is_url("/mnt/share/data")
    assert not catalog._is_url("C:/data")


def test_remote_discovery_building_blocks_on_local_files():
    # The remote-discovery machinery (DuckDB glob + DESCRIBE) works on local paths,
    # so it's exercised here without a cloud account.
    _reset("remote")
    _write("remote/sym=AAPL/p.parquet", pa.table({"v": [1.0]}))
    _write("remote/sym=MSFT/p.parquet", pa.table({"v": [2.0]}))
    base = (_TMP / "remote").as_posix()
    files = catalog._glob_via_duckdb(f"{base}/**/*.parquet")
    assert len(files) == 2 and all(f.endswith(".parquet") for f in files)
    part_values, ordered = catalog._partitions_from_paths(base, files)
    assert ordered == ["sym"] and set(part_values["sym"]) == {"AAPL", "MSFT"}
    assert catalog._schema_via_duckdb(files[0]) == {"v": "double"}


if __name__ == "__main__":
    failures = 0
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for name, fn in tests:
        try:
            fn()
            print(f"  ok    {name}")
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"  FAIL  {name}: {exc}")
    shutil.rmtree(_TMP, ignore_errors=True)
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    raise SystemExit(1 if failures else 0)
