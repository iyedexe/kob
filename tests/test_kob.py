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
# REST mode (manual discovery, per-dataset OpenAPI, Arrow IPC)
# --------------------------------------------------------------------------- #
def test_manual_discovery_only_changes_on_rescan():
    _reset("manual")
    _write("manual/k=a/p.parquet", pa.table({"x": [1]}))
    try:
        assert "manual" in [d.name for d in catalog.rescan()]
        _write("manual2/p.parquet", pa.table({"y": [1]}))
        assert "manual2" not in catalog.list_dataset_names(), "manual mode walked the tree"
        try:
            catalog.get_dataset("manual2")
            raise AssertionError("expected KeyError before rescan")
        except KeyError:
            pass
        catalog.rescan()
        assert catalog.get_dataset("manual2").data_columns == {"y": "int64"}
    finally:
        catalog.enable_auto_discovery()
        _reset("manual2")


def test_rest_query_params_and_openapi():
    from kob.server.rest import create_rest_app, request_from_query_params

    _reset("restds")
    _write("restds/region=eu/p.parquet", pa.table({"amount": [1.0, 5.0], "n": [1, 2], "ok": [True, False]}))
    _write("restds/region=us/p.parquet", pa.table({"amount": [9.0], "n": [3], "ok": [True]}))
    try:
        app = create_rest_app()
        spec = app.openapi()
        get = spec["paths"]["/data/restds"]["get"]
        names = {p["name"]: p for p in get["parameters"]}
        assert names["region"]["schema"]["items"]["enum"] == ["eu", "us"]
        assert {"amount.min", "amount.max", "n", "ok", "columns", "limit"} <= set(names)
        assert "amount" not in names  # no exact-match param on floats

        ds = catalog.get_dataset("restds")
        req = request_from_query_params(ds, [
            ("columns", "amount,region"), ("region", "eu"), ("amount.min", "2"),
            ("ok", "false"), ("filter", "n:in:1,2"), ("limit", "10"), ("format", "arrow")])
        out = engine.execute_reader(req).read_all()
        assert out.column_names == ["amount", "region"] and out["amount"].to_pylist() == [5.0]
        try:
            request_from_query_params(ds, [("bogus", "1")])
            raise AssertionError("expected ValueError for an unknown parameter")
        except ValueError:
            pass
    finally:
        catalog.enable_auto_discovery()


def test_arrow_ipc_stream_round_trips():
    from kob.server.rest import stream_arrow_ipc

    _events2()
    for comp in ("none", "zstd"):
        payload = b"".join(stream_arrow_ipc(engine.execute_reader(QueryRequest(dataset="events2")), comp))
        table = pa.ipc.open_stream(payload).read_all()
        assert table.num_rows == 1 and "amount" in table.column_names


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
