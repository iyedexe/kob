"""kob test suite — plain Python, no test-framework dependency.

Run:   make test   (= uv run --extra client python tests/test_kob.py; pytest works too)

Each test builds a throwaway Parquet tree under a tmp dir and points the catalog
at it, exercising discovery, the query contract, ranges, the engine, REST mode and
the JSON edge cases. `test_end_to_end_rest_and_flight_servers` starts real REST and
Flight servers in-process (skipped without the [client] extra).
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
# start / end date range
# --------------------------------------------------------------------------- #
def test_date_range_is_inclusive_and_explicit():
    import datetime as dt

    _reset("closes")
    days = [dt.date(2024, 1, 1) + dt.timedelta(days=i) for i in range(10)]
    _write("closes/year=2024/p.parquet", pa.table({
        "listed": [dt.date(2000, 1, 1)] * 10, "trade_date": days, "close": [float(i) for i in range(10)]}))
    ds = catalog.get_dataset("closes")
    assert set(ds.date_columns) == {"listed", "trade_date"}
    assert "date_column" not in ds.to_public_dict()  # no implicit default column
    r = lambda **b: {"trade_date": b}  # noqa: E731
    assert _q(dataset="closes", ranges=r(start="2024-01-03", end="2024-01-05"))["close"].to_pylist() \
        == [2.0, 3.0, 4.0]  # both ends inclusive
    assert _q(dataset="closes", ranges=r(start="2024-01-09")).num_rows == 2
    assert _q(dataset="closes", ranges=r(end="2024-01-01")).num_rows == 1
    assert _q(dataset="closes", ranges={"listed": {"start": "2000-01-01", "end": "2000-01-01"}}).num_rows == 10
    for bad in (r(start="2024-01-05", end="2024-01-01"), r(start="not-a-date")):
        try:
            build_sql(QueryRequest(dataset="closes", ranges=bad))
            raise AssertionError(f"expected ValueError for {bad}")
        except ValueError:
            pass


def test_top_level_start_end_rejected_not_ignored():
    # A dropped date bound would silently return the whole history — must be an error.
    for legacy in ({"start": "2024-01-01"}, {"end": "2024-01-01"}, {"date_column": "trade_date"}):
        try:
            QueryRequest.from_dict({"dataset": "closes", **legacy})
            raise AssertionError(f"expected ValueError for {legacy}")
        except ValueError as exc:
            assert "ranges" in str(exc)


def test_end_date_covers_whole_day_on_timestamps():
    import datetime as dt

    _reset("ticks")
    ts = [dt.datetime(2024, 1, 1, 9), dt.datetime(2024, 1, 1, 17, 30), dt.datetime(2024, 1, 2, 9)]
    _write("ticks/p.parquet", pa.table({"ts": ts, "px": [1.0, 2.0, 3.0]}))
    assert _q(dataset="ticks", ranges={"ts": {"start": "2024-01-01", "end": "2024-01-01"}}).num_rows == 2
    assert _q(dataset="ticks", ranges={"ts": {"end": "2024-01-01T12:00:00"}}).num_rows == 1


def test_iso_date_partition_is_a_date_column():
    _reset("daily")
    for d in ("2024-01-01", "2024-01-02", "2024-01-03"):
        _write(f"daily/dt={d}/p.parquet", pa.table({"v": [1]}))
    assert catalog.get_dataset("daily").date_columns == ("dt",)
    assert _q(dataset="daily", ranges={"dt": {"start": "2024-01-02", "end": "2024-01-03"}}).num_rows == 2


def test_ranges_on_any_date_column():
    import datetime as dt

    from kob.server.rest import create_rest_app, request_from_query_params

    _reset("divs")
    ex = [dt.date(2024, m, 1) for m in (1, 2, 3, 4)]
    pay = [dt.date(2024, m, 15) for m in (2, 3, 4, 5)]
    _write("divs/p.parquet", pa.table({"ex_date": ex, "pay_date": pay, "amount": [1.0, 2.0, 3.0, 4.0]}))
    ds = catalog.get_dataset("divs")
    assert set(ds.date_columns) == {"ex_date", "pay_date"}
    # Two date columns at once, AND-ed; open-ended sides allowed.
    out = _q(dataset="divs", ranges={"ex_date": {"start": "2024-02-01", "end": "2024-04-01"},
                                     "pay_date": {"end": "2024-04-15"}})
    assert out["amount"].to_pylist() == [2.0, 3.0]
    # JSON round trip (Flight / POST)
    req = QueryRequest.from_dict({"dataset": "divs", "ranges": {"pay_date": {"start": "2024-05-15"}}})
    assert engine.execute_reader(req).read_all()["amount"].to_pylist() == [4.0]
    for bad in ({"amount": {"start": "2024-01-01"}},            # not a number
                {"ex_date": {"start": "2024-05-01", "end": "2024-01-01"}},
                {"ex_date": {"from": "2024-01-01"}}):            # unknown key
        try:
            build_sql(QueryRequest.from_dict({"dataset": "divs", "ranges": bad}))
            raise AssertionError(f"expected ValueError for {bad}")
        except ValueError:
            pass
    # REST: <col>.start / <col>.end in the generated spec and the GET parser
    try:
        app = create_rest_app()
        names = {p["name"] for p in app.openapi()["paths"]["/data/divs"]["get"]["parameters"]}
        assert {"ex_date.start", "ex_date.end", "pay_date.start", "pay_date.end"} <= names
        assert "ex_date.min" not in names
        req = request_from_query_params(catalog.get_dataset("divs"),
                                        [("ex_date.start", "2024-03-01"), ("pay_date.end", "")])
        assert req.ranges == {"ex_date": {"start": "2024-03-01", "end": None}}
        assert engine.execute_reader(req).read_all().num_rows == 2
    finally:
        catalog.enable_auto_discovery()


def test_numeric_and_integer_partition_ranges():
    _reset("nums2")
    for y in (2022, 2023, 2024):
        _write(f"nums2/year={y}/region=eu/p.parquet", pa.table({"px": [10.0, 20.0, 30.0], "qty": [1, 2, 3]}))
    ds = catalog.get_dataset("nums2")
    assert ds.sliceable_columns == {"px": "number", "qty": "number", "year": "number"}
    assert "region" not in ds.sliceable_columns  # string partition: not sliceable
    out = _q(dataset="nums2", ranges={"year": {"start": 2023}, "px": {"start": 15, "end": 30.0},
                                      "qty": {"end": "2"}})  # numeric strings accepted
    assert out.num_rows == 2 and set(out["year"].to_pylist()) == {2023, 2024}
    for bad in ({"region": {"start": "a"}}, {"px": {"start": 30, "end": 10}}, {"px": {"start": "abc"}}):
        try:
            build_sql(QueryRequest(dataset="nums2", ranges=bad))
            raise AssertionError(f"expected ValueError for {bad}")
        except ValueError:
            pass


def test_python_client_slicing():
    import datetime as dt

    try:
        from kob.tools.client import KobClient
    except ModuleNotFoundError:  # the [client] extra (requests) isn't installed
        print("        (skipped: needs the [client] extra)")
        return
    _reset("divs2")
    _write("divs2/p.parquet", pa.table({"ex_date": [dt.date(2024, m, 1) for m in (1, 2, 3, 4)]}))
    om = KobClient("http://unused")["divs2"]
    req = om.request(om.ex_date["2024-02-01":dt.date(2024, 4, 1)], om["pay_date"][:"2024-04-15"],
                     amount=slice(1.5, None), cp_flag="C", region=["eu", "us"], limit=5)
    assert req.ranges == {"ex_date": {"start": "2024-02-01", "end": dt.date(2024, 4, 1)},
                          "pay_date": {"start": None, "end": "2024-04-15"},
                          "amount": {"start": 1.5, "end": None}}
    assert [(f.column, f.op, f.value) for f in req.filters] == \
        [("cp_flag", "=", "C"), ("region", "in", ["eu", "us"])]
    assert req.to_dict()["ranges"]["ex_date"]["end"] == "2024-04-01"  # JSON-safe on the wire
    # The slice is inclusive at both ends, end-to-end through the engine:
    out = engine.execute_reader(om.request(om.ex_date["2024-02-01":"2024-04-01"])).read_all()
    assert out.num_rows == 3
    for bad in (lambda: om.ex_date["a":"b":2], lambda: om.ex_date["2024-01-01"],
                lambda: om.request(om.x[1:2], om.x[3:4])):
        try:
            bad()
            raise AssertionError("expected an error")
        except (TypeError, ValueError):
            pass


# --------------------------------------------------------------------------- #
# Inventory: files listed + footers read once; queries planned in memory
# --------------------------------------------------------------------------- #
def _segmented(name: str) -> None:
    """name/year=Y/dt=D/sym=S/part-0.parquet: 2 years x 3 days x 2 symbols = 12 files."""
    import datetime as dt

    _reset(name)
    for y in (2023, 2024):
        for d in (1, 2, 3):
            day = dt.date(y, 1, d)
            for s in ("A", "B"):
                ts = [dt.datetime.combine(day, dt.time(h)) for h in (9, 10)]
                _write(f"{name}/year={y}/dt={day.isoformat()}/sym={s}/part-0.parquet",
                       pa.table({"ts": ts, "px": [float(y + d), float(y + d) + 0.5], "qty": [d, d * 10]}))


def test_queries_never_list_files_and_prune_in_memory():
    from kob.core import inventory
    from kob.core.contract import plan_files

    _segmented("seg")
    try:
        catalog.rescan()
        ds = catalog.get_dataset("seg")
        inv = ds.inventory
        assert inv.complete and inv.uniform_schema and len(inv.files) == 12 and inv.total_rows == 24
        assert ds.sliceable_columns["dt"] == "date" and ds.sliceable_columns["year"] == "number"
        assert not ds.union_by_name  # uniform schema: DuckDB needn't open every footer
        assert ds.hive_types == {"year": "BIGINT", "dt": "DATE", "sym": "VARCHAR"}

        def files(**kw):
            return [f.split("/seg/")[1].rsplit("/", 1)[0] for f in plan_files(QueryRequest("seg", **kw), ds)]

        assert files(filters=[Filter("sym", "=", "A"), Filter("dt", "=", "2024-01-02")]) == \
            ["year=2024/dt=2024-01-02/sym=A"]
        assert len(files(filters=[Filter("year", "in", [2023])])) == 6
        assert len(files(filters=[Filter("year", ">=", 2024), Filter("sym", "!=", "A")])) == 6  # != never prunes
        assert len(files(ranges={"dt": {"start": "2023-01-02", "end": "2023-01-03"}})) == 4
        # Footer min/max: no partition filter, yet only the files whose stats can match.
        assert len(files(ranges={"ts": {"start": "2024-01-03", "end": "2024-01-03"}})) == 2
        assert len(files(ranges={"qty": {"start": 30}})) == 4                  # max(qty)=30 on day 3
        assert len(files(filters=[Filter("px", "=", 2026.5)])) == 4             # 2024+2 .. +0.5
        assert files(filters=[Filter("px", ">", 9999.0)]) == []
        assert len(files(filters=[Filter("qty", ">=", 29.5)])) == 12  # can't cast to int: keep all

        # The engine agrees with a brute-force filter of a full scan, file pruning or not.
        full = _q(dataset="seg")
        for kw in ({"ranges": {"ts": {"start": "2024-01-03", "end": "2024-01-03"}}},
                   {"filters": [Filter("qty", ">=", 20), Filter("year", "=", 2023)]},
                   {"filters": [Filter("px", ">", 9999.0)]}):
            assert _q(dataset="seg", **kw).num_rows == full.filter(_mask(full, kw)).num_rows, kw
        empty = _q(dataset="seg", filters=[Filter("px", ">", 9999.0)], columns=["ts", "sym"])
        assert empty.num_rows == 0 and empty.column_names == ["ts", "sym"]

        # facets min/max straight from the footers
        f = engine.column_facets("seg", ["qty", "ts"])
        assert f["qty"]["min"] == 1 and f["qty"]["max"] == 30

        # Unknown stats never drop a file.
        lo, hi = inv.stats["qty"]
        holes = inventory.Inventory(files=inv.files, partitions=inv.partitions, complete=True,
                                    stats={"qty": (pa.array([None] * 12, lo.type), hi)})
        assert len(inventory.prune(holes, [("qty", ">=", 30)])) == 12 - 8  # min null → kept by max
    finally:
        catalog.enable_auto_discovery()


def _mask(table: pa.Table, kw: dict):
    import datetime as dt

    import pyarrow.compute as pc

    m = pa.array([True] * table.num_rows)
    for f in kw.get("filters", []):
        fn = {">=": pc.greater_equal, ">": pc.greater, "=": pc.equal}[f.op]
        m = pc.and_(m, fn(table[f.column], f.value))
    for col, b in kw.get("ranges", {}).items():
        day = dt.date.fromisoformat(b["start"])
        lo, hi = dt.datetime.combine(day, dt.time()), dt.datetime.combine(day, dt.time(23, 59, 59))
        m = pc.and_(m, pc.and_(pc.greater_equal(table[col], lo), pc.less_equal(table[col], hi)))
    return m


def test_inventory_schema_evolution_and_corrupt_files():
    _reset("evo2")
    _write("evo2/k=1/a.parquet", pa.table({"x": [1]}))
    _write("evo2/k=2/b.parquet", pa.table({"x": [2], "extra": [0.5]}))
    (_TMP / "evo2" / "k=3").mkdir()
    (_TMP / "evo2" / "k=3" / "broken.parquet").write_bytes(b"PAR1 not really")
    try:
        catalog.rescan()
        ds = catalog.get_dataset("evo2")
        assert not ds.inventory.uniform_schema and ds.union_by_name  # schemas differ → union
        assert len(ds.inventory.unreadable) == 1 and len(ds.inventory.files) == 2  # corrupt one excluded
        assert ds.partition_values["k"] == ["1", "2"]
        out = _q(dataset="evo2", columns=["x", "extra"])  # query works despite the corrupt file
        assert sorted(out["x"].to_pylist()) == [1, 2] and None in out["extra"].to_pylist()
        os.environ["KOB_UNION_BY_NAME"] = "0"
        assert not ds.union_by_name
    finally:
        os.environ.pop("KOB_UNION_BY_NAME", None)
        catalog.enable_auto_discovery()


def test_walk_and_footers_match_and_go_parallel_on_slow_storage():
    import time as _time

    from kob.core import inventory

    _segmented("seg2")
    root = str(_TMP / "seg2")
    ref = sorted((d, sorted(os.path.join(d, f) for f in fs if f.endswith(".parquet")))
                 for d, _, fs in os.walk(root))
    assert sorted(inventory.walk(root)) == ref

    calls = {"threads": set()}
    orig_scandir, orig_tail = inventory._scandir, inventory._tail

    def slow(fn):
        def wrapped(p):
            import threading
            calls["threads"].add(threading.get_ident())
            _time.sleep(0.002)  # a network round trip
            return fn(p)
        return wrapped
    try:
        inventory._scandir, inventory._tail = slow(orig_scandir), slow(orig_tail)
        assert sorted(inventory.walk(root)) == ref
        files = [f for _, fs in ref for f in fs] * 8  # > 64 probes, so the pool kicks in
        footers = inventory.read_footers(files)
        assert all(f.schema is not None for f in footers) and len(footers) == len(files)
        assert len(calls["threads"]) > 1, "slow storage should have used the thread pool"
    finally:
        inventory._scandir, inventory._tail = orig_scandir, orig_tail


# --------------------------------------------------------------------------- #
# kob-open-data builders (offline: inline CSV, no network)
# --------------------------------------------------------------------------- #
def test_open_data_builders():
    import datetime as dt

    import pyarrow.compute as pc

    from kob.tools import open_data as od

    fx, parts = od.build_fx_daily(b"Date,Country,Exchange rate\n"
                                  b"2024-06-03,Euro,0.9183\n2024-06-03,Japan,156.04\n"
                                  b"2024-06-04,Euro,\n")  # blank rate dropped
    assert parts == ["year"] and fx.num_rows == 2
    eur = fx.filter(pc.equal(fx["currency"], "EUR")).to_pylist()[0]
    assert eur["units_per_usd"] == 0.9183 and abs(eur["usd_per_unit"] - 1.0890) < 1e-3  # 1 EUR ≈ 1.09 USD
    assert eur["date"] == dt.date(2024, 6, 3) and eur["year"] == 2024

    mem, parts = od.build_index_membership(b'date,tickers\n2020-01-02,"AAPL,MSFT"\n'
                                           b'2020-01-02,"AAPL,MSFT"\n2020-01-03,"AAPL, GOOG"\n')
    assert parts == ["index", "year"]
    assert [(r["date"].day, r["ticker"]) for r in mem.to_pylist()] == \
        [(2, "AAPL"), (2, "MSFT"), (3, "AAPL"), (3, "GOOG")]  # deduped, trimmed, sorted

    per, _ = od.build_index_membership_periods(b"ticker,start_date,end_date\nA,2000-06-05,\n"
                                               b"AABA,1999-12-08,2017-06-19\n")
    assert per["end_date"].to_pylist() == [None, dt.date(2017, 6, 19)]  # still a member → null

    px, _ = od.build_prices_daily(b"date,open,high,low,close,volume,Name\n"
                                  b"2013-02-08,15.07,15.12,14.63,14.75,8407500,AAL\n")
    assert px.column_names == ["date", "ticker", "open", "high", "low", "close", "volume", "year"]
    assert od.BUILDERS.keys() <= od.SOURCES.keys()


def test_get_data_helpers_and_cli():
    import contextlib
    import datetime as dt
    import io as _io

    from kob.server.app import main
    from kob.tools import open_data as od

    # Binance klines: ms (≤2024) and µs (2025+) timestamps, with or without a header row.
    row = "{t},100.0,101.0,99.0,100.5,12.5,{c},1256.25,42,6.0,603.0,0\n"
    ms = row.format(t=1704067200000, c=1704067259999)                    # 2024-01-01 00:00 UTC
    us = row.format(t=1735689600000000, c=1735689659999999)              # 2025-01-01 00:00 UTC
    for raw, want in ((ms, dt.datetime(2024, 1, 1)), (us, dt.datetime(2025, 1, 1)),
                      ("open_time,open,high,low,close,volume,close_time,quote_volume,count,"
                       "taker_buy_volume,taker_buy_quote_volume,ignore\n" + us, dt.datetime(2025, 1, 1))):
        t = od.parse_klines(raw.encode())
        assert t.num_rows == 1 and t["open_time"][0].as_py() == want, t["open_time"][0]
        assert t["trades"][0].as_py() == 42 and t["close"][0].as_py() == 100.5

    assert od._months("2023-11", "2024-02") == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
    assert od._months("2022", "2022")[-1] == (2022, 12) and len(od._months("2022", "2022")) == 12
    assert od._shift("2024-01", -1) == "2023-12" and od._shift("2024-12", -11) == "2024-01"
    assert od.resolve("core") == od.GROUPS["core"]
    assert od.resolve("forex_ticks,heavy") == ["forex_ticks", "uk_house_prices", "crypto_klines_1m"]
    assert set(od.DATASETS) <= set(od.SOURCES)

    out = _io.StringIO()
    with contextlib.redirect_stdout(out):
        main(["--get-data", "list"])
    assert "forex_ticks" in out.getvalue() and "groups:" in out.getvalue()
    for bad in (["--get-data", "nope"], ["--get-data", "core", "--from", "2024/01"]):
        try:
            with contextlib.redirect_stderr(_io.StringIO()):
                main(bad)
            raise AssertionError(f"expected argparse error for {bad}")
        except SystemExit as exc:
            assert exc.code == 2


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_end_to_end_rest_and_flight_servers():
    # Real servers in-process: `kob --mode rest` (uvicorn) and Arrow Flight, queried with the
    # KobClient slicing API over both transports — the results must be identical.
    import datetime as dt
    import threading
    import time

    try:
        import requests
        import uvicorn

        from kob.tools.client import KobClient
    except ModuleNotFoundError:
        print("        (skipped: needs the [client] extra)")
        return
    from kob.server.flight import build_server
    from kob.server.rest import create_rest_app

    _reset("e2e")
    days = [dt.date(2024, 1, 1) + dt.timedelta(days=i) for i in range(60)]
    for region in ("eu", "us"):
        _write(f"e2e/region={region}/p.parquet", pa.table({
            "date": days, "px": [float(i) for i in range(60)], "sym": ["A", "B"] * 30}))

    flight_server, flight_url = build_server("127.0.0.1", _free_port())
    threading.Thread(target=flight_server.serve, daemon=True).start()
    rest_port = _free_port()
    rest = uvicorn.Server(uvicorn.Config(create_rest_app(), host="127.0.0.1", port=rest_port,
                                         log_level="warning"))
    threading.Thread(target=rest.run, daemon=True).start()
    base = f"http://127.0.0.1:{rest_port}"
    try:
        for _ in range(100):
            if rest.started:
                break
            time.sleep(0.05)
        assert rest.started, "REST server did not start"

        results = []
        for url in (base, flight_url):
            ds = KobClient(url, compression="zstd")["e2e"]
            t = ds.query(ds.date["2024-01-10":"2024-01-19"], ds.px[12:15], region=["eu"], sym="A",
                         columns=["date", "px", "sym", "region"])
            assert min(t["date"].to_pylist()) >= dt.date(2024, 1, 10)
            assert max(t["date"].to_pylist()) <= dt.date(2024, 1, 19)  # end is inclusive
            assert t["px"].to_pylist() == [12.0, 14.0] and set(t["region"].to_pylist()) == {"eu"}
            results.append(t)
        assert results[0].equals(results[1]), "REST and Flight returned different data"

        # REST extras: the generated Swagger, the GET form, reload, errors.
        kob = KobClient(base)
        spec = requests.get(f"{base}/openapi.json", timeout=10).json()
        get_params = {p["name"] for p in spec["paths"]["/data/e2e"]["get"]["parameters"]}
        assert {"date.start", "date.end", "px.min", "px.max", "region", "sym"} <= get_params
        r = requests.get(f"{base}/data/e2e", params={"date.start": "2024-02-01", "region": "us",
                                                     "format": "json"}, timeout=10)
        assert r.status_code == 200 and len(r.json()) == 29
        assert "Reload datasets" in requests.get(f"{base}/docs", timeout=10).text
        _write("e2e_new/p.parquet", pa.table({"x": [1]}))
        assert "e2e_new" in kob.reload()["datasets"]
        assert "/data/e2e_new" in requests.get(f"{base}/openapi.json", timeout=10).json()["paths"]
        for bad in (lambda: kob["e2e"].query(kob["e2e"].sym["A":"B"]),  # string: not sliceable
                    lambda: kob["nope"].query()):
            try:
                bad()
                raise AssertionError("expected an HTTP error")
            except requests.HTTPError:
                pass
        r = requests.get(f"{base}/data/e2e", params={"start": "2024-01-01"}, timeout=10)
        assert r.status_code == 400  # no implicit date column

        # Flight mode's Swagger app (manual discovery): Reload button + POST /reload, and
        # Flight sees the new dataset only after the reload.
        from kob.server.api import create_app

        swag_port = _free_port()
        swag = uvicorn.Server(uvicorn.Config(create_app(flight_url, manual_discovery=True),
                                             host="127.0.0.1", port=swag_port, log_level="warning"))
        threading.Thread(target=swag.run, daemon=True).start()
        for _ in range(100):
            if swag.started:
                break
            time.sleep(0.05)
        swag_base = f"http://127.0.0.1:{swag_port}"
        assert "Reload datasets" in requests.get(f"{swag_base}/docs", timeout=10).text
        _write("e2e_flight/p.parquet", pa.table({"x": [7]}))
        assert "e2e_flight" not in [d["name"] for d in requests.get(f"{swag_base}/datasets", timeout=10).json()["datasets"]]
        assert "e2e_flight" in requests.post(f"{swag_base}/reload", timeout=30).json()["datasets"]
        fl = KobClient(flight_url)["e2e_flight"]
        assert fl.query()["x"].to_pylist() == [7]
        swag.should_exit = True
    finally:
        rest.should_exit = True
        flight_server.shutdown()
        catalog.enable_auto_discovery()
        _reset("e2e_new")
        _reset("e2e_flight")


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
