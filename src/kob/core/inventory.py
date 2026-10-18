"""File inventory: what a dataset's Parquet files contain, taken once at discovery.

On a network drive / UNC share / object-store mount every directory listing, ``stat`` and
footer read is a round trip, and an over-partitioned Hive tree has tens of thousands of
them. Handing DuckDB ``<root>/**/*.parquet`` made it re-list the whole tree and (with
``union_by_name``) open every footer on *every* query. Instead, discovery records:

* the **file list** with each file's partition values (from the folder names);
* every file's **footer**, read once and in parallel (``KOB_SCAN_THREADS``): the exact
  schema union, whether all files share one schema, row counts, and per-file **min/max
  statistics** for every column that has them.

Queries are then planned in memory: :func:`prune` drops files whose partition values or
min/max cannot match the filters/ranges (vectorised with ``pyarrow.compute``), and DuckDB
gets an explicit file list — no listing, no glob, footers only for files that can match.
DuckDB still applies every filter itself; pruning only decides which files it opens, and
it is conservative (unknown stats, odd types or unsupported operators never drop a file).
"""

from __future__ import annotations

import datetime as dt
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCAN_THREADS = int(os.environ.get("KOB_SCAN_THREADS", "32"))
MAX_IN_VALUES_TO_PRUNE = 1000  # bigger IN lists skip stats pruning (DuckDB still filters)


@dataclass(frozen=True)
class Inventory:
    files: pa.Array                                  # string, posix paths
    partitions: dict[str, pa.Array]                  # key -> string values per file (null = absent)
    stats: dict[str, tuple[pa.Array, pa.Array]] = field(default_factory=dict)  # col -> (min, max)
    rows: pa.Array | None = None                     # int64 row count per file
    complete: bool = False                           # every footer read (vs sampled)
    uniform_schema: bool = True                      # all (read) files share one schema
    schema_files: tuple[str, ...] = ()               # one file per distinct schema
    unreadable: tuple[str, ...] = ()                 # footers that failed to read (excluded)

    @property
    def total_rows(self) -> int | None:
        return int(pc.sum(self.rows).as_py() or 0) if self.rows is not None else None


@dataclass
class Footer:
    path: str
    schema: pa.Schema | None
    rows: int = 0
    stats: dict[str, tuple[Any, Any]] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Reading footers (discovery time only)
# --------------------------------------------------------------------------- #
FOOTER_TAIL_BYTES = 64 * 1024   # one read usually catches the whole footer
_SLOW_READ_S = 0.0005            # mean footer read above this ⇒ remote storage ⇒ go parallel


def _tail(path: str) -> bytes | None:
    """Raw footer bytes (``PAR1`` + metadata + length + ``PAR1``) — the only I/O at discovery.

    Opened through Python's ``open()``: pyarrow's own path handling chokes on Windows UNC
    paths and non-ASCII characters, which Python handles correctly. File reads release the
    GIL, so a thread pool overlaps network round trips."""
    try:
        with open(path, "rb") as fh:
            size = fh.seek(0, os.SEEK_END)
            if size < 12:
                return None
            fh.seek(max(0, size - FOOTER_TAIL_BYTES))
            buf = fh.read()
            if buf[-4:] != b"PAR1":
                return None
            n = int.from_bytes(buf[-8:-4], "little")
            if n + 8 > len(buf):  # footer bigger than the first read: one more read
                fh.seek(size - n - 8)
                buf = fh.read()
        return b"PAR1" + buf[-(n + 8):]
    except OSError:
        return None


def _parse(path: str, raw: bytes | None, with_stats: bool) -> Footer:
    """Footer bytes → schema, row count, per-column min/max (None if unknown)."""
    if raw is None:
        return Footer(path, None)
    try:
        md = pq.read_metadata(pa.BufferReader(raw))
        schema = md.schema.to_arrow_schema()
    except Exception:
        return Footer(path, None)
    footer = Footer(path, schema, md.num_rows)
    if not with_stats or md.num_row_groups == 0:
        return footer
    names = [md.schema.column(j).path for j in range(md.num_columns)]
    for j, name in enumerate(names):
        if "." in name:
            continue  # nested field: not a filterable top-level column
        lo = hi = None
        for i in range(md.num_row_groups):
            st = md.row_group(i).column(j).statistics
            if st is None or not st.has_min_max:
                lo = hi = None
                break
            try:
                lo = st.min if lo is None or st.min < lo else lo
                hi = st.max if hi is None or st.max > hi else hi
            except TypeError:
                lo = hi = None
                break
        if lo is not None and hi is not None:
            footer.stats[name] = (lo, hi)
    return footer


def _scandir(path: str) -> tuple[list[str], list[str]]:
    """(sub-directories, .parquet files) of one directory, sorted; [] on error."""
    dirs, files = [], []
    try:
        with os.scandir(path) as it:
            for e in it:
                if e.is_dir(follow_symlinks=False):
                    if not e.name.startswith("."):
                        dirs.append(e.path)
                elif e.name.endswith(".parquet"):
                    files.append(e.path)
    except OSError:
        pass
    return sorted(dirs), sorted(files)


def walk(root: str) -> list[tuple[str, list[str]]]:
    """``(directory, parquet files)`` for every directory under ``root``, breadth first.

    Each directory listing is a network round trip on a share, and an over-partitioned
    Hive tree has thousands of them: a level's directories are listed in parallel
    (``KOB_SCAN_THREADS``) once listings prove slow; on local disk it stays serial."""
    out: list[tuple[str, list[str]]] = []
    level, parallel, timed, spent = [root], False, 0, 0.0
    with ThreadPoolExecutor(max(SCAN_THREADS, 1)) as pool:
        while level:
            if parallel:
                listed = list(pool.map(_scandir, level))
            else:
                listed = []
                for d in level:  # serial, timing each listing until we know the storage
                    t0 = time.perf_counter()
                    listed.append(_scandir(d))
                    spent, timed = spent + time.perf_counter() - t0, timed + 1
                    if SCAN_THREADS > 1 and timed >= 4 and spent / timed > _SLOW_READ_S:
                        parallel = True  # slow storage: list the rest of this level in parallel
                        listed += list(pool.map(_scandir, level[len(listed):]))
                        break
            nxt = []
            for d, (subdirs, files) in zip(level, listed):
                out.append((d, files))
                nxt += subdirs
            level = nxt
    return out


def read_footer(path: str, *, with_stats: bool = True) -> Footer:
    return _parse(path, _tail(path), with_stats)


def read_footers(paths: list[str], *, with_stats: bool = True) -> list[Footer]:
    """Read many footers. Adaptive: the first reads are timed serially; if storage is slow
    (network drive, UNC share, object mount) the rest of the I/O goes to a thread pool of
    ``KOB_SCAN_THREADS``. Parsing stays on this thread either way (no GIL contention)."""
    probe = min(len(paths), 64)
    t0 = time.perf_counter()
    raws = [_tail(p) for p in paths[:probe]]
    rest = paths[probe:]
    if rest:
        slow = (time.perf_counter() - t0) / max(probe, 1) > _SLOW_READ_S
        if slow and SCAN_THREADS > 1:
            with ThreadPoolExecutor(SCAN_THREADS) as pool:
                raws += list(pool.map(_tail, rest))
        else:
            raws += [_tail(p) for p in rest]
    return [_parse(p, raw, with_stats) for p, raw in zip(paths, raws)]


def build(files: list[tuple[str, dict[str, str]]], part_keys: list[str], footers: list[Footer],
          *, complete: bool) -> Inventory:
    """Assemble an inventory from ``(path, partition values)`` pairs and the footers read."""
    by_path = {f.path: f for f in footers}
    unreadable = tuple(f.path for f in footers if f.schema is None)
    if complete:  # every footer was read: drop corrupt files instead of failing queries
        bad = set(unreadable)
        files = [(p, parts) for p, parts in files if p not in bad]
    paths = [p for p, _ in files]

    schemas: dict[pa.Schema, str] = {}
    for f in footers:
        if f.schema is not None:
            # Compare data columns only (a partition key duplicated in a file doesn't count).
            key = pa.schema([fl for fl in f.schema if fl.name not in part_keys])
            schemas.setdefault(key, f.path)

    stats: dict[str, tuple[pa.Array, pa.Array]] = {}
    rows = None
    if complete:
        rows = pa.array([by_path[p].rows if p in by_path else None for p in paths], pa.int64())
        types: dict[str, pa.DataType] = {}
        for schema in schemas:
            for fl in schema:
                types.setdefault(fl.name, fl.type)
        for col, typ in types.items():
            if not (pa.types.is_integer(typ) or pa.types.is_floating(typ) or pa.types.is_decimal(typ)
                    or pa.types.is_temporal(typ) or pa.types.is_string(typ)
                    or pa.types.is_large_string(typ)):
                continue
            lows, highs = [], []
            for p in paths:
                lo_hi = by_path[p].stats.get(col) if p in by_path else None
                lows.append(lo_hi[0] if lo_hi else None)
                highs.append(lo_hi[1] if lo_hi else None)
            if all(v is None for v in lows):
                continue
            try:
                stats[col] = (pa.array(lows, typ), pa.array(highs, typ))
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError, OverflowError):
                continue  # mixed types across files (schema evolution): no stats pruning

    return Inventory(
        files=pa.array(paths, pa.string()),
        partitions={k: pa.array([parts.get(k) for _, parts in files], pa.string()) for k in part_keys},
        stats=stats,
        rows=rows,
        complete=complete,
        uniform_schema=len(schemas) <= 1,
        schema_files=tuple(schemas.values()),
        unreadable=unreadable,
    )


# --------------------------------------------------------------------------- #
# Query-time pruning (in memory, no I/O)
# --------------------------------------------------------------------------- #
def _bound(value: Any, typ: pa.DataType) -> pa.Scalar | None:
    """A filter value as a scalar of the column's type, or None if it can't be (→ no pruning)."""
    try:
        if isinstance(value, str) and (pa.types.is_temporal(typ)):
            value = dt.datetime.fromisoformat(value) if len(value) > 10 else dt.date.fromisoformat(value)
        if isinstance(value, dt.date) and not isinstance(value, dt.datetime) and pa.types.is_timestamp(typ):
            value = dt.datetime.combine(value, dt.time())
        if pa.types.is_timestamp(typ) and isinstance(value, dt.datetime):
            if typ.tz and value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
        if isinstance(value, bool):
            return None
        return pa.scalar(value).cast(typ)  # safe cast: 99.5 → int64 raises → None
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, TypeError, ValueError,
            OverflowError):
        return None


def _may_match(lo: pa.Array, hi: pa.Array, op: str, value: Any) -> pa.Array | None:
    """Boolean mask: could a file with [lo, hi] contain a row where ``col op value``?"""
    if op == "in":
        if not isinstance(value, (list, tuple, set)) or not value or len(value) > MAX_IN_VALUES_TO_PRUNE:
            return None
        masks = [_may_match(lo, hi, "=", v) for v in value]
        if any(m is None for m in masks):
            return None
        out = masks[0]
        for m in masks[1:]:
            out = pc.or_(out, m)
        return out
    v = _bound(value, lo.type)
    if v is None:
        return None
    if op == "=":
        return pc.and_(pc.less_equal(lo, v), pc.greater_equal(hi, v))
    return {">": lambda: pc.greater(hi, v), ">=": lambda: pc.greater_equal(hi, v),
            "<": lambda: pc.less(lo, v), "<=": lambda: pc.less_equal(lo, v)}.get(op, lambda: None)()


def _partition_values(inv: Inventory, col: str, kind: str | None) -> pa.Array:
    arr = inv.partitions[col]
    if kind == "number":
        try:
            return pc.cast(arr, pa.int64())
        except pa.ArrowInvalid:
            return arr
    return arr  # ISO dates compare correctly as strings; plain strings for = / in


def _partition_scalar(value: Any, arr: pa.Array) -> Any:
    if pa.types.is_integer(arr.type):
        return int(value) if not isinstance(value, bool) else None
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()[:10] if not isinstance(value, dt.datetime) else value.date().isoformat()
    return str(value)


def _partition_may_match(arr: pa.Array, op: str, value: Any) -> pa.Array | None:
    try:
        if op == "in":
            if not isinstance(value, (list, tuple, set)) or not value:
                return None
            vals = [_partition_scalar(v, arr) for v in value]
            return pc.is_in(arr, value_set=pa.array(vals, arr.type))
        v = _partition_scalar(value, arr)
        if v is None:
            return None
        if op in (">", ">=", "<", "<=") and pa.types.is_string(arr.type) and not _looks_iso(v):
            return None  # only ISO-date strings order meaningfully
        fn = {"=": pc.equal, ">": pc.greater, ">=": pc.greater_equal,
              "<": pc.less, "<=": pc.less_equal}.get(op)
        return fn(arr, pa.scalar(v, arr.type)) if fn else None
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        return None


def _looks_iso(v: Any) -> bool:
    return isinstance(v, str) and len(v) >= 10 and v[4] == "-" and v[7] == "-"


def prune(inv: Inventory, predicates: list[tuple[str, str, Any]],
          sliceable: dict[str, str] | None = None) -> list[str]:
    """Files that may contain rows matching ALL ``(column, op, value)`` predicates."""
    mask = None
    for col, op, value in predicates:
        op = op.lower()
        if col in inv.partitions:
            cond = _partition_may_match(_partition_values(inv, col, (sliceable or {}).get(col)), op, value)
        elif col in inv.stats:
            lo, hi = inv.stats[col]
            cond = _may_match(lo, hi, op, value)
        else:
            cond = None
        if cond is None:
            continue
        cond = pc.fill_null(cond, True)  # unknown partition value / missing stats → keep
        mask = cond if mask is None else pc.and_(mask, cond)
    files = inv.files if mask is None else pc.filter(inv.files, mask)
    return files.to_pylist()


def column_min_max(inv: Inventory, col: str) -> tuple[Any, Any] | None:
    """Dataset-wide min/max from the footers alone, or None if any file lacks stats."""
    if col not in inv.stats or not inv.complete:
        return None
    lo, hi = inv.stats[col]
    if lo.null_count or hi.null_count or len(lo) == 0:
        return None
    return pc.min(lo).as_py(), pc.max(hi).as_py()
