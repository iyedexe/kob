"""Filesystem auto-discovery catalog.

There is **no** hardcoded list of datasets. A producer microservice drops Parquet
under ``KOB_DATA_ROOT`` in a Hive-partitioned tree, e.g.::

    <root>/events/dt=2026-06-08/region=eu/part-0.parquet
    <root>/prices/symbol=BTCEUR/year=2026/part-0.parquet

and this module discovers, per dataset (a top-level folder containing Parquet):

* **partition columns + their values** — read straight from the ``key=value`` folder
  names (filesystem only, no data scan) — these are the "filter per folder" options;
* **data columns + types** — read from a few Parquet files' schemas (footers only,
  never data), so one corrupt file or an evolved schema can't hide the dataset.

Discovery is cached with a short TTL so freshly-dropped files/partitions appear without
a restart, while steady-state lookups are a single dict hit (all derived lookups are
precomputed at discovery time). The cache also holds the *file list*, so queries reuse
the walk instead of re-globbing the tree — a big win on network shares (see ``source``).

``KOB_DATA_ROOT`` may be a local path/mount **or** an object-store URL
(``s3://``, ``az://``, ``gs://``, ``https://`` …); URLs are read natively through
DuckDB's httpfs/object-store readers rather than a FUSE mount.

Security: a dataset name is always a single path segment that must resolve *inside*
the data root, and only names found by discovery are accepted — there is no way to
escape the root via ``..`` or absolute paths.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .metrics import METRICS, timed

REPO_ROOT = Path(__file__).resolve().parents[3]
_RAW_DATA_ROOT = os.environ.get("KOB_DATA_ROOT", str(REPO_ROOT / "data"))


def _is_url(p: str) -> bool:
    # Object-store / HTTP schemes DuckDB reads natively (not a local FUSE mount).
    scheme = p.split("://", 1)[0].lower() if "://" in p else ""
    return scheme in {"s3", "s3a", "gs", "gcs", "az", "azure", "abfs", "abfss",
                      "http", "https", "r2"}


IS_URL_ROOT = _is_url(_RAW_DATA_ROOT)
# Local roots stay a Path (os.scandir / is_dir / path-join keep working); object-store
# roots stay a bare string handed straight to DuckDB.
URL_ROOT: str | None = _RAW_DATA_ROOT.rstrip("/") if IS_URL_ROOT else None
DATA_ROOT = Path(_RAW_DATA_ROOT) if IS_URL_ROOT else Path(_RAW_DATA_ROOT).resolve()
DATA_ROOT_DISPLAY = URL_ROOT if IS_URL_ROOT else str(DATA_ROOT)

# Re-scan a dataset at most this often (seconds). Bump it high on slow/remote roots so
# the tree walk isn't repeated constantly (new data still surfaces within one TTL).
DISCOVERY_TTL = float(os.environ.get("KOB_DISCOVERY_TTL", "15"))

# How many Parquet footers to sample per dataset at discovery (at most one per
# directory) — >1 makes discovery robust to a single corrupt/partial file and picks up
# schema-evolution columns, while staying metadata-only.
MAX_SAMPLE_FILES = int(os.environ.get("KOB_MAX_SAMPLE_FILES", "4"))

# --------------------------------------------------------------------------- #
# Query-source strategy (see Dataset.source)
#   KOB_UNION_BY_NAME    : "1"/"0" to force; unset = auto (on only when sampled
#                          files disagree on columns). union_by_name reconciles
#                          schemas but forces reading many footers — costly on
#                          network shares, so it's off by default for uniform data.
#   KOB_USE_GLOB         : "1" = always hand DuckDB a glob (live files) instead of
#                          the cached, partition-pruned file list.
#   KOB_MAX_INLINE_FILES : cap on the explicit file list before falling back to glob.
#   KOB_MAX_TRACKED_FILES: cap on files cached per dataset (memory bound).
# --------------------------------------------------------------------------- #
_UNION_ENV = os.environ.get("KOB_UNION_BY_NAME")
USE_GLOB = os.environ.get("KOB_USE_GLOB", "0") == "1"
MAX_INLINE_FILES = int(os.environ.get("KOB_MAX_INLINE_FILES", "8192"))
MAX_TRACKED_FILES = int(os.environ.get("KOB_MAX_TRACKED_FILES", "200000"))


@dataclass(frozen=True)
class Dataset:
    name: str
    root: str                                # posix path (local) or object-store URL base
    partition_cols: tuple[str, ...]
    partition_values: dict[str, list[str]]
    data_columns: dict[str, str]             # column -> friendly type (from Parquet schema)
    columns: dict[str, str]                  # data + virtual partition columns (precomputed)
    allowed_columns: frozenset[str]          # precomputed: hot path for filter validation
    file_count: int
    discovered_at: float
    files: tuple[str, ...] = ()              # discovered parquet paths, for query pruning
    schema_uniform: bool = True              # all sampled files had the same columns

    @property
    def glob(self) -> str:
        return f"{self.root}/**/*.parquet"

    def _read_parquet_opts(self) -> str:
        if _UNION_ENV is not None:
            union = _UNION_ENV.strip().lower() in ("1", "true", "yes", "on")
        else:
            union = not self.schema_uniform
        return f"hive_partitioning = true, union_by_name = {'true' if union else 'false'}"

    def source(self, filters=None) -> str:
        """The ``read_parquet(...)`` FROM-source for a query.

        Prefers an explicit, partition-pruned file list (from the cached walk) over
        a glob, so DuckDB never re-lists the tree over the network and only touches
        files a partition filter can't rule out. Falls back to a glob for object
        stores, huge datasets, or when ``KOB_USE_GLOB=1`` forces live-file semantics.
        """
        opts = self._read_parquet_opts()
        if self.files and not USE_GLOB:
            survivors = _prune_files(self, filters or [])
            if survivors and len(survivors) <= MAX_INLINE_FILES:
                arr = ", ".join("'" + f.replace("'", "''") + "'" for f in survivors)
                return f"read_parquet([{arr}], {opts})"
        glob = self.glob.replace("'", "''")
        return f"read_parquet('{glob}', {opts})"

    def to_public_dict(self, *, with_values: bool = True) -> dict:
        return {
            "name": self.name,
            "file_count": self.file_count,
            "partition_columns": [
                {"name": k, **({"values": self.partition_values.get(k, [])} if with_values else {})}
                for k in self.partition_cols
            ],
            "data_columns": [{"name": c, "type": t} for c, t in self.data_columns.items()],
        }


def _prune_files(ds: Dataset, filters) -> list[str]:
    """Keep only files a partition ``=``/``in`` filter can't rule out.

    Pure string matching on the cached paths (``/key=value/``) — no I/O. Other
    operators (ranges, LIKE) and non-partition columns are left to DuckDB.
    """
    survivors = list(ds.files)
    for f in filters:
        if f.column not in ds.partition_cols:
            continue
        op = f.op.lower()
        if op == "=":
            vals = {str(f.value)}
        elif op == "in" and isinstance(f.value, (list, tuple, set)):
            vals = {str(v) for v in f.value}
        else:
            continue
        needles = tuple(f"/{f.column}={v}/" for v in vals)
        survivors = [p for p in survivors if any(n in p for n in needles)]
    return survivors


# --------------------------------------------------------------------------- #
# Type mapping
# --------------------------------------------------------------------------- #
def _friendly(t: pa.DataType) -> str:
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_integer(t):
        return "int64"
    if pa.types.is_floating(t):
        return "double"
    if pa.types.is_decimal(t):
        return "decimal"
    if pa.types.is_temporal(t):
        return "date" if pa.types.is_date(t) else "timestamp"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    return str(t)


def _friendly_duck(t: str) -> str:
    """Map a DuckDB DESCRIBE type name to the same friendly types as ``_friendly``."""
    t = t.upper()
    if t == "BOOLEAN":
        return "bool"
    if t.startswith("DECIMAL"):
        return "decimal"
    if t in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
             "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"):
        return "int64"
    if t in ("FLOAT", "DOUBLE", "REAL"):
        return "double"
    if t == "DATE":
        return "date"
    if t.startswith("TIMESTAMP") or t == "TIME":
        return "timestamp"
    if t in ("VARCHAR", "TEXT", "STRING", "BLOB", "UUID"):
        return "string"
    return t.lower()


# --------------------------------------------------------------------------- #
# Discovery — local filesystem
# --------------------------------------------------------------------------- #
def _scan_tree(dataset_root: Path):
    part_values: dict[str, set[str]] = {}
    ordered: list[str] = []
    samples: list[str] = []
    files: list[str] = []
    file_count = 0
    for dirpath, _dirnames, filenames in os.walk(dataset_root):
        rel = os.path.relpath(dirpath, dataset_root)
        segs = [] if rel == "." else [s for s in rel.split(os.sep) if "=" in s]
        for seg in segs:
            k, v = seg.split("=", 1)
            part_values.setdefault(k, set()).add(v)
        pqs = [fn for fn in filenames if fn.endswith(".parquet")]
        file_count += len(pqs)
        # Canonical partition-key order comes from the first *partitioned* file's own
        # path — a stray Parquet at the dataset root must not erase it.
        if pqs and segs and not ordered:
            ordered = [s.split("=", 1)[0] for s in segs]
        if pqs and len(samples) < MAX_SAMPLE_FILES:
            samples.append(os.path.join(dirpath, pqs[0]))
        if len(files) < MAX_TRACKED_FILES:
            files.extend(Path(os.path.join(dirpath, fn)).as_posix() for fn in pqs)
    return ordered, part_values, samples, files, file_count


def _discover(name: str) -> Dataset | None:
    if IS_URL_ROOT:
        return _discover_remote(name)
    root = _safe_dataset_path(name)
    if root is None or not root.is_dir():
        return None
    with timed("discover.dataset", name):
        part_keys, part_values, samples, files, file_count = _scan_tree(root)
        if not samples:
            return None  # a folder with no Parquet is not a dataset
        # Merge the schemas of the sampled footers (metadata only — no data read):
        # one corrupt file no longer hides the dataset, and schema-evolution columns
        # are discovered. Track whether all sampled files agreed (drives union_by_name).
        data_columns: dict[str, str] = {}
        colsets: list[frozenset[str]] = []
        for sample in samples:
            try:
                # An open handle (not the path string) so pyarrow's own path handling
                # can't choke on UNC/network paths or non-ASCII characters.
                with open(sample, "rb") as fh:
                    schema = pq.read_schema(fh)
            except Exception:
                continue  # skip an unreadable candidate; another may work
            colsets.append(frozenset(schema.names))
            for f in schema:
                if f.name not in part_keys:
                    data_columns.setdefault(f.name, _friendly(f.type))
        if not data_columns:
            return None  # every sampled file was unreadable
        return _build_dataset(
            name, root.as_posix(), part_keys, part_values, data_columns, file_count,
            () if file_count > MAX_TRACKED_FILES else tuple(files),
            len(set(colsets)) <= 1,
        )


# --------------------------------------------------------------------------- #
# Discovery — object-store URL root (native, via DuckDB — no FUSE mount)
# --------------------------------------------------------------------------- #
def _glob_via_duckdb(pattern: str) -> list[str]:
    from . import engine  # lazy: avoids an import cycle

    cur = engine.cursor()
    try:
        rows = cur.execute("SELECT file FROM glob(?) ORDER BY file", [pattern]).fetchall()
    finally:
        cur.close()
    return [r[0] for r in rows]


def _schema_via_duckdb(file: str) -> dict[str, str] | None:
    from . import engine  # lazy: avoids an import cycle

    cur = engine.cursor()
    try:
        # hive_partitioning=false so we get the file's own columns (partition keys
        # come from _partitions_from_paths), matching the local pyarrow-footer path.
        rows = cur.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [file]
        ).fetchall()
    except Exception:
        return None
    finally:
        cur.close()
    return {r[0]: _friendly_duck(str(r[1])) for r in rows}  # (name, type, ...)


def _partitions_from_paths(base: str, files: list[str]):
    part_values: dict[str, set[str]] = {}
    ordered: list[str] = []
    blen = len(base.rstrip("/")) + 1
    for p in files:
        rel = p[blen:] if p.startswith(base) else p
        segs = [s for s in rel.split("/")[:-1] if "=" in s]
        for s in segs:
            k, v = s.split("=", 1)
            part_values.setdefault(k, set()).add(v)
        if segs and not ordered:
            ordered = [s.split("=", 1)[0] for s in segs]
    return part_values, ordered


def _discover_remote(name: str) -> Dataset | None:
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        return None
    base = f"{URL_ROOT}/{name}"
    with timed("discover.dataset", name):
        try:
            files = _glob_via_duckdb(f"{base}/**/*.parquet")
        except Exception:
            METRICS.incr("discover.error")
            return None
        if not files:
            return None
        part_values, ordered = _partitions_from_paths(base, files)
        data_columns: dict[str, str] = {}
        colsets: list[frozenset[str]] = []
        for s in files[:MAX_SAMPLE_FILES]:
            cols = _schema_via_duckdb(s)
            if cols is None:
                continue
            colsets.append(frozenset(cols))
            for n, t in cols.items():
                if n not in ordered:
                    data_columns.setdefault(n, t)
        if not data_columns:
            return None
        return _build_dataset(
            name, base, ordered, part_values, data_columns, len(files),
            () if len(files) > MAX_TRACKED_FILES else tuple(files),
            len(set(colsets)) <= 1,
        )


def _build_dataset(name, root_str, part_keys, part_values, data_columns,
                   file_count, files, schema_uniform) -> Dataset:
    columns = dict(data_columns)
    for k in part_keys:
        columns.setdefault(k, "string")  # Hive keys arrive as strings (DuckDB re-infers)
    return Dataset(
        name=name,
        root=root_str,
        partition_cols=tuple(part_keys),
        partition_values={k: sorted(part_values.get(k, set())) for k in part_keys},
        data_columns=data_columns,
        columns=columns,
        allowed_columns=frozenset(columns),
        file_count=file_count,
        discovered_at=time.monotonic(),
        files=files,
        schema_uniform=schema_uniform,
    )


def _safe_dataset_path(name: str) -> Path | None:
    """Resolve a dataset name to a path strictly inside DATA_ROOT, or None.

    ``name`` is always a single path segment (we reject anything containing a
    separator or ``..``), so ``DATA_ROOT / name`` cannot escape the root. We
    deliberately avoid ``Path.resolve()`` here: on Windows it can rewrite UNC /
    network paths into an inconsistent extended-length form, which made the old
    ``relative_to`` check raise and silently hide otherwise-valid datasets.
    """
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        return None
    return DATA_ROOT / name


# --------------------------------------------------------------------------- #
# TTL cache
# --------------------------------------------------------------------------- #
_lock = threading.Lock()
_cache: dict[str, Dataset] = {}


def _fresh(ds: Dataset) -> bool:
    return (time.monotonic() - ds.discovered_at) < DISCOVERY_TTL


def get_dataset(name: str, *, refresh: bool = False) -> Dataset:
    with _lock:
        cached = _cache.get(name)
        if cached is not None and not refresh and _fresh(cached):
            METRICS.incr("cache.hit")
            return cached
    METRICS.incr("cache.miss")
    ds = _discover(name)  # outside the lock (does IO)
    if ds is None:
        with _lock:
            # Evict any stale entry: a dataset deleted from disk must go 404,
            # not be served from cache until process restart.
            _cache.pop(name, None)
        raise KeyError(f"Unknown dataset '{name}' (no Parquet found under {DATA_ROOT_DISPLAY}/{name})")
    with _lock:
        _cache[name] = ds
    return ds


def list_dataset_names(*, refresh: bool = False) -> list[str]:
    """Top-level folders under the data root that contain at least one Parquet file."""
    with timed("discover.list"):
        if IS_URL_ROOT:
            return sorted(_list_remote_names(refresh=refresh))
        if not DATA_ROOT.is_dir():
            return []
        names = []
        for entry in os.scandir(DATA_ROOT):
            if entry.is_dir():
                try:
                    get_dataset(entry.name, refresh=refresh)
                    names.append(entry.name)
                except KeyError:
                    continue
        return sorted(names)


def _list_remote_names(*, refresh: bool = False) -> list[str]:
    try:
        files = _glob_via_duckdb(f"{URL_ROOT}/**/*.parquet")
    except Exception:
        METRICS.incr("discover.error")
        return []
    prefix = f"{URL_ROOT}/"
    candidates = sorted({
        p[len(prefix):].split("/", 1)[0]
        for p in files
        if p.startswith(prefix) and "/" in p[len(prefix):]  # a dataset folder, not a root file
    })
    ok = []
    for n in candidates:
        try:
            get_dataset(n, refresh=refresh)
            ok.append(n)
        except KeyError:
            continue
    return ok


def list_datasets(*, refresh: bool = False) -> list[Dataset]:
    return [get_dataset(n) for n in list_dataset_names(refresh=refresh)]
