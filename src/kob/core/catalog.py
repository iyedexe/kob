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
precomputed at discovery time).

**Manual mode** (used by ``kob --mode rest``): :func:`rescan` takes one snapshot of the
whole tree and from then on lookups are served from it only — no TTL, no tree walks on
the request path. The tree is re-walked only when :func:`rescan` is called again (the
Swagger "Reload" button). :func:`enable_auto_discovery` switches back to TTL mode.

Security: a dataset name is always a single path segment that must resolve *inside*
the data root, and only names found by discovery are accepted — there is no way to
escape the root via ``..`` or absolute paths.
"""

from __future__ import annotations

import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = Path(os.environ.get("KOB_DATA_ROOT", str(REPO_ROOT / "data"))).resolve()

# Re-scan a dataset at most this often (seconds). New files in existing partitions
# don't change the schema; new partitions/datasets surface within one TTL.
DISCOVERY_TTL = float(os.environ.get("KOB_DISCOVERY_TTL", "15"))

# How many Parquet footers to sample per dataset at discovery time (at most one
# per directory). >1 makes discovery robust to a single corrupt/partial file and
# picks up columns added by schema evolution, while staying metadata-only.
MAX_SAMPLE_FILES = int(os.environ.get("KOB_MAX_SAMPLE_FILES", "4"))


@dataclass(frozen=True)
class Dataset:
    name: str
    root: Path
    partition_cols: tuple[str, ...]
    partition_values: dict[str, list[str]]
    data_columns: dict[str, str]            # column -> friendly type (from Parquet schema)
    columns: dict[str, str]                  # data + virtual partition columns (precomputed)
    allowed_columns: frozenset[str]          # precomputed: hot path for filter validation
    file_count: int
    discovered_at: float
    # Columns usable for a start/end date range (date/timestamp data columns, plus partition
    # keys whose folder values are ISO dates), and the default one picked for `start`/`end`.
    date_columns: tuple[str, ...] = ()
    date_column: str | None = None

    @property
    def glob(self) -> str:
        # Forward slashes (as_posix) so DuckDB's globber matches on Windows
        # paths too — backslash separators and UNC "\\" confuse read_parquet.
        return (self.root / "**" / "*.parquet").as_posix()

    def to_public_dict(self, *, with_values: bool = True) -> dict:
        return {
            "name": self.name,
            "file_count": self.file_count,
            "partition_columns": [
                {"name": k, **({"values": self.partition_values.get(k, [])} if with_values else {})}
                for k in self.partition_cols
            ],
            "data_columns": [{"name": c, "type": t} for c, t in self.data_columns.items()],
            "date_column": self.date_column,
            "date_columns": list(self.date_columns),
        }


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


# --------------------------------------------------------------------------- #
# Date column detection (for start/end range queries)
# --------------------------------------------------------------------------- #
# Preferred names for the default date column, most specific first; otherwise the first
# date/timestamp column in schema order wins.
_DATE_NAME_PREFERENCE = ("date", "as_of_date", "asof_date", "trade_date", "value_date",
                         "price_date", "effective_date", "dt", "timestamp", "ts")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _date_columns(data_columns: dict[str, str], part_keys: list[str],
                  part_values: dict[str, set[str]]) -> tuple[tuple[str, ...], str | None]:
    cols = [c for c, t in data_columns.items() if t in ("date", "timestamp")]
    cols += [k for k in part_keys
             if part_values.get(k) and all(_ISO_DATE.match(v) for v in part_values[k])]
    by_lower = {c.lower(): c for c in cols}
    default = next((by_lower[n] for n in _DATE_NAME_PREFERENCE if n in by_lower),
                   cols[0] if cols else None)
    return tuple(cols), default


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #
def _scan_tree(dataset_root: Path) -> tuple[list[str], dict[str, set[str]], list[str], int]:
    part_values: dict[str, set[str]] = {}
    samples: list[str] = []
    ordered: list[str] = []
    file_count = 0
    for dirpath, _dirnames, filenames in os.walk(dataset_root):
        rel = os.path.relpath(dirpath, dataset_root)
        segs = [] if rel == "." else [s for s in rel.split(os.sep) if "=" in s]
        for seg in segs:
            k, v = seg.split("=", 1)
            part_values.setdefault(k, set()).add(v)
        pqs = [f for f in filenames if f.endswith(".parquet")]
        file_count += len(pqs)
        if pqs and len(samples) < MAX_SAMPLE_FILES:
            samples.append(os.path.join(dirpath, pqs[0]))
        # Canonical partition-key order comes from the first *partitioned* file's
        # own path — a stray Parquet at the dataset root must not erase it.
        if pqs and segs and not ordered:
            ordered = [s.split("=", 1)[0] for s in segs]
    return ordered, part_values, samples, file_count


def _discover(name: str) -> Dataset | None:
    root = _safe_dataset_path(name)
    if root is None or not root.is_dir():
        return None
    part_keys, part_values, samples, file_count = _scan_tree(root)
    if not samples:
        return None  # a folder with no Parquet is not a dataset
    # Merge the schemas of the sampled footers (metadata only — no data read):
    # one corrupt/partial file no longer hides the dataset, and columns added by
    # schema evolution are discovered if any sampled file carries them.
    data_columns: dict[str, str] = {}
    for sample in samples:
        try:
            # Read via an open file handle rather than handing pyarrow the path
            # string: pyarrow's own path handling chokes on Windows UNC/network
            # paths and on non-ASCII characters (accents, etc.), which silently
            # hides the dataset. Python's open() handles those correctly.
            with open(sample, "rb") as fh:
                schema = pq.read_schema(fh)
        except Exception:
            continue  # skip an unreadable/corrupt candidate; another may work
        for f in schema:
            if f.name not in part_keys:
                data_columns.setdefault(f.name, _friendly(f.type))
    if not data_columns:
        return None  # every sampled file was unreadable
    columns = dict(data_columns)
    for k in part_keys:
        columns.setdefault(k, "string")  # Hive keys arrive as strings (DuckDB re-infers)
    date_columns, date_column = _date_columns(data_columns, part_keys, part_values)
    return Dataset(
        name=name,
        root=root,
        partition_cols=tuple(part_keys),
        partition_values={k: sorted(part_values.get(k, set())) for k in part_keys},
        data_columns=data_columns,
        columns=columns,
        allowed_columns=frozenset(columns),
        file_count=file_count,
        discovered_at=time.monotonic(),
        date_columns=date_columns,
        date_column=date_column,
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
# When not None, discovery is manual: lookups are served from this snapshot only and
# the filesystem is walked again only by rescan().
_snapshot: dict[str, Dataset] | None = None
_snapshot_at: float | None = None  # wall-clock time of the last rescan()


def _fresh(ds: Dataset) -> bool:
    return (time.monotonic() - ds.discovered_at) < DISCOVERY_TTL


def get_dataset(name: str, *, refresh: bool = False) -> Dataset:
    snap = _snapshot
    if snap is not None:  # manual mode: never touch the filesystem (refresh is ignored)
        try:
            return snap[name]
        except KeyError:
            raise KeyError(f"Unknown dataset '{name}' (not in the last scan of {DATA_ROOT}; "
                           "reload to rescan)") from None
    with _lock:
        cached = _cache.get(name)
        if cached is not None and not refresh and _fresh(cached):
            return cached
    ds = _discover(name)  # outside the lock (does IO)
    if ds is None:
        with _lock:
            # Evict any stale entry: a dataset deleted from disk must go 404,
            # not be served from cache until process restart.
            _cache.pop(name, None)
        raise KeyError(f"Unknown dataset '{name}' (no Parquet found under {DATA_ROOT}/{name})")
    with _lock:
        _cache[name] = ds
    return ds


def list_dataset_names(*, refresh: bool = False) -> list[str]:
    """Top-level folders under the data root that contain at least one Parquet file."""
    snap = _snapshot
    if snap is not None:
        return sorted(snap)
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


def list_datasets(*, refresh: bool = False) -> list[Dataset]:
    return [get_dataset(n) for n in list_dataset_names(refresh=refresh)]


# --------------------------------------------------------------------------- #
# Manual discovery (snapshot) mode
# --------------------------------------------------------------------------- #
def rescan() -> list[Dataset]:
    """Walk the whole data root once and serve lookups from that snapshot only.

    Calling this switches the catalog into manual mode (if it was not already). The new
    snapshot is built off-lock and swapped in atomically, so in-flight requests keep
    seeing a consistent catalog while the walk runs.
    """
    global _snapshot, _snapshot_at
    found: dict[str, Dataset] = {}
    if DATA_ROOT.is_dir():
        for entry in os.scandir(DATA_ROOT):
            if entry.is_dir():
                ds = _discover(entry.name)
                if ds is not None:
                    found[entry.name] = ds
    with _lock:
        _snapshot = found
        _snapshot_at = time.time()
        _cache.clear()
    return [found[n] for n in sorted(found)]


def last_scan_time() -> float | None:
    """Unix time of the last :func:`rescan`, or None in automatic (TTL) mode."""
    return _snapshot_at if _snapshot is not None else None


def enable_auto_discovery() -> None:
    """Leave manual mode: back to TTL-cached, on-demand discovery."""
    global _snapshot, _snapshot_at
    with _lock:
        _snapshot = None
        _snapshot_at = None
        _cache.clear()
