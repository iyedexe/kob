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
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

from . import inventory
from .inventory import Inventory

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
    # Sliceable columns -> kind ("date" | "timestamp" | "number"): the columns a request can
    # give an inclusive start/end range. Date/timestamp/numeric data columns, plus partition
    # keys whose folder values are all ISO dates or all integers.
    sliceable_columns: dict[str, str] = field(default_factory=dict)
    # Every file + partition values (+ footers' min/max when fully scanned): queries are
    # planned against this in memory and never list the tree (see core/inventory.py).
    inventory: Inventory | None = None

    @property
    def date_columns(self) -> tuple[str, ...]:
        return tuple(c for c, k in self.sliceable_columns.items() if k in ("date", "timestamp"))

    @property
    def union_by_name(self) -> bool:
        """DuckDB ``union_by_name`` only when files disagree on schema: with it on, DuckDB
        opens *every* listed footer to reconcile columns before reading a single row."""
        forced = os.environ.get("KOB_UNION_BY_NAME")
        if forced in ("0", "1"):
            return forced == "1"
        return self.inventory is None or not self.inventory.uniform_schema

    @property
    def hive_types(self) -> dict[str, str]:
        """Partition column types, so DuckDB never has to infer them from the paths."""
        kinds = {"number": "BIGINT", "date": "DATE"}
        return {k: kinds.get(self.sliceable_columns.get(k, ""), "VARCHAR") for k in self.partition_cols}

    def to_public_dict(self, *, with_values: bool = True) -> dict:
        return {
            "name": self.name,
            "file_count": self.file_count,
            "partition_columns": [
                {"name": k, **({"values": self.partition_values.get(k, [])} if with_values else {})}
                for k in self.partition_cols
            ],
            "data_columns": [{"name": c, "type": t} for c, t in self.data_columns.items()],
            "sliceable_columns": dict(self.sliceable_columns),
            **({"inventory": {
                "complete": self.inventory.complete,
                "uniform_schema": self.inventory.uniform_schema,
                "rows": self.inventory.total_rows,
                "columns_with_stats": len(self.inventory.stats),
                "unreadable_files": list(self.inventory.unreadable),
            }} if self.inventory is not None else {}),
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
# Sliceable columns (usable in start/end range queries)
# --------------------------------------------------------------------------- #
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INTEGER = re.compile(r"^-?\d+$")
_SLICE_KIND = {"date": "date", "timestamp": "timestamp",
               "int64": "number", "double": "number", "decimal": "number"}


def _sliceable_columns(data_columns: dict[str, str], part_keys: list[str],
                       part_values: dict[str, set[str]]) -> dict[str, str]:
    out = {c: _SLICE_KIND[t] for c, t in data_columns.items() if t in _SLICE_KIND}
    for k in part_keys:
        vals = part_values.get(k)
        if vals and all(_ISO_DATE.match(v) for v in vals):
            out[k] = "date"
        elif vals and all(_INTEGER.match(v) for v in vals):
            out[k] = "number"  # DuckDB infers integer Hive keys (year=2024) as BIGINT
    return out


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #
def _scan_tree(dataset_root: Path) -> tuple[list[str], dict[str, set[str]], list[tuple[str, dict[str, str]]]]:
    """One walk of the tree: every Parquet file (posix path) with its partition values."""
    part_values: dict[str, set[str]] = {}
    files: list[tuple[str, dict[str, str]]] = []
    ordered: list[str] = []
    for dirpath, pqs in inventory.walk(str(dataset_root)):
        rel = os.path.relpath(dirpath, dataset_root)
        segs = [] if rel == "." else [s for s in rel.split(os.sep) if "=" in s]
        parts = dict(seg.split("=", 1) for seg in segs)
        for k, v in parts.items():
            part_values.setdefault(k, set()).add(v)
        # Forward slashes (as_posix): DuckDB's Hive parsing and Windows UNC paths both want them.
        files += [(Path(f).as_posix(), parts) for f in pqs]
        # Canonical partition-key order comes from the first *partitioned* file's
        # own path — a stray Parquet at the dataset root must not erase it.
        if pqs and segs and not ordered:
            ordered = list(parts)
    return ordered, part_values, files


def _samples(files: list[tuple[str, dict[str, str]]]) -> list[str]:
    """Up to MAX_SAMPLE_FILES footers to sample, at most one per directory."""
    seen, out = set(), []
    for path, _ in files:
        d = path.rsplit("/", 1)[0]
        if d not in seen:
            seen.add(d)
            out.append(path)
            if len(out) >= MAX_SAMPLE_FILES:
                break
    return out


def _discover(name: str, *, full: bool = False) -> Dataset | None:
    """Discover one dataset. ``full`` reads every footer (in parallel) for the exact schema
    and per-file statistics — what a Reload does; otherwise a few footers are sampled."""
    root = _safe_dataset_path(name)
    if root is None or not root.is_dir():
        return None
    part_keys, part_values, files = _scan_tree(root)
    if not files:
        return None  # a folder with no Parquet is not a dataset
    footers = inventory.read_footers([p for p, _ in files] if full else _samples(files), with_stats=full)
    # Merge the schemas of the read footers (metadata only — no data read): one corrupt or
    # partial file doesn't hide the dataset, and columns added by schema evolution count.
    data_columns: dict[str, str] = {}
    for f in footers:
        for fl in f.schema or ():
            if fl.name not in part_keys:
                data_columns.setdefault(fl.name, _friendly(fl.type))
    if not data_columns:
        return None  # every footer read was unreadable
    inv = inventory.build(files, part_keys, footers, complete=full)
    if full and len(inv.files) == 0:
        return None
    if full:  # partition values from readable files only
        part_values = {k: set(v for v in inv.partitions[k].to_pylist() if v is not None) for k in part_keys}
    columns = dict(data_columns)
    for k in part_keys:
        columns.setdefault(k, "string")  # Hive keys arrive as strings (DuckDB re-infers)
    sliceable_columns = _sliceable_columns(data_columns, part_keys, part_values)
    return Dataset(
        name=name,
        root=root,
        partition_cols=tuple(part_keys),
        partition_values={k: sorted(part_values.get(k, set())) for k in part_keys},
        data_columns=data_columns,
        columns=columns,
        allowed_columns=frozenset(columns),
        file_count=len(inv.files),
        discovered_at=time.monotonic(),
        sliceable_columns=sliceable_columns,
        inventory=inv,
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
        if entry.is_dir() and not entry.name.startswith("."):
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
            if entry.is_dir() and not entry.name.startswith("."):  # skip .kob-staging-* etc.
                ds = _discover(entry.name, full=True)
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
