"""Filesystem auto-discovery catalog.

There is **no** hardcoded list of datasets. A producer microservice drops Parquet
under ``KOB_DATA_ROOT`` in a Hive-partitioned tree, e.g.::

    <root>/events/dt=2026-06-08/region=eu/part-0.parquet
    <root>/prices/symbol=BTCEUR/year=2026/part-0.parquet

and this module discovers, per dataset (a top-level folder containing Parquet):

* **partition columns + their values** — read straight from the ``key=value`` folder
  names (filesystem only, no data scan) — these are the "filter per folder" options;
* **data columns + types** — read from one Parquet file's schema (metadata only).

Discovery is cached with a short TTL so freshly-dropped files/partitions appear without
a restart, while steady-state lookups are a single dict hit (all derived lookups are
precomputed at discovery time).

Security: a dataset name is always a single path segment that must resolve *inside*
the data root, and only names found by discovery are accepted — there is no way to
escape the root via ``..`` or absolute paths.
"""

from __future__ import annotations

import os
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
# Discovery
# --------------------------------------------------------------------------- #
def _scan_tree(dataset_root: Path) -> tuple[list[str], dict[str, set[str]], str | None, int]:
    part_values: dict[str, set[str]] = {}
    sample: str | None = None
    file_count = 0
    for dirpath, _dirnames, filenames in os.walk(dataset_root):
        rel = os.path.relpath(dirpath, dataset_root)
        if rel != ".":
            for seg in rel.split(os.sep):
                if "=" in seg:
                    k, v = seg.split("=", 1)
                    part_values.setdefault(k, set()).add(v)
        pqs = [f for f in filenames if f.endswith(".parquet")]
        file_count += len(pqs)
        if sample is None and pqs:
            sample = os.path.join(dirpath, pqs[0])
    # Canonical partition-key order comes from the sample file's own path.
    ordered: list[str] = []
    if sample is not None:
        relpath = os.path.relpath(os.path.dirname(sample), dataset_root)
        if relpath != ".":
            ordered = [s.split("=", 1)[0] for s in relpath.split(os.sep) if "=" in s]
    return ordered, part_values, sample, file_count


def _discover(name: str) -> Dataset | None:
    root = _safe_dataset_path(name)
    if root is None or not root.is_dir():
        return None
    part_keys, part_values, sample, file_count = _scan_tree(root)
    if sample is None:
        return None  # a folder with no Parquet is not a dataset
    try:
        # Read via an open file handle rather than handing pyarrow the path
        # string: pyarrow's own path handling chokes on Windows UNC/network
        # paths and on non-ASCII characters (accents, etc.), which silently
        # hides the dataset. Python's open() handles those correctly.
        with open(sample, "rb") as fh:
            schema = pq.read_schema(fh)  # metadata only — no data read
    except Exception:
        return None
    data_columns = {f.name: _friendly(f.type) for f in schema if f.name not in part_keys}
    columns = dict(data_columns)
    for k in part_keys:
        columns.setdefault(k, "string")  # Hive keys arrive as strings (DuckDB re-infers)
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
            return cached
    ds = _discover(name)  # outside the lock (does IO)
    if ds is None:
        raise KeyError(f"Unknown dataset '{name}' (no Parquet found under {DATA_ROOT}/{name})")
    with _lock:
        _cache[name] = ds
    return ds


def list_dataset_names(*, refresh: bool = False) -> list[str]:
    """Top-level folders under the data root that contain at least one Parquet file."""
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
