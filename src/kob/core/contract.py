"""The query contract — one small JSON object understood by every transport/client.

`dataset` is a discovered folder; `columns` and `filters` may target any data column
*or* any partition (folder) key. Example::

    {
      "dataset": "events",
      "columns": ["dt", "region", "user_id", "amount"],   # optional projection
      "filters": [                                         # optional, AND-ed
        {"column": "dt",     "op": "=",  "value": "2026-06-08"},   # partition (folder) filter
        {"column": "region", "op": "in", "value": ["eu", "us"]},   # partition (folder) filter
        {"column": "amount", "op": ">=", "value": 100.0}           # data column filter
      ],
      "ranges": {"dt": {"start": "2026-06-01", "end": "2026-06-30"}},  # optional date ranges
      "limit": 1000000                                    # optional
    }

``ranges`` gives an inclusive start/end to *sliceable* columns — date, timestamp and numeric
(including ``year=2024``-style integer and ``dt=2024-01-01``-style date folders) — always
named explicitly, several at once, AND-ed::

    "ranges": {"ex_date": {"start": "2024-01-01", "end": "2024-03-31"},
               "pay_date": {"end": "2024-06-30"},
               "amount": {"start": 0.5}}

Bounds are inclusive; either side may be omitted. Dates are ISO dates or datetimes, and a
date-only `end` on a timestamp column covers that whole day. (The Python client spells this
as slices: ``ds.ex_date["2024-01-01":"2024-03-31"]``.)

`build_sql` turns that into a parameterised DuckDB statement over an explicit list of the
dataset's Parquet files — pruned in memory from the catalog's inventory, never globbed. Column/op names are validated against the *discovered*
schema and all *values* are bound parameters, so untrusted clients cannot inject SQL.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from . import inventory
from .catalog import Dataset, get_dataset

# Operators clients may use. Mapped to their SQL spelling.
_SCALAR_OPS = {"=", "!=", "<>", "<", "<=", ">", ">=", "like", "not like", "ilike"}
_LIST_OPS = {"in", "not in"}
ALLOWED_OPS = _SCALAR_OPS | _LIST_OPS

MAX_LIMIT = 100_000_000


@dataclass
class Filter:
    column: str
    op: str
    value: Any


@dataclass
class QueryRequest:
    dataset: str
    columns: list[str] | None = None
    filters: list[Filter] = field(default_factory=list)
    limit: int | None = None
    # Any sliceable column -> {"start": ..., "end": ...} (inclusive, either optional), AND-ed.
    ranges: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> "QueryRequest":
        legacy = [k for k in ("start", "end", "date_column") if d.get(k) is not None]
        if legacy:
            # Never silently drop a date bound (that would return the whole history).
            raise ValueError(f"{legacy} are not supported: name the date column explicitly, "
                             'e.g. "ranges": {"date": {"start": "2024-01-01", "end": "2024-03-31"}}')
        raw_filters = d.get("filters") or []
        filters: list[Filter] = []
        for f in raw_filters:
            if isinstance(f, Filter):
                filters.append(f)
            else:
                filters.append(Filter(column=f["column"], op=f["op"], value=f["value"]))
        limit = d.get("limit")
        return cls(
            dataset=d["dataset"],
            columns=d.get("columns"),
            filters=filters,
            limit=int(limit) if limit is not None else None,
            ranges=_ranges_from(d.get("ranges")),
        )

    def to_dict(self) -> dict:
        return {
            "dataset": self.dataset,
            "columns": self.columns,
            "filters": [{"column": f.column, "op": f.op, "value": f.value} for f in self.filters],
            "limit": self.limit,
            "ranges": _ranges_from(self.ranges),  # date objects -> ISO, JSON-safe
        }


def _ranges_from(raw: Any) -> dict[str, dict[str, Any]]:
    if not raw:
        return {}
    if not isinstance(raw, dict):
        raise ValueError('ranges must be an object: {"<column>": {"start": ..., "end": ...}}')
    out: dict[str, dict[str, Any]] = {}
    for col, bounds in raw.items():
        if not isinstance(bounds, dict) or not set(bounds) <= {"start", "end"}:
            raise ValueError(f'ranges[{col!r}] must be {{"start": ..., "end": ...}}, got {bounds!r}')
        out[col] = {"start": _bound_or_none(bounds.get("start")), "end": _bound_or_none(bounds.get("end"))}
    return out


def _bound_or_none(v: Any) -> Any:
    """JSON-safe range bound: numbers stay numbers, dates become ISO strings."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return v
    return v.isoformat() if isinstance(v, (dt.date, dt.datetime)) else str(v)


def _parse_number(name: str, raw: Any) -> int | float:
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return raw
    try:
        return int(raw)
    except (TypeError, ValueError):
        pass
    try:
        return float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number, got {raw!r}") from None


def _parse_iso(name: str, raw: str) -> dt.date | dt.datetime:
    try:
        return dt.date.fromisoformat(raw)
    except ValueError:
        pass
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"{name} must be an ISO date or datetime (e.g. 2026-06-01), got {raw!r}") from None


def _range_bounds(ds: Dataset, col: str, start_raw: Any, end_raw: Any) -> list[tuple[str, Any]]:
    """Validate one inclusive start/end on a sliceable column → ``(sql_op, value)`` bounds."""
    kind = ds.sliceable_columns.get(col)
    if kind is None:
        raise ValueError(f"{col!r} is not a sliceable (date/timestamp/numeric) column of "
                         f"'{ds.name}' (sliceable: {list(ds.sliceable_columns)})")
    if kind == "number":
        lo = _parse_number(f"{col} start", start_raw) if start_raw is not None else None
        hi = _parse_number(f"{col} end", end_raw) if end_raw is not None else None
        if lo is not None and hi is not None and lo > hi:
            raise ValueError(f"{col}: start ({start_raw}) is after end ({end_raw})")
        return [(op, v) for op, v in ((">=", lo), ("<=", hi)) if v is not None]
    start_raw = _bound_or_none(start_raw)
    end_raw = _bound_or_none(end_raw)
    start = _parse_iso(f"{col} start", start_raw) if start_raw is not None else None
    end = _parse_iso(f"{col} end", end_raw) if end_raw is not None else None
    bounds: list[tuple[str, Any]] = []
    if start is not None:
        bounds.append((">=", start))
    if end is not None:
        if not isinstance(end, dt.datetime) and kind == "timestamp":
            # A date-only end on a timestamp column means "through the end of that day".
            bounds.append(("<", dt.datetime.combine(end + dt.timedelta(days=1), dt.time())))
        else:
            bounds.append(("<=", end))
    if start is not None and end is not None:
        as_dt = lambda v: v if isinstance(v, dt.datetime) else dt.datetime.combine(v, dt.time())  # noqa: E731
        if as_dt(start) > as_dt(end):
            raise ValueError(f"{col}: start ({start_raw}) is after end ({end_raw})")
    return bounds


def _date_ranges(req: QueryRequest, ds: Dataset) -> list[tuple[str, str, Any]]:
    """All range bounds of a request as ``(column, sql_op, value)``."""
    out: list[tuple[str, str, Any]] = []
    for col, b in req.ranges.items():
        out += [(col, op, v) for op, v in _range_bounds(ds, col, b.get("start"), b.get("end"))]
    return out


def _ident(name: str) -> str:
    """Quote a SQL identifier; embedded double quotes are doubled per SQL rules.

    Column names come from the discovered Parquet schema (producer-controlled),
    so a name containing ``"`` must not be able to break out of the quoting.
    """
    return '"' + name.replace('"', '""') + '"'


def _validate(req: QueryRequest) -> Dataset:
    ds = get_dataset(req.dataset)
    if req.columns:
        unknown = [c for c in req.columns if c not in ds.allowed_columns]
        if unknown:
            raise ValueError(f"Unknown column(s) for '{ds.name}': {unknown}")
    for f in req.filters:
        if f.column not in ds.allowed_columns:
            raise ValueError(f"Unknown filter column for '{ds.name}': {f.column!r}")
        if f.op.lower() not in ALLOWED_OPS:
            raise ValueError(f"Unsupported operator: {f.op!r}")
        if f.op.lower() in _LIST_OPS and (
            isinstance(f.value, (str, bytes)) or not isinstance(f.value, (list, tuple, set))
        ):
            # A plain string would silently iterate character-by-character
            # ("eu" -> IN ('e','u')); anything non-iterable would blow up later.
            raise ValueError(
                f"Operator {f.op!r} on {f.column!r} requires a list of values, "
                f'e.g. {{"column": "{f.column}", "op": "{f.op}", "value": ["a", "b"]}}'
            )
    if req.limit is not None and not (0 < req.limit <= MAX_LIMIT):
        raise ValueError(f"limit must be in (0, {MAX_LIMIT}]")
    return ds


def scan_sql(ds: Dataset) -> str:
    """``read_parquet(?, …)`` over an explicit file list (bound as the first parameter).

    No glob: DuckDB never lists the tree. Partition types come from discovery
    (``hive_types``), and ``union_by_name`` is on only when files disagree on schema."""
    opts = ["hive_partitioning = true", f"union_by_name = {'true' if ds.union_by_name else 'false'}"]
    if ds.hive_types:
        types = ", ".join(f"'{k.replace(chr(39), chr(39) * 2)}': '{t}'" for k, t in ds.hive_types.items())
        opts.append("hive_types = {" + types + "}")
    return f"read_parquet(?, {', '.join(opts)})"


def plan_files(req: QueryRequest, ds: Dataset) -> list[str]:
    """The files that may hold matching rows — decided in memory from the inventory."""
    preds = [(f.column, f.op, f.value) for f in req.filters]
    preds += _date_ranges(req, ds)
    return inventory.prune(ds.inventory, preds, ds.sliceable_columns)


def build_sql(req: QueryRequest) -> tuple[str, list[Any]]:
    """Return ``(sql, params)`` for a parameterised DuckDB query.

    The first parameter is the explicit list of files to scan (planned from the catalog's
    inventory — see :func:`plan_files`); every client-supplied *value* is a bound ``?``.
    """
    ds = _validate(req)
    files = plan_files(req, ds)

    if req.columns:
        projection = ", ".join(_ident(c) for c in req.columns)
    else:
        projection = "*"

    clauses: list[str] = []
    if not files:
        # Nothing can match: read one footer per distinct schema, just for the result schema.
        schema_files = ds.inventory.schema_files
        files = list(schema_files if ds.union_by_name else schema_files[:1])
        clauses.append("FALSE")
    params: list[Any] = [files]
    sql = f"SELECT {projection} FROM {scan_sql(ds)}"

    for f in req.filters:
        col = _ident(f.column)
        op = f.op.lower()
        if op in _LIST_OPS:
            values = list(f.value)
            if not values:
                # `col IN ()` is invalid SQL; encode as always-false / always-true.
                clauses.append("FALSE" if op == "in" else "TRUE")
                continue
            placeholders = ", ".join(["?"] * len(values))
            clauses.append(f"{col} {op.upper()} ({placeholders})")
            params.extend(values)
        else:
            clauses.append(f"{col} {op.upper()} ?")
            params.append(f.value)

    for col, op, value in _date_ranges(req, ds):
        clauses.append(f"{_ident(col)} {op} ?")
        params.append(value)

    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if req.limit is not None:
        sql += f" LIMIT {int(req.limit)}"

    return sql, params
