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
      "start": "2026-06-01", "end": "2026-06-30",         # optional, inclusive date range
      "limit": 1000000                                    # optional
    }

`start` / `end` filter the dataset's date column (auto-detected at discovery, e.g. ``date``
or ``trade_date``; override with ``"date_column"``). Both are inclusive ISO dates or
datetimes; a date-only `end` on a timestamp column covers that whole day.

`build_sql` turns that into a parameterised DuckDB statement against the dataset's
Hive-partitioned Parquet glob. Column/op names are validated against the *discovered*
schema and all *values* are bound parameters, so untrusted clients cannot inject SQL.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

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
    start: str | None = None        # inclusive ISO date/datetime on the date column
    end: str | None = None          # inclusive ISO date/datetime on the date column
    date_column: str | None = None  # defaults to the dataset's detected date column

    @classmethod
    def from_dict(cls, d: dict) -> "QueryRequest":
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
            start=_iso_or_none(d.get("start")),
            end=_iso_or_none(d.get("end")),
            date_column=d.get("date_column") or None,
        )

    def to_dict(self) -> dict:
        return {
            "dataset": self.dataset,
            "columns": self.columns,
            "filters": [{"column": f.column, "op": f.op, "value": f.value} for f in self.filters],
            "limit": self.limit,
            "start": self.start,
            "end": self.end,
            "date_column": self.date_column,
        }


def _iso_or_none(v: Any) -> str | None:
    if v is None or v == "":
        return None
    return v.isoformat() if isinstance(v, (dt.date, dt.datetime)) else str(v)


def _parse_iso(name: str, raw: str) -> dt.date | dt.datetime:
    try:
        return dt.date.fromisoformat(raw)
    except ValueError:
        pass
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"{name} must be an ISO date or datetime (e.g. 2026-06-01), got {raw!r}") from None


def _date_range(req: QueryRequest, ds: Dataset) -> list[tuple[str, Any]]:
    """Validate start/end and return ``(sql_op, value)`` bounds for the date column."""
    if req.start is None and req.end is None:
        return []
    col = req.date_column or ds.date_column
    if col is None:
        raise ValueError(f"Dataset '{ds.name}' has no date/timestamp column; start/end need one "
                         "(pass date_column to pick a column explicitly)")
    if col not in ds.allowed_columns:
        raise ValueError(f"Unknown date_column for '{ds.name}': {col!r}")
    start = _parse_iso("start", req.start) if req.start is not None else None
    end = _parse_iso("end", req.end) if req.end is not None else None
    bounds: list[tuple[str, Any]] = []
    if start is not None:
        bounds.append((">=", start))
    if end is not None:
        if not isinstance(end, dt.datetime) and ds.columns.get(col) == "timestamp":
            # A date-only end on a timestamp column means "through the end of that day".
            bounds.append(("<", dt.datetime.combine(end + dt.timedelta(days=1), dt.time())))
        else:
            bounds.append(("<=", end))
    if start is not None and end is not None:
        as_dt = lambda v: v if isinstance(v, dt.datetime) else dt.datetime.combine(v, dt.time())  # noqa: E731
        if as_dt(start) > as_dt(end):
            raise ValueError(f"start ({req.start}) is after end ({req.end})")
    return bounds


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
    if req.date_column is not None and req.date_column not in ds.allowed_columns:
        raise ValueError(f"Unknown date_column for '{ds.name}': {req.date_column!r}")
    if req.limit is not None and not (0 < req.limit <= MAX_LIMIT):
        raise ValueError(f"limit must be in (0, {MAX_LIMIT}]")
    return ds


def build_sql(req: QueryRequest) -> tuple[str, list[Any]]:
    """Return ``(sql, params)`` for a parameterised DuckDB query.

    The Parquet glob comes from the trusted catalog and is inlined as a literal;
    every client-supplied *value* is a bound ``?`` parameter.
    """
    ds = _validate(req)

    if req.columns:
        projection = ", ".join(_ident(c) for c in req.columns)
    else:
        projection = "*"

    # Glob is server-controlled (from discovery), so inlining is safe. Escape quotes defensively.
    glob_literal = ds.glob.replace("'", "''")
    sql = (
        f"SELECT {projection} "
        f"FROM read_parquet('{glob_literal}', hive_partitioning = true, union_by_name = true)"
    )

    params: list[Any] = []
    clauses: list[str] = []
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

    date_col = req.date_column or ds.date_column
    for op, value in _date_range(req, ds):
        clauses.append(f"{_ident(date_col)} {op} ?")
        params.append(value)

    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if req.limit is not None:
        sql += f" LIMIT {int(req.limit)}"

    return sql, params
