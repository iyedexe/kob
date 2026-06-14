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
      "limit": 1000000                                    # optional
    }

`build_sql` turns that into a parameterised DuckDB statement against the dataset's
Hive-partitioned Parquet glob. Column/op names are validated against the *discovered*
schema and all *values* are bound parameters, so untrusted clients cannot inject SQL.
"""

from __future__ import annotations

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
        )

    def to_dict(self) -> dict:
        return {
            "dataset": self.dataset,
            "columns": self.columns,
            "filters": [{"column": f.column, "op": f.op, "value": f.value} for f in self.filters],
            "limit": self.limit,
        }


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
        projection = ", ".join(f'"{c}"' for c in req.columns)
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
        col = f'"{f.column}"'
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

    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if req.limit is not None:
        sql += f" LIMIT {int(req.limit)}"

    return sql, params
