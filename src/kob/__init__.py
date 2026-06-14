"""kob — a tiny, self-discovering, read-only Parquet query server.

A producer drops Parquet under ``KOB_DATA_ROOT``; kob discovers datasets, partitions
and columns automatically and serves results as Apache Arrow — via plain HTTP
(:mod:`kob.server_http`, the default) or Arrow Flight (:mod:`kob.server_flight`).

Core modules: :mod:`kob.catalog` (filesystem discovery), :mod:`kob.contract`
(the JSON query contract), :mod:`kob.engine` (DuckDB → Arrow execution).
"""

__version__ = "0.3.0"
