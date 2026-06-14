"""kob — a tiny, self-discovering, read-only Parquet query server.

Drop Hive-partitioned Parquet under ``KOB_DATA_ROOT``; kob discovers datasets, partitions
and columns automatically and serves results as **Apache Arrow**. The default ``kob``
command runs the fastest transport — Arrow **Flight** (gRPC) — alongside a **Swagger** UI
for discovery and interactive exploration.

Package layout
--------------
* :mod:`kob.core`   — the product: discovery (:mod:`~kob.core.catalog`), the query
  contract (:mod:`~kob.core.contract`), and DuckDB → Arrow execution (:mod:`~kob.core.engine`).
* :mod:`kob.server` — the ``kob`` entry point: Arrow Flight (fast data) + Swagger HTTP.
* :mod:`kob.demos`  — secondary transports, kept only to demonstrate the alternatives
  (Arrow-over-HTTP, REST/JSON, gRPC/Protobuf).
* :mod:`kob.tools`  — optional utilities: sample-data generator, reference client, benchmark.
"""

__version__ = "0.4.0"
