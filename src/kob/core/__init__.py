"""kob core — the transport-agnostic product.

Three small modules, no web/RPC code, no hardcoded schema:

* :mod:`kob.core.catalog`  — filesystem auto-discovery (datasets, partitions, columns).
* :mod:`kob.core.contract` — the JSON query contract and its parameterised SQL builder.
* :mod:`kob.core.engine`   — DuckDB execution that streams results out as Apache Arrow.

Every transport (the primary Flight server, the HTTP/Swagger control plane, and the
demo servers) is a thin shell around exactly these three modules.
"""
