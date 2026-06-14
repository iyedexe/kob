"""kob demos — secondary transports, here only to demonstrate why the product uses Arrow.

Each folder is a small, standalone server that serves the **same** discovered Parquet via
a different wire format, so the benchmark (:mod:`kob.tools.benchmark`) can compare them
head-to-head against the primary Arrow Flight server:

* :mod:`kob.demos.arrow_http` — Arrow IPC streamed over plain HTTP.
* :mod:`kob.demos.json`       — REST/JSON (the ubiquitous, and slowest, baseline).
* :mod:`kob.demos.protobuf`   — gRPC + Protocol Buffers (row and columnar encodings).

None of these is recommended for real use — see ``docs/PERFORMANCE_REPORT.md``.
"""
