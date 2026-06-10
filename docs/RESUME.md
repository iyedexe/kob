# Resuming work on kob

One-page orientation for picking this project back up (human or AI assistant).

## What this is

**kob** — a tiny, self-discovering, read-only Parquet query server. A producer
microservice drops Hive-partitioned Parquet under `KOB_DATA_ROOT`; kob auto-discovers
datasets/partitions/columns and serves filtered queries as Apache Arrow over HTTP
(default) or Arrow Flight (optional). Repo: `git@github.com:iyedexe/kob.git`.

## Layout

```
src/kob/
  catalog.py        filesystem auto-discovery + TTL cache + path sandbox
  contract.py       the JSON query contract -> parameterised DuckDB SQL
  engine.py         DuckDB -> streaming Arrow RecordBatchReader (the hot path)
  server_http.py    THE product: FastAPI app, discovery + /query, Swagger at /docs
  server_flight.py  optional Arrow Flight (gRPC) transport
  client.py         [client] reference Python client (HTTP + Flight)
  generate_data.py  [gen] synthetic GeoRev/OptionMetrics sample datasets
  benchmark.py      [bench] 6-method transport shoot-out -> docs/BENCHMARKS.md
  server_proto.py   [bench] gRPC/Protobuf baseline (exists to lose benchmarks fairly)
  proto/            generated protobuf stubs for the baseline
clients/            C# (.NET), C++ (Arrow+libcurl), Excel (PowerQuery/xlwings) clients
docs/               DESIGN.md (rationale) · PERFORMANCE_REPORT.md (5-way bench) · BENCHMARKS.md (raw)
data/               local sample Parquet (gitignored, never committed) — regenerate with kob-gen
```

## Dev loop

```bash
uv sync --extra all                 # everything (server core needs no extras)
uv run kob-gen --scale small        # sample data into ./data
uv run kob                          # serve :8000 — Swagger at /docs
uv run kob-client --dataset optionmetrics --filter 'underlying:=:AAPL' --limit 5
uv run kob-bench --scale small      # full transport benchmark (spawns servers itself)
```

C# client: `dotnet run --project clients/csharp` (needs .NET SDK ≥ 8).
C++ client: `cmake -S clients/cpp -B clients/cpp/build -DCMAKE_PREFIX_PATH=$(brew --prefix apache-arrow) && cmake --build clients/cpp/build`.

## Key decisions already made (don't relitigate without numbers)

1. **Arrow on the wire, not JSON/Protobuf** — measured 150–1750× vs JSON, 5–56× vs
   best-case Protobuf (`docs/PERFORMANCE_REPORT.md`). Protobuf survives only as the
   benchmark baseline.
2. **DuckDB as the engine** — partition/predicate/projection pushdown over Hive trees,
   zero-copy Arrow out, `enable_object_cache` for repeated-query metadata caching.
3. **Discovery over configuration** — datasets/partitions/columns come from the
   filesystem (folder names + one Parquet footer), TTL-cached (`KOB_DISCOVERY_TTL`).
   No catalog file, ever.
4. **HTTP is the default transport; Flight is optional** — HTTP+Arrow ≈ 90% of Flight
   throughput with none of the gRPC infra friction.
5. **Core stays at 4 deps** — anything else (numpy/pandas/grpc/...) lives in extras.
6. **Read-only by design** — no write path; auth/TLS delegated to a fronting proxy.

## Known gaps / natural next steps

- **No auth** — front with a reverse proxy, or add bearer-token middleware (~20 lines).
- **Flight discovery parity** — `list_flights` exists; no facets RPC over Flight.
- **S3/object-store roots** — DuckDB httpfs can read `s3://`; catalog walk is local-FS-only today.
- **Watch-based cache invalidation** (inotify/FSEvents) instead of TTL, if 15s staleness ever matters.
- **C# client** uses `Apache.Arrow.Compression` + raised gRPC `MaxReceiveMessageSize` — keep those if touched.

## Memory

The AI-assistant memory for this project lives at
`~/.claude/projects/-Users-iyedexe/memory/parquet-data-server.md` and should be updated
when project state changes materially (location, naming, big decisions, pending work).
