# Resuming work on kob

One-page orientation for picking this project back up (human or AI assistant).

## What this is

**kob** — a tiny, self-discovering, read-only Parquet query server. A producer
microservice drops Hive-partitioned Parquet under `KOB_DATA_ROOT`; kob auto-discovers
datasets/partitions/columns and serves filtered queries as Apache Arrow. The default
`kob` command runs **Arrow Flight** (gRPC, the fast data path) alongside a **Swagger**
HTTP control plane for discovery/exploration. Repo: `git@github.com:iyedexe/kob.git`.

## Layout

```
src/kob/
  core/            the product (no web/RPC code)
    catalog.py       filesystem auto-discovery + TTL cache + path sandbox
    contract.py      the JSON query contract -> parameterised DuckDB SQL
    engine.py        DuckDB -> streaming Arrow RecordBatchReader (the hot path)
  server/          the `kob` entry point (Flight + Swagger, started together)
    flight.py        Arrow Flight (gRPC) — the fast, default data path
    api.py           FastAPI: Swagger UI, discovery, /flight how-to, interactive /query
    app.py           main(): Flight on a thread + the Swagger app in the foreground
  demos/           secondary transports — exist only to lose benchmarks fairly
    arrow_http/      Arrow IPC over plain HTTP
    json/            REST/JSON baseline
    protobuf/        [bench] gRPC/Protobuf baseline + generated proto stubs
  tools/           optional utilities (each needs its extra)
    generate_data.py [gen] synthetic GeoRev/OptionMetrics sample datasets
    client.py        [client] reference Python client (Flight + HTTP)
    benchmark.py     [bench] 6-method transport shoot-out -> docs/BENCHMARKS.md
clients/            C# (.NET), C++ (Arrow+libcurl), Excel (PowerQuery/xlwings) clients
docs/               DESIGN.md (rationale) · PERFORMANCE_REPORT.md (5-way bench) · BENCHMARKS.md (raw)
data/               local sample Parquet (gitignored, never committed) — regenerate with kob-gen
```

## Dev loop

```bash
uv sync --extra all                 # everything (server core needs no extras)
uv run kob-gen --scale small        # sample data into ./data
uv run kob                          # Flight :8815 + Swagger http://localhost:8000/docs
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
4. **Flight is the default transport (fastest); Swagger runs alongside it** — `kob`
   starts Arrow Flight (data) + a Swagger HTTP control plane (discovery/exploration) in
   one process. HTTP-Arrow / JSON / Protobuf survive only as demos under `kob/demos/`.
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
