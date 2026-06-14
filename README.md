# kob

**Drop Parquet files in a folder — get a fast, self-describing query API in front of them.**

kob is a tiny, read-only query server for Parquet. Point it at a directory and it
discovers your datasets, tells every client exactly which columns and partitions it can
**filter on**, and streams results back as **Apache Arrow** — the columnar format that
lands large results in your client with almost no deserialization — all with **zero
schema configuration**.

No database to stand up. No schema to declare. No restart when new data lands. Four
dependencies, ~750 lines of core server.

```bash
KOB_DATA_ROOT=/path/to/parquet uv run kob      # → http://localhost:8000/docs
```

---

## Why kob exists

A producer writes Parquet to a folder. You need to serve that data — read-only, to
Python / C# / C++ / Excel clients — and the usual options are all overkill or all slow:

- **A warehouse or query broker** is a lot of moving parts to put in front of what is
  *already* a queryable, columnar file format.
- **A hand-written API** means declaring a schema and a catalog in code, then
  re-deploying every time a producer adds a dataset, a partition, or a column.
- **A JSON (or Protobuf) endpoint** pays the serialization tax that dominates analytical
  workloads — Apache Arrow's own FAQ notes (de)serialization can run to **80–90% of compute
  cost**. You feel it as latency that grows *super-linearly* with result size.

kob is the small answer to all three: a **stateless** service that turns "a folder of
Parquet" into "a discoverable, filterable, blazing-fast Arrow API" — and nothing more.

## What it does

```
producer drops Parquet                     kob  (≈ stateless, self-configuring)
─────────────────────                      ────────────────────────────────────────
$KOB_DATA_ROOT/                            GET /datasets          → discover folders
  events/                                  GET /datasets/{name}   → partitions + columns
    dt=2026-06-08/region=eu/*.parquet      GET .../facets         → min/max, distinct values
    dt=2026-06-08/region=us/*.parquet      POST /query            → Apache Arrow stream
  prices/                                          │
    symbol=BTCEUR/year=2026/*.parquet              ▼
                                            DuckDB: partition + projection + predicate
                                            pruning  →  Arrow IPC stream (columnar)
```

1. **Discovers** every dataset, partition and column straight from the filesystem — no
   hardcoded schema, no config file.
2. **Advertises the filter surface** — partition values *per folder* and min/max/distinct
   *per column* — so clients build valid queries without guessing.
3. **Executes** with DuckDB, which pushes partition + projection + predicate pruning down
   into the Parquet scan, so only the requested slice is ever read.
4. **Streams** the result as Apache Arrow over plain HTTP (default) or Arrow Flight
   (optional), with bounded memory even for multi-GB results.

## Why it's good

- **Tiny.** Four dependencies — `duckdb`, `pyarrow`, `fastapi`, `uvicorn`. No database, no
  broker, no config files, no hardcoded schema. The whole core server is ~750 lines.
- **Self-configuring.** A producer drops new files / partitions / datasets — they appear
  live, no restart, no code change. Discovery reads folder names and one file's metadata;
  it never scans your data.
- **Fast.** DuckDB pushes partition / predicate / projection pruning into the scan, caches
  Parquet metadata across queries, and streams results as Arrow with no row-by-row
  conversion — **150–1750× faster than JSON** and **5–56× faster than Protobuf**
  ([full report](docs/PERFORMANCE_REPORT.md)).
- **Safe.** Read-only by design. Dataset names are sandboxed to the data root (no `..`
  escape); columns and operators are validated against the discovered schema; every value
  is a bound parameter — untrusted clients cannot traverse the filesystem or inject SQL.

## Install & run (30 seconds)

```bash
git clone git@github.com:iyedexe/kob.git && cd kob
uv sync                                       # or: pip install -e .  (Python ≥ 3.12)
KOB_DATA_ROOT=/path/to/parquet uv run kob     # serves on :8000
```

No data yet? Generate realistic samples and serve them:

```bash
uv sync --extra gen && uv run kob-gen --scale small && uv run kob
```

Open **http://localhost:8000/docs** — full interactive Swagger UI, with a working
**Try it out** for every endpoint.

## How discovery works

Point kob at a folder of Hive-partitioned Parquet and it derives everything from the
layout:

- **Datasets** = the top-level folders that contain Parquet.
- **Partition columns + their values** = the `key=value` folder levels — read from folder
  *names* only, no data scan. These are the **filter-per-folder** options; a partition
  filter prunes whole folders before any file is opened.
- **Data columns + types** = the schema of one Parquet file (metadata only). These are the
  **filter-per-column** options; `facets` adds min/max from row-group statistics.

Discovery is cached with a short TTL (default 15s), so freshly-dropped files and
partitions surface automatically while steady-state lookups stay a single dict hit.

## Query

One small JSON contract, understood by every transport and client:

```bash
curl -s -X POST 'localhost:8000/query' -H 'content-type: application/json' -d '{
  "dataset": "events",
  "columns": ["dt", "region", "user_id", "amount"],
  "filters": [
    {"column": "region", "op": "in",  "value": ["eu", "us"]},
    {"column": "amount", "op": ">=",  "value": 100.0}
  ],
  "limit": 100000
}' --output result.arrow
```

Read it back anywhere Arrow lives — Python: `pa.ipc.open_stream(...).read_all()`;
C#: `ArrowStreamReader`; C++: `RecordBatchStreamReader`.

- **Operators:** `= != <> < <= > >= like "not like" ilike in "not in"`.
- **Filters** may target data columns **or** partition keys — partition filters prune
  whole folders before any file is read.
- **Formats:** `?format=arrow|csv|json` (`csv`/`json` for interop & debugging).
- **Compression:** `?compression=zstd|lz4|none` — `zstd` over a network, `none` on a LAN.

Working clients for **Python, C#, C++ and Excel** — see [`clients/`](clients/README.md).

## API

| Endpoint | Returns |
|---|---|
| `GET /health` | O(1) liveness (never walks the data tree) |
| `GET /datasets` | every discovered dataset: partition + data columns (with types) |
| `GET /datasets/{name}` | **partition values** — the per-folder filter options |
| `GET /datasets/{name}/facets?columns=a,b[&distinct=true]` | per-column **min/max** (+ approx-distinct) — the per-column filter options |
| `POST /query` | the query contract above → Arrow / CSV / JSON |

Swagger UI at `/docs`, ReDoc at `/redoc`, raw schema at `/openapi.json`.
Append `?refresh=true` to `/datasets` or `/datasets/{name}` to bypass the discovery cache.

## Configuration (all optional)

| Env var | Default | Meaning |
|---|---|---|
| `KOB_DATA_ROOT` | `./data` | Folder the producer drops Parquet into |
| `KOB_DISCOVERY_TTL` | `15` | Seconds to cache discovery before re-scanning |
| `KOB_DUCKDB_THREADS` | all cores | DuckDB threads per worker |

CLI: `kob --host 0.0.0.0 --port 8000 --workers 4 --threads 0`

## Optional extras

The core install is the whole product. These are opt-in and never loaded by the server:

```bash
uv run kob-flight                  # Arrow Flight (gRPC) transport — max throughput
uv sync --extra client             # reference Python client CLI (kob-client)
uv sync --extra gen                # synthetic sample-data generator (kob-gen)
uv sync --extra bench              # benchmark + Protobuf baseline (kob-bench, kob-proto)
```

## Why Arrow (not JSON or Protobuf)?

JSON and Protobuf are **row-oriented**: the sender encodes every value field-by-field and
the receiver decodes every value and rebuilds columns — cost scales with `rows × cols`,
and the wire bytes are never the in-memory representation.

Apache Arrow's **columnar wire layout *is* its in-memory layout**, so reading a result is a
header parse plus a buffer-pointer setup — essentially nothing to deserialize. That is why
even **Arrow Flight uses Protobuf only for its tiny control envelope** (tickets/descriptors)
and never for the bulk data.

The full 5-way benchmark (Flight / HTTP-Arrow / Protobuf ×2 / JSON) and analysis is in
[`docs/PERFORMANCE_REPORT.md`](docs/PERFORMANCE_REPORT.md); the architecture and rationale
in [`docs/DESIGN.md`](docs/DESIGN.md).

## License

Code: **MIT**. Synthetic sample data: **CC0** (public domain). See [`LICENSE`](LICENSE).
