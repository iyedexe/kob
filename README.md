# kob

A **tiny, self-discovering, read-only Parquet query server.** Drop Parquet files in a
folder; kob discovers them, tells clients exactly what they can **filter on (per folder
and per column)**, and serves results as **Apache Arrow** — blazing fast, via DuckDB.

- **Tiny.** Four dependencies: `duckdb`, `pyarrow`, `fastapi`, `uvicorn`. No database, no broker, no config files, no hardcoded schema.
- **Self-configuring.** A producer drops new files/partitions/datasets — they appear live, no restart, no code change.
- **Fast.** DuckDB pushes partition/predicate/projection pruning into the scan, caches Parquet metadata across queries, and streams results zero-copy as Arrow — measured **150–1750× faster than JSON**, **5–56× faster than Protobuf** ([report](docs/PERFORMANCE_REPORT.md)).
- **Safe.** Read-only. Dataset names sandboxed to the data root; columns/filters validated against the discovered schema; all values are bound parameters (no SQL injection).

## Install & run (30 seconds)

```bash
git clone git@github.com:iyedexe/kob.git && cd kob
uv sync                                  # or: pip install -e .
KOB_DATA_ROOT=/path/to/parquet uv run kob   # serves on :8000
```

No data yet? Generate samples: `uv sync --extra gen && uv run kob-gen --scale small && uv run kob`

Open **http://localhost:8000/docs** — full interactive Swagger UI.

## How it works

```
producer drops Parquet                     kob (≈ stateless)
─────────────────────                      ──────────────────────────
$KOB_DATA_ROOT/                            GET /datasets          → discover folders
  events/                                  GET /datasets/{name}   → partitions + columns
    dt=2026-06-08/region=eu/*.parquet      GET .../facets         → min/max, distinct values
    dt=2026-06-08/region=us/*.parquet      POST /query            → Arrow (filter folder+column)
  prices/                                          │
    symbol=BTCEUR/year=2026/*.parquet              ▼
                                            DuckDB: partition pruning + projection
                                            + predicate pushdown → Arrow IPC stream
```

Every `key=value` folder level is a **partition column** (filter per folder); the Parquet
schema gives the **data columns** (filter per column). Discovery reads folder names and one
file's metadata — no data scan — cached with a short TTL so fresh drops surface automatically.

## Query

```bash
curl -s -X POST 'localhost:8000/query' -H 'content-type: application/json' -d '{
  "dataset": "events",
  "columns": ["dt","region","user_id","amount"],
  "filters": [
    {"column":"region","op":"in","value":["eu","us"]},
    {"column":"amount","op":">=","value":100.0}
  ],
  "limit": 100000
}' --output result.arrow
```

Read it back anywhere Arrow exists — Python: `pa.ipc.open_stream(...).read_all()`;
C#: `ArrowStreamReader`; C++: `RecordBatchStreamReader`. Working clients for
**Python, C#, C++ and Excel** live in [`clients/`](clients/README.md).

Operators: `= != < <= > >= in "not in" like ilike`. Filters may target data columns
**or** partition keys; partition filters prune whole folders before any file is read.
`?format=csv|json` for interop/debug, `?compression=zstd|lz4|none` (zstd over a network,
none on a LAN).

## API

| Endpoint | Returns |
|---|---|
| `GET /health` | O(1) liveness |
| `GET /datasets` | every discovered dataset: partition + data columns (+types) |
| `GET /datasets/{name}` | **partition values** — the per-folder filter options |
| `GET /datasets/{name}/facets?columns=a,b[&distinct=true]` | per-column **min/max** (+ approx-distinct) — the per-column filter options |
| `POST /query` | the query contract above → Arrow / CSV / JSON |

Swagger UI at `/docs`, ReDoc at `/redoc`, raw schema at `/openapi.json`.
Append `?refresh=true` to discovery endpoints to bypass the cache.

## Configuration (all optional)

| Env var | Default | Meaning |
|---|---|---|
| `KOB_DATA_ROOT` | `./data` | Folder the producer drops Parquet into |
| `KOB_DISCOVERY_TTL` | `15` | Seconds to cache discovery before re-scanning |
| `KOB_DUCKDB_THREADS` | all cores | DuckDB threads per worker |

CLI: `kob --host 0.0.0.0 --port 8000 --workers 4 --threads 0`

## Optional extras

```bash
uv run kob-flight                  # Arrow Flight (gRPC) transport — max throughput
uv sync --extra client             # Python client CLI (kob-client)
uv sync --extra gen                # sample-data generator (kob-gen)
uv sync --extra bench              # benchmark + Protobuf baseline (kob-bench, kob-proto)
```

## Why Arrow (not JSON/Protobuf)?

Arrow's wire layout *is* its in-memory layout — there is essentially nothing to
deserialize. Full 5-way benchmark (Flight / HTTP-Arrow / Protobuf×2 / JSON) and analysis:
[`docs/PERFORMANCE_REPORT.md`](docs/PERFORMANCE_REPORT.md). Architecture & rationale:
[`docs/DESIGN.md`](docs/DESIGN.md). Resuming work on this repo: [`docs/RESUME.md`](docs/RESUME.md).

## License

Code: MIT. Synthetic sample data: CC0.
