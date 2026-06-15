# kob

**Drop Parquet files in a folder — get a fast, self-describing query API in front of them.**

kob is a tiny, read-only query server for Parquet. Point it at a directory and it
discovers your datasets, tells every client exactly which columns and partitions it can
**filter on**, and serves results as **Apache Arrow**. The default `kob` command runs the
**fastest** transport — Arrow **Flight** (gRPC) — right next to a **Swagger UI** for
discovery and interactive exploration.

No database to stand up. No schema to declare. No restart when new data lands. Four
dependencies.

```bash
KOB_DATA_ROOT=/path/to/parquet uv run kob
#   data  (fast)     Arrow Flight    grpc://127.0.0.1:8815
#   docs  (Swagger)  HTTP            http://127.0.0.1:8000/docs
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

## How it works

Everything is one straight line from a folder of Parquet to an Arrow stream. The same
three **core** modules feed every transport:

```
 $KOB_DATA_ROOT/                          ┌─ kob.core ─ the product (transport-agnostic) ─┐
   events/dt=…/region=…/*.parquet         │  catalog   discover datasets/partitions/cols  │
   prices/symbol=…/year=…/*.parquet       │  contract  validate query → parameterised SQL │
         │                                │  engine    DuckDB → Apache Arrow (pushdown)   │
         ▼  folder walk + one file's      └───────────────────────┬───────────────────────┘
            metadata (no data scan)                               │  one Arrow RecordBatch stream
            self-discovery, cached TTL          ┌─────────────────┴─────────────────┐
                                                ▼                                   ▼
                                       Arrow Flight (gRPC)                    Swagger / HTTP
                                       the fast DATA path  ⚡                 discovery + try-it-out
                                       grpc://…:8815                          http://…:8000/docs
```

1. **Discover** — `core/catalog` walks `KOB_DATA_ROOT`. Datasets are top-level folders of
   Parquet; partition columns + values come from the `key=value` folder names (filesystem
   only); data columns + types come from one Parquet file's metadata. No data is scanned,
   and the result is cached with a short TTL so freshly-dropped files appear automatically.
2. **Describe the filter surface** — clients learn what they can filter on *per folder*
   (partition values) and *per column* (min/max/distinct via `facets`), so they build valid
   queries without guessing.
3. **Validate & build SQL** — `core/contract` turns one small JSON query object into a
   parameterised DuckDB statement. Column/operator names are checked against the discovered
   schema; every value is a bound parameter (no SQL injection).
4. **Execute** — `core/engine` runs it on DuckDB, which pushes partition + projection +
   predicate pruning *into* the Parquet scan and streams the result out as Arrow with no
   row-by-row conversion.
5. **Serve** — the `kob` command (`server/app`) starts **two** things that share that one
   engine: **Arrow Flight** for the fast data path, and a **Swagger HTTP** control plane for
   discovery and interactive exploration. The data path stays pure Flight; the HTTP side
   never touches it.

## The two faces of `kob`

| | Arrow **Flight** (gRPC) | **Swagger** HTTP |
|---|---|---|
| For | machines pulling data, fast | humans exploring & integrating |
| Port | `:8815` | `:8000` (`/docs`) |
| Payload | Arrow IPC — the wire layout *is* the in-memory layout, nothing to decode | JSON / CSV for interactive `POST /query`; JSON for discovery |
| Use it to | stream query results at full throughput | list datasets, inspect filters, try a query, read how to call Flight |

One process, one discovery cache, one DuckDB. You don't choose a transport at deploy time —
you get both, and clients use whichever fits.

## Project layout

```
src/kob/
  core/      the product — no web/RPC code
    catalog.py    filesystem auto-discovery (datasets, partitions, columns)
    contract.py   the JSON query contract + parameterised SQL builder
    engine.py     DuckDB execution → streaming Apache Arrow
  server/    the `kob` entry point (Flight + Swagger, started together)
    flight.py     Arrow Flight server (the fast, default data path)
    api.py        FastAPI app: Swagger UI, discovery, /flight how-to, interactive /query
    app.py        main(): runs Flight on a thread + the Swagger app in the foreground
  demos/     secondary transports — here only to demonstrate the alternatives
    arrow_http/   Arrow IPC over plain HTTP
    json/         REST/JSON (the slow, ubiquitous baseline)
    protobuf/     gRPC + Protocol Buffers (row + columnar)
  tools/     optional utilities (each needs its install extra)
    generate_data.py   synthetic sample datasets (kob-gen)
    client.py          reference Python client + CLI (kob-client)
    benchmark.py       Flight-vs-HTTP-vs-Protobuf-vs-JSON shoot-out (kob-bench)
```

## Install & run (30 seconds)

```bash
git clone git@github.com:iyedexe/kob.git && cd kob
uv sync                                       # or: pip install -e .  (Python ≥ 3.12)
KOB_DATA_ROOT=/path/to/parquet uv run kob     # Flight :8815 + Swagger :8000/docs
```

No data yet? Generate realistic samples and serve them:

```bash
uv sync --extra gen && uv run kob-gen --scale small && uv run kob
```

Open **http://localhost:8000/docs** — Swagger UI with a working **Try it out** for every
endpoint, plus a `/flight` page describing how to pull data over Flight.

## Query

One small JSON contract, used by Flight (as the descriptor/ticket) and by the HTTP
`/query` endpoint alike:

```json
{
  "dataset": "events",
  "columns": ["dt", "region", "user_id", "amount"],
  "filters": [
    {"column": "region", "op": "in",  "value": ["eu", "us"]},
    {"column": "amount", "op": ">=",  "value": 100.0}
  ],
  "limit": 100000
}
```

**Fast path — Arrow Flight** (Python; C#, C++ clients in [`clients/`](clients/README.md)):

```python
from kob.tools.client import query_flight
from kob.core.contract import QueryRequest, Filter

table = query_flight("grpc://127.0.0.1:8815",
                     QueryRequest(dataset="events",
                                  filters=[Filter("region", "in", ["eu", "us"])]))
```

**Interactive / interop — HTTP** returns JSON (default) or CSV, for the browser, Excel,
and quick debugging:

```bash
curl -s -X POST 'localhost:8000/query?format=csv' -H 'content-type: application/json' \
  -d '{"dataset":"events","filters":[{"column":"region","op":"=","value":"eu"}],"limit":1000}'
```

- **Operators:** `= != <> < <= > >= like "not like" ilike in "not in"`.
- **Filters** may target data columns **or** partition keys — partition filters prune whole
  folders before any file is read.
- For large or latency-sensitive pulls, use **Flight**; the HTTP `/query` is for
  exploration and interop, not throughput.

## API (Swagger HTTP)

| Endpoint | Returns |
|---|---|
| `GET /health` | O(1) liveness (never walks the data tree) |
| `GET /datasets` | every discovered dataset: partition + data columns (with types) |
| `GET /datasets/{name}` | **partition values** — the per-folder filter options |
| `GET /datasets/{name}/facets?columns=a,b[&distinct=true]` | per-column **min/max** (+ approx-distinct) — the per-column filter options |
| `GET /flight` | the Flight connection URI + how to pull results over it |
| `POST /query` | run a query interactively → JSON (default) or CSV |

Swagger UI at `/docs`, ReDoc at `/redoc`, raw schema at `/openapi.json`.
Append `?refresh=true` to `/datasets` or `/datasets/{name}` to bypass the discovery cache.

## Configuration (all optional)

| Env var | Default | Meaning |
|---|---|---|
| `KOB_DATA_ROOT` | `./data` | Folder the producer drops Parquet into |
| `KOB_DISCOVERY_TTL` | `15` | Seconds to cache discovery before re-scanning |
| `KOB_DUCKDB_THREADS` | all cores | DuckDB threads |

CLI: `kob --host 0.0.0.0 --flight-port 8815 --http-port 8000 --threads 0`

## Demos & benchmark

The secondary transports exist **only to demonstrate** why the product serves Arrow over
Flight. Each is a small, standalone server in its own folder, and the benchmark spawns all
four to compare them head-to-head:

```bash
uv run kob-flight                 # Flight only, standalone (:8815)
uv run kob-demo-arrow             # Arrow IPC over HTTP (:8001)
uv run kob-demo-json              # REST/JSON baseline   (:8002)
uv sync --extra bench && uv run kob-demo-proto    # gRPC/Protobuf (:8816)
uv run --extra bench kob-bench --scale small      # the shoot-out → docs/BENCHMARKS.md
```

Or run the **demo notebook** — it spins up every transport, runs the *same* query over
Flight / HTTP-Arrow / JSON / Protobuf, checks they return identical data, and times them:

```bash
uv sync --extra demo && make notebook    # notebooks/kob_transports_demo.ipynb
```

Measured on this data: Flight is **150–1750× faster than JSON** and **5–56× faster than
the best Protobuf** ([full report](docs/PERFORMANCE_REPORT.md)).

## Clients

Working clients for **Python, C#, C++ and Excel** — see [`clients/`](clients/README.md).
All speak the JSON contract above. Flight (`:8815`) is the recommended transport; Excel and
other HTTP-only consumers use the Swagger server's JSON/CSV `/query` (`:8000`).

## Why Arrow + Flight?

JSON and Protobuf are **row-oriented**: the sender encodes every value field-by-field and
the receiver decodes every value and rebuilds columns — cost scales with `rows × cols`.

Apache Arrow's **columnar wire layout *is* its in-memory layout**, so reading a result is a
header parse plus a buffer-pointer setup — essentially nothing to deserialize. Arrow
**Flight** carries those same buffers straight over gRPC; it uses Protobuf only for its tiny
control envelope (tickets/descriptors), never for the bulk data. Architecture and rationale:
[`docs/DESIGN.md`](docs/DESIGN.md); full 5-way benchmark: [`docs/PERFORMANCE_REPORT.md`](docs/PERFORMANCE_REPORT.md).

## License

Code: **MIT**. Synthetic sample data: **CC0** (public domain). See [`LICENSE`](LICENSE).
