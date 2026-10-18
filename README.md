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

No gRPC where you are? Run it as one plain-HTTP server that still serves Arrow, with a
Swagger UI generated from your datasets ([REST mode](#rest-mode--kob---mode-rest)):

```bash
KOB_DATA_ROOT=/path/to/parquet uv run kob --mode rest --port 8000
#   data + docs      HTTP           http://127.0.0.1:8000/docs
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

## REST mode — `kob --mode rest`

When gRPC isn't an option (proxies, firewalls, browsers, plain-HTTP tooling), run kob as a
**single HTTP server** that still serves **Apache Arrow**:

```bash
KOB_DATA_ROOT=/path/to/parquet uv run kob --mode rest --port 9000
#   data + docs      HTTP           http://127.0.0.1:9000/docs
```

- **Swagger generated from your data.** Every discovered dataset gets its own section with
  typed endpoints. `GET /data/{dataset}` has one query parameter per column:
  - partition keys and string columns → multi-select pickers (`IN`), filled with real values;
  - date/timestamp columns → `col.start` / `col.end` (inclusive range — the column is always
    named explicitly, there is no implicit "main" date column);
  - numeric columns → `col` (exact, integers) and `col.min` / `col.max` (range);
  - `columns` (projection, an enum of the dataset's columns), `filter` (`col:op:value`), `limit`.

  `POST /data/{dataset}` takes the usual JSON contract, with column names as enums.
- **Arrow over REST.** Responses are an Arrow IPC stream (`application/vnd.apache.arrow.stream`,
  `?compression=none|zstd|lz4`). `?format=json|csv` is there for eyeballing results in Swagger.
- **Manual discovery.** The tree is scanned **once at startup** and then only when you press
  **Reload datasets** in the Swagger UI (or `POST /reload`). No request ever triggers a tree
  walk, so a huge data root costs nothing on the hot path. Reload rebuilds the Swagger spec,
  so new datasets, columns and partition values show up immediately.

```python
from kob.tools.client import KobClient

kob = KobClient("http://127.0.0.1:9000")        # REST; KobClient("grpc://…:8815") for Flight
kob.reload()                                    # rescan Parquet (same as the Swagger button)

om = kob["optionmetrics"]
tbl = om.query(om.date["2023-03-01":"2023-03-31"],   # inclusive at BOTH ends
               om.expiry[:"2023-04-30"],              # open start
               om.strike[90:110],                     # numbers slice too
               underlying=["AAPL", "MSFT"],           # list  → IN
               cp_flag="C",                           # value → =
               columns=["date", "expiry", "strike", "mid"], limit=10_000)
df = tbl.to_pandas()
```

See [Python slicing](#python-slicing) for the details, and
[`docs/QUERYING.md`](docs/QUERYING.md) for the full query reference.

## Python slicing

> Full reference — every syntax (Python, JSON, REST GET, CLI) side by side:
> [`docs/QUERYING.md`](docs/QUERYING.md).

`KobClient(url)` speaks REST (`http://`) or Flight (`grpc://`) with the same API. Index it by
dataset, then slice columns — attributes (`ds.date`) or items (`ds["my col"]`, for names that
aren't identifiers or clash with `query` / `request`):

```python
from kob.tools.client import KobClient
import datetime as dt

kob = KobClient("http://127.0.0.1:8000")
px = kob["close_prices"]

px.query(px.date["2024-01-01":"2024-03-31"])                 # Q1 2024, both ends included
px.query(px.date["2024-01-01":])                             # from 2024-01-01 on
px.query(px.date[:"2023-12-31"])                             # up to the end of 2023
px.query(px.date[dt.date(2024, 1, 1):dt.date(2024, 3, 31)])  # date objects work too
px.query(px.close[100:200], px.year[2022:2024])              # numbers, integer folders
px.query(px.date["2024-03-01":"2024-03-31"], security_id=["AAPL", "MSFT"], currency="USD")

dv = kob["dividends"]
dv.query(dv.ex_date["2024-01-01":"2024-03-31"], dv.pay_date[:"2024-06-30"])   # two ranges, AND-ed
```

- **Inclusive at both ends.** `col[a:b]` means start/end — unlike a Python list, `b` *is*
  included. No step (`col[a:b:2]` is an error).
- **Sliceable columns:** dates, timestamps and numbers (and `dt=` / `year=`-style folders). A
  slice on a string column is a 400 that lists the sliceable ones.
- **Keyword filters:** a list → `IN`, a scalar → `=`, a `slice(...)` object → range. Pass
  `Filter(col, op, value)` positionally for any other operator (`<`, `like`, `not in`, …).
- `ds.request(...)` returns the `QueryRequest` without sending it; `kob.reload()` and
  `kob.datasets()` work against a REST server.

## Arrow Flight vs Arrow over REST

Both carry **the same bytes** — Arrow IPC record batches, nothing to decode row by row. What
differs is the envelope around them:

| | Flight (`kob`, gRPC `:8815`) | Arrow over REST (`kob --mode rest`) |
|---|---|---|
| Protocol | gRPC on HTTP/2 | plain HTTP — one request, Arrow IPC body |
| Client needs | a Flight library (pyarrow, Arrow C++/C#/Java/Go/Rust) | any HTTP client + an Arrow IPC reader (`curl`, `requests`, browsers/JS) |
| Round trips | 2 (`GetFlightInfo` → `DoGet`); schema known before any data | 1; schema at the head of the body |
| Scale-out | multiple endpoints per query, parallel streams, HTTP/2 multiplexing | one stream per request |
| Errors | gRPC status, also mid-stream | HTTP status up front; a mid-stream error truncates the stream |
| Infrastructure | needs gRPC-aware proxies/LBs end to end | works through anything that speaks HTTP: gateways, WAFs, CDNs, Swagger |
| Discovery | `/docs` control plane on `:8000` | per-dataset Swagger generated from the data, with **Reload** |
| Discovery refresh | automatic (TTL cache) | manual (startup + Reload / `POST /reload`) |

Measured on one localhost stream they are neck and neck (1.75M rows: Flight 67 ms vs
HTTP-Arrow 72 ms) — both 150–1750× faster than JSON ([report](docs/PERFORMANCE_REPORT.md)).
Flight pulls ahead under concurrency and with parallel endpoints; REST wins on reach. Use
`compression=none` on a LAN and `zstd` across a WAN. The query contract is identical, so a
client switches with one call: `query_flight(...)` ↔ `query_rest(...)`.

**Pick Flight** for service-to-service bulk pulls on a network you control. **Pick REST**
when gRPC is blocked or awkward — corporate proxies, API gateways, browsers, Excel/Power
Query, `curl` scripts — or when people should explore the data through Swagger.

## Project layout

```
src/kob/
  core/      the product — no web/RPC code
    catalog.py    filesystem auto-discovery (datasets, partitions, columns)
    contract.py   the JSON query contract + parameterised SQL builder
    engine.py     DuckDB execution → streaming Apache Arrow
  server/    the `kob` entry point (Flight + Swagger, started together; or --mode rest)
    flight.py     Arrow Flight server (the fast, default data path)
    api.py        FastAPI app: Swagger UI, discovery, /flight how-to, interactive /query
    rest.py       `--mode rest`: Arrow over HTTP, per-dataset Swagger, manual Reload
    app.py        main(): runs Flight on a thread + the Swagger app in the foreground
  demos/     secondary transports — here only to demonstrate the alternatives
    arrow_http/   Arrow IPC over plain HTTP
    json/         REST/JSON (the slow, ubiquitous baseline)
    protobuf/     gRPC + Protocol Buffers (row + columnar)
  tools/     optional utilities (most need an install extra)
    open_data.py       real open datasets → Parquet (`kob --get-data`, no extra)
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

No data yet? Grab **real, open financial data** into the data root and serve it:

```bash
uv run kob --get-data                  # core sets: ~25 MB, ~5 s        (or: make get-data)
uv run kob --get-data all              # + ~60M rows of heavy data     (or: make get-data-heavy)
uv run kob --mode rest                 # → http://localhost:8000/docs
```

Open **http://localhost:8000/docs** — Swagger UI with a working **Try it out** for every
endpoint. (Synthetic samples are still available: `uv sync --extra gen && uv run kob-gen`.)

## Open datasets — `kob --get-data`

`kob --get-data [SETS]` downloads real, public data and writes it as Hive-partitioned
Parquet, sorted for range pruning, into the data root kob serves (`KOB_DATA_ROOT`, default
`./data`) — then exits. Sources and licences land in `<root>/SOURCES.md`. No extra
dependencies.

```bash
uv run kob --get-data list                                   # every set, with sizes
uv run kob --get-data                                        # = core (default)
uv run kob --get-data heavy                                  # the three heavy sets, default ranges
uv run kob --get-data crypto_klines_1m --symbols BTCUSDT,ETHUSDT --from 2021-01 --to 2024-12
uv run kob --get-data forex_ticks --from 2015-01 --to 2015-06   # ~6 x 70M ticks
uv run kob --get-data uk_house_prices --from 2015                # years 2015..2023
KOB_DATA_ROOT=/big/disk uv run kob --get-data all
```

Heavy sets are **resumable** — months/years already on disk are skipped — and each piece is
written to a staging folder outside the data root and moved in when complete, so a running
`kob` never sees a half-written partition (press **Reload** in REST mode to pick them up).

### Core sets (public CSVs on GitHub, ~5 s)

| dataset | rows | span | columns | partitions | source (licence) |
|---|---:|---|---|---|---|
| `prices_daily` | 619k | 2013-02 → 2018-02 | date, ticker, open, high, low, close, volume | `year` | Kaggle S&P 500 stock data, 505 tickers (CC0) |
| `fx_daily` | 263k | 1971 → today | date, currency, country, units_per_usd, usd_per_unit | `year` | Fed H.10 via FRED / datahub.io (PDDL) |
| `index_membership` | 1.6M | 1996 → 2025-08 | date, ticker | `index`, `year` | S&P 500 member list on each day membership changed — members on day D = latest snapshot ≤ D (Wikipedia-derived) |
| `index_membership_periods` | 1.3k | 1996 → today | index, ticker, start_date, end_date (null = still in) | — | fja05680/sp500 |
| `sp500_constituents` | 503 | today | ticker, name, sector, sub_industry, headquarters, date_added, cik, founded | — | datahub.io, from Wikipedia |
| `sp500_monthly` | 1.9k | 1871 → today | date, sp500, dividend, earnings, cpi, long_interest_rate, real_*, pe10 | — | Robert Shiller via datahub.io (PDDL) |
| `vix_daily` | 9.3k | 1990 → today | date, open, high, low, close | — | CBOE via datahub.io |
| `brent_daily` | 9.1k | 1987 → today | date, price | — | US EIA via datahub.io (PDDL) |

### Heavy sets (public S3 buckets, streamed)

| dataset | rows (default range) | span | columns | partitions | source (licence) |
|---|---:|---|---|---|---|
| `uk_house_prices` | 28.1M (all years, ~1 min) | 1995 → 2023-02 | date, price, postcode, type, is_new, duration, addr1, addr2, street, locality, town, district, county | `year` | HM Land Registry Price Paid (Open Government Licence v3.0 — attribution required) |
| `crypto_klines_1m` | ~5M (10 symbols x 12 months) | 2017 → last month | open_time, close_time (UTC), open, high, low, close, volume, quote_volume, trades, taker_buy_* | `symbol`, `year` | Binance public market data (see Binance's terms) |
| `forex_ticks` | 70–145M per month (default 2022-08: 144.5M, ~30 s) | 2000-05 → 2022-08 | datetime (UTC), bid, ask, base, quote | `pair`, `year` | histdata.com ticks, hosted by ClickHouse (check histdata's terms) |

Choose ranges with `--from` / `--to` (`YYYY` or `YYYY-MM`) and `--symbols`. The whole forex
archive is ~11.5 billion ticks / ~550 GB, so pick months deliberately.

```python
from kob.tools.client import KobClient
import duckdb

kob = KobClient("http://127.0.0.1:8000")
px, fx, mem = kob["prices_daily"], kob["fx_daily"], kob["index_membership"]

px.query(px.date["2017-01-01":"2017-03-31"], ticker=["AAPL", "MSFT"])   # Q1 2017 closes
px.query(px.volume[100_000_000:], columns=["date", "ticker", "volume"])  # 100M+ share days
fx.query(fx.date["2024-01-01":"2024-12-31"], currency=["EUR", "GBP"])    # FX for 2024
kob["vix_daily"].query(kob["vix_daily"].close[50:])                      # VIX panics

# The S&P 500 basket on 2017-06-30, priced in EUR: three queries + a client-side join.
day = "2017-06-30"
asof = max(mem.query(mem.date[:day], columns=["date"])["date"].to_pylist())   # snapshot <= day
members = mem.query(mem.date[asof:asof], columns=["ticker"])["ticker"].to_pylist()
closes = px.query(px.date[day:day], ticker=members, columns=["ticker", "close"])
eur = fx.query(fx.date[day:day], currency="EUR", columns=["usd_per_unit"])
duckdb.sql("SELECT c.ticker, c.close / e.usd_per_unit AS close_eur FROM closes c, eur e").df()
```

FX note: `units_per_usd` is units of the currency per 1 USD (EUR 0.92), `usd_per_unit` its
inverse (1 EUR = 1.09 USD). `index_membership_periods.end_date` is null for current
members.

## Query

> Complete reference with every syntax side by side: [`docs/QUERYING.md`](docs/QUERYING.md).

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
from kob.tools.client import KobClient

events = KobClient("grpc://127.0.0.1:8815")["events"]
table = events.query(events.dt["2026-06-01":"2026-06-30"], region=["eu", "us"])
```

**Interactive / interop — HTTP** returns JSON (default) or CSV, for the browser, Excel,
and quick debugging:

```bash
curl -s -X POST 'localhost:8000/query?format=csv' -H 'content-type: application/json' \
  -d '{"dataset":"events","filters":[{"column":"region","op":"=","value":"eu"}],"limit":1000}'
```

- **Operators:** `= != <> < <= > >= like "not like" ilike in "not in"`.
- **Ranges — always on a named column:** `ranges` maps a *sliceable* column (date,
  timestamp, numeric, plus `dt=2024-01-01` / `year=2024`-style folders — listed as
  `sliceable_columns` in `GET /datasets/{name}`) to an inclusive start/end; several columns
  are AND-ed, either side may be omitted:
  `"ranges": {"ex_date": {"start": "2024-01-01", "end": "2024-03-31"}, "amount": {"start": 0.5}}`.
  A range on any other column is a 400; there is no implicit default date column (a bare
  top-level `start` / `end` is rejected, never ignored). A date-only `end` on a timestamp
  column covers that whole day. Python: `ds.ex_date["2024-01-01":"2024-03-31"]`; REST GET:
  `?ex_date.start=…&ex_date.end=…` (numbers: `?amount.min=…`); CLI: `--range ex_date=2024-01-01..2024-03-31`.
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

## Tests

```bash
make test        # = uv run --extra client python tests/test_kob.py   (pytest works too)
```

Plain Python, no test framework. Covers discovery, the query contract, ranges, the engine,
REST mode (generated Swagger, GET parsing, manual reload) and an **end-to-end** run that
starts real REST and Flight servers in-process and checks the Python slicing client returns
identical Arrow tables over both. Without the `[client]` extra the end-to-end test is skipped.

## Configuration (all optional)

| Env var | Default | Meaning |
|---|---|---|
| `KOB_DATA_ROOT` | `./data` | Folder the producer drops Parquet into |
| `KOB_DISCOVERY_TTL` | `15` | Seconds to cache discovery before re-scanning |
| `KOB_DUCKDB_THREADS` | all cores | DuckDB threads |

CLI: `kob --host 0.0.0.0 --flight-port 8815 --http-port 8000 --threads 0`
or `kob --mode rest --host 0.0.0.0 --port 8000` (`KOB_DISCOVERY_TTL` doesn't apply in REST
mode — discovery there is manual).

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
All speak the JSON contract above. Flight (`:8815`) is the recommended transport; HTTP-only
consumers can use `kob --mode rest` (Arrow over HTTP, `query_rest` in the Python client) or,
for Excel, the Swagger server's JSON/CSV `/query` (`:8000`).

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
