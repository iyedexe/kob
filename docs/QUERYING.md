# Querying kob

Every query reads **one dataset** and returns an **Apache Arrow** table. A query is:

- a **projection** — which columns to return (default: all);
- **filters** — `column op value`, AND-ed;
- **ranges** — an inclusive start/end on *sliceable* columns, AND-ed;
- an optional **row limit**.

The same query can be written five ways — Python slicing, the JSON contract (REST POST and
Flight), REST GET parameters (what the Swagger form sends), and the CLI. They are all
equivalent; pick whichever fits the caller.

The examples use the sample data from `uv run --extra gen kob-gen --scale small`:
`optionmetrics` (partitioned by `underlying`, `year`; date columns `date`, `expiry`) and
`georev` (partitioned by `year`; date column `period_end`).

---

## 1. Python — slicing (recommended)

```python
from kob.tools.client import KobClient

kob = KobClient("http://127.0.0.1:8000")    # kob --mode rest   (Arrow over HTTP)
# kob = KobClient("grpc://127.0.0.1:8815")  # kob               (Arrow Flight) — same API

om = kob["optionmetrics"]
tbl = om.query(
    om.date["2023-03-01":"2023-03-31"],     # range on a date column — BOTH ends included
    om.expiry[:"2023-04-30"],               # open start
    om.strike[90:110],                      # numeric range
    underlying=["AAPL", "MSFT"],            # list   → IN
    cp_flag="C",                            # scalar → =
    columns=["date", "expiry", "strike", "mid"],
    limit=10_000,
)
df = tbl.to_pandas()
```

### Slices

| You write | Means |
|---|---|
| `ds.col[a:b]` | `a <= col <= b` — **inclusive at both ends** (not Python's half-open `[a, b)`) |
| `ds.col[a:]` | `col >= a` |
| `ds.col[:b]` | `col <= b` |
| `ds.col[a:b:step]` | error — no step |
| `ds.col[a]` | error — use a slice, or a keyword filter `col=a` |

- **Sliceable columns:** dates, timestamps, numbers, and partition folders whose values are
  all ISO dates (`dt=2024-01-01`) or all integers (`year=2024`). The server lists them as
  `sliceable_columns` in `GET /datasets/{name}`. Slicing anything else (e.g. a string
  column) is a 400 that names the sliceable columns.
- **Bounds:** ISO strings (`"2024-03-31"`, `"2024-03-31T16:00:00"`), `datetime.date` /
  `datetime.datetime` objects, or numbers.
- **Timestamps:** a date-only end covers that whole day — `ts[:"2024-03-31"]` includes
  `2024-03-31 23:59:59`. Give a datetime to cut inside the day.
- **Several slices** are AND-ed; one slice per column (combine two into one).
- **Column access:** `ds.date` or `ds["date"]`. Use the item form for names that aren't
  Python identifiers, start with `_`, or clash with `query` / `request`.

### Keyword filters

| Keyword value | Filter |
|---|---|
| list / tuple / set | `IN` |
| `slice(a, b)` | range (same as `ds.col[a:b]`) |
| anything else | `=` |

`columns=` and `limit=` are reserved. For any other operator pass a `Filter` positionally:

```python
from kob.core.contract import Filter
om.query(om.date["2023-03-01":"2023-03-31"], Filter("cp_flag", "!=", "P"), Filter("strike", "<", 100))
```

Operators: `= != <> < <= > >= like "not like" ilike in "not in"`.

### Other calls

```python
om.request(om.date["2023-03-01":"2023-03-31"])   # build the QueryRequest without sending it
kob.datasets()                                   # REST only: GET /datasets
kob.reload()                                     # REST only: rescan Parquet (POST /reload)
KobClient(url, compression="none")               # REST: Arrow IPC codec (zstd default; none on a LAN)
```

Low-level functions take a `QueryRequest`: `query_flight(url, req)`, `query_rest(url, req)`.

---

## 2. JSON contract — REST POST and Flight

```json
{
  "columns": ["date", "expiry", "strike", "mid"],
  "filters": [
    {"column": "underlying", "op": "in", "value": ["AAPL", "MSFT"]},
    {"column": "cp_flag",    "op": "=",  "value": "C"}
  ],
  "ranges": {
    "date":   {"start": "2023-03-01", "end": "2023-03-31"},
    "expiry": {"end": "2023-04-30"},
    "strike": {"start": 90, "end": 110}
  },
  "limit": 10000
}
```

- **REST:** `POST /data/{dataset}` with that body (`dataset` comes from the path).
  Query string: `format=arrow|json|csv` (default `arrow`), `compression=none|zstd|lz4`.
- **Flight:** add `"dataset": "optionmetrics"` and send it as the
  `FlightDescriptor.for_command(...)` (see `GET /flight` on the Flight-mode Swagger).
- `ranges` keys must be sliceable columns; either side may be omitted. A top-level
  `start` / `end` / `date_column` is rejected with a 400 — there is no implicit date column,
  and a dropped bound is never silently ignored.

```bash
curl -X POST "http://127.0.0.1:8000/data/optionmetrics?format=json" \
  -H "Content-Type: application/json" \
  -d '{"ranges": {"date": {"start": "2023-03-01", "end": "2023-03-31"}}, "limit": 5}'
```

---

## 3. REST GET — the Swagger form

`GET /data/{dataset}` takes one parameter per column (this is what each dataset's Swagger
section at `/docs` sends):

| Parameter | For | Means |
|---|---|---|
| `col.start` / `col.end` | date & timestamp columns, date folders | inclusive range |
| `col.min` / `col.max` | numeric columns | inclusive range |
| `col` (repeatable or comma-separated) | partitions, strings, ints, dates | `IN` |
| `col` | bool columns | `=` |
| `columns` | — | projection, comma-separated |
| `filter` (repeatable) | any column | `column:op:value`, e.g. `strike:<:100`, `cp_flag:in:C,P` |
| `limit`, `format`, `compression` | — | as above |

```bash
curl "http://127.0.0.1:8000/data/optionmetrics?date.start=2023-03-01&date.end=2023-03-31&expiry.end=2023-04-30&strike.min=90&strike.max=110&underlying=AAPL,MSFT&cp_flag=C&format=json&limit=10"
```

Unknown parameters are a 400 (so a typo never silently widens a query).

---

## 4. CLI

```bash
uv run --extra client kob-client --transport rest --dataset optionmetrics \
    --range date=2023-03-01..2023-03-31 --range expiry=..2023-04-30 --range strike=90..110 \
    --filter 'underlying:=:AAPL' --filter 'cp_flag:=:C' \
    --columns date,expiry,strike,mid --limit 10
```

`--range column=start..end` (repeatable, either side optional). Drop `--transport rest` to
use Flight; add `--reload` to rescan the server first.

---

## 5. Performance notes

- **Partition filters prune whole folders** before any file is opened — filter on
  `underlying=…` / `year=…` whenever you can (slicing an integer folder like
  `om.year[2022:2023]` prunes too).
- **Ranges and filters on data columns** use Parquet row-group statistics: blocks outside
  the range are skipped. Writing files sorted by the columns you range on (e.g.
  `security_id, date`) makes this much more effective.
- **Projection** reads only the requested columns — pass `columns=` for wide tables.
- **Compression:** `none` on a LAN / loopback, `zstd` across a WAN.
- **Joins** happen client-side: query each dataset, then join the Arrow tables (e.g.
  `duckdb.sql("SELECT … FROM a JOIN b USING (…)")` reads them zero-copy).
