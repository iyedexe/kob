# Clients

Every client speaks the same JSON query contract. The **recommended** transport is **Arrow
Flight** (the product's fast, default data path). The Swagger server offers JSON/CSV for
browsers and spreadsheets, and a standalone Arrow-over-HTTP **demo** server exists for HTTP
clients that want Arrow without gRPC.

| Server | Start it with | Port | Serves |
|---|---|---|---|
| **Flight** (data, fast) | `uv run kob` | **8815** | Arrow IPC over gRPC |
| **Swagger** (discovery + interop) | `uv run kob` | **8000** | discovery, `POST /query` (JSON/CSV), `/docs` |
| Arrow-over-HTTP **demo** | `uv run kob-demo-arrow` | 8001 | Arrow IPC over plain HTTP |

`uv run kob` starts **Flight + Swagger together**. Start `kob-demo-arrow` only if you want
the HTTP-Arrow path.

| Client | Flight (:8815) | HTTP-Arrow demo (:8001) | Swagger JSON/CSV (:8000) | Notes |
|---|:---:|:---:|:---:|---|
| Python | ✅ | ✅ | ✅ | reference implementation |
| C# / .NET | ✅ | ✅ | — | needs `Apache.Arrow.Compression` for zstd/lz4 |
| C++ | ✅ | ✅ | — | Arrow C++ auto-decompresses |
| Excel | — | — | ✅ (CSV/JSON) | Excel can't parse Arrow; POSTs to `/query` on :8000 |

---

## Python

```bash
uv sync --extra client
# Flight — the fast default:
uv run kob-client --dataset optionmetrics --filter 'underlying:=:AAPL' --filter 'year:=:2023' --columns date,strike,impl_vol,delta --limit 5
# Arrow-over-HTTP demo (start `kob-demo-arrow` first):
uv run kob-client --transport http --port 8001 --dataset georev --filter 'year:=:2023' --columns company_id,region,revenue_usd --limit 5
```

Importable API: `from kob.tools.client import query_flight, query_http`.

## C# / .NET  (`clients/csharp/`)

Requires the .NET SDK (`brew install dotnet`). Defaults to Flight (`:8815`).

```bash
dotnet run --project clients/csharp -c Release -- \
    --transport flight --dataset optionmetrics --underlying AAPL --year 2023
```

Add `--transport http --http-port 8001` to also exercise the Arrow-over-HTTP demo (run
`kob-demo-arrow` first). Three things this client gets right (and you must too):
1. **`Apache.Arrow.Compression`** is a *separate* NuGet package — without it the reader
   can't decompress zstd/lz4. We pass a `CompressionCodecFactory` to `ArrowStreamReader`.
2. **gRPC `MaxReceiveMessageSize`** defaults to 4 MB; we raise it to 1 GB.
3. pyarrow's Flight server is plaintext **h2c**, so we enable unencrypted HTTP/2.

> Use `DuckDB.NET` only if you want to embed DuckDB *inside* the client — its result reader
> is row-oriented ADO.NET and gives up Arrow's zero-copy advantage, so it is the wrong tool
> for *receiving* columnar data from this server.

## C++  (`clients/cpp/`)

Requires Arrow C++ with Flight (`brew install apache-arrow`) and libcurl (system). Defaults
to Flight (`:8815`).

```bash
cd clients/cpp
cmake -S . -B build -DCMAKE_PREFIX_PATH=$(brew --prefix apache-arrow) -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/arrow_client --transport flight --dataset optionmetrics --underlying AAPL --year 2023
```

Add `--transport http --http-port 8001` for the Arrow-over-HTTP demo (run `kob-demo-arrow` first).

## Excel  (`clients/excel/`)

Excel has no native Arrow reader. `power_query.m` and `office_script.ts` POST the query
contract to the **Swagger server** (`:8000`) and read **CSV/JSON**; DuckDB still does the
filtering server-side.

* **`power_query.m`** — *recommended for local use.* Paste into Excel's Power Query Advanced
  Editor; refreshable, no Python needed. Pulls `/query?format=csv` from `:8000`.
* **`office_script.ts`** — Office Script; pulls `/query?format=json` from `:8000` (note the
  localhost sandbox caveat in the file).
* **`xlwings_client.py`** — pulls over the fast **Flight** path into pandas, then into Excel
  (`uv run --extra client python clients/excel/xlwings_client.py`). Falls back to `.xlsx`/`.csv`
  if Excel/xlwings aren't present.
