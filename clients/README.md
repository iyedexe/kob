# Clients

Every client speaks the same JSON query contract and pulls **Apache Arrow** back over
one of the two transports:

* **HTTP + Arrow IPC** (default, port **8000**)
* **Arrow Flight** (gRPC, optional, port **8815**)

Start a server first (from the repo root):

```bash
uv run kob          # HTTP on :8000  (Swagger at /docs)
uv run kob-flight   # Flight on :8815
```

| Client | Flight | HTTP-Arrow | Notes |
|---|:---:|:---:|---|
| Python | ✅ | ✅ | reference implementation |
| C# / .NET | ✅ | ✅ | needs `Apache.Arrow.Compression` for zstd/lz4 |
| C++ | ✅ | ✅ | Arrow C++ auto-decompresses |
| Excel | — | ✅ (CSV/JSON) | Excel can't parse Arrow; uses `POST /query?format=csv\|json` |

---

## Python

```bash
uv sync --extra client
uv run kob-client --dataset optionmetrics --filter 'underlying:=:AAPL' --filter 'year:=:2023' --columns date,strike,impl_vol,delta --limit 5
uv run kob-client --transport flight --dataset georev --filter 'year:=:2023' --columns company_id,region,revenue_usd --limit 5
```

Importable API: `from kob.client import query_http, query_flight`.

## C# / .NET  (`clients/csharp/`)

Requires the .NET SDK (`brew install dotnet`).

```bash
dotnet run --project clients/csharp -c Release -- \
    --transport both --dataset optionmetrics --underlying AAPL --year 2023
```

Three things this client gets right (and you must too):
1. **`Apache.Arrow.Compression`** is a *separate* NuGet package — without it the
   reader can't decompress zstd/lz4. We pass a `CompressionCodecFactory` to
   `ArrowStreamReader`.
2. **gRPC `MaxReceiveMessageSize`** defaults to 4 MB; we raise it to 1 GB.
3. pyarrow's Flight server is plaintext **h2c**, so we enable unencrypted HTTP/2.

> Use `DuckDB.NET` only if you want to embed DuckDB *inside* the client — its result
> reader is row-oriented ADO.NET and gives up Arrow's zero-copy advantage, so it is the
> wrong tool for *receiving* columnar data from this server.

## C++  (`clients/cpp/`)

Requires Arrow C++ with Flight (`brew install apache-arrow`) and libcurl (system).

```bash
cd clients/cpp
cmake -S . -B build -DCMAKE_PREFIX_PATH=$(brew --prefix apache-arrow) -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/arrow_client --transport both --dataset optionmetrics --underlying AAPL --year 2023
```

## Excel  (`clients/excel/`)

Excel has no native Arrow reader, so these POST the standard query contract to
`/query?format=csv|json`. DuckDB still does the filtering server-side.

* **`power_query.m`** — *recommended for local use.* Paste into Excel's Power Query
  Advanced Editor; refreshable, no Python needed.
* **`xlwings_client.py`** — pulls over the fast Arrow path into pandas, then into Excel
  (`uv run --extra client python clients/excel/xlwings_client.py`). Falls back to
  `.xlsx`/`.csv` if Excel/xlwings aren't present.
* **`office_script.ts`** — Office Script; works where the Office runtime can reach the
  server (note the localhost sandbox caveat in the file).
