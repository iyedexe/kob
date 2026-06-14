# Performance Report — Arrow vs Protobuf vs REST/JSON

A head-to-head of five wire protocols serving the **same** DuckDB query results from the
**same** nested-Parquet store. Only the transport/serialization changes.

| # | Transport | What it is |
|---|---|---|
| **A** | **Arrow Flight** | gRPC/HTTP-2 carrying Arrow IPC record batches (columnar, zero-copy) |
| **B** | **Arrow IPC / HTTP** | Arrow IPC stream over plain HTTP; `zstd` and uncompressed (`none`) |
| — | **Protobuf / gRPC (columnar)** | packed scalar columns — "protobuf done right" |
| — | **Protobuf / gRPC (row)** | generic `oneof` cells — the honest equivalent of a JSON row API |
| — | **REST / JSON** | row objects over HTTP — the ubiquitous baseline |

---

## TL;DR

- **Apache Arrow wins by one to three orders of magnitude.** Flight and HTTP-Arrow are
  **150–1750× faster than REST/JSON** and **5–56× faster than the *best* protobuf**.
- **Protobuf is much better than JSON but nowhere near Arrow.** Columnar protobuf is
  **13–45× faster than JSON**; row protobuf **7–25×**. Both are **10–56× slower than Arrow**.
- **The differentiator is (de)serialization, not "binary vs text".** Protobuf is binary and
  still loses badly, because — like JSON — it must encode/decode every value. Arrow's wire
  layout *is* its in-memory layout, so there is essentially nothing to decode.
- **Flight ≈ HTTP-Arrow** on a single localhost stream; Flight edges ahead on large pulls and
  has headroom (parallel endpoints, concurrency) this single-client test doesn't exercise.
- **On the wire:** Arrow+zstd is smallest; JSON is **8× larger** than Arrow+zstd and **2× larger
  than row-protobuf**.

---

## Method

- **Engine held constant:** every transport runs the identical DuckDB query over the same
  Hive-partitioned Parquet (projection + predicate + partition pushdown), then serializes the
  resulting Arrow batches its own way. The only variable is the wire format.
- **What's timed:** end-to-end client wall time — issue request → receive all bytes → fully
  decode into the language's native form (Arrow Table / parsed protobuf messages / `json.loads`
  list). This is the number an application actually feels.
- **Reps:** columnar transports = median of 3 after a warmup. Row-oriented baselines
  (`rest_json`, `proto_row`) = a single run (they are slow; a warmup would only add minutes).
- **Reproduce:** `uv run --extra bench kob-bench --scale medium --reps 3`

### Setup

| | |
|---|---|
| Machine | Apple M4, 10 cores, 16 GB RAM, macOS 26.3 |
| Server | single Python process per transport, DuckDB threads = 4 |
| Network | localhost loopback — **understates** the win of compact formats on a real network |
| Dataset | `medium`: 4.77M rows / 217 MB Parquet (georev 1.28M, optionmetrics 3.49M) |
| Versions | pyarrow 24.0.0, DuckDB 1.5.3, Arrow C++/Flight 24.0.0, grpcio 1.81 / protobuf 6.33 |

---

## Results (medium dataset)

```
scenario                 method                 rows  wire MB  mem MB  median ms   best ms     MB/s
---------------------------------------------------------------------------------------------------
om_bulk_one_chain        flight               87,360        -    12.7       23.4      23.3      541
om_bulk_one_chain        http_arrow_zstd      87,360      5.7    12.7       28.5      28.0      444
om_bulk_one_chain        http_arrow_none      87,360     12.7    12.7       22.9      22.6      552
om_bulk_one_chain        proto_columnar       87,360     12.0       -      506.2     505.4        -
om_bulk_one_chain        proto_row            87,360     17.1       -      980.9     980.9        -
om_bulk_one_chain        rest_json            87,360     32.8       -     6735.3    6735.3        -
om_filtered_pushdown     flight               29,348        -     1.2       13.5      13.5       89
om_filtered_pushdown     http_arrow_zstd      29,348      0.5     1.2       12.0      11.5      101
om_filtered_pushdown     http_arrow_none      29,348      1.2     1.2       11.4      11.4      105
om_filtered_pushdown     proto_columnar       29,348      1.4       -       67.8      66.3        -
om_filtered_pushdown     proto_row            29,348      1.9       -      117.4     117.4        -
om_filtered_pushdown     rest_json            29,348      3.5       -     2049.9    2049.9        -
om_all_underlyings_year  flight            1,747,200        -    70.4       66.9      65.5     1052
om_all_underlyings_year  http_arrow_zstd   1,747,200     24.8    70.4      114.9     114.5      613
om_all_underlyings_year  http_arrow_none   1,747,200     70.4    70.4       71.5      71.4      985
om_all_underlyings_year  proto_columnar    1,747,200     66.9       -     3741.2    3734.3        -
om_all_underlyings_year  proto_row         1,747,200     96.6       -     6475.6    6475.6        -
om_all_underlyings_year  rest_json         1,747,200    201.8       -   117128.9  117128.9        -
georev_full_year         flight              320,000        -    38.3       42.1      41.2      911
georev_full_year         http_arrow_zstd     320,000     10.5    38.3       58.2      56.4      658
georev_full_year         http_arrow_none     320,000     38.3    38.3       47.0      46.4      815
georev_full_year         proto_columnar      320,000     34.1       -     1545.9    1537.2        -
georev_full_year         proto_row           320,000     46.9       -     2781.9    2781.9        -
georev_full_year         rest_json           320,000    112.3       -    23324.7   23324.7        -
georev_projection        flight              320,000        -    14.1       20.3      20.3      695
georev_projection        http_arrow_zstd     320,000      5.1    14.1       23.4      23.3      603
georev_projection        http_arrow_none     320,000     14.1    14.1       20.7      20.7      681
georev_projection        proto_columnar      320,000     12.2       -      469.4     464.6        -
georev_projection        proto_row           320,000     15.7       -      850.9     850.9        -
georev_projection        rest_json           320,000     31.8       -    21080.8   21080.8        -
```

### Speedup vs REST/JSON (median latency, same query)

| scenario | rows | Flight | HTTP-Arrow z | proto-columnar | proto-row |
|---|---:|---:|---:|---:|---:|
| om_bulk_one_chain | 87,360 | **288×** | 236× | 13× | 7× |
| om_filtered_pushdown | 29,348 | **152×** | 171× | 30× | 17× |
| om_all_underlyings_year | 1,747,200 | **1751×** | 1019× | 31× | 18× |
| georev_full_year | 320,000 | **554×** | 401× | 15× | 8× |
| georev_projection | 320,000 | **1038×** | 900× | 45× | 25× |

### Arrow vs the *best* protobuf (Flight ÷ proto-columnar latency)

| scenario | Arrow Flight is faster than columnar protobuf by |
|---|---:|
| om_bulk_one_chain | **22×** |
| om_filtered_pushdown | **5×** |
| om_all_underlyings_year | **56×** |
| georev_full_year | **37×** |
| georev_projection | **23×** |

---

## Analysis

### 1. Latency — Arrow is in a different class

For the 1.75M-row pull, Arrow Flight returns in **67 ms**. The same result is **3.7 s** over
columnar protobuf, **6.5 s** over row protobuf, and **117 s** over JSON. That is not a tuning
gap; it is a structural one. JSON and protobuf must visit every value to encode it (server) and
again to decode it (client); Arrow ships the columnar buffers as-is.

### 2. The gap grows with result size

Arrow-vs-JSON goes from ~150× on a 29k-row filtered slice to **~1750×** on 1.75M rows. Per-value
serialization cost scales with `rows × cols`; Arrow's near-zero decode does not. Choose the wrong
format and your latency degrades **super-linearly** with data size.

### 3. Binary ≠ fast: protobuf is the proof

Protobuf is a compact binary format and still loses to Arrow by **5–56×**. Why? It is a
*messaging* format, not an in-memory layout — values are tag-encoded and must be parsed into
objects. This is exactly why **Arrow Flight itself uses protobuf only for its tiny control
envelope** (tickets/descriptors) and never for the bulk data.

### 4. Columnar beats row — even within protobuf

Columnar protobuf is **~1.8× faster and ~30% smaller** than row protobuf (e.g. 3,741 vs 6,476 ms;
66.9 vs 96.6 MB on the big pull). Same lesson as Arrow vs JSON, in miniature: column layout +
packed scalars cut per-value overhead. But once you go columnar-over-protobuf you have hand-built
a slower, lossier Arrow — so just use Arrow.

### 5. Payload size on the wire

| format (om_all, 1.75M rows) | wire MB | vs Arrow+zstd |
|---|---:|---:|
| Arrow IPC + zstd | 24.8 | 1.0× |
| Arrow IPC (none) | 70.4 | 2.8× |
| protobuf columnar | 66.9 | 2.7× |
| protobuf row | 96.6 | 3.9× |
| JSON | 201.8 | **8.1×** |

JSON is a bandwidth hog too — 8× heavier than Arrow+zstd. Note **packed columnar protobuf is about
as compact as *uncompressed* Arrow**, yet ~52× slower to produce/consume here: compactness on the
wire does not buy you decode speed.

### 6. Flight vs HTTP-Arrow, and the zstd trade-off

On a single localhost stream the two Arrow transports are neck-and-neck (Flight 67 ms vs HTTP-none
72 ms on the big pull; Flight sustains ~1,050 MB/s). **zstd costs latency on loopback** (115 vs 72 ms)
because bandwidth is free here, but it ships **2.8× fewer bytes** — over a real WAN that reverses the
ranking. Rule of thumb: **`none` on a LAN/loopback, `zstd` across a network.** Flight's structural
advantages (parallel endpoints, multiplexed concurrent streams) are not exercised by this single
sequential client and would widen its lead under load.

### 7. Pushdown still matters regardless of format

`om_filtered_pushdown` moves 29k rows in ~12 ms / 0.5 MB (Arrow+zstd) because DuckDB prunes
partitions, row-groups and columns *before* serialization. Pushing the filter to the server is
orthogonal to — and compounds with — picking a fast wire format.

---

## Fairness & caveats

- **Python server.** Row-oriented encoders (JSON, proto-row) pay per-value *Python* overhead; in
  Go/Rust/C++ their absolute numbers would improve. But (a) the target here **is** a Python
  microservice, and (b) Arrow's "no serialization" advantage is language-independent — it would
  still dominate. Columnar protobuf already uses bulk C-accelerated `extend()`, so it is close to
  protobuf's practical ceiling in Python.
- **Wire-size measurement differs by transport.** HTTP rows count actual socket bytes; protobuf
  sums serialized message sizes (excludes HTTP/2 framing); Flight wire size is not instrumented
  (shown as `-`). Treat wire MB as ±framing, not to three significant figures.
- **Single client, loopback, no concurrency.** Real networks add latency that punishes chatty/large
  payloads more, and concurrency favors Flight's multiplexing — both make Arrow look *better* than
  shown here, not worse.
- **`medium` dataset.** On the multi-GB `large` set the absolute throughput rises and the
  Arrow-vs-JSON gap widens further. Regenerate with `make data-large`, then re-run.

---

## Recommendation

For serving Parquet-backed dataframes to Python + C# (+ C++/Excel):

1. **Use Apache Arrow on the wire.** Not JSON (150–1750× slower, 8× heavier), not protobuf
   (5–56× slower).
2. **Start with Option B (Arrow IPC over HTTP).** ~90% of Flight's speed with trivial infra;
   enable `zstd` across a network.
3. **Graduate to Flight (Option A)** when you need maximum throughput, parallel/partitioned
   streams, or many concurrent clients.
4. **Keep JSON only for tiny/metadata/debug endpoints**, and reserve protobuf for *control
   messages* — exactly where Arrow Flight itself uses it.

Reproduce everything: `uv run --extra bench kob-bench --scale medium --reps 3` (see also [`BENCHMARKS.md`](BENCHMARKS.md)).
