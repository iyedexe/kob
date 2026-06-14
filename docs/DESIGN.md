# Design & rationale

## The question

A producer microservice drops Parquet under a folder. Serve it, read-only, to Python/C#/C++/Excel
clients — **self-discovering** (no hardcoded schema), **minimal** (tiny dependency surface), and
**blazing fast** — letting clients see what they can filter on **per folder and per column**.

## Self-discovering, not configured

There is no catalog of datasets in code. The server scans `KOB_DATA_ROOT` and derives everything
from the filesystem:

- **Datasets** = top-level folders containing Parquet.
- **Partition columns + values** = the `key=value` folder levels (read from folder *names* only —
  no data scan). These are the "filter per folder" options.
- **Data columns + types** = the schema of one Parquet file (metadata only). These are the
  "filter per column" options; `facets` adds min/max from row-group statistics (still cheap).

Discovery is cached with a short TTL (`KOB_DISCOVERY_TTL`, default 15s), so freshly-dropped
files/partitions/datasets surface with no restart while steady-state lookups stay hot. The result
is a stateless, self-configuring service: point it at a folder and it just works.

**Security:** a dataset name is one path segment that must resolve *inside* the data root (no `..`
escape), only discovered names are accepted, columns/filters are validated against the discovered
schema, and every value is a bound parameter — so untrusted clients cannot traverse the filesystem
or inject SQL.

## Minimal by construction

The core server depends on only `duckdb`, `pyarrow`, `fastapi`, `uvicorn`. Everything else — the
sample-data generator, the Python client CLI, the benchmark harness, the gRPC/Protobuf baseline —
is an opt-in extra and never loaded by the server. No database, no message broker, no background
indexer; discovery is a directory walk plus one metadata read.

## Why Apache Arrow (and not JSON or Protobuf)

JSON and Protobuf are **row-oriented**: the sender encodes every value field-by-field and
the receiver must decode every value and rebuild columns. Cost scales with `rows × cols`,
and the wire bytes are never the in-memory representation.

Apache Arrow's **columnar wire layout *is* its in-memory layout**. Reading a result is a
header parse plus buffer pointer setup — effectively zero-copy, no per-value decode. Apache
Arrow's own FAQ notes that (de)serialization "can often represent 80-90% of computing costs"
in analytical workloads — exactly the cost Arrow removes.

That is why even **Arrow Flight uses Protobuf only for its small control envelope**
(tickets/descriptors) and never for the bulk data. Protobuf for the *handshake*, Arrow for
the *payload*.

| Format | Orientation | Bulk-dataframe verdict |
|---|---|---|
| JSON | row, text | ❌ slowest; fine only for tiny/metadata/debug |
| Protobuf | row, binary | ❌ fast for records, but still full decode; loses zero-copy |
| **Arrow IPC** | **columnar** | ✅ near-zero deserialization |
| Parquet-on-wire | columnar, compressed | ◑ smallest payload, but pays encode/decode |

The numbers in [`BENCHMARKS.md`](BENCHMARKS.md) show Arrow beating JSON by **hundreds of ×**
on this data — and the gap grows with result size.

## Transports

The **default** server is Arrow IPC over plain HTTP — minimal infra, works through any proxy/gateway,
trivially debuggable. Arrow **Flight** (gRPC) ships as an optional server for maximum throughput,
parallel/streaming pulls, or many concurrent clients. Both share the same discovery, contract and
DuckDB engine — the only difference is the wire.

## The query engine: DuckDB

DuckDB reads the Hive-partitioned Parquet directly and stacks three I/O reducers:
**partition pruning** (folder names), **projection pushdown** (only needed columns), and
**predicate pushdown** (row-group zonemaps). So a filtered query transfers only the slice a
client asked for, and DuckDB streams results out as Arrow with no row-by-row conversion.
This covers both the "bulk" and the "analytical query" access patterns with one engine.

## The C#/.NET reality (the riskiest piece, mid-2026)

- **`Apache.Arrow`** (IPC reader) and **`Apache.Arrow.Flight`** (gRPC) are production-viable
  but a *second-tier* Arrow implementation (no compute kernels, thinner docs).
- **LZ4/ZSTD needs the separate `Apache.Arrow.Compression` package** + a
  `CompressionCodecFactory` wired into the reader. Core Arrow won't decompress without it.
- **gRPC `MaxReceiveMessageSize` defaults to 4 MB** and rejects large RecordBatches — raise it.
- **`DuckDB.NET` is row-oriented ADO.NET with no Arrow result API** — great for embedding
  DuckDB in a client, wrong for *receiving* columnar data from this server.

The C# client in `clients/csharp/` handles all four points.

## Scope notes

- Real FactSet GeoRev / OptionMetrics are proprietary; this repo ships a generator that
  fabricates same-shape data (see `generate_data.py`), so it is freely redistributable.
- Read-only by design: there is no write/ingest path, which keeps the server lightweight and
  means the C# Flight SQL write-side gaps (transactions/savepoints/Substrait) are irrelevant.
