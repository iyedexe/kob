"""Demo transport — **Apache Arrow IPC over plain HTTP**.

A standalone, single-purpose server that streams query results as an Arrow IPC stream over
HTTP. It is here to *demonstrate* the Arrow-over-HTTP option and to give the benchmark a
target; the product's recommended fast path is Arrow Flight (:mod:`kob.server.flight`),
which avoids HTTP framing entirely.

A background producer thread writes Arrow batches into a small bounded queue while the
response generator drains it, so DuckDB scans batch N+1 while batch N is on the wire
(pipelining), with the queue providing backpressure.

    python -m kob.demos.arrow_http.server --port 8001
"""

from __future__ import annotations

import argparse
import io
import os
import queue
import threading

import pyarrow as pa
import uvicorn
from fastapi import FastAPI, HTTPException, Query

from ...core import engine
from ...core.catalog import DATA_ROOT
from ...core.contract import QueryRequest

ARROW_STREAM_MIME = "application/vnd.apache.arrow.stream"


class _QueueFile(io.RawIOBase):
    def __init__(self, q: "queue.Queue[bytes | None]") -> None:
        self._q = q

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:  # type: ignore[override]
        self._q.put(bytes(b))
        return len(b)


def _stream_arrow(reader: pa.RecordBatchReader, compression: str | None):
    comp = None if compression in (None, "none", "") else compression
    opts = pa.ipc.IpcWriteOptions(compression=comp)
    q: "queue.Queue[bytes | None]" = queue.Queue(maxsize=16)

    def produce() -> None:
        try:
            sink = pa.output_stream(_QueueFile(q))
            writer = pa.ipc.new_stream(sink, reader.schema, options=opts)
            for batch in reader:
                writer.write_batch(batch)
                sink.flush()
            writer.close()
            sink.close()
        except Exception:  # noqa: BLE001 — a truncated stream signals the error to the client
            pass
        q.put(None)

    threading.Thread(target=produce, daemon=True).start()

    def consume():
        while True:
            item = q.get()
            if item is None:
                break
            yield item

    return consume()


def create_app() -> FastAPI:
    from fastapi.responses import StreamingResponse

    app = FastAPI(title="kob demo — Arrow IPC over HTTP", version="0.4.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "data_root": str(DATA_ROOT), "transport": "arrow-ipc/http"}

    @app.post("/query")
    async def query(body: dict, compression: str = Query("zstd", pattern="^(zstd|lz4|none)$")):
        try:
            reader = engine.execute_reader(QueryRequest.from_dict(body))
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from None
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from None
        return StreamingResponse(
            _stream_arrow(reader, compression),
            media_type=ARROW_STREAM_MIME,
            headers={"X-KOB-Compression": compression},
        )

    return app


app = create_app()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="kob demo — Arrow IPC over HTTP.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8001)
    p.add_argument("--threads", type=int, default=0, help="DuckDB threads (0 = all cores).")
    args = p.parse_args(argv)
    if args.threads:
        os.environ["KOB_DUCKDB_THREADS"] = str(args.threads)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
