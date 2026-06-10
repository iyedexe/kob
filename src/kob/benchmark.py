"""Head-to-head benchmark: Arrow Flight vs Arrow-IPC-over-HTTP vs Protobuf vs JSON.

By default it spawns all three servers as subprocesses, runs a set of representative
queries (bulk + pushdown) over whichever data you generated, and prints a comparison
of wall-clock latency, wire payload size and throughput. Use ``--no-spawn`` to point
at servers you started yourself.

    uv run --extra bench kob-bench --scale small
    uv run --extra bench kob-bench --reps 5 --http-port 8000 --flight-port 8815
"""

from __future__ import annotations

import argparse
import io
import json
import socket
import statistics
import subprocess
import sys
import time

import grpc
import pyarrow as pa
import pyarrow.flight as flight
import requests

from .contract import Filter, QueryRequest
from .proto import data_service_pb2 as pb
from .proto import data_service_pb2_grpc as pbg

_GRPC_MAX = 512 * 1024 * 1024


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #
def scenarios(year: int = 2023, underlying: str = "AAPL") -> list[tuple[str, QueryRequest]]:
    return [
        ("om_bulk_one_chain", QueryRequest(
            dataset="optionmetrics",
            filters=[Filter("underlying", "=", underlying), Filter("year", "=", year)],
        )),
        ("om_filtered_pushdown", QueryRequest(
            dataset="optionmetrics",
            columns=["date", "strike", "cp_flag", "impl_vol", "delta", "und_price"],
            filters=[Filter("underlying", "=", underlying), Filter("year", "=", year),
                     Filter("cp_flag", "=", "C"), Filter("delta", ">=", 0.4)],
        )),
        ("om_all_underlyings_year", QueryRequest(
            dataset="optionmetrics",
            columns=["date", "underlying", "strike", "cp_flag", "impl_vol", "volume"],
            filters=[Filter("year", "=", year)],
        )),
        ("georev_full_year", QueryRequest(
            dataset="georev",
            filters=[Filter("year", "=", year)],
        )),
        ("georev_projection", QueryRequest(
            dataset="georev",
            columns=["company_id", "region", "segment", "revenue_usd"],
            filters=[Filter("year", "=", year)],
        )),
    ]


# --------------------------------------------------------------------------- #
# Fetch methods — each returns (rows, in_memory_bytes, wire_bytes|None)
# --------------------------------------------------------------------------- #
class _CountingReader(io.RawIOBase):
    """Wrap a file-like, counting bytes read (to measure on-the-wire payload size)."""

    def __init__(self, raw):
        super().__init__()
        self._raw = raw
        self.count = 0

    def readable(self) -> bool:
        return True

    def read(self, n=-1):
        b = self._raw.read(n)
        self.count += len(b)
        return b

    def readinto(self, b):
        data = self._raw.read(len(b))
        self.count += len(data)
        b[: len(data)] = data
        return len(data)


def fetch_http_arrow(base: str, req: QueryRequest, compression: str):
    resp = requests.post(f"{base}/query", params={"format": "arrow", "compression": compression},
                         json=req.to_dict(), stream=True, timeout=600)
    resp.raise_for_status()
    resp.raw.decode_content = True
    counter = _CountingReader(resp.raw)
    table = pa.ipc.open_stream(counter).read_all()
    return table.num_rows, table.nbytes, counter.count


def fetch_http_json(base: str, req: QueryRequest):
    resp = requests.post(f"{base}/query", params={"format": "json"},
                         json=req.to_dict(), stream=True, timeout=600)
    resp.raise_for_status()
    payload = resp.content
    rows = json.loads(payload)
    return len(rows), None, len(payload)


def fetch_flight(location: str, req: QueryRequest):
    client = flight.connect(location)
    descriptor = flight.FlightDescriptor.for_command(json.dumps(req.to_dict()).encode())
    info = client.get_flight_info(descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()
    return table.num_rows, table.nbytes, None


def fetch_proto(host: str, port: int, req: QueryRequest, encoding: str):
    """gRPC + Protobuf. wire_bytes = sum of serialized message sizes (excludes h2 framing)."""
    channel = grpc.insecure_channel(
        f"{host}:{port}",
        options=[("grpc.max_receive_message_length", _GRPC_MAX),
                 ("grpc.max_send_message_length", _GRPC_MAX)])
    stub = pbg.DataServiceStub(channel)
    request = pb.QueryRequest(request_json=json.dumps(req.to_dict()), encoding=encoding)
    rows = 0
    wire = 0
    for reply in stub.Query(request):
        wire += reply.ByteSize()
        which = reply.WhichOneof("payload")
        if which == "row_batch":
            rows += len(reply.row_batch.rows)
        elif which == "col_batch":
            rows += reply.col_batch.num_rows
            # touch each column so the packed arrays are fully materialised (fair decode cost)
            for col in reply.col_batch.columns:
                _ = len(col.doubles) + len(col.ints) + len(col.strings) + len(col.bools)
    channel.close()
    return rows, None, wire


# --------------------------------------------------------------------------- #
# Server lifecycle
# --------------------------------------------------------------------------- #
def _wait_tcp(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"server on {host}:{port} did not come up")


def _wait_http(base: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{base}/health", timeout=1).ok:
                return
        except requests.RequestException:
            time.sleep(0.2)
    raise TimeoutError(f"HTTP server {base} did not become healthy")


def spawn_servers(host, http_port, flight_port, proto_port, threads):
    def launch(module, port):
        return subprocess.Popen(
            [sys.executable, "-m", module, "--host", host, "--port", str(port),
             "--threads", str(threads)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    procs = [
        launch("kob.server_http", http_port),
        launch("kob.server_flight", flight_port),
        launch("kob.server_proto", proto_port),
    ]
    _wait_http(f"http://{host}:{http_port}")
    _wait_tcp(host, flight_port)
    _wait_tcp(host, proto_port)
    time.sleep(0.5)  # gRPC servers need a beat after the port opens
    return procs


# --------------------------------------------------------------------------- #
# Run
# --------------------------------------------------------------------------- #
def _timeit(fn, reps: int, warmup: bool = True):
    rows = in_mem = wire = None
    if warmup:
        fn()
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        rows, in_mem, wire = fn()
        times.append(time.perf_counter() - t0)
    return rows, in_mem, wire, statistics.median(times), min(times)


def _fmt_mb(n):
    return "-" if n is None else f"{n / 1e6:.1f}"


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Benchmark Flight vs HTTP-Arrow vs JSON.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--http-port", type=int, default=8000)
    p.add_argument("--flight-port", type=int, default=8815)
    p.add_argument("--proto-port", type=int, default=8816)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--year", type=int, default=2023)
    p.add_argument("--underlying", default="AAPL")
    p.add_argument("--no-spawn", action="store_true", help="Use already-running servers.")
    p.add_argument("--scale", default="small", help="Informational label for the report.")
    p.add_argument("--out", help="Optional path to write a Markdown report.")
    args = p.parse_args(argv)

    base = f"http://{args.host}:{args.http_port}"
    loc = f"grpc://{args.host}:{args.flight_port}"

    procs: list = []
    if not args.no_spawn:
        print("Spawning servers ...")
        procs = spawn_servers(args.host, args.http_port, args.flight_port, args.proto_port, args.threads)

    host = args.host
    proto_port = args.proto_port
    # Ordered fastest-expected first; rest_json is the baseline.
    methods = [
        ("flight", lambda req: fetch_flight(loc, req)),
        ("http_arrow_zstd", lambda req: fetch_http_arrow(base, req, "zstd")),
        ("http_arrow_none", lambda req: fetch_http_arrow(base, req, "none")),
        ("proto_columnar", lambda req: fetch_proto(host, proto_port, req, "columnar")),
        ("proto_row", lambda req: fetch_proto(host, proto_port, req, "row")),
        ("rest_json", lambda req: fetch_http_json(base, req)),
    ]
    # The row-oriented baselines are slow — measure them once, no warmup.
    slow_methods = {"rest_json", "proto_row"}

    rows_out: list[dict] = []
    try:
        for sc_name, req in scenarios(args.year, args.underlying):
            for m_name, fn in methods:
                # Row-oriented baselines are slow — measure once, no warmup, to keep runtime sane.
                reps = 1 if m_name in slow_methods else args.reps
                warmup = m_name not in slow_methods
                try:
                    rows, in_mem, wire, med, best = _timeit(lambda: fn(req), reps, warmup)
                except Exception as exc:  # keep going if one method fails
                    print(f"  [{sc_name}/{m_name}] ERROR: {exc}")
                    continue
                mbps = (in_mem / med / 1e6) if in_mem else float("nan")
                rows_out.append(dict(scenario=sc_name, method=m_name, rows=rows,
                                     wire_mb=_fmt_mb(wire), inmem_mb=_fmt_mb(in_mem),
                                     med_ms=med * 1e3, best_ms=best * 1e3, mbps=mbps))
    finally:
        for pr in procs:
            pr.terminate()
        for pr in procs:
            try:
                pr.wait(timeout=5)
            except Exception:
                pr.kill()

    # ------- report ------- #
    hdr = f"{'scenario':<24} {'method':<16} {'rows':>10} {'wire MB':>8} {'mem MB':>7} {'median ms':>10} {'best ms':>9} {'MB/s':>8}"
    lines = [hdr, "-" * len(hdr)]
    for r in rows_out:
        mbps = "-" if r["mbps"] != r["mbps"] else f"{r['mbps']:.0f}"  # nan check
        lines.append(f"{r['scenario']:<24} {r['method']:<16} {r['rows']:>10,} {r['wire_mb']:>8} "
                     f"{r['inmem_mb']:>7} {r['med_ms']:>10.1f} {r['best_ms']:>9.1f} {mbps:>8}")
    report = "\n".join(lines)
    print("\n" + report)

    # Speedup vs the REST/JSON baseline
    print("\nSpeedup vs REST/JSON (median latency, same query):")
    by = {(r["scenario"], r["method"]): r for r in rows_out}
    compare = ["flight", "http_arrow_zstd", "proto_columnar", "proto_row"]
    for sc_name, _ in scenarios(args.year, args.underlying):
        base_r = by.get((sc_name, "rest_json"))
        if not base_r:
            continue
        parts = []
        for m in compare:
            r = by.get((sc_name, m))
            if r:
                parts.append(f"{m} {base_r['med_ms'] / r['med_ms']:.0f}x")
        print(f"  {sc_name:<24} " + ", ".join(parts))

    if args.out:
        with open(args.out, "w") as fh:
            fh.write(f"# Benchmark (scale={args.scale}, reps={args.reps})\n\n")
            fh.write("```\n" + report + "\n```\n")
        print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
