"""Baseline transport — gRPC + Protocol Buffers (for comparison, not recommended).

This exists so the benchmark can fairly measure protobuf against Arrow. It offers two
encodings of the *same* DuckDB result:

* ``row``       — generic ``oneof`` cells; the honest equivalent of a flexible JSON row
                  API. Row-oriented: every value is encoded/decoded individually.
* ``columnar``  — packed scalar columns; "protobuf done right". Much faster than ``row``,
                  but at this point you are hand-rolling a worse Arrow.

Neither is recommended over Arrow for this workload — see docs/PERFORMANCE_REPORT.md.
"""

from __future__ import annotations

import argparse
import json
import os
from concurrent import futures

import grpc
import pyarrow as pa

from ...core import engine
from ...core.contract import QueryRequest
from .proto import data_service_pb2 as pb
from .proto import data_service_pb2_grpc as pbg

_MAX_MSG = 512 * 1024 * 1024  # 512 MB — gRPC default is 4 MB, far too small for batches


def _ptype(arrow_type: pa.DataType) -> str:
    if pa.types.is_floating(arrow_type):
        return "double"
    if pa.types.is_integer(arrow_type):
        return "int64"
    if pa.types.is_boolean(arrow_type):
        return "bool"
    return "string"  # string, date, timestamp, ... -> stringified (as JSON would)


def _row_batch(batch: pa.RecordBatch, ptypes: list[str]) -> pb.RowBatch:
    rb = pb.RowBatch()
    col_lists = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
    rows = rb.rows
    for r in range(batch.num_rows):
        cells = rows.add().cells
        for c, pt in enumerate(ptypes):
            v = col_lists[c][r]
            cell = cells.add()
            if v is None:
                continue
            if pt == "double":
                cell.d = v
            elif pt == "int64":
                cell.i = int(v)
            elif pt == "bool":
                cell.b = v
            else:
                cell.s = v if isinstance(v, str) else str(v)
    return rb


def _col_batch(batch: pa.RecordBatch, ptypes: list[str]) -> pb.ColumnarBatch:
    cb = pb.ColumnarBatch(num_rows=batch.num_rows)
    for c, pt in enumerate(ptypes):
        col = cb.columns.add()
        values = batch.column(c).to_pylist()
        if pt == "double":
            col.doubles.extend([0.0 if v is None else v for v in values])
        elif pt == "int64":
            col.ints.extend([0 if v is None else int(v) for v in values])
        elif pt == "bool":
            col.bools.extend([False if v is None else v for v in values])
        else:
            col.strings.extend(["" if v is None else (v if isinstance(v, str) else str(v)) for v in values])
    return cb


class DataService(pbg.DataServiceServicer):
    def Query(self, request, context):
        try:
            req = QueryRequest.from_dict(json.loads(request.request_json))
            reader = engine.execute_reader(req)
        except (ValueError, KeyError) as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            return

        schema = reader.schema
        ptypes = [_ptype(f.type) for f in schema]
        yield pb.QueryReply(schema=pb.Schema(
            fields=[pb.Field(name=f.name, type=pt) for f, pt in zip(schema, ptypes)]))

        columnar = request.encoding == "columnar"
        for batch in reader:
            if columnar:
                yield pb.QueryReply(col_batch=_col_batch(batch, ptypes))
            else:
                yield pb.QueryReply(row_batch=_row_batch(batch, ptypes))


def make_server(host: str, port: int, workers: int = 8) -> grpc.Server:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=workers),
        options=[
            ("grpc.max_send_message_length", _MAX_MSG),
            ("grpc.max_receive_message_length", _MAX_MSG),
        ],
    )
    pbg.add_DataServiceServicer_to_server(DataService(), server)
    server.add_insecure_port(f"{host}:{port}")
    return server


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Run the gRPC + Protobuf data server (baseline).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8816)
    p.add_argument("--threads", type=int, default=0, help="DuckDB threads (0 = all cores).")
    args = p.parse_args(argv)

    if args.threads:
        os.environ["KOB_DUCKDB_THREADS"] = str(args.threads)
    server = make_server(args.host, args.port)
    server.start()
    print(f"gRPC/Protobuf server listening on {args.host}:{args.port}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
