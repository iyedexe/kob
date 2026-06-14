"""kob — Apache Arrow **Flight** server (gRPC + Arrow IPC). The primary, fastest transport.

A Flight client discovers datasets with ``list_flights`` / ``get_flight_info`` and pulls
Arrow record batches with ``do_get``. The Flight wire payload *is* the Arrow columnar
layout, so there is no serialize/deserialize step — the server streams the same batches
DuckDB produces straight onto gRPC.

The :class:`~kob.core.contract.QueryRequest` JSON is carried as the Flight *command*
(in the descriptor) and echoed back as the *ticket*, so a client can either call
``get_flight_info`` first or build a ticket directly.

This module is started for you by the primary ``kob`` server (:mod:`kob.server.app`),
alongside the Swagger control plane. ``python -m kob.server.flight`` runs Flight on its
own (no HTTP) — handy for benchmarking the raw transport.
"""

from __future__ import annotations

import argparse
import json
import os

import pyarrow.flight as flight

from ..core import engine
from ..core.catalog import list_datasets
from ..core.contract import QueryRequest


class ParquetFlightServer(flight.FlightServerBase):
    def __init__(self, location: str, batch_rows: int = engine.DEFAULT_BATCH_ROWS, **kwargs) -> None:
        super().__init__(location, **kwargs)
        self._location = location
        self._batch_rows = batch_rows

    # -- helpers ---------------------------------------------------------- #
    @staticmethod
    def _request_from_descriptor(descriptor: flight.FlightDescriptor) -> QueryRequest:
        if descriptor.descriptor_type == flight.DescriptorType.CMD:
            return QueryRequest.from_dict(json.loads(descriptor.command.decode()))
        if descriptor.descriptor_type == flight.DescriptorType.PATH and descriptor.path:
            return QueryRequest(dataset=descriptor.path[0].decode())
        raise flight.FlightError("FlightDescriptor must carry a command or a path")

    def _flight_info(self, req: QueryRequest, descriptor: flight.FlightDescriptor) -> flight.FlightInfo:
        schema = engine.fetch_schema(req)
        ticket = flight.Ticket(json.dumps(req.to_dict()).encode())
        endpoint = flight.FlightEndpoint(ticket, [self._location])
        # total_records / total_bytes unknown (-1) — avoids an extra count(*) scan.
        return flight.FlightInfo(schema, descriptor, [endpoint], -1, -1)

    # -- Flight RPCs ------------------------------------------------------ #
    def list_flights(self, context, criteria):
        for ds in list_datasets():
            req = QueryRequest(dataset=ds.name)
            descriptor = flight.FlightDescriptor.for_command(json.dumps(req.to_dict()).encode())
            yield self._flight_info(req, descriptor)

    def get_flight_info(self, context, descriptor):
        req = self._request_from_descriptor(descriptor)
        try:
            return self._flight_info(req, descriptor)
        except (ValueError, KeyError) as exc:
            raise flight.FlightServerError(str(exc)) from None

    def do_get(self, context, ticket):
        try:
            req = QueryRequest.from_dict(json.loads(ticket.ticket.decode()))
            reader = engine.execute_reader(req, self._batch_rows)
        except (ValueError, KeyError) as exc:
            raise flight.FlightServerError(str(exc)) from None
        return flight.RecordBatchStream(reader)


def build_server(host: str, port: int,
                 batch_rows: int = engine.DEFAULT_BATCH_ROWS) -> tuple[ParquetFlightServer, str]:
    """Construct (but don't serve) the Flight server. Returns ``(server, location)``.

    The primary ``kob`` app calls this and runs ``server.serve()`` on a background thread.
    """
    location = f"grpc://{host}:{port}"
    return ParquetFlightServer(location, batch_rows), location


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="kob — Arrow Flight data server (the fast path, run standalone).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8815)
    p.add_argument("--threads", type=int, default=0, help="DuckDB threads (0 = all cores).")
    args = p.parse_args(argv)

    if args.threads:
        os.environ["KOB_DUCKDB_THREADS"] = str(args.threads)
    server, location = build_server(args.host, args.port)
    print(f"kob Flight server listening on {location}")
    server.serve()


if __name__ == "__main__":
    main()
