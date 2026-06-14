"""kob server — the product: Arrow Flight (fast data path) + a Swagger control plane.

``kob`` (see :mod:`kob.server.app`) starts both at once:

* :mod:`kob.server.flight` — Apache Arrow **Flight** over gRPC. The default, fastest
  transport; the Flight wire payload *is* the Arrow columnar layout (nothing to decode).
* :mod:`kob.server.api`    — a small FastAPI app serving **Swagger UI**: dataset
  discovery, an interactive ``/query`` (JSON/CSV) for exploration, and a documented
  ``/flight`` page describing how to pull data over Flight.
"""
