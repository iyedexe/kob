"""Lightweight, dependency-free timing + metrics for kob.

Answers the operational question "is *discovery* slow, or is the *query* slow?"
without pulling in Prometheus or any extra dependency. Every timed section
records into an in-process registry (count / avg / p50 / p95 / max, plus a
rolling list of the slowest recent ops) and emits a console log line, so you
get three views of the same data:

* ``GET /metrics``   — the registry as JSON
* ``GET /dashboard`` — a small self-contained HTML page that polls ``/metrics``
* server logs        — one line per timed op (WARNING once it crosses SLOW_MS)

Timer names are namespaced ``discover.*`` vs ``query.*`` so the dashboard can
split them and you can see at a glance where the time goes.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field

log = logging.getLogger("kob")

_RECENT = 256  # per-timer ring buffer for percentiles
SLOW_MS = float(os.environ.get("KOB_SLOW_MS", "250"))  # ops >= this are logged loud + flagged


@dataclass
class _Timer:
    count: int = 0
    total_ms: float = 0.0
    min_ms: float = float("inf")
    max_ms: float = 0.0
    recent: deque[float] = field(default_factory=lambda: deque(maxlen=_RECENT))

    def add(self, ms: float) -> None:
        self.count += 1
        self.total_ms += ms
        self.min_ms = min(self.min_ms, ms)
        self.max_ms = max(self.max_ms, ms)
        self.recent.append(ms)

    def stats(self) -> dict:
        if not self.count:
            return {"count": 0}
        s = sorted(self.recent)
        pct = lambda p: s[min(len(s) - 1, int(p * len(s)))]  # noqa: E731
        return {
            "count": self.count,
            "avg_ms": round(self.total_ms / self.count, 2),
            "p50_ms": round(pct(0.50), 2),
            "p95_ms": round(pct(0.95), 2),
            "max_ms": round(self.max_ms, 2),
        }


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._timers: dict[str, _Timer] = {}
        self._counters: dict[str, int] = {}
        self._slow: deque[dict] = deque(maxlen=25)
        self.started = time.time()

    def record(self, name: str, ms: float, detail: str | None = None) -> None:
        with self._lock:
            self._timers.setdefault(name, _Timer()).add(ms)
            if ms >= SLOW_MS:
                self._slow.appendleft(
                    {"op": name, "ms": round(ms, 1), "detail": detail, "at": round(time.time())}
                )

    def incr(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + n

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "uptime_s": round(time.time() - self.started, 1),
                "slow_threshold_ms": SLOW_MS,
                "counters": dict(self._counters),
                "timers": {k: v.stats() for k, v in sorted(self._timers.items())},
                "recent_slow": list(self._slow),
            }

    def reset(self) -> None:  # handy for tests
        with self._lock:
            self._timers.clear()
            self._counters.clear()
            self._slow.clear()
            self.started = time.time()


METRICS = Metrics()


class timed:
    """``with timed("query.execute", detail): ...`` — records + logs the duration."""

    __slots__ = ("name", "detail", "_t0")

    def __init__(self, name: str, detail: str | None = None) -> None:
        self.name = name
        self.detail = detail

    def __enter__(self) -> "timed":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *_exc) -> None:
        ms = (time.perf_counter() - self._t0) * 1000.0
        METRICS.record(self.name, ms, self.detail)
        log.log(
            logging.WARNING if ms >= SLOW_MS else logging.INFO,
            "kob %-18s %8.1f ms  %s", self.name, ms, self.detail or "",
        )


# --------------------------------------------------------------------------- #
# Self-contained dashboard (no external assets; polls /metrics every 2s)
# --------------------------------------------------------------------------- #
DASHBOARD_HTML = """<!doctype html><html><head><meta charset="utf-8">
<title>kob metrics</title><style>
:root{--pl:#3a1647;--pu:#6d2c8f;--mg:#c71e7a;--ink:#2b2230;--mut:#8a8692;--bg:#f4f1f6}
*{box-sizing:border-box}body{font:14px/1.4 Georgia,serif;margin:0;color:var(--ink);background:#fff}
header{background:var(--pl);color:#fff;padding:14px 22px}header b{color:#fff;font-size:20px}
header span{color:#c9a0dc;margin-left:10px;font-size:12px}
.wrap{padding:18px 22px;max-width:1100px}.cards{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px}
.card{background:var(--bg);border-left:4px solid var(--pu);border-radius:8px;padding:12px 16px;min-width:150px}
.card .k{font-size:11px;color:var(--mut);text-transform:uppercase}.card .v{font-size:22px;font-weight:bold;color:var(--pl)}
h2{color:var(--pl);font-size:15px;margin:20px 0 8px;border-bottom:2px solid var(--pu);padding-bottom:4px}
table{width:100%;border-collapse:collapse;font-size:13px}th{text-align:left;color:#fff;background:var(--pl);padding:6px 10px}
td{padding:5px 10px;border-bottom:1px solid #eee}tr:nth-child(even) td{background:var(--bg)}
.mono{font-family:Consolas,monospace}.slow{color:var(--mg);font-weight:bold}.muted{color:var(--mut)}
</style></head><body>
<header><b>kob</b> metrics <span id="up"></span><span id="ago" style="float:right"></span></header>
<div class="wrap">
 <div class="cards" id="cards"></div>
 <h2>Discovery — folder walks &amp; schema reads</h2><table id="discover"></table>
 <h2>Query — DuckDB execute &amp; serialization</h2><table id="query"></table>
 <h2>Other</h2><table id="other"></table>
 <h2>Recent slow ops <span class="muted" id="slowthr"></span></h2><table id="slow"></table>
</div>
<script>
const HEAD='<tr><th>op</th><th>count</th><th>avg</th><th>p50</th><th>p95</th><th>max</th></tr>';
function rows(timers, pred){let h=HEAD;for(const k of Object.keys(timers).sort()){if(!pred(k))continue;
 const t=timers[k];if(!t.count)continue;
 h+=`<tr><td class=mono>${k}</td><td>${t.count}</td><td>${t.avg_ms}</td><td>${t.p50_ms}</td><td>${t.p95_ms}</td><td>${t.max_ms}</td></tr>`;}
 return h;}
function card(k,v){return `<div class=card><div class=k>${k}</div><div class=v>${v}</div></div>`;}
async function tick(){let m;try{m=await(await fetch('metrics')).json();}catch(e){return;}
 const c=m.counters||{},t=m.timers||{};
 const hit=c['cache.hit']||0,miss=c['cache.miss']||0,tot=hit+miss;
 document.getElementById('up').textContent='uptime '+m.uptime_s+'s';
 document.getElementById('ago').textContent='updated '+new Date().toLocaleTimeString();
 document.getElementById('slowthr').textContent='(≥ '+m.slow_threshold_ms+' ms)';
 document.getElementById('cards').innerHTML=
  card('queries',(t['query.total']||{}).count||0)+
  card('query p95',((t['query.total']||{}).p95_ms??'–')+' ms')+
  card('discovery p95',((t['discover.dataset']||{}).p95_ms??(t['discover.list']||{}).p95_ms??'–')+' ms')+
  card('cache hit-rate',tot?Math.round(100*hit/tot)+'%':'–')+
  card('errors',(c['query.error']||0)+(c['discover.error']||0));
 document.getElementById('discover').innerHTML=rows(t,k=>k.startsWith('discover'));
 document.getElementById('query').innerHTML=rows(t,k=>k.startsWith('query')||k.startsWith('facets'));
 document.getElementById('other').innerHTML=rows(t,k=>!k.startsWith('discover')&&!k.startsWith('query')&&!k.startsWith('facets'));
 let s='<tr><th>op</th><th>ms</th><th>detail</th><th>when</th></tr>';
 for(const r of (m.recent_slow||[]))
  s+=`<tr><td class=mono>${r.op}</td><td class=slow>${r.ms}</td><td class=mono>${r.detail||''}</td><td class=muted>${new Date(r.at*1000).toLocaleTimeString()}</td></tr>`;
 document.getElementById('slow').innerHTML=s;}
tick();setInterval(tick,2000);
</script></body></html>"""
