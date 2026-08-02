"""Vajra — free-tier live demo.

Single process: generates a UPI transaction every ~2s, runs it through the
same validate -> feature-engineer -> 5-rule fraud engine as the full platform,
persists to Postgres, and serves a small auto-refreshing dashboard.

This trades Kafka + Redis for an in-process loop + in-memory feature state —
correct for one instance, not the horizontally-scaled design. The full
enterprise architecture (Kafka partitioning, Redis-shared state, Airflow,
Spark, Prometheus/Grafana, Metabase) lives in the main repo's docker-compose.
"""
from __future__ import annotations

import asyncio
import contextlib
import random
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

import db
from pipeline import FeatureEngine, UserProfile, evaluate, generate, validate

NUM_USERS = 40
FRAUD_RATE = 0.10
TICK_SECONDS = 2.0
TRIM_EVERY_N_TICKS = 200

engine = FeatureEngine()
users = [UserProfile(f"U{100+i}") for i in range(NUM_USERS)]
counters = {"processed": 0, "rejected": 0, "flagged": 0}
_tick = 0
_bg_task: asyncio.Task | None = None


async def _generator_loop():
    global _tick
    conn = db.get_conn()
    conn.autocommit = True
    try:
        while True:
            user = random.choice(users)
            now = datetime.now(timezone.utc)
            events = generate(user, now, FRAUD_RATE)

            for evt in events:
                db.insert_raw(conn, evt)
                failed = validate(evt)
                if failed:
                    db.insert_rejected(conn, evt, failed)
                    counters["rejected"] += 1
                    continue

                event_time = evt["event_time"]
                feats = engine.compute(evt, event_time)
                findings = evaluate(evt, feats, engine.recent_statuses(evt["user_id"]))
                flagged = bool(findings)

                db.insert_processed(conn, evt, feats, event_time, flagged)
                counters["processed"] += 1

                if findings:
                    counters["flagged"] += 1
                    rows = [(evt["transaction_id"], evt["user_id"], f["rule"], f["severity"],
                            f["reason"], float(evt["amount"]), evt.get("city"), event_time)
                           for f in findings]
                    db.insert_fraud_events(conn, rows)

            _tick += 1
            if _tick % TRIM_EVERY_N_TICKS == 0:
                for t in ("raw_transactions", "processed_transactions",
                         "fraud_events", "rejected_transactions"):
                    db.trim_table(conn, t)

            await asyncio.sleep(TICK_SECONDS)
    except asyncio.CancelledError:
        pass
    finally:
        conn.close()


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global _bg_task
    db.init_schema()
    _bg_task = asyncio.create_task(_generator_loop())
    yield
    _bg_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await _bg_task


app = FastAPI(title="Vajra Demo", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/stats")
def api_stats():
    return JSONResponse(db.stats())


PAGE = """<!doctype html>
<html><head>
<meta charset="utf-8"><title>Vajra — Live Fraud Detection Demo</title>
<meta http-equiv="refresh" content="8">
<style>
  body {{ font-family: -apple-system, Segoe UI, sans-serif; background:#0b0f14; color:#e6edf3; margin:0; padding:24px; }}
  h1 {{ font-size:20px; margin-bottom:4px; }}
  .sub {{ color:#8b949e; font-size:13px; margin-bottom:20px; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-bottom:24px; }}
  .card {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:14px; }}
  .card .n {{ font-size:26px; font-weight:700; }}
  .card .l {{ font-size:12px; color:#8b949e; margin-top:4px; }}
  table {{ width:100%; border-collapse:collapse; background:#161b22; border:1px solid #30363d; border-radius:8px; overflow:hidden; margin-bottom:20px; }}
  th, td {{ text-align:left; padding:8px 12px; font-size:13px; border-bottom:1px solid #30363d; }}
  th {{ color:#8b949e; font-weight:600; }}
  .sev-CRITICAL {{ color:#f85149; }} .sev-HIGH {{ color:#ff9800; }} .sev-MEDIUM {{ color:#d29922; }}
  a {{ color:#58a6ff; }}
  footer {{ color:#8b949e; font-size:12px; margin-top:24px; }}
</style>
</head><body>
<h1>Vajra — Real-Time UPI Fraud Detection</h1>
<div class="sub">Live demo (auto-refreshes every 8s) · single-instance trimmed build ·
<a href="https://github.com/YOUR_USERNAME/vajra" target="_blank">full architecture on GitHub</a></div>

<div class="grid">
  <div class="card"><div class="n">{total}</div><div class="l">Total Transactions</div></div>
  <div class="card"><div class="n">{success_rate}%</div><div class="l">Success Rate</div></div>
  <div class="card"><div class="n">{flagged}</div><div class="l">Fraud Alerts</div></div>
  <div class="card"><div class="n">{rejected}</div><div class="l">Rejected (DQ)</div></div>
  <div class="card"><div class="n">Rs.{avg_amount}</div><div class="l">Avg Payment</div></div>
</div>

<table>
<tr><th>Rule</th><th>Severity</th><th>Events</th></tr>
{rule_rows}
</table>

<table>
<tr><th>Time</th><th>Rule</th><th>Severity</th><th>User</th><th>Amount</th><th>Reason</th></tr>
{recent_rows}
</table>

<footer>
This demo runs the same validation, feature-engineering, and 5-rule fraud engine
as the full platform, minus Kafka/Redis/Airflow/Spark (single-instance, no
horizontal scaling here). See the GitHub repo for the complete architecture.
</footer>
</body></html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    data = db.stats()
    s = data["summary"]
    total = s["total"] or 0
    success_rate = round(100 * (s["success"] or 0) / total, 1) if total else 0.0

    rule_rows = "".join(
        f'<tr><td>{r["rule_triggered"]}</td>'
        f'<td class="sev-{r["severity"]}">{r["severity"]}</td>'
        f'<td>{r["events"]}</td></tr>'
        for r in data["by_rule"]
    ) or "<tr><td colspan=3>No fraud alerts yet — check back in a few seconds.</td></tr>"

    recent_rows = "".join(
        f'<tr><td>{r["detected_at"].strftime("%H:%M:%S")}</td>'
        f'<td>{r["rule_triggered"]}</td>'
        f'<td class="sev-{r["severity"]}">{r["severity"]}</td>'
        f'<td>{r["user_id"]}</td><td>Rs.{r["amount"]}</td><td>{r["reason"]}</td></tr>'
        for r in data["recent"]
    ) or "<tr><td colspan=6>No alerts yet.</td></tr>"

    html = PAGE.format(
        total=total, success_rate=success_rate, flagged=s["flagged"] or 0,
        rejected=s["rejected"] or 0, avg_amount=s["avg_amount"] or 0,
        rule_rows=rule_rows, recent_rows=recent_rows,
    )
    return HTMLResponse(html)
