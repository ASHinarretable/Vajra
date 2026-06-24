"""Metabase provisioning script — idempotent, re-runnable.

Creates (or reconnects to) an existing Metabase instance and provisions:
  - Admin account
  - Vajra Data source (Postgres connection)
  - Dashboard 1: Executive  — transaction KPIs
  - Dashboard 2: Operations — fraud monitoring
  - Dashboard 3: Risk       — Spark-computed customer + merchant baselines

Run from repo root after `docker compose up`:
    python dashboard/provision_metabase.py

If Metabase already has an admin (e.g. you're re-running), set env vars:
    MB_EMAIL=your@email.com MB_PASSWORD=yourpass python dashboard/provision_metabase.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# Config — override via env vars for non-default deployments.
# ---------------------------------------------------------------------------
BASE = os.getenv("MB_URL", "http://localhost:3000")
EMAIL = os.getenv("MB_EMAIL", "aishwaryapawar3082002@gmail.com")
PASSWORD = os.getenv("MB_PASSWORD", "Vajra@2026")

PG_HOST = os.getenv("MB_PG_HOST", "postgres")          # inside docker network
PG_PORT = int(os.getenv("MB_PG_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "vajra")
PG_USER = os.getenv("POSTGRES_USER", "vajra")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "vajra")

DB_NAME = "Vajra Data"

# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
def http(method: str, path: str, body=None, token: str | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("X-Metabase-Session", token)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()[:400]
        # 409 on user-create = already exists; caller handles it.
        if e.code == 409:
            raise
        print(f"  HTTP {e.code} {method} {path}: {body_txt}", file=sys.stderr)
        raise


def wait_healthy(retries: int = 30, delay: float = 5.0) -> None:
    print(f"Waiting for Metabase at {BASE} ...")
    for i in range(retries):
        try:
            with urllib.request.urlopen(BASE + "/api/health", timeout=5) as r:
                d = json.loads(r.read())
                if d.get("status") == "ok":
                    print("  Metabase is ready.")
                    return
        except Exception:
            pass
        print(f"  ({i+1}/{retries}) not ready yet...")
        time.sleep(delay)
    raise RuntimeError("Metabase did not become healthy in time.")


# ---------------------------------------------------------------------------
# Auth — setup (first run) or login (subsequent runs)
# ---------------------------------------------------------------------------
def get_session() -> str:
    props = http("GET", "/api/session/properties")
    setup_token = props.get("setup-token")
    has_user = props.get("has-user-setup", True)

    if setup_token and not has_user:
        print("First-time setup: creating admin account...")
        resp = http("POST", "/api/setup", {
            "token": setup_token,
            "user": {
                "first_name": "Vajra", "last_name": "Admin",
                "email": EMAIL, "password": PASSWORD, "site_name": "Vajra",
            },
            "prefs": {"site_name": "Vajra", "site_locale": "en", "allow_tracking": False},
        })
        session = resp["id"] if isinstance(resp, dict) else resp
        print("  Admin created.")
        return session

    print("Logging in as existing admin...")
    resp = http("POST", "/api/session", {"username": EMAIL, "password": PASSWORD})
    return resp["id"]


# ---------------------------------------------------------------------------
# Database — add if not present, return ID
# ---------------------------------------------------------------------------
def ensure_database(session: str) -> int:
    dbs = http("GET", "/api/database", token=session)
    db_list = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
    for db in db_list:
        if db["name"] == DB_NAME:
            print(f"Database '{DB_NAME}' already exists (id={db['id']}), reusing.")
            return db["id"]

    print(f"Adding database '{DB_NAME}'...")
    resp = http("POST", "/api/database", {
        "engine": "postgres",
        "name": DB_NAME,
        "details": {
            "host": PG_HOST, "port": PG_PORT, "dbname": PG_DB,
            "user": PG_USER, "password": PG_PASS,
            "ssl": False, "tunnel-enabled": False,
        },
        "is_full_sync": True,
    }, session)
    db_id = resp["id"]
    http("POST", f"/api/database/{db_id}/sync_schema", {}, session)
    time.sleep(3)
    print(f"  Database added (id={db_id}).")
    return db_id


# ---------------------------------------------------------------------------
# Cards + dashboards
# ---------------------------------------------------------------------------
def card(session: str, db_id: int, name: str, sql: str,
         display: str, viz: dict | None = None) -> int:
    resp = http("POST", "/api/card", {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql, "template-tags": {}},
            "database": db_id,
        },
        "display": display,
        "visualization_settings": viz or {},
    }, session)
    print(f"  card: {name}")
    return resp["id"]


def dashboard(session: str, name: str, desc: str, dashcards: list) -> int:
    resp = http("POST", "/api/dashboard", {"name": name, "description": desc}, session)
    did = resp["id"]
    http("PUT", f"/api/dashboard/{did}", {"dashcards": dashcards}, session)
    print(f"DASHBOARD: {name} (id={did})")
    return did


def dc(neg_id: int, card_id: int, row: int, col: int, sx: int, sy: int) -> dict:
    return {
        "id": neg_id, "card_id": card_id,
        "row": row, "col": col, "size_x": sx, "size_y": sy,
        "series": [], "parameter_mappings": [], "visualization_settings": {},
    }


# ---------------------------------------------------------------------------
# Remove demo clutter Metabase ships with on first boot
# ---------------------------------------------------------------------------
def remove_demo_content(session: str) -> None:
    # Dashboards not belonging to Vajra
    dl = http("GET", "/api/dashboard", token=session)
    dashes = dl.get("data", dl) if isinstance(dl, dict) else dl
    for d in dashes:
        if not d["name"].startswith("Vajra"):
            http("DELETE", f"/api/dashboard/{d['id']}", token=session)
            print(f"  removed demo dashboard: {d['name']}")

    # Databases not belonging to Vajra
    dbs = http("GET", "/api/database", token=session)
    db_list = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
    for db in db_list:
        if db["name"] != DB_NAME:
            http("DELETE", f"/api/database/{db['id']}", token=session)
            print(f"  removed demo database: {db['name']}")


# ---------------------------------------------------------------------------
# Dashboard definitions
# ---------------------------------------------------------------------------
def build_executive(session: str, db_id: int) -> int:
    c = lambda n, sql, disp, viz=None: card(session, db_id, n, sql, disp, viz)  # noqa: E731
    print("\n-- Executive Dashboard --")
    total    = c("Total Transactions", "SELECT count(*) FROM processed_transactions", "scalar")
    succ_rt  = c("Success Rate %", "SELECT round(100.0*count(*) FILTER (WHERE status='SUCCESS')/NULLIF(count(*),0),1) FROM processed_transactions", "scalar")
    failed   = c("Failed Transactions", "SELECT count(*) FROM processed_transactions WHERE status='FAILED'", "scalar")
    flagged  = c("Flagged Transactions", "SELECT count(*) FROM processed_transactions WHERE is_flagged", "scalar")
    avg_pay  = c("Avg Payment (Rs)", "SELECT round(avg(amount),0) FROM processed_transactions", "scalar")
    status_p = c("Transaction Status", "SELECT status, count(*) AS count FROM processed_transactions GROUP BY status", "pie",
                 {"pie.dimension": "status", "pie.metric": "count"})
    hourly   = c("Hourly Volume", "SELECT date_trunc('hour', event_time) AS hour, count(*) AS transactions FROM processed_transactions GROUP BY 1 ORDER BY 1", "line",
                 {"graph.dimensions": ["hour"], "graph.metrics": ["transactions"]})
    merch    = c("Top Merchants", "SELECT merchant, count(*) AS transactions FROM processed_transactions GROUP BY merchant ORDER BY transactions DESC LIMIT 10", "row",
                 {"graph.dimensions": ["merchant"], "graph.metrics": ["transactions"]})
    city     = c("Transactions by City", "SELECT city, count(*) AS transactions FROM processed_transactions GROUP BY city ORDER BY transactions DESC", "bar",
                 {"graph.dimensions": ["city"], "graph.metrics": ["transactions"]})

    return dashboard(session, "Vajra - Executive Dashboard", "High-level UPI transaction KPIs", [
        dc(-1, total,   0,  0, 5, 3), dc(-2, succ_rt, 0,  5, 5, 3),
        dc(-3, failed,  0, 10, 5, 3), dc(-4, flagged,  0, 15, 5, 3),
        dc(-5, avg_pay, 0, 20, 4, 3),
        dc(-6, status_p, 3,  0,  8, 6), dc(-7, hourly, 3,  8, 16, 6),
        dc(-8, merch,   9,  0, 12, 6), dc(-9, city,    9, 12, 12, 6),
    ])


def build_operations(session: str, db_id: int) -> int:
    c = lambda n, sql, disp, viz=None: card(session, db_id, n, sql, disp, viz)  # noqa: E731
    print("\n-- Operations Dashboard --")
    total   = c("Total Fraud Events", "SELECT count(*) FROM fraud_events", "scalar")
    crit    = c("Critical Alerts", "SELECT count(*) FROM fraud_events WHERE severity='CRITICAL'", "scalar")
    risk    = c("Amount at Risk (Rs)", "SELECT round(sum(amount),0) FROM fraud_events", "scalar")
    by_rule = c("Fraud by Rule", "SELECT rule_triggered, count(*) AS events FROM fraud_events GROUP BY 1 ORDER BY events DESC", "bar",
                {"graph.dimensions": ["rule_triggered"], "graph.metrics": ["events"]})
    by_sev  = c("Fraud by Severity", "SELECT severity, count(*) AS events FROM fraud_events GROUP BY 1 ORDER BY events DESC", "pie",
                {"pie.dimension": "severity", "pie.metric": "events"})
    trend   = c("Fraud Trend (hourly)", "SELECT date_trunc('hour', detected_at) AS hour, count(*) AS events FROM fraud_events GROUP BY 1 ORDER BY 1", "line",
                {"graph.dimensions": ["hour"], "graph.metrics": ["events"]})
    amt_rl  = c("Amount at Risk by Rule", "SELECT rule_triggered, round(sum(amount),0) AS amount_at_risk FROM fraud_events GROUP BY 1 ORDER BY amount_at_risk DESC", "row",
                {"graph.dimensions": ["rule_triggered"], "graph.metrics": ["amount_at_risk"]})
    recent  = c("Recent Fraud Alerts", "SELECT detected_at, severity, rule_triggered, user_id, amount, reason FROM fraud_events ORDER BY detected_at DESC LIMIT 20", "table")

    return dashboard(session, "Vajra - Operations Dashboard", "Fraud detection monitoring & alerts", [
        dc(-1, total,   0,  0,  8, 3), dc(-2, crit,  0,  8,  8, 3), dc(-3, risk,   0, 16,  8, 3),
        dc(-4, by_rule, 3,  0, 12, 6), dc(-5, by_sev, 3, 12, 12, 6),
        dc(-6, trend,   9,  0, 24, 6),
        dc(-7, amt_rl, 15,  0, 10, 7), dc(-8, recent, 15, 10, 14, 7),
    ])


def build_risk(session: str, db_id: int) -> int:
    """Dashboard backed by Spark batch tables (run `docker compose run --rm spark` first)."""
    c = lambda n, sql, disp, viz=None: card(session, db_id, n, sql, disp, viz)  # noqa: E731
    print("\n-- Risk Analytics Dashboard (Spark tables) --")
    cust_total = c("Users Profiled", "SELECT count(*) FROM customer_profile", "scalar")
    high_risk  = c("High-Risk Merchants", "SELECT count(*) FROM merchant_risk WHERE risk_tier='HIGH'", "scalar")
    avg_fraud  = c("Avg Customer Fraud Rate %", "SELECT round(avg(fraud_rate_pct)::numeric, 1) FROM customer_profile", "scalar")

    top_cust   = c("Top 10 Highest Fraud-Rate Users",
        "SELECT user_id, txn_count, avg_amount, fraud_txns, fraud_rate_pct FROM customer_profile ORDER BY fraud_rate_pct DESC LIMIT 10",
        "table")
    merch_risk = c("Merchant Risk Tiers",
        "SELECT risk_tier, count(*) AS merchants FROM merchant_risk GROUP BY 1 ORDER BY merchants DESC",
        "pie", {"pie.dimension": "risk_tier", "pie.metric": "merchants"})
    merch_top  = c("Top Merchants by Fraud Rate",
        "SELECT merchant, merchant_category, txn_count, fraud_rate_pct, risk_tier FROM merchant_risk ORDER BY fraud_rate_pct DESC LIMIT 15",
        "table")
    spend_dist = c("Customer Avg Spend Distribution",
        "SELECT CASE WHEN avg_amount < 500 THEN '<500' WHEN avg_amount < 1000 THEN '500-1000' WHEN avg_amount < 2000 THEN '1000-2000' ELSE '2000+' END AS spend_band, count(*) AS users FROM customer_profile GROUP BY 1 ORDER BY 1",
        "bar", {"graph.dimensions": ["spend_band"], "graph.metrics": ["users"]})
    travel     = c("Users with High City Spread (5+ cities)",
        "SELECT user_id, distinct_cities, distinct_devices, fraud_rate_pct FROM customer_profile WHERE distinct_cities >= 5 ORDER BY distinct_cities DESC",
        "table")

    return dashboard(session, "Vajra - Risk Analytics", "Spark-computed customer & merchant baselines", [
        dc(-1, cust_total, 0,  0,  8, 3), dc(-2, high_risk, 0,  8,  8, 3), dc(-3, avg_fraud, 0, 16,  8, 3),
        dc(-4, top_cust,   3,  0, 14, 8), dc(-5, merch_risk, 3, 14, 10, 8),
        dc(-6, merch_top,  11, 0, 14, 8), dc(-7, spend_dist, 11,14, 10, 8),
        dc(-8, travel,     19, 0, 24, 6),
    ])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    wait_healthy()
    session = get_session()
    db_id = ensure_database(session)
    remove_demo_content(session)
    build_executive(session, db_id)
    build_operations(session, db_id)
    build_risk(session, db_id)

    print(f"\nDone. Open {BASE}  (login: {EMAIL} / {PASSWORD})")


if __name__ == "__main__":
    main()
