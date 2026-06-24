"""Data-quality gate.

Every event passes through here before any processing. Failures are collected
(not raised on first error) so a record can report *all* its problems, and bad
records are quarantined rather than dropped. Mirrors how enterprise ETL handles
dirty data.
"""
from __future__ import annotations

from datetime import datetime, timezone

from domain import CITIES

VALID_STATUSES = {"SUCCESS", "FAILED", "PENDING"}


def validate(evt: dict) -> list[str]:
    """Return a list of failed-check names. Empty list == record is clean."""
    failed: list[str] = []

    # transaction_id present
    if not evt.get("transaction_id"):
        failed.append("MISSING_TRANSACTION_ID")

    # user_id present
    if not evt.get("user_id"):
        failed.append("MISSING_USER_ID")

    # amount: present, numeric, non-null, positive
    amount = evt.get("amount")
    if amount is None:
        failed.append("NULL_AMOUNT")
    else:
        try:
            if float(amount) <= 0:
                failed.append("NON_POSITIVE_AMOUNT")
        except (TypeError, ValueError):
            failed.append("NON_NUMERIC_AMOUNT")

    # merchant present
    if not evt.get("merchant"):
        failed.append("MISSING_MERCHANT")

    # upi handle looks like name@bank
    handle = evt.get("upi_handle") or ""
    if "@" not in handle or len(handle) < 4:
        failed.append("INVALID_UPI_HANDLE")

    # status in allowed set
    if evt.get("status") not in VALID_STATUSES:
        failed.append("INVALID_STATUS")

    # city recognised
    if evt.get("city") not in CITIES:
        failed.append("INVALID_CITY")

    # timestamp parseable and not in the future
    ts = _parse_time(evt.get("event_time"))
    if ts is None:
        failed.append("INVALID_TIMESTAMP")
    elif ts > datetime.now(timezone.utc):
        failed.append("FUTURE_TIMESTAMP")

    return failed


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except (TypeError, ValueError):
        return None


def parse_time(value) -> datetime | None:
    """Public wrapper used by the processing pipeline."""
    return _parse_time(value)
