"""
customer_unit_transformer.py — Field-level transformation helpers for customer_unit ETL.

Provides pure functions only — no DB calls, no I/O, no side effects.
All helpers are consumed by migrate_customer_unit.py.

Functions:
    parse_date(value)                  → date | None
    safe_decimal(value, fallback)      → float
    trim_access_code(value)            → str | None
"""

import pandas as pd
from datetime import date


# ── parse_date ─────────────────────────────────────────────────────────────────

def parse_date(value) -> date | None:
    """
    Safely parse a pandas/Excel date cell to a Python date.
    Returns None for blank, NaT, or unparseable values.

    Examples:
        "2018-04-15"  → date(2018, 4, 15)
        pd.NaT        → None
        None          → None
        "not a date"  → None  (caller logs the error)
    """
    if value is None:
        return None
    try:
        parsed = pd.to_datetime(value)
        if pd.isnull(parsed):
            return None
        return parsed.date()
    except (ValueError, TypeError):
        return None


# ── safe_decimal ───────────────────────────────────────────────────────────────

def safe_decimal(value, fallback: float = 0.0) -> float:
    """
    Safely convert an Excel cell to a float (stored as DECIMAL(12,2) in DB).
    Strips leading $ signs and commas before parsing.
    Returns fallback for blank, NaN, or non-numeric inputs.

    Examples:
        225.00    → 225.0
        "$225"    → 225.0
        "1,250"   → 1250.0
        None      → 0.0  (fallback)
        ""        → 0.0  (fallback)
    """
    if value is None:
        return fallback
    s = str(value).strip().lstrip("$").replace(",", "")
    if s.lower() in ("nan", "none", ""):
        return fallback
    try:
        return float(s)
    except ValueError:
        return fallback


# ── trim_access_code ───────────────────────────────────────────────────────────

def trim_access_code(value) -> str | None:
    """
    Clean and truncate sAccessCode to the 10-character DB column limit.
    Returns None for blank or NaN values.

    Examples:
        "ABC123"       → "ABC123"
        "12345678901"  → "1234567890"  (truncated at 10)
        None           → None
        "nan"          → None
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in ("nan", "none", ""):
        return None
    return s[:10]
