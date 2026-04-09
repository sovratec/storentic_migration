"""
rental_agreement_transformer.py — Field-level transformation helpers for rental_agreements ETL.

Provides pure functions only — no DB calls, no I/O, no side effects.
All helpers are consumed by migrate_rental_agreements.py.

Functions:
    parse_date(value)                         → date | None
    to_cents(value, fallback)                 → int
    derive_charge_day(move_in_date)           → int
    derive_insurance_option(rate_in_cents)    → str
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


# ── to_cents ───────────────────────────────────────────────────────────────────

def to_cents(value, fallback: int = 0) -> int:
    """
    Convert a decimal dollar amount to integer cents (BIGINT storage).
    Strips leading $ signs and commas before parsing.
    Returns fallback for blank, NaN, or non-numeric inputs.

    Examples:
        225.00    → 22500
        "$225"    → 22500
        "1,250"   → 125000
        None      → 0  (fallback)
        ""        → 0  (fallback)
    """
    if value is None:
        return fallback
    s = str(value).strip().lstrip("$").replace(",", "")
    if s.lower() in ("nan", "none", ""):
        return fallback
    try:
        return int(round(float(s) * 100))
    except ValueError:
        return fallback


# ── derive_charge_day ──────────────────────────────────────────────────────────

def derive_charge_day(move_in_date: date) -> int:
    """
    Extract the day-of-month from move_in_date to use as the monthly charge day.
    Clamps to 30 (DB column max: charge_day BETWEEN 1 AND 30).

    Examples:
        date(2012, 7, 27) → 27
        date(2022, 1, 31) → 30  (clamped)
    """
    return min(move_in_date.day, 30)


# ── derive_insurance_option ────────────────────────────────────────────────────

def derive_insurance_option(insurance_rate_in_cents: int) -> str:
    """
    Determine insurance_option enum value from the premium amount in cents.
    Returns 'BASIC' if the premium is greater than 0, otherwise 'NONE'.

    Examples:
        500  → "BASIC"
        0    → "NONE"
    """
    return "BASIC" if insurance_rate_in_cents > 0 else "NONE"
