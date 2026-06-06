"""
payment_transformer.py — Field-level transformation logic for SiteLink payments.

Pure functions only — no DB calls, no file I/O, no side effects.
Each function takes raw values from the source CSV/DataFrame and returns
the appropriate Storentic database value, or raises ValueError on bad input.

Source:  Payments.csv  (SiteLink export)
Target:  storentic.payments
"""

from datetime import datetime
from typing import Optional


# ── String helper ──────────────────────────────────────────────────────────────

def _clean_str(value) -> str | None:
    """Strip and return None for blank/nan values."""
    if value is None:
        return None
    s = str(value).strip()
    return None if s.lower() in ("nan", "none", "") else s


# ── Date parsing ───────────────────────────────────────────────────────────────

# SiteLink exports dates in various formats; try them in order.
_DATE_FORMATS = [
    "%m/%d/%Y %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    # Dash-separated variants (present in some SiteLink export flavours, e.g. Payments_1.csv)
    "%m-%d-%Y %H:%M:%S.%f",
    "%m-%d-%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    "%m-%d-%Y",
]


def parse_date(value) -> Optional[datetime]:
    """
    Parse a SiteLink date string into a Python datetime.
    Returns None if the value is blank, NaN, or unparseable.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in ("", "nan", "nat", "none"):
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            # Treat SiteLink sentinel dates (year < 1970) as missing
            if dt.year < 1970:
                return None
            return dt
        except ValueError:
            continue
    return None  # caller decides how to handle unparseable dates


# ── Amount ─────────────────────────────────────────────────────────────────────

def dollars_to_cents(value) -> int:
    """
    Convert a dollar amount (float/string) to integer cents.
    Returns 0 for null/blank/zero amounts.
    Raises ValueError if the value cannot be converted to a number.
    """
    if value is None:
        return 0
    s = str(value).strip()
    if s.lower() in ("", "nan", "none"):
        return 0
    try:
        dollars = float(s)
        return max(0, round(dollars * 100))
    except (ValueError, TypeError):
        raise ValueError(f"Cannot convert amount to cents: {value!r}")


# ── Payment status ─────────────────────────────────────────────────────────────

def derive_status(b_nsf, d_deleted) -> str:
    """
    Determine PaymentStatus enum string.

    Rules (in priority order):
      - bNSF = True                 → FAILED
      - dDeleted is not null/blank  → FAILED
      - Otherwise                   → COMPLETED
    """
    nsf = _is_true(b_nsf)
    deleted = _has_value(d_deleted)
    if nsf or deleted:
        return "FAILED"
    return "COMPLETED"


def derive_failure_reason(b_nsf, d_deleted) -> Optional[str]:
    """
    Build the failure_reason string when status is FAILED.

    - NSF only:      "NSF - Returned check"
    - Deleted only:  "Voided in SiteLink on {date}"
    - Both:          "NSF - Returned check; Voided in SiteLink on {date}"
    - Neither:       None
    """
    nsf = _is_true(b_nsf)
    deleted_date = _safe_date_str(d_deleted)

    parts = []
    if nsf:
        parts.append("NSF - Returned check")
    if deleted_date:
        parts.append(f"Voided in SiteLink on {deleted_date}")

    return "; ".join(parts) if parts else None


# ── Receipt / reference ────────────────────────────────────────────────────────

def derive_receipt_number(payment_id) -> str:
    """
    Build a unique receipt_number from SiteLink PaymentID.
    Format: "SL-{PaymentID}"

    ReceiptID is NOT used here because a single SiteLink receipt can contain
    multiple payments (i.e. ReceiptID is not unique per payment row).
    PaymentID is always unique in SiteLink.
    """
    return f"SL-{int(float(payment_id))}"


def derive_external_id(payment_id) -> str:
    """Store SiteLink PaymentID as the external_id for deduplication."""
    return str(int(float(payment_id)))


def derive_reference_number(s_check_num) -> Optional[str]:
    """Use SiteLink check/ACH reference number as reference_number. Null if absent."""
    if s_check_num is None:
        return None
    s = str(s_check_num).strip()
    return s if s.lower() not in ("", "nan", "none") else None


def derive_notes(ledger_id, receipt_id) -> str:
    """Build an audit note recording the SiteLink LedgerID and ReceiptID."""
    parts = ["Imported from SiteLink."]
    if ledger_id is not None and str(ledger_id).strip().lower() not in ("", "nan"):
        parts.append(f"LedgerID: {int(float(ledger_id))}.")
    if receipt_id is not None and str(receipt_id).strip().lower() not in ("", "nan"):
        parts.append(f"ReceiptID: {int(float(receipt_id))}.")
    return " ".join(parts)


# ── Payment method type ────────────────────────────────────────────────────────

def derive_payment_method_type_id(pmt_type_id) -> int:
    """
    Use SiteLink PmtTypeID directly as payment_method_type_id.
    Raises ValueError if the value is null or non-integer.

    Known SiteLink PmtTypeID values in this dataset:
      99  — Cash
      100 — Check
      101 — (to confirm)
      102 — (to confirm)
      103 — (to confirm)
      104 — ACH / eCheck  (40.6% of payments)
      105 — (to confirm)
      106 — (to confirm)
      110 — (to confirm)
      111 — (to confirm)
      114 — (to confirm)
    """
    if pmt_type_id is None or str(pmt_type_id).strip().lower() in ("", "nan"):
        raise ValueError(f"PmtTypeID is null or blank: {pmt_type_id!r}")
    try:
        return int(float(pmt_type_id))
    except (ValueError, TypeError):
        raise ValueError(f"Non-integer PmtTypeID: {pmt_type_id!r}")


# ── Full row transform ─────────────────────────────────────────────────────────

def transform_row(
    row,
    customer_id: int,
    org_id: int,
    loc_id: int,
    created_by: int,
) -> dict:
    """
    Transform one Payments.csv row into a dict ready for DB insertion.

    Args:
        row         — pandas Series (one row from Payments.csv)
        customer_id — resolved from LedgerID → TenantID → storentic.customer.id
        org_id      — from .env ORGANIZATION_ID
        loc_id      — from .env LOCATION_ID
        created_by  — from .env CREATED_BY

    Returns:
        dict of column_name → value suitable for SQLAlchemy text() INSERT.

    Raises:
        ValueError if any critical field cannot be transformed.
    """
    payment_id  = row.get("PAYMENTID")
    ledger_id   = row.get("LEDGERID")
    pmt_type_id = row.get("PMTTYPEID")
    dc_pmt_amt  = row.get("DCPMTAMT")
    d_pmt       = row.get("DPMT")
    d_created   = row.get("DCREATED")
    s_check_num = row.get("SCHECKNUM")
    b_nsf       = row.get("BNSF")
    d_deleted   = row.get("DDELETED")
    receipt_id  = row.get("RECEIPTID")

    # ── Parse dates ───────────────────────────────────────────────────────────
    payment_date = parse_date(d_pmt)
    created_date = parse_date(d_created)

    # payment_date is required — fall back to created_date, then now()
    if payment_date is None:
        payment_date = created_date
    if payment_date is None:
        raise ValueError(f"PaymentID {payment_id}: both dPmt and dCreated are unparseable")

    # created_at — prefer dCreated, fall back to dPmt
    created_at = created_date if created_date is not None else payment_date

    # ── Transform fields ──────────────────────────────────────────────────────
    return {
        "external_id"            : derive_external_id(payment_id),
        "external_payment_id"    : derive_external_id(payment_id),
        "receipt_number"         : derive_receipt_number(payment_id),
        "customer_id"            : customer_id,
        "location_id"            : loc_id,
        "organization_id"        : org_id,
        "amount_in_cents"        : dollars_to_cents(dc_pmt_amt),
        "payment_method_type_id" : derive_payment_method_type_id(pmt_type_id),
        "status"                 : derive_status(b_nsf, d_deleted),
        "payment_date"           : payment_date,
        "failure_reason"         : derive_failure_reason(b_nsf, d_deleted),
        "reference_number"       : derive_reference_number(s_check_num),
        "notes"                  : derive_notes(ledger_id, receipt_id),
        "created_by"             : created_by,
        "created_at"             : created_at,
        "updated_at"             : created_at,
        # External system fields
        "external_employee_id"      : _clean_str(row.get("EMPLOYEEID")),
        "external_system"           : "sitelink",
        # Fields not applicable to legacy imports — explicitly set to safe defaults
        "payment_profile_id"        : None,
        "stripe_payment_intent_id"  : None,
        "stripe_charge_id"          : None,
        "stripe_refund_id"          : None,
        "transaction_fee_in_cents"  : 0,
        "refunded_amount_in_cents"  : 0,
        "reversal_reason"           : None,
        "reversed_by"               : None,
        "reversed_at"               : None,
    }


# ── Private helpers ────────────────────────────────────────────────────────────

def _is_true(value) -> bool:
    """Return True if the value represents a truthy SiteLink boolean (1, '1', True, 'true', 'yes')."""
    if value is None:
        return False
    s = str(value).strip().lower()
    return s in ("1", "true", "yes", "t", "y")


def _has_value(value) -> bool:
    """Return True if the value is non-null and non-blank (used to detect dDeleted)."""
    if value is None:
        return False
    s = str(value).strip().lower()
    return s not in ("", "nan", "nat", "none")


def _safe_date_str(value) -> Optional[str]:
    """Return a human-readable date string, or None if blank/null."""
    if not _has_value(value):
        return None
    dt = parse_date(value)
    if dt:
        return dt.strftime("%Y-%m-%d")
    return str(value).strip()
