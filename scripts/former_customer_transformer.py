"""
former_customer_transformer.py — Field-level transformations for the
"All Former Tenants" SiteLink export (Powdersville format).

Column names in this export differ from the active-tenant SiteLink export
(FIRSTNAME/LASTNAME vs sFname/sLname). This module handles the new column
names and generates a deterministic external_id since TenantID is absent.
"""

import hashlib
from datetime import datetime

from scripts.customer_transformer import clean_str, derive_full_name


def make_external_id(lastname: str, firstname: str, phone: str, mobile: str, email: str) -> str:
    """
    Generate a stable, deterministic external_id for a former tenant.

    Because the former-tenant export has no TenantID we build a synthetic key
    from normalized identity fields so the same person always produces the same
    hash across re-runs (idempotent).

    Key = LASTNAME,FIRSTNAME | best-available contact (phone > mobile > email)
    Result = "FORMER-<first 12 hex chars of SHA-256>"
    """
    name_part = f"{(lastname or '').upper().strip()},{(firstname or '').upper().strip()}".strip(",")
    contact_part = (
        (phone or "").strip()
        or (mobile or "").strip()
        or (email or "").strip()
    )
    raw = f"{name_part}|{contact_part}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    return f"FORMER-{digest}"


def _fmt_date(value: str) -> str | None:
    """Parse a date string and return ISO date (YYYY-MM-DD) or None."""
    if not value or str(value).strip().lower() in ("", "nan", "none", "nat"):
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(s[:len(fmt) + 6].strip(), fmt)
            return dt.strftime("%Y-%m-%d") if dt.year >= 1970 else None
        except ValueError:
            continue
    return None


def build_notes(unit: str, move_in: str, move_out: str) -> str | None:
    """Build a plain-text notes string from former-tenant metadata."""
    parts = []
    u = clean_str(unit)
    mi = _fmt_date(move_in)
    mo = _fmt_date(move_out)
    if u:
        parts.append(f"Last unit: {u}")
    if mi:
        parts.append(f"Move-in: {mi}")
    if mo:
        parts.append(f"Move-out: {mo}")
    return " | ".join(parts) if parts else None


def transform_row(
    row,
    org_id: int,
    loc_id: int,
    created_by: int = 0,
) -> dict | None:
    """
    Transform one deduped former-tenant row into a storentic.customer insert dict.
    Returns None if the row must be skipped (last_name is blank).

    Expected columns (from FormerTenants_deduped.xlsx):
        FIRSTNAME, MI, LASTNAME, COMPANY,
        ADDRESS1, ADDRESS2, CITY, STATE, ZIPCODE,
        EMAIL, PHONE, MOBILE, GATECODE,
        UNITNAME, DATEMOVEDIN, DATEMOVEDOUT, ACTIVELEDGERS
    """
    def g(col):
        val = row.get(col, "")
        return "" if val is None else str(val).strip()

    last_name = clean_str(g("LASTNAME"))
    if not last_name:
        return None  # critical field — skip row

    first_name = derive_full_name(g("FIRSTNAME"), g("MI"))
    phone  = clean_str(g("PHONE"))
    mobile = clean_str(g("MOBILE"))
    email  = clean_str(g("EMAIL"))

    external_id = make_external_id(
        lastname=g("LASTNAME"),
        firstname=g("FIRSTNAME"),
        phone=g("PHONE"),
        mobile=g("MOBILE"),
        email=g("EMAIL"),
    )

    notes = build_notes(g("UNITNAME"), g("DATEMOVEDIN"), g("DATEMOVEDOUT"))

    return {
        "external_id":                  external_id,
        "first_name":                   first_name,
        "last_name":                    last_name,
        "company_name":                 clean_str(g("COMPANY")),
        "address1":                     clean_str(g("ADDRESS1")),
        "address2":                     clean_str(g("ADDRESS2")),
        "city":                         clean_str(g("CITY")),
        "state":                        clean_str(g("STATE")),
        "zip":                          clean_str(g("ZIPCODE")),
        "country":                      None,             # not in export
        "phone":                        phone,
        "alternate_first_name":         None,
        "alternate_last_name":          None,
        "alternate_address1":           None,
        "alternate_address2":           None,
        "alternate_city":               None,
        "alternate_state":              None,
        "alternate_zip":                None,
        "alternate_country":            None,
        "alternate_phone":              None,
        "email":                        email,
        "alternate_email":              None,
        "mobile":                       mobile,
        "access_gate_code":             clean_str(g("GATECODE")),
        "driver_license_id":            None,
        "driver_license_issue_state":   None,
        "notes":                        notes,
        # Context from .env
        "organization_id":              org_id,
        "location_id":                  loc_id,
        # Audit
        "created_by":                   created_by,
        "updated_by":                   created_by,
        "created_datetime":             datetime.utcnow(),
        "updated_datetime":             datetime.utcnow(),
    }
