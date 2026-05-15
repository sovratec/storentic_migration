"""
migrate_invoices.py
===================
ETL script: SiteLink Invoices export + Payments CSV → storentic.invoices

Column mapping
--------------
invoice_number       = iInvoiceNum
customer_id          = storentic.ledger_charges.customer_id  (join on external_charge_id = ChargeID)
invoice_type_id      = derived from ledger_charges.charge_type_id:
                           charge_type_id → invoice_type_id
                           3 → 1 | 1 → 2 | 21 → 3 | 13 → 4 | anything else → 7
status               = PAID / PARTIAL_PAID / UNPAID  (see payment logic below)
issue_date           = dInvoiced  (date only)
due_date             = dDue       (date only)
subtotal_in_cents    = sum of ledger_charges.amount_in_cents for all charges on the invoice
tax_in_cents         = 0
total_in_cents       = subtotal_in_cents
amount_paid_in_cents = sum of (dcPmtAmt * 100) from Payments for matching ChargeIDs
balance_in_cents     = total_in_cents − amount_paid_in_cents
external_invoice_id  = InvoiceID
organization_id      = ORGANIZATION_ID from .env
location_id          = LOCATION_ID from .env
created_by           = CREATED_BY from .env
created_at           = current timestamp
updated_at           = current timestamp
version              = 0

Payment status logic (per invoice, across all its ChargeIDs)
------------------------------------------------------------
Find all Payments rows where ChargeID is in the invoice's charge list,
dPmt is not null, and dPmt date <= today.

    total_paid >= total_charge  →  PAID
    0 < total_paid < total_charge  →  PARTIAL_PAID
    total_paid == 0  →  UNPAID

One invoice may reference multiple charges (one detail row per charge in the
source file). All charge amounts and payment amounts are summed at the invoice
level before the status decision.

Pre-requisite
-------------
Run sql/V9__invoices_external_invoice_id.sql against the target DB before
running this script.

Usage
-----
    # Dry run — counts only, no DB writes
    python migrate_invoices.py \\
        --file-invoices "data/Invoices.csv" \\
        --file-payments "data/Payments.csv" \\
        --dry-run

    # Live import
    python migrate_invoices.py \\
        --file-invoices "data/Invoices.csv" \\
        --file-payments "data/Payments.csv"

Environment (.env)
------------------
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID  — injected into every row (default: 1)
    LOCATION_ID      — injected into every row (default: 1)
    CREATED_BY       — user id for audit fields (default: 0)
    DRY_RUN          — "true" to preview without writing
    BATCH_SIZE       — rows per commit (default: 1000)
"""

import argparse
import os
import sys
from datetime import datetime, date

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import text as sa_text

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── charge_type_id → invoice_type_id ──────────────────────────────────────────
_CHARGE_TO_INVOICE_TYPE: dict[int, int] = {3: 1, 1: 2, 21: 3, 13: 4}
_DEFAULT_INVOICE_TYPE_ID = 7

# ── SQL ────────────────────────────────────────────────────────────────────────
_INSERT_SQL = """
    INSERT INTO storentic.invoices (
        invoice_number, customer_id, location_id, organization_id,
        invoice_type_id, status, issue_date, due_date,
        subtotal_in_cents, tax_in_cents, total_in_cents,
        amount_paid_in_cents, balance_in_cents,
        created_by, created_at, updated_at, version,
        external_invoice_id
    ) VALUES %s
    ON CONFLICT (external_invoice_id) WHERE external_invoice_id IS NOT NULL DO NOTHING
"""

_INSERT_COLS = [
    "invoice_number", "customer_id", "location_id", "organization_id",
    "invoice_type_id", "status", "issue_date", "due_date",
    "subtotal_in_cents", "tax_in_cents", "total_in_cents",
    "amount_paid_in_cents", "balance_in_cents",
    "created_by", "created_at", "updated_at", "version",
    "external_invoice_id",
]

SKIPPED_COLUMNS = ["InvoiceID", "iInvoiceNum", "ChargeIDs", "skip_reason"]


# =============================================================================
# Pre-flight
# =============================================================================

def check_prerequisites(engine):
    """Abort early if the V9 migration hasn't been applied."""
    with engine.connect() as conn:
        col = conn.execute(sa_text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'storentic' AND table_name = 'invoices' "
            "  AND column_name = 'external_invoice_id'"
        )).fetchone()
        idx = conn.execute(sa_text(
            "SELECT 1 FROM pg_indexes "
            "WHERE schemaname = 'storentic' AND tablename = 'invoices' "
            "  AND indexname = 'idx_invoices_external_invoice_id'"
        )).fetchone()

    fix = "\n    Run sql/V9__invoices_external_invoice_id.sql against the DB first.\n"
    if not col:
        raise SystemExit("❌  Column 'external_invoice_id' missing on storentic.invoices." + fix)
    if not idx:
        raise SystemExit("❌  Unique index 'idx_invoices_external_invoice_id' missing." + fix)


# =============================================================================
# DB lookups
# =============================================================================

def load_charge_map(engine) -> dict[str, dict]:
    """
    Returns {external_charge_id: {customer_id, charge_type_id, amount_in_cents}}
    for every row in storentic.ledger_charges that has an external_charge_id.
    """
    logger.info("Loading ledger_charges lookup from DB ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT external_charge_id, customer_id, charge_type_id, amount_in_cents "
            "FROM storentic.ledger_charges "
            "WHERE external_charge_id IS NOT NULL"
        )).fetchall()
    mapping = {
        row.external_charge_id: {
            "customer_id":     row.customer_id,
            "charge_type_id":  row.charge_type_id,
            "amount_in_cents": row.amount_in_cents,
        }
        for row in rows
    }
    logger.info(f"    Charges loaded: {len(mapping):,}")
    return mapping


def load_existing_invoice_ids(engine) -> set[str]:
    """All external_invoice_id values already in storentic.invoices."""
    logger.info("Loading existing external_invoice_ids ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT external_invoice_id FROM storentic.invoices "
            "WHERE external_invoice_id IS NOT NULL"
        )).fetchall()
    ids = {r.external_invoice_id for r in rows}
    logger.info(f"    Already imported: {len(ids):,} invoices")
    return ids


# =============================================================================
# Source file loading
# =============================================================================

def load_invoices_file(filepath: str) -> pd.DataFrame:
    """
    Read Invoices.csv. Each row is one invoice-detail record (one ChargeID).
    An invoice may have multiple detail rows; we group them later by InvoiceID.
    """
    logger.info(f"Loading invoices file: {filepath}")
    df = pd.read_csv(filepath, dtype=str, encoding='utf-8')
    df.columns = df.columns.str.strip()
    df = df.drop(columns=["Totals & Averages"], errors="ignore")

    required = {"InvoiceID", "iInvoiceNum", "ChargeID", "dInvoiced", "dDue"}
    missing  = required - set(df.columns)
    if missing:
        raise SystemExit(f"❌  Invoices file missing columns: {missing}")

    # Drop summary/blank rows
    df = df[df["InvoiceID"].notna() & (df["InvoiceID"].str.strip() != "")].copy()
    df = df.reset_index(drop=True)
    logger.info(f"    Detail rows      : {len(df):,}")
    logger.info(f"    Unique invoices  : {df['InvoiceID'].nunique():,}")
    return df


def load_payments_file(filepath: str) -> dict[str, int]:
    """
    Read Payments CSV and build {ChargeID: total_paid_in_cents}.
    Only includes payments where dPmt is not null and dPmt <= today.
    Multiple payments for the same ChargeID are summed.
    """
    logger.info(f"Loading payments file: {filepath}")
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip()
    today = datetime.utcnow().date()

    payment_map: dict[str, int] = {}
    skipped = 0

    for _, row in df.iterrows():
        charge_id = _clean_id(row.get("ChargeID", ""))
        if not charge_id:
            continue

        d_pmt_raw  = row.get("dPmt", "")
        dc_pmt_amt = row.get("dcPmtAmt", "")

        d_pmt = _parse_dt(d_pmt_raw)
        if d_pmt is None or d_pmt.date() > today:
            skipped += 1
            continue

        try:
            amt_cents = int(round(float(dc_pmt_amt) * 100))
        except (TypeError, ValueError):
            skipped += 1
            continue

        payment_map[charge_id] = payment_map.get(charge_id, 0) + amt_cents

    logger.info(f"    Charges with payment : {len(payment_map):,}  (rows skipped: {skipped})")
    return payment_map


# =============================================================================
# Helpers
# =============================================================================

def _clean_id(val) -> str | None:
    """Strip whitespace, remove trailing .0, return None for blank/nan."""
    s = str(val).strip()
    if s in ("", "nan", "NaT", "None"):
        return None
    # "29021.0" → "29021"
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s or None


def _parse_date(val) -> date | None:
    """Parse any date-like string/value into a Python date; None on failure."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "nan", "NaT", "None"):
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


def _parse_dt(val) -> datetime | None:
    """Parse any datetime-like string into a Python datetime; None on failure."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "nan", "NaT", "None"):
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _invoice_type_id(charge_type_id) -> int:
    try:
        return _CHARGE_TO_INVOICE_TYPE.get(int(charge_type_id), _DEFAULT_INVOICE_TYPE_ID)
    except (TypeError, ValueError):
        return _DEFAULT_INVOICE_TYPE_ID


def _derive_status(charge_ids: list[str], charge_map: dict, payment_map: dict) -> tuple[str, int, int]:
    """
    Returns (status, total_charge_cents, total_paid_cents).
    Sums amounts across all charges on the invoice.
    """
    total_charge = sum(
        charge_map[cid]["amount_in_cents"]
        for cid in charge_ids
        if cid in charge_map
    )
    total_paid = sum(payment_map.get(cid, 0) for cid in charge_ids)

    if total_paid <= 0:
        status = "UNPAID"
    elif total_paid < total_charge:
        status = "PARTIAL_PAID"
    else:
        status = "PAID"

    return status, total_charge, total_paid


# =============================================================================
# Invoice builder
# =============================================================================

def build_invoice_records(
    df: pd.DataFrame,
    charge_map: dict,
    payment_map: dict,
    existing_ids: set[str],
    loc_id: int,
    org_id: int,
    created_by: int,
) -> tuple[list[dict], list[dict]]:
    """
    Groups detail rows by InvoiceID, resolves all lookups, and returns:
        (records_to_insert, skipped_rows)
    """
    now     = datetime.utcnow()
    records = []
    skipped = []

    grouped = df.groupby("InvoiceID", sort=False)
    logger.info(f"    Building records for {grouped.ngroups:,} unique invoices ...")

    for invoice_id_raw, group in grouped:
        invoice_id = str(invoice_id_raw).strip()

        if invoice_id in existing_ids:
            continue

        first = group.iloc[0]

        invoice_num = _clean_id(first.get("iInvoiceNum"))
        issue_date  = _parse_date(first.get("dInvoiced"))
        due_date    = _parse_date(first.get("dDue"))

        if not invoice_num:
            skipped.append({"InvoiceID": invoice_id, "skip_reason": "iInvoiceNum is blank"})
            log_skipped(0, invoice_id, "iInvoiceNum", "iInvoiceNum is blank")
            continue
        if issue_date is None:
            skipped.append({"InvoiceID": invoice_id, "iInvoiceNum": invoice_num,
                             "skip_reason": "dInvoiced missing or unparseable"})
            log_skipped(0, invoice_id, "dInvoiced", "dInvoiced missing or unparseable")
            continue
        if due_date is None:
            skipped.append({"InvoiceID": invoice_id, "iInvoiceNum": invoice_num,
                             "skip_reason": "dDue missing or unparseable"})
            log_skipped(0, invoice_id, "dDue", "dDue missing or unparseable")
            continue

        # All ChargeIDs for this invoice
        charge_ids = [
            cid for cid in (
                _clean_id(v) for v in group["ChargeID"].dropna()
            ) if cid
        ]

        if not charge_ids:
            skipped.append({"InvoiceID": invoice_id, "iInvoiceNum": invoice_num,
                             "skip_reason": "No valid ChargeIDs in detail rows"})
            log_skipped(0, invoice_id, "ChargeID", "No valid ChargeIDs in detail rows")
            continue

        # Resolve customer_id and charge_type_id from first matched charge in DB
        customer_id    = None
        charge_type_id = None
        for cid in charge_ids:
            charge_data = charge_map.get(cid)
            if charge_data:
                customer_id    = charge_data["customer_id"]
                charge_type_id = charge_data["charge_type_id"]
                break

        if customer_id is None:
            reason = "No ChargeID matched storentic.ledger_charges.external_charge_id"
            skipped.append({
                "InvoiceID":   invoice_id,
                "iInvoiceNum": invoice_num,
                "ChargeIDs":   ",".join(charge_ids),
                "skip_reason": reason,
            })
            log_skipped(0, invoice_id, "ChargeID", reason, ",".join(charge_ids))
            continue

        status, total_cents, paid_cents = _derive_status(charge_ids, charge_map, payment_map)
        balance_cents = max(total_cents - paid_cents, 0)

        records.append({
            "invoice_number":      invoice_num,
            "customer_id":         customer_id,
            "location_id":         loc_id,
            "organization_id":     org_id,
            "invoice_type_id":     _invoice_type_id(charge_type_id),
            "status":              status,
            "issue_date":          issue_date,
            "due_date":            due_date,
            "subtotal_in_cents":   total_cents,
            "tax_in_cents":        0,
            "total_in_cents":      total_cents,
            "amount_paid_in_cents": paid_cents,
            "balance_in_cents":    balance_cents,
            "created_by":          created_by,
            "created_at":          now,
            "updated_at":          now,
            "version":             0,
            "external_invoice_id": invoice_id,
        })

    return records, skipped


# =============================================================================
# DB write
# =============================================================================

def insert_invoices(records: list[dict], engine, batch_size: int) -> int:
    """Bulk-insert all records using execute_values. Returns count inserted."""
    raw_conn   = engine.raw_connection()
    raw_cursor = raw_conn.cursor()
    inserted   = 0

    try:
        for i in range(0, len(records), batch_size):
            batch  = records[i: i + batch_size]
            tuples = [tuple(r[col] for col in _INSERT_COLS) for r in batch]
            execute_values(raw_cursor, _INSERT_SQL, tuples, page_size=batch_size)
            raw_conn.commit()
            inserted += len(batch)
            logger.info(f"    Committed batch {i // batch_size + 1}: {len(batch):,} rows  (total so far: {inserted:,})")
    except Exception as exc:
        raw_conn.rollback()
        raise
    finally:
        raw_cursor.close()
        raw_conn.close()

    return inserted


# =============================================================================
# Outputs
# =============================================================================

def _write_skipped(skipped: list[dict], run_ts: str):
    if not skipped:
        return
    path = os.path.join(OUTPUT_DIR, f"invoices_skipped_{run_ts}.xlsx")
    pd.DataFrame(skipped, columns=[c for c in SKIPPED_COLUMNS if c in (skipped[0] if skipped else {})]) \
      .to_excel(path, index=False)
    print(f"  ⚠️   {len(skipped):,} skipped invoices → {path}", flush=True)
    logger.info(f"Skipped output: {path}")


def _print_summary(run_ts: str, dry_run: bool, inserted: int,
                   skipped_dup: int, skipped_no_match: int):
    from scripts.logger import LOG_FILE, ERROR_CSV
    lines = [
        "=" * 65,
        "  INVOICES MIGRATION — SUMMARY",
        f"  Run timestamp  : {run_ts}",
        f"  Dry run        : {dry_run}",
        "=" * 65,
        f"  Inserted                      : {inserted:,}",
        f"  Skipped (already imported)    : {skipped_dup:,}",
        f"  Skipped (no ledger_charges match): {skipped_no_match:,}",
        "=" * 65,
        f"  Log file  : {LOG_FILE}",
        f"  Error CSV : {ERROR_CSV}",
        "=" * 65,
    ]
    text = "\n".join(lines)
    print("\n" + text, flush=True)
    logger.info(text)


# =============================================================================
# CLI
# =============================================================================

def parse_args(args=None):
    p = argparse.ArgumentParser(
        description="SiteLink Invoices + Payments → storentic.invoices migration"
    )
    p.add_argument("--file-invoices", required=True, help="Path to Invoices.csv")
    p.add_argument("--file-payments", required=True, help="Path to Payments.csv")
    p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    return p.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    dry_run    = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    org_id     = int(os.getenv("ORGANIZATION_ID", 1))
    loc_id     = int(os.getenv("LOCATION_ID", 1))
    created_by = int(os.getenv("CREATED_BY", 0))
    batch_size = int(os.getenv("BATCH_SIZE", 1000))
    run_ts     = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(
        f"\n  Storentic Invoices Migration"
        f"\n  Mode: {'DRY RUN' if dry_run else 'LIVE'}  |"
        f"  org={org_id}  loc={loc_id}  created_by={created_by}\n",
        flush=True,
    )
    logger.info("=" * 65)
    logger.info("INVOICES ETL MIGRATION STARTING")
    logger.info(f"Invoices file  : {args.file_invoices}")
    logger.info(f"Payments file  : {args.file_payments}")
    logger.info(f"Dry run        : {dry_run}")
    logger.info(f"Org/Loc/By     : {org_id}/{loc_id}/{created_by}")
    logger.info("=" * 65)

    # DB connection
    try:
        from scripts.db import get_engine
        engine = get_engine()
        logger.info("Database connection established.")
    except Exception as exc:
        print(f"\n  ERROR: Cannot connect to DB: {exc}\n", flush=True)
        sys.exit(1)

    # Pre-flight check
    check_prerequisites(engine)

    # Load all sources
    df_invoices  = load_invoices_file(args.file_invoices)
    payment_map  = load_payments_file(args.file_payments)
    charge_map   = load_charge_map(engine)
    existing_ids = load_existing_invoice_ids(engine)

    # Build invoice records
    records, skipped = build_invoice_records(
        df          = df_invoices,
        charge_map  = charge_map,
        payment_map = payment_map,
        existing_ids = existing_ids,
        loc_id      = loc_id,
        org_id      = org_id,
        created_by  = created_by,
    )

    print(f"  To insert  : {len(records):,}", flush=True)
    print(f"  Skipped    : {len(skipped):,}  (no charge match / bad data)", flush=True)

    # Write to DB
    inserted = 0
    if dry_run:
        inserted = len(records)
        print(f"  [DRY RUN] Would insert {inserted:,} invoices.", flush=True)
    elif records:
        inserted = insert_invoices(records, engine, batch_size)

    _write_skipped(skipped, run_ts)
    _print_summary(run_ts, dry_run, inserted,
                   skipped_dup=len(existing_ids),
                   skipped_no_match=len(skipped))
    close_logger()


if __name__ == "__main__":
    main()
