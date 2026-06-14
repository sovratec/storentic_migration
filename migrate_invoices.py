"""
migrate_invoices.py
===================
ETL script: SiteLink Invoices export + Payments CSV → storentic.invoices

Column mapping
---------------
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
from datetime import datetime, date, timezone

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import text as sa_text

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger
from scripts import to_bigint

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── charge_type_id → invoice_type_id ──────────────────────────────────────────
_CHARGE_TO_INVOICE_TYPE: dict[int, int] = {3: 1, 1: 2, 21: 3, 13: 4}
_DEFAULT_INVOICE_TYPE_ID = 7

# ── invoice_type_id → invoice_type name (from storentic.invoice_types) ────────
_INVOICE_TYPE_NAMES: dict[int, str] = {
    1: "RENTAL",
    2: "DEPOSIT",
    3: "INSURANCE",
    4: "LATE_FEE",
    5: "MERCHANDISE",
    6: "CREDIT_MEMO",
    7: "MISC",
}

# ── SQL ────────────────────────────────────────────────────────────────────────
_INSERT_SQL = """
    INSERT INTO storentic.invoices (
        invoice_number, customer_id, location_id, organization_id,
        invoice_type_id, invoice_type, status, issue_date, due_date,
        subtotal_in_cents, tax_in_cents, total_in_cents,
        amount_paid_in_cents, balance_in_cents,
        created_by, created_at, updated_at, version,
        external_invoice_id, external_employee_id, external_system
    ) VALUES %s
    ON CONFLICT (external_invoice_id) WHERE external_invoice_id IS NOT NULL DO NOTHING
"""

_INSERT_COLS = [
    "invoice_number", "customer_id", "location_id", "organization_id",
    "invoice_type_id", "invoice_type", "status", "issue_date", "due_date",
    "subtotal_in_cents", "tax_in_cents", "total_in_cents",
    "amount_paid_in_cents", "balance_in_cents",
    "created_by", "created_at", "updated_at", "version",
    "external_invoice_id", "external_employee_id", "external_system",
]

SKIPPED_COLUMNS = ["INVOICEID", "IINVOICENUM", "ChargeIDs", "skip_reason"]


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
    df = pd.read_csv(filepath, dtype=str, encoding='latin-1')
    df.columns = df.columns.str.strip().str.upper()
    df = df.drop(columns=["Totals & Averages"], errors="ignore")

    required = {"INVOICEID", "IINVOICENUM", "CHARGEID", "DINVOICED", "DDUE"}
    missing  = required - set(df.columns)
    if missing:
        raise SystemExit(f"❌  Invoices file missing columns: {missing}")

    # Drop summary/blank rows
    df = df[df["INVOICEID"].notna() & (df["INVOICEID"].str.strip() != "")].copy()
    df = df.reset_index(drop=True)
    logger.info(f"    Detail rows      : {len(df):,}")
    logger.info(f"    Unique invoices  : {df['INVOICEID'].nunique():,}")
    return df


def load_payments_file(filepath: str) -> dict[str, int]:
    """
    Read Payments CSV and build {ChargeID: total_paid_in_cents}.
    Only includes payments where dPmt is not null and dPmt <= today.
    Multiple payments for the same ChargeID are summed.
    """
    logger.info(f"Loading payments file: {filepath}")
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    today = datetime.now(timezone.utc).date()

    payment_map: dict[str, int] = {}
    skipped = 0

    for _, row in df.iterrows():
        charge_id = _clean_id(row.get("CHARGEID", ""))
        if not charge_id:
            continue

        d_pmt_raw  = row.get("DPMT", "")
        dc_pmt_amt = row.get("DCPMTAMT", "")

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

    Each record includes a '_payment_ids' key (list of PAYMENTID strings from the
    source file) used later to link payments → invoices after insert. This key is
    NOT written to the DB — it is stripped before building INSERT tuples.
    """
    now     = datetime.now(timezone.utc)
    records = []
    skipped = []
    has_payment_col = "PAYMENTID" in df.columns

    grouped = df.groupby("INVOICEID", sort=False)
    logger.info(f"    Building records for {grouped.ngroups:,} unique invoices ...")

    for invoice_id_raw, group in grouped:
        invoice_id = str(invoice_id_raw).strip()

        if invoice_id in existing_ids:
            continue

        first = group.iloc[0]

        invoice_num = _clean_id(first.get("IINVOICENUM"))
        issue_date  = _parse_date(first.get("DINVOICED"))
        due_date    = _parse_date(first.get("DDUE"))

        if not invoice_num:
            skipped.append({"INVOICEID": invoice_id, "skip_reason": "iInvoiceNum is blank"})
            log_skipped(0, invoice_id, "IINVOICENUM", "iInvoiceNum is blank")
            continue
        if issue_date is None:
            skipped.append({"INVOICEID": invoice_id, "IINVOICENUM": invoice_num,
                             "skip_reason": "dInvoiced missing or unparseable"})
            log_skipped(0, invoice_id, "DINVOICED", "dInvoiced missing or unparseable")
            continue
        if due_date is None:
            skipped.append({"INVOICEID": invoice_id, "IINVOICENUM": invoice_num,
                             "skip_reason": "dDue missing or unparseable"})
            log_skipped(0, invoice_id, "DDUE", "dDue missing or unparseable")
            continue

        # All ChargeIDs for this invoice
        charge_ids = [
            cid for cid in (
                _clean_id(v) for v in group["CHARGEID"].dropna()
            ) if cid
        ]

        if not charge_ids:
            skipped.append({"INVOICEID": invoice_id, "IINVOICENUM": invoice_num,
                             "skip_reason": "No valid ChargeIDs in detail rows"})
            log_skipped(0, invoice_id, "CHARGEID", "No valid ChargeIDs in detail rows")
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
                "INVOICEID":   invoice_id,
                "IINVOICENUM": invoice_num,
                "ChargeIDs":   ",".join(charge_ids),
                "skip_reason": reason,
            })
            log_skipped(0, invoice_id, "CHARGEID", reason, ",".join(charge_ids))
            continue

        status, total_cents, paid_cents = _derive_status(charge_ids, charge_map, payment_map)

        # Skip negative or zero-total invoices (credit notes) — chk_amounts requires total_in_cents >= 0
        if total_cents <= 0:
            skipped.append({
                "INVOICEID":   invoice_id,
                "IINVOICENUM": invoice_num,
                "ChargeIDs":   ",".join(charge_ids),
                "skip_reason": f"Skipped: total_in_cents={total_cents} (credit note / negative invoice)",
            })
            log_skipped(0, invoice_id, "total_in_cents",
                        f"total_in_cents={total_cents} — credit note skipped", invoice_id)
            continue

        balance_cents = max(total_cents - paid_cents, 0)

        # Collect unique PAYMENTIDs from all detail rows for this invoice
        payment_ids: list[str] = []
        if has_payment_col:
            payment_ids = list({
                pid for pid in (
                    _clean_id(v) for v in group["PAYMENTID"].dropna()
                ) if pid
            })

        emp_id = to_bigint(first.get("EMPLOYEEID"))

        inv_type_id = _invoice_type_id(charge_type_id)
        inv_type    = _INVOICE_TYPE_NAMES.get(inv_type_id, "MISC")

        records.append({
            "invoice_number":       invoice_num,
            "customer_id":          customer_id,
            "location_id":          loc_id,
            "organization_id":      org_id,
            "invoice_type_id":      inv_type_id,
            "invoice_type":         inv_type,
            "status":               status,
            "issue_date":           issue_date,
            "due_date":             due_date,
            "subtotal_in_cents":    total_cents,
            "tax_in_cents":         0,
            "total_in_cents":       total_cents,
            "amount_paid_in_cents": paid_cents,
            "balance_in_cents":     balance_cents,
            "created_by":           created_by,
            "created_at":           now,
            "updated_at":           now,
            "version":              0,
            "external_invoice_id":  invoice_id,
            "external_employee_id": emp_id,
            "external_system":      "sitelink",
            # ── not written to DB — used only for payment linking ──
            "_payment_ids":         payment_ids,
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
            # _payment_ids is metadata only — exclude from INSERT tuples
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


def link_payments_to_invoices(records: list[dict], engine, dry_run: bool) -> int:
    """
    After invoices are inserted, update storentic.payments.invoice_id for every
    payment referenced in the source file.

    Steps:
      1. Collect all external_invoice_ids that have at least one PAYMENTID.
      2. Query storentic.invoices to get their DB PKs.
      3. For each (invoice_pk, external_payment_id) pair, update
         storentic.payments SET invoice_id = <pk>
         WHERE external_payment_id = <pay_id> AND invoice_id IS NULL.

    Returns the total number of payment rows updated.
    """
    # Build list of (external_invoice_id, external_payment_id) pairs
    pairs: list[tuple[str, str]] = []
    for r in records:
        ext_inv_id = r["external_invoice_id"]
        for pay_id in r.get("_payment_ids", []):
            pairs.append((ext_inv_id, pay_id))

    if not pairs:
        logger.info("    No PAYMENTID values found — skipping payment link step.")
        return 0

    # Fetch invoice PKs for all inserted external_invoice_ids
    ext_inv_ids = list({p[0] for p in pairs})
    logger.info(f"🔗  Linking payments → invoices  ({len(pairs):,} pairs, {len(ext_inv_ids):,} invoices) ...")

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        # Cast the text array to bigint[] in SQL to match the bigint column type
        cur.execute(
            "SELECT id, external_invoice_id FROM storentic.invoices "
            "WHERE external_invoice_id = ANY(%s::bigint[])",
            (ext_inv_ids,)
        )
        # Store as str key to match the string keys used in records
        invoice_pk_map: dict[str, int] = {str(row[1]): row[0] for row in cur.fetchall()}
        cur.close()
    finally:
        raw_conn.close()

    logger.info(f"    Invoice PKs resolved: {len(invoice_pk_map):,}")

    if not invoice_pk_map:
        logger.warning("    No invoice PKs found — payment link skipped.")
        return 0

    # Build (invoice_pk, external_payment_id) tuples for bulk UPDATE
    update_tuples = []
    for ext_inv_id, pay_id in pairs:
        invoice_pk = invoice_pk_map.get(ext_inv_id)
        if invoice_pk is not None:
            update_tuples.append((invoice_pk, pay_id))

    if not update_tuples:
        logger.info("    No matching invoice PKs found — payment link skipped.")
        return 0

    if dry_run:
        logger.info(f"    [DRY RUN] Would update {len(update_tuples):,} payments.invoice_id.")
        return len(update_tuples)

    # Single bulk UPDATE — replaces N individual statements
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE storentic.payments AS p
                SET invoice_id = v.invoice_id
                FROM (VALUES %s) AS v(invoice_id, external_payment_id)
                WHERE p.external_payment_id = v.external_payment_id
                  AND p.invoice_id IS NULL
                """,
                update_tuples,
                page_size=1000,
            )
        raw_conn.commit()
        updated = len(update_tuples)
    except Exception as exc:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    logger.info(f"✅  Payments updated with invoice_id: {updated:,}")
    return updated


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
                   skipped_dup: int, skipped_no_match: int,
                   payments_linked: int = 0):
    from scripts.logger import LOG_FILE, ERROR_CSV
    lines = [
        "=" * 65,
        "  INVOICES MIGRATION — SUMMARY",
        f"  Run timestamp  : {run_ts}",
        f"  Dry run        : {dry_run}",
        "=" * 65,
        f"  Invoices inserted                   : {inserted:,}",
        f"  Skipped (already imported)          : {skipped_dup:,}",
        f"  Skipped (no ledger_charges match)   : {skipped_no_match:,}",
        f"  Payments linked (invoice_id set)    : {payments_linked:,}",
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
    p.add_argument("--output", default="db", choices=["db"], help="Output destination (currently only 'db')")
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

    # Link payments → invoices (set payments.invoice_id)
    payments_linked = 0
    if records:
        payments_linked = link_payments_to_invoices(records, engine, dry_run)
        if not dry_run:
            print(f"  Payments linked : {payments_linked:,}", flush=True)

    _write_skipped(skipped, run_ts)
    _print_summary(run_ts, dry_run, inserted,
                   skipped_dup=len(existing_ids),
                   skipped_no_match=len(skipped),
                   payments_linked=payments_linked)
    close_logger()


if __name__ == "__main__":
    main()
