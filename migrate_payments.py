"""
migrate_payments.py — ETL script: SiteLink Payments → storentic.payments

Usage :
    # Dry run (preview row counts, no DB writes)
    python migrate_payments.py \\
        --file-payments data/Payments.csv \\
        --file-tenants  "data/Tenants+Units+Ledgers+Access.csv" \\
        --file-credits  data/Credits.csv \\
        --output db --dry-run

    # Export to Excel for review before DB import
    python migrate_payments.py \\
        --file-payments data/Payments.csv \\
        --file-tenants  "data/Tenants+Units+Ledgers+Access.csv" \\
        --file-credits  data/Credits.csv \\
        --output excel

    # Production import
    python migrate_payments.py \\
        --file-payments data/Payments.csv \\
        --file-tenants  "data/Tenants+Units+Ledgers+Access.csv" \\
        --file-credits  data/Credits.csv \\
        --output db

Arguments:
    --file-payments   Path to Payments.csv from SiteLink (required)
    --file-tenants    Path to Tenants+Units+Ledgers+Access.csv (required)
    --output          'db' (default) or 'excel'
    --dry-run         [db mode] Preview without writing to DB
    --out-file        [excel mode] Output Excel file path

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — required, scopes the customer lookup
    LOCATION_ID       — written to every payment row
    CREATED_BY        — user ID written to created_by column
    DRY_RUN           — alternative to --dry-run flag
    BATCH_SIZE        — rows per DB commit batch (default: 500)

Pre-requisite:
    Run sql/V5__payments_sitelink_migration_columns.sql against the target DB
    before running this script.

Join chain (how SiteLink payments map to Storentic customers):
    Payments.csv       LedgerID
        |
        v  (via Tenants+Units+Ledgers+Access.csv)
    TenantID
        |
        v  (via storentic.customer WHERE external_id = TenantID)
    customer.id        → payment.customer_id
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import text as sa_text

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger
from scripts import payment_transformer as PT

# ── Output directory ───────────────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── INSERT SQL ─────────────────────────────────────────────────────────────────
_INSERT_SQL = """
    INSERT INTO storentic.payments (
        external_id,
        external_payment_id,
        receipt_number,
        customer_id,
        location_id,
        organization_id,
        amount_in_cents,
        payment_method_type_id,
        status,
        payment_date,
        failure_reason,
        reference_number,
        notes,
        created_by,
        created_at,
        updated_at,
        payment_profile_id,
        stripe_payment_intent_id,
        stripe_charge_id,
        stripe_refund_id,
        transaction_fee_in_cents,
        refunded_amount_in_cents,
        reversal_reason,
        reversed_by,
        reversed_at,
        external_employee_id,
        external_system
    ) VALUES %s
    ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING
"""

_INSERT_COLS = [
    "external_id", "external_payment_id", "receipt_number",
    "customer_id", "location_id", "organization_id",
    "amount_in_cents", "payment_method_type_id", "status",
    "payment_date", "failure_reason", "reference_number",
    "notes", "created_by", "created_at", "updated_at",
    "payment_profile_id", "stripe_payment_intent_id",
    "stripe_charge_id", "stripe_refund_id",
    "transaction_fee_in_cents", "refunded_amount_in_cents",
    "reversal_reason", "reversed_by", "reversed_at",
    "external_employee_id", "external_system",
]

# Excel output columns (in display order, audit fields last)
EXCEL_OUTPUT_COLUMNS = [
    "external_id", "external_payment_id", "receipt_number", "customer_id", "location_id",
    "organization_id", "amount_in_cents", "payment_method_type_id",
    "status", "payment_date", "failure_reason",
    "reference_number", "notes", "created_by", "created_at",
]


# =============================================================================
# STEP 1 — Build lookup maps
# =============================================================================

def build_ledger_to_tenant_map(tenants_file: str) -> dict[int, int]:
    """
    Read Tenants CSV and return {LedgerID: TenantID}.

    One tenant can have multiple ledgers (multiple units). Each row in the
    CSV file represents one ledger. We build the inverse map here so that
    for every LedgerID we know which TenantID it belongs to.
    """
    logger.info(f"📂  Loading tenants file: {tenants_file}")
    df = pd.read_csv(tenants_file, dtype=str, encoding='utf-8')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()

    if "LEDGERID" not in df.columns or "TENANTID" not in df.columns:
        raise SystemExit(
            "❌  Tenants file must contain both 'TENANTID' and 'LEDGERID' columns.\n"
            f"    Found: {list(df.columns[:20])}"
        )

    ledger_to_tenant: dict[int, int] = {}
    skipped = 0
    for _, row in df.iterrows():
        try:
            ledger_id = int(float(row["LEDGERID"]))
            tenant_id = int(float(row["TENANTID"]))
            ledger_to_tenant[ledger_id] = tenant_id
        except (ValueError, TypeError):
            skipped += 1

    logger.info(
        f"    Rows read        : {len(df)}"
        f"\n    Unique LedgerIDs : {len(ledger_to_tenant)}"
        f"\n    Skipped (bad IDs): {skipped}"
    )
    return ledger_to_tenant


def load_storentic_customers(engine, org_id: int) -> dict[int, int]:
    """
    Query storentic.customer and return {TenantID (external_id): customer.id}.

    Only customers that have external_id set (i.e. migrated from SiteLink) are
    included. Customers with no external_id are ignored.
    """
    logger.info(f"🔍  Loading Storentic customers for org_id={org_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(
            sa_text(
                "SELECT id, external_id FROM storentic.customer "
                "WHERE external_id IS NOT NULL AND organization_id = :org_id"
            ),
            {"org_id": org_id},
        ).fetchall()

    tenant_to_customer: dict[int, int] = {}
    for row in rows:
        try:
            tenant_id = int(float(row.external_id))
            tenant_to_customer[tenant_id] = row.id
        except (ValueError, TypeError):
            pass  # external_id not a numeric TenantID — skip

    logger.info(f"    Customers with TenantID: {len(tenant_to_customer)}")
    return tenant_to_customer


def build_ledger_to_customer_map(
    ledger_to_tenant: dict[int, int],
    tenant_to_customer: dict[int, int],
) -> dict[int, int]:
    """
    Combine the two lookup dicts into the final {LedgerID: customer.id} map.
    Logs how many LedgerIDs could not be resolved to a Storentic customer.
    """
    ledger_to_customer: dict[int, int] = {}
    unresolved_tenants: set[int] = set()

    for ledger_id, tenant_id in ledger_to_tenant.items():
        customer_id = tenant_to_customer.get(tenant_id)
        if customer_id is not None:
            ledger_to_customer[ledger_id] = customer_id
        else:
            unresolved_tenants.add(tenant_id)

    if unresolved_tenants:
        # File log has full list; console gets a single clean line
        logger.info(
            f"Unresolved TenantIDs ({len(unresolved_tenants)}): "
            + str(sorted(unresolved_tenants))
        )

    logger.info(
        f"LedgerID->customer map: {len(ledger_to_customer)} resolved, "
        f"{len(ledger_to_tenant) - len(ledger_to_customer)} unresolved"
    )
    return ledger_to_customer


def load_credit_payment_ids(credits_file: str) -> set[str]:
    """
    Read Credits.csv and return the set of PaymentIDs that represent credit
    transactions.  These payments are already imported as negative ledger_charges
    by migrate_credits.py, so they must NOT also be imported as payment rows
    (that would double-count the credit and push the balance negative).
    """
    logger.info(f"📂  Loading credits file for dedup: {credits_file}")
    try:
        df = pd.read_csv(credits_file, dtype=str, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(credits_file, dtype=str, encoding="latin-1")
    df = df.drop(columns=["Totals & Averages", "TOTALS & AVERAGES"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()

    if "PAYMENTID" not in df.columns:
        logger.warning("Credits file has no PAYMENTID column — credit dedup skipped.")
        return set()

    ids = {
        str(int(float(v)))
        for v in df["PAYMENTID"].dropna()
        if str(v).strip() not in ("", "nan")
    }
    logger.info(f"    Credit PaymentIDs loaded: {len(ids):,}  (will be excluded from payments)")
    return ids


def load_existing_external_ids(engine) -> set[str]:
    """
    Load all external_id values already present in storentic.payments.
    Used to skip already-imported payments on re-runs (idempotency).
    """
    logger.info("🔍  Loading existing payment external_ids for deduplication ...")

    # Pre-flight: confirm the external_id column exists.
    # If not, the V5 SQL migration has not been run yet.
    with engine.connect() as conn:
        col_exists = conn.execute(sa_text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'storentic' "
            "  AND table_name   = 'payments' "
            "  AND column_name  = 'external_id'"
        )).fetchone()

    if not col_exists:
        raise SystemExit(
            "\n❌  Column 'external_id' does not exist on storentic.payments.\n\n"
            "    Run the V5 SQL migration first:\n\n"
            "      psql -h <DB_HOST> -U <DB_USER> -d storentic \\\n"
            "           -f sql/V5__payments_sitelink_migration_columns.sql\n"
        )

    with engine.connect() as conn:
        ext_pmt_col_exists = conn.execute(sa_text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'storentic' "
            "  AND table_name   = 'payments' "
            "  AND column_name  = 'external_payment_id'"
        )).fetchone()

    if not ext_pmt_col_exists:
        raise SystemExit(
            "\n❌  Column 'external_payment_id' does not exist on storentic.payments.\n\n"
            "    Run the V10 SQL migration first:\n\n"
            "      psql -h <DB_HOST> -U <DB_USER> -d storentic \\\n"
            "           -f sql/V10__payments_external_payment_id.sql\n\n"
            "    Or paste this into your DB client:\n\n"
            "      ALTER TABLE storentic.payments\n"
            "          ADD COLUMN IF NOT EXISTS external_payment_id VARCHAR(100);\n\n"
            "      CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_external_payment_id\n"
            "          ON storentic.payments(external_payment_id)\n"
            "          WHERE external_payment_id IS NOT NULL;\n"
        )

    with engine.connect() as conn:
        rows = conn.execute(
            sa_text(
                "SELECT external_id FROM storentic.payments "
                "WHERE external_id IS NOT NULL"
            )
        ).fetchall()
    ids = {row.external_id for row in rows}
    logger.info(f"    Already imported: {len(ids)} payments")
    return ids


# =============================================================================
# STEP 2 — Process payments
# =============================================================================

def process_payments(
    payments_file: str,
    ledger_to_customer: dict[int, int],
    org_id: int,
    loc_id: int,
    created_by: int,
    output_mode: str,
    dry_run: bool,
    existing_ids: set[str],
    engine,
    out_file: str,
    batch_size: int,
    credit_payment_ids: set[str] | None = None,
) -> dict:
    """
    Main processing loop.

    Reads Payments.csv row by row, transforms each row, and either:
      - Inserts into storentic.payments (db mode)
      - Appends to an in-memory list for Excel export (excel mode)
      - Logs the would-be action (dry-run mode)

    Returns a stats dict.
    """
    logger.info(f"📂  Loading payments file: {payments_file}")
    df = pd.read_csv(payments_file, dtype=str, low_memory=False, encoding='latin-1')
    df.columns = df.columns.str.strip().str.upper()
    df = df.drop(columns=["TOTALS & AVERAGES"], errors="ignore")
    total_raw = len(df)
    logger.info(f"    Total payment rows: {total_raw:,}")

    stats = {
        "total"             : total_raw,
        "inserted"          : 0,
        "skipped_dup"       : 0,
        "skipped_deleted"   : 0,
        "skipped_credits"   : 0,
        "skipped_no_cust"   : 0,
        "skipped_zero_amt"  : 0,
        "errors"            : 0,
        "failed_payments"   : 0,
        "dry_run"           : dry_run,
        "output_mode"       : output_mode,
    }

    # ── Step 1: Pre-filter with vectorized pandas (fast) ─────────────────────

    # Drop blank LedgerID
    df = df[df["LEDGERID"].notna() & (df["LEDGERID"].str.strip() != "")].copy()

    # Map LedgerID → customer_id vectorized
    df["LEDGERID_INT"] = pd.to_numeric(df["LEDGERID"], errors="coerce")
    df = df[df["LEDGERID_INT"].notna()].copy()
    df["LEDGERID_INT"] = df["LEDGERID_INT"].astype(int)
    df["customer_id"] = df["LEDGERID_INT"].map(ledger_to_customer)
    no_cust = df["customer_id"].isna().sum()
    stats["skipped_no_cust"] = int(no_cust)
    df = df[df["customer_id"].notna()].copy()
    df["customer_id"] = df["customer_id"].astype(int)

    # Drop zero-amount rows
    df["DCPMTAMT_NUM"] = pd.to_numeric(df["DCPMTAMT"], errors="coerce").fillna(0)
    zero_amt = (df["DCPMTAMT_NUM"] == 0).sum()
    stats["skipped_zero_amt"] = int(zero_amt)
    df = df[df["DCPMTAMT_NUM"] != 0].copy()

    # Drop deleted/voided payments (DDELETED not null).
    # These were reversed in SiteLink and should not appear in the migrated ledger.
    if "DDELETED" in df.columns:
        deleted_mask = df["DDELETED"].notna() & (df["DDELETED"].str.strip() != "")
        stats["skipped_deleted"] = int(deleted_mask.sum())
        df = df[~deleted_mask].copy()

    # Drop credit payments — their PaymentID exists in Credits.csv, meaning
    # migrate_credits.py already imported them as negative ledger_charges.
    # Importing them here too would double-count the credit and push balances negative.
    if credit_payment_ids:
        df["_pid_str"] = df["PAYMENTID"].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() not in ("", "nan") else None
        )
        credit_mask = df["_pid_str"].isin(credit_payment_ids)
        stats["skipped_credits"] = int(credit_mask.sum())
        df = df[~credit_mask].drop(columns=["_pid_str"]).copy()
    logger.info(f"    Skipped credit payments  : {stats['skipped_credits']:,}")

    # Drop already-imported PaymentIDs
    df["ext_id"] = df["PAYMENTID"].apply(
        lambda x: PT.derive_external_id(x) if pd.notna(x) else None
    )
    df = df[df["ext_id"].notna()].copy()
    dup_mask = df["ext_id"].isin(existing_ids)
    stats["skipped_dup"] = int(dup_mask.sum())
    df = df[~dup_mask].copy()
    df = df.reset_index(drop=True)

    logger.info(f"    After pre-filter      : {len(df):,} rows to process")
    logger.info(f"    Skipped no-customer   : {stats['skipped_no_cust']:,}")
    logger.info(f"    Skipped zero-amount   : {stats['skipped_zero_amt']:,}")
    logger.info(f"    Skipped deleted/voided: {stats['skipped_deleted']:,}")
    logger.info(f"    Skipped credits       : {stats['skipped_credits']:,}")
    logger.info(f"    Skipped duplicates    : {stats['skipped_dup']:,}")
    print(f"  {len(df):,} rows to insert after filtering\n", flush=True)

    if dry_run:
        stats["inserted"] = len(df)
        logger.info(f"DRY RUN — {len(df):,} rows would be inserted.")
        return stats

    if output_mode == "excel":
        excel_records = []
        for row in df.itertuples(index=False):
            row_dict = row._asdict()
            customer_id = row_dict["customer_id"]
            try:
                record = PT.transform_row(pd.Series(row_dict), customer_id, org_id, loc_id, created_by)
                excel_records.append({k: record[k] for k in EXCEL_OUTPUT_COLUMNS if k in record})
                stats["inserted"] += 1
            except ValueError as exc:
                log_error(0, row_dict.get("ext_id"), "TRANSFORM", str(exc), row_dict.get("PAYMENTID"))
                stats["errors"] += 1
        if excel_records:
            import openpyxl
            pd.DataFrame(excel_records).to_excel(out_file, index=False)
            logger.info(f"Excel written: {out_file}")
        return stats

    # ── Step 2: Transform rows using itertuples (fast) ───────────────────────
    tuples = []
    for row in df.itertuples(index=False):
        row_dict = row._asdict()
        customer_id = row_dict["customer_id"]
        try:
            record = PT.transform_row(pd.Series(row_dict), customer_id, org_id, loc_id, created_by)
            if record["status"] == "FAILED":
                stats["failed_payments"] += 1
            tuples.append(tuple(record.get(c) for c in _INSERT_COLS))
        except ValueError as exc:
            log_error(0, row_dict.get("ext_id"), "TRANSFORM", str(exc), row_dict.get("PAYMENTID"))
            stats["errors"] += 1

    logger.info(f"    Transformed: {len(tuples):,} rows ready for insert")

    # ── Step 3: Bulk insert with execute_values (fast) ───────────────────────
    raw_conn = engine.raw_connection()
    cursor   = raw_conn.cursor()
    try:
        for i in range(0, len(tuples), batch_size):
            chunk = tuples[i:i + batch_size]
            execute_values(cursor, _INSERT_SQL, chunk, page_size=batch_size)
            raw_conn.commit()
            stats["inserted"] += len(chunk)
            pct = min(100, (i + len(chunk)) / len(tuples) * 100)
            print(f"  {pct:5.1f}%  |  inserted {stats['inserted']:,} / {len(tuples):,}", flush=True)
    except Exception as exc:
        raw_conn.rollback()
        logger.error(f"❌  Bulk insert failed: {exc}")
        raise
    finally:
        cursor.close()
        raw_conn.close()

    return stats


def _write_excel(records: list[dict], out_path: str):
    """Write transformed records to an Excel file for review."""
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Payments")
        ws = writer.sheets["Payments"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(c.value)) if c.value is not None else 0
                for c in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
    logger.info(f"📊  Excel output written: {out_path}  ({len(records):,} rows)")


# =============================================================================
# STEP 3 — Summary
# =============================================================================

def write_summary(stats: dict, run_ts: str):
    """Print and log the final migration summary."""
    from scripts.logger import LOG_FILE, ERROR_CSV, SUMMARY_FILE

    mode = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 65,
        "  STORENTIC PAYMENTS MIGRATION — SUMMARY",
        f"  Run timestamp     : {run_ts}",
        f"  Output mode       : {mode}",
        f"  Dry run           : {stats.get('dry_run', False)}",
        "=" * 65,
        f"  Total rows read           : {stats.get('total', 0):,}",
        f"  Successfully written       : {stats.get('inserted', 0):,}",
        f"    of which FAILED status   : {stats.get('failed_payments', 0):,}  (NSF)",
        f"  Skipped (credit payments) : {stats.get('skipped_credits', 0):,}  (already in ledger_charges via Credits.csv)",
        f"  Skipped (deleted/voided)  : {stats.get('skipped_deleted', 0):,}",
        f"  Skipped (already exist)   : {stats.get('skipped_dup', 0):,}",
        f"  Skipped (no customer)     : {stats.get('skipped_no_cust', 0):,}",
        f"  Skipped (zero amount)     : {stats.get('skipped_zero_amt', 0):,}",
        f"  Errors / rejected         : {stats.get('errors', 0):,}",
        "=" * 65,
    ]
    if stats.get("excel_output"):
        lines.append(f"  Excel output : {stats['excel_output']}")
    lines += [
        f"  Log file   : {LOG_FILE}",
        f"  Error CSV  : {ERROR_CSV}",
        "=" * 65,
    ]
    text = "\n".join(lines)
    print("\n" + text)
    logger.info(text)


# =============================================================================
# CLI
# =============================================================================

def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="SiteLink → Storentic Payment History Migration"
    )
    parser.add_argument(
        "--file-payments", required=True,
        help="Path to Payments.csv (SiteLink export)",
    )
    parser.add_argument(
        "--file-tenants", required=True,
        help="Path to Tenants+Units+Ledgers+Access.csv (SiteLink export)",
    )
    parser.add_argument(
        "--file-credits", default=None,
        help="Path to Credits.csv — PaymentIDs here are excluded from payments import "
             "(they are already loaded as negative ledger_charges by migrate_credits.py)",
    )
    parser.add_argument(
        "--output", default="db", choices=["db", "excel"],
        help="Output destination: 'db' (default) or 'excel'",
    )
    parser.add_argument(
        "--out-file", default=None,
        help="[excel mode] Output Excel file path",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="[db mode] Preview without writing to DB",
    )
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    run_ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_mode  = args.output
    dry_run      = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    org_id       = int(os.getenv("ORGANIZATION_ID", 1))
    loc_id       = int(os.getenv("LOCATION_ID", 1))
    created_by   = int(os.getenv("CREATED_BY", 0))
    batch_size   = int(os.getenv("BATCH_SIZE", 500))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"payments_transformed_{ts}.xlsx")

    print(
        f"\n  Storentic Payments Migration"
        f"\n  Mode: {output_mode.upper()}{'  [DRY RUN]' if dry_run else ''}  |"
        f"  org={org_id}  loc={loc_id}  created_by={created_by}\n",
        flush=True,
    )
    logger.info("=" * 65)
    logger.info("STORENTIC PAYMENTS ETL MIGRATION STARTING")
    logger.info(f"Payments file : {args.file_payments}")
    logger.info(f"Tenants file  : {args.file_tenants}")
    logger.info(f"Credits file  : {args.file_credits or 'not provided'}")
    logger.info(f"Output mode   : {output_mode.upper()}")
    logger.info(f"Dry run       : {dry_run}")
    logger.info(f"Org / Loc / CreatedBy: {org_id} / {loc_id} / {created_by}")
    logger.info(f"Batch size    : {batch_size}")
    logger.info("=" * 65)

    # ── Get DB engine (always needed for customer lookup, even in excel mode) ──
    try:
        from scripts.db import get_engine
        engine = get_engine()
        logger.info("Database connection established.")
    except Exception as exc:
        print(f"\n  ERROR: Cannot connect to database: {exc}\n", flush=True)
        logger.error(f"Cannot connect to database: {exc}")
        sys.exit(1)

    # ── Step 1: Build lookup maps ──────────────────────────────────────────────
    ledger_to_tenant   = build_ledger_to_tenant_map(args.file_tenants)
    tenant_to_customer = load_storentic_customers(engine, org_id)
    ledger_to_customer = build_ledger_to_customer_map(ledger_to_tenant, tenant_to_customer)

    # ── Step 2: Load credit PaymentIDs to exclude (if Credits.csv provided) ───
    credit_payment_ids: set[str] = set()
    if args.file_credits:
        credit_payment_ids = load_credit_payment_ids(args.file_credits)
    else:
        logger.warning("--file-credits not provided; credit payments will NOT be excluded.")
        print("  ⚠️   --file-credits not provided — credit payments (already in ledger_charges) "
              "will be imported as payments too. Pass --file-credits data/Credits.csv to avoid "
              "double-counting credits.\n", flush=True)

    # ── Step 3: Load existing external_ids for deduplication ──────────────────
    existing_ids: set[str] = set()
    if output_mode == "db" and not dry_run:
        existing_ids = load_existing_external_ids(engine)
    # In dry-run or excel mode we still de-dup within the run using an empty set

    # ── Step 4: Process payments ───────────────────────────────────────────────
    stats = process_payments(
        payments_file      = args.file_payments,
        ledger_to_customer = ledger_to_customer,
        org_id             = org_id,
        loc_id             = loc_id,
        created_by         = created_by,
        output_mode        = output_mode,
        dry_run            = dry_run,
        existing_ids       = existing_ids,
        engine             = engine,
        out_file           = out_file,
        batch_size         = batch_size,
        credit_payment_ids = credit_payment_ids,
    )

    # ── Step 4: Print summary ──────────────────────────────────────────────────
    write_summary(stats, run_ts)
    close_logger()


if __name__ == "__main__":
    main()
