"""
migrate_former_customers.py
===========================
ETL script: SiteLink "All Former Tenants" Excel export -> storentic.customer

Source format (differs from active-tenant export  used by migrate_customers.py):
    LOCATIONCODE, SITENAME, UNITNAME, SALUTATION,
    FIRSTNAME, MI, LASTNAME, NAME, COMPANY,
    ADDRESS1, ADDRESS2, CITY, STATE, ZIPCODE,
    EMAIL, PHONE, MOBILE, GATECODE,
    BIRTHDATE, TAXID, DATEMOVEDIN, DATEMOVEDOUT, ACTIVELEDGERS

Key behaviours
--------------
1. Rows where ACTIVELEDGERS > 0 are written to
   output/active_ledgers_review.xlsx and excluded from import.
2. Remaining rows are deduplicated by (LASTNAME, FIRSTNAME, PHONE/MOBILE/EMAIL)
   keeping the row with the most recent DATEMOVEDOUT.
3. external_id = TenantId from the input file (same column used by active
   customers). Rows with a blank or missing TenantId are rejected.
4. Fully idempotent: re-runs skip rows whose external_id already exists in
   storentic.customer (checks both active and former customers).
5. Same INSERT/UPDATE SQL and storentic.customer table as migrate_customers.py.

Usage
-----
    # Dry run — shows counts, no DB writes
    python migrate_former_customers.py --file "data/FormerTenants.csv" --dry-run

    # Live import
    python migrate_former_customers.py --file "data/FormerTenants.csv"

    # Export to Excel for QA review before importing
    python migrate_former_customers.py --file "data/FormerTenants.csv" --output excel

Environment (.env)
------------------
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   -- injected into every row (default: 1)
    LOCATION_ID       -- injected into every row (default: 1)
    CREATED_BY        -- user id for audit fields (default: 0)
    DRY_RUN           -- "true" to preview without writing
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import text as sa_text

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, write_summary, close as close_logger
from scripts import customer_transformer as T
from scripts import to_bigint

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── SQL (same table as migrate_customers.py) ───────────────────────────────────

_INSERT_COLS = [
    "external_id", "external_source", "external_employee_id", "external_system",
    "first_name", "last_name", "company_name",
    "address1", "address2", "city", "state", "zip", "country", "phone",
    "alternate_first_name", "alternate_last_name",
    "alternate_address1", "alternate_address2",
    "alternate_city", "alternate_state", "alternate_zip",
    "alternate_country", "alternate_phone",
    "email", "alternate_email",
    "mobile", "access_gate_code",
    "driver_license_id", "driver_license_issue_state",
    "organization_id", "location_id",
    "customer_status_id",
    "created_by", "updated_by", "created_datetime", "updated_datetime",
]

_INSERT_SQL = """
    INSERT INTO storentic.customer (
        external_id, external_source, external_employee_id, external_system,
        first_name, last_name, company_name,
        address1, address2, city, state, zip, country, phone,
        alternate_first_name, alternate_last_name,
        alternate_address1, alternate_address2,
        alternate_city, alternate_state, alternate_zip,
        alternate_country, alternate_phone,
        email, alternate_email,
        mobile, access_gate_code,
        driver_license_id, driver_license_issue_state,
        organization_id, location_id,
        customer_status_id,
        created_by, updated_by, created_datetime, updated_datetime
    ) VALUES %s
    ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING
"""

_UPDATE_SQL = """
    UPDATE storentic.customer SET
        first_name       = :first_name,
        last_name        = :last_name,
        company_name     = :company_name,
        address1         = :address1,
        address2         = :address2,
        city             = :city,
        state            = :state,
        zip              = :zip,
        phone            = :phone,
        email            = :email,
        mobile           = :mobile,
        access_gate_code = :access_gate_code,
        external_source  = :external_source,
        location_id      = :location_id,
        updated_by       = :updated_by,
        updated_datetime = :updated_datetime
    WHERE external_id = :external_id
"""

EXCEL_OUTPUT_COLUMNS = [
    "external_id", "first_name", "last_name", "company_name",
    "address1", "address2", "city", "state", "zip",
    "phone", "mobile", "email", "access_gate_code",
    "organization_id", "location_id",
]


# ── Step 1: Load, split active/former, deduplicate ─────────────────────────────

def load_and_prepare(filepath: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read the Former Tenants CSV file.

    Returns:
        (former_df, active_df)
        - former_df : rows with ACTIVELEDGERS == 0
        - active_df : rows with ACTIVELEDGERS > 0, for review only

    Deduplication is handled at insert time by checking external_id against
    the customer table, not here.
    """
    logger.info(f"Loading file: {filepath}")
    df = pd.read_csv(filepath, dtype=str, encoding='latin-1')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()
    df = df.fillna("").apply(lambda c: c.str.strip() if c.dtype == "object" else c)

    required = {"FIRSTNAME", "LASTNAME", "ACTIVELEDGERS"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns in source file: {missing}")

    logger.info(f"    Total rows      : {len(df)}")

    active_mask = ~df["ACTIVELEDGERS"].isin(["", "0"])
    active_df   = df[active_mask].copy()
    former_df   = df[~active_mask].reset_index(drop=True)

    logger.info(f"    Active (AL > 0) : {len(active_df)}")
    logger.info(f"    Former (AL == 0): {len(former_df)}")

    return former_df, active_df


# ── Step 2: Row transformer ────────────────────────────────────────────────────

def _extract_tenant_id(row: pd.Series) -> str | None:
    """
    Case-insensitive lookup for the TenantId column.
    Returns the stripped string value, or None if the column is absent or blank.
    """
    for col in row.index:
        if col.strip().lower() == "tenantid":
            return T.clean_str(row[col]) or None
    return None


def transform_row(row: pd.Series, org_id: int, loc_id: int, created_by: int, external_source: str | None = None) -> dict:
    """Map one Former Tenants Excel row to a storentic.customer dict."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "external_id":                  _extract_tenant_id(row),
        "external_source":              external_source,
        "external_employee_id":         to_bigint(row.get("EMPLOYEEID")),
        "external_system":              "sitelink",
        "first_name":                   T.derive_full_name(row.get("FIRSTNAME"), row.get("MI")),
        "last_name":                    T.clean_str(row.get("LASTNAME")),
        "company_name":                 T.clean_str(row.get("COMPANY")),
        "address1":                     T.clean_str(row.get("ADDRESS1")),
        "address2":                     T.clean_str(row.get("ADDRESS2")),
        "city":                         T.clean_str(row.get("CITY")),
        "state":                        T.clean_str(row.get("STATE")),
        "zip":                          T.clean_str(row.get("ZIPCODE")),
        "country":                      None,
        "phone":                        T.clean_str(row.get("PHONE")),
        # Former tenant format has no alternate contact fields
        "alternate_first_name":         None,
        "alternate_last_name":          None,
        "alternate_address1":           None,
        "alternate_address2":           None,
        "alternate_city":               None,
        "alternate_state":              None,
        "alternate_zip":                None,
        "alternate_country":            None,
        "alternate_phone":              None,
        "email":                        T.clean_str(row.get("EMAIL")),
        "alternate_email":              None,
        "mobile":                       T.clean_str(row.get("MOBILE")),
        "access_gate_code":             T.clean_str(row.get("GATECODE")),
        "driver_license_id":            None,
        "driver_license_issue_state":   None,
        "organization_id":              org_id,
        "location_id":                  loc_id,
        "customer_status_id":           2,        # 2 = Former tenant
        "created_by":                   created_by,
        "updated_by":                   created_by,
        "created_datetime":             now,
        "updated_datetime":             now,
    }


# ── Step 3: Idempotency — load already-imported FT-* ids ──────────────────────

def load_existing_external_ids(engine) -> set[str]:
    """Load all non-null external_ids from the customer table for dedup."""
    logger.info("Loading existing external_ids from customer table ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT external_id FROM storentic.customer "
            "WHERE external_id IS NOT NULL"
        )).fetchall()
    ids = {r.external_id for r in rows}
    logger.info(f"    Already in DB: {len(ids)} customers with external_id")
    return ids


# ── Step 4: Main processing loop ───────────────────────────────────────────────

def process(
    df: pd.DataFrame,
    org_id: int,
    loc_id: int,
    created_by: int,
    output_mode: str,
    dry_run: bool,
    existing_ids: set[str],
    engine,
    out_file: str,
    external_source: str | None = None,
    batch_size: int = 500,
) -> dict:

    stats = {
        "total": 0, "inserted": 0, "updated": 0,
        "skipped_dup": 0, "errors": 0,
        "dry_run": dry_run, "output_mode": output_mode,
    }
    excel_records  = []
    new_records    = []
    update_records = []
    total = len(df)
    print(f"  Processing {total:,} former customers ...\n", flush=True)

    for row_idx, row in enumerate(df.itertuples(index=False)):
        stats["total"] += 1

        try:
            record = transform_row(pd.Series(row._asdict()), org_id, loc_id, created_by, external_source)
        except Exception as exc:
            log_error(row_idx + 2, getattr(row, "LASTNAME", ""), "TRANSFORM", str(exc), None)
            stats["errors"] += 1
            continue

        if not record.get("last_name"):
            log_skipped(row_idx + 2, "UNKNOWN", "LASTNAME", "last_name is blank — skipped", None)
            stats["errors"] += 1
            continue

        ext_id       = record["external_id"]
        display_name = f"{record.get('first_name') or ''} {record.get('last_name') or ''}".strip()

        if ext_id is None:
            log_skipped(row_idx + 2, display_name, "MISSING_TENANT_ID",
                        "TenantId is blank — cannot deduplicate, row skipped", None)
            stats["errors"] += 1
            continue

        if ext_id in existing_ids:
            stats["skipped_dup"] += 1
            continue

        # Excel mode
        if output_mode == "excel":
            excel_records.append({k: record[k] for k in EXCEL_OUTPUT_COLUMNS if k in record})
            existing_ids.add(ext_id)
            stats["inserted"] += 1
            continue

        # Dry run
        if dry_run:
            logger.debug(f"[DRY RUN] {display_name}  ext_id={ext_id}")
            stats["inserted"] += 1
            continue

        # Classify for bulk write
        new_records.append(record)
        existing_ids.add(ext_id)

        # Progress every batch_size rows
        if stats["total"] % batch_size == 0:
            pct = stats["total"] / total * 100
            print(
                f"  {pct:5.1f}%  |  {stats['total']:,}/{total:,}"
                f"  queued={len(new_records):,}"
                f"  skipped={stats['skipped_dup']:,}"
                f"  errors={stats['errors']:,}",
                flush=True,
            )

    # ── Bulk INSERT new records ───────────────────────────────────────────────
    if new_records and not dry_run and output_mode == "db":
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                execute_values(
                    cur,
                    _INSERT_SQL,
                    [tuple(r[c] for c in _INSERT_COLS) for r in new_records],
                    page_size=batch_size,
                )
            raw_conn.commit()
        except Exception as exc:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
        stats["inserted"] = len(new_records)
        logger.info(f"✅  Bulk inserted {stats['inserted']:,} former customers.")

    # ── Batch UPDATE existing records ─────────────────────────────────────────
    if update_records and not dry_run and output_mode == "db":
        with engine.begin() as conn:
            for record in update_records:
                conn.execute(sa_text(_UPDATE_SQL), record)
        stats["updated"] = len(update_records)
        logger.info(f"🔄  Updated {stats['updated']:,} former customers.")

    # Flush Excel
    if output_mode == "excel" and excel_records:
        _write_excel(excel_records, out_file)
        stats["excel_output"] = out_file

    return stats


def _write_excel(records: list[dict], out_path: str):
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Former Customers")
        ws = writer.sheets["Former Customers"]
        for col_cells in ws.columns:
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 45)
    print(f"  Excel written: {out_path}  ({len(records):,} rows)", flush=True)
    logger.info(f"Excel output: {out_path}  ({len(records):,} rows)")


def _write_active_review(df_active: pd.DataFrame, out_path: str):
    """Save active-ledger rows to a separate Excel file for manual review."""
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_active.to_excel(writer, index=False, sheet_name="Active Ledgers")
        ws = writer.sheets["Active Ledgers"]
        for col_cells in ws.columns:
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)
    print(f"  Active-ledger review: {out_path}  ({len(df_active):,} rows)", flush=True)
    logger.info(f"Active review file: {out_path}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args(args=None):
    p = argparse.ArgumentParser(
        description="SiteLink Former Tenants -> storentic.customer migration"
    )
    p.add_argument("--file",     required=True, help="Path to Former Tenants Excel file")
    p.add_argument("--output",   default="db",  choices=["db", "excel"],
                   help="'db' (default) or 'excel'")
    p.add_argument("--out-file", default=None,  help="[excel mode] Output file path")
    p.add_argument("--dry-run",  action="store_true",
                   help="Preview counts without writing to DB")
    return p.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    output_mode     = args.output
    dry_run         = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    org_id          = int(os.getenv("ORGANIZATION_ID", 1))
    loc_id          = int(os.getenv("LOCATION_ID", 1))
    created_by      = int(os.getenv("CREATED_BY", 0))
    external_source = os.getenv("EXTERNAL_SOURCE") or None

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"former_customers_{ts}.xlsx")
    active_review_file = os.path.join(OUTPUT_DIR, "active_ledgers_review.xlsx")

    print(
        f"\n  Former Customers Migration"
        f"\n  Mode: {output_mode.upper()}{'  [DRY RUN]' if dry_run else ''}"
        f"  |  org={org_id}  loc={loc_id}  created_by={created_by}\n",
        flush=True,
    )
    logger.info("=" * 65)
    logger.info("FORMER CUSTOMERS ETL MIGRATION STARTING")
    logger.info(f"File        : {args.file}")
    logger.info(f"Output mode : {output_mode.upper()}")
    logger.info(f"Dry run     : {dry_run}")
    logger.info(f"Org/Loc/By  : {org_id}/{loc_id}/{created_by}")
    logger.info(f"Ext source  : {external_source or '(none)'}")
    logger.info("=" * 65)

    # Step 1: Load + split + dedup
    former_df, active_df = load_and_prepare(args.file)
    _write_active_review(active_df, active_review_file)

    # Step 2: DB connection
    engine = None
    if output_mode == "db" and not dry_run:
        try:
            from scripts.db import get_engine
            engine = get_engine()
            logger.info("Database connection established.")
        except Exception as exc:
            print(f"\n  ERROR: Cannot connect to DB: {exc}\n", flush=True)
            sys.exit(1)

    # Step 3: Load existing ids for idempotency
    existing_ids: set[str] = set()
    if output_mode == "db" and not dry_run:
        existing_ids = load_existing_external_ids(engine)

    # Step 4: Process
    batch_size = int(os.getenv("BATCH_SIZE", 500))
    stats = process(
        df              = former_df,
        org_id          = org_id,
        loc_id          = loc_id,
        created_by      = created_by,
        output_mode     = output_mode,
        dry_run         = dry_run,
        existing_ids    = existing_ids,
        engine          = engine,
        out_file        = out_file,
        external_source = external_source,
        batch_size      = batch_size,
    )

    # Step 5: Summary
    from scripts.logger import LOG_FILE, ERROR_CSV
    lines = [
        "=" * 65,
        "  FORMER CUSTOMERS MIGRATION — SUMMARY",
        f"  Run timestamp  : {ts}",
        f"  Output mode    : {output_mode.upper()}",
        f"  Dry run        : {dry_run}",
        "=" * 65,
        f"  Total rows processed       : {stats['total']:,}",
        f"  Inserted                   : {stats['inserted']:,}",
        f"  Updated (already existed)  : {stats['updated']:,}",
        f"  Skipped (already imported) : {stats['skipped_dup']:,}",
        f"  Errors / rejected          : {stats['errors']:,}",
        "=" * 65,
        f"  Active-ledger review : {active_review_file}",
        f"  Log file             : {LOG_FILE}",
        f"  Error CSV            : {ERROR_CSV}",
        "=" * 65,
    ]
    if stats.get("excel_output"):
        lines.insert(-1, f"  Excel output : {stats['excel_output']}")

    text = "\n".join(lines)
    print("\n" + text, flush=True)
    logger.info(text)
    close_logger()


if __name__ == "__main__":
    main()
