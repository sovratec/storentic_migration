"""
migrate_customers.py — ETL script for loading legacy Customer data.

Usage (standalone):
    python migrate_customers.py --file data/CustomerUnitSample.csv --output db
    python migrate_customers.py --file data/CustomerUnitSample.csv --output db --dry-run
    python migrate_customers.py --file data/CustomerUnitSample.csv --output db --update
    python migrate_customers.py --file data/CustomerUnitSample.csv --output excel

Usage (via unified entry point — preferred) :
    python migrate.py --type customer --file data/CustomerUnitSample.csv [--output db|excel] [--dry-run] [--update]

Arguments:
    --file      Path to the source CSV file (required)
    --output    Destination: 'db' (default) or 'excel'
    --out-file  [excel mode only] Output Excel filename
    --dry-run   [db mode only] Preview without writing to DB
    --update    [db mode only] Update existing records instead of skipping duplicates

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — required, injected into every customer row
    LOCATION_ID       — required, injected into every customer row
    CREATED_BY        — user id recorded in created_by / updated_by
    DRY_RUN           — alternative to --dry-run flag
    UPDATE_EXISTING   — alternative to --update flag

Duplicate detection key: external_id (TenantId from Excel)

NOTE: For former/historical tenants use migrate_former_customers.py instead.
"""

import argparse
import os
import sys

import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, write_summary, close as close_logger
from scripts.validator import validate
from scripts import customer_transformer as T
from scripts import to_bigint

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Column short-key constants ─────────────────────────────────────────────────

COL_TENANT_ID   = "TENANTID"
COL_FNAME       = "SFNAME"
COL_MI          = "SMI"
COL_LNAME       = "SLNAME"
COL_COMPANY     = "SCOMPANY"
COL_ADDR1       = "SADDR1"
COL_ADDR2       = "SADDR2"
COL_CITY        = "SCITY"
COL_REGION      = "SREGION"
COL_POSTAL      = "SPOSTALCODE"
COL_COUNTRY     = "SCOUNTRY"
COL_PHONE       = "SPHONE"
COL_FNAME_ALT   = "SFNAMEALT"
COL_MI_ALT      = "SMIALT"
COL_LNAME_ALT   = "SLNAMEALT"
COL_ADDR1_ALT   = "SADDR1ALT"
COL_ADDR2_ALT   = "SADDR2ALT"
COL_CITY_ALT    = "SCITYALT"
COL_REGION_ALT  = "SREGIONALT"
COL_POSTAL_ALT  = "SPOSTALCODEALT"
COL_COUNTRY_ALT = "SCOUNTRYALT"
COL_PHONE_ALT   = "SPHONEALT"
COL_EMAIL       = "SEMAIL"
COL_EMAIL_ALT   = "SEMAILALT"
COL_MOBILE      = "SMOBILE"
COL_ACCESS_CODE = "SACCESSCODE"
COL_LICENSE     = "SLICENSE"
COL_LIC_REGION  = "SLICREGION"
COL_EMPLOYEE_ID = "EMPLOYEEID"


# ── Row transformer ────────────────────────────────────────────────────────────

def transform_row(row_idx: int, row: pd.Series, col: dict, org_id: int, loc_id: int, created_by: int = 0, external_source: str | None = None) -> dict | None:
    """
    Transform one Excel row into a dict ready for DB insertion.
    Returns None if the row must be skipped (critical field missing).
    """
    def get(key):
        col_name = col.get(key)
        return row.get(col_name) if col_name else None

    last_name   = T.clean_str(get(COL_LNAME))
    external_id = T.clean_str(get(COL_TENANT_ID))

    if not external_id:
        log_skipped(row_idx + 2, "UNKNOWN", COL_TENANT_ID, "TenantId is blank — row rejected", None)
        return None

    if not last_name:
        log_skipped(row_idx + 2, "UNKNOWN", COL_LNAME, "sLname is blank — row rejected", None)
        return None

    record = {
        "external_id":                  external_id,
        "external_source":              external_source,
        "external_employee_id":         to_bigint(row.get(COL_EMPLOYEE_ID)),
        "external_system":              "sitelink",
        "first_name":                   T.derive_full_name(get(COL_FNAME), get(COL_MI)),
        "last_name":                    last_name,
        "company_name":                 T.clean_str(get(COL_COMPANY)),
        "address1":                     T.clean_str(get(COL_ADDR1)),
        "address2":                     T.clean_str(get(COL_ADDR2)),
        "city":                         T.clean_str(get(COL_CITY)),
        "state":                        T.clean_str(get(COL_REGION)),
        "zip":                          T.clean_str(get(COL_POSTAL)),
        "country":                      T.clean_str(get(COL_COUNTRY)),
        "phone":                        T.clean_str(get(COL_PHONE)),
        "alternate_first_name":         T.derive_full_name(get(COL_FNAME_ALT), get(COL_MI_ALT)),
        "alternate_last_name":          T.clean_str(get(COL_LNAME_ALT)),
        "alternate_address1":           T.clean_str(get(COL_ADDR1_ALT)),
        "alternate_address2":           T.clean_str(get(COL_ADDR2_ALT)),
        "alternate_city":               T.clean_str(get(COL_CITY_ALT)),
        "alternate_state":              T.clean_str(get(COL_REGION_ALT)),
        "alternate_zip":                T.clean_str(get(COL_POSTAL_ALT)),
        "alternate_country":            T.clean_str(get(COL_COUNTRY_ALT)),
        "alternate_phone":              T.clean_str(get(COL_PHONE_ALT)),
        "email":                        T.clean_str(get(COL_EMAIL)),
        "alternate_email":              T.clean_str(get(COL_EMAIL_ALT)),
        "mobile":                       T.clean_str(get(COL_MOBILE)),
        "access_gate_code":             T.clean_str(get(COL_ACCESS_CODE)),
        "driver_license_id":            T.clean_str(get(COL_LICENSE)),
        "driver_license_issue_state":   T.clean_str(get(COL_LIC_REGION)),
        # Context from .env — not in Excel
        "organization_id":              org_id,
        "location_id":                  loc_id,
        "customer_status_id":           1,        # 1 = Active customer
        # Audit fields
        "created_by":                   created_by,
        "updated_by":                   created_by,
        "created_datetime":             datetime.now(timezone.utc),
        "updated_datetime":             datetime.now(timezone.utc),
    }
    return record


# ── Duplicate check (bulk — loads all existing ids once) ──────────────────────

def load_existing_external_ids(engine, org_id: int) -> set[str]:
    """Load all existing external_ids for the org in one query."""
    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT external_id FROM storentic.customer "
            "WHERE external_id IS NOT NULL AND organization_id = :org"
        ), {"org": org_id}).fetchall()
    ids = {r.external_id for r in rows}
    logger.info(f"    Already in DB: {len(ids):,} customers with external_id")
    return ids


# ── SQL statements ─────────────────────────────────────────────────────────────

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
        first_name                  = :first_name,
        last_name                   = :last_name,
        company_name                = :company_name,
        address1                    = :address1,
        address2                    = :address2,
        city                        = :city,
        state                       = :state,
        zip                         = :zip,
        country                     = :country,
        phone                       = :phone,
        alternate_first_name        = :alternate_first_name,
        alternate_last_name         = :alternate_last_name,
        alternate_address1          = :alternate_address1,
        alternate_address2          = :alternate_address2,
        alternate_city              = :alternate_city,
        alternate_state             = :alternate_state,
        alternate_zip               = :alternate_zip,
        alternate_country           = :alternate_country,
        alternate_phone             = :alternate_phone,
        email                       = :email,
        alternate_email             = :alternate_email,
        mobile                      = :mobile,
        access_gate_code            = :access_gate_code,
        driver_license_id           = :driver_license_id,
        driver_license_issue_state  = :driver_license_issue_state,
        external_source             = :external_source,
        location_id                 = :location_id,
        updated_by                  = :updated_by,
        updated_datetime            = :updated_datetime
    WHERE external_id = :external_id
"""


# ── Excel output ───────────────────────────────────────────────────────────────

EXCEL_OUTPUT_COLUMNS = [
    "external_id",
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
]


def write_to_excel(records: list[dict], out_path: str):
    """Write transformed records to an Excel file."""
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Transformed Customers")

        ws = writer.sheets["Transformed Customers"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)

    logger.info(f"📊  Excel output written: {out_path}  ({len(records)} rows)")


# ── Argument parsing (shared with migrate.py) ──────────────────────────────────

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Storentic Customers ETL Migration")
    parser.add_argument("--file",     required=True,  help="Path to source CSV file")
    parser.add_argument("--output",   default="db",   choices=["db", "excel"],
                        help="Output destination: 'db' (default) or 'excel'")
    parser.add_argument("--out-file", default=None,   help="[excel mode] Output file path")
    parser.add_argument("--dry-run",  action="store_true", help="[db mode] Preview without writing to DB")
    parser.add_argument("--update",   action="store_true", help="[db mode] Update existing records instead of skipping")
    return parser.parse_args(args)


# ── Main ───────────────────────────────────────────────────────────────────────

def main(args=None):
    if args is None:
        args = parse_args()

    output_mode     = args.output
    dry_run         = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    update_existing = args.update  or os.getenv("UPDATE_EXISTING", "false").lower() == "true"
    org_id          = int(os.getenv("ORGANIZATION_ID", 1))
    loc_id          = int(os.getenv("LOCATION_ID", 1))
    created_by      = int(os.getenv("CREATED_BY", 0))
    external_source = os.getenv("EXTERNAL_SOURCE") or None

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"customers_transformed_{ts}.xlsx")

    logger.info("=" * 60)
    logger.info("  STORENTIC CUSTOMERS ETL MIGRATION STARTING")
    logger.info(f"  File          : {args.file}")
    logger.info(f"  Output mode   : {output_mode.upper()}")
    if output_mode == "db":
        logger.info(f"  Dry run       : {dry_run}")
        logger.info(f"  Update mode   : {update_existing}")
    else:
        logger.info(f"  Excel output  : {out_file}")
    logger.info(f"  Organization  : {org_id}")
    logger.info(f"  Location      : {loc_id}")
    logger.info(f"  Created by    : {created_by}")
    logger.info("=" * 60)

    # ── Step 1: Validate ──────────────────────────────────────────────────────
    df, col_map = validate(args.file, mode="customer")

    # ── Step 2: Set up DB only when needed ────────────────────────────────────
    engine = None
    if output_mode == "db" and not dry_run:
        try:
            from scripts.db import get_engine
            engine = get_engine()
            logger.info("✅  Database connection established.")
        except ImportError:
            logger.error("❌  sqlalchemy / psycopg2 not installed. Run: pip install sqlalchemy psycopg2-binary")
            sys.exit(1)

    # ── Step 3: Process rows ──────────────────────────────────────────────────
    stats = {
        "total": 0, "inserted": 0, "updated": 0,
        "skipped": 0, "errors": 0, "dry_run": dry_run,
        "output_mode": output_mode,
    }
    excel_records  = []
    new_records    = []
    update_records = []

    # Pre-load existing ids once (O(1) dedup instead of N per-row SELECTs)
    existing_ids: set[str] = set()
    if output_mode == "db" and not dry_run:
        existing_ids = load_existing_external_ids(engine, org_id)

    batch_size = int(os.getenv("BATCH_SIZE", 500))

    for row_idx, row in enumerate(df.itertuples(index=False)):
        stats["total"] += 1

        record = transform_row(row_idx, pd.Series(row._asdict()), col_map, org_id, loc_id, created_by, external_source)

        if record is None:
            stats["errors"] += 1
            continue

        external_id  = record["external_id"]
        display_name = f"{record.get('first_name', '')} {record.get('last_name', '')}".strip()

        # ── Excel output mode ─────────────────────────────────────────────────
        if output_mode == "excel":
            excel_records.append(record)
            stats["inserted"] += 1
            continue

        # ── DB dry run ────────────────────────────────────────────────────────
        if dry_run:
            logger.info(f"[DRY RUN] Would upsert customer: {display_name} (ext_id={external_id})")
            stats["inserted"] += 1
            continue

        # ── Classify: new vs update ───────────────────────────────────────────
        if external_id in existing_ids:
            if update_existing:
                update_records.append(record)
            else:
                stats["skipped"] += 1
        else:
            new_records.append(record)
            existing_ids.add(external_id)

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
        finally:
            raw_conn.close()
        stats["inserted"] = len(new_records)
        logger.info(f"✅  Bulk inserted {stats['inserted']:,} customers.")

    # ── Batch UPDATE existing records ─────────────────────────────────────────
    if update_records and not dry_run and output_mode == "db":
        from sqlalchemy import text as sa_text
        with engine.begin() as conn:
            for record in update_records:
                conn.execute(sa_text(_UPDATE_SQL), record)
        stats["updated"] = len(update_records)
        logger.info(f"🔄  Updated {stats['updated']:,} customers.")

    # ── Step 4: Flush Excel file if in excel mode ─────────────────────────────
    if output_mode == "excel":
        if excel_records:
            write_to_excel(excel_records, out_file)
            stats["excel_output"] = out_file
        else:
            logger.warning("⚠️   No records to write — Excel file not created.")

    # ── Step 5: Summary ───────────────────────────────────────────────────────
    write_summary(stats)
    close_logger()


if __name__ == "__main__":
    main()
