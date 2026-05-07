"""
migrate_ledger_charges.py — ETL script: SiteLink Select Charges → storentic.ledger_charges

Usage:
    # Dry run (preview row counts, no DB writes)
    python migrate_ledger_charges.py \\
        --file-charges  "data/Powdersville Self Storage - Select Charges_20260505.xlsx" \\
        --file-tenants  "data/Tenants_Units_Ledgers_Access_20260502.xlsx" \\
        --file-charge-types data/ChargeType.xlsx \\
        --output db --dry-run

    # Export to Excel for review before DB import
    python migrate_ledger_charges.py \\
        --file-charges  "data/Powdersville Self Storage - Select Charges_20260505.xlsx" \\
        --file-tenants  "data/Tenants_Units_Ledgers_Access_20260502.xlsx" \\
        --file-charge-types data/ChargeType.xlsx \\
        --output excel

    # Production import
    python migrate_ledger_charges.py \\
        --file-charges  "data/Powdersville Self Storage - Select Charges_20260505.xlsx" \\
        --file-tenants  "data/Tenants_Units_Ledgers_Access_20260502.xlsx" \\
        --file-charge-types data/ChargeType.xlsx \\
        --output db

Arguments:
    --file-charges      Path to SiteLink Select Charges Excel file (required)
    --file-tenants      Path to Tenants+Units+Ledgers+Access Excel file (required)
    --file-charge-types Path to ChargeType.xlsx mapping file (required)
    --output            'db' (default) or 'excel'
    --out-file          [excel mode] Output Excel file path
    --dry-run           [db mode] Preview without writing to DB

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — written to every ledger_charge row
    LOCATION_ID       — scopes unit/customer lookups and written to every row
    CREATED_BY        — user ID written to created_by column
    DRY_RUN           — alternative to --dry-run flag
    BATCH_SIZE        — rows per DB commit batch (default: 500)

Pre-requisite:
    Run sql/V7__ledger_charges_external_charge_id.sql against the target DB
    before running this script.

Join chain (how SiteLink charges map to Storentic records):
    Select Charges     LedgerID
           |
           v  (via Tenants file)
       TenantID  ──────────────────► storentic.customer (external_id) → customer_id
       sUnitName ──────────────────► storentic.units (unit_number)    → unit_id
           |
    ChargeDescID ──────────────────► ChargeType.xlsx                  → charge_type_id

Status logic:
    bNSF = True OR dDeleted not null  →  REVERSED
    otherwise                         →  POSTED

Skipped rows (written to output/ledger_charges_skipped_<ts>.xlsx):
    - LedgerID not found in Tenants file / no matching customer
    - ChargeDescID not in ChargeType.xlsx mapping
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text as sa_text
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, close as close_logger

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── SQL ────────────────────────────────────────────────────────────────────────

# Used by execute_values — %s is the multi-row placeholder
_INSERT_SQL = """
    INSERT INTO storentic.ledger_charges (
        customer_id, unit_id, location_id, organization_id,
        charge_type_id, amount_in_cents, effective_date,
        memo, internal_note, status, invoice_id,
        source_screen, reversed_by_id, reversed_at,
        reversal_reason, created_by, created_at, updated_at,
        external_charge_id
    ) VALUES %s
    ON CONFLICT (external_charge_id) DO NOTHING
"""

# Column order must match _INSERT_SQL above
_INSERT_COLS = [
    "customer_id", "unit_id", "location_id", "organization_id",
    "charge_type_id", "amount_in_cents", "effective_date",
    "memo", "internal_note", "status", "invoice_id",
    "source_screen", "reversed_by_id", "reversed_at",
    "reversal_reason", "created_by", "created_at", "updated_at",
    "external_charge_id",
]

EXCEL_OUTPUT_COLUMNS = [
    "external_charge_id", "customer_id", "unit_id", "location_id",
    "organization_id", "charge_type_id", "amount_in_cents",
    "effective_date", "memo", "internal_note", "status",
    "reversed_at", "reversal_reason", "source_screen",
    "created_by", "created_at", "updated_at",
]

SKIPPED_COLUMNS = [
    "ChargeID", "ChargeDescID", "LedgerID", "dcAmt",
    "dChgStrt", "dCreated", "skip_reason",
]


# =============================================================================
# STEP 1 — Build lookup maps from source files
# =============================================================================

def load_charge_type_map(charge_types_file: str) -> dict:
    """
    Read ChargeType.xlsx and return {ChargeDescID (int): charge_type_id (int)}.
    Rows with no id or no ChargeDescID are ignored (summary/total rows).
    """
    logger.info(f"📂  Loading charge type mapping: {charge_types_file}")
    df = pd.read_excel(charge_types_file)

    mapping = {}
    for _, row in df.iterrows():
        if pd.isna(row.get("id")) or pd.isna(row.get("ChargeDescID")):
            continue
        try:
            desc_id = int(float(row["ChargeDescID"]))
            ct_id   = int(float(row["id"]))
            mapping[desc_id] = ct_id
        except (ValueError, TypeError):
            continue

    logger.info(f"    ChargeDescID mappings loaded: {len(mapping)}")
    return mapping


def load_tenants_maps(tenants_file: str) -> tuple[dict, dict]:
    """
    Read Tenants file and return two dicts:
        ledger_to_tenant   : {LedgerID (int): TenantID (int)}
        ledger_to_unitname : {LedgerID (int): sUnitName (str)}
    """
    logger.info(f"📂  Loading tenants file: {tenants_file}")
    df = pd.read_excel(tenants_file, dtype=str)
    df.columns = df.columns.str.strip()

    required = {"LedgerID", "TenantID", "sUnitName"}
    missing  = required - set(df.columns)
    if missing:
        raise SystemExit(f"❌  Tenants file missing columns: {missing}")

    ledger_to_tenant   = {}
    ledger_to_unitname = {}
    skipped = 0

    for _, row in df.iterrows():
        try:
            ledger_id = int(float(row["LedgerID"]))
            tenant_id = int(float(row["TenantID"]))
            unit_name = str(row["sUnitName"]).strip() if pd.notna(row["sUnitName"]) else None
            ledger_to_tenant[ledger_id]   = tenant_id
            ledger_to_unitname[ledger_id] = unit_name
        except (ValueError, TypeError):
            skipped += 1

    logger.info(
        f"    Rows read        : {len(df)}\n"
        f"    LedgerID entries : {len(ledger_to_tenant)}\n"
        f"    Skipped bad rows : {skipped}"
    )
    return ledger_to_tenant, ledger_to_unitname


# =============================================================================
# STEP 2 — DB lookup maps
# =============================================================================

def check_external_charge_id_column(engine):
    """Verify V7 SQL migration has been applied."""
    with engine.connect() as conn:
        col_exists = conn.execute(sa_text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'storentic' "
            "  AND table_name   = 'ledger_charges' "
            "  AND column_name  = 'external_charge_id'"
        )).fetchone()

    if not col_exists:
        raise SystemExit(
            "\n❌  Column 'external_charge_id' does not exist on storentic.ledger_charges.\n\n"
            "    Run the V7 SQL migration first:\n\n"
            "      psql -h <DB_HOST> -U <DB_USER> -d storentic \\\n"
            "           -f sql/V7__ledger_charges_external_charge_id.sql\n\n"
            "    Or paste into DBeaver / pgAdmin:\n\n"
            "      ALTER TABLE storentic.ledger_charges\n"
            "          ADD COLUMN IF NOT EXISTS external_charge_id VARCHAR(100);\n\n"
            "      CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_charges_external_charge_id\n"
            "          ON storentic.ledger_charges(external_charge_id)\n"
            "          WHERE external_charge_id IS NOT NULL;\n"
        )


def load_customer_map(engine, org_id: int) -> dict:
    """{TenantID (int): customer.id (int)} for all migrated customers."""
    logger.info(f"🔍  Loading customer map for org_id={org_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, external_id FROM storentic.customer "
            "WHERE external_id IS NOT NULL AND organization_id = :org_id"
        ), {"org_id": org_id}).fetchall()

    mapping = {}
    for row in rows:
        try:
            mapping[int(float(row.external_id))] = row.id
        except (ValueError, TypeError):
            pass

    logger.info(f"    Customers loaded: {len(mapping)}")
    return mapping


def load_unit_map(engine, loc_id: int) -> dict:
    """{unit_number (str): units.id (int)} for the given location."""
    logger.info(f"🔍  Loading unit map for location_id={loc_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, unit_number FROM storentic.units WHERE location_id = :loc"
        ), {"loc": loc_id}).fetchall()

    mapping = {str(row.unit_number).strip(): row.id for row in rows}
    logger.info(f"    Units loaded: {len(mapping)}")
    return mapping


def load_existing_external_ids(engine) -> set:
    """All external_charge_id values already in ledger_charges (for idempotency)."""
    logger.info("🔍  Loading existing external_charge_ids ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT external_charge_id FROM storentic.ledger_charges "
            "WHERE external_charge_id IS NOT NULL"
        )).fetchall()
    ids = {row.external_charge_id for row in rows}
    logger.info(f"    Already imported: {len(ids)} charges")
    return ids


# =============================================================================
# STEP 3 — Row transformation
# =============================================================================

def _parse_dt(val):
    """Parse a datetime value from pandas, return None if null/NaT/unparseable."""
    if val is None:
        return None
    # pd.isna handles NaN, NaT, and None; guard with try for non-scalar types
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    try:
        ts = pd.to_datetime(val)
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _to_cents(val) -> int:
    """Convert a dollar amount to integer cents. Returns 0 for null/invalid."""
    try:
        return int(round(float(val) * 100))
    except (TypeError, ValueError):
        return 0


def transform_row(row: pd.Series, customer_id: int, unit_id, charge_type_id: int,
                  loc_id: int, org_id: int, created_by: int) -> dict:
    """Transform one Select Charges row into a ledger_charges insert dict."""

    charge_id   = str(int(float(row["ChargeID"])))
    dc_amt      = row.get("dcAmt")
    d_chg_strt  = _parse_dt(row.get("dChgStrt"))
    d_created   = _parse_dt(row.get("dCreated"))
    d_updated   = _parse_dt(row.get("dUpdated"))
    d_deleted   = _parse_dt(row.get("dDeleted"))
    b_nsf       = bool(row.get("bNSF", False))
    now         = datetime.utcnow()

    # Status logic
    is_reversed = b_nsf or (d_deleted is not None)
    status      = "REVERSED" if is_reversed else "POSTED"

    # Reversal metadata
    reversed_at     = None
    reversal_reason = None
    if is_reversed:
        if d_deleted is not None:
            reversed_at = d_deleted
        else:
            reversed_at = d_created or now
        reversal_reason = "NSF" if b_nsf else "Deleted in SiteLink"

    return {
        "external_charge_id": charge_id,
        "customer_id":        customer_id,
        "unit_id":            unit_id,
        "location_id":        loc_id,
        "organization_id":    org_id,
        "charge_type_id":     charge_type_id,
        "amount_in_cents":    _to_cents(dc_amt),
        "effective_date":     d_chg_strt or d_created or now,
        "memo":               "Migrated from SiteLink",
        "internal_note":      None,
        "status":             status,
        "invoice_id":         None,
        "source_screen":      "MIGRATION",
        "reversed_by_id":     None,
        "reversed_at":        reversed_at,
        "reversal_reason":    reversal_reason,
        "created_by":         created_by,
        "created_at":         d_created or now,
        "updated_at":         d_updated or now,
    }


# =============================================================================
# STEP 4 — Main processing loop
# =============================================================================

def _record_to_tuple(record: dict) -> tuple:
    """Convert a record dict to an ordered tuple matching _INSERT_COLS."""
    return tuple(record[col] for col in _INSERT_COLS)


def process_charges(
    charges_file: str,
    charge_type_map: dict,
    ledger_to_tenant: dict,
    ledger_to_unitname: dict,
    customer_map: dict,
    unit_map: dict,
    existing_ids: set,
    loc_id: int,
    org_id: int,
    created_by: int,
    output_mode: str,
    dry_run: bool,
    engine,
    out_file: str,
    batch_size: int,
) -> dict:

    # ── Read source Excel ──────────────────────────────────────────────────────
    logger.info(f"📂  Loading charges file: {charges_file}")
    df = pd.read_excel(charges_file, engine="openpyxl",
                       dtype={"ChargeID": "float64", "ChargeDescID": "float64",
                              "LedgerID": "float64", "dcAmt": "float64",
                              "bNSF": "bool", "bMoveIn": "bool", "bMoveOut": "bool"})
    df.columns = df.columns.str.strip()
    df = df[df["ChargeID"].notna()].copy()
    logger.info(f"    Charge rows to process: {len(df):,}")
    print(f"\n  Processing {len(df):,} rows — batch size {batch_size:,}\n", flush=True)

    stats = {
        "total": 0, "inserted": 0, "skipped_dup": 0,
        "skipped_no_cust": 0, "skipped_no_ct": 0,
        "errors": 0, "dry_run": dry_run, "output_mode": output_mode,
    }

    excel_records = []
    skipped_rows  = []
    batch_tuples  = []   # list of tuples for execute_values
    batch_ids     = []   # parallel list of charge_ids for logging on error

    # ── Get raw psycopg2 connection once — reused for all batches ─────────────
    raw_conn   = None
    raw_cursor = None
    if output_mode == "db" and not dry_run:
        raw_conn   = engine.raw_connection()
        raw_cursor = raw_conn.cursor()

    def flush_batch():
        if not batch_tuples:
            return
        try:
            execute_values(raw_cursor, _INSERT_SQL, batch_tuples, page_size=batch_size)
            raw_conn.commit()
            stats["inserted"] += len(batch_tuples)
            logger.info(f"    Flushed batch: {len(batch_tuples):,} rows committed.")
        except Exception as exc:
            raw_conn.rollback()
            exc_str = str(exc)
            is_conn_err = any(kw in exc_str for kw in (
                "could not translate host name", "could not connect to server",
                "connection refused", "OperationalError", "timeout",
            ))
            if is_conn_err:
                logger.error(f"DB connection lost: {exc_str[:200]}")
                print(f"\n  ERROR: DB connection lost — re-run to resume.\n", flush=True)
                raise SystemExit(1)
            # Data error — fall back to row-by-row for this batch only
            logger.warning(f"Batch insert failed ({exc_str[:120]}); retrying row-by-row ...")
            for tup, cid in zip(batch_tuples, batch_ids):
                try:
                    execute_values(raw_cursor, _INSERT_SQL, [tup])
                    raw_conn.commit()
                    stats["inserted"] += 1
                except Exception as row_exc:
                    raw_conn.rollback()
                    log_error(0, cid, "DB_INSERT", str(row_exc), cid)
                    stats["errors"] += 1
        finally:
            batch_tuples.clear()
            batch_ids.clear()

    # ── Main row loop ──────────────────────────────────────────────────────────
    for row_idx, row in df.iterrows():
        stats["total"] += 1
        excel_row = row_idx + 2

        # ── Parse key IDs ─────────────────────────────────────────────────────
        try:
            charge_id      = str(int(row["ChargeID"]))
            charge_desc_id = int(row["ChargeDescID"])
            ledger_id      = int(row["LedgerID"])
        except (ValueError, TypeError) as exc:
            log_error(excel_row, row.get("ChargeID"), "PARSE", str(exc), row.get("ChargeID"))
            stats["errors"] += 1
            continue

        # ── Dedup (in-memory — ON CONFLICT also handles DB-level) ────────────
        if charge_id in existing_ids:
            stats["skipped_dup"] += 1
            continue

        # ── Resolve charge_type_id ────────────────────────────────────────────
        charge_type_id = charge_type_map.get(charge_desc_id)
        if charge_type_id is None:
            reason = f"ChargeDescID {charge_desc_id} not in charge_type mapping"
            skipped_rows.append({
                "ChargeID": charge_id, "ChargeDescID": charge_desc_id,
                "LedgerID": ledger_id, "dcAmt": row.get("dcAmt"),
                "dChgStrt": row.get("dChgStrt"), "dCreated": row.get("dCreated"),
                "skip_reason": reason,
            })
            stats["skipped_no_ct"] += 1
            continue

        # ── Resolve customer_id ───────────────────────────────────────────────
        tenant_id   = ledger_to_tenant.get(ledger_id)
        customer_id = customer_map.get(tenant_id) if tenant_id else None
        if customer_id is None:
            reason = f"LedgerID {ledger_id} → TenantID {tenant_id} not found in storentic.customer"
            skipped_rows.append({
                "ChargeID": charge_id, "ChargeDescID": charge_desc_id,
                "LedgerID": ledger_id, "dcAmt": row.get("dcAmt"),
                "dChgStrt": row.get("dChgStrt"), "dCreated": row.get("dCreated"),
                "skip_reason": reason,
            })
            stats["skipped_no_cust"] += 1
            continue

        # ── Resolve unit_id (nullable) ────────────────────────────────────────
        unit_name = ledger_to_unitname.get(ledger_id)
        unit_id   = unit_map.get(unit_name) if unit_name else None

        # ── Transform ─────────────────────────────────────────────────────────
        try:
            record = transform_row(row, customer_id, unit_id, charge_type_id,
                                   loc_id, org_id, created_by)
        except Exception as exc:
            log_error(excel_row, charge_id, "TRANSFORM", str(exc), charge_id)
            stats["errors"] += 1
            continue

        # ── Route ─────────────────────────────────────────────────────────────
        if output_mode == "excel":
            excel_records.append({k: record[k] for k in EXCEL_OUTPUT_COLUMNS if k in record})
            existing_ids.add(charge_id)
            stats["inserted"] += 1

        elif dry_run:
            stats["inserted"] += 1

        else:
            batch_tuples.append(_record_to_tuple(record))
            batch_ids.append(charge_id)
            existing_ids.add(charge_id)
            if len(batch_tuples) >= batch_size:
                flush_batch()

        # ── Progress ──────────────────────────────────────────────────────────
        if stats["total"] % 10_000 == 0:
            pct = stats["total"] / len(df) * 100
            print(
                f"  {pct:5.1f}%  |  {stats['total']:,}/{len(df):,}"
                f"  inserted={stats['inserted']:,}"
                f"  skip_cust={stats['skipped_no_cust']:,}"
                f"  skip_ct={stats['skipped_no_ct']:,}"
                f"  err={stats['errors']:,}",
                flush=True,
            )

    # ── Flush remainder and close connection ──────────────────────────────────
    if output_mode == "db" and not dry_run:
        flush_batch()
        raw_cursor.close()
        raw_conn.close()

    # ── Write Excel outputs ───────────────────────────────────────────────────
    if output_mode == "excel" and excel_records:
        _write_excel(excel_records, out_file, EXCEL_OUTPUT_COLUMNS, "LedgerCharges")
        stats["excel_output"] = out_file

    if skipped_rows:
        ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
        skipped_path = os.path.join(OUTPUT_DIR, f"ledger_charges_skipped_{ts}.xlsx")
        _write_excel(skipped_rows, skipped_path, SKIPPED_COLUMNS, "Skipped")
        stats["skipped_output"] = skipped_path
        print(f"\n  ⚠️   {len(skipped_rows):,} skipped rows → {skipped_path}", flush=True)

    return stats


def _write_excel(records: list, out_path: str, columns: list, sheet_name: str):
    df_out = pd.DataFrame(records, columns=[c for c in columns if c in (records[0] if records else {})])
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max(
                len(str(c.value)) if c.value is not None else 0
                for c in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
    logger.info(f"📊  Excel written: {out_path}  ({len(records):,} rows)")


# =============================================================================
# STEP 5 — Summary
# =============================================================================

def _print_summary(stats: dict, run_ts: str):
    from scripts.logger import LOG_FILE, ERROR_CSV
    mode  = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 65,
        "  STORENTIC LEDGER CHARGES MIGRATION — SUMMARY",
        f"  Run timestamp     : {run_ts}",
        f"  Output mode       : {mode}",
        f"  Dry run           : {stats.get('dry_run', False)}",
        "=" * 65,
        f"  Total rows read             : {stats.get('total', 0):,}",
        f"  Successfully written        : {stats.get('inserted', 0):,}",
        f"  Skipped (already imported)  : {stats.get('skipped_dup', 0):,}",
        f"  Skipped (no customer match) : {stats.get('skipped_no_cust', 0):,}",
        f"  Skipped (no charge type)    : {stats.get('skipped_no_ct', 0):,}",
        f"  Errors / rejected           : {stats.get('errors', 0):,}",
        "=" * 65,
    ]
    if stats.get("excel_output"):
        lines.append(f"  Excel output   : {stats['excel_output']}")
    if stats.get("skipped_output"):
        lines.append(f"  Skipped rows   : {stats['skipped_output']}")
    lines += [
        f"  Log file       : {LOG_FILE}",
        f"  Error CSV      : {ERROR_CSV}",
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
        description="SiteLink Select Charges → Storentic Ledger Charges Migration"
    )
    parser.add_argument("--file-charges",      required=True,
                        help="Path to SiteLink Select Charges Excel file")
    parser.add_argument("--file-tenants",      required=True,
                        help="Path to Tenants+Units+Ledgers+Access Excel file")
    parser.add_argument("--file-charge-types", required=True,
                        help="Path to ChargeType.xlsx mapping file")
    parser.add_argument("--output",   default="db", choices=["db", "excel"],
                        help="Output destination: 'db' (default) or 'excel'")
    parser.add_argument("--out-file", default=None,
                        help="[excel mode] Output Excel file path")
    parser.add_argument("--dry-run",  action="store_true",
                        help="[db mode] Preview without writing to DB")
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    run_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_mode = args.output
    dry_run     = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    org_id      = int(os.getenv("ORGANIZATION_ID", 1))
    loc_id      = int(os.getenv("LOCATION_ID", 1))
    created_by  = int(os.getenv("CREATED_BY", 0))
    batch_size  = int(os.getenv("BATCH_SIZE", 5000))

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"ledger_charges_transformed_{ts}.xlsx")

    print(
        f"\n  Storentic Ledger Charges Migration"
        f"\n  Mode: {output_mode.upper()}{'  [DRY RUN]' if dry_run else ''}  |"
        f"  org={org_id}  loc={loc_id}  created_by={created_by}\n",
        flush=True,
    )
    logger.info("=" * 65)
    logger.info("STORENTIC LEDGER CHARGES ETL MIGRATION STARTING")
    logger.info(f"Charges file      : {args.file_charges}")
    logger.info(f"Tenants file      : {args.file_tenants}")
    logger.info(f"Charge types file : {args.file_charge_types}")
    logger.info(f"Output mode       : {output_mode.upper()}")
    logger.info(f"Dry run           : {dry_run}")
    logger.info(f"Org / Loc / CreatedBy : {org_id} / {loc_id} / {created_by}")
    logger.info(f"Batch size        : {batch_size}")
    logger.info("=" * 65)

    # ── DB connection ─────────────────────────────────────────────────────────
    try:
        from scripts.db import get_engine
        engine = get_engine()
        logger.info("✅  Database connection established.")
    except Exception as exc:
        print(f"\n  ERROR: Cannot connect to database: {exc}\n", flush=True)
        sys.exit(1)

    # ── Pre-flight: check column exists ───────────────────────────────────────
    check_external_charge_id_column(engine)

    # ── Load source file maps ─────────────────────────────────────────────────
    charge_type_map                    = load_charge_type_map(args.file_charge_types)
    ledger_to_tenant, ledger_to_unit   = load_tenants_maps(args.file_tenants)

    # ── Load DB lookup maps ───────────────────────────────────────────────────
    customer_map = load_customer_map(engine, org_id)
    unit_map     = load_unit_map(engine, loc_id)
    existing_ids = load_existing_external_ids(engine)

    # ── Process ───────────────────────────────────────────────────────────────
    stats = process_charges(
        charges_file        = args.file_charges,
        charge_type_map     = charge_type_map,
        ledger_to_tenant    = ledger_to_tenant,
        ledger_to_unitname  = ledger_to_unit,
        customer_map        = customer_map,
        unit_map            = unit_map,
        existing_ids        = existing_ids,
        loc_id              = loc_id,
        org_id              = org_id,
        created_by          = created_by,
        output_mode         = output_mode,
        dry_run             = dry_run,
        engine              = engine,
        out_file            = out_file,
        batch_size          = batch_size,
    )

    _print_summary(stats, run_ts)
    close_logger()


if __name__ == "__main__":
    main()
