"""
migrate_receipts.py — ETL script: SiteLink Receipts CSV → storentic.receipts

Usage:
    # Dry run (preview row counts, no DB writes)
    python migrate_receipts.py \\
        --file-receipts "data/Receipts.csv" \\
        --output db --dry-run

    # Export to Excel for review
    python migrate_receipts.py \\
        --file-receipts "data/Receipts.csv" \\
        --output excel

    # Production import
    python migrate_receipts.py \\
        --file-receipts "data/Receipts.csv" \\
        --output db

Arguments:
    --file-receipts   Path to Receipts.csv from SiteLink (required)
    --output          'db' (default) or 'excel'
    --dry-run         [db mode] Preview without writing to DB
    --out-file        [excel mode] Output Excel file path

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — scopes the customer lookup
    LOCATION_ID       — not written to receipts but used for scoping if needed
    CREATED_BY        — not used; generated_by_user_id is hard-coded to 9
    BATCH_SIZE        — rows per DB commit batch (default: 500)

Column mapping (SiteLink CSV → storentic.receipts):
    receipt_number        = "SL-{IRECEIPTNUM}-{PAYMENTID}"     (unique per payment)
    payment_id            = payments.id  WHERE external_payment_id = PAYMENTID
    customer_id           = customer.id  WHERE external_id = TENANTID
    receipt_date          = DRCPT
    payment_date          = payments.payment_date  (from DB)
    amount_in_cents       = DCAMT * 100
    payment_method        = payment_method_types.name  (joined via payments.payment_method_type_id)
    transaction_id        = NULL
    generated_by_user_id  = 9
    email_sent            = false
    version               = 1
    is_latest             = true
    all other fields      = NULL / defaults

Skipped rows:
    - PAYMENTID not found in storentic.payments (external_payment_id)
    - TENANTID not found in storentic.customer (external_id)
    - DCAMT is zero or unparseable
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

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

GENERATED_BY_USER_ID = 9

# ── INSERT SQL ─────────────────────────────────────────────────────────────────
_INSERT_SQL = """
    INSERT INTO storentic.receipts (
        receipt_number,
        payment_id,
        customer_id,
        receipt_date,
        payment_date,
        amount_in_cents,
        payment_method,
        transaction_id,
        pdf_url,
        pdf_filename,
        pdf_size_bytes,
        email_sent,
        email_sent_at,
        email_recipient,
        email_subject,
        version,
        is_latest,
        superseded_by_receipt_id,
        generated_by_user_id,
        created_at,
        updated_at
    ) VALUES %s
    ON CONFLICT (receipt_number) DO NOTHING
"""

_INSERT_COLS = [
    "receipt_number",
    "payment_id",
    "customer_id",
    "receipt_date",
    "payment_date",
    "amount_in_cents",
    "payment_method",
    "transaction_id",
    "pdf_url",
    "pdf_filename",
    "pdf_size_bytes",
    "email_sent",
    "email_sent_at",
    "email_recipient",
    "email_subject",
    "version",
    "is_latest",
    "superseded_by_receipt_id",
    "generated_by_user_id",
    "created_at",
    "updated_at",
]

EXCEL_OUTPUT_COLUMNS = [
    "receipt_number", "payment_id", "customer_id",
    "receipt_date", "payment_date", "amount_in_cents",
    "payment_method", "generated_by_user_id",
    "created_at", "updated_at",
]

SKIPPED_COLUMNS = [
    "RECEIPTID", "IRECEIPTNUM", "PAYMENTID", "TENANTID",
    "DCAMT", "DRCPT", "skip_reason",
]


# =============================================================================
# STEP 1 — DB lookup maps
# =============================================================================

def load_payment_map(engine) -> dict[str, dict]:
    """
    Query storentic.payments and return:
        { external_payment_id (str): { "id": int, "payment_date": datetime,
                                       "payment_method_type_id": int } }
    Only payments with external_payment_id set are included.
    """
    logger.info("🔍  Loading payment map from storentic.payments ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, external_payment_id, payment_date, payment_method_type_id "
            "FROM storentic.payments "
            "WHERE external_payment_id IS NOT NULL"
        )).fetchall()

    mapping: dict[str, dict] = {}
    for row in rows:
        mapping[str(row.external_payment_id)] = {
            "id":                     row.id,
            "payment_date":           row.payment_date,
            "payment_method_type_id": row.payment_method_type_id,
        }

    logger.info(f"    Payments loaded: {len(mapping):,}")
    return mapping


def load_payment_method_type_map(engine) -> dict[int, str]:
    """
    Query storentic.payment_method_types and return {id: name}.
    Used to populate the payment_method string on each receipt.
    """
    logger.info("🔍  Loading payment method type names ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, name FROM storentic.payment_method_types"
        )).fetchall()

    mapping = {row.id: row.name for row in rows}
    logger.info(f"    Payment method types loaded: {len(mapping):,}")
    return mapping


def load_customer_map(engine, org_id: int) -> dict[int, int]:
    """
    Query storentic.customer and return {TenantID (external_id int): customer.id}.
    """
    logger.info(f"🔍  Loading customer map for org_id={org_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, external_id FROM storentic.customer "
            "WHERE external_id IS NOT NULL AND organization_id = :org"
        ), {"org": org_id}).fetchall()

    mapping: dict[int, int] = {}
    for row in rows:
        try:
            mapping[int(float(row.external_id))] = row.id
        except (ValueError, TypeError):
            pass

    logger.info(f"    Customers loaded: {len(mapping):,}")
    return mapping


def load_existing_receipt_numbers(engine) -> set[str]:
    """All receipt_number values already in storentic.receipts (idempotency)."""
    logger.info("🔍  Loading existing receipt_numbers for deduplication ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT receipt_number FROM storentic.receipts "
            "WHERE receipt_number IS NOT NULL"
        )).fetchall()
    ids = {row.receipt_number for row in rows}
    logger.info(f"    Already imported: {len(ids):,} receipts")
    return ids


# =============================================================================
# STEP 2 — Process receipts
# =============================================================================

def process_receipts(
    receipts_file: str,
    payment_map: dict,
    payment_method_type_map: dict,
    customer_map: dict,
    existing_receipt_numbers: set,
    org_id: int,
    output_mode: str,
    dry_run: bool,
    engine,
    out_file: str,
    batch_size: int,
) -> dict:

    # ── Read source CSV ────────────────────────────────────────────────────────
    logger.info(f"📂  Loading receipts file: {receipts_file}")
    df = pd.read_csv(receipts_file, dtype=str, low_memory=False, encoding="latin-1")
    df.columns = df.columns.str.strip().str.upper()
    df = df.drop(columns=["TOTALS & AVERAGES"], errors="ignore")

    required = {"RECEIPTID", "TENANTID", "DCAMT", "DRCPT", "IRECEIPTNUM", "PAYMENTID"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"❌  Receipts file missing columns: {missing}")

    total_raw = len(df)
    logger.info(f"    Total receipt rows: {total_raw:,}")

    stats = {
        "total": total_raw, "inserted": 0, "skipped_dup": 0,
        "skipped_no_payment": 0, "skipped_no_cust": 0,
        "skipped_zero_amt": 0, "errors": 0,
        "dry_run": dry_run, "output_mode": output_mode,
    }

    # ── Step 1: Vectorized pre-filter ─────────────────────────────────────────

    # Parse PAYMENTID and TENANTID as integers
    df["PAYMENTID_INT"] = pd.to_numeric(df["PAYMENTID"], errors="coerce")
    df["TENANTID_INT"]  = pd.to_numeric(df["TENANTID"],  errors="coerce")
    bad_ids = df["PAYMENTID_INT"].isna() | df["TENANTID_INT"].isna()
    if bad_ids.any():
        stats["errors"] += int(bad_ids.sum())
        logger.warning(f"    Dropped {bad_ids.sum()} rows with unparseable PAYMENTID/TENANTID")
    df = df[~bad_ids].copy()
    df["PAYMENTID_INT"] = df["PAYMENTID_INT"].astype(int)
    df["TENANTID_INT"]  = df["TENANTID_INT"].astype(int)

    # Drop zero / unparseable amounts
    df["DCAMT_NUM"] = pd.to_numeric(df["DCAMT"], errors="coerce").fillna(0)
    zero_amt = (df["DCAMT_NUM"] == 0).sum()
    stats["skipped_zero_amt"] = int(zero_amt)
    df = df[df["DCAMT_NUM"] != 0].copy()

    # Build receipt_number and dedup
    df["receipt_number"] = "SL-" + df["IRECEIPTNUM"].str.strip() + "-" + df["PAYMENTID_INT"].astype(str)
    dup_mask = df["receipt_number"].isin(existing_receipt_numbers)
    stats["skipped_dup"] = int(dup_mask.sum())
    df = df[~dup_mask].copy()

    # Resolve payment details (payment_id, payment_date, payment_method_type_id)
    df["_pmt_key"]  = df["PAYMENTID_INT"].astype(str)
    df["_pmt_data"] = df["_pmt_key"].map(payment_map)
    no_pmt_df = df[df["_pmt_data"].isna()].copy()
    stats["skipped_no_payment"] = len(no_pmt_df)
    df = df[df["_pmt_data"].notna()].copy()

    df["payment_id"]            = df["_pmt_data"].map(lambda d: d["id"])
    df["payment_date"]          = df["_pmt_data"].map(lambda d: d["payment_date"])
    df["_pmt_method_type_id"]   = df["_pmt_data"].map(lambda d: d["payment_method_type_id"])
    df["payment_method"]        = df["_pmt_method_type_id"].map(payment_method_type_map)

    # Resolve customer_id
    df["customer_id"] = df["TENANTID_INT"].map(customer_map)
    no_cust_df = df[df["customer_id"].isna()].copy()
    stats["skipped_no_cust"] = len(no_cust_df)
    df = df[df["customer_id"].notna()].copy()
    df["customer_id"] = df["customer_id"].astype(int)
    df["payment_id"]  = df["payment_id"].astype(int)

    df = df.reset_index(drop=True)

    logger.info(f"    After pre-filter     : {len(df):,} rows to process")
    logger.info(f"    Skipped zero-amount  : {stats['skipped_zero_amt']:,}")
    logger.info(f"    Skipped duplicates   : {stats['skipped_dup']:,}")
    logger.info(f"    Skipped no-payment   : {stats['skipped_no_payment']:,}")
    logger.info(f"    Skipped no-customer  : {stats['skipped_no_cust']:,}")
    print(f"  {len(df):,} rows to insert after filtering\n", flush=True)

    # ── Build skipped-rows list ───────────────────────────────────────────────
    skipped_rows: list[dict] = []
    for r in no_pmt_df.itertuples(index=False):
        rd = r._asdict()
        skipped_rows.append({
            "RECEIPTID": rd.get("RECEIPTID"), "IRECEIPTNUM": rd.get("IRECEIPTNUM"),
            "PAYMENTID": rd.get("PAYMENTID"), "TENANTID": rd.get("TENANTID"),
            "DCAMT": rd.get("DCAMT"), "DRCPT": rd.get("DRCPT"),
            "skip_reason": f"PAYMENTID {rd.get('PAYMENTID')} not found in storentic.payments",
        })
    for r in no_cust_df.itertuples(index=False):
        rd = r._asdict()
        skipped_rows.append({
            "RECEIPTID": rd.get("RECEIPTID"), "IRECEIPTNUM": rd.get("IRECEIPTNUM"),
            "PAYMENTID": rd.get("PAYMENTID"), "TENANTID": rd.get("TENANTID"),
            "DCAMT": rd.get("DCAMT"), "DRCPT": rd.get("DRCPT"),
            "skip_reason": f"TENANTID {rd.get('TENANTID')} not found in storentic.customer",
        })

    if dry_run:
        stats["inserted"] = len(df)
        logger.info(f"DRY RUN — {len(df):,} rows would be inserted.")
        _write_skipped(skipped_rows, stats)
        return stats

    # ── Step 2: Excel export mode ──────────────────────────────────────────────
    if output_mode == "excel":
        excel_records = []
        for row in df.itertuples(index=False):
            rd = row._asdict()
            record = _build_record(rd)
            excel_records.append({k: record[k] for k in EXCEL_OUTPUT_COLUMNS if k in record})
            stats["inserted"] += 1
        if excel_records:
            _write_excel(excel_records, out_file, EXCEL_OUTPUT_COLUMNS, "Receipts")
            stats["excel_output"] = out_file
        _write_skipped(skipped_rows, stats)
        return stats

    # ── Step 3: Transform with itertuples + bulk insert ────────────────────────
    batch_tuples: list[tuple] = []
    batch_ids:    list[str]   = []
    raw_conn   = engine.raw_connection()
    raw_cursor = raw_conn.cursor()

    def flush_batch():
        if not batch_tuples:
            return
        try:
            execute_values(raw_cursor, _INSERT_SQL, batch_tuples, page_size=batch_size)
            raw_conn.commit()
            stats["inserted"] += len(batch_tuples)
            logger.info(f"    Flushed {len(batch_tuples):,} rows (total: {stats['inserted']:,})")
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
            logger.warning(f"Batch failed ({exc_str[:120]}); retrying row-by-row ...")
            for tup, rnum in zip(batch_tuples, batch_ids):
                try:
                    execute_values(raw_cursor, _INSERT_SQL, [tup])
                    raw_conn.commit()
                    stats["inserted"] += 1
                except Exception as row_exc:
                    raw_conn.rollback()
                    log_error(0, rnum, "DB_INSERT", str(row_exc), rnum)
                    stats["errors"] += 1
        finally:
            batch_tuples.clear()
            batch_ids.clear()

    total_to_insert = len(df)
    for row in df.itertuples(index=False):
        rd          = row._asdict()
        receipt_num = rd["receipt_number"]
        try:
            record = _build_record(rd)
            batch_tuples.append(tuple(record[col] for col in _INSERT_COLS))
            batch_ids.append(receipt_num)
            existing_receipt_numbers.add(receipt_num)
            if len(batch_tuples) >= batch_size:
                flush_batch()
                pct = min(100, stats["inserted"] / total_to_insert * 100)
                print(
                    f"  {pct:5.1f}%  |  inserted {stats['inserted']:,} / {total_to_insert:,}"
                    f"  err={stats['errors']:,}",
                    flush=True,
                )
        except Exception as exc:
            log_error(0, receipt_num, "TRANSFORM", str(exc), receipt_num)
            stats["errors"] += 1

    flush_batch()
    raw_cursor.close()
    raw_conn.close()

    _write_skipped(skipped_rows, stats)
    return stats


def _build_record(rd: dict) -> dict:
    """Assemble the receipts insert dict from a pre-enriched row dict."""
    receipt_date = _parse_dt(rd.get("DRCPT"))
    now          = datetime.utcnow()
    created_at   = receipt_date or now

    return {
        "receipt_number":           rd["receipt_number"],
        "payment_id":               rd["payment_id"],
        "customer_id":              rd["customer_id"],
        "receipt_date":             receipt_date or now,
        "payment_date":             rd["payment_date"],
        "amount_in_cents":          _to_cents(rd.get("DCAMT_NUM", rd.get("DCAMT"))),
        "payment_method":           rd.get("payment_method"),
        "transaction_id":           None,
        "pdf_url":                  None,
        "pdf_filename":             None,
        "pdf_size_bytes":           None,
        "email_sent":               False,
        "email_sent_at":            None,
        "email_recipient":          None,
        "email_subject":            None,
        "version":                  1,
        "is_latest":                True,
        "superseded_by_receipt_id": None,
        "generated_by_user_id":     GENERATED_BY_USER_ID,
        "created_at":               created_at,
        "updated_at":               created_at,
    }


def _parse_dt(val) -> datetime | None:
    """Parse a date string to datetime; returns None for null/blank/unparseable."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("", "nan", "nat", "none"):
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _to_cents(val) -> int:
    """Convert a dollar amount to integer cents."""
    try:
        return max(0, int(round(float(val) * 100)))
    except (TypeError, ValueError):
        return 0


def _write_skipped(skipped_rows: list, stats: dict):
    if not skipped_rows:
        return
    ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
    skipped_path = os.path.join(OUTPUT_DIR, f"receipts_skipped_{ts}.xlsx")
    _write_excel(skipped_rows, skipped_path, SKIPPED_COLUMNS, "Skipped")
    stats["skipped_output"] = skipped_path
    print(f"\n  ⚠️   {len(skipped_rows):,} skipped rows → {skipped_path}", flush=True)


def _write_excel(records: list, out_path: str, columns: list, sheet_name: str):
    cols   = [c for c in columns if c in (records[0] if records else {})]
    df_out = pd.DataFrame(records, columns=cols)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
    logger.info(f"📊  Excel written: {out_path}  ({len(records):,} rows)")


# =============================================================================
# STEP 3 — Summary
# =============================================================================

def _print_summary(stats: dict, run_ts: str):
    from scripts.logger import LOG_FILE, ERROR_CSV
    mode  = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 65,
        "  STORENTIC RECEIPTS MIGRATION — SUMMARY",
        f"  Run timestamp     : {run_ts}",
        f"  Output mode       : {mode}",
        f"  Dry run           : {stats.get('dry_run', False)}",
        "=" * 65,
        f"  Total rows read              : {stats.get('total', 0):,}",
        f"  Successfully written         : {stats.get('inserted', 0):,}",
        f"  Skipped (already imported)   : {stats.get('skipped_dup', 0):,}",
        f"  Skipped (zero amount)        : {stats.get('skipped_zero_amt', 0):,}",
        f"  Skipped (payment not found)  : {stats.get('skipped_no_payment', 0):,}",
        f"  Skipped (customer not found) : {stats.get('skipped_no_cust', 0):,}",
        f"  Errors / rejected            : {stats.get('errors', 0):,}",
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
        description="SiteLink Receipts CSV → storentic.receipts migration"
    )
    parser.add_argument("--file-receipts", required=True,
                        help="Path to Receipts.csv (SiteLink export)")
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
    batch_size  = int(os.getenv("BATCH_SIZE", 500))

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"receipts_transformed_{ts}.xlsx")

    print(
        f"\n  Storentic Receipts Migration"
        f"\n  Mode: {output_mode.upper()}{'  [DRY RUN]' if dry_run else ''}  |"
        f"  org={org_id}  generated_by={GENERATED_BY_USER_ID}\n",
        flush=True,
    )
    logger.info("=" * 65)
    logger.info("STORENTIC RECEIPTS ETL MIGRATION STARTING")
    logger.info(f"Receipts file     : {args.file_receipts}")
    logger.info(f"Output mode       : {output_mode.upper()}")
    logger.info(f"Dry run           : {dry_run}")
    logger.info(f"Org / GeneratedBy : {org_id} / {GENERATED_BY_USER_ID}")
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

    # ── DB lookups ────────────────────────────────────────────────────────────
    payment_map              = load_payment_map(engine)
    payment_method_type_map  = load_payment_method_type_map(engine)
    customer_map             = load_customer_map(engine, org_id)
    existing_receipt_numbers = load_existing_receipt_numbers(engine)

    # ── Process ───────────────────────────────────────────────────────────────
    stats = process_receipts(
        receipts_file            = args.file_receipts,
        payment_map              = payment_map,
        payment_method_type_map  = payment_method_type_map,
        customer_map             = customer_map,
        existing_receipt_numbers = existing_receipt_numbers,
        org_id                   = org_id,
        output_mode              = output_mode,
        dry_run                  = dry_run,
        engine                   = engine,
        out_file                 = out_file,
        batch_size               = batch_size,
    )

    _print_summary(stats, run_ts)
    close_logger()


if __name__ == "__main__":
    main()
