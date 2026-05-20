"""
migrate_units.py — Main ETL script for loading legacy Unit data.

Usage :
    # Insert transformed records into PostgreSQL (default)
    python migrate_units.py --file data/Units.csv --output db

    # Write transformed records to an Excel file instead of the DB
    python migrate_units.py --file data/Units.csv --output excel

    # DB mode with additional flags
    python migrate_units.py --file data/Units.csv --output db --dry-run
    python migrate_units.py --file data/Units.csv --output db --update

Arguments:
    --file      Path to the source CSV file (required)
    --output    Destination: 'db' (default) or 'excel'
    --dry-run   [db mode only] Preview without writing to DB
    --update    [db mode only] Update existing records instead of skipping duplicates
    --out-file  [excel mode only] Output Excel filename (default: output/units_transformed_<ts>.xlsx)

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — required, not in Excel source
    LOCATION_ID       — required, not in Excel source
    DRY_RUN           — alternative to --dry-run flag
    UPDATE_EXISTING   — alternative to --update flag
"""

import argparse
import os
import sys

import pandas as pd
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

# ── Bootstrap path so sub-modules resolve correctly ───────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

load_dotenv()

from scripts.logger import logger, log_error, log_skipped, write_summary, close as close_logger
from scripts.validator import validate
from scripts import transformer as T

# ── Output directory for Excel exports ────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Column short-key constants ─────────────────────────────────────────────────
COL_UNIT_NAME     = "UNITNAME"
COL_BLDG          = "BLDG"
COL_TYPE          = "TYPE"
COL_ENTRY_LOC     = "ENTRY LOCATION"
COL_UNIT_ACCESS   = "UNIT ACCESS"
COL_FLOOR         = "FLOOR"
COL_POWER         = "POWER (YES"
COL_POWER_OUTLET  = "POWER_ELECTRICALOUTLET"
COL_POWER_LIGHTS  = "POWER_LIGHTS"
COL_POWER_TIMER   = "POWER_TIMER_HR"
COL_ACCESS_HOURS  = "UNIT ACCESS HOURS"
COL_ENTRY_LOCK    = "ENTRYLOCK"
COL_DOOR_TYPE     = "DOORTYPE"
COL_ALARM         = "ALARM"
COL_ADA           = "ADA"
COL_AREA          = "AREA"
COL_UNIT_SIZE     = "UNITSIZE"
COL_STD_RATE      = "STANDARDRATE"
COL_PUSH_RATE     = "PUSHRATE"
COL_WEEKLY_RATE   = "WEEKLYRATE"
COL_RENTABLE      = "RENTABLE"
COL_RENTED        = "RENTED"
COL_MAINTENANCE   = "MAINTENANCE"
COL_RESERVED      = "RESERVED"
COL_ON_SITE_RES   = "ON-SITE RESIDENT"
COL_BOX_TRUCK     = "BOX TRUCK RENTAL"
COL_MOBILE_UNIT   = "MOBILE UNIT"


# ── Row transformer ────────────────────────────────────────────────────────────

def transform_row(row_idx: int, row: pd.Series, col: dict, org_id: int, loc_id: int, created_by: int = 0,
                  unit_size_map: dict = None, unit_size_default_id: int = 0) -> dict | None:
    """
    Transform one Excel row into a dict ready for DB insertion.
    Returns None if the row should be skipped due to a critical error.
    """
    unit_name = row.get(col[COL_UNIT_NAME])
    errors_found = False

    def get(key):
        """Safe getter from col map."""
        col_name = col.get(key)
        if col_name:
            return row.get(col_name)
        return None

    def safe(field_name, fn, *args, nullable=False, default=None):
        """
        Call a transformer function safely.
        Logs the error and returns default on failure.
        Sets errors_found flag if critical.
        """
        nonlocal errors_found
        try:
            return fn(*args)
        except ValueError as e:
            log_error(row_idx + 2, unit_name, field_name, str(e), args[0] if args else None)
            return default

    # ── Critical check: UnitName must exist ──────────────────────────────────
    if not unit_name or str(unit_name).strip().lower() in ("nan", "none", ""):
        log_error(row_idx + 2, "UNKNOWN", COL_UNIT_NAME, "UnitName is blank — row rejected", None)
        return None

    type_val = get(COL_TYPE)

    # ── Build the record ──────────────────────────────────────────────────────
    record = {
        # Core identification
        "unit_number"         : str(unit_name).strip(),
        "building"            : str(get(COL_BLDG)).strip() if get(COL_BLDG) else None,

        # Classification
        "unit_type_id"        : T.derive_unit_type_id(type_val),
        "climate_controlled"  : T.derive_climate_controlled(type_val),

        # Location flags
        "inside"              : T.derive_inside(get(COL_ENTRY_LOC)),
        "unit_access"         : safe("unit_access", T.derive_unit_access, get(COL_UNIT_ACCESS)),

        # Floor
        "floor_level"         : safe("floor_level", T.derive_floor_level, get(COL_FLOOR)),

        # Power
        "power"               : safe("power", T.yes_no_to_bool, get(COL_POWER)),
        "power_outlet"        : safe("power_outlet", T.yes_no_to_bool, get(COL_POWER_OUTLET)),
        "power_lights"        : safe("power_lights", T.derive_power_lights, get(COL_POWER_LIGHTS)),
        "power_timer_hr"      : safe("power_timer_hr", T.derive_power_timer_hr, get(COL_POWER_TIMER)),

        # Access & security
        "unit_access_hours"   : T.derive_unit_access_hours(get(COL_ACCESS_HOURS)),
        "entry_lock"          : safe("entry_lock", T.derive_entry_lock, get(COL_ENTRY_LOCK)),
        "door_type_id"        : safe("door_type_id", T.derive_door_type_id, get(COL_DOOR_TYPE)),

        # Safety
        "alarm"               : safe("alarm", T.yes_no_to_bool, get(COL_ALARM)),
        "ada"                 : safe("ada", T.yes_no_to_bool, get(COL_ADA), nullable=True),

        # Physical attributes
        "unit_area"           : str(get(COL_AREA)).strip() if get(COL_AREA) not in (None, "nan") else None,
        "unit_size_id"        : T.derive_unit_size_id(get(COL_UNIT_SIZE), unit_size_map or {}, unit_size_default_id),

        # Rates
        "standard_rate"       : safe("standard_rate", T.derive_rate, get(COL_STD_RATE)),
        "push_rate"           : safe("push_rate", T.derive_rate, get(COL_PUSH_RATE)),
        "weekly_rate"         : safe("weekly_rate", T.derive_rate, get(COL_WEEKLY_RATE)),

        # Derived status
        "unit_status_id"      : T.derive_unit_status_id(
                                    get(COL_RENTABLE),
                                    get(COL_RENTED),
                                    get(COL_RESERVED),
                                    get(COL_MAINTENANCE),
                                ),

        # Additional booleans
        "box_truck_rental"    : safe("box_truck_rental", T.yes_no_to_bool, get(COL_BOX_TRUCK), nullable=True),
        "mobile_unit"         : safe("mobile_unit", T.derive_mobile_unit, get(COL_MOBILE_UNIT)),
        "on_site_resident"    : safe("on_site_resident", T.yes_no_to_bool, get(COL_ON_SITE_RES), nullable=True),

        # Context (from env, not Excel)
        "organization_id"     : org_id,
        "location_id"         : loc_id,

        # Defaults for required fields not in Excel
        "reserved"            : False,
        "one_hour_timer"      : False,
        "drive_up"            : False,
        "lighting"            : False,
        "fire_suppression"    : False,
        "location_type_id"    : 1,  # default
        "created_by"          : created_by,
        "updated_by"          : created_by,
        "created_datetime"    : datetime.utcnow(),
        "updated_datetime"    : datetime.utcnow(),
    }

    return record


# ── Unit size ref table loader ─────────────────────────────────────────────────

def load_unit_size_map(conn, org_id: int, loc_id: int) -> tuple[dict, int]:
    """
    Load storentic.unit_size into a normalised lookup dict.

    Returns:
        (size_map, default_id) where
          size_map   — {normalised_value: id}  e.g. {"10x30": 3, "5x10": 7, ...}
          default_id — id of the '0x0' row used as the fallback
    Normalisation: lowercase, strip spaces, '*' → 'x'.
    """
    from sqlalchemy import text as sa_text
    rows = conn.execute(
        sa_text(
            "SELECT id, value FROM storentic.unit_size "
            "WHERE organization_id = :org_id AND location_id = :loc_id"
        ),
        {"org_id": org_id, "loc_id": loc_id},
    ).fetchall()

    size_map: dict = {}
    default_id: int = 0
    for row in rows:
        normalized = str(row.value).strip().lower().replace("*", "x").replace(" ", "")
        size_map[normalized] = row.id
        if normalized == "0x0":
            default_id = row.id

    return size_map, default_id


# ── Duplicate check ────────────────────────────────────────────────────────────

def unit_exists(conn, unit_number: str, org_id: int, loc_id: int) -> bool:
    """Check if a unit with the same number already exists for this org/location."""
    from sqlalchemy import text as sa_text
    result = conn.execute(
        sa_text(
            "SELECT id FROM storentic.units "
            "WHERE unit_number = :unit_number "
            "  AND organization_id = :org_id "
            "  AND location_id = :loc_id "
            "LIMIT 1"
        ),
        {"unit_number": unit_number, "org_id": org_id, "loc_id": loc_id},
    )
    return result.fetchone() is not None


# ── Insert / Update ────────────────────────────────────────────────────────────

# ── SQL statements (defined as plain strings; wrapped in text() at runtime) ───

_INSERT_SQL = """
    INSERT INTO storentic.units (
        unit_number, building, unit_type_id, climate_controlled,
        inside, unit_access, floor_level,
        power, power_outlet, power_lights, power_timer_hr,
        unit_access_hours, entry_lock, door_type_id,
        alarm, ada, unit_area, unit_size_id,
        standard_rate, push_rate, weekly_rate,
        unit_status_id, box_truck_rental, mobile_unit, on_site_resident,
        organization_id, location_id, reserved, one_hour_timer,
        drive_up, lighting, fire_suppression, location_type_id,
        created_by, updated_by, created_datetime, updated_datetime
    ) VALUES (
        :unit_number, :building, :unit_type_id, :climate_controlled,
        :inside, :unit_access, :floor_level,
        :power, :power_outlet, :power_lights, :power_timer_hr,
        :unit_access_hours, :entry_lock, :door_type_id,
        :alarm, :ada, :unit_area, :unit_size_id,
        :standard_rate, :push_rate, :weekly_rate,
        :unit_status_id, :box_truck_rental, :mobile_unit, :on_site_resident,
        :organization_id, :location_id, :reserved, :one_hour_timer,
        :drive_up, :lighting, :fire_suppression, :location_type_id,
        :created_by, :updated_by, :created_datetime, :updated_datetime
    )
"""

_UPDATE_SQL = """
    UPDATE storentic.units SET
        building            = :building,
        unit_type_id        = :unit_type_id,
        climate_controlled  = :climate_controlled,
        inside              = :inside,
        unit_access         = :unit_access,
        floor_level         = :floor_level,
        power               = :power,
        power_outlet        = :power_outlet,
        power_lights        = :power_lights,
        power_timer_hr      = :power_timer_hr,
        unit_access_hours   = :unit_access_hours,
        entry_lock          = :entry_lock,
        door_type_id        = :door_type_id,
        alarm               = :alarm,
        ada                 = :ada,
        unit_area           = :unit_area,
        unit_size_id        = :unit_size_id,
        standard_rate       = :standard_rate,
        push_rate           = :push_rate,
        weekly_rate         = :weekly_rate,
        unit_status_id      = :unit_status_id,
        box_truck_rental    = :box_truck_rental,
        mobile_unit         = :mobile_unit,
        on_site_resident    = :on_site_resident,
        updated_by          = :updated_by,
        updated_datetime    = :updated_datetime
    WHERE unit_number = :unit_number
      AND organization_id = :organization_id
      AND location_id = :location_id
"""


# ── Excel output ───────────────────────────────────────────────────────────────

# Columns to include in the Excel output, in display order.
# Excludes internal audit fields (created_by, updated_by, etc.)
EXCEL_OUTPUT_COLUMNS = [
    "unit_number", "building", "unit_type_id", "climate_controlled",
    "inside", "unit_access", "floor_level",
    "power", "power_outlet", "power_lights", "power_timer_hr",
    "unit_access_hours", "entry_lock", "door_type_id",
    "alarm", "ada", "unit_area", "unit_size_id",
    "standard_rate", "push_rate", "weekly_rate",
    "unit_status_id", "box_truck_rental", "mobile_unit", "on_site_resident",
    "organization_id", "location_id",
]


def write_to_excel(records: list[dict], out_path: str):
    """Write transformed records to an Excel file."""
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Transformed Units")

        # Auto-size columns for readability
        ws = writer.sheets["Transformed Units"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)

    logger.info(f"📊  Excel output written: {out_path}  ({len(records)} rows)")


# ── Argument parsing (shared with migrate.py) ──────────────────────────────────

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Storentic Units ETL Migration")
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

    output_mode    = args.output
    dry_run        = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    update_existing = args.update or os.getenv("UPDATE_EXISTING", "false").lower() == "true"
    org_id         = int(os.getenv("ORGANIZATION_ID", 1))
    loc_id         = int(os.getenv("LOCATION_ID", 1))
    created_by     = int(os.getenv("CREATED_BY", 0))

    # Resolve Excel output path
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"units_transformed_{ts}.xlsx")

    logger.info("=" * 60)
    logger.info("  STORENTIC UNITS ETL MIGRATION STARTING")
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
    df, col_map = validate(args.file)

    # ── Step 2: Set up DB only when needed ────────────────────────────────────
    engine = None
    if output_mode == "db" and not dry_run:
        try:
            from sqlalchemy import text as sa_text
            from scripts.db import get_engine
            engine = get_engine()
            logger.info("✅  Database connection established.")
        except ImportError:
            logger.error("❌  sqlalchemy / psycopg2 not installed. Run: pip install sqlalchemy psycopg2-binary")
            sys.exit(1)

    # ── Step 2b: Load unit_size ref map ───────────────────────────────────────
    unit_size_map: dict = {}
    unit_size_default_id: int = 0
    try:
        from sqlalchemy import text as sa_text
        from scripts.db import get_engine as _get_engine
        _lookup_engine = engine if engine is not None else _get_engine()
        with _lookup_engine.connect() as _rc:
            unit_size_map, unit_size_default_id = load_unit_size_map(_rc, org_id, loc_id)
        logger.info(f"✅  Loaded {len(unit_size_map)} unit size mappings  (default id={unit_size_default_id})")
    except Exception as _e:
        logger.warning(f"⚠️  Could not load storentic.unit_size ref table — unit_size_id will default to 0.  ({_e})")

    # ── Step 3: Process rows ──────────────────────────────────────────────────
    stats = {
        "total": 0, "inserted": 0, "updated": 0,
        "skipped": 0, "errors": 0, "dry_run": dry_run,
        "output_mode": output_mode,
    }
    excel_records = []   # accumulates rows when output_mode == "excel"

    for row_idx, row in df.iterrows():
        stats["total"] += 1

        record = transform_row(row_idx, row, col_map, org_id, loc_id, created_by,
                               unit_size_map=unit_size_map,
                               unit_size_default_id=unit_size_default_id)

        if record is None:
            stats["errors"] += 1
            continue

        unit_number = record["unit_number"]

        # ── Excel output mode ─────────────────────────────────────────────────
        if output_mode == "excel":
            excel_records.append(record)
            logger.info(f"📋  Queued for Excel: {unit_number}")
            stats["inserted"] += 1
            continue

        # ── DB dry run ────────────────────────────────────────────────────────
        if dry_run:
            logger.info(f"[DRY RUN] Would upsert unit: {unit_number}  status_id={record['unit_status_id']}")
            stats["inserted"] += 1
            continue

        # ── Live DB write (upsert: update if exists, insert if new) ──────────
        try:
            from sqlalchemy import text as sa_text
            with engine.begin() as conn:
                exists = unit_exists(conn, unit_number, org_id, loc_id)

                if exists:
                    conn.execute(sa_text(_UPDATE_SQL), record)
                    logger.info(f"🔄  Updated: unit_number={unit_number}")
                    stats["updated"] += 1
                else:
                    conn.execute(sa_text(_INSERT_SQL), record)
                    logger.info(f"✅  Inserted: unit_number={unit_number}")
                    stats["inserted"] += 1

        except Exception as e:
            log_error(row_idx + 2, unit_number, "DB_UPSERT", str(e), unit_number)
            stats["errors"] += 1

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
