"""
validator.py — Pre-flight validation of the source Excel file.

Checks that all required columns are present and reports basic stats
before the main migration begins.

Supports three modes:
  mode="unit"          — validates Unit Excel columns (default)
  mode="customer"      — validates Customer Excel columns
  mode="customer_unit" — validates Customer-Unit rental Excel columns
"""

import pandas as pd
from scripts.logger import logger, LOGS_DIR
import os

# Short alias -> actual Excel column header (partial match key)
REQUIRED_COLUMN_KEYS = [
    "UNITNAME",
    "BLDG",
    "TYPE",
    "ENTRY LOCATION",
    "UNIT ACCESS",
    "FLOOR",
    "POWER (YES",
    "POWER_ELECTRICALOUTLET",
    "POWER_LIGHTS",
    "POWER_TIMER_HR",
    "UNIT ACCESS HOURS",
    "ENTRYLOCK",
    "DOORTYPE",
    "ALARM",
    "ADA",
    "AREA",
    "UNITSIZE",
    "STANDARDRATE",
    "PUSHRATE",
    "WEEKLYRATE",
    "RENTABLE",
    "RENTED",
    "MAINTENANCE",
    "RESERVED",
    "ON-SITE RESIDENT",
    "BOX TRUCK RENTAL",
    "MOBILE UNIT",
]

REQUIRED_CUSTOMER_COLUMN_KEYS = [
    "TENANTID",      # → external_id
    "SFNAME",         # → first_name (prefix)
    "SMI",            # → first_name (middle initial, optional)
    "SLNAME",         # → last_name
    "SCOMPANY",       # → company_name
    "SADDR1",         # → address1
    "SADDR2",         # → address2
    "SCITY",          # → city
    "SREGION",        # → state
    "SPOSTALCODE",    # → zip
    "SCOUNTRY",       # → country (new column)
    "SPHONE",         # → phone
    "SFNAMEALT",      # → alternate_first_name (prefix)
    "SMIALT",         # → alternate_first_name (middle initial, optional)
    "SLNAMEALT",      # → alternate_last_name
    "SADDR1ALT",      # → alternate_address1
    "SADDR2ALT",      # → alternate_address2
    "SCITYALT",       # → alternate_city
    "SREGIONALT",     # → alternate_state
    "SPOSTALCODEALT", # → alternate_zip
    "SCOUNTRYALT",    # → alternate_country (new column)
    "SPHONEALT",      # → alternate_phone
    "SEMAIL",         # → email
    "SEMAILALT",      # → alternate_email
    "SMOBILE",        # → mobile (new column)
    "SACCESSCODE",    # → access_gate_code
    "SLICENSE",       # → driver_license_id
    "SLICREGION",     # → driver_license_issue_state
]

REQUIRED_CUSTOMER_UNIT_COLUMN_KEYS = [
    "TENANTID",      # FK → storentic.customer.external_id
    "SUNITNAME",     # FK → storentic.units.unit_number  (scoped to LOCATION_ID)
    "DMOVEDIN",      # Required — drives charge_day derivation and lease_date fallback
    "DLEASE",        # → lease_date  (falls back to dMovedIn if null)
    "DPAIDTHRU",     # → paid_through_date
    "DMOVEDOUT",     # → move_out_date  (NULL for all 377 active rows)
    "DSCHEDOUT",     # → scheduled_date (NULL for all 377 active rows)
    "DCRENT",        # → rental_rate  (required; stored as DECIMAL dollars)
    "DCSCHEDRENT",   # → scheduled_rate (copies rental_rate if 0 or null)
    "SACCESSCODE",   # → access_code  (truncated to 10 characters)
]

REQUIRED_RENTAL_AGREEMENT_COLUMN_KEYS = [
    "TENANTID",        # FK → storentic.customer.external_id
    "SUNITNAME",       # FK → storentic.units.unit_number  (scoped to LOCATION_ID)
    "DMOVEDIN",        # Required — drives move_in_date and charge_day
    "DLEASE",          # → lease_date  (falls back to dMovedIn if null)
    "DPAIDTHRU",       # → paid_through_date  (required)
    "DMOVEDOUT",       # → move_out_date  (NULL for all 377 active rows)
    "DSCHEDOUT",       # → schedule_date  (falls back to dMovedIn if null)
    "DCRENT",          # → rental_rate_in_cents  (required; dollars × 100)
    "DCSCHEDRENT",     # → schedule_rate_in_cents  (copies rental_rate if 0 or null)
    "DCSECDEPPAID",    # → security_deposit_in_cents  (dollars × 100; 0 if null)
    "DCINSURPREMIUM",  # → insurance_rate_in_cents + insurance_option  (0/NONE if null)
]

IGNORED_COLUMNS = ["Width", "Length", "Zone", "Climate", "SecurityDeposit"]


def find_column(df: pd.DataFrame, key: str):
    """Find a DataFrame column that starts with the given key (case-insensitive)."""
    for col in df.columns:
        if col.strip().lower().startswith(key.strip().lower()):
            return col
    return None


def validate(filepath: str, mode: str = "unit") -> tuple[pd.DataFrame, dict]:
    """
    Validate the source Excel file.
    Returns (dataframe, column_map) where column_map maps short keys to actual column names.
    Raises SystemExit if critical columns are missing.

    Args:
        filepath: Path to the source Excel file.
        mode:     "unit" (default) or "customer" — selects the required column set.
    """
    if mode == "customer":
        required_keys = REQUIRED_CUSTOMER_COLUMN_KEYS
    elif mode == "customer_unit":
        required_keys = REQUIRED_CUSTOMER_UNIT_COLUMN_KEYS
    elif mode == "rental_agreement":
        required_keys = REQUIRED_RENTAL_AGREEMENT_COLUMN_KEYS
    else:
        required_keys = REQUIRED_COLUMN_KEYS

    logger.info(f"📂  Loading source file: {filepath}")
    logger.info(f"    Migration mode : {mode.upper()}")
    df = pd.read_csv(filepath, dtype=str, encoding='utf-8')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()

    # Strip whitespace from all string values
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    logger.info(f"    Rows found: {len(df)}")
    logger.info(f"    Columns found: {len(df.columns)}")

    # Build column map
    column_map = {}
    missing = []
    for key in required_keys:
        col = find_column(df, key)
        if col:
            column_map[key] = col
        else:
            missing.append(key)

    # Report ignored columns
    for key in IGNORED_COLUMNS:
        col = find_column(df, key)
        if col:
            logger.info(f"    ℹ️  Column '{col}' is present but intentionally excluded from migration.")

    if missing:
        logger.error(f"❌  Missing required columns: {missing}")
        raise SystemExit(f"Validation failed — missing columns: {missing}")

    # Drop fully empty rows
    original_len = len(df)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    dropped = original_len - len(df)
    if dropped:
        logger.warning(f"    ⚠️  Dropped {dropped} fully empty row(s).")

    # Write validation report
    report_path = os.path.join(LOGS_DIR, "validation_report.txt")
    with open(report_path, "w") as f:
        f.write(f"Source file  : {filepath}\n")
        f.write(f"Total rows   : {len(df)}\n")
        f.write(f"Empty dropped: {dropped}\n\n")
        f.write("Column mapping:\n")
        for k, v in column_map.items():
            f.write(f"  {k:30s} -> {v}\n")
        if missing:
            f.write(f"\nMISSING: {missing}\n")

    logger.info(f"✅  Validation passed. Report: {report_path}")
    return df, column_map
