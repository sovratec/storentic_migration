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
from scripts.utils import normalize_columns
import os

# Short alias -> actual Excel column header (partial match key)
REQUIRED_COLUMN_KEYS = [
    "UnitName",
    "Bldg",
    "Type",
    "Entry Location",
    "Unit Access",
    "Floor",
    "Power (Yes",
    "Power_ElectricalOutlet",
    "Power_Lights",
    "Power_Timer_Hr",
    "Unit Access Hours",
    "EntryLock",
    "DoorType",
    "Alarm",
    "ADA",
    "Area",
    "UnitSize",
    "StandardRate",
    "PushRate",
    "WeeklyRate",
    "Rentable",
    "Rented",
    "Maintenance",
    "Reserved",
    "On-Site Resident",
    "Box Truck Rental",
    "Mobile Unit",
]

REQUIRED_CUSTOMER_COLUMN_KEYS = [
    "TenantId",      # → external_id
    "sFname",         # → first_name (prefix)
    "sMI",            # → first_name (middle initial, optional)
    "sLname",         # → last_name
    "sCompany",       # → company_name
    "sAddr1",         # → address1
    "sAddr2",         # → address2
    "sCity",          # → city
    "sRegion",        # → state
    "sPostalCode",    # → zip
    "sCountry",       # → country (new column)
    "sPhone",         # → phone
    "sFNameAlt",      # → alternate_first_name (prefix)
    "sMIAlt",         # → alternate_first_name (middle initial, optional)
    "sLnameAlt",      # → alternate_last_name
    "sAddr1Alt",      # → alternate_address1
    "sAddr2Alt",      # → alternate_address2
    "sCityAlt",       # → alternate_city
    "sRegionAlt",     # → alternate_state
    "sPostalCodeAlt", # → alternate_zip
    "sCountryAlt",    # → alternate_country (new column)
    "sPhoneAlt",      # → alternate_phone
    "sEmail",         # → email
    "sEmailAlt",      # → alternate_email
    "sMobile",        # → mobile (new column)
    "sAccessCode",    # → access_gate_code
    "sLicense",       # → driver_license_id
    "sLicRegion",     # → driver_license_issue_state
]

REQUIRED_CUSTOMER_UNIT_COLUMN_KEYS = [
    "TenantId",      # FK → storentic.customer.external_id
    "sUnitName",     # FK → storentic.units.unit_number  (scoped to LOCATION_ID)
    "dMovedIn",      # Required — drives charge_day derivation and lease_date fallback
    "dLease",        # → lease_date  (falls back to dMovedIn if null)
    "dPaidThru",     # → paid_through_date
    "dMovedOut",     # → move_out_date  (NULL for all 377 active rows)
    "dSchedOut",     # → scheduled_date (NULL for all 377 active rows)
    "dcRent",        # → rental_rate  (required; stored as DECIMAL dollars)
    "dcSchedRent",   # → scheduled_rate (copies rental_rate if 0 or null)
    "sAccessCode",   # → access_code  (truncated to 10 characters)
]

REQUIRED_RENTAL_AGREEMENT_COLUMN_KEYS = [
    "TenantId",        # FK → storentic.customer.external_id
    "sUnitName",       # FK → storentic.units.unit_number  (scoped to LOCATION_ID)
    "dMovedIn",        # Required — drives move_in_date and charge_day
    "dLease",          # → lease_date  (falls back to dMovedIn if null)
    "dPaidThru",       # → paid_through_date  (required)
    "dMovedOut",       # → move_out_date  (NULL for all 377 active rows)
    "dSchedOut",       # → schedule_date  (falls back to dMovedIn if null)
    "dcRent",          # → rental_rate_in_cents  (required; dollars × 100)
    "dcSchedRent",     # → schedule_rate_in_cents  (copies rental_rate if 0 or null)
    "dcSecDepPaid",    # → security_deposit_in_cents  (dollars × 100; 0 if null)
    "dcInsurPremium",  # → insurance_rate_in_cents + insurance_option  (0/NONE if null)
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
    df.columns = df.columns.str.strip()
    df = normalize_columns(df, required_keys)

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
