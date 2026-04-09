"""
transformer.py — All field-level transformation and mapping logic.

Each function takes a raw value from the Excel row and returns the
appropriate database value, or raises ValueError for unrecoverable issues.
"""

from decimal import Decimal, InvalidOperation
from typing import Optional


# ── Boolean helpers ────────────────────────────────────────────────────────────

def yes_no_to_bool(value, nullable: bool = False) -> Optional[bool]:
    """
    Convert 'Yes'/'No' string to True/False.
    If nullable=True, returns None for blank/null values.
    Defaults to False for blank when nullable=False.
    """
    if value is None or (isinstance(value, float) and str(value) == "nan"):
        return None if nullable else False
    s = str(value).strip().lower()
    if s in ("yes", "y", "true", "1"):
        return True
    if s in ("no", "n", "false", "0"):
        return False
    if s == "" or s == "nan":
        return None if nullable else False
    raise ValueError(f"Unexpected Yes/No value: {value!r}")


# ── Unit Type & Climate Control ────────────────────────────────────────────────

CLIMATE_CONTROL_TYPES = {"climate control", "climate"}

UNIT_TYPE_MAP = {
    "apartment": 4,
    "vehicle parking": 5,
    "rv parking": 5,
    "boat parking": 5,
}


def derive_unit_type_id(type_value) -> int:
    """Derive unit_type_id from the Type column."""
    if type_value is None or str(type_value).strip().lower() in ("nan", ""):
        return 1  # default
    key = str(type_value).strip().lower()
    return UNIT_TYPE_MAP.get(key, 1)


def derive_climate_controlled(type_value) -> bool:
    """Derive climateControlled from the Type column."""
    if type_value is None or str(type_value).strip().lower() in ("nan", ""):
        return False
    return str(type_value).strip().lower() in CLIMATE_CONTROL_TYPES


# ── Inside flag ────────────────────────────────────────────────────────────────

def derive_inside(entry_location) -> bool:
    """True if Entry Location is 'Interior'."""
    if entry_location is None:
        return False
    return str(entry_location).strip().lower() == "interior"


# ── Unit Access enum ───────────────────────────────────────────────────────────

UNIT_ACCESS_MAP = {
    "ground floor driveup": "GROUND_FLOOR_DRIVEUP",
    "ground floor interior walkable": "GROUND_FLOOR_INTERIOR_WALKABLE",
    "ground floor exterior walkable": "GROUND_FLOOR_EXTERIOR_WALKABLE",
    "driveup non-covered exterior parking": "DRIVEUP_NON_COVERED_EXT_PARKING",
    "driveup covered exterior parking": "DRIVEUP_COVERED_EXT_PARKING",
    "driveup interior parking": "DRIVEUP_INTERIOR_PARKING",
    "multi-floor/level elevator accessible": "MULTI_FLOOR_ELEVATOR_ACCESSIBLE",
    "multi-floor/level elevator no-accessible": "MULTI_FLOOR_ELEVATOR_NON_ACCESSIBLE",
    "multi-floor/level elevator non-accessible": "MULTI_FLOOR_ELEVATOR_NON_ACCESSIBLE",
}


def derive_unit_access(value) -> Optional[str]:
    """Map Unit Access column to enum string."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    key = str(value).strip().lower()
    result = UNIT_ACCESS_MAP.get(key)
    if result is None:
        raise ValueError(f"Unrecognised Unit Access value: {value!r}")
    return result


# ── Floor level ────────────────────────────────────────────────────────────────

def derive_floor_level(value) -> Optional[int]:
    """Convert floor value to integer."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f"Non-integer floor value: {value!r}")


# ── Power Lights enum ──────────────────────────────────────────────────────────

POWER_LIGHTS_MAP = {
    "hallway": "HALLWAY",
    "inside unit": "INSIDE_UNIT",
    "no": "NO",
}


def derive_power_lights(value) -> Optional[str]:
    """Map Power_Lights column to enum string."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    key = str(value).strip().lower()
    result = POWER_LIGHTS_MAP.get(key)
    if result is None:
        raise ValueError(f"Unrecognised Power_Lights value: {value!r}")
    return result


# ── Power Timer enum ───────────────────────────────────────────────────────────

def derive_power_timer_hr(value) -> Optional[str]:
    """
    Map Power_Timer_Hr to enum string.
    'Continuous' -> CONTINUOUS
    '1' .. '24'  -> HOURS_1 .. HOURS_24  (any integer 1-24)
    'No'         -> NO
    """
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    s = str(value).strip().lower()
    if s == "continuous":
        return "CONTINUOUS"
    if s == "no":
        return "NO"
    # Try to parse as an integer hour value 1-24
    try:
        hours = int(float(s))
        if 1 <= hours <= 24:
            return f"HOURS_{hours}"
        raise ValueError(f"Power_Timer_Hr numeric value out of range (1-24): {value!r}")
    except (ValueError, TypeError):
        pass
    raise ValueError(f"Unrecognised Power_Timer_Hr value: {value!r}")


# ── Entry Lock enum ────────────────────────────────────────────────────────────

ENTRY_LOCK_MAP = {
    "pad lock": "PAD_LOCK",
    "padlock": "PAD_LOCK",
    "smart lock": "SMART_LOCK",
    "rfid": "RFID",
    "bluetooth": "BLUETOOTH",
}


def derive_entry_lock(value) -> Optional[str]:
    """Map EntryLock column to enum string."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    key = str(value).strip().lower()
    result = ENTRY_LOCK_MAP.get(key)
    if result is None:
        raise ValueError(f"Unrecognised EntryLock value: {value!r}")
    return result


# ── Door Type ID ───────────────────────────────────────────────────────────────

DOOR_TYPE_MAP = {
    "rollup": 1,
    "roll up": 1,
    "swing": 2,
}


def derive_door_type_id(value) -> Optional[int]:
    """Map DoorType column to door_type_id integer."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    key = str(value).strip().lower()
    result = DOOR_TYPE_MAP.get(key)
    if result is None:
        raise ValueError(f"Unrecognised DoorType value: {value!r}")
    return result


# ── Mobile Unit enum ───────────────────────────────────────────────────────────

MOBILE_UNIT_MAP = {
    "pod": "POD",
    "shipping container": "SHIPPING_CONTAINER",
    "non-permanent structure": "NON_PERMANENT_STRUCTURE",
    "no": "NO",
}


def derive_mobile_unit(value) -> Optional[str]:
    """Map Mobile Unit column to enum string."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    key = str(value).strip().lower()
    result = MOBILE_UNIT_MAP.get(key)
    if result is None:
        raise ValueError(f"Unrecognised Mobile Unit value: {value!r}")
    return result


# ── Unit Status derivation ─────────────────────────────────────────────────────

def derive_unit_status_id(rentable, rented, reserved, maintenance) -> int:
    """
    Derive unit_status_id from four source columns in priority order:
      1. Rentable = Yes  -> 1 (Available)
      2. Rented   = Yes  -> 2 (Rented)
      3. Reserved = Yes  -> 4 (Reserved)
      4. Maintenance=Yes -> 5 (Maintenance)
      Default            -> 1 (Available)
    """
    def is_yes(v):
        return str(v).strip().lower() in ("yes", "y") if v is not None else False

    if is_yes(rentable):
        return 1
    if is_yes(rented):
        return 2
    if is_yes(reserved):
        return 4
    if is_yes(maintenance):
        return 5
    return 1  # default


# ── Rate / decimal ─────────────────────────────────────────────────────────────

def derive_rate(value) -> Optional[Decimal]:
    """Convert rate value to Decimal, returning None for blank/null."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except InvalidOperation:
        raise ValueError(f"Non-numeric rate value: {value!r}")


# ── Unit Access Hours ──────────────────────────────────────────────────────────

def derive_unit_access_hours(value) -> Optional[str]:
    """Pass through Unit Access Hours as a plain string."""
    if value is None or str(value).strip().lower() in ("nan", ""):
        return None
    return str(value).strip()


# ── Unit Size ID (ref table lookup) ────────────────────────────────────────────

def derive_unit_size_id(value, unit_size_map: dict, default_id: int) -> int:
    """
    Look up unit_size_id from the storentic.unit_size ref table map.

    Normalisation applied before lookup:
      - strip surrounding whitespace
      - lowercase
      - replace '*' with 'x'  (handles '10*30' style entries)
      - remove spaces         (handles '10 x 30' style entries)

    Returns default_id (the '0x0' row) when:
      - value is blank / null / NaN
      - no matching entry exists in the map
    """
    if value is None or str(value).strip().lower() in ("nan", ""):
        return default_id
    normalized = str(value).strip().lower().replace("*", "x").replace(" ", "")
    return unit_size_map.get(normalized, default_id)
