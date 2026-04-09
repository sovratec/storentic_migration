# storentic-migration

Python ETL pipeline for loading legacy unit and customer data into the Storentic PostgreSQL database.

---

## Project Structure

```
storentic-migration/
├── migrate.py                # Unified entry point (dispatches to units or customers)
├── migrate_units.py          # Unit ETL orchestrator
├── migrate_customers.py      # Customer ETL orchestrator
├── requirements.txt
├── .env.example              # Copy to .env and fill in credentials
├── data/
│   ├── Units.xlsx            # Place source Units Excel file here
│   └── CustomerUnitSample.xlsx  # Place source Customers Excel file here
├── logs/                     # Auto-generated per run
│   ├── migration_<ts>.log
│   ├── errors_<ts>.csv
│   └── summary_<ts>.txt
├── output/                   # Excel output files (--output excel mode)
├── scripts/
│   ├── db.py                         # DB connection utility
│   ├── logger.py                     # Logging setup
│   ├── transformer.py                # Unit field mapping & business logic
│   ├── customer_transformer.py       # Customer field mapping & business logic
│   └── validator.py                  # Pre-flight source file validation
└── sql/
    ├── V2__units_legacy_migration_columns.sql      # Unit enum types & columns
    ├── V3__customer_legacy_migration_columns.sql   # Customer legacy columns
    └── V4__unit_size_id_column.sql                 # Adds unit_size_id FK column
```

---

## Prerequisites

### 1. Apply DB Schema Changes

Run the SQL migrations in order against your PostgreSQL database before running the ETL:

```sql
-- Adds enum types + legacy columns to storentic.units
V2__units_legacy_migration_columns.sql

-- Adds legacy columns to storentic.customer
V3__customer_legacy_migration_columns.sql

-- Adds unit_size_id (FK to storentic.unit_size) to storentic.units
V4__unit_size_id_column.sql
```

### 2. Populate the `storentic.unit_size` Reference Table

The Units migration looks up `unit_size_id` from `storentic.unit_size` using the `value` column
(e.g. `"10x30"`, `"5x10"`). A row with `value = '0x0'` must exist — it is used as the default
when no matching size is found.

### 3. Update Units.java Entity (storentic-api)

Add the following new fields to your `Units.java` entity:

- `unitAccess` (UnitAccess enum)
- `power` (Boolean)
- `powerLights` (PowerLights enum)
- `powerTimerHr` (PowerTimerHr enum)
- `unitAccessHours` (String)
- `entryLock` (EntryLock enum)
- `mobileUnit` (MobileUnit enum)
- `boxTruckRental` (Boolean)
- `onSiteResident` (Boolean)
- `weeklyRate` (BigDecimal)
- `unitSizeId` (Integer, FK → storentic.unit_size)

---

## Setup

```bash
# 1. Clone or copy this folder
cd storentic-migration

# 2. Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your DB credentials, ORGANIZATION_ID, and LOCATION_ID

# 5. Place source files
cp /path/to/Units.xlsx data/Units.xlsx
cp /path/to/Customers.xlsx data/CustomerUnitSample.xlsx
```

---

## Running the Migration

### Units

```bash
# Dry run — preview only, no DB writes
python migrate_units.py --file data/Units.xlsx --output db --dry-run

# Live run — upserts every row (insert if new, update if unit_number + location already exists)
python migrate_units.py --file data/Units.xlsx --output db

# Write transformed data to Excel instead of DB (useful for QA review)
python migrate_units.py --file data/Units.xlsx --output excel
```

### Customers

```bash
# Dry run
python migrate_customers.py --file data/CustomerUnitSample.xlsx --output db --dry-run

# Live run — upserts every row (insert if new, update if TenantId already exists)
python migrate_customers.py --file data/CustomerUnitSample.xlsx --output db

# Excel preview
python migrate_customers.py --file data/CustomerUnitSample.xlsx --output excel
```

### Via unified entry point

```bash
python migrate.py --type unit     --file data/Units.xlsx --output db
python migrate.py --type customer --file data/CustomerUnitSample.xlsx --output db
```

### Test DB connection only

```bash
python scripts/db.py
```

---

## Upsert Behaviour

Both migrations use **upsert** — no manual `--update` flag is required:

| Migration | Dedup Key | On duplicate |
|---|---|---|
| Units | `unit_number + organization_id + location_id` | UPDATE |
| Customers | `external_id` (TenantId from Excel) | UPDATE |

If the record does not exist it is INSERTed. If it does exist it is UPDATEd with the current Excel values.

---

## Output

After each run, check the `logs/` folder:

| File | Description |
|---|---|
| `migration_<ts>.log` | Full run log with per-row status (✅ Inserted / 🔄 Updated / ❌ Error) |
| `errors_<ts>.csv` | Rows that failed with field, error, and original value |
| `summary_<ts>.txt` | Final counts: inserted / updated / errors |
| `validation_report.txt` | Column mapping confirmed at start of run |

---

## Units Field Mapping Summary

| Excel Column | DB Column | Rule |
|---|---|---|
| UnitName | unit_number | Direct |
| Bldg | building | Direct |
| Type | unit_type_id | Climate Control→1, Apartment→4, Vehicle/RV/Boat→5 |
| Type | climate_controlled | `'Climate Control'` → true |
| Entry Location | inside | `'Interior'` → true |
| Unit Access | unit_access | Enum mapping |
| Floor | floor_level | Integer |
| Power | power | Yes/No → bool |
| Power_ElectricalOutlet | power_outlet | Yes/No → bool |
| Power_Lights | power_lights | Enum mapping |
| Power_Timer_Hr | power_timer_hr | `CONTINUOUS` \| `HOURS_1..HOURS_24` \| `NO` |
| Unit Access Hours | unit_access_hours | String passthrough |
| EntryLock | entry_lock | Enum mapping |
| DoorType | door_type_id | Rollup→1, Swing→2 |
| Alarm | alarm | Yes/No → bool |
| ADA | ada | Yes/No → bool (nullable) |
| Area | unit_area | String passthrough |
| UnitSize | unit_size_id | Lookup in `storentic.unit_size` by `value`; defaults to `0x0` row if no match |
| StandardRate | standard_rate | Decimal |
| PushRate | push_rate | Decimal |
| WeeklyRate | weekly_rate | Decimal |
| Rentable/Rented/Reserved/Maintenance | unit_status_id | Priority logic → 1/2/4/5 |
| Box Truck Rental | box_truck_rental | Yes/No → bool (nullable) |
| Mobile Unit | mobile_unit | Enum mapping |
| On-Site Resident | on_site_resident | Yes/No → bool (nullable) |
| Width, Length, Zone, Climate, SecurityDeposit | — | Intentionally excluded |

---

## Customers Field Mapping Summary

| Excel Column | DB Column | Rule |
|---|---|---|
| TenantId | external_id | Direct (also the dedup key) |
| sFname + sMI | first_name | Concatenated with space if MI present |
| sLname | last_name | Direct (required — row rejected if blank) |
| sCompany | company_name | Direct |
| sAddr1–sPostalCode | address1–zip | Direct |
| sCountry | country | Direct |
| sPhone | phone | Direct |
| sFNameAlt + sMIAlt | alternate_first_name | Same concat logic as first_name |
| sLnameAlt–sPhoneAlt | alternate_last_name–alternate_phone | Direct |
| sEmail / sEmailAlt | email / alternate_email | Direct |
| sMobile | mobile | Direct |
| sAccessCode | access_gate_code | Direct |
| sLicense | driver_license_id | Direct |
| sLicRegion | driver_license_issue_state | Direct |

---

## Re-running After Errors

1. Open `logs/errors_<ts>.csv`
2. Fix the source data in the Excel file
3. Re-run — the migration will automatically UPDATE any rows that were previously inserted

The migration is **fully idempotent**: re-running against the same data is safe and will update existing records with fresh values.
