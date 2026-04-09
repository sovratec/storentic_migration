# Field Mapping — Powdersville Self Storage Legacy Migration

**Source File:** `data/CustomerUnitSample.xlsx` (Sheet1)
**Target DB:** `storentic` PostgreSQL schema
**Total Source Rows:** 377 (all active rentals, `bRented = True`)
**Format:** All monetary values in Excel are decimal dollars (e.g. `225.00`). DB stores cents as `BIGINT` unless noted.

---

## Prerequisites & Dependency Order

The following migrations must run in this order due to foreign-key dependencies:

```
Step 1 → migrate_units.py              (no dependencies)
Step 2 → migrate_customers.py          (no dependencies)
          ↓
Step 3 → migrate_rental_agreements.py  (requires: customer + units records to exist)
Step 4 → migrate_customer_unit.py      (requires: customer + units records to exist)
Step 5 → migrate_customer_balances.py  (requires: customer records to exist)
```

Steps 3, 4, and 5 may run in any order after Steps 1 and 2.

---

## Entity 1 — `storentic.rental_agreements`

**Purpose:** Records the active lease agreement between a customer and a unit.
**Dedup Key:** `customer_id + unit_id` (composite — one active lease per customer-unit pair)
**Source Rows Used:** All rows (one rental agreement per row)

| # | Excel Column | DB Column | DB Type | Derivation Rule |
|---|---|---|---|---|
| 1 | `TenantID` | `customer_id` | `BIGINT NOT NULL` | `SELECT id FROM storentic.customer WHERE external_id = TenantID`; skip row + log error if not found |
| 2 | `sUnitName` | `unit_id` | `BIGINT NOT NULL` | `SELECT id FROM storentic.units WHERE unit_number = sUnitName AND location_id = :LOCATION_ID`; skip row + log error if not found |
| 3 | `.env → LOCATION_ID` | `facility_id` | `BIGINT NOT NULL` | Constant — read from environment variable `LOCATION_ID` |
| 4 | `dMovedIn` | `move_in_date` | `DATE NOT NULL` | Parse to `LocalDate`; skip row if null or unparseable |
| 5 | `dLease` | `lease_date` | `DATE NOT NULL` | Use `dLease`; fall back to `dMovedIn` if `dLease` is null |
| 6 | `dPaidThru` | `paid_through_date` | `DATE NOT NULL` | Parse to `LocalDate`; skip row if null |
| 7 | `dSchedOut` | `schedule_date` | `DATE NOT NULL` | Use `dSchedOut`; fall back to `dMovedIn` if null (no scheduled move-outs in current data) |
| 8 | `dMovedOut` | `move_out_date` | `DATE` (nullable) | Parse to `LocalDate`; set `NULL` if blank (all null in current data) |
| 9 | `dcRent` | `rental_rate_in_cents` | `BIGINT NOT NULL` | `int(round(dcRent * 100))` — convert dollars to integer cents |
| 10 | `dcSchedRent` | `schedule_rate_in_cents` | `BIGINT NOT NULL` | `int(round(dcSchedRent * 100))`; if value is 0 or null → copy `rental_rate_in_cents` |
| 11 | *(none)* | `rate_variance_in_cents` | `BIGINT NOT NULL` | Hardcoded `0` — no variance column in source |
| 12 | `dcSecDepPaid` | `security_deposit_in_cents` | `BIGINT` | `int(round(dcSecDepPaid * 100))`; default `0` if null |
| 13 | `dcInsurPremium` | `insurance_rate_in_cents` | `BIGINT` | `int(round(dcInsurPremium * 100))`; default `0` if null |
| 14 | `dcInsurPremium` | `insurance_option` | `ENUM (NONE, BASIC, PREMIUM)` | `BASIC` if `dcInsurPremium > 0`, otherwise `NONE` |
| 15 | `dMovedIn` | `charge_day` | `INT (1–30) NOT NULL` | Extract day-of-month from `dMovedIn` (e.g. `dMovedIn = 2012-07-27` → `charge_day = 27`) |
| 16 | *(none)* | `status` | `ENUM (ACTIVE, SUSPENDED, TERMINATED, EXPIRED)` | Hardcoded `ACTIVE` — all rows have `bRented = True` |
| 17 | *(none)* | `promo_code` | `VARCHAR(50)` | `NULL` — no promo data in source |
| 18 | *(none)* | `promo_discount_in_cents` | `BIGINT` | Hardcoded `0` |
| 19 | *(none)* | `notes` | `TEXT` | `NULL` |
| 20 | `.env → CREATED_BY` | `created_by` | `BIGINT NOT NULL` | From environment variable `CREATED_BY` |
| 21 | `.env → CREATED_BY` | `updated_by` | `BIGINT` | Same as `created_by` on initial load |
| 22 | *(auto)* | `version` | `BIGINT` | Hardcoded `1` |

**Excluded Excel Columns (not mapped):**
- `UnitID` — internal legacy key; replaced by `unit_id` lookup via `sUnitName + LOCATION_ID`
- `BillingFreqID` — billing frequency (3 = monthly); no direct DB column in `rental_agreements`
- `iLeaseNum` — legacy lease number; no equivalent column
- `sAccessCode` — mapped in `customer_unit` instead
- All `dcRecChg*` recurring charges — no mapping in this entity

---

## Entity 2 — `storentic.customer_balances`

**Purpose:** Stores the current financial balance snapshot for each customer.
**Dedup Key:** `customer_id` (unique constraint — one balance record per customer)
**Source Rows Used:** All rows (one balance record per tenant)

| # | Excel Column(s) | DB Column | DB Type | Derivation Rule |
|---|---|---|---|---|
| 1 | `TenantID` | `customer_id` | `BIGINT NOT NULL UNIQUE` | `SELECT id FROM storentic.customer WHERE external_id = TenantID`; skip row + log error if not found |
| 2 | Sum of: `dcRentBal`, `dcLateFee1Bal`, `dcLateFee2Bal`, `dcLateFee3Bal`, `dcLateFee4Bal`, `dcLateFee5Bal`, `dcNSFBal`, `dcAdminFeeBal`, `dcCutLockFeeBal`, `dcAuctionFeeBal`, `dcInsurBal`, `dcOtherBal`, `dcRecChg1Bal` – `dcRecChg8Bal` | `balance_in_cents` | `BIGINT NOT NULL` | Sum all listed columns (treat `NaN`/missing as `0`), then `int(round(total * 100))`; minimum `0` |
| 3 | `dcCreditBal` | `credit_balance_in_cents` | `BIGINT NOT NULL` | `int(round(dcCreditBal * 100))`; default `0` if null |
| 4 | `dPmtLast` | `last_payment_date` | `TIMESTAMP` | Parse to `Timestamp`; set `NULL` if blank |
| 5 | `dcPmtLastAmt` | `last_payment_amount_in_cents` | `BIGINT` | `int(round(dcPmtLastAmt * 100))`; default `0` if null |
| 6 | *(none)* | `total_invoiced_in_cents` | `BIGINT NOT NULL` | Hardcoded `0` — full invoice history not available in this snapshot export |
| 7 | *(none)* | `total_paid_in_cents` | `BIGINT NOT NULL` | Hardcoded `0` — full payment history not available in this snapshot export |
| 8 | `.env → ORGANIZATION_ID` | `organization_id` | `BIGINT NOT NULL` | From environment variable `ORGANIZATION_ID` |
| 9 | `.env → LOCATION_ID` | `location_id` | `BIGINT NOT NULL` | From environment variable `LOCATION_ID` |
| 10 | *(auto)* | `version` | `INT NOT NULL` | Hardcoded `0` (Hibernate optimistic locking default) |

**Note on `balance_in_cents` calculation:**
The source has separate balance columns per charge type. All must be summed to produce a single outstanding balance:
- Rent balance: `dcRentBal`
- Late fees: `dcLateFee1Bal` through `dcLateFee5Bal`
- NSF fees: `dcNSFBal`
- Admin / cut-lock / auction fees: `dcAdminFeeBal`, `dcCutLockFeeBal`, `dcAuctionFeeBal`
- Insurance balance: `dcInsurBal`
- Recurring charges: `dcRecChg1Bal` through `dcRecChg8Bal`
- Other: `dcOtherBal`

**Excluded Excel Columns (not mapped):**
- `dcRentTaxBal`, `dcOtherTaxBal`, `dcRecChgTaxBal`, `dcInsurTaxBal`, `dcPOSTaxBal` — tax portions; no separate tax-balance field in entity
- `dcPOSBal` — POS balance; no direct DB column
- `dcSecDepBal` — security deposit balance; no direct DB column in `customer_balances`

---

## Entity 3 — `storentic.customer_unit`

**Purpose:** Links a customer to a unit with rental rate and billing details. Parallel to `rental_agreements`; used by the current API for active tenancy lookups.
**Dedup Key:** `customer_id + unit_id` (composite)
**Source Rows Used:** All rows

| # | Excel Column | DB Column | DB Type | Derivation Rule |
|---|---|---|---|---|
| 1 | `TenantID` | `customer_id` | `BIGINT NOT NULL` | `SELECT id FROM storentic.customer WHERE external_id = TenantID`; skip row + log error if not found |
| 2 | `sUnitName` | `unit_id` | `BIGINT NOT NULL` | `SELECT id FROM storentic.units WHERE unit_number = sUnitName AND location_id = :LOCATION_ID`; skip row + log error if not found |
| 3 | `dLease` (or `dMovedIn`) | `lease_date` | `DATE` | Use `dLease`; fall back to `dMovedIn` if null |
| 4 | `dMovedOut` | `move_out_date` | `DATE` (nullable) | Null for all current rows |
| 5 | `dPaidThru` | `paid_through_date` | `DATE` | Parse to `LocalDate` |
| 6 | `dcRent` | `rental_rate` | `DECIMAL(12,2)` | Direct decimal value — stored as dollars (not cents) |
| 7 | *(none)* | `rate_variance` | `DECIMAL(12,2)` | Hardcoded `0.00` |
| 8 | `dcSchedRent` | `scheduled_rate` | `DECIMAL(12,2)` | Direct decimal; copy `rental_rate` if 0 or null |
| 9 | `dSchedOut` | `scheduled_date` | `DATE` (nullable) | Null for all current rows |
| 10 | `dMovedIn` | `charge_day` | `INT` | Extract day-of-month from `dMovedIn` |
| 11 | `sAccessCode` | `access_code` | `VARCHAR(10)` | Direct string passthrough; truncate to 10 chars if longer |
| 12 | *(none)* | `rental_contract_signed` | `BOOLEAN` | Hardcoded `false` — no signature data in source |
| 13 | `.env → CREATED_BY` | `created_by` | `BIGINT NOT NULL` | From environment variable `CREATED_BY` |
| 14 | `.env → CREATED_BY` | `updated_by` | `BIGINT` | Same as `created_by` on initial load |

**Excluded Excel Columns (not mapped):**
- `sAccessCode2`, `iKeypadZ`, `AccessID` — secondary/keypad access; no columns in `customer_unit`
- `iLeaseNum`, `LedgerID` — legacy IDs; no equivalent columns
- `BillingFreqID` — billing frequency; not stored in `customer_unit`
- `dcInsurPremium` — insurance stored in `rental_agreements` instead

---

## Columns Intentionally Not Migrated (any entity)

| Excel Column | Reason |
|---|---|
| `sCreditCardNum`, `dCreditCardExpir`, `sCreditCardCVV2` | PCI sensitive — never migrate raw card data |
| `sACH_ABA_RoutingNum`, `sACH_CheckWriterAcctNum` | ACH sensitive — banking data must not be bulk-imported |
| `sSSN` | PII — Social Security Number; excluded |
| `sWebPassword`, `sWebSecurityQ`, `sWebSecurityQA` | Legacy passwords — excluded |
| `sPicFileN1`–`sPicFileN9` | File references only; no binary data available |
| `dcRentTaxBal`, `dcPOSBal`, `dcSecDepBal` | No matching DB column in any target entity |
| `MktgDistanceID`, `MktgWhatID`, `MktgReasonID`, etc. | Marketing analytics — no target entity |
| `bi_Tenant_GlobalNum`, `iGlobalNum_*` | Multi-site chain IDs — not applicable |
| `sRFID`, `sTrackingCode` | Hardware tracking — no DB column |
| `TokenID` | Legacy access token — no DB column |
