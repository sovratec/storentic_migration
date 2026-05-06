-- V7__ledger_charges_external_charge_id.sql
-- Adds external_charge_id column to storentic.ledger_charges for SiteLink import deduplication.
-- Run this script against the target database BEFORE executing migrate_ledger_charges.py.
--
-- external_charge_id stores the SiteLink ChargeID as a string.
-- The partial unique index prevents duplicate imports on re-runs while still
-- allowing NULL external_charge_id for charges created natively in Storentic.

ALTER TABLE storentic.ledger_charges
    ADD COLUMN IF NOT EXISTS external_charge_id VARCHAR(100);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_charges_external_charge_id
    ON storentic.ledger_charges(external_charge_id)
    WHERE external_charge_id IS NOT NULL;
