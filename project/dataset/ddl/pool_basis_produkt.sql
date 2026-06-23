-- DDL for project.dataset.PoolBasisprodukt
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
--
-- This table's schema was not defined in detail in the source KornShell script or the provided design document.
-- The DDL below provides a minimal structure. Please define the actual schema for PoolBasisprodukt
-- based on the original d_ausd_bp_ta_bpr_beschr.sql script's table interactions.
-- A `stichtag_date` column is included as a common pattern for date partitioning/filtering.
--
CREATE TABLE IF NOT EXISTS `project.dataset.PoolBasisprodukt`
(
    id STRING OPTIONS(description="Placeholder ID for the PoolBasisprodukt table"),
    data STRING OPTIONS(description="Placeholder for actual data columns"),
    stichtag_date DATE OPTIONS(description="Date associated with the data, derived from the input Stichtag parameter.")
    -- Add actual business columns here based on d_ausd_bp_ta_bpr_beschr.sql
);