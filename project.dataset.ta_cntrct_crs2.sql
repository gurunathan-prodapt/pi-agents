-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- This file defines the target table schema for ta_cntrct_crs2.
-- The specific columns should be derived from the analysis of d_ausd_v_ta_cntrct_crs2.sql.

CREATE TABLE IF NOT EXISTS `project.dataset.ta_cntrct_crs2` (
    contract_id STRING OPTIONS(description="Unique identifier for the contract"),
    contract_data JSON OPTIONS(description="Placeholder for contract-related data, to be refined from source SQL"),
    effective_date DATE,
    created_at TIMESTAMP OPTIONS(description="Timestamp when the record was created"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp when the record was last updated")
)
OPTIONS(
    description="Target table for processed contract data, migrated from legacy Oracle system."
);