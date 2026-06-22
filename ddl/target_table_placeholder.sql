-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

-- Placeholder DDL for the target FOS table.
-- The exact schema needs to be defined based on the analysis of `k_ausd_bp_ta_bpr_instance.ksh`.
-- This example includes the DWH_VERTRAG_ID column, critical for the MERGE and DELETE operations.
CREATE TABLE IF NOT EXISTS `project.dataset.target_table` (
    DWH_VERTRAG_ID INT64 OPTIONS(description="Contract ID, primary key or unique identifier"),
    -- Add other columns from the actual target schema here
    target_column_1 STRING,
    target_column_2 NUMERIC,
    last_updated_at TIMESTAMP
);