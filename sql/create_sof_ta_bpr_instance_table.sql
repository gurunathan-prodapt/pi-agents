-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- This file creates the source table for the data loading.
-- This is a placeholder schema based on usage in the stored procedure.
-- Actual schema might need adjustment based on full legacy table structure.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.sof_ta_bpr_instance`
(
    CNTRCT_ID STRING NOT NULL OPTIONS(description="Contract Identifier"),
    BPR_ID STRING NOT NULL OPTIONS(description="Base Product Identifier"),
    DWH_VERTRAG_ID INT64 NOT NULL OPTIONS(description="Data Warehouse Contract ID, used for restart logic")
    -- Add other columns from the original Oracle sof$ta_bpr_instance table here,
    -- including any columns that were part of original WHERE clauses
    -- e.g., GUELTIG_VON DATE, GUELTIG_BIS DATE, LADEDATUM DATE if date filtering is reintroduced
)
OPTIONS(
    description="Source table for basic product instances, migrated from Oracle sof$ta_bpr_instance"
);