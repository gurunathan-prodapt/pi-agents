-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- This file creates the target table for the data loading.
-- This is a placeholder schema based on usage in the stored procedure.
-- Actual schema might need adjustment based on full legacy table structure.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen`
(
    CNTRCT_ID STRING NOT NULL OPTIONS(description="Contract Identifier"),
    BPR_ID STRING NOT NULL OPTIONS(description="Base Product Identifier")
    -- Add other columns from the original Oracle sof$ta_bpr_optionen table here
)
OPTIONS(
    description="Target table for basic product options, migrated from Oracle sof$ta_bpr_optionen"
);