-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- This file creates the dwtk_meldungen metadata table.
-- This is a placeholder schema based on usage in the stored procedure.
-- Actual schema might need adjustment based on full legacy table structure.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.dwtk_meldungen`
(
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job or process"),
    value STRING NOT NULL OPTIONS(description="Value associated with the job_kennung, e.g., a date in DDMMYYYY format")
)
OPTIONS(
    description="Metadata table migrated from Oracle isbert_schema.dwtk_meldungen"
);