--
-- BigQuery DDL for job_metadata_log table
-- Replaces metadata logging functionality (e.g., log file name, stichtag info) from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
--
CREATE TABLE IF NOT EXISTS project.dataset.job_metadata_log (
    run_id STRING NOT NULL OPTIONS(description="Foreign key to job_run_log, linking to the specific job execution."),
    meta_key STRING NOT NULL OPTIONS(description="Key for the metadata entry (e.g., 'log_file_name', 'stichtag_raw', 'wiederanlauf_wert_raw', 'stichtag_processed')."),
    meta_value STRING OPTIONS(description="Value of the metadata entry.")
);