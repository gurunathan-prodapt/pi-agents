--
-- BigQuery DDL for the job_error_log table
-- Replaces error logging aspects of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
--

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_error_log` (
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run"),
    job_kenn_ung STRING NOT NULL OPTIONS(description="Job identifier from the legacy system (p_JobKennung)"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number from the legacy system (p_EintragsNr)"),
    error_code STRING OPTIONS(description="Error code, if available"),
    error_message STRING OPTIONS(description="Detailed error message"),
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    source_script STRING OPTIONS(description="Name of the BigQuery stored procedure or script that reported the error")
)
OPTIONS(
    description="Logs errors encountered during job execution"
);