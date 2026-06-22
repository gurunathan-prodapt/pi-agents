-- Legacy Source: Logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
--
-- This DDL creates the BigQuery table to store job execution logs.
-- Replace `your_gcp_project.your_bq_dataset` with your actual GCP project ID and BigQuery dataset name.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_log` (
    job_run_id STRING NOT NULL OPTIONS(description="Unique ID for each job run, corresponding to JobKennung"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job (e.g., r_ausd_bp_ta_bpr_optionen.ksh)"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING NOT NULL OPTIONS(description="Log level (INFO, WARNING, ERROR, DEBUG)"),
    message STRING NOT NULL OPTIONS(description="Log message content"),
    stichtag DATE OPTIONS(description="Reference date for the job run (p_stichtag)"),
    wiederanlaufwert INT64 OPTIONS(description="Restart value for the job (p_wiederanlaufWert)"),
    process_id STRING OPTIONS(description="Identifier for the process within the job run (like shell $$)"),
    line_number INT64 OPTIONS(description="Line number in the source code where the error occurred, if applicable"),
    error_code STRING OPTIONS(description="Error code from the original script's error handling (ErrNr)"),
    error_arg STRING OPTIONS(description="Error argument from the original script's error handling (ErrArg)"),
    log_entry_id STRING NOT NULL OPTIONS(description="Unique ID for this log entry, similar to DW_EintragsNr")
)
PARTITION BY DATE(log_timestamp)
CLUSTER BY job_run_id, log_level;