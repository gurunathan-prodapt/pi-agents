-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Purpose: Table for logging messages and errors, replacing f_alis_msgerr.ksh functionality.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset.job_log` (
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING NOT NULL OPTIONS(description="Severity level of the log (e.g., 'INFO', 'WARNING', 'ERROR')"),
    job_run_id STRING OPTIONS(description="Unique identifier for the job execution instance"),
    job_name STRING OPTIONS(description="Name of the job that generated the log"),
    job_kennung STRING OPTIONS(description="Job identifier associated with the log"),
    eintrags_nr STRING OPTIONS(description="Entry number associated with the log"),
    message STRING NOT NULL OPTIONS(description="The log message content"),
    error_code INT64 OPTIONS(description="Error code (if applicable, from original ksh ErrNr)"),
    error_argument STRING OPTIONS(description="Error argument (if applicable, from original ksh ErrArg)")
)
PARTITION BY DATE(log_timestamp)
CLUSTER BY job_name, log_level;