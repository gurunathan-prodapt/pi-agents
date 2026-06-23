-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- Description: DDL for the centralized error logging table in BigQuery.

CREATE TABLE IF NOT EXISTS project.dataset.error_log (
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the log entry was created."),
    job_id STRING NOT NULL OPTIONS(description="Identifier for the job that generated the log."),
    entry_number STRING OPTIONS(description="Entry number associated with the job run."),
    severity STRING OPTIONS(description="Severity level of the log entry, e.g., 'ERROR', 'WARNING', 'INFO'."),
    message STRING NOT NULL OPTIONS(description="Log message or error description."),
    procedure_name STRING OPTIONS(description="Name of the BigQuery stored procedure where the log originated.")
);