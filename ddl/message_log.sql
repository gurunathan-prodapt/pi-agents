-- BigQuery DDL for message_log
-- Replaces Oracle table used by vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- This table stores detailed error log entries for job executions.

CREATE TABLE IF NOT EXISTS dw_is_error_management.message_log (
    log_id INT64 OPTIONS(description="Unique identifier for each log entry (auto-generated)"), -- Could be generated via SEQUENCE or UUID
    eintragsnr INT64 NOT NULL OPTIONS(description="Foreign key to message_table.eintragsnr"),
    log_type STRING NOT NULL OPTIONS(description="Type of log entry (e.g., 'FEHLER', 'INFO')"),
    fehler_nr INT64 OPTIONS(description="Error number (if applicable)"),
    zusatz1 STRING OPTIONS(description="Additional information 1 for the log entry"),
    zusatz2 STRING OPTIONS(description="Additional information 2 for the log entry"),
    logged_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was recorded")
)
PARTITION BY DATE(logged_at)
CLUSTER BY eintragsnr, log_type
OPTIONS(
    description="Table to store detailed log entries, particularly for errors."
);