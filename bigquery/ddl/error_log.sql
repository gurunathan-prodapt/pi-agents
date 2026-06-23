-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- Description: BigQuery DDL for the error log table, replacing legacy error handling.
CREATE TABLE IF NOT EXISTS `mydataset.error_log` (
    log_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred."),
    job_id STRING OPTIONS(description="Job ID associated with the error."),
    job_kennung STRING OPTIONS(description="JobKennung parameter at the time of error."),
    eintrags_nr STRING OPTIONS(description="EintragsNr parameter at the time of error."),
    error_message STRING NOT NULL OPTIONS(description="Description of the error."),
    severity STRING OPTIONS(description="Severity of the error (e.g., INFO, WARNING, ERROR, FATAL)."),
    stack_trace STRING OPTIONS(description="Optional stack trace or additional debugging information.")
)
OPTIONS(
    description="Table to log errors from ETL processes and stored procedures."
);