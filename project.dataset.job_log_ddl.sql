-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for BigQuery audit table to store general job logs.
CREATE TABLE IF NOT EXISTS project.dataset.job_log (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job, e.g., 'BERT_V_TA_P_DISCOUNT_RR'"),
    entry_nr INT64 NOT NULL OPTIONS(description="Sequential entry number for the job run"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created"),
    log_message STRING NOT NULL OPTIONS(description="The actual log message"),
    log_level STRING OPTIONS(description="Severity level of the log message (e.g., 'INFO', 'WARN', 'DEBUG')")
)
OPTIONS(
    description="Table to store general log messages for batch jobs, replacing file-based logging."
);