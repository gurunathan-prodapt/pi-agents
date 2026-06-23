-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for BigQuery audit table to store job error logs.
CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job, e.g., 'BERT_V_TA_P_DISCOUNT_RR'"),
    entry_nr INT64 NOT NULL OPTIONS(description="Sequential entry number for the job run"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error was logged"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    component STRING OPTIONS(description="Component where the error occurred (e.g., 'WRAPPER', 'CORE')"),
    error_code STRING OPTIONS(description="Optional error code or type")
)
OPTIONS(
    description="Table to store error logs for batch jobs, replacing file-based error logging."
);