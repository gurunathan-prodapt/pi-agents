-- Header: BigQuery DDL for job_log table
-- Legacy Source: N/A (new log table)
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE TABLE IF NOT EXISTS `bert_dwh.job_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr INT66 NOT NULL,
    log_timestamp TIMESTAMP NOT NULL,
    message STRING NOT NULL,
    log_level STRING NOT NULL OPTIONS(description="e.g., INFO, WARN, ERROR")
);