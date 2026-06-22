-- Header: BigQuery DDL for job_error_log table
-- Legacy Source: N/A (new error log table)
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE TABLE IF NOT EXISTS `bert_dwh.job_error_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr INT66 NOT NULL,
    error_timestamp TIMESTAMP NOT NULL,
    error_message STRING NOT NULL,
    stack_trace STRING
);