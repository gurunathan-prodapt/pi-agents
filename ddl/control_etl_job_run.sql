-- BigQuery DDL for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: N/A (new control table)
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.control.etl_job_run` (
    job_id STRING,
    run_id STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING, -- e.g., 'SUCCESS', 'FAILED', 'RUNNING'
    message STRING,
    processed_row_count INT64,
    error_details JSON
);