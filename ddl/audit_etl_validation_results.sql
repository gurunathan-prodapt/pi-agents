-- BigQuery DDL for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: N/A (new audit table)
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.audit.etl_validation_results` (
    job_id STRING,
    run_id STRING,
    validation_timestamp TIMESTAMP,
    validation_type STRING, -- e.g., 'row_count_check', 'data_type_check'
    status STRING, -- e.g., 'PASSED', 'FAILED'
    details JSON
);