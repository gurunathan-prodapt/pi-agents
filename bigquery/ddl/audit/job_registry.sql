-- DDL for job_registry table
-- Legacy Source: DW.BERT_AUSD_BP_TA_ICCID_VERTRAG
-- Purpose: To store job execution metadata.

CREATE TABLE IF NOT EXISTS `project.audit_dataset.job_registry` (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    job_name STRING NOT NULL OPTIONS(description="Name of the ETL job"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCEEDED', 'FAILED')"),
    parameters JSON OPTIONS(description="JSON object containing input parameters for the job"),
    processed_records INT64 OPTIONS(description="Number of records processed by the job"),
    error_message STRING OPTIONS(description="Details of any error encountered"),
    last_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of the last update to this record")
);