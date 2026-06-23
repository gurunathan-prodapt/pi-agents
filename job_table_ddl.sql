-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- DDL for the job status tracking table.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_table` (
    job_id STRING NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'SUCCEEDED', 'FAILED'
    record_count INT64,
    error_message STRING,
    params JSON, -- Store input parameters as JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description="Log table for tracking ETL job status and metadata."
);