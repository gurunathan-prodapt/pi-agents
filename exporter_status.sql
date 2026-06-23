-- BigQuery DDL for exporter_status table
-- Replaces: DWH monitoring (dwh$ta_k_meldungen) for job vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
-- This table tracks the overall status and last run details for exporter jobs.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset.exporter_status` (
    job_name STRING NOT NULL OPTIONS(description="Unique identifier for the exporter job, e.g., 'r_exis_v2'"),
    last_run_id STRING OPTIONS(description="The unique identifier of the last completed or attempted run"),
    status STRING NOT NULL OPTIONS(description="Current or last known status of the job (e.g., 'SUCCESS', 'FAILED', 'RUNNING', 'SKIPPED')"),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the last run started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the last run ended"),
    duration_seconds INT OPTIONS(description="Duration of the last run in seconds"),
    error_message STRING OPTIONS(description="Detailed error message if the last run failed"),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_name) NOT ENFORCED
);