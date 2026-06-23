-- BigQuery migration for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- DDL for a general job tracking table.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset.job_tracking_bq` (
    `job_id` STRING,
    `run_id` STRING,
    `start_timestamp` TIMESTAMP,
    `end_timestamp` TIMESTAMP,
    `status` STRING, -- e.g., 'SUCCESS', 'FAILED', 'RUNNING'
    `processed_records` INT64,
    `input_parameters` JSON,
    `output_details` JSON
);