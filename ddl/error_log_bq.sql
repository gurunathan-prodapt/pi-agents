-- BigQuery migration for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- DDL for a general error logging table.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset.error_log_bq` (
    `job_id` STRING,
    `run_id` STRING,
    `log_timestamp` TIMESTAMP,
    `error_message` STRING,
    `error_code` STRING,
    `severity` STRING,
    `component` STRING,
    `parameters` JSON
);