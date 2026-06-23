-- DDL for BigQuery table: your_project_id.your_dataset_id.job_status
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_status`
(
  job_nr INT64 OPTIONS(description="Unique job run number, linking to job_audit_log"),
  job_kennung STRING OPTIONS(description="Identifier for the type of job (e.g., 'ausd_bp_ta_apn_carmen')"),
  status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'OK', 'ERROR')"),
  updated_ts TIMESTAMP OPTIONS(description="Timestamp when the status was last updated")
)
OPTIONS(
  description="Table to track the current status of ongoing or last-run jobs."
);