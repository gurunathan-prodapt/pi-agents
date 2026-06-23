-- DDL for BigQuery table: your_project_id.your_dataset_id.job_audit_log
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_audit_log`
(
  job_nr INT64 OPTIONS(description="Unique job run number, often incremented per job_kennung"),
  job_kennung STRING OPTIONS(description="Identifier for the type of job (e.g., 'ausd_bp_ta_apn_carmen')"),
  source_name STRING OPTIONS(description="Name of the source component (e.g., 'sp_ausd_bp_ta_apn_carmen')"),
  log_ref STRING OPTIONS(description="Reference to a log file or other logging artifact"),
  stichtag STRING OPTIONS(description="Processing date (Stichtag) in DDMMYYYY format"),
  sysdate_ddmmyyyy STRING OPTIONS(description="System date when the entry was created in DDMMYYYY format"),
  status STRING OPTIONS(description="Status of the job run (e.g., 'STARTED', 'SUCCESS', 'FAILED')"),
  created_ts TIMESTAMP OPTIONS(description="Timestamp when the audit record was created"),
  message STRING OPTIONS(description="Additional message or description for the audit entry")
)
PARTITION BY DATE(created_ts)
CLUSTER BY job_kennung, status
OPTIONS(
  description="Audit log for job executions, tracking start, end, and key parameters."
);