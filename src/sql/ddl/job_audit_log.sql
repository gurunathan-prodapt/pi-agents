-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- Target: BigQuery Audit Table Schema

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
  audit_id STRING,
  tab_name STRING,
  status STRING,
  type_code STRING,
  stichtag_from DATE,
  stichtag_to DATE,
  active_flag STRING,
  record_count INT64,
  job_kennung STRING,
  eintrags_nr STRING,
  wiederanlauf_wert STRING,
  run_timestamp TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(run_timestamp)
CLUSTER BY tab_name, status;