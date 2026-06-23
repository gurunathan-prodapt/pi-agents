-- BigQuery DDL for job_audit_log table
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
  job_name STRING,
  stichtag STRING,
  wiederanlaufwert INT64,
  sysdate_value STRING,
  status STRING,
  created_at TIMESTAMP
);