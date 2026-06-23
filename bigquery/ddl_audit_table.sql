-- Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

-- DDL for job_audit_table to log execution details.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_table` (
  job_name STRING NOT NULL,
  job_kennung STRING,
  eintrags_nr STRING,
  stichtag DATE,
  wiederanlauf_wert STRING,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  status STRING, -- e.g., 'STARTED', 'COMPLETED', 'FAILED'
  records_processed INT64,
  message STRING
);