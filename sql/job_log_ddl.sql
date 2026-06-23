-- DDL for the job_log table
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_name STRING,
  status STRING,
  error_nr INT64,
  error_arg STRING,
  stichtag DATE,
  records_processed INT64,
  created_at TIMESTAMP
);