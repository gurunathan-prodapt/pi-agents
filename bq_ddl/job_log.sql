-- Migrated from vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_name STRING,
  tab_name STRING,
  error_nr INT64,
  error_msg STRING,
  record_count INT64,
  status_msg STRING,
  created_at TIMESTAMP
);