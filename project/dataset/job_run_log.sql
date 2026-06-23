-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
-- Description: DDL for the job run log table.
CREATE TABLE `project.dataset.job_run_log` (
  job_kennung STRING NOT NULL,
  eintrags_nr STRING NOT NULL,
  tab_name STRING,
  records_processed INT64,
  logged_at TIMESTAMP
);