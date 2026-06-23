-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
-- Description: DDL for the job control table.
CREATE TABLE `project.dataset.job_table` (
  job_kennung STRING NOT NULL,
  eintrags_nr STRING NOT NULL,
  tab_name STRING,
  active_flag BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);