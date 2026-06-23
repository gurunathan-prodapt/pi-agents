-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Log table for job execution.

CREATE TABLE `project_id.dataset_id.job_log_table` (
  job_nr INT64,
  job_kennung STRING,
  log_file STRING,
  message STRING,
  created_at TIMESTAMP,
  status STRING
);