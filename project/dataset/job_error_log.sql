-- BigQuery table for job error logs
-- Replaces: part of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
CREATE TABLE `project.dataset.job_error_log` (
  job_kennung STRING,
  eintrags_nr INT64,
  err_nr INT64,
  err_arg STRING,
  created_at TIMESTAMP
);