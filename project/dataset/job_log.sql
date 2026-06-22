-- BigQuery table for job log messages
-- Replaces: part of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
CREATE TABLE `project.dataset.job_log` (
  eintrags_nr INT64,
  job_kennung STRING,
  log_level STRING,
  message STRING,
  created_at TIMESTAMP
);