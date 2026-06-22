--
-- BigQuery DDL for job_run_log
-- Replaces: logging functionality and temporary file output in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_run_log` (
  job_kennung STRING,
  eintrags_nr STRING,
  records_processed INT64,
  created_ts TIMESTAMP
);