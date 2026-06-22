-- BigQuery table for job control metadata
-- Replaces: part of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
CREATE TABLE `project.dataset.job_control` (
  eintrags_nr INT64,
  job_kennung STRING,
  script_name STRING,
  log_datei STRING,
  stichtag_info STRING,
  status STRING,
  created_at TIMESTAMP,
  finished_at TIMESTAMP
);