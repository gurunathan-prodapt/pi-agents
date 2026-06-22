-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
CREATE TABLE IF NOT EXISTS `isrpt.dw_job_log` (
  job_kennung STRING,
  eintrags_nr INT64,
  log_level STRING,
  err_nr INT64,
  err_arg STRING,
  log_text STRING,
  stichtag STRING,
  created_at TIMESTAMP
);