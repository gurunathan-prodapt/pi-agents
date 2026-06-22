-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
  job_id STRING,
  job_name STRING,
  script_name STRING,
  status STRING,
  message STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  run_date DATE,
  error_code INT64,
  error_detail STRING
);