-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh

-- Create logging table as specified in the design document
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  eintragsnr INT64,
  job_kennung STRING,
  log_level STRING,
  err_nr INT64,
  err_arg STRING,
  message STRING,
  stichtag STRING,
  restart_value STRING,
  created_at TIMESTAMP
);

-- Create job status table as specified in the design document
CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
  eintragsnr INT64,
  job_kennung STRING,
  status STRING,
  updated_at TIMESTAMP
);