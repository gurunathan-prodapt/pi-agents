-- Target code for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
-- This BigQuery Stored Procedure replicates the error logging functionality of the original DWMSG_MeldeFehler shell function.

CREATE OR REPLACE PROCEDURE `project.dataset.DWMSG_MeldeFehler`(
  p_Eintragsnr STRING,
  error_type STRING,
  error_code INT64,
  message STRING
)
BEGIN
  -- This procedure logs error details into a BigQuery logging table.
  -- The schema of `project.dataset.migration_log` is assumed to have:
  -- log_timestamp TIMESTAMP, job_id STRING, log_level STRING, error_code INT64, message STRING
  INSERT INTO `project.dataset.migration_log` (
    log_timestamp,
    job_id,
    log_level,
    error_code,
    message
  )
  VALUES (
    CURRENT_TIMESTAMP(),
    p_Eintragsnr, -- Using p_Eintragsnr as job_id for logging context
    error_type,   -- 'E' for Error, 'W' for Warning, 'I' for Info, etc.
    error_code,
    message
  );
END;