-- File: stored_procedures/sp_raise_and_log_error.sql
-- Reusable error logging and raising procedure

CREATE OR REPLACE PROCEDURE `gcp-project-placeholder.dw_isbert_dataset.sp_raise_and_log_error`(
  IN p_job_name STRING,
  IN p_err_nr INT64,
  IN p_err_arg STRING,
  IN p_err_text STRING
)
BEGIN
  INSERT INTO `gcp-project-placeholder.dw_isbert_dataset.error_log`
  (
    job_name,
    error_nr,
    error_arg,
    error_text,
    created_at
  )
  VALUES
  (
    p_job_name,
    p_err_nr,
    p_err_arg,
    p_err_text,
    CURRENT_TIMESTAMP()
  );

  SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = CONCAT('FEHLER: 0 E ', CAST(p_err_nr AS STRING), ' ', p_err_arg);
END;