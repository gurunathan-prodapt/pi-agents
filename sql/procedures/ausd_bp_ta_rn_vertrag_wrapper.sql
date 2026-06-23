-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Description: BigQuery SQL stored procedure replacing the KornShell wrapper script.
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_rn_vertrag_wrapper`(
  IN p_stichtag STRING, -- Optional: Processing date in DDMMYYYY format
  IN p_wiederanlaufWert INT64 -- Optional: Restart value
)
OPTIONS(
  description="Orchestration wrapper for contract snapshot processing, handling parameters, logging, and calling the core logic procedure."
)
BEGIN
  -- Declare local variables to mimic shell script variables and for audit logging
  DECLARE v_job_kennung STRING DEFAULT 'RN_VERTRAG';
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_sysdate_str STRING;
  DECLARE v_job_entry_nr INT64; -- Mimics DW_EintragsNr
  DECLARE v_log_file_name STRING;
  DECLARE v_error_message STRING;

  -- Determine system date and format it as DDMMYYYY
  SET v_sysdate_str = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default p_stichtag to system date if not provided
  SET v_stichtag = IFNULL(p_stichtag, v_sysdate_str);

  -- Default p_wiederanlaufWert to 0 if not provided
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Generate a unique job entry number for logging purposes
  SET v_job_entry_nr = CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S%f', CURRENT_TIMESTAMP()) AS INT64);
  SET v_log_file_name = 'r_ausd_bp_ta_rn_vertrag_' || v_stichtag || '_' || CAST(v_job_entry_nr AS STRING) || '.log';

  -- Parameter Validation
  IF v_wiederanlaufWert < 0 THEN
    SET v_error_message = 'Invalid restart value: Wiederanlaufwert cannot be negative.';
    INSERT INTO `project.dataset.job_audit` (job_entry_nr, job_kennung, status, error_nr, error_arg, log_ts, message, stichtag, sysdate_value, restart_value, log_file_name)
    VALUES (v_job_entry_nr, v_job_kennung, 'ERROR', 193, v_error_message, CURRENT_TIMESTAMP(), 'Job failed: ' || v_error_message, v_stichtag, v_sysdate_str, v_wiederanlaufWert, v_log_file_name);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  -- Log job start
  INSERT INTO `project.dataset.job_audit` (job_entry_nr, job_kennung, status, error_nr, error_arg, log_ts, message, stichtag, sysdate_value, restart_value, log_file_name)
  VALUES (v_job_entry_nr, v_job_kennung, 'STARTED', NULL, NULL, CURRENT_TIMESTAMP(), 'Job started.', v_stichtag, v_sysdate_str, v_wiederanlaufWert, v_log_file_name);

  -- Main processing block with error handling
  BEGIN
    -- Call the core kernel stored procedure
    CALL `project.dataset.k_ausd_bp_ta_rn_vertrag`(
      v_job_kennung,
      v_stichtag,
      v_job_entry_nr,
      v_wiederanlaufWert
    );

    -- Log job success
    INSERT INTO `project.dataset.job_audit` (job_entry_nr, job_kennung, status, error_nr, error_arg, log_ts, message, stichtag, sysdate_value, restart_value, log_file_name)
    VALUES (v_job_entry_nr, v_job_kennung, 'OK', NULL, NULL, CURRENT_TIMESTAMP(), 'Job completed successfully.', v_stichtag, v_sysdate_str, v_wiederanlaufWert, v_log_file_name);

  EXCEPTION WHEN ERROR THEN
    -- Capture error message and log job failure
    SET v_error_message = @@error.message;
    INSERT INTO `project.dataset.job_audit` (job_entry_nr, job_kennung, status, error_nr, error_arg, log_ts, message, stichtag, sysdate_value, restart_value, log_file_name)
    VALUES (v_job_entry_nr, v_job_kennung, 'ERROR', 192, v_error_message, CURRENT_TIMESTAMP(), 'Job failed with error: ' || v_error_message, v_stichtag, v_sysdate_str, v_wiederanlaufWert, v_log_file_name);
    -- Re-raise the error to propagate it to the caller/orchestrator
    RAISE;
  END;

END;