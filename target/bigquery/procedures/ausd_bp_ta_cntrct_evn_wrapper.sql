--
-- BigQuery Stored Procedure: project.dataset.ausd_bp_ta_cntrct_evn_wrapper
-- Replaces orchestration logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`(
  IN p_stichtag_str STRING, -- Input as string "DDMMYYYY"
  IN p_wiederanlaufWert_input INT64 -- Input as integer, can be NULL
)
BEGIN
  DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
  DECLARE v_stichtag DATE;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
  DECLARE v_job_nr INT64;
  DECLARE v_error_message STRING;

  -- Default restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert_input, 0);

  -- Determine cutoff date
  IF p_stichtag_str IS NULL OR TRIM(p_stichtag_str) = '' THEN
    SET v_stichtag = v_sysdate;
  ELSE
    -- Handle potential parsing errors gracefully if required by original logic.
    -- For now, PARSE_DATE will error on invalid format.
    SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_str);
  END IF;

  -- Validate required parameter
  IF v_stichtag IS NULL THEN
    SET v_error_message = 'Required parameter Stichtag is missing or invalid (expected DDMMYYYY).';
    INSERT INTO `project.dataset.job_log` (job_kennung, log_ts, log_level, message)
    VALUES (v_jobkennung, CURRENT_TIMESTAMP(), 'ERROR', v_error_message);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  -- Generate job number
  -- This approach assumes a simple increment. For production, consider a
  -- more robust mechanism for job_nr generation (e.g., sequence table, UUID).
  SET v_job_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `project.dataset.job_control` WHERE job_kennung = v_jobkennung);
  -- If job_control is empty, MAX returns NULL, so +1 results in 1, which is fine.

  -- Log job start
  INSERT INTO `project.dataset.job_log`
  (job_nr, job_kennung, log_ts, log_level, message, stichtag, restart_value)
  VALUES
  (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'INFO',
   'Job started', v_stichtag, v_wiederanlaufWert);

  -- Insert initial job control record
  INSERT INTO `project.dataset.job_control` (job_nr, job_kennung, start_ts, status)
  VALUES (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'RUNNING');

  BEGIN
    -- Call the core processing procedure
    CALL `project.dataset.ausd_bp_ta_cntrct_evn_core`(
      v_jobkennung,
      v_stichtag,
      v_job_nr,
      v_wiederanlaufWert
    );

    -- Log job success
    INSERT INTO `project.dataset.job_log`
    (job_nr, job_kennung, log_ts, log_level, message)
    VALUES
    (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'INFO',
     'Die Abarbeitung wurde ohne erkennbare Fehler beendet');

    -- Update job control record for success
    UPDATE `project.dataset.job_control`
    SET status = 'OK', end_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr AND job_kennung = v_jobkennung;

  EXCEPTION WHEN ERROR THEN
    SET v_error_message = CONCAT('AppError: Abbruch - ', @@error.message);
    -- Log job error
    INSERT INTO `project.dataset.job_log`
    (job_nr, job_kennung, log_ts, log_level, message, error_message)
    VALUES
    (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'ERROR', 'Job failed', v_error_message);

    -- Update job control record for error
    UPDATE `project.dataset.job_control`
    SET status = 'ERROR', end_ts = CURRENT_TIMESTAMP(), error_message = v_error_message
    WHERE job_nr = v_job_nr AND job_kennung = v_jobkennung;

    RAISE; -- Re-raise the exception to signal failure
  END;

END;