-- BigQuery Stored Procedure for the job orchestrator
-- Replaces legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_progname STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
  DECLARE v_progversion STRING DEFAULT 'V2.0.0';
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_bpr_beschr';
  DECLARE v_dweintragsnr INT64 DEFAULT 0;
  DECLARE v_sysdate STRING;
  DECLARE v_effective_stichtag STRING;
  DECLARE v_restartwert INT64;
  DECLARE v_log_message STRING;
  DECLARE v_error_message STRING;

  SET v_restartwert = IFNULL(p_wiederanlaufWert, 0);
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  SET v_effective_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- Determine initial job number before logging the start
  SET v_dweintragsnr = (
    SELECT IFNULL(MAX(job_number), 0) + 1 FROM `project.dataset.job_log` WHERE job_name = v_jobkennung
  );

  -- Log job start
  INSERT INTO `project.dataset.job_log`
  (job_name, job_version, job_number, log_level, log_message, created_at)
  VALUES
  (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', 'Job started', CURRENT_TIMESTAMP());

  -- Parameter validation
  IF v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = '' THEN
    SET v_error_message = 'Stichtag parameter missing or empty after default calculation';
    INSERT INTO `project.dataset.job_log`
    (job_name, job_version, job_number, log_level, log_message, created_at)
    VALUES
    (v_jobkennung, v_progversion, v_dweintragsnr, 'ERROR', v_error_message, CURRENT_TIMESTAMP());
    SELECT ERROR('Required parameter Stichtag is missing');
  END IF;

  INSERT INTO `project.dataset.job_log`
  (job_name, job_version, job_number, log_level, log_message, created_at)
  VALUES
  (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', CONCAT('Job number assigned: ', v_dweintragsnr), CURRENT_TIMESTAMP());

  INSERT INTO `project.dataset.job_log`
  (job_name, job_version, job_number, log_level, log_message, created_at)
  VALUES
  (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', CONCAT('Stichtag=', v_effective_stichtag, ', Sysdate=', v_sysdate, ', Wiederanlaufwert=', v_restartwert), CURRENT_TIMESTAMP());

  BEGIN
    -- Call downstream business procedure
    CALL `project.dataset.ausd_bp_ta_bpr_beschr_core`(
      v_jobkennung,
      v_effective_stichtag,
      v_dweintragsnr,
      v_restartwert
    );

    INSERT INTO `project.dataset.job_log`
    (job_name, job_version, job_number, log_level, log_message, created_at)
    VALUES
    (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', 'Job completed successfully', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Capture the error message and log it
    SET v_error_message = @@error.message;
    INSERT INTO `project.dataset.job_log`
    (job_name, job_version, job_number, log_level, log_message, created_at, error_code)
    VALUES
    (v_jobkennung, v_progversion, v_dweintragsnr, 'ERROR', CONCAT('AppError: Abbruch. Details: ', v_error_message), CURRENT_TIMESTAMP(), 'APP_ERROR');
    SELECT ERROR('AppError: Abbruch'); -- Re-raise the error to indicate job failure
  END;
END;