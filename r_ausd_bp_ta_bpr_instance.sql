-- BigQuery SQL for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- This procedure orchestrates the initial provisioning of selected basic products for BERT.
-- It handles parameter parsing, environment setup, error handling, and delegates
-- core processing logic to the k_ausd_bp_ta_bpr_instance procedure.

CREATE SCHEMA IF NOT EXISTS `project.dataset`;

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_instance`(
  IN p_stichtag STRING,          -- DDMMYYYY, nullable
  IN p_wiederanlaufwert INT64    -- nullable, defaults to 0
)
BEGIN
  DECLARE v_prog_name STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
  DECLARE v_prog_version STRING DEFAULT 'V2.0.0';
  DECLARE v_job_name STRING DEFAULT 'ausd_bp_ta_bpr_instance';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE v_stichtag STRING;
  DECLARE v_restart INT64 DEFAULT 0;
  DECLARE v_job_nr INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_msg STRING;

  -- Default restart value
  SET v_restart = IFNULL(p_wiederanlaufwert, 0);

  -- Default cutoff date to system date if missing
  SET v_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- Validate cutoff date format
  IF NOT REGEXP_CONTAINS(v_stichtag, r'^\d{8}$') THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Stichtag must be in DDMMYYYY format';
  END IF;

  -- Assign job number only after validation passes
  IF v_err_nr = 0 THEN
    SET v_job_nr = (
      SELECT IFNULL(MAX(job_nr), 0) + 1
      FROM `project.dataset.job_log`
      WHERE job_name = v_job_name
    );
  END IF;

  -- Handle validation failure
  IF v_err_nr != 0 THEN
    INSERT INTO `project.dataset.job_log`
      (job_name, job_nr, log_level, message, stichtag, restart_value, created_at)
    VALUES
      (v_job_name, v_job_nr, 'E',
       CONCAT('Parameter validation failed: ', v_err_arg),
       v_stichtag, v_restart, CURRENT_TIMESTAMP());

    INSERT INTO `project.dataset.job_status`
      (job_name, job_nr, status, updated_at)
    VALUES
      (v_job_name, v_job_nr, 'ERROR', CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('AppError: Abbruch. Error Message: ', v_err_arg);
  END IF;

  -- Log job start
  SET v_msg = CONCAT(
    'Job start: ', v_prog_name, ' ', v_prog_version,
    ', Stichtag: ', v_stichtag,
    ', Wiederanlaufwert: ', CAST(v_restart AS STRING)
  );

  INSERT INTO `project.dataset.job_log`
    (job_name, job_nr, log_level, message, stichtag, restart_value, created_at)
  VALUES
    (v_job_name, v_job_nr, 'I', v_msg, v_stichtag, v_restart, CURRENT_TIMESTAMP());

  INSERT INTO `project.dataset.job_metadata`
    (job_name, job_nr, log_file_name, sysdate_ddmmyyyy, stichtag_ddmmyyyy, restart_value, created_at)
  VALUES
    (v_job_name, v_job_nr, CONCAT(v_job_name, '_', CAST(v_job_nr AS STRING), '.log'),
     v_sysdate, v_stichtag, v_restart, CURRENT_TIMESTAMP());

  INSERT INTO `project.dataset.job_status`
    (job_name, job_nr, status, updated_at)
  VALUES
    (v_job_name, v_job_nr, 'RUNNING', CURRENT_TIMESTAMP());

  -- Delegate core business processing to downstream kernel procedure
  BEGIN
    CALL `project.dataset.k_ausd_bp_ta_bpr_instance`(
      v_job_name,
      v_stichtag,
      v_job_nr,
      v_restart
    );

    INSERT INTO `project.dataset.job_log`
      (job_name, job_nr, log_level, message, stichtag, restart_value, created_at)
    VALUES
      (v_job_name, v_job_nr, 'I',
       'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
       v_stichtag, v_restart, CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_status`
    SET status = 'OK',
        updated_at = CURRENT_TIMESTAMP()
    WHERE job_name = v_job_name
      AND job_nr = v_job_nr;

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_log`
      (job_name, job_nr, log_level, message, stichtag, restart_value, created_at)
    VALUES
      (v_job_name, v_job_nr, 'E',
       CONCAT('AppError: Abbruch. Error Message: ', @@error.message),
       v_stichtag, v_restart, CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_status`
    SET status = 'ERROR',
        updated_at = CURRENT_TIMESTAMP()
    WHERE job_name = v_job_name
      AND job_nr = v_job_nr;

    RAISE USING MESSAGE = CONCAT('AppError: Abbruch. Error Message: ', @@error.message);
  END;

END;