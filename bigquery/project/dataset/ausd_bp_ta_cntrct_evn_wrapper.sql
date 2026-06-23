-- BigQuery Stored Procedure: ausd_bp_ta_cntrct_evn_wrapper
-- Generated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_effective_stichtag STRING;
  DECLARE v_restart_value INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Simulate system date in DDMMYYYY format
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default restart value if not provided
  SET v_restart_value = IFNULL(p_wiederanlaufWert, 0);

  -- Default cutoff date to system date if not provided
  SET v_effective_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

  -- Validate required parameter
  IF v_effective_stichtag IS NULL OR v_effective_stichtag = '' THEN
    SET v_errnr = 193;
    SET v_errarg = 'Stichtag';
  END IF;

  IF v_errnr != 0 THEN
    INSERT INTO `project.dataset.job_log`
    (job_name, log_level, error_nr, error_arg, message, created_at)
    VALUES
    ('ausd_bp_ta_cntrct_evn', 'E', v_errnr, v_errarg, 'Required parameter missing', CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg, ' - Required parameter missing.');
  END IF;

  -- Simulate job number and log file creation
  -- (Assuming job_control table exists and job_nr is auto-incremented or managed)
  SET v_eintragsnr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_name = v_jobkennung
  );

  SET v_logdatei = CONCAT('job_', v_jobkennung, '_', CAST(v_eintragsnr AS STRING), '.log');

  INSERT INTO `project.dataset.job_control`
  (job_nr, job_name, script_name, log_file, stichtag_info, status, created_at)
  VALUES
  (
    v_eintragsnr,
    v_jobkennung,
    'ausd_bp_ta_cntrct_evn_wrapper',
    v_logdatei,
    v_sysdate,
    'RUNNING',
    CURRENT_TIMESTAMP()
  );

  BEGIN
    -- Job header log
    INSERT INTO `project.dataset.job_log`
    (job_nr, job_name, log_level, message, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'I',
     CONCAT('Job started. Stichtag=', v_effective_stichtag,
            ', RestartValue=', CAST(v_restart_value AS STRING)),
     CURRENT_TIMESTAMP());

    -- Downstream kernel logic invocation (placeholder for k_ausd_bp_ta_cntrct_evn.ksh equivalent)
    CALL `project.dataset.k_ausd_bp_ta_cntrct_evn_core`(
      v_jobkennung,
      v_effective_stichtag,
      v_eintragsnr,
      v_restart_value
    );

    INSERT INTO `project.dataset.job_log`
    (job_nr, job_name, log_level, message, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'I',
     'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
     CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_nr = v_eintragsnr
      AND job_name = v_jobkennung;

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_log`
    (job_nr, job_name, log_level, message, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'E',
     CONCAT('AppError: Abbruch - ', @@error.message),
     CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_nr = v_eintragsnr
      AND job_name = v_jobkennung;

    RAISE; -- Re-raise the error to propagate it
  END;
END;