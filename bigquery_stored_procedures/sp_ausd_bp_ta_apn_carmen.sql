-- BigQuery Stored Procedure: your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_ausd_bp_ta_apn_carmen`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
OPTIONS(
  description="BigQuery Stored Procedure for orchestration and parameter handling of BERT basic product provisioning.
               Replaces r_ausd_bp_ta_apn_carmen.ksh.
               Receives Stichtag (DDMMYYYY) and Wiederanlaufwert (integer) as input.
               Manages job logging, error handling, and invokes the core processing logic."
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_apn_carmen';
  DECLARE v_job_nr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Initialize restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Current system date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default cutoff date if not provided
  SET v_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- Validate required parameter
  IF v_stichtag IS NULL OR TRIM(v_stichtag) = '' THEN
    SET v_errnr = 193;
    SET v_errarg = 'Stichtag';
  END IF;

  -- Error handling (simulated DWMSG_MeldeFehler for initial validation)
  IF v_errnr != 0 THEN
    INSERT INTO `your_project_id.your_dataset_id.job_error_log`
    (
      job_kennung,
      error_nr,
      error_arg,
      log_ts,
      message
    )
    VALUES
    (
      v_jobkennung,
      v_errnr,
      v_errarg,
      CURRENT_TIMESTAMP(),
      'Required parameter missing or invalid'
    );

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg, ' - Required parameter missing or invalid');
  END IF;

  -- Create job number and log file reference equivalent (simulated DWMSG_ErmittleNr, DWMSG_Logdateiname)
  -- This assumes job_nr is incremented globally or per job_kennung. For simplicity, we increment based on max for this job_kennung.
  SET v_job_nr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `your_project_id.your_dataset_id.job_audit_log`
    WHERE job_kennung = v_jobkennung
  );

  SET v_logdatei = CONCAT('job_', v_jobkennung, '_', CAST(v_job_nr AS STRING), '.log');

  -- Insert job start audit record (simulated DWMSG_ErzeugeEintrag)
  INSERT INTO `your_project_id.your_dataset_id.job_audit_log`
  (
    job_nr,
    job_kennung,
    source_name,
    log_ref,
    stichtag,
    sysdate_ddmmyyyy,
    status,
    created_ts,
    message
  )
  VALUES
  (
    v_job_nr,
    v_jobkennung,
    'sp_ausd_bp_ta_apn_carmen',
    v_logdatei,
    v_stichtag,
    v_sysdate,
    'STARTED',
    CURRENT_TIMESTAMP(),
    CONCAT('Job started with Stichtag: ', v_stichtag, ', Wiederanlaufwert: ', CAST(v_wiederanlaufWert AS STRING))
  );

  -- Optionally, insert into job_status for real-time tracking
  INSERT INTO `your_project_id.your_dataset_id.job_status`
  (
    job_nr,
    job_kennung,
    status,
    updated_ts
  )
  VALUES
  (
    v_job_nr,
    v_jobkennung,
    'STARTED',
    CURRENT_TIMESTAMP()
  );

  -- Main logic with error handling (simulated trap)
  BEGIN
    -- Main orchestration: Call the core processing stored procedure
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_apn_carmen`(
      v_jobkennung,
      v_stichtag,
      v_job_nr,
      v_wiederanlaufWert
    );

    SET v_status = 'SUCCESS';

    -- Insert success audit record (simulated DWMSG_SetzeStatusOK)
    INSERT INTO `your_project_id.your_dataset_id.job_audit_log`
    (
      job_nr,
      job_kennung,
      source_name,
      log_ref,
      stichtag,
      sysdate_ddmmyyyy,
      status,
      created_ts,
      message
    )
    VALUES
    (
      v_job_nr,
      v_jobkennung,
      'sp_ausd_bp_ta_apn_carmen',
      v_logdatei,
      v_stichtag,
      v_sysdate,
      'SUCCESS',
      CURRENT_TIMESTAMP(),
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    );

    -- Update job status table
    UPDATE `your_project_id.your_dataset_id.job_status`
    SET
      status = 'SUCCESS',
      updated_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr AND job_kennung = v_jobkennung;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';

    -- Insert error audit record (simulated DWMSG_Fehlerbehandlung)
    INSERT INTO `your_project_id.your_dataset_id.job_error_log`
    (
      job_nr,
      job_kennung,
      error_nr,
      error_arg,
      log_ts,
      message
    )
    VALUES
    (
      v_job_nr,
      v_jobkennung,
      COALESCE(ERROR_CODE(), v_errnr), -- Use actual error code if available, otherwise script's error code
      COALESCE(ERROR_MESSAGE(), v_errarg), -- Use actual error message if available
      CURRENT_TIMESTAMP(),
      CONCAT('AppError: Abbruch - ', COALESCE(ERROR_MESSAGE(), 'Unknown error during core processing'))
    );

    -- Insert error into audit log
    INSERT INTO `your_project_id.your_dataset_id.job_audit_log`
    (
      job_nr,
      job_kennung,
      source_name,
      log_ref,
      stichtag,
      sysdate_ddmmyyyy,
      status,
      created_ts,
      message
    )
    VALUES
    (
      v_job_nr,
      v_jobkennung,
      'sp_ausd_bp_ta_apn_carmen',
      v_logdatei,
      v_stichtag,
      v_sysdate,
      'ERROR',
      CURRENT_TIMESTAMP(),
      CONCAT('AppError: Abbruch - ', COALESCE(ERROR_MESSAGE(), 'Unknown error during core processing'))
    );

    -- Update job status table
    UPDATE `your_project_id.your_dataset_id.job_status`
    SET
      status = 'ERROR',
      updated_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr AND job_kennung = v_jobkennung;

    RAISE USING MESSAGE = CONCAT('AppError: Abbruch - ', COALESCE(ERROR_MESSAGE(), 'Unknown error'));
  END;

END;