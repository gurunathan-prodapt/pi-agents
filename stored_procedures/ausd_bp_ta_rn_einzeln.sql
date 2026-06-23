-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Target: BigQuery Stored Procedure for r_ausd_bp_ta_rn_einzeln.ksh orchestration

-- BigQuery Stored Procedure: orchestration wrapper for BERT base product provisioning

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_rn_einzeln`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_effective_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'AUSD_BP_TA_RN_EINZELN';
  DECLARE v_dwh_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Default restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- System date in DDMMYYYY format
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Effective cutoff date
  SET v_effective_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- Validate required parameter
  IF v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = '' THEN
    SET v_errnr = 193;
    SET v_errarg = 'Stichtag';
  END IF;

  IF v_errnr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (
      job_name,
      error_number,
      error_argument,
      created_at,
      message
    )
    VALUES
    (
      v_jobkennung,
      v_errnr,
      v_errarg,
      CURRENT_TIMESTAMP(),
      'Required parameter missing'
    );

    RAISE USING MESSAGE = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg);
  END IF;

  -- Create job entry number
  SET v_dwh_eintragsnr = (
    SELECT IFNULL(MAX(job_entry_nr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_name = v_jobkennung
  );

  -- Create log record / job entry
  SET v_logdatei = CONCAT('job_', v_jobkennung, '_', CAST(v_dwh_eintragsnr AS STRING), '.log');

  INSERT INTO `project.dataset.job_control`
  (
    job_entry_nr,
    job_name,
    source_script,
    log_name,
    stichtag,
    sysdate_ddmmyyyy,
    restart_value,
    status,
    created_at
  )
  VALUES
  (
    v_dwh_eintragsnr,
    v_jobkennung,
    'ausd_bp_ta_rn_einzeln',
    v_logdatei,
    v_effective_stichtag,
    v_sysdate,
    v_wiederanlaufWert,
    'RUNNING',
    CURRENT_TIMESTAMP()
  );

  BEGIN
    -- Downstream core logic replacement - Placeholder for migrated k_ausd_bp_ta_rn_einzeln.ksh
    CALL `project.dataset.k_ausd_bp_ta_rn_einzeln`(
      v_jobkennung,
      v_effective_stichtag,
      v_dwh_eintragsnr,
      v_wiederanlaufWert
    );

    SET v_status = 'OK';

    UPDATE `project.dataset.job_control`
    SET
      status = v_status,
      finished_at = CURRENT_TIMESTAMP(),
      success_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    WHERE job_entry_nr = v_dwh_eintragsnr
      AND job_name = v_jobkennung;

    INSERT INTO `project.dataset.job_messages`
    (
      job_entry_nr,
      job_name,
      message_type,
      message_text,
      created_at
    )
    VALUES
    (
      v_dwh_eintragsnr,
      v_jobkennung,
      'INFO',
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
      CURRENT_TIMESTAMP()
    );

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';

    UPDATE `project.dataset.job_control`
    SET
      status = v_status,
      finished_at = CURRENT_TIMESTAMP(),
      error_message = @@error.message
    WHERE job_entry_nr = v_dwh_eintragsnr
      AND job_name = v_jobkennung;

    INSERT INTO `project.dataset.job_messages`
    (
      job_entry_nr,
      job_name,
      message_type,
      message_text,
      created_at
    )
    VALUES
    (
      v_dwh_eintragsnr,
      v_jobkennung,
      'ERROR',
      @@error.message,
      CURRENT_TIMESTAMP()
    );

    RAISE;
  END;

END;