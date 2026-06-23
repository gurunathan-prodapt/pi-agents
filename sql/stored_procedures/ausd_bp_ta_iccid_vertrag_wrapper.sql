-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
-- Description: BigQuery Stored Procedure that acts as a wrapper for the core data provisioning logic.
-- It handles parameter parsing, defaulting, validation, job audit logging, and orchestrates the call
-- to the `k_ausd_bp_ta_iccid_vertrag` BigQuery Stored Procedure.

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_actual_stichtag STRING;
  DECLARE v_actual_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_iccid_vertrag';
  DECLARE v_dwh_eintragsnr INT64;
  DECLARE v_log_message STRING;

  -- Equivalent to DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY'
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default restart value
  SET v_actual_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Default stichtag
  SET v_actual_stichtag = IFNULL(p_stichtag, v_sysdate);

  -- Validate required parameter
  ASSERT v_actual_stichtag IS NOT NULL AND LENGTH(v_actual_stichtag) = 8
    AS 'Stichtag must be provided in DDMMYYYY format.';

  -- Optional: validate date parseability
  ASSERT SAFE.PARSE_DATE('%d%m%Y', v_actual_stichtag) IS NOT NULL
    AS 'Stichtag is not a valid DDMMYYYY date. Format should be DDMMYYYY.';

  -- Job number generation. For robustness in production, consider a sequence table or dedicated ID generation.
  -- This simple MAX(job_nr) + 1 might have race conditions in concurrent scenarios.
  SET v_dwh_eintragsnr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `project.dataset.job_audit`
    WHERE job_kennung = v_jobkennung
  );

  -- Write start audit entry
  INSERT INTO `project.dataset.job_audit` (
    job_nr,
    job_kennung,
    script_name,
    log_timestamp,
    stichtag,
    status,
    message
  )
  VALUES (
    v_dwh_eintragsnr,
    v_jobkennung,
    'ausd_bp_ta_iccid_vertrag_wrapper',
    CURRENT_TIMESTAMP(),
    v_actual_stichtag,
    'STARTED',
    'Job started with Stichtag: ' || v_actual_stichtag || ', Wiederanlaufwert: ' || CAST(v_actual_wiederanlaufWert AS STRING)
  );

  BEGIN
    -- Call downstream business procedure replacing:
    -- ${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufwert}
    CALL `project.dataset.k_ausd_bp_ta_iccid_vertrag`(
      v_jobkennung,
      v_actual_stichtag,
      v_dwh_eintragsnr,
      v_actual_wiederanlaufWert
    );

    -- Success audit
    INSERT INTO `project.dataset.job_audit` (
      job_nr,
      job_kennung,
      script_name,
      log_timestamp,
      stichtag,
      status,
      message
    )
    VALUES (
      v_dwh_eintragsnr,
      v_jobkennung,
      'ausd_bp_ta_iccid_vertrag_wrapper',
      CURRENT_TIMESTAMP(),
      v_actual_stichtag,
      'OK',
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    );

  EXCEPTION WHEN ERROR THEN
    -- Get error message
    SET v_log_message = CONCAT('AppError: Abbruch - ', @@error.message);

    -- Failure audit
    INSERT INTO `project.dataset.job_audit` (
      job_nr,
      job_kennung,
      script_name,
      log_timestamp,
      stichtag,
      status,
      message
    )
    VALUES (
      v_dwh_eintragsnr,
      v_jobkennung,
      'ausd_bp_ta_iccid_vertrag_wrapper',
      CURRENT_TIMESTAMP(),
      v_actual_stichtag,
      'ERROR',
      v_log_message
    );

    RAISE USING MESSAGE = v_log_message;
  END;

END;