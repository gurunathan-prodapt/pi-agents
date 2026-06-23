-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.Vertragsdatenabgleich`(
  IN p_h BOOL,
  IN p_s STRING, -- Stichtag
  IN p_l STRING  -- Laufnummer
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE ErrVal INT64 DEFAULT 0;
  DECLARE DW_EintragsNr STRING DEFAULT '';
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_P_DISCOUNT_RR';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE LogDatei STRING DEFAULT '';
  DECLARE Name_Kernskript STRING DEFAULT 'my_gcp_project.my_bq_dataset.k_ausd_v_ta_p_discount_rr';
  DECLARE usage_text STRING DEFAULT '''
Usage: bq query --project_id=<my_gcp_project> --use_legacy_sql=false \\
       --parameter='p_h:BOOL:<true/false>,p_s:STRING:<stichtag>,p_l:STRING:<laufnummer>' \\
       "CALL `my_gcp_project.my_bq_dataset.Vertragsdatenabgleich`(p_h, p_s, p_l);"

  Options:
    -h          Display this help message.
    -s <stichtag> Date for processing (DDMMYYYY). Required.
    -l <laufnummer> Run number. Required.
  ''';

  IF p_h THEN
    SELECT usage_text AS message;
    RETURN; -- Exit the procedure
  END IF;

  -- Parameter validation for p_s (Stichtag)
  IF p_s IS NULL OR LENGTH(p_s) != 8 THEN
    SET ErrNr = 1;
    SET ErrArg = 'Stichtag';
    SELECT 'Parameter -s (Stichtag) is required and must be in DDMMYYYY format.' AS message;
  END IF;

  -- Parameter validation for p_l (Laufnummer)
  IF p_l IS NULL OR NOT SAFE.PARSE_INT64(p_l) IS NOT NULL THEN
    SET ErrNr = 2;
    SET ErrArg = 'Laufnummer';
    SELECT 'Parameter -l (Laufnummer) is required and must be a number.' AS message;
  END IF;

  IF ErrNr != 0 THEN
    -- Simulate logging of parameter error
    -- DWMSG_MeldeFehler (not explicitly pseudocoded, but implied by ksh original)
    INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
      (job_id, job_name, severity, error_code, error_arg, message, created_at)
    VALUES
      ('N/A', JobKennung, 'E', ErrNr, ErrArg, 'Parameterfehler', CURRENT_TIMESTAMP());

    SELECT usage_text AS message;
    RAISE USING MESSAGE = CONCAT('Error ', CAST(ErrNr AS STRING), ': ', ErrArg, ' - Parameter validation failed.');
  END IF;

  -- Obtain a unique entry number for this job run
  CALL `my_gcp_project.my_bq_dataset.DWMSG_ErmittleNr`(DW_EintragsNr);

  -- Determine a conceptual log identifier (no physical file in BQ)
  CALL `my_gcp_project.my_bq_dataset.DWMSG_Logdateiname`(LogDatei, JobKennung, DW_EintragsNr);

  -- Log job start
  CALL `my_gcp_project.my_bq_dataset.DWMSG_ErzeugeEintrag`(DW_EintragsNr, JobKennung, ProgName, LogDatei);
  CALL `my_gcp_project.my_bq_dataset.DWMSG_SetzeStichtagInfo`(DW_EintragsNr, p_s, 'DDMMYYYY');

  BEGIN
    -- Log job header
    INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
      (job_id, job_name, severity, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'I', CONCAT('JobHeader: ', ProgName, ' Version: ', ProgVersion, ' Stichtag: ', p_s, ' Laufnummer: ', p_l), CURRENT_TIMESTAMP());

    -- Call the core processing script (migrated to a BigQuery Stored Procedure)
    CALL `my_gcp_project.my_bq_dataset.k_ausd_v_ta_p_discount_rr`(
      JobKennung,
      DW_EintragsNr
    );

    -- Log successful completion message
    INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
      (job_id, job_name, severity, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'I', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    -- Set status to OK
    CALL `my_gcp_project.my_bq_dataset.DWMSG_SetzeStatusOK`(DW_EintragsNr);

  EXCEPTION WHEN ERROR THEN
    -- Handle errors using the dedicated error handling procedure
    CALL `my_gcp_project.my_bq_dataset.DWMSG_Fehlerbehandlung`(DW_EintragsNr);

    INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
      (job_id, job_name, severity, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'E', 'AppError: Abbruch - Job terminated due to an error.', CURRENT_TIMESTAMP());

    RAISE; -- Re-raise the exception to signal failure to the caller (e.g., Airflow)
  END;
END;