-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Wrapper Stored Procedure orchestrating the data reconciliation process
-- for the ta_disc_zusgf table, replacing the original KornShell script.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.Vertragsdatenabgleich_wrapper_sp`(
  -- Parameters replacing shell script arguments
  p_param_s STRING DEFAULT NULL, -- Placeholder for -s parameter
  p_param_l STRING DEFAULT NULL, -- Placeholder for -l parameter
  p_display_help BOOL DEFAULT FALSE
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64; -- Assigned by utility SP
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_DISC_ZUSGF';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE LogDatei STRING; -- Assigned by utility SP
  DECLARE Name_Kernskript_SP STRING DEFAULT 'k_ausd_v_ta_disc_zusgf_sp';

  IF p_display_help THEN
    SELECT
      CONCAT('Programm: ', ProgName) AS line1,
      CONCAT('Version:  ', ProgVersion) AS line2,
      'Aufruf:   Parameter' AS line3,
      '-h     zeigt diese Seite an' AS line4,
      'Beschreibung:' AS line5,
      'Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_disc_zusgf.' AS line6;
    RETURN;
  END IF;

  -- Determine job entry number
  CALL `project_id.dataset_id.DWMSG_ErmittleNr_sp`(DW_EintragsNr);

  -- Determine log file name
  CALL `project_id.dataset_id.DWMSG_Logdateiname_sp`(LogDatei, JobKennung, DW_EintragsNr);

  -- Create log entry
  CALL `project_id.dataset_id.DWMSG_ErzeugeEintrag_sp`(DW_EintragsNr, JobKennung, 'Vertragsdatenabgleich_wrapper_sp', LogDatei);

  -- Set reference date info
  CALL `project_id.dataset_id.DWMSG_SetzeStichtagInfo_sp`(DW_EintragsNr, v_sysdate, 'DDMMYYYY');

  BEGIN
    -- Log job banner
    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_file, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, LogDatei, ' ----------------- Job -----------------------', CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, CONCAT(' Job-Nr    : \'', CAST(DW_EintragsNr AS STRING), '\''), CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, CONCAT(' JobKennung: \'', JobKennung, '\''), CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, CONCAT(' Logdatei  : \'', LogDatei, '\''), CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, ' ---------------------------------------------', CURRENT_TIMESTAMP());

    -- Replace shell execution with stored procedure call for the core script
    CALL `project_id.dataset_id.k_ausd_v_ta_disc_zusgf_sp`(DW_EintragsNr, JobKennung);

    -- Success handling
    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_file, message, created_at)
    VALUES (DW_EintragsNr, JobKennung, LogDatei, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    CALL `project_id.dataset_id.DWMSG_SetzeStatusOK_sp`(DW_EintragsNr);

  EXCEPTION WHEN ERROR THEN
    -- Equivalent to trap ERR / INT handling
    CALL `project_id.dataset_id.DWMSG_Fehlerbehandlung_sp`(DW_EintragsNr);

    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_file, message, created_at)
    VALUES (DW_EintragsNr, JobKennung, LogDatei, 'AppError: Abbruch', CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('Job failed for JobKennung: ', JobKennung, ' and JobNr: ', CAST(DW_EintragsNr AS STRING), '. Error: ', ERROR());
  END;
END;