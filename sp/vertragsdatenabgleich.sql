-- BigQuery Stored Procedure: vertragsdatenabgleich
-- Replaces wrapper script vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.vertragsdatenabgleich`(
  IN p_jobkennung STRING,
  IN p_run_date DATE,
  IN p_enable_help BOOL
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64;
  DECLARE LogDatei STRING;
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE Name_Kernskript STRING DEFAULT '`your_project.your_dataset.k_ausd_v_ta_action_assoc`';
  DECLARE v_jobkennung_actual STRING DEFAULT UPPER(COALESCE(p_jobkennung, 'BERT_V_TA_ACTION_ASSOC'));

  -- Help handling equivalent
  IF p_enable_help THEN
    SELECT
      ProgName AS Programm,
      ProgVersion AS Version,
      'Aufruf: CALL `your_project.your_dataset.vertragsdatenabgleich`(p_jobkennung => ''JOB_ID'', p_run_date => CURRENT_DATE(), p_enable_help => FALSE)' AS Aufruf,
      'Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_action_assoc.' AS Beschreibung;
    LEAVE;
  END IF;

  -- Parameter validation equivalent
  IF v_jobkennung_actual IS NULL OR TRIM(v_jobkennung_actual) = '' THEN
    SET ErrNr = 193; -- Custom error code for missing parameter
    SET ErrArg = 'p_jobkennung';
  END IF;

  -- Initialize DW_EintragsNr before error check for logging consistency
  SET DW_EintragsNr = (
    SELECT IFNULL(MAX(entry_no), 0) + 1
    FROM `your_project.your_dataset.job_log`
    WHERE job_kennung = v_jobkennung_actual
  );

  IF ErrNr != 0 THEN
    INSERT INTO `your_project.your_dataset.job_error_log`
      (entry_no, job_kennung, program_name, error_no, error_arg, created_ts)
    VALUES
      (DW_EintragsNr, v_jobkennung_actual, ProgName, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Parameterfehler: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- LogDatei generation (for consistency with original script, though logging is to table)
  SET LogDatei = CONCAT(v_jobkennung_actual, '_', CAST(DW_EintragsNr AS STRING), '.log');

  INSERT INTO `your_project.your_dataset.job_log`
    (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
  VALUES
    (DW_EintragsNr, v_jobkennung_actual, ProgName, ProgVersion, LogDatei, 'STARTED', v_sysdate, CURRENT_TIMESTAMP());

  BEGIN
    -- Equivalent to kernel script invocation
    CALL `your_project.your_dataset.k_ausd_v_ta_action_assoc`(v_jobkennung_actual, DW_EintragsNr);

    -- Success handling
    INSERT INTO `your_project.your_dataset.job_log`
      (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
    VALUES
      (DW_EintragsNr, v_jobkennung_actual, ProgName, ProgVersion, LogDatei, 'OK', v_sysdate, CURRENT_TIMESTAMP());

    SELECT 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' AS message;

  EXCEPTION WHEN ERROR THEN
    -- Error handling and logging
    INSERT INTO `your_project.your_dataset.job_error_log`
      (entry_no, job_kennung, program_name, error_message, created_ts)
    VALUES
      (DW_EintragsNr, v_jobkennung_actual, ProgName, @@error.message, CURRENT_TIMESTAMP());

    INSERT INTO `your_project.your_dataset.job_log`
      (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
    VALUES
      (DW_EintragsNr, v_jobkennung_actual, ProgName, ProgVersion, LogDatei, 'ERROR', v_sysdate, CURRENT_TIMESTAMP());

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('AppError: Abbruch - ', @@error.message);
  END;
END;