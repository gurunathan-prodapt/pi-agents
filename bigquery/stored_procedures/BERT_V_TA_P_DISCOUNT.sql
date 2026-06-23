-- BigQuery Stored Procedure: wrapper orchestration for ta_p_discount reconciliation
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(
  IN p_s STRING,
  IN p_l STRING,
  IN p_h BOOL
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_P_DISCOUNT';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE DW_EintragsNr INT64 DEFAULT 0;
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE LogDatei STRING DEFAULT '';
  DECLARE Name_Kernskript STRING DEFAULT 'my_project.my_dataset.k_ausd_v_ta_p_discount';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Usage/help branch
  IF p_h = TRUE THEN
    SELECT
      ProgName AS Programm,
      ProgVersion AS Version,
      'Aufruf: Parameter -h zeigt diese Seite an' AS Beschreibung;
    RETURN;
  END IF;

  -- Parameter validation (simplified for pseudocode)
  -- The original ksh script parses -s, -l, -h. The -s and -l are declared but not explicitly used in the provided logic.
  -- For this migration, we assume if p_s or p_l are critical, further validation would be added here.
  -- For now, ErrNr is initialized to 0 and not changed by other logic, so this IF will not trigger unless ErrNr is explicitly set.
  IF ErrNr != 0 THEN
    INSERT INTO `my_project.my_dataset.job_error_log`
    (job_name, job_entry_nr, error_nr, error_arg, created_at)
    VALUES
    (JobKennung, DW_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Parameter validation failed';
  END IF;

  -- Job number and log file initialization
  SET DW_EintragsNr = (
    SELECT IFNULL(MAX(job_entry_nr), 0) + 1
    FROM `my_project.my_dataset.job_control`
    WHERE job_name = JobKennung
  );

  SET LogDatei = CONCAT(JobKennung, '_', CAST(DW_EintragsNr AS STRING), '.log');

  INSERT INTO `my_project.my_dataset.job_control`
  (job_entry_nr, job_name, script_name, log_file, status, stichtag, created_at)
  VALUES
  (DW_EintragsNr, JobKennung, 'BERT_V_TA_P_DISCOUNT.sql', LogDatei, 'RUNNING', v_sysdate, CURRENT_TIMESTAMP());

  -- Core processing call placeholder
  BEGIN
    -- Call the core reconciliation procedure
    CALL `my_project.my_dataset.k_ausd_v_ta_p_discount`(JobKennung, DW_EintragsNr);

    SET v_status = 'OK';

    INSERT INTO `my_project.my_dataset.job_log`
    (job_entry_nr, job_name, log_message, created_at)
    VALUES
    (DW_EintragsNr, JobKennung, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    UPDATE `my_project.my_dataset.job_control`
    SET status = 'OK',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = DW_EintragsNr
      AND job_name = JobKennung;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';

    INSERT INTO `my_project.my_dataset.job_log`
    (job_entry_nr, job_name, log_message, created_at)
    VALUES
    (DW_EintragsNr, JobKennung, 'AppError: Abbruch - ' || ERROR_MESSAGE(), CURRENT_TIMESTAMP()); -- Added ERROR_MESSAGE() for more detail

    UPDATE `my_project.my_dataset.job_control`
    SET status = 'ERROR',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = DW_EintragsNr
      AND job_name = JobKennung;

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Job aborted due to error';
  END;

END;