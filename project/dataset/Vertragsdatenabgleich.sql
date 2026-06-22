-- BigQuery Stored Procedure to replace vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.Vertragsdatenabgleich`(
  IN p_h STRING,
  IN p_s STRING,
  IN p_l STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64 DEFAULT 0;
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_BARRIER';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE LogDatei STRING DEFAULT ''; -- Will be a logical name, actual logging to table
  DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.k_ausd_v_ta_barrier';
  DECLARE usage_text STRING DEFAULT '''
    Programm: Vertragsdatenabgleich
    Version:  V1.0.0
    Aufruf:   Parameter
    Parameter:
        -h     zeigt diese Seite an

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_barrier.
  ''';

  -- Parameter handling
  IF p_h IS NOT NULL AND p_h = '-h' THEN
    SELECT usage_text AS usage;
    LEAVE;
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
      (job_kennung, eintrags_nr, err_nr, err_arg, created_at)
    VALUES
      (JobKennung, DW_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SELECT usage_text AS usage;
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Parameterfehler: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Get unique job entry number
  SET DW_EintragsNr = (
    SELECT IFNULL(MAX(eintrags_nr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_kennung = JobKennung
  );

  -- Determine logical log file name (for logging purposes in tables)
  SET LogDatei = CONCAT('log_', JobKennung, '_', CAST(DW_EintragsNr AS STRING), '.log');

  -- Create initial job control entry
  INSERT INTO `project.dataset.job_control`
    (eintrags_nr, job_kennung, script_name, log_datei, stichtag_info, status, created_at)
  VALUES
    (DW_EintragsNr, JobKennung, 'Vertragsdatenabgleich', LogDatei, v_sysdate, 'RUNNING', CURRENT_TIMESTAMP());

  -- Set reference date
  UPDATE `project.dataset.job_control`
  SET stichtag_info = v_sysdate
  WHERE eintrags_nr = DW_EintragsNr
    AND job_kennung = JobKennung;

  BEGIN
    -- Core script replacement: Call the migrated core stored procedure
    CALL `project.dataset.k_ausd_v_ta_barrier`(JobKennung, DW_EintragsNr);

    -- Log success message
    INSERT INTO `project.dataset.job_log`
      (eintrags_nr, job_kennung, log_level, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'INFO', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    -- Update job status to OK
    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        finished_at = CURRENT_TIMESTAMP()
    WHERE eintrags_nr = DW_EintragsNr
      AND job_kennung = JobKennung;

  EXCEPTION WHEN ERROR THEN
    -- Handle errors and log
    INSERT INTO `project.dataset.job_log`
      (eintrags_nr, job_kennung, log_level, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'ERROR', 'AppError: Abbruch', CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        finished_at = CURRENT_TIMESTAMP()
    WHERE eintrags_nr = DW_EintragsNr
      AND job_kennung = JobKennung;

    RAISE USING MESSAGE = 'AppError: Abbruch';
  END;

END;