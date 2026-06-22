-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_drop_temp_table.ksh
-- This BigQuery Stored Procedure replaces the ksh script k_drop_temp_table.ksh,
-- handling parameter parsing, validation, date derivation, and orchestrating
-- the dropping of temporary tables.

CREATE OR REPLACE PROCEDURE dataset.r_drop_temp_table_control(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert INT64
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'PoolVertrag'; -- Original script implied interaction with PoolVertrag
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_monat_heute STRING;
  DECLARE v_monat_gestern STRING;
  DECLARE v_restart INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Stichtag';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO dataset.job_error_log
    (job_name, error_nr, error_arg, created_at)
    VALUES
    ('r_drop_temp_table_control', ErrNr, ErrArg, CURRENT_TIMESTAMP());

    SELECT CONCAT('FEHLER: 0 E ', CAST(ErrNr AS STRING), ' ', ErrArg) AS message;
    RETURN; -- Exit procedure
  END IF;

  -- Date format validation (expected DDMMYYYY)
  IF SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL THEN
    INSERT INTO dataset.job_error_log
    (job_name, error_nr, error_arg, created_at)
    VALUES
    ('r_drop_temp_table_control', 193, p_Stichtag, CURRENT_TIMESTAMP());

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Invalid date format for Stichtag. Expected DDMMYYYY.';
  END IF;

  -- Derive dates
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  SET v_monat_heute = FORMAT_DATE('%m', v_datum_heute);
  SET v_monat_gestern = FORMAT_DATE('%m', v_datum_gestern);

  -- Initialize restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET v_restart = 0;
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Execute the SQL logic (from d_drop_temp_table.sql)
  -- This assumes 'dataset.d_drop_temp_table' has been implemented based on the original SQL file.
  CALL dataset.d_drop_temp_table(
      p_EintragsNr,
      p_JobKennung,
      p_Stichtag,
      v_restart,
      v_datum_heute,
      v_datum_gestern,
      v_monat_heute,
      v_monat_gestern,
      v_records -- This INOUT parameter will be updated by d_drop_temp_table
  );

  -- Handle record count (v_records would be set by d_drop_temp_table)
  -- For example, log it or use it for job control.
  SELECT CONCAT('Records processed by d_drop_temp_table: ', CAST(v_records AS STRING)) AS message;


  -- (Re-enable and implement if job management was active in the original ksh)
  -- This section assumes 'dataset.job_table' has been created.
  /*
  INSERT INTO dataset.job_table (tab_name, status, mode, stichtag_from, stichtag_to, job_type, active_flag, records, description)
  VALUES
  (v_TabName, 'A', 'I', PARSE_DATE('%d%m%Y', p_Stichtag), PARSE_DATE('%d%m%Y', p_Stichtag), 'J', 'N', v_records, 'Initialbefuellung');
  */

  SELECT '---------- ENDE Datenverarbeitung ----------' AS message;
END;