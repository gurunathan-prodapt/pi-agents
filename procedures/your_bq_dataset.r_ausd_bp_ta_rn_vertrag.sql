-- BigQuery Stored Procedure for k_ausd_bp_ta_rn_vertrag.ksh migration
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
-- This procedure handles parameter validation, date derivation, and the main data transformation.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.r_ausd_bp_ta_rn_vertrag`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Expected format: DDMMYYYY
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE ErrMessage STRING;

  -- Parameter Validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1; SET ErrArg = 'Jobkennung'; SET ErrMessage = 'FEHLER: Notwendiges Argument fehlt - Jobkennung';
  ELSEIF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET ErrNr = 1; SET ErrArg = 'Stichtag'; SET ErrMessage = 'FEHLER: Notwendiges Argument fehlt - Stichtag';
  ELSEIF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 1; SET ErrArg = 'EintragsNr'; SET ErrMessage = 'FEHLER: Notwendiges Argument fehlt - EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    INSERT INTO `your_gcp_project.your_bq_dataset.error_log` (timestamp, job_name, error_code, error_argument, job_kennung, eintrags_nr, stichtag, error_message)
    VALUES (CURRENT_TIMESTAMP(), v_TabName, ErrNr, ErrArg, p_JobKennung, p_EintragsNr, NULL, ErrMessage);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ErrMessage;
  END IF;

  -- Date Validation (DDMMYYYY)
  IF NOT REGEXP_CONTAINS(p_Stichtag, r'^[0-3][0-9][0-1][0-9][0-9]{4}$') THEN
    SET ErrMessage = 'Ungueltiges Datumformat fuer Stichtag, erwartet DDMMYYYY';
    INSERT INTO `your_gcp_project.your_bq_dataset.error_log` (timestamp, job_name, error_code, error_argument, job_kennung, eintrags_nr, stichtag, error_message)
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 2, 'StichtagFormat', p_JobKennung, p_EintragsNr, NULL, ErrMessage);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ErrMessage;
  END IF;

  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

  -- Derive yesterday and today dates
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Initialize p_wiederanlaufWert
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET p_wiederanlaufWert = '0';
  END IF;

  -- Main SQL logic from d_ausd_bp_ta_rn_vertrag.sql
  BEGIN
    DECLARE insert_data ARRAY<STRUCT<
      MELDUNG_CD STRING, MELDUNG_TEXT STRING, EINTRAG_NR STRING,
      VERTRAG_NR STRING, MELDUNG_GUELTIG_CD STRING, MELDUNG_GUELTIG_TEXT STRING
    >>;

    SET insert_data = (
      SELECT AS STRUCT
        T1.MELDUNG_CD,
        T1.MELDUNG_TEXT,
        T2.EINTRAG_NR,
        T2.VERTRAG_NR,
        T2.MELDUNG_GUELTIG_CD,
        T2.MELDUNG_GUELTIG_TEXT
      FROM `your_gcp_project.your_bq_dataset.DWTK_MELDUNGEN` AS T1
      JOIN `your_gcp_project.your_bq_dataset.SOF$TA_RN_EINZELN` AS T2
        ON T1.MELDUNG_CD = T2.MELDUNG_CD
        AND T1.MELDUNG_GUELTIG_CD = T2.MELDUNG_GUELTIG_CD
    );

    SET v_records = ARRAY_LENGTH(insert_data);

    INSERT INTO `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG` (
      MELDUNG_CD, MELDUNG_TEXT, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT
    )
    SELECT * FROM UNNEST(insert_data);

  EXCEPTION WHEN ERROR THEN
    SET ErrMessage = 'FEHLER: Data transformation failed - ' || ERROR_MESSAGE();
    INSERT INTO `your_gcp_project.your_bq_dataset.error_log` (timestamp, job_name, error_code, error_argument, job_kennung, eintrags_nr, stichtag, error_message)
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 3, 'DataTransformation', p_JobKennung, p_EintragsNr, v_stichtag_date, ErrMessage);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ErrMessage;
  END;

  -- Log job completion/metrics
  INSERT INTO `your_gcp_project.your_bq_dataset.job_tracking` (timestamp, job_name, job_kennung, eintrags_nr, stichtag, records_processed, description)
  VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, v_stichtag_date, v_records, 'Initialbefuellung');

  SELECT CONCAT('---------- ENDE Datenverarbeitung (', v_TabName, ') ----------') AS StatusMessage, v_records AS RecordsProcessed;

END;