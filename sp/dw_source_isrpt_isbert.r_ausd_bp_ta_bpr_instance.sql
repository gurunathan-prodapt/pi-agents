-- BigQuery Stored Procedure for orchestration logic
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- Calls sp/dw_source_isrpt_isbert.d_ausd_bp_ta_bpr_instance.sql

CREATE OR REPLACE PROCEDURE `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE DEFAULT NULL;

  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET p_wiederanlaufWert = '0';
  END IF;

  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_Stichtag IS NULL OR p_Stichtag = '') THEN
    SET ErrNr = 1;
    SET ErrArg = 'Stichtag';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS error_message;
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen';
  END IF;

  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Datum hat ungueltiges Format DDMMYYYY';
  END IF;

  -- Core SQL logic migrated from d_ausd_bp_ta_bpr_instance.sql
  CALL `dw_source_isrpt_isbert.d_ausd_bp_ta_bpr_instance`(
    p_JobKennung,
    v_stichtag_date
  );

  -- Get record count from the target table for the current processing date
  SET v_records = (
    SELECT COUNT(*)
    FROM `dw_source_isrpt_isbert.sof_ta_bpr_instance`
    WHERE processing_date = v_stichtag_date
  );

  INSERT INTO `control_log.job_log`
  (
    tab_name,
    job_status,
    record_count,
    stichtag,
    created_at
  )
  VALUES
  (
    v_TabName,
    'A', -- Assuming 'A' for Active/Running or 'C' for Complete here
    v_records,
    v_stichtag_date,
    CURRENT_TIMESTAMP()
  );

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;