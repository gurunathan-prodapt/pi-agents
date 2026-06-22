-- Migrated from vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_tarifoption`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_err_nr INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 1;
    SET v_err_msg = 'Jobkennung fehlt';
  END IF;
  -- ... (other parameter validations)

  IF v_err_nr != 0 THEN
    INSERT INTO `project.dataset.job_log`
    (job_name, tab_name, error_nr, error_msg, created_at)
    VALUES
    ('r_ausd_bp_ta_tarifoption', v_TabName, v_err_nr, v_err_msg, CURRENT_TIMESTAMP());
    -- SELECT FORMAT('FEHLER: 0 E %d %s', v_err_nr, v_err_msg) AS message; -- For logging only
    LEAVE;
  END IF;

  -- Date validation
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    INSERT INTO `project.dataset.job_log`
    (job_name, tab_name, error_nr, error_msg, created_at)
    VALUES
    ('r_ausd_bp_ta_tarifoption', v_TabName, 2, 'Datum hat ungueltiges Format', CURRENT_TIMESTAMP());
    -- SELECT 'Bitte ueber Rahmenscript aufrufen' AS message; -- For logging only
    LEAVE;
  END IF;

  -- Initialize p_wiederanlaufWert
  IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN
    SET p_wiederanlaufWert = '0';
  END IF;

  -- Log start of date check
  INSERT INTO `project.dataset.job_log` (job_name, tab_name, status_msg, created_at)
  VALUES ('r_ausd_bp_ta_tarifoption', v_TabName, 'Pruefe Datum', CURRENT_TIMESTAMP());
  INSERT INTO `project.dataset.job_log` (job_name, tab_name, status_msg, created_at)
  VALUES ('r_ausd_bp_ta_tarifoption', v_TabName, 'Datum OK', CURRENT_TIMESTAMP());

  -- Core SQL logic migrated from d_ausd_bp_ta_tarifoption.sql
  CREATE TEMP TABLE tmp_result AS
  SELECT
    *
  FROM `project.dataset.PoolBasisprodukt` -- Assuming PoolBasisprodukt is the source table
  WHERE business_date = v_stichtag_date; -- Example filter using the validated date

  SET v_records = (SELECT COUNT(*) FROM tmp_result);

  -- Example: Insert into a target table or process further
  -- INSERT INTO `project.dataset.target_table` SELECT * FROM tmp_result;

  -- Log record count and job completion
  INSERT INTO `project.dataset.job_log`
  (job_name, tab_name, record_count, status_msg, created_at)
  VALUES
  ('r_ausd_bp_ta_tarifoption', v_TabName, v_records, 'Initialbefuellung', CURRENT_TIMESTAMP());

  -- SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message; -- For logging only
END;