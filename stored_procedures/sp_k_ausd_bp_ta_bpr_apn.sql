-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Purpose: Main orchestration and control procedure. Replaces the KornShell execution wrapper.

CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value STRING DEFAULT '0';

  -- Initialize fallback for restart/wiederanlaufWert
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET v_restart_value = '0';
  ELSE
    SET v_restart_value = p_wiederanlaufWert;
  END IF;

  -- 1. Parameter presence validations
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Jobkennung is missing';
  END IF;

  IF v_err_nr = 0 AND (p_Stichtag IS NULL OR p_Stichtag = '') THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Stichtag is missing';
  END IF;

  IF v_err_nr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'EintragsNr is missing';
  END IF;

  -- Exit and log if parameters are missing
  IF v_err_nr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (tab_name, error_code, error_arg, created_at)
    VALUES (v_TabName, v_err_nr, v_err_arg, CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' - ', v_err_arg);
  END IF;

  -- 2. Validate date format (expected DDMMYYYY, e.g., 07052001)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SET v_err_nr = 192;
    SET v_err_arg = CONCAT('Invalid date format for Stichtag: ', p_Stichtag);
    
    INSERT INTO `project.dataset.job_error_log`
    (tab_name, error_code, error_arg, created_at)
    VALUES (v_TabName, v_err_nr, v_err_arg, CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' - ', v_err_arg);
  END IF;

  -- 3. Execute Core Migrated SQL Logic
  CALL `project.dataset.sp_d_ausd_bp_ta_bpr_apn`(
    p_JobKennung,
    p_EintragsNr,
    v_stichtag_date,
    v_restart_value,
    v_datum_heute,
    v_datum_gestern
  );

  -- 4. Calculate records generated / processed
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.PoolBasisprodukt`
    WHERE stichtag = v_stichtag_date
  );

  -- 5. Create audit log entry
  INSERT INTO `project.dataset.job_audit_log`
  (
    tab_name,
    job_kennung,
    eintrags_nr,
    stichtag,
    records_loaded,
    status,
    created_at
  )
  VALUES
  (
    v_TabName,
    p_JobKennung,
    p_EintragsNr,
    p_Stichtag,
    v_records,
    'SUCCESS',
    CURRENT_TIMESTAMP()
  );

END;