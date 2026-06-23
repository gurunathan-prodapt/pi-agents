-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_iccid_einzeln`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Original format DDMMYYYY
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart INT64;

  -- Parameter defaults
  IF p_wiederanlaufWert IS NULL THEN
    SET v_restart = 0;
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Required parameter checks (equivalent to pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 193, 'Jobkennung fehlt');
    RAISE USING MESSAGE = 'FEHLER: 0 E 193 Jobkennung fehlt';
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 193, 'Stichtag fehlt');
    RAISE USING MESSAGE = 'FEHLER: 0 E 193 Stichtag fehlt';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 193, 'EintragsNr fehlt');
    RAISE USING MESSAGE = 'FEHLER: 0 E 193 EintragsNr fehlt';
  END IF;

  -- Date validation: DDMMYYYY (equivalent to DWDate_Datum_Check)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, 'E', 194, 'Ungueltiges Datum: ' || p_Stichtag);
    RAISE USING MESSAGE = 'FEHLER: 0 E 194 Ungueltiges Datum';
  END IF;

  -- Execute core SQL logic formerly in external SQL file
  CALL `project.dataset.d_ausd_bp_ta_iccid_einzeln`(
    p_EintragsNr,
    p_JobKennung,
    p_Stichtag,
    v_restart,
    v_datum_heute,
    v_datum_gestern
  );

  -- Capture record count from target/result table
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.sof_ta_iccid_einzeln`
  );

  -- Persist job metadata / completion status (equivalent to FOSJobErzeugeEintrag if uncommented)
  INSERT INTO `project.dataset.job_log` (
    tab_name,
    job_status,
    job_type,
    stichtag,
    run_date,
    record_count,
    message,
    created_at
  )
  VALUES (
    v_TabName,
    'A',
    'I',
    v_stichtag_date,
    v_stichtag_date,
    v_records,
    'Initialbefuellung',
    CURRENT_TIMESTAMP()
  );

  -- Optional: write processing summary
  INSERT INTO `project.dataset.process_log` (
    job_kennung,
    eintrags_nr,
    stichtag,
    records,
    created_at
  )
  VALUES (
    p_JobKennung,
    p_EintragsNr,
    v_stichtag_date,
    v_records,
    CURRENT_TIMESTAMP()
  );

END;