-- BigQuery Stored Procedure: control wrapper for k_ausd_bp_ta_bpr_apn.ksh
-- Replace ${GCP_PROJECT_ID} and ${GCP_DATASET} during build/deploy.

CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_tab_name STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value STRING DEFAULT '0';
  DECLARE v_err_msg STRING DEFAULT '';

  -- =========================================================
  -- Reusable validation helpers
  -- =========================================================
  IF p_wiederanlaufWert IS NOT NULL AND TRIM(p_wiederanlaufWert) <> '' THEN
    SET v_restart_value = p_wiederanlaufWert;
  END IF;

  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_msg = 'Jobkennung fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_err_msg = 'EintragsNr fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET v_err_msg = 'Stichtag fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- =========================================================
  -- Date validation: DDMMYYYY
  -- =========================================================
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    SET v_err_msg = CONCAT('Ungültiges Datum: ', p_Stichtag);
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- =========================================================
  -- Call inner business logic procedure
  -- =========================================================
  CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.d_ausd_bp_ta_bpr_apn`(
    p_JobKennung,
    p_EintragsNr,
    p_Stichtag,
    v_restart_value,
    v_datum_heute,
    v_datum_gestern
  );

  -- =========================================================
  -- Record count capture
  -- Replace temp file with control table / result table count
  -- =========================================================
  SET v_records = (
    SELECT COUNT(*)
    FROM `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt`
    WHERE stichtag = v_stichtag_date
  );

  -- =========================================================
  -- Persist job/control metadata
  -- =========================================================
  INSERT INTO `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table` (
    tab_name,
    job_kennung,
    eintrags_nr,
    stichtag,
    restart_value,
    record_count,
    status_code,
    process_type,
    active_flag,
    description,
    created_at
  )
  VALUES (
    v_tab_name,
    p_JobKennung,
    p_EintragsNr,
    v_stichtag_date,
    v_restart_value,
    v_records,
    'A',
    'I',
    'N',
    'Initialbefuellung',
    CURRENT_TIMESTAMP()
  );

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;