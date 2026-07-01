-- ============================================================
-- STORED PROCEDURE: sp_k_ausd_bp_ta_cntrct_dist
-- Replaces legacy k_ausd_bp_ta_cntrct_dist.ksh control flow
-- ============================================================

-- Ensure Audit and logging tables exist
CREATE TABLE IF NOT EXISTS `gcp-project-id.audit_log.job_error_log` (
  job_name STRING,
  entry_nr STRING,
  stichtag STRING,
  error_message STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `gcp-project-id.audit_log.job_run_log` (
  job_name STRING,
  entry_nr STRING,
  stichtag DATE,
  restart_value INT64,
  records_written INT64,
  created_at TIMESTAMP
);

CREATE OR REPLACE PROCEDURE `gcp-project-id.isbert_schema.sp_k_ausd_bp_ta_cntrct_dist`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64,
  IN p_datum_heute STRING,
  IN p_datum_gestern STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute_date DATE;
  DECLARE v_datum_gestern_date DATE;
  DECLARE v_err STRING DEFAULT NULL;
  DECLARE v_restart INT64 DEFAULT COALESCE(p_wiederanlaufWert, 0);

  -- 1. Parameter Validations
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err = 'Jobkennung fehlt';
    INSERT INTO `gcp-project-id.audit_log.job_error_log` (job_name, entry_nr, stichtag, error_message, created_at)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_err = 'EintragsNr fehlt';
    INSERT INTO `gcp-project-id.audit_log.job_error_log` (job_name, entry_nr, stichtag, error_message, created_at)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET v_err = 'Stichtag fehlt';
    INSERT INTO `gcp-project-id.audit_log.job_error_log` (job_name, entry_nr, stichtag, error_message, created_at)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  -- 2. Date parsing and validation
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    SET v_err = CONCAT('Ungueltiges Stichtag Datum: ', p_Stichtag);
    INSERT INTO `gcp-project-id.audit_log.job_error_log` (job_name, entry_nr, stichtag, error_message, created_at)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  SET v_datum_heute_date = SAFE.PARSE_DATE('%d%m%Y', p_datum_heute);
  SET v_datum_gestern_date = SAFE.PARSE_DATE('%d%m%Y', p_datum_gestern);

  -- 3. Execute the Core Business SQL Transformation Query (ported from d_ausd_bp_ta_cntrct_dist.sql)
  CALL `gcp-project-id.isbert_schema.sp_d_ausd_bp_ta_cntrct_dist`(
    p_EintragsNr,
    p_JobKennung,
    v_stichtag_date,
    v_datum_heute_date,
    v_datum_gestern_date,
    v_restart,
    v_records
  );

  -- 4. Log completion and written record counts
  INSERT INTO `gcp-project-id.audit_log.job_run_log` (
    job_name,
    entry_nr,
    stichtag,
    restart_value,
    records_written,
    created_at
  )
  VALUES (
    v_TabName,
    p_EintragsNr,
    v_stichtag_date,
    v_restart,
    v_records,
    CURRENT_TIMESTAMP()
  );

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS status_message;
END;