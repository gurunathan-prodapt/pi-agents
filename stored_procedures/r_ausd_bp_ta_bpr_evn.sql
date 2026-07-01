-- BigQuery stored procedure for: r_ausd_bp_ta_bpr_evn
-- Derived from: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_evn.ksh
--
-- This stored procedure validates mandatory job parameters, records errors, and calls standard
-- SQL operations to process PoolBasisprodukt data.

CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET}.r_ausd_bp_ta_bpr_evn`(
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
  DECLARE v_restart STRING DEFAULT p_wiederanlaufWert;
  DECLARE v_stichtag_date DATE;

  -- Initialize restart/recovery value
  IF v_restart IS NULL OR v_restart = '' THEN
    SET v_restart = '0';
  END IF;

  -- Validate parameters natively in BigQuery
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Jobkennung';
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Stichtag';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'EintragsNr';
  END IF;

  -- If validation failed, write to logging tables and throw a clean BigQuery error
  IF v_err_nr <> 0 THEN
    INSERT INTO `${GCP_PROJECT_ID}.${BQ_DATASET}.job_error_log`
    (job_name, error_code, error_arg, created_at)
    VALUES
    ('r_ausd_bp_ta_bpr_evn', v_err_nr, v_err_arg, CURRENT_TIMESTAMP());

    ERROR(CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' ', v_err_arg));
  END IF;

  -- Parse the key date to standard DATE format
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

  -- Log start audit state
  INSERT INTO `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`
  (job_name, job_identifier, entry_nr, stichtag, status, created_at)
  VALUES
  (v_TabName, p_JobKennung, p_EintragsNr, v_stichtag_date, 'STARTED', CURRENT_TIMESTAMP());

  -- ==========================================================================
  -- Core SQL Logic placeholder (equivalent to external d_ausd_bp_ta_bpr_evn.sql)
  -- Real business extraction query on table PoolBasisprodukt would go here.
  -- Example SQL pattern matching the migration design document:
  -- 
  -- INSERT INTO `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt` (...)
  -- SELECT ...
  -- FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.source_table` 
  -- WHERE business_date = v_stichtag_date;
  -- ==========================================================================

  -- Retrieve count of processed records for execution metadata reporting
  SET v_records = (
    SELECT COUNT(1) 
    FROM `${GCP_PROJECT_ID}.${BQ_DATASET}.PoolBasisprodukt`
    WHERE business_date = v_stichtag_date
  );

  -- Log finish audit state
  INSERT INTO `${GCP_PROJECT_ID}.${BQ_DATASET}.job_audit_log`
  (job_name, job_identifier, entry_nr, stichtag, status, record_count, created_at)
  VALUES
  (v_TabName, p_JobKennung, p_EintragsNr, v_stichtag_date, 'FINISHED', v_records, CURRENT_TIMESTAMP());

END;