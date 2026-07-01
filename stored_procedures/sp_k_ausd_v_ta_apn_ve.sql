-- ============================================================
-- SQL Migration target for k_ausd_v_ta_apn_ve.ksh
-- Stored Procedure: sp_k_ausd_v_ta_apn_ve
-- ============================================================

-- Ensure operational schema tables exist in target dataset
CREATE TABLE IF NOT EXISTS `job_table` (
  job_kennung STRING NOT NULL,
  eintrags_nr STRING NOT NULL,
  tab_name STRING,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `job_run_summary` (
  job_kennung STRING,
  eintrags_nr STRING,
  tab_name STRING,
  records_processed INT64,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `job_error_log` (
  job_kennung STRING,
  eintrags_nr STRING,
  err_nr INT64,
  err_arg STRING,
  created_at TIMESTAMP
);

CREATE OR REPLACE PROCEDURE `sp_k_ausd_v_ta_apn_ve`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_apn_ve';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';

  -- 1. Validate required parameter constraints
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Jobkennung';
  END IF;

  IF v_err_nr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'EintragsNr';
  END IF;

  IF v_err_nr <> 0 THEN
    INSERT INTO `job_error_log`
      (job_kennung, eintrags_nr, err_nr, err_arg, created_at)
    VALUES
      (p_JobKennung, p_EintragsNr, v_err_nr, v_err_arg, CURRENT_TIMESTAMP());
    
    SELECT FORMAT('FEHLER: 0 E %d %s', v_err_nr, v_err_arg) AS message;
    RETURN;
  END IF;

  -- 2. Transaction wrapper for job logging and active job deactivation
  BEGIN TRANSACTION;

    -- Log the execution run as ACTIVE
    INSERT INTO `job_table`
      (job_kennung, eintrags_nr, tab_name, status, created_at, updated_at)
    VALUES
      (p_JobKennung, p_EintragsNr, v_TabName, 'ACTIVE', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    -- =========================================================================
    -- CORE BUSINESS LOGIC PLACEHOLDER
    -- (Migrated from original d_ausd_v_ta_apn_ve.sql)
    -- =========================================================================
    -- Example placeholder query targeting ta_apn_ve:
    -- INSERT INTO `ta_apn_ve` (job_kennung, eintrags_nr, updated_at)
    -- VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP());
    -- =========================================================================

    -- Capture row count of processed entries
    SET v_records = @@row_count;

    -- Deactivate older active jobs under the same identifier
    UPDATE `job_table`
    SET status = 'INACTIVE',
        updated_at = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr <> p_EintragsNr
      AND status = 'ACTIVE';

    -- Persist record execution counts to the job summary table
    INSERT INTO `job_run_summary`
      (job_kennung, eintrags_nr, tab_name, records_processed, created_at)
    VALUES
      (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

  COMMIT TRANSACTION;

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;