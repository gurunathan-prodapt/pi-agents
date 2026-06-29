-- BigQuery GoogleSQL Stored Procedures for k_aurd_rechstan orchestration.
-- Project ID: gcp-isbert-prod
-- Dataset: isbert_aufbereitung

-- =========================================================
-- 1) Helper Procedure: validate DDMMYYYY date string
-- =========================================================
CREATE OR REPLACE PROCEDURE `gcp-isbert-prod.isbert_aufbereitung.sp_validate_ddmmyyyy`(
  p_date_str STRING,
  OUT o_valid_date DATE,
  OUT o_error_code INT64,
  OUT o_error_message STRING
)
BEGIN
  SET o_valid_date = SAFE.PARSE_DATE('%d%m%Y', p_date_str);

  IF p_date_str IS NULL OR TRIM(p_date_str) = '' THEN
    SET o_error_code = 193;
    SET o_error_message = 'Stichtag fehlt';
  ELSEIF o_valid_date IS NULL THEN
    SET o_error_code = 193;
    SET o_error_message = 'Ungueltiges Datum im Format DDMMYYYY';
  ELSE
    SET o_error_code = 0;
    SET o_error_message = NULL;
  END IF;
END;

-- =========================================================
-- 2) Helper Procedure: validate required parameters
-- =========================================================
CREATE OR REPLACE PROCEDURE `gcp-isbert-prod.isbert_aufbereitung.sp_validate_required_params`(
  p_job_kennung STRING,
  p_eintrags_nr STRING,
  p_stichtag STRING,
  OUT o_error_code INT64,
  OUT o_error_message STRING
)
BEGIN
  SET o_error_code = 0;
  SET o_error_message = NULL;

  IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
    SET o_error_code = 1;
    SET o_error_message = 'Jobkennung fehlt';
  ELSEIF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
    SET o_error_code = 2;
    SET o_error_message = 'EintragsNr fehlt';
  ELSEIF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
    SET o_error_code = 3;
    SET o_error_message = 'Stichtag fehlt';
  END IF;
END;

-- =========================================================
-- 3) Helper Procedure: default restart value
-- =========================================================
CREATE OR REPLACE PROCEDURE `gcp-isbert-prod.isbert_aufbereitung.sp_default_restart_value`(
  p_restart_value STRING,
  OUT o_restart_value STRING
)
BEGIN
  IF p_restart_value IS NULL OR TRIM(p_restart_value) = '' THEN
    SET o_restart_value = '0';
  ELSE
    SET o_restart_value = p_restart_value;
  END IF;
END;

-- =========================================================
-- 4) Helper Procedure: write error log
-- =========================================================
CREATE OR REPLACE PROCEDURE `gcp-isbert-prod.isbert_aufbereitung.sp_write_job_error_log`(
  p_job_name STRING,
  p_entry_nr STRING,
  p_stichtag STRING,
  p_error_code INT64,
  p_error_message STRING
)
BEGIN
  INSERT INTO `gcp-isbert-prod.isbert_aufbereitung.job_error_log`
    (job_name, entry_nr, stichtag, error_code, error_message, created_at)
  VALUES
    (p_job_name, p_entry_nr, p_stichtag, p_error_code, p_error_message, CURRENT_TIMESTAMP());
END;

-- =========================================================
-- 5) Helper Procedure: deactivate active jobs for a table
-- =========================================================
CREATE OR REPLACE PROCEDURE `gcp-isbert-prod.isbert_aufbereitung.sp_deactivate_active_jobs`(
  p_table_name STRING
)
BEGIN
  UPDATE `gcp-isbert-prod.isbert_aufbereitung.job_table`
  SET active_flag = 'N',
      updated_at = CURRENT_TIMESTAMP()
  WHERE table_name = p_table_name
    AND active_flag = 'Y';
END;

-- =========================================================
-- 6) Helper Procedure: create job table entry
-- =========================================================
CREATE OR REPLACE PROCEDURE `gcp-isbert-prod.isbert_aufbereitung.sp_create_job_entry`(
  p_table_name STRING,
  p_status_code STRING,
  p_active_flag STRING,
  p_stichtag_from DATE,
  p_stichtag_to DATE,
  p_job_type STRING,
  p_restart_flag STRING,
  p_record_count INT64,
  p_description STRING,
  p_job_kennung STRING,
  p_eintrags_nr STRING
)
BEGIN
  INSERT INTO `gcp-isbert-prod.isbert_aufbereitung.job_table`
  (
    table_name,
    status_code,
    active_flag,
    stichtag_from,
    stichtag_to,
    job_type,
    restart_flag,
    record_count,
    description,
    job_kennung,
    eintrags_nr,
    created_at
  )
  VALUES
  (
    p_table_name,
    p_status_code,
    p_active_flag,
    p_stichtag_from,
    p_stichtag_to,
    p_job_type,
    p_restart_flag,
    p_record_count,
    p_description,
    p_job_kennung,
    p_eintrags_nr,
    CURRENT_TIMESTAMP()
  );
END;

-- =========================================================
-- 7) Main Control Procedure: r_aurd_rechstan_control
-- =========================================================
CREATE OR REPLACE PROCEDURE `gcp-isbert-prod.isbert_aufbereitung.r_aurd_rechstan_control`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'RKopfStan';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_wiederanlaufWert STRING DEFAULT '0';
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errmsg STRING DEFAULT NULL;

  -- Required parameter validation
  CALL `gcp-isbert-prod.isbert_aufbereitung.sp_validate_required_params`(
    p_JobKennung,
    p_EintragsNr,
    p_Stichtag,
    v_errnr,
    v_errmsg
  );

  IF v_errnr <> 0 THEN
    CALL `gcp-isbert-prod.isbert_aufbereitung.sp_write_job_error_log`(
      'r_aurd_rechstan',
      p_EintragsNr,
      p_Stichtag,
      v_errnr,
      v_errmsg
    );
    SELECT FORMAT('FEHLER: 0 E %d %s', v_errnr, v_errmsg) AS message;
    RETURN;
  END IF;

  -- Date validation (DDMMYYYY format check)
  CALL `gcp-isbert-prod.isbert_aufbereitung.sp_validate_ddmmyyyy`(
    p_Stichtag,
    v_stichtag_date,
    v_errnr,
    v_errmsg
  );

  IF v_errnr <> 0 THEN
    CALL `gcp-isbert-prod.isbert_aufbereitung.sp_write_job_error_log`(
      'r_aurd_rechstan',
      p_EintragsNr,
      p_Stichtag,
      v_errnr,
      v_errmsg
    );
    RAISE USING MESSAGE = v_errmsg;
  END IF;

  -- Default restart value
  CALL `gcp-isbert-prod.isbert_aufbereitung.sp_default_restart_value`(
    p_wiederanlaufWert,
    v_wiederanlaufWert
  );

  -- Optional: deactivate active jobs (commented out in source, available if needed)
  -- CALL `gcp-isbert-prod.isbert_aufbereitung.sp_deactivate_active_jobs`(v_TabName);

  -- =========================================================================
  -- Core business logic placeholder:
  -- Originally d_aurd_rechstan.sql was called with parameters:
  --   $p_EintragsNr, $Name_SQLskript, $p_EintragsNr, $p_JobKennung, 
  --   $p_Stichtag, $tmpFile, $p_wiederanlaufWert, $p_datum_heute, $p_datum_gestern
  --
  -- Place the BigQuery-adapted contents of d_aurd_rechstan.sql here.
  -- For validation and testing purposes, we count records in the target table.
  -- =========================================================================
  
  -- S&T Pipeline transformation logic goes here.
  -- Below is the runnable structure representing target table state evaluation.
  SET v_records = (
    SELECT COUNT(*)
    FROM `gcp-isbert-prod.isbert_aufbereitung.RKopfStan`
    WHERE stichtag_from = v_stichtag_date
  );

  -- Intended job-table entry creation
  CALL `gcp-isbert-prod.isbert_aufbereitung.sp_create_job_entry`(
    v_TabName,
    'A',
    'I',
    v_stichtag_date,
    v_stichtag_date,
    'J',
    'N',
    v_records,
    'Initialbefuellung',
    p_JobKennung,
    p_EintragsNr
  );

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;