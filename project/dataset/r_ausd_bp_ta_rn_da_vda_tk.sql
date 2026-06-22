-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- Description: BigQuery Stored Procedure migrating the orchestration logic from the legacy KornShell script.
-- This procedure validates parameters, handles dates, orchestrates the core SQL logic, and logs execution.

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_rn_da_vda_tk`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING, -- Expected format DDMMYYYY
  p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt'; -- Placeholder, update if source analysis reveals actual table name
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err STRING DEFAULT '';
  DECLARE v_errnr INT64 DEFAULT 0;

  -- Initialize restart value if null
  IF p_wiederanlaufWert IS NULL THEN
    SET p_wiederanlaufWert = 0;
  END IF;

  -- Parameter Validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_errnr = 1; SET v_err = 'Jobkennung fehlt';
  END IF;
  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_errnr = 1; SET v_err = 'Stichtag fehlt';
  END IF;
  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_errnr = 1; SET v_err = 'EintragsNr fehlt';
  END IF;

  -- Error Handling for initial parameter validation
  IF v_errnr <> 0 THEN
    INSERT INTO `project.dataset.error_log` (log_timestamp, table_name, job_kennung, entry_number, business_date_param, error_code, error_message)
    VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, v_errnr, v_err);
    RAISE USING MESSAGE = CONCAT('FEHLER: ', CAST(v_errnr AS STRING), ' ', v_err);
  END IF;

  -- Date Validation and Conversion
  BEGIN
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.error_log` (log_timestamp, table_name, job_kennung, entry_number, business_date_param, error_code, error_message)
    VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, 194, 'Ungueltiges Datumsformat fuer Stichtag');
    RAISE USING MESSAGE = 'FEHLER: Ungueltiges Datumsformat fuer Stichtag';
  END;

  -- Log process start
  INSERT INTO `project.dataset.process_log` (log_timestamp, table_name, job_kennung, entry_number, business_date_param, message)
  VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, 'Pruefe Datum OK');

  --------------------------------------------------------------------------------------
  -- PLACEHOLDER FOR CORE BUSINESS LOGIC FROM d_ausd_bp_ta_rn_da_vda_tk.sql
  --
  -- The original SQL script 'd_ausd_bp_ta_rn_da_vda_tk.sql' contains the primary
  -- business logic. This SQL must be migrated to BigQuery SQL and inserted here.
  --
  -- Example of what might go here:
  -- INSERT INTO `project.dataset.target_table` (col1, col2, ..., business_date_column)
  -- SELECT
  --   source_col1,
  --   source_col2,
  --   ...,
  --   v_stichtag_date AS business_date_column
  -- FROM
  --   `project.dataset.source_table`
  -- WHERE
  --   some_date_column = v_stichtag_date;
  --
  -- If this logic creates a new table or modifies an existing one, ensure `v_records`
  -- is updated with the actual number of rows processed/inserted.
  --------------------------------------------------------------------------------------

  -- Simulate record count capture (REPLACE with actual count from transformed data)
  -- For example, if you inserted into `project.dataset.some_target_table`:
  -- SELECT COUNT(*) FROM `project.dataset.some_target_table` WHERE business_date_column = v_stichtag_date INTO v_records;
  SET v_records = 12345; -- This is a placeholder value. UPDATE with actual count.

  -- Log job entry (migrated from FOSJobErzeugeEintrag concept)
  INSERT INTO `project.dataset.job_table` (log_timestamp, table_name, job_status_code_1, job_status_code_2, business_date_start, business_date_end, process_flag_1, process_flag_2, records_processed, description)
  VALUES (
    CURRENT_TIMESTAMP(),
    v_TabName,
    'A', -- As per design doc pseudocode
    'I', -- As per design doc pseudocode
    v_stichtag_date,
    v_stichtag_date, -- Often start and end dates are the same for daily processing
    'J', -- As per design doc pseudocode
    'N', -- As per design doc pseudocode
    v_records,
    'Initialbefuellung'
  );

  -- Log process end
  INSERT INTO `project.dataset.process_log` (log_timestamp, table_name, job_kennung, entry_number, business_date_param, message)
  VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, '---------- ENDE Datenverarbeitung ----------');

EXCEPTION WHEN ERROR THEN
  -- Catch any unhandled errors during the core logic execution
  INSERT INTO `project.dataset.error_log` (log_timestamp, table_name, job_kennung, entry_number, business_date_param, error_code, error_message)
  VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, ERROR_CODE(), ERROR_MESSAGE());
  RAISE USING MESSAGE = CONCAT('UNEXPECTED ERROR: ', ERROR_MESSAGE());
END;