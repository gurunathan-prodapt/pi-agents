--
-- Target BigQuery Stored Procedure
-- Replaces legacy KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
--
-- This procedure orchestrates data preparation, handling parameter parsing, date validation,
-- and execution of the core data processing logic (which needs to be migrated separately).
-- It logs job status and processed record counts to the 'job_log' table.
--
-- To run:
-- CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
--   'YourJobId', 'YourEntryNr', '01012023', '0'
-- );
--
-- IMPORTANT: Replace `your_project_id.your_dataset_id` with your actual BigQuery project and dataset.
-- The core data processing logic (commented out) needs to be implemented based on the migration of
-- the original 'd_ausd_bp_ta_bcp_msisdn.sql' file.
--

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.r_ausd_bp_ta_bcp_msisdn`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  -- Declare variables
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt'; -- Derived from original script logic
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_restart_value STRING DEFAULT '0';

  -- Parameter initialization and validation
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET v_restart_value = '0';
  ELSE
    SET v_restart_value = p_wiederanlaufWert;
  END IF;

  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_msg = 'Jobkennung fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_err_msg = 'EintragsNr fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_err_msg = 'Stichtag fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- Validate Stichtag format (DDMMYYYY)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SET v_err_msg = 'Stichtag hat kein gueltiges Format DDMMYYYY';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- Core processing logic (migrated from d_ausd_bp_ta_bcp_msisdn.sql)
  BEGIN
    -- Placeholder for the actual INSERT/MERGE/UPDATE statements
    -- that perform the data processing based on p_EintragsNr, v_stichtag_date, etc.
    -- This section needs to be populated with the BigQuery SQL translation of
    -- the original 'd_ausd_bp_ta_bcp_msisdn.sql' content.
    -- Example structure:
    -- INSERT INTO `your_project_id.your_dataset_id.target_table` (col1, col2, process_date, ...)
    -- SELECT
    --   source_col1,
    --   source_col2,
    --   v_stichtag_date,
    --   ...
    -- FROM
    --   `your_project_id.your_dataset_id.source_table`
    -- WHERE
    --   ...;

    -- For demonstration, assuming a target_table exists and has a process_date column
    -- If your logic inserts into a temporary table or returns a count directly, adjust this part.
    -- This COUNT(*) assumes the core logic has already loaded data into `target_table`
    -- filtered by the `v_stichtag_date`.
    SET v_records = (
      SELECT COUNT(*)
      FROM `your_project_id.your_dataset_id.target_table` -- Replace with your actual target table
      WHERE DATE(process_date) = v_stichtag_date -- Adjust column name if different
    );

    -- Log job success
    INSERT INTO `your_project_id.your_dataset_id.job_log` (job_name, entry_nr, stichtag, restart_value, records_processed, status, created_at)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_restart_value, v_records, 'SUCCESS', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Log job failure
    INSERT INTO `your_project_id.your_dataset_id.job_log` (job_name, entry_nr, stichtag, restart_value, records_processed, status, created_at, error_message)
    VALUES (v_TabName, p_EintragsNr, p_Stichtag, v_restart_value, v_records, 'FAILED', CURRENT_TIMESTAMP(), @@error.message);
    RAISE; -- Re-raise the error to propagate it
  END;

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
  SELECT v_records AS records_processed;
END;