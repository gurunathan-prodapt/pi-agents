-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh
-- Description: BigQuery Stored Procedure encapsulating the orchestration logic.
CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Expected format: DDMMYYYY
  IN p_wiederanlaufWert INT64 -- Optional restart value
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_resolved_wiederanlaufWert INT64;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING;
  DECLARE v_err_nr INT64 DEFAULT 0;

  -- 1. Parameter Validation
  -- p_JobKennung check
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 1;
    SET v_err_msg = 'Jobkennung fehlt (p_JobKennung).';
  END IF;

  -- p_EintragsNr check (assuming it's also mandatory for logging as in legacy)
  IF v_err_nr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_err_nr = 2;
    SET v_err_msg = 'Eintragsnummer fehlt (p_EintragsNr).';
  END IF;

  -- p_Stichtag check for presence
  IF v_err_nr = 0 AND (p_Stichtag IS NULL OR TRIM(p_Stichtag) = '') THEN
    SET v_err_nr = 3;
    SET v_err_msg = 'Stichtag fehlt (p_Stichtag).';
  END IF;

  -- If any initial parameter validation error, log and raise exception
  IF v_err_nr <> 0 THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, error_nr, error_msg, created_at)
    VALUES (IFNULL(p_JobKennung, 'UNKNOWN_JOB'), IFNULL(p_EintragsNr, 'UNKNOWN_ENTRY'), v_err_nr, v_err_msg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_err_msg;
  END IF;

  -- Date validation for p_Stichtag (DDMMYYYY)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    SET v_err_nr = 4;
    SET v_err_msg = FORMAT('Stichtag %s hat ungueltiges Format DDMMYYYY.', p_Stichtag);
    INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, error_nr, error_msg, created_at)
    VALUES (p_JobKennung, p_EintragsNr, v_err_nr, v_err_msg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_err_msg;
  END IF;

  -- Restart value initialization
  SET v_resolved_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- 2. Core Data Processing (equivalent to d_ausd_bp_ta_apn_carmen.sql)
  -- This section would contain the translated BigQuery SQL logic.
  -- For demonstration, using the pseudocode from the design document.

  -- The actual content of d_ausd_bp_ta_apn_carmen.sql should be translated and
  -- either embedded here directly, or called as a separate script.
  -- If called as a separate script, consider EXECUTE IMMEDIATE or BQ CLI call.
  -- For this example, we'll embed a simplified version of the logic.

  -- Example: Create a temporary table with processed data
  CREATE OR REPLACE TEMPORARY TABLE `tmp_processed_carmen_data` AS
  SELECT
      -- Placeholder columns, replace with actual schema from source SQL
      'BP_PROD_123' AS product_id,
      v_stichtag_date AS process_date,
      'APN_CARMEN' AS source_system_id,
      TO_JSON(STRUCT(
          -- Example payload fields
          'data_field_1' AS field1,
          'data_field_2' AS field2,
          v_resolved_wiederanlaufWert AS restart_val_used
      )) AS payload
  FROM
      `project.dataset.source_table_for_carmen` -- This is a placeholder for the actual source table
  WHERE
      process_date_col = v_stichtag_date
      AND restart_value_col >= v_resolved_wiederanlaufWert
  LIMIT 10; -- Limiting for placeholder, remove in actual implementation

  SET v_records = (SELECT COUNT(*) FROM `tmp_processed_carmen_data`);

  INSERT INTO `project.dataset.PoolBasisprodukt_target`
  (product_id, process_date, source_system_id, created_at, payload)
  SELECT
      product_id,
      process_date,
      source_system_id,
      CURRENT_TIMESTAMP(),
      payload
  FROM
      `tmp_processed_carmen_data`;

  -- Optional: Post-processing logic if `cibasis_postprocessing_bq.sql` is active
  -- If `cibasis_postprocessing_bq.sql` contains logic, it would be called here.
  -- Example: EXECUTE IMMEDIATE (SELECT content FROM `project.dataset.cibasis_postprocessing_bq_script_table`);

  -- 3. Job Logging (equivalent to FOSJobErzeugeEintrag)
  INSERT INTO `project.dataset.job_control_log`
  (tab_name, status_a, status_i, stichtag_from, stichtag_to, job_type, active_flag, record_count, comment_text, job_name, entry_nr, created_at)
  VALUES
  (v_TabName, 'A', 'I', v_stichtag_date, v_stichtag_date, 'J', 'N', v_records, 'Initialbefuellung durch sp_k_ausd_bp_ta_apn_carmen', p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP());

EXCEPTION WHEN ERROR THEN
  -- Catch any unhandled SQL errors, log them, and re-raise
  SET v_err_nr = -1; -- Generic error number for unhandled exceptions
  SET v_err_msg = FORMAT("Unhandled SQL error: %s", @@error.message);
  INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, error_nr, error_msg, created_at)
  VALUES (IFNULL(p_JobKennung, 'UNKNOWN_JOB'), IFNULL(p_EintragsNr, 'UNKNOWN_ENTRY'), v_err_nr, v_err_msg, CURRENT_TIMESTAMP());
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_err_msg;

END;