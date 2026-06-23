-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- This file contains the BigQuery Stored Procedure for orchestrating the ETL process.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.k_ausd_bp_ta_rn_da_vda_tk`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING, -- Expected DDMMYYYY
  p_wiederanlaufWert STRING
)
OPTIONS(
  description="Migrated orchestration procedure from k_ausd_bp_ta_rn_da_vda_tk.ksh"
)
BEGIN
  -- Variable Declarations
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_record_count INT64;
  DECLARE v_error_message STRING;
  DECLARE v_error_stack STRING;
  DECLARE v_p_wiederanlaufWert_final STRING;

  -- Initialize derived dates
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- 1. Parameter Validation (replacing pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_error_message = 'Parameter p_JobKennung is missing or empty.';
    INSERT INTO `your_project_id.your_dataset_id.error_log` (job_id, error_message, error_details, created_at)
    VALUES (p_JobKennung, v_error_message, 'Missing mandatory parameter', CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_error_message = 'Parameter p_EintragsNr is missing or empty.';
    INSERT INTO `your_project_id.your_dataset_id.error_log` (job_id, error_message, error_details, created_at)
    VALUES (p_JobKennung, v_error_message, 'Missing mandatory parameter', CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET v_error_message = 'Parameter p_Stichtag is missing or empty.';
    INSERT INTO `your_project_id.your_dataset_id.error_log` (job_id, error_message, error_details, created_at)
    VALUES (p_JobKennung, v_error_message, 'Missing mandatory parameter', CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  -- Set default for p_wiederanlaufWert if not provided
  SET v_p_wiederanlaufWert_final = COALESCE(TRIM(p_wiederanlaufWert), '0');

  -- 2. Date Validation (replacing DWDate_Datum_Check)
  BEGIN
    SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_stichtag_date IS NULL THEN
      SET v_error_message = FORMAT("Invalid date format for p_Stichtag: '%s'. Expected DDMMYYYY.", p_Stichtag);
      INSERT INTO `your_project_id.your_dataset_id.error_log` (job_id, error_message, error_details, created_at)
      VALUES (p_JobKennung, v_error_message, 'Date format error', CURRENT_TIMESTAMP());
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;
  EXCEPTION WHEN ERROR THEN
    SET v_error_message = FORMAT("Error parsing p_Stichtag '%s': %s", p_Stichtag, @@error.message);
    INSERT INTO `your_project_id.your_dataset_id.error_log` (job_id, error_message, error_details, created_at)
    VALUES (p_JobKennung, v_error_message, @@error.stack_trace, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END;

  -- Main ETL Logic
  BEGIN
    -- Log job start (replacing FOSJobErzeugeEintrag for start)
    INSERT INTO `your_project_id.your_dataset_id.job_table` (job_id, start_timestamp, status, params)
    VALUES (p_JobKennung, CURRENT_TIMESTAMP(), 'RUNNING', TO_JSON(STRUCT(p_JobKennung, p_EintragsNr, p_Stichtag, v_p_wiederanlaufWert_final AS p_wiederanlaufWert)));

    -- Call the core data transformation procedure
    CALL `your_project_id.your_dataset_id.d_ausd_bp_ta_rn_da_vda_tk_core`(v_stichtag_date);

    -- Record Counting
    SELECT COUNT(*) INTO v_record_count FROM `your_project_id.your_dataset_id.sof$ta_rn_da_vda_tk`;

    -- Log job success (replacing FOSJobErzeugeEintrag for success)
    INSERT INTO `your_project_id.your_dataset_id.job_table` (job_id, end_timestamp, status, record_count)
    VALUES (p_JobKennung, CURRENT_TIMESTAMP(), 'SUCCEEDED', v_record_count);

    SELECT FORMAT('Job %s completed successfully. Processed %d records.', p_JobKennung, v_record_count);

  EXCEPTION WHEN ERROR THEN
    -- Capture error details
    SET v_error_message = @@error.message;
    SET v_error_stack = @@error.stack_trace;

    -- Log error (replacing DWMSG_MeldeFehler)
    INSERT INTO `your_project_id.your_dataset_id.error_log` (job_id, error_message, error_details, created_at)
    VALUES (p_JobKennung, v_error_message, v_error_stack, CURRENT_TIMESTAMP());

    -- Update job status to FAILED (replacing FOSJobErzeugeEintrag for failure)
    INSERT INTO `your_project_id.your_dataset_id.job_table` (job_id, end_timestamp, status, error_message)
    VALUES (p_JobKennung, CURRENT_TIMESTAMP(), 'FAILED', v_error_message);

    -- Re-raise the error to terminate the procedure
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Job %s failed: %s', p_JobKennung, v_error_message);
  END;

END;