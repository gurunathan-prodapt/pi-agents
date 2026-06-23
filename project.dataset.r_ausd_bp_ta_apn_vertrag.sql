-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_apn_vertrag`(
  IN p_job_kennung_param STRING,
  IN p_eintrags_nr_param STRING,
  IN p_stichtag_param STRING, -- Expected format DDMMYYYY
  IN p_wiederanlauf_wert_param STRING OPTIONS(description="Optional restart value, defaults to '0'")
)
OPTIONS(
  description="Migrated KornShell script k_ausd_bp_ta_apn_vertrag.ksh to BigQuery Stored Procedure. Orchestrates data preparation and execution of core logic."
)
BEGIN
  DECLARE v_tab_name STRING DEFAULT 'PoolBasisprodukt'; -- Variable name from original script
  DECLARE v_records_processed INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_wiederanlauf_wert_final STRING;
  DECLARE v_job_run_id STRING;

  -- Generate a unique job run ID for logging purposes
  SET v_job_run_id = GENERATE_UUID();

  -- Initialize final restart value, defaulting to '0' if not provided
  SET v_wiederanlauf_wert_final = COALESCE(p_wiederanlauf_wert_param, '0');

  -- Log Job Start
  INSERT INTO `project.dataset.job_log` (job_identifier, job_name, start_time, status, message, stichtag, eintrags_nr, wiederanlauf_wert)
  VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'RUNNING', 'Job execution started.', NULL, p_eintrags_nr_param, v_wiederanlauf_wert_final);

  -- Parameter Validation
  IF p_job_kennung_param IS NULL OR p_job_kennung_param = '' THEN
    SET v_err_msg = 'Parameter Error: Jobkennung fehlt (p_job_kennung_param is NULL or empty).';
    INSERT INTO `project.dataset.job_log` (job_identifier, job_name, end_time, status, message, stichtag, eintrags_nr, wiederanlauf_wert)
    VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'FAILED', v_err_msg, NULL, p_eintrags_nr_param, v_wiederanlauf_wert_final);
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_eintrags_nr_param IS NULL OR p_eintrags_nr_param = '' THEN
    SET v_err_msg = 'Parameter Error: EintragsNr fehlt (p_eintrags_nr_param is NULL or empty).';
    INSERT INTO `project.dataset.job_log` (job_identifier, job_name, end_time, status, message, stichtag, eintrags_nr, wiederanlauf_wert)
    VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'FAILED', v_err_msg, NULL, p_eintrags_nr_param, v_wiederanlauf_wert_final);
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_stichtag_param IS NULL OR p_stichtag_param = '' THEN
    SET v_err_msg = 'Parameter Error: Stichtag fehlt (p_stichtag_param is NULL or empty).';
    INSERT INTO `project.dataset.job_log` (job_identifier, job_name, end_time, status, message, stichtag, eintrags_nr, wiederanlauf_wert)
    VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'FAILED', v_err_msg, NULL, p_eintrags_nr_param, v_wiederanlauf_wert_final);
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- Validate Stichtag format (DDMMYYYY)
  IF NOT REGEXP_CONTAINS(p_stichtag_param, r'^[0-9]{8}$') THEN
    SET v_err_msg = 'Parameter Error: Stichtag hat ungueltiges Format (expected DDMMYYYY). Provided: ' || p_stichtag_param;
    INSERT INTO `project.dataset.job_log` (job_identifier, job_name, end_time, status, message, stichtag, eintrags_nr, wiederanlauf_wert)
    VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'FAILED', v_err_msg, NULL, p_eintrags_nr_param, v_wiederanlauf_wert_final);
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- Attempt to parse the date, raise error if invalid date value (e.g., 31022023)
  BEGIN
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag_param);
  EXCEPTION WHEN ERROR THEN
    SET v_err_msg = 'Parameter Error: Stichtag ist kein gueltiges Datum: ' || ERROR_MESSAGE() || '. Provided: ' || p_stichtag_param;
    INSERT INTO `project.dataset.job_log` (job_identifier, job_name, end_time, status, message, stichtag, eintrags_nr, wiederanlauf_wert)
    VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'FAILED', v_err_msg, NULL, p_eintrags_nr_param, v_wiederanlauf_wert_final);
    RAISE USING MESSAGE = v_err_msg;
  END;

  -- Derive dates (replacing legacy gestern.ksh functionality)
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Execute Core SQL Logic (migrated from d_ausd_bp_ta_apn_vertrag.sql)
  -- This calls the separate stored procedure for modularity and maintainability.
  CALL `project.dataset.d_ausd_bp_ta_apn_vertrag_core_logic`(
    v_stichtag_date,
    p_job_kennung_param,
    p_eintrags_nr_param,
    v_wiederanlauf_wert_final,
    v_records_processed
  );

  -- Log Job Finish
  INSERT INTO `project.dataset.job_log` (job_identifier, job_name, end_time, status, records_processed, message, stichtag, eintrags_nr, wiederanlauf_wert)
  VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'SUCCESS', v_records_processed, 'Job completed successfully.', v_stichtag_date, p_eintrags_nr_param, v_wiederanlauf_wert_final);

  -- Optional: Return records_processed for external orchestrator or for debugging.
  SELECT v_records_processed AS records_processed;

EXCEPTION WHEN ERROR THEN
  -- Catch any unhandled errors during execution and log them
  SET v_err_msg = 'Job failed with unhandled error: ' || ERROR_MESSAGE();
  INSERT INTO `project.dataset.job_log` (job_identifier, job_name, end_time, status, message, stichtag, eintrags_nr, wiederanlauf_wert)
  VALUES (v_job_run_id, 'r_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'FAILED', v_err_msg, v_stichtag_date, p_eintrags_nr_param, v_wiederanlauf_wert_final);
  RAISE USING MESSAGE = v_err_msg; -- Re-raise the error to signal job failure to the caller
END;