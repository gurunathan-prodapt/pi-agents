-- BigQuery Stored Procedure for k_ausd_bp_ta_bpr_instance.ksh
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This procedure orchestrates the data preparation process, including parameter validation and calling the core logic.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING, -- Expected format: 'DDMMYYYY'
  p_wiederanlaufWert STRING
)
OPTIONS(
    description="Migrated orchestration logic for Basisprodukt preparation process."
)
BEGIN
  DECLARE v_target_table_name STRING DEFAULT 'sof_ta_bpr_instance'; -- Or 'PoolBasisprodukt' if that's the final name
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value STRING DEFAULT '0';
  DECLARE v_job_status STRING DEFAULT 'SUCCESS';

  -- Parameter checks and error logging
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 1;
    SET v_err_arg = 'Jobkennung';
  END IF;

  IF v_err_nr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_err_nr = 2; -- Example error code
    SET v_err_arg = 'EintragsNr';
  END IF;

  IF v_err_nr = 0 AND (p_Stichtag IS NULL OR TRIM(p_Stichtag) = '') THEN
    SET v_err_nr = 3; -- Example error code
    SET v_err_arg = 'Stichtag';
  END IF;

  IF v_err_nr <> 0 THEN
    INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_name, error_code, error_arg, created_at)
    VALUES ('r_ausd_bp_ta_bpr_instance', v_err_nr, v_err_arg, CURRENT_TIMESTAMP());
    SET v_job_status = 'FAILURE';
    -- Exit procedure
    SELECT FORMAT('Error: %s is missing or empty. Error code: %d', v_err_arg, v_err_nr);
    RETURN;
  END IF;

  -- Date validation (DDMMYYYY)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    SET v_err_nr = 193; -- As per design doc
    SET v_err_arg = CONCAT('Invalid Stichtag format: ', p_Stichtag);
    INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_name, error_code, error_arg, created_at)
    VALUES ('r_ausd_bp_ta_bpr_instance', v_err_nr, v_err_arg, CURRENT_TIMESTAMP());
    SET v_job_status = 'FAILURE';
    -- Exit procedure
    SELECT FORMAT('Error: Invalid Stichtag format. Error code: %d', v_err_nr);
    RETURN;
  END IF;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN
    SET v_restart_value = '0';
  ELSE
    SET v_restart_value = p_wiederanlaufWert;
  END IF;

  -- Yesterday and today
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  BEGIN
    -- Execute migrated SQL logic (d_ausd_bp_ta_bpr_instance)
    CALL `your_project_id.your_dataset_id.d_ausd_bp_ta_bpr_instance`(
      p_EintragsNr, p_JobKennung, p_Stichtag, v_restart_value, v_datum_heute, v_datum_gestern
    );

    -- Record count replacement for temp file
    -- Assuming 'business_date' column exists in the target table or derived from p_Stichtag.
    -- If 'sof_ta_bpr_instance' is not partitioned by date, consider removing the WHERE clause or adjusting it.
    SET v_records = (SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` /* WHERE some_date_column = v_stichtag_date */);

  EXCEPTION WHEN ERROR THEN
    SET v_err_nr = 999; -- Generic SQL execution error
    SET v_err_arg = ERROR_MESSAGE();
    INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_name, error_code, error_arg, created_at)
    VALUES ('r_ausd_bp_ta_bpr_instance', v_err_nr, v_err_arg, CURRENT_TIMESTAMP());
    SET v_job_status = 'FAILURE';
    -- Re-raise error to indicate failure to caller
    RAISE;
  END;

  -- Log successful run
  INSERT INTO `your_project_id.your_dataset_id.job_run_log` (job_name, job_id, entry_nr, business_date, record_count, status, created_at)
  VALUES ('r_ausd_bp_ta_bpr_instance', p_JobKennung, p_EintragsNr, v_stichtag_date, v_records, v_job_status, CURRENT_TIMESTAMP());

END;