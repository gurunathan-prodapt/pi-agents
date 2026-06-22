-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
-- This file contains the BigQuery stored procedure that acts as a wrapper,
-- replacing the functionality of r_ausd_v_ta_inv_assign.ksh.
-- Replace 'your_gcp_project_id.your_bq_dataset_name' with your actual project ID and dataset name.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(
  IN p_h BOOL,
  IN p_s STRING, -- Placeholder, as it was parsed but not explicitly used in the original ksh.
  IN p_l STRING  -- Placeholder, as it was parsed but not explicitly used in the original ksh.
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_INV_ASSIGN';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE DW_EintragsNr INT64;
  DECLARE v_log_message STRING;
  DECLARE v_status STRING;

  -- Parameter validation for help flag
  IF p_h THEN
    SELECT 'Usage: CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => TRUE, p_s => NULL, p_l => NULL);';
    SELECT '       CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => ''optional_s_param'', p_l => ''optional_l_param'');';
    SELECT '       This procedure orchestrates the contract data reconciliation for ta_inv_assign.';
    RETURN;
  END IF;

  -- Determine the next entry number for logging
  SET DW_EintragsNr = (SELECT IFNULL(MAX(entry_nr), 0) + 1 FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries` WHERE job_kennung = JobKennung);

  -- Log job start
  INSERT INTO `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`
    (entry_nr, job_kennung, script_name, sysdate_ddmmyyyy, status, created_at)
  VALUES
    (DW_EintragsNr, JobKennung, ProgName, v_sysdate, 'STARTED', CURRENT_TIMESTAMP());

  SET v_log_message = 'Job gestartet. JobKennung: ' || JobKennung || ', EntryNr: ' || CAST(DW_EintragsNr AS STRING);
  INSERT INTO `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
    (entry_nr, job_kennung, message, created_at)
  VALUES
    (DW_EintragsNr, JobKennung, v_log_message, CURRENT_TIMESTAMP());

  BEGIN
    -- Call the core processing stored procedure
    CALL `your_gcp_project_id.your_bq_dataset_name.k_ausd_v_ta_inv_assign`(JobKennung, DW_EintragsNr);

    -- If core procedure completes without error
    SET v_log_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet.';
    INSERT INTO `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
      (entry_nr, job_kennung, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, v_log_message, CURRENT_TIMESTAMP());

    SET v_status = 'OK';

  EXCEPTION WHEN ERROR THEN
    -- Error handling block for any errors during core procedure call
    DECLARE error_message STRING DEFAULT ERROR_MESSAGE();
    DECLARE error_stack STRING DEFAULT ERROR_STACK();
    DECLARE error_code STRING DEFAULT ERROR_CODE();

    SET v_log_message = 'AppError: Abbruch. Error: ' || error_message || '. Stack: ' || error_stack;
    INSERT INTO `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
      (entry_nr, job_kennung, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, v_log_message, CURRENT_TIMESTAMP());

    INSERT INTO `your_gcp_project_id.your_bq_dataset_name.dw_error_log`
      (entry_nr, job_kennung, error_message, error_code, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, error_message, error_code, CURRENT_TIMESTAMP());

    SET v_status = 'ERROR';

    -- Re-raise the error to signal failure to the caller
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Job execution failed: ' || error_message;
  END;

  -- Update final job status in dw_job_entries
  UPDATE `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`
  SET status = v_status, finished_at = CURRENT_TIMESTAMP()
  WHERE entry_nr = DW_EintragsNr AND job_kennung = JobKennung;

END;