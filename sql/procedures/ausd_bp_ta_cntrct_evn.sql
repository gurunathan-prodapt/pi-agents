-- Main Orchestration BigQuery Stored Procedure
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag_final STRING;
  DECLARE v_wiederanlaufWert_final INT64;
  DECLARE v_run_id STRING;

  SET v_run_id = GENERATE_UUID(); -- Unique identifier for this job run

  -- Initialize restart value: p_wiederanlaufWert defaults to 0 if not provided
  SET v_wiederanlaufWert_final = IFNULL(p_wiederanlaufWert, 0);

  -- System date in DDMMYYYY format
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default cutoff date: p_stichtag defaults to v_sysdate if not explicitly set
  SET v_stichtag_final = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

  -- Log job start
  INSERT INTO `your_project_id.your_dataset_id.job_log`
    (job_name, log_level, message, created_at)
  VALUES
    (v_job_kennung, 'INFO', 'Job started. Run ID: ' || v_run_id || ', Stichtag: ' || v_stichtag_final || ', Wiederanlaufwert: ' || v_wiederanlaufWert_final, CURRENT_TIMESTAMP());

  -- Validate required parameter: Stichtag
  IF v_stichtag_final IS NULL OR v_stichtag_final = '' THEN
    INSERT INTO `your_project_id.your_dataset_id.job_log`
      (job_name, log_level, message, created_at)
    VALUES
      (v_job_kennung, 'ERROR', 'Stichtag parameter missing. Run ID: ' || v_run_id, CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = 'Stichtag parameter missing.';
  END IF;

  -- Trap for errors during core processing
  BEGIN
    -- Core processing: Call the migrated kernel stored procedure
    CALL `your_project_id.your_dataset_id.process_contract_data`(v_stichtag_final, v_wiederanlaufWert_final);

    -- Log success
    INSERT INTO `your_project_id.your_dataset_id.job_log`
      (job_name, log_level, message, created_at)
    VALUES
      (v_job_kennung, 'INFO', 'Job completed successfully. Run ID: ' || v_run_id, CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO `your_project_id.your_dataset_id.job_log`
      (job_name, log_level, message, created_at)
    VALUES
      (v_job_kennung, 'ERROR', 'Job failed. Run ID: ' || v_run_id || '. Error: ' || @@error.message, CURRENT_TIMESTAMP());
    
    -- Re-raise the error to propagate it
    RAISE;
  END;

END;