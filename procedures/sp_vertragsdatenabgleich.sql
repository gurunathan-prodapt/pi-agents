--
-- BigQuery Stored Procedure replacing the KornShell wrapper script
-- vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh.
-- This procedure handles orchestration, parameter parsing, logging, and calls the
-- core processing logic in sp_k_ausd_v_ta_vertrag_tmp.
--
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich`(
  IN p_job_name_param STRING, -- Corresponds to JobKennung (e.g., 'BERT_V_TA_VERTRAG_TMP')
  IN p_stichtag_param DATE -- Corresponds to v_sysdate (optional, defaults to CURRENT_DATE())
)
BEGIN
  -- Declarations for internal variables
  DECLARE v_job_name STRING DEFAULT COALESCE(p_job_name_param, 'BERT_V_TA_VERTRAG_TMP');
  DECLARE v_job_run_id STRING;
  DECLARE v_start_time TIMESTAMP;
  DECLARE v_end_time TIMESTAMP;
  DECLARE v_status STRING;
  DECLARE v_message STRING;
  DECLARE v_error_message STRING;
  DECLARE v_stichtag DATE DEFAULT COALESCE(p_stichtag_param, CURRENT_DATE());

  -- Generate a unique job run ID
  SET v_job_run_id = GENERATE_UUID();
  SET v_start_time = CURRENT_TIMESTAMP();
  SET v_status = 'RUNNING';
  SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich started.';

  -- Log job start
  INSERT INTO `project.dataset.job_audit_log` (job_name, job_run_id, start_time, status, message, stichtag)
  VALUES (v_job_name, v_job_run_id, v_start_time, v_status, v_message, v_stichtag);

  BEGIN
    -- The original script's `getopts` for -s and -l are unused, so not explicitly migrated.
    -- Parameter validation can be added here if needed.

    -- Call the core processing stored procedure
    CALL `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`(v_job_name, v_job_run_id);

    SET v_end_time = CURRENT_TIMESTAMP();
    SET v_status = 'SUCCESS';
    SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich completed successfully. Core script finished.';

    -- Log job success
    INSERT INTO `project.dataset.job_audit_log` (job_name, job_run_id, start_time, end_time, status, message, stichtag)
    VALUES (v_job_name, v_job_run_id, v_start_time, v_end_time, v_status, v_message, v_stichtag);

  EXCEPTION WHEN ERROR THEN
    SET v_end_time = CURRENT_TIMESTAMP();
    SET v_status = 'FAILED';
    SET v_error_message = @@error.message;
    SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich failed due to an error.';

    -- Log job failure
    INSERT INTO `project.dataset.job_audit_log` (job_name, job_run_id, start_time, end_time, status, message, error_message, stichtag)
    VALUES (v_job_name, v_job_run_id, v_start_time, v_end_time, v_status, v_message, v_error_message, v_stichtag);

    -- Re-raise the error so the calling environment is aware of the failure
    RAISE USING MESSAGE = 'Job ' || v_job_name || ' (Run ID: ' || v_job_run_id || ') FAILED: ' || v_error_message;

  END;
END;