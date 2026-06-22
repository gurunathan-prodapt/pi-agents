-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
-- Target: BigQuery Stored Procedure for the wrapper logic

-- This script defines the main wrapper stored procedure for the BERT_V_TA_CNTRCT_VALID process.
-- It replaces the legacy r_ausd_v_ta_cntrct_valid.ksh KornShell script.
-- Replace 'project' and 'dataset' with your actual GCP project ID and BigQuery dataset name.

CREATE OR REPLACE PROCEDURE project.dataset.BERT_V_TA_CNTRCT_VALID(
    IN p_job_kennung STRING,      -- Corresponds to JobKennung from legacy script (e.g., -j parameter)
    IN p_eintragsnr INT64,        -- Corresponds to EintragsNr from legacy script (e.g., -e parameter)
    IN p_program_name STRING,     -- Name of this program, typically 'BERT_V_TA_CNTRCT_VALID'
    IN p_caller_process STRING    -- The system triggering this SP, e.g., 'Cloud Scheduler', 'Cloud Composer'
)
OPTIONS(
  description="BigQuery wrapper stored procedure for BERT_V_TA_CNTRCT_VALID, handling orchestration, logging, and error management."
)
BEGIN
    DECLARE job_run_id_val STRING;
    DECLARE job_params_json JSON;
    DECLARE error_message_val STRING DEFAULT NULL;
    DECLARE job_status STRING DEFAULT 'FAILED';

    -- Construct JSON for parameters to be stored in the job log
    SET job_params_json = TO_JSON(STRUCT(
        p_job_kennung AS job_kennung,
        p_eintragsnr AS eintragsnr,
        p_program_name AS program_name,
        p_caller_process AS caller_process
    ));

    -- Initialize job log entry with 'RUNNING' status
    CALL project.dataset.create_job_log_entry(
        p_program_name,
        job_params_json,
        p_caller_process,
        job_run_id_val
    );

    BEGIN
        -- Log informational message to standard output, which will be captured by Cloud Logging
        SELECT FORMAT("INFO: Job Run ID %s - Starting core logic procedure project.dataset.BERT_K_TA_CNTRCT_VALID with JobKennung: %s, EintragsNr: %d",
                      job_run_id_val, p_job_kennung, p_eintragsnr) AS log_message;

        -- Call the core logic stored procedure. This procedure would contain the actual data transformation logic.
        -- It is expected to be migrated from k_ausd_v_ta_cntrct_valid.ksh.
        CALL project.dataset.BERT_K_TA_CNTRCT_VALID(p_job_kennung, p_eintragsnr);

        -- If the core logic completes without an error, set the job status to SUCCESS
        SET job_status = 'SUCCESS';
        SELECT FORMAT("INFO: Job Run ID %s - Core logic completed successfully.", job_run_id_val) AS log_message;

    EXCEPTION WHEN ERROR THEN
        -- Capture the error message if any exception occurs during core logic execution
        SET error_message_val = @@error.message;
        SELECT FORMAT("ERROR: Job Run ID %s - Core logic failed with error: %s", job_run_id_val, error_message_val) AS log_message;

    END;

    -- Update the final job log entry status (SUCCESS or FAILED) along with end timestamp and error message
    CALL project.dataset.update_job_log_status(
        job_run_id_val,
        job_status,
        error_message_val
    );

    -- If the job failed, raise an error to indicate failure to the caller/orchestrator (e.g., Cloud Scheduler)
    IF job_status = 'FAILED' THEN
        RAISE USING MESSAGE = FORMAT("Job %s (Run ID: %s) failed. Error: %s", p_program_name, job_run_id_val, COALESCE(error_message_val, 'Unknown error.'));
    END IF;

END;