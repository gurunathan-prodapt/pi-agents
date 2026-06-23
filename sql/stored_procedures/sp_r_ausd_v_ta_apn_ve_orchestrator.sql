-- Header: BigQuery Stored Procedure for job orchestration
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

-- Placeholder for BigQuery project and dataset
-- Replace `your_project_id.your_dataset_id` with your actual project and dataset.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_r_ausd_v_ta_apn_ve_orchestrator`(
    p_process_date DATE, -- Corresponds to a date parameter, e.g., from original shell script options
    p_job_version STRING DEFAULT '1.0', -- Corresponds to ProgVersion
    p_job_name STRING DEFAULT 'r_ausd_v_ta_apn_ve_orchestrator' -- Corresponds to ProgName/JobKennung
)
OPTIONS(
  description="Orchestrates the reconciliation of contract data for ta_apn_ve, replacing r_ausd_v_ta_apn_ve.ksh. Handles parameters, logging, and error trapping."
)
BEGIN
    -- Declare variables for job execution tracking, mimicking shell script environment variables
    DECLARE v_job_run_id STRING;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack_trace STRING;
    DECLARE v_log_correlation_id STRING;

    -- Mimic DWMSG_ErmittleNr: Generate a unique run ID for this execution
    SET v_job_run_id = GENERATE_UUID();
    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    -- Mimic DWMSG_Logdateiname functionality for correlation in Cloud Logging
    SET v_log_correlation_id = FORMAT('%s_%s', p_job_name, REPLACE(v_job_run_id, '-', ''));

    -- Mimic DWMSG_ErzeugeEintrag: Insert initial job entry into the job_log table
    INSERT INTO `your_project_id.your_dataset_id.job_log` (
        job_run_id,
        job_name,
        start_timestamp,
        status,
        process_date,
        version,
        log_correlation_id,
        additional_info
    )
    VALUES (
        v_job_run_id,
        p_job_name,
        v_start_timestamp,
        v_status,
        p_process_date,
        p_job_version,
        v_log_correlation_id,
        TO_JSON(STRUCT(p_process_date AS input_process_date))
    );

    SELECT FORMAT('INFO: Job %s (Run ID: %s) started at %t for process date %t.', p_job_name, v_job_run_id, v_start_timestamp, p_process_date);

    -- Main logic block with error handling (replaces shell 'set -eu' and 'trap' mechanisms)
    BEGIN
        -- Mimic DWMSG_SetzeStichtagInfo if any specific "Stichtag" (key date) info
        -- needs to be stored or validated beyond the p_process_date parameter.
        -- For this migration, p_process_date is assumed to cover the primary date context.

        -- Call the core reconciliation logic procedure
        -- This replaces the shell call to k_ausd_v_ta_apn_ve.ksh
        CALL `your_project_id.your_dataset_id.sp_k_ausd_v_ta_apn_ve_combined`(
            v_job_run_id,
            p_process_date
        );

        SET v_status = 'SUCCESS';
        SELECT FORMAT('INFO: Job %s (Run ID: %s) completed successfully.', p_job_name, v_job_run_id);

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;
        SET v_error_stack_trace = @@error.stack_trace;
        SELECT FORMAT('ERROR: Job %s (Run ID: %s) failed with error: %s', p_job_name, v_job_run_id, v_error_message);
        -- Mimic DWMSG_MeldeFehler, DWMSG_Fehlerbehandlung: Error will be logged in the final UPDATE below.
    END;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Mimic DWMSG_SetzeStatusOK / DWMSG_Fehlerbehandlung: Update final status in job_log table
    UPDATE `your_project_id.your_dataset_id.job_log`
    SET
        end_timestamp = v_end_timestamp,
        status = v_status,
        error_message = v_error_message,
        error_stack_trace = v_error_stack_trace,
        additional_info = TO_JSON(STRUCT(
            CURRENT_USER() AS executed_by,
            p_job_version AS final_version,
            (SELECT IFNULL(MAX(job_run_id), 'N/A') FROM `your_project_id.your_dataset_id.job_log` WHERE job_name = p_job_name AND status = 'SUCCESS') AS last_successful_run
        ))
    WHERE job_run_id = v_job_run_id;

    -- If the job failed, re-raise the error to signal upstream orchestrators (e.g., Airflow)
    IF v_status = 'FAILED' THEN
        RAISE;
    END IF;

END;