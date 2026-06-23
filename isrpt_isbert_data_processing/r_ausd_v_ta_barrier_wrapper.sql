-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh

CREATE OR REPLACE PROCEDURE `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`(
    p_job_name STRING,
    p_job_kennung STRING, -- Corresponds to -j option
    p_aktiv_nr STRING    -- Corresponds to -f option
)
BEGIN
    DECLARE v_error_message STRING;
    DECLARE v_script_name STRING DEFAULT 'r_ausd_v_ta_barrier_wrapper';
    DECLARE v_log_message STRING;

    -- Define BigQuery table names for logging and job control
    -- These should be created as part of step 3 in the build plan.
    DECLARE C_JOB_AKTIV_TAB_NAME STRING DEFAULT '`isrpt_isbert_data_processing.job_table`';
    DECLARE C_JOB_LOG_TAB_NAME STRING DEFAULT '`isrpt_isbert_data_processing.job_log`';
    DECLARE C_JOB_ERROR_LOG_TAB_NAME STRING DEFAULT '`isrpt_isbert_data_processing.job_error_log`';
    DECLARE C_SQL_EXECUTION_RESULTS_TAB_NAME STRING DEFAULT '`isrpt_isbert_data_processing.sql_execution_results`';
    DECLARE C_JOB_RUN_SUMMARY_TAB_NAME STRING DEFAULT '`isrpt_isbert_data_processing.job_run_summary`'; -- Not explicitly used, but for completeness

    -- Initial Logging
    INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
    VALUES (p_job_kennung, p_job_name, 'Wrapper script started.', CURRENT_TIMESTAMP(), 'INFO');

    BEGIN
        -- Call the control stored procedure
        CALL `isrpt_isbert_data_processing.k_ausd_v_ta_barrier_control`(
            p_job_name,
            p_job_kennung,
            p_aktiv_nr,
            'isrpt_isbert_data_processing', -- Schema name for table references
            C_JOB_AKTIV_TAB_NAME,
            C_JOB_LOG_TAB_NAME,
            C_JOB_ERROR_LOG_TAB_NAME,
            C_SQL_EXECUTION_RESULTS_TAB_NAME
        );

        -- Final success log
        SET v_log_message = 'Wrapper script completed successfully for job: ' || p_job_name || ' (' || p_job_kennung || ')';
        INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
        VALUES (p_job_kennung, p_job_name, v_log_message, CURRENT_TIMESTAMP(), 'INFO');

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_log_message = 'Wrapper script failed for job: ' || p_job_name || ' (' || p_job_kennung || ') with error: ' || v_error_message;
        INSERT INTO `isrpt_isbert_data_processing.job_error_log` (job_id, job_name, script_name, error_message, error_timestamp)
        VALUES (p_job_kennung, p_job_name, v_script_name, v_error_message, CURRENT_TIMESTAMP());
        INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
        VALUES (p_job_kennung, p_job_name, v_log_message, CURRENT_TIMESTAMP(), 'ERROR');
        RAISE; -- Re-raise the error for external orchestration tools (e.g., Cloud Composer)
    END;
END;