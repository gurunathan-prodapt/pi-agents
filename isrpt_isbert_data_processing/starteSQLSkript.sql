-- Legacy Source: Part of k_ausd_v_ta_barrier.ksh (function starteSQLSkript)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh

CREATE OR REPLACE PROCEDURE `isrpt_isbert_data_processing.starteSQLSkript`(
    job_id STRING,
    job_name STRING,
    skript_name STRING,
    p_job_aktiv_tab_name STRING,
    p_job_log_tab_name STRING,
    p_sql_execution_results_tab_name STRING
)
BEGIN
    DECLARE v_error_message STRING;
    DECLARE v_records_processed INT64;

    -- Logging: Job Start
    INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
    VALUES (job_id, job_name, 'Starting SQL script.', CURRENT_TIMESTAMP(), 'INFO');

    BEGIN
        -- Job deactivation logic (simplified, assuming p_job_aktiv_tab_name is full path)
        -- The original script had logic for "ignore active job" and "deactivate older job" which should be handled by the calling procedure.
        -- This procedure will only update its own status for the current execution.

        -- Call the ETL procedure
        CALL `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`();

        -- Get records processed (example, actual implementation needs to fetch this from the ETL procedure or source)
        -- For now, we'll assume a placeholder or that the ETL logs its own counts
        SET v_records_processed = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.sof_ta_barrier`);

        -- Log success
        INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
        VALUES (job_id, job_name, 'SQL script completed successfully.', CURRENT_TIMESTAMP(), 'INFO');

        -- Log execution results
        INSERT INTO `isrpt_isbert_data_processing.sql_execution_results` (job_id, job_name, script_name, records_processed, execution_timestamp, status)
        VALUES (job_id, job_name, skript_name, v_records_processed, CURRENT_TIMESTAMP(), 'SUCCESS');

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        -- Log error
        INSERT INTO `isrpt_isbert_data_processing.job_error_log` (job_id, job_name, script_name, error_message, error_timestamp)
        VALUES (job_id, job_name, skript_name, v_error_message, CURRENT_TIMESTAMP());

        INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
        VALUES (job_id, job_name, 'SQL script failed with error: ' || v_error_message, CURRENT_TIMESTAMP(), 'ERROR');

        INSERT INTO `isrpt_isbert_data_processing.sql_execution_results` (job_id, job_name, script_name, records_processed, execution_timestamp, status, error_message)
        VALUES (job_id, job_name, skript_name, 0, CURRENT_TIMESTAMP(), 'FAILED', v_error_message);

        RAISE; -- Re-raise the error to the calling procedure
    END;
END;