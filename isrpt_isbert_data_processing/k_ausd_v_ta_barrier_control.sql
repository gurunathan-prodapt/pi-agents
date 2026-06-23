-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh

CREATE OR REPLACE PROCEDURE `isrpt_isbert_data_processing.k_ausd_v_ta_barrier_control`(
    p_job_name STRING,
    p_job_kennung STRING,
    p_aktiv_nr STRING,
    p_schema_name STRING,
    p_job_aktiv_tab_name STRING,
    p_job_log_tab_name STRING,
    p_job_error_log_tab_name STRING,
    p_sql_execution_results_tab_name STRING
)
BEGIN
    DECLARE v_error_message STRING;
    DECLARE v_job_active BOOL;
    DECLARE v_sql_skript_name STRING DEFAULT 'd_ausd_v_ta_barrier_etl';
    DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Logging: Job Start
    INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
    VALUES (p_job_kennung, p_job_name, 'Control script started.', v_current_timestamp, 'INFO');

    -- Parameter validation (simplified for BQ SP)
    IF p_job_kennung IS NULL OR p_job_aktiv_tab_name IS NULL THEN
        SET v_error_message = 'Essential parameters (job_kennung, job_aktiv_tab_name) are missing.';
        INSERT INTO `isrpt_isbert_data_processing.job_error_log` (job_id, job_name, script_name, error_message, error_timestamp)
        VALUES (p_job_kennung, p_job_name, 'k_ausd_v_ta_barrier_control', v_error_message, v_current_timestamp);
        INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
        VALUES (p_job_kennung, p_job_name, 'Control script failed: ' || v_error_message, v_current_timestamp, 'ERROR');
        RAISE;
    END IF;

    -- Check if job is already active
    SET v_job_active = (SELECT COUNT(1) FROM `isrpt_isbert_data_processing.job_table` WHERE job_kennung = p_job_kennung AND status = 'ACTIVE');

    IF v_job_active THEN
        SET v_error_message = 'Job ' || p_job_kennung || ' is already active. Ignoring this run.';
        INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
        VALUES (p_job_kennung, p_job_name, v_error_message, v_current_timestamp, 'WARNING');
        -- Exit gracefully without re-raising as per legacy behavior
    ELSE
        -- Deactivate any older, potentially stuck jobs for this job_kennung
        UPDATE `isrpt_isbert_data_processing.job_table`
        SET status = 'DEACTIVATED', end_timestamp = v_current_timestamp, last_modified = v_current_timestamp
        WHERE job_kennung = p_job_kennung AND status = 'ACTIVE';

        -- Activate current job
        INSERT INTO `isrpt_isbert_data_processing.job_table` (job_kennung, status, start_timestamp, last_modified)
        VALUES (p_job_kennung, 'ACTIVE', v_current_timestamp, v_current_timestamp);

        -- Logging: Call SQL Script
        INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
        VALUES (p_job_kennung, p_job_name, 'Calling SQL transformation script: ' || v_sql_skript_name, v_current_timestamp, 'INFO');

        BEGIN
            CALL `isrpt_isbert_data_processing.starteSQLSkript`(
                p_job_kennung,
                p_job_name,
                v_sql_skript_name,
                p_job_aktiv_tab_name,
                p_job_log_tab_name,
                p_sql_execution_results_tab_name
            );

            -- Deactivate job upon successful completion
            UPDATE `isrpt_isbert_data_processing.job_table`
            SET status = 'SUCCESS', end_timestamp = CURRENT_TIMESTAMP(), last_modified = CURRENT_TIMESTAMP()
            WHERE job_kennung = p_job_kennung AND status = 'ACTIVE';

            INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
            VALUES (p_job_kennung, p_job_name, 'Control script completed successfully.', CURRENT_TIMESTAMP(), 'INFO');

        EXCEPTION WHEN ERROR THEN
            SET v_error_message = @@error.message;
            -- Update job status to FAILED
            UPDATE `isrpt_isbert_data_processing.job_table`
            SET status = 'FAILED', end_timestamp = CURRENT_TIMESTAMP(), last_modified = CURRENT_TIMESTAMP()
            WHERE job_kennung = p_job_kennung AND status = 'ACTIVE';

            INSERT INTO `isrpt_isbert_data_processing.job_error_log` (job_id, job_name, script_name, error_message, error_timestamp)
            VALUES (p_job_kennung, p_job_name, 'k_ausd_v_ta_barrier_control', v_error_message, CURRENT_TIMESTAMP());
            INSERT INTO `isrpt_isbert_data_processing.job_log` (job_id, job_name, log_message, log_timestamp, log_level)
            VALUES (p_job_kennung, p_job_name, 'Control script failed during SQL execution: ' || v_error_message, CURRENT_TIMESTAMP(), 'ERROR');
            RAISE; -- Re-raise the error to the calling procedure
        END;
    END IF;
END;