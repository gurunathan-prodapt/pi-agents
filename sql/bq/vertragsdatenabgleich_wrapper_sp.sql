--
-- Target code for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Target code for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- This BigQuery stored procedure combines the orchestration logic of the original
-- r_ausd_v_ta_vvl_upgrade.ksh and k_ausd_v_ta_vvl_upgrade.ksh shell scripts.
-- It handles parameter validation, logging, and calls the core data transformation procedure.
--
CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_sp`(
    p_job_kennung STRING,
    p_eintrags_nr STRING
)
BEGIN
    DECLARE v_sysdate_str STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack STRING;
    DECLARE v_row_count INT64;

    -- Initialize job parameters and logging variables
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_sysdate_str = FORMAT_DATE('%Y%m%d', CURRENT_DATE());

    -- Log job start
    INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_log` (log_timestamp, job_kennung, eintrags_nr, log_level, message, procedure_name, status)
    VALUES (v_start_time, p_job_kennung, p_eintrags_nr, 'INFO', 'Job started for Vertragsdatenabgleich.', 'vertragsdatenabgleich_wrapper_sp', 'STARTED');

    -- Parameter validation
    IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
        SET v_error_message = 'Jobkennung parameter is missing or empty.';
        INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_log` (log_timestamp, job_kennung, eintrags_nr, log_level, message, procedure_name, status, error_code, error_argument)
        VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'ERROR', v_error_message, 'vertragsdatenabgleich_wrapper_sp', 'FAILED', 193, 'p_job_kennung');
        RAISE BQ EXCEPTION MESSAGE v_error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR p_eintrags_nr = '' THEN
        SET v_error_message = 'EintragsNr parameter is missing or empty.';
        INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_log` (log_timestamp, job_kennung, eintrags_nr, log_level, message, procedure_name, status, error_code, error_argument)
        VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'ERROR', v_error_message, 'vertragsdatenabgleich_wrapper_sp', 'FAILED', 193, 'p_eintrags_nr');
        RAISE BQ EXCEPTION MESSAGE v_error_message;
    END IF;

    -- Main logic block with error handling
    BEGIN
        -- Call the core data transformation stored procedure
        CALL `your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_upgrade_sp`();

        -- Get row count if needed (assuming d_ausd_v_ta_vvl_upgrade_sp doesn't return it,
        -- or modify it to return row count)
        -- For now, we assume success or failure is handled by the exception block.
        -- If an actual row count is needed, d_ausd_v_ta_vvl_upgrade_sp would need to output it.
        -- Example: SELECT COUNT(1) INTO v_row_count FROM `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade`;

        SET v_end_time = CURRENT_TIMESTAMP();
        INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_log` (log_timestamp, job_kennung, eintrags_nr, log_level, message, procedure_name, status)
        VALUES (v_end_time, p_job_kennung, p_eintrags_nr, 'INFO', 'The processing was completed without recognizable errors.', 'vertragsdatenabgleich_wrapper_sp', 'COMPLETED');

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_stack = @@error.stack_trace;
        SET v_end_time = CURRENT_TIMESTAMP();
        INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_log` (log_timestamp, job_kennung, eintrags_nr, log_level, message, procedure_name, status, error_code, error_argument)
        VALUES (v_end_time, p_job_kennung, p_eintrags_nr, 'ERROR', CONCAT('Job failed: ', v_error_message), 'vertragsdatenabgleich_wrapper_sp', 'FAILED', -1, v_error_stack);
        RAISE; -- Re-raise the exception to propagate the error
    END;

END;