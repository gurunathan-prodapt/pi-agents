--
-- BigQuery Stored Procedure for `r_ausd_bp_ta_cntrct_dist.ksh` wrapper logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`(
    IN p_stichtag STRING,
    IN p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_job_nr INT64;
    DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_cntrct_dist_wrapper';
    DECLARE v_source_program STRING DEFAULT 'r_ausd_bp_ta_cntrct_dist.ksh';
    DECLARE v_restart_value INT64;
    DECLARE v_sysdate STRING;
    DECLARE v_effective_stichtag STRING;
    DECLARE v_current_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_log_file_name STRING DEFAULT 'placeholder_log_file.log'; -- Placeholder as per design document schema

    -- Get next job_nr and current timestamp for job_control entry
    SET v_job_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `project.dataset.job_control`);
    SET v_current_timestamp = CURRENT_TIMESTAMP();

    -- Set defaults and derived values based on input parameters
    SET v_restart_value = IFNULL(p_wiederanlaufWert, 0);
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    SET v_effective_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);
    SET v_status = 'RUNNING';

    -- Insert initial job control entry
    INSERT INTO `project.dataset.job_control` (
        job_nr, job_kennung, source_program, stichtag, sysdate, restart_value, created_at, status
    ) VALUES (
        v_job_nr, v_job_kennung, v_source_program, p_stichtag, v_sysdate, v_restart_value, v_current_timestamp, v_status
    );

    -- Log job start to audit log
    INSERT INTO `project.dataset.job_audit_log` (
        log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
    ) VALUES (
        GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
        'Job started. Effective Stichtag: ' || v_effective_stichtag || ', Restart Value: ' || v_restart_value,
        v_current_timestamp
    );

    -- Validate if v_effective_stichtag is set
    IF v_effective_stichtag IS NULL OR v_effective_stichtag = '' THEN
        SET v_status = 'FAILED';
        SET v_error_message = 'ERROR: Effective Stichtag parameter is missing or empty. Please provide a valid date.';

        -- Log error to job_error_log
        INSERT INTO `project.dataset.job_error_log` (
            error_id, job_kennung, err_nr, err_arg, created_at, message
        ) VALUES (
            GENERATE_UUID(), v_job_kennung, 1, 'Stichtag_Validation', CURRENT_TIMESTAMP(), v_error_message
        );

        -- Update job_control status to FAILED
        UPDATE `project.dataset.job_control`
        SET status = v_status, finished_at = CURRENT_TIMESTAMP()
        WHERE job_nr = v_job_nr;

        -- Log validation failure to audit log
        INSERT INTO `project.dataset.job_audit_log` (
            log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
        ) VALUES (
            GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
            'Validation failed: ' || v_error_message, CURRENT_TIMESTAMP()
        );

        -- Signal an error to the caller
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Begin an exception block to handle errors during the core procedure call
    BEGIN
        -- Call the core business logic stored procedure
        CALL `project.dataset.ausd_bp_ta_cntrct_dist_core`(v_effective_stichtag, v_restart_value);

        -- If core procedure succeeds, update job status
        SET v_status = 'SUCCESS';
        UPDATE `project.dataset.job_control`
        SET status = v_status, finished_at = CURRENT_TIMESTAMP()
        WHERE job_nr = v_job_nr;

        -- Log success to audit log
        INSERT INTO `project.dataset.job_audit_log` (
            log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
        ) VALUES (
            GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
            'Core procedure `ausd_bp_ta_cntrct_dist_core` completed successfully.', CURRENT_TIMESTAMP()
        );

    EXCEPTION WHEN ERROR THEN
        -- If an error occurs during the core procedure call
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;

        -- Log the error to job_error_log
        INSERT INTO `project.dataset.job_error_log` (
            error_id, job_kennung, err_nr, err_arg, created_at, message
        ) VALUES (
            GENERATE_UUID(), v_job_kennung, 2, 'Core_Procedure_Call', CURRENT_TIMESTAMP(), v_error_message
        );

        -- Update job_control status to FAILED
        UPDATE `project.dataset.job_control`
        SET status = v_status, finished_at = CURRENT_TIMESTAMP()
        WHERE job_nr = v_job_nr;

        -- Log failure to audit log
        INSERT INTO `project.dataset.job_audit_log` (
            log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
        ) VALUES (
            GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
            'Core procedure `ausd_bp_ta_cntrct_dist_core` failed with error: ' || v_error_message, CURRENT_TIMESTAMP()
        );

        -- Re-raise the error to propagate it to the caller
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;

END;