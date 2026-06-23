--
-- BigQuery Stored Procedure: vertragsdatenabgleich_wrapper
-- Migrated wrapper script from legacy vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
-- Handles parameter parsing, logging, error handling, and orchestration.
--
-- Parameters:
--   p_s_parameter: Corresponds to '-s' parameter in the original script.
--   p_l_parameter: Corresponds to '-l' parameter in the original script (log file name).
--   p_h_flag: If TRUE, prints usage information and exits.
--
CREATE OR REPLACE PROCEDURE `project.dataset.vertragsdatenabgleich_wrapper`(
    IN p_s_parameter STRING,
    IN p_l_parameter STRING,
    IN p_h_flag BOOL
)
BEGIN
    -- Declare variables
    DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_ACC_REF';
    DECLARE v_job_entry_number STRING;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_return_code INT64;
    DECLARE v_return_message STRING;

    -- Generate a unique job entry number (simulating DW_EintragsNr)
    SET v_job_entry_number = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()) || '_' || GENERATE_UUID();
    SET v_start_timestamp = CURRENT_TIMESTAMP();

    -- Handle help flag
    IF p_h_flag THEN
        SELECT '''Usage: CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_s_parameter => <string>, p_l_parameter => <string>, p_h_flag => FALSE);
        -h: Display this help message.
        -s: System parameter.
        -l: Log file name (for logging purposes).''';
        RETURN;
    END IF;

    -- Parameter validation
    IF p_s_parameter IS NULL OR p_l_parameter IS NULL THEN
        SET v_status = 'FAILED';
        SET v_message = 'Missing required parameters. Both -s and -l are mandatory.';
        INSERT INTO `project.dataset.job_audit_log` (job_name, job_entry_number, start_timestamp, end_timestamp, status, message, parameters)
        VALUES (v_job_kennung, v_job_entry_number, v_start_timestamp, CURRENT_TIMESTAMP(), v_status, v_message, TO_JSON(STRUCT(p_s_parameter AS s_param, p_l_parameter AS l_param)));

        INSERT INTO `project.dataset.job_error_log` (job_name, job_entry_number, error_timestamp, error_message, error_code)
        VALUES (v_job_kennung, v_job_entry_number, CURRENT_TIMESTAMP(), v_message, 'PARAM_MISSING');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Initial log entry: Job Started
    INSERT INTO `project.dataset.job_audit_log` (job_name, job_entry_number, start_timestamp, status, message, parameters)
    VALUES (v_job_kennung, v_job_entry_number, v_start_timestamp, 'STARTED', 'Job execution started.', TO_JSON(STRUCT(p_s_parameter AS s_param, p_l_parameter AS l_param)));

    BEGIN
        -- Call the core logic procedure
        CALL `project.dataset.k_ausd_v_ta_acc_ref`(
            p_job_kennung => v_job_kennung,
            p_dw_eintrags_nr => v_job_entry_number,
            p_return_code => v_return_code,
            p_return_message => v_return_message
        );

        -- Check return code from core logic
        IF v_return_code = 0 THEN
            SET v_status = 'COMPLETED';
            SET v_message = 'Core logic executed successfully: ' || v_return_message;
        ELSE
            SET v_status = 'FAILED';
            SET v_message = 'Core logic failed: ' || v_return_message;
            INSERT INTO `project.dataset.job_error_log` (job_name, job_entry_number, error_timestamp, error_message, error_code)
            VALUES (v_job_kennung, v_job_entry_number, CURRENT_TIMESTAMP(), v_message, 'CORE_LOGIC_FAILED');
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message; -- Re-raise error to wrapper's EXCEPTION block
        END IF;

    EXCEPTION WHEN OTHERS THEN
        SET v_status = 'FAILED';
        SET v_message = 'An unexpected error occurred in the wrapper: ' || @@error.message;
        INSERT INTO `project.dataset.job_error_log` (job_name, job_entry_number, error_timestamp, error_message, error_code, stack_trace)
        VALUES (v_job_kennung, v_job_entry_number, CURRENT_TIMESTAMP(), v_message, 'UNEXPECTED_ERROR', @@error.stack_trace);
    END;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Final log entry: Job Completed or Failed
    UPDATE `project.dataset.job_audit_log`
    SET
        end_timestamp = v_end_timestamp,
        status = v_status,
        message = v_message
    WHERE job_name = v_job_kennung AND job_entry_number = v_job_entry_number AND start_timestamp = v_start_timestamp;

END;