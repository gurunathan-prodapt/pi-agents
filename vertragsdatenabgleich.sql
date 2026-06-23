-- BigQuery Stored Procedure for migrating r_ausd_v_ta_vvl_dwh.ksh wrapper logic
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh
CREATE OR REPLACE PROCEDURE project.dataset.vertragsdatenabgleich(
    IN p_stichtag STRING, -- Corresponds to -s parameter, expected format YYYYMMDD
    IN p_loglevel STRING DEFAULT 'INFO' -- Corresponds to -l parameter, not directly used in this wrapper, but available
)
BEGIN
    DECLARE v_job_kennung STRING;
    DECLARE v_entry_nr INT64;
    DECLARE v_script_name STRING DEFAULT 'r_ausd_v_ta_vvl_dwh';
    DECLARE v_log_name STRING;
    DECLARE v_current_timestamp TIMESTAMP;
    DECLARE v_error_message STRING;
    DECLARE v_stack_trace STRING;
    DECLARE v_error_code INT64;

    -- Generate job metadata
    SET v_current_timestamp = CURRENT_TIMESTAMP();
    SET v_entry_nr = UNIX_MICROS(v_current_timestamp); -- Unique entry number
    SET v_job_kennung = GENERATE_UUID();
    SET v_log_name = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', v_current_timestamp) || '_' || v_job_kennung || '.log';

    -- Parameter Validation
    IF p_stichtag IS NULL OR NOT SAFE_CAST(p_stichtag AS BIGNUMERIC) IS NOT NULL THEN
        SET v_error_message = 'ERROR: Parameter -s (stichtag) is missing or invalid. Usage: CALL vertragsdatenabgleich(''YYYYMMDD'');';
        INSERT INTO project.dataset.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
        VALUES (v_entry_nr, v_job_kennung, v_error_message, 'ERROR', v_current_timestamp);
        INSERT INTO project.dataset.job_error_log (job_kennung, error_nr, error_arg, created_at)
        VALUES (v_job_kennung, 1, 'Invalid Stichtag', v_current_timestamp);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Start Transaction Block for error handling
    BEGIN
        -- Log job start
        INSERT INTO project.dataset.job_control (entry_nr, job_kennung, script_name, log_name, stichtag, status, created_at, finished_at)
        VALUES (v_entry_nr, v_job_kennung, v_script_name, v_log_name, p_stichtag, 'RUNNING', v_current_timestamp, NULL);

        INSERT INTO project.dataset.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
        VALUES (v_entry_nr, v_job_kennung, 'Job started: ' || v_script_name || ' with JobKennung: ' || v_job_kennung, 'INFO', v_current_timestamp);

        -- Call the core processing logic (placeholder for k_ausd_v_ta_vvl_dwh.ksh)
        -- The actual implementation of k_ausd_v_ta_vvl_dwh will be migrated separately.
        CALL project.dataset.k_ausd_v_ta_vvl_dwh(v_job_kennung, v_entry_nr);

        -- Log successful completion
        UPDATE project.dataset.job_control
        SET status = 'OK', finished_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = v_job_kennung;

        INSERT INTO project.dataset.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
        VALUES (v_entry_nr, v_job_kennung, 'Job completed successfully for JobKennung: ' || v_job_kennung, 'INFO', CURRENT_TIMESTAMP());

        INSERT INTO project.dataset.job_audit (entry_nr, job_kennung, status, log_name, created_at)
        VALUES (v_entry_nr, v_job_kennung, 'OK', v_log_name, CURRENT_TIMESTAMP());

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_stack_trace = @@error.stack_trace;
        SET v_error_code = @@error.code;

        -- Log error details
        UPDATE project.dataset.job_control
        SET status = 'ERROR', finished_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = v_job_kennung;

        INSERT INTO project.dataset.job_messages (entry_nr, job_kennung, message_text, message_type, created_at)
        VALUES (v_entry_nr, v_job_kennung, 'Job failed for JobKennung: ' || v_job_kennung || '. Error: ' || v_error_message, 'ERROR', CURRENT_TIMESTAMP());

        INSERT INTO project.dataset.job_error_log (job_kennung, error_nr, error_arg, created_at)
        VALUES (v_job_kennung, v_error_code, v_error_message, CURRENT_TIMESTAMP());

        INSERT INTO project.dataset.job_audit (entry_nr, job_kennung, status, log_name, created_at)
        VALUES (v_entry_nr, v_job_kennung, 'ERROR', v_log_name, CURRENT_TIMESTAMP());

        -- Re-raise the error to the caller
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;
END;