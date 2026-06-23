--
-- BigQuery Stored Procedure for the main wrapper logic
-- Replaces legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
--
CREATE OR REPLACE PROCEDURE project.dataset.bereitstellung_basisprodukte_bert(
    p_stichtag STRING,           -- Input: Cutoff date in 'DDMMYYYY' format
    p_wiederanlaufWert INT64     -- Input: Restart value
)
BEGIN
    DECLARE v_run_id STRING;
    DECLARE v_job_name STRING DEFAULT 'bereitstellung_basisprodukte_bert';
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_log_file_name STRING;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack_trace STRING;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_run_id = GENERATE_UUID();

    -- Initialize job run log
    INSERT INTO project.dataset.job_run_log (run_id, job_name, start_timestamp, status, message)
    VALUES (v_run_id, v_job_name, v_start_timestamp, 'RUNNING', 'Job started');

    INSERT INTO project.dataset.job_status_log (run_id, status_timestamp, status_message)
    VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Job started and run_id generated');

    BEGIN
        -- Parameter defaulting and validation
        SET v_wiederanlaufWert = COALESCE(p_wiederanlaufWert, 0);
        SET v_stichtag = COALESCE(p_stichtag, FORMAT_DATE('%d%m%Y', CURRENT_DATE()));

        -- Log raw parameters
        INSERT INTO project.dataset.job_metadata_log (run_id, meta_key, meta_value)
        VALUES
            (v_run_id, 'p_stichtag_raw', p_stichtag),
            (v_run_id, 'p_wiederanlaufWert_raw', CAST(p_wiederanlaufWert AS STRING));

        INSERT INTO project.dataset.job_status_log (run_id, status_timestamp, status_message, detail)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Parameters processed', CONCAT('Stichtag: ', v_stichtag, ', WiederanlaufWert: ', CAST(v_wiederanlaufWert AS STRING)));

        -- Generate simulated log file name
        SET v_log_file_name = CONCAT(
            'log_',
            REPLACE(v_job_name, '.', '_'),
            '_',
            v_stichtag,
            '_',
            FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()),
            '.log'
        );

        INSERT INTO project.dataset.job_metadata_log (run_id, meta_key, meta_value)
        VALUES
            (v_run_id, 'stichtag_processed', v_stichtag),
            (v_run_id, 'wiederanlaufWert_processed', CAST(v_wiederanlaufWert AS STRING)),
            (v_run_id, 'log_file_name', v_log_file_name);

        -- Validate Stichtag
        IF v_stichtag IS NULL OR v_stichtag = '' THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Stichtag parameter is required and cannot be empty.';
        END IF;

        INSERT INTO project.dataset.job_status_log (run_id, status_timestamp, status_message)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Stichtag validated');

        -- Call the kernel stored procedure
        INSERT INTO project.dataset.job_status_log (run_id, status_timestamp, status_message, detail)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Calling kernel stored procedure', 'project.dataset.k_ausd_bp_ta_iccid_einzeln');

        -- The actual kernel script migration (k_ausd_bp_ta_iccid_einzeln.ksh)
        -- will result in a separate BigQuery Stored Procedure.
        -- This call assumes it takes p_stichtag and p_wiederanlaufWert as parameters.
        CALL project.dataset.k_ausd_bp_ta_iccid_einzeln(v_stichtag, v_wiederanlaufWert);

        INSERT INTO project.dataset.job_status_log (run_id, status_timestamp, status_message)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Kernel stored procedure completed successfully');

        -- Update job run log for success
        UPDATE project.dataset.job_run_log
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = 'SUCCESS',
            message = 'Job completed successfully'
        WHERE run_id = v_run_id;

        INSERT INTO project.dataset.job_status_log (run_id, status_timestamp, status_message)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Job completed successfully');

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_stack_trace = @@error.stack_trace;

        -- Log the error
        INSERT INTO project.dataset.job_error_log (run_id, error_timestamp, error_code, error_message, stack_trace)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), @@error.code, v_error_message, v_error_stack_trace);

        INSERT INTO project.dataset.job_status_log (run_id, status_timestamp, status_message, detail)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'Job failed', v_error_message);

        -- Update job run log for failure
        UPDATE project.dataset.job_run_log
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = 'FAILED',
            message = CONCAT('Job failed: ', v_error_message)
        WHERE run_id = v_run_id;

        -- Re-raise the error to indicate failure to the caller
        RAISE;
    END;
END;