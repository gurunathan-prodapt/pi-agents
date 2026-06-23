-- BigQuery Stored Procedure for r_ausd_bp_ta_bpr_opt_text.ksh wrapper logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_opt_text_wrapper`(
    IN p_input_stichtag STRING,     -- Optional: Stichtag in 'DDMMYYYY' format
    IN p_input_wiederanlaufwert INT64 -- Optional: Restart value
)
BEGIN
    DECLARE v_sysdate DATE;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufwert INT64 DEFAULT 0;
    DECLARE v_job_id STRING;
    DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_bpr_opt_text';
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_error_code INT64 DEFAULT 0;
    DECLARE v_error_message STRING DEFAULT '';
    DECLARE v_log_file_path STRING; -- In BQ, this might be a reference to job_id or a log bucket path

    -- Determine system date (v_sysdate)
    SET v_sysdate = CURRENT_DATE();

    -- Initialize parameters
    SET v_wiederanlaufwert = IFNULL(p_input_wiederanlaufwert, 0);

    IF p_input_stichtag IS NOT NULL THEN
        BEGIN
            -- Attempt to parse input stichtag
            SET v_stichtag = PARSE_DATE('%d%m%Y', p_input_stichtag);
        EXCEPTION WHEN ERROR THEN
            SET v_error_code = 193; -- Corresponds to 'Notwendiges Argument fehlt' or invalid format
            SET v_error_message = 'Invalid Stichtag format. Expected DDMMYYYY.';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
        END;
    ELSE
        -- If stichtag not provided, default to v_sysdate (simplified, original had more complex logic involving maxladedatum)
        SET v_stichtag = v_sysdate;
    END IF;

    -- Start job control entry and generate job_id
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_job_id = GENERATE_UUID(); -- Using UUID for unique job identifier

    -- Placeholder for LogDatei generation logic from DWMSG_Logdateiname
    SET v_log_file_path = CONCAT('gs://your-log-bucket/', v_job_kennung, '_', REPLACE(CAST(v_start_time AS STRING), ' ', '_'), '_', v_job_id, '.log');

    INSERT INTO `your_gcp_project.your_bigquery_dataset.job_control` (
        job_id, job_name, job_status, start_time,
        parameter_stichtag, parameter_wiederanlaufwert, log_file_path, sys_date
    )
    VALUES (
        v_job_id, v_job_kennung, 'RUNNING', v_start_time,
        v_stichtag, v_wiederanlaufwert, v_log_file_path, v_sysdate
    );

    INSERT INTO `your_gcp_project.your_bigquery_dataset.job_message_log` (log_timestamp, job_id, message_type, message, script_name)
    VALUES (CURRENT_TIMESTAMP(), v_job_id, 'INFO', CONCAT('Job started. Stichtag: ', FORMAT_DATE('%Y-%m-%d', v_stichtag), ', Wiederanlaufwert: ', CAST(v_wiederanlaufwert AS STRING)), 'ausd_bp_ta_bpr_opt_text_wrapper');

    -- Error handling block for the core logic
    BEGIN
        -- Call the downstream stored procedure with processed parameters
        CALL `your_gcp_project.your_bigquery_dataset.k_ausd_bp_ta_bpr_opt_text`(v_job_id, v_job_kennung, v_stichtag, v_wiederanlaufwert);

        -- If the kernel script completes successfully, update job_control and log success
        UPDATE `your_gcp_project.your_bigquery_dataset.job_control`
        SET
            job_status = 'OK',
            end_time = CURRENT_TIMESTAMP(),
            message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
        WHERE job_id = v_job_id;

        INSERT INTO `your_gcp_project.your_bigquery_dataset.job_message_log` (log_timestamp, job_id, message_type, message, script_name)
        VALUES (CURRENT_TIMESTAMP(), v_job_id, 'INFO', 'Job completed successfully.', 'ausd_bp_ta_bpr_opt_text_wrapper');

    EXCEPTION WHEN ERROR THEN
        -- Capture error information
        SET v_error_code = 1; -- Generic error code, could be more specific
        SET v_error_message = @@error.message;

        -- Update job_control with error status
        UPDATE `your_gcp_project.your_bigquery_dataset.job_control`
        SET
            job_status = 'ERROR',
            end_time = CURRENT_TIMESTAMP(),
            error_code = v_error_code,
            error_message = v_error_message,
            message = 'Job ended with error'
        WHERE job_id = v_job_id;

        -- Log the error
        INSERT INTO `your_gcp_project.your_bigquery_dataset.job_error_log` (log_timestamp, job_id, error_code, error_argument, error_message, script_name)
        VALUES (CURRENT_TIMESTAMP(), v_job_id, v_error_code, NULL, v_error_message, 'ausd_bp_ta_bpr_opt_text_wrapper');

        INSERT INTO `your_gcp_project.your_bigquery_dataset.job_message_log` (log_timestamp, job_id, message_type, message, script_name)
        VALUES (CURRENT_TIMESTAMP(), v_job_id, 'ERROR', CONCAT('Job failed: ', v_error_message), 'ausd_bp_ta_bpr_opt_text_wrapper');

        -- Re-raise the error to propagate it
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;

END;