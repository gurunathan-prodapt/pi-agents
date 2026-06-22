-- BigQuery Stored Procedure for orchestration and parameter handling
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.ausd_bp_ta_bpr_apn`(
    IN p_stichtag_str STRING, -- Expected format: DDMMYYYY
    IN p_wiederanlaufwert_int INT64
)
BEGIN
    DECLARE v_job_entry_number INT64;
    DECLARE v_job_name STRING DEFAULT 'ausd_bp_ta_bpr_apn';
    DECLARE v_script_name STRING DEFAULT 'r_ausd_bp_ta_bpr_apn.ksh';
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufwert INT64;
    DECLARE v_message STRING;
    DECLARE v_status STRING;

    -- Determine a new job entry number
    SET v_job_entry_number = (SELECT IFNULL(MAX(job_entry_number), 0) + 1 FROM `my_project.my_dataset.job_audit`);

    -- Initialize audit log entry
    INSERT INTO `my_project.my_dataset.job_audit` (
        job_entry_number,
        job_name,
        script_name,
        start_timestamp,
        status,
        stichtag,
        wiederanlaufwert,
        sysdate_at_run,
        message
    )
    VALUES (
        v_job_entry_number,
        v_job_name,
        v_script_name,
        CURRENT_TIMESTAMP(),
        'INITIALIZING',
        NULL, -- Will be updated after parsing
        NULL, -- Will be updated after defaulting
        v_sysdate,
        'Job initialization'
    );

    BEGIN
        -- Default p_wiederanlaufwert if not provided
        SET v_wiederanlaufwert = IFNULL(p_wiederanlaufwert_int, 0);

        -- Parse and default p_stichtag_str
        IF p_stichtag_str IS NULL OR TRIM(p_stichtag_str) = '' THEN
            SET v_stichtag = v_sysdate; -- Default to current system date
            SET v_message = 'Stichtag not provided, defaulting to system date.';
        ELSE
            BEGIN
                SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_str);
                SET v_message = 'Stichtag parsed successfully.';
            EXCEPTION WHEN ERROR THEN
                SET v_status = 'FAILED';
                SET v_message = CONCAT('Invalid p_stichtag format: ', p_stichtag_str, '. Expected DDMMYYYY.');
                
                UPDATE `my_project.my_dataset.job_audit`
                SET end_timestamp = CURRENT_TIMESTAMP(),
                    status = v_status,
                    message = v_message,
                    stichtag = NULL,
                    wiederanlaufwert = v_wiederanlaufwert
                WHERE job_entry_number = v_job_entry_number;
                
                RAISE USING MESSAGE = v_message; -- Abort on invalid stichtag
            END;
        END IF;

        -- Update audit log with parsed parameters
        UPDATE `my_project.my_dataset.job_audit`
        SET stichtag = v_stichtag,
            wiederanlaufwert = v_wiederanlaufwert,
            message = CONCAT('Parameters resolved: Stichtag=', CAST(v_stichtag AS STRING), ', Wiederanlaufwert=', CAST(v_wiederanlaufwert AS STRING))
        WHERE job_entry_number = v_job_entry_number;

        -- Call the core processing stored procedure
        CALL `my_project.my_dataset.k_ausd_bp_ta_bpr_apn`(v_job_entry_number, v_stichtag, v_wiederanlaufwert);

        -- If core processing succeeded, update wrapper status to success
        SET v_status = 'SUCCESS';
        SET v_message = 'Orchestration completed successfully, core processing finished.';

        UPDATE `my_project.my_dataset.job_audit`
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_status,
            message = v_message
        WHERE job_entry_number = v_job_entry_number;

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_message = CONCAT('Orchestration failed: ', @@error.message);

        -- Update audit log with failure status from wrapper
        UPDATE `my_project.my_dataset.job_audit`
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_status,
            message = v_message
        WHERE job_entry_number = v_job_entry_number;
        
        RAISE USING MESSAGE = v_message; -- Re-raise the error to the orchestrator
    END;
END;