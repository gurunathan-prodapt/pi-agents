--
-- BigQuery Stored Procedure for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- This is the wrapper script logic.
--
-- Parameters:
--   p_stichtag_string: Optional. Snapshot date in 'DDMMYYYY' format. Defaults to current date.
--   p_wiederanlaufWert: Optional. Restart value. Not used by the core SQL logic in its current form, but passed for compatibility.
--
-- Call example:
-- CALL `bert_reporting`.`r_ausd_austausch_sp`('28022023', 0);
-- CALL `bert_reporting`.`r_ausd_austausch_sp`(NULL, NULL);
--

CREATE OR REPLACE PROCEDURE `bert_reporting`.`r_ausd_austausch_sp`(
    p_stichtag_string STRING,
    p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_stichtag DATE;
    DECLARE v_job_name STRING DEFAULT 'r_ausd_austausch.ksh';
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_error_details STRING;
    DECLARE v_parsed_wiederanlaufWert INT64;

    -- Initialize job start time
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Default p_wiederanlaufWert if NULL
    SET v_parsed_wiederanlaufWert = COALESCE(p_wiederanlaufWert, 0);

    -- Parameter parsing and validation for Stichtag
    IF p_stichtag_string IS NULL OR TRIM(p_stichtag_string) = '' THEN
        SET v_stichtag = CURRENT_DATE();
        SET v_message = 'Stichtag not provided, defaulting to current date.';
        INSERT INTO `bert_reporting`.`job_audit_log` VALUES (v_job_name, v_start_time, NULL, 'INFO', v_message, v_stichtag, v_parsed_wiederanlaufWert, NULL);
    ELSE
        BEGIN
            SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_string);
            SET v_message = 'Stichtag parsed successfully.';
            INSERT INTO `bert_reporting`.`job_audit_log` VALUES (v_job_name, v_start_time, NULL, 'INFO', v_message, v_stichtag, v_parsed_wiederanlaufWert, NULL);
        EXCEPTION WHEN ERROR THEN
            SET v_status = 'FAILED';
            SET v_message = 'Stichtag is missing or invalid. Format should be DDMMYYYY.';
            SET v_error_details = @@error.message;
            SET v_end_time = CURRENT_TIMESTAMP();
            INSERT INTO `bert_reporting`.`job_audit_log` VALUES (v_job_name, v_start_time, v_end_time, v_status, v_message, NULL, v_parsed_wiederanlaufWert, v_error_details);
            RAISE USING MESSAGE = v_message;
        END;
    END IF;

    BEGIN
        -- Log job start
        SET v_message = 'Job started.';
        INSERT INTO `bert_reporting`.`job_audit_log` VALUES (v_job_name, v_start_time, NULL, 'RUNNING', v_message, v_stichtag, v_parsed_wiederanlaufWert, NULL);

        -- Invoke the core logic stored procedure
        CALL `bert_reporting`.`k_ausd_austausch_sp`(v_stichtag);

        -- Log successful completion
        SET v_status = 'SUCCESS';
        SET v_message = 'Job completed successfully.';
        SET v_end_time = CURRENT_TIMESTAMP();
        INSERT INTO `bert_reporting`.`job_audit_log` VALUES (v_job_name, v_start_time, v_end_time, v_status, v_message, v_stichtag, v_parsed_wiederanlaufWert, NULL);

    EXCEPTION WHEN ERROR THEN
        -- Log error details
        SET v_status = 'FAILED';
        SET v_message = 'Job failed during execution of core logic.';
        SET v_error_details = @@error.message;
        SET v_end_time = CURRENT_TIMESTAMP();
        INSERT INTO `bert_reporting`.`job_audit_log` VALUES (v_job_name, v_start_time, v_end_time, v_status, v_message, v_stichtag, v_parsed_wiederanlaufWert, v_error_details);
        RAISE; -- Re-raise the error to propagate it
    END;
END;