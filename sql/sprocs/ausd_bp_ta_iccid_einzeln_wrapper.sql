-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_iccid_einzeln_wrapper`(
    IN p_stichtag_raw STRING,        -- Input Stichtag in DDMMYYYY format or NULL
    IN p_wiederanlaufWert_raw STRING -- Input Wiederanlaufwert as string or NULL
)
OPTIONS(
    description="Wrapper stored procedure for r_ausd_bp_ta_iccid_einzeln.ksh. Handles parameter parsing, logging, and calls the core logic."
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'ausd_bp_ta_iccid_einzeln_wrapper';
    DECLARE v_run_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;

    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufWert INT64;

    -- Generate a unique run ID for this execution
    SET v_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_message = 'Starting wrapper stored procedure.';

    -- Log start of the wrapper stored procedure
    INSERT INTO `project.dataset.job_audit_log` (job_name, run_id, start_time, status, message, parameters_json)
    VALUES (v_job_name, v_run_id, v_start_time, v_status, v_message,
            TO_JSON(STRUCT(p_stichtag_raw AS stichtag_raw, p_wiederanlaufWert_raw AS wiederanlaufwert_raw)));

    BEGIN
        -- Parameter Parsing and Validation
        IF p_stichtag_raw IS NULL OR TRIM(p_stichtag_raw) = '' THEN
            SET v_stichtag = CURRENT_DATE();
            SET v_message = 'Stichtag not provided, defaulting to current date.';
            CALL `project.dataset.log_job_event`(v_job_name, v_run_id, 'INFO', v_message);
        ELSE
            BEGIN
                SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_raw);
            EXCEPTION WHEN ERROR THEN
                SET v_message = FORMAT("Invalid Stichtag format: %s. Expected DDMMYYYY.", p_stichtag_raw);
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
            END;
        END IF;

        IF p_wiederanlaufWert_raw IS NOT NULL AND TRIM(p_wiederanlaufWert_raw) != '' THEN
            BEGIN
                SET v_wiederanlaufWert = CAST(p_wiederanlaufWert_raw AS INT64);
            EXCEPTION WHEN ERROR THEN
                SET v_message = FORMAT("Invalid Wiederanlaufwert format: %s. Expected integer.", p_wiederanlaufWert_raw);
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
            END;
        END IF;

        SET v_message = FORMAT("Parameters validated. Stichtag: %t, Wiederanlaufwert: %d", v_stichtag, v_wiederanlaufWert);
        CALL `project.dataset.log_job_event`(v_job_name, v_run_id, 'INFO', v_message);

        -- Call the core logic stored procedure
        CALL `project.dataset.k_ausd_bp_ta_iccid_einzeln`(
            v_stichtag,
            v_wiederanlaufWert,
            v_run_id -- Pass the wrapper's run_id to the kernel
        );

        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'SUCCEEDED';
        SET v_message = 'Wrapper stored procedure completed successfully.';

        -- Update the audit log with success status
        UPDATE `project.dataset.job_audit_log`
        SET
            end_time = v_end_time,
            status = v_status,
            message = v_message
        WHERE
            run_id = v_run_id AND job_name = v_job_name AND status = 'RUNNING';

    EXCEPTION WHEN ERROR THEN
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'FAILED';
        SET v_message = FORMAT("Wrapper stored procedure failed with error: %s", @@error.message);

        -- Update the audit log with failure status
        UPDATE `project.dataset.job_audit_log`
        SET
            end_time = v_end_time,
            status = v_status,
            message = v_message
        WHERE
            run_id = v_run_id AND job_name = v_job_name AND status = 'RUNNING';

        RAISE; -- Re-raise the error to the caller (e.g., Cloud Composer)
    END;
END;

-- Helper procedure to log events within other stored procedures
CREATE OR REPLACE PROCEDURE `project.dataset.log_job_event`(
    p_job_name STRING,
    p_run_id STRING,
    p_event_type STRING,
    p_event_message STRING
)
BEGIN
    INSERT INTO `project.dataset.job_audit_log` (job_name, run_id, start_time, status, message)
    VALUES (p_job_name, p_run_id, CURRENT_TIMESTAMP(), p_event_type, p_event_message);
END;