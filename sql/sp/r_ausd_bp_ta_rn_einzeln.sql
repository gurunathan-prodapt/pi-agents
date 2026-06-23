-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: BigQuery Stored Procedure replacing the KornShell orchestration script.

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_rn_einzeln`(
    IN p_stichtag STRING, -- Optional: 'DDMMYYYY' format
    IN p_wiederanlaufwert INT64 -- Optional: restart value, defaults to 0
)
BEGIN
    -- Declare variables
    DECLARE v_job_id STRING;
    DECLARE v_sysdate_ddmmyyyy STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    DECLARE v_effective_stichtag STRING;
    DECLARE v_effective_wiederanlaufwert INT64;
    DECLARE v_job_parameters JSON;

    -- Initialize job_id for this run
    SET v_job_id = GENERATE_UUID();

    -- Determine effective parameters with defaulting
    SET v_effective_stichtag = IF(NULLIF(p_stichtag, '') IS NULL, v_sysdate_ddmmyyyy, p_stichtag);
    SET v_effective_wiederanlaufwert = IFNULL(p_wiederanlaufwert, 0);

    -- Construct JSON of parameters for logging
    SET v_job_parameters = TO_JSON(STRUCT(
        p_stichtag AS p_stichtag_input,
        p_wiederanlaufwert AS p_wiederanlaufwert_input,
        v_effective_stichtag AS effective_stichtag,
        v_effective_wiederanlaufwert AS effective_wiederanlaufwert
    ));

    -- Record job initiation in job_control table
    INSERT INTO `project.dataset.job_control` (
        job_id, job_name, start_time, status, p_stichtag, p_wiederanlaufwert,
        effective_stichtag, effective_wiederanlaufwert, parameters_json
    )
    VALUES (
        v_job_id, 'ausd_bp_ta_rn_einzeln', CURRENT_TIMESTAMP(), 'RUNNING', p_stichtag, p_wiederanlaufwert,
        v_effective_stichtag, v_effective_wiederanlaufwert, v_job_parameters
    );

    -- Log start
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'Main orchestration procedure started.', 'orchestration');
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Effective Stichtag: %s, Wiederanlaufwert: %d', v_effective_stichtag, v_effective_wiederanlaufwert), 'orchestration');

    -- Error handling block for the core logic
    BEGIN
        -- Call the kernel stored procedure
        CALL `project.dataset.k_ausd_bp_ta_rn_einzeln`(v_job_id, v_effective_stichtag, v_effective_wiederanlaufwert);

        -- If successful, update job_control status
        UPDATE `project.dataset.job_control`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'SUCCESS'
        WHERE job_id = v_job_id;

        -- Log success
        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
        VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'Main orchestration procedure finished successfully.', 'orchestration');

    EXCEPTION WHEN ERROR THEN
        -- Capture error details
        DECLARE v_error_message STRING DEFAULT ERROR_MESSAGE();
        DECLARE v_stack_trace STRING DEFAULT (SELECT CONCAT(stack_trace, '\n', @@script.stack_trace) FROM UNNEST(SPLIT(STACK_TRACE(), '\n')) AS stack_trace WHERE STARTS_WITH(stack_trace, '  ')); -- Simplified stack trace capture

        -- Update job_control with failure status and error details
        UPDATE `project.dataset.job_control`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'FAILED',
            error_message = v_error_message,
            stack_trace = v_stack_trace
        WHERE job_id = v_job_id;

        -- Log error to job_log
        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
        VALUES (v_job_id, CURRENT_TIMESTAMP(), 'ERROR', FORMAT('Main orchestration procedure failed: %s', v_error_message), 'orchestration');

        -- Log detailed error to job_error_log
        INSERT INTO `project.dataset.job_error_log` (
            error_id, job_id, error_timestamp, error_code, error_message, stack_trace, component
        )
        VALUES (
            CAST(GENERATE_UUID() AS STRING), v_job_id, CURRENT_TIMESTAMP(), ERROR_CODE(), v_error_message,
            v_stack_trace, 'r_ausd_bp_ta_rn_einzeln'
        );

        -- Re-raise the error for external orchestration systems if needed
        RAISE;
    END;
END;