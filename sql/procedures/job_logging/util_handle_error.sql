-- Utility procedure to log error details and update job status to 'ERROR'.
-- Corresponds to legacy DWMSG_Fehlerbehandlung / DWMSG_MeldeFehler.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE PROCEDURE project.dataset.util_handle_error(
    IN p_job_id STRING,
    IN p_error_message STRING,
    IN p_source_procedure STRING,
    IN p_error_stack_trace STRING
)
BEGIN
    INSERT INTO project.dataset.job_error_log (
        error_id,
        job_id,
        timestamp,
        error_message,
        error_stack_trace,
        severity,
        source_procedure
    )
    VALUES (
        (SELECT IFNULL(MAX(error_id), 0) + 1 FROM project.dataset.job_error_log),
        p_job_id,
        CURRENT_TIMESTAMP(),
        p_error_message,
        p_error_stack_trace,
        'HIGH',
        p_source_procedure
    );

    UPDATE project.dataset.job_log
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = 'ERROR'
    WHERE
        job_id = p_job_id;

    CALL project.dataset.util_log_detail(
        p_job_id,
        'ERROR',
        FORMAT('Job failed: %s', p_error_message),
        p_source_procedure
    );
END;