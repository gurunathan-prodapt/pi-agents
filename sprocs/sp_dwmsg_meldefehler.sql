-- Stored procedure to log an error
-- Replaces DWMSG_MeldeFehler function from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_dwmsg_meldefehler`(
    IN p_job_id STRING,
    IN p_run_id STRING,
    IN p_error_code STRING,
    IN p_error_message STRING,
    IN p_source_component STRING,
    IN p_stack_trace STRING
)
BEGIN
    INSERT INTO `project.dataset.dw_error_log` (
        job_id, run_id, error_timestamp, error_code, error_message, source_component, stack_trace
    )
    VALUES (
        p_job_id, p_run_id, CURRENT_TIMESTAMP(), p_error_code, p_error_message, p_source_component, p_stack_trace
    );

    UPDATE `project.dataset.dw_job_entries`
    SET
        end_timestamp = CURRENT_TIMESTAMP(),
        status = 'FAILED',
        message = p_error_message
    WHERE job_id = p_job_id AND run_id = p_run_id AND status = 'RUNNING';

    UPDATE `project.dataset.dw_job_status`
    SET
        status = 'FAILED',
        last_update_timestamp = CURRENT_TIMESTAMP(),
        last_message = p_error_message
    WHERE job_id = p_job_id AND run_id = p_run_id;
END;