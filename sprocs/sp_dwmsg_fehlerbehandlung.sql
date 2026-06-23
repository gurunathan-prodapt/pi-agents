-- Stored procedure for error handling, calls sp_dwmsg_meldefehler and raises an error
-- Replaces DWMSG_FehlerBehandlung function from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_dwmsg_fehlerbehandlung`(
    IN p_job_id STRING,
    IN p_run_id STRING,
    IN p_error_code STRING,
    IN p_error_message STRING,
    IN p_source_component STRING,
    IN p_stack_trace STRING
)
BEGIN
    CALL `project.dataset.sp_dwmsg_meldefehler`(
        p_job_id, p_run_id, p_error_code, p_error_message, p_source_component, p_stack_trace
    );
    RAISE USING MESSAGE 'Job failed: ' || p_error_message;
END;