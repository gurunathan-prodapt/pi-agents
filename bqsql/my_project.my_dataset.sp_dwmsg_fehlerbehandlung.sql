-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- Replicates DWMSG_Fehlerbehandlung for updating job status to FAILED and logging errors.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_dwmsg_fehlerbehandlung`(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_error_message STRING,
    IN p_script_name STRING,
    IN p_error_code STRING
)
BEGIN
    UPDATE `my_project.my_dataset.job_control`
    SET status = 'FAILED',
        end_timestamp = CURRENT_TIMESTAMP(),
        last_modified = CURRENT_TIMESTAMP(),
        message = p_error_message
    WHERE job_kennung = p_job_kennung AND entry_nr = p_entry_nr;

    INSERT INTO `my_project.my_dataset.error_log` (
        job_kennung, entry_nr, error_timestamp, error_message, error_code, script_name, stack_trace
    )
    VALUES (
        p_job_kennung,
        p_entry_nr,
        CURRENT_TIMESTAMP(),
        p_error_message,
        p_error_code,
        p_script_name,
        ERROR_STACK_TRACE()
    );

    CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(p_job_kennung, p_entry_nr, CONCAT('ERROR: ', p_error_message), 'ERROR', p_script_name);
END;