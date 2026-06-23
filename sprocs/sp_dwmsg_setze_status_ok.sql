-- Stored procedure to set job status to OK
-- Replaces DWMSG_setze_Status_OK function from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_dwmsg_setze_status_ok`(
    IN p_job_id STRING,
    IN p_run_id STRING,
    IN p_message STRING
)
BEGIN
    UPDATE `project.dataset.dw_job_entries`
    SET
        end_timestamp = CURRENT_TIMESTAMP(),
        status = 'SUCCESS',
        message = p_message
    WHERE job_id = p_job_id AND run_id = p_run_id AND status = 'RUNNING';

    UPDATE `project.dataset.dw_job_status`
    SET
        status = 'SUCCESS',
        last_update_timestamp = CURRENT_TIMESTAMP(),
        last_message = p_message
    WHERE job_id = p_job_id AND run_id = p_run_id;
END;