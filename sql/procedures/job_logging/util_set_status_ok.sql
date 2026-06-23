-- Utility procedure to update a job's status to 'OK' upon successful completion.
-- Corresponds to legacy DWMSG_SetzeStatusOK.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE PROCEDURE project.dataset.util_set_status_ok(
    IN p_job_id STRING
)
BEGIN
    UPDATE project.dataset.job_log
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = 'OK'
    WHERE
        job_id = p_job_id;

    CALL project.dataset.util_log_detail(
        p_job_id,
        'INFO',
        'Job completed successfully.',
        'util_set_status_ok'
    );
END;