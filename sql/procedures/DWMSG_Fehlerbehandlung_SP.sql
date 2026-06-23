-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure for generic error handling.
-- This procedure mimics the error handling logic (e.g., trap) from the shell script.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.DWMSG_Fehlerbehandlung_SP`(
    IN p_dw_eintrags_nr INT64
)
BEGIN
    INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
        p_dw_eintrags_nr,
        (SELECT job_kennung FROM `my_project_id.my_dataset_id.job_status_table` WHERE job_nr = p_dw_eintrags_nr),
        CONCAT('Job failed due to an unhandled exception. Error message: ', @@error.message),
        CURRENT_TIMESTAMP(),
        'ERROR'
    );
    UPDATE `my_project_id.my_dataset_id.job_status_table`
    SET status = 'FAILED', last_update_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = p_dw_eintrags_nr;
END;