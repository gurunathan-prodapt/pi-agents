-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure to log an error message with a specific error number and argument.
-- This procedure mimics the `DWMSG_MeldeFehler` shell function.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.DWMSG_MeldeFehler_SP`(
    IN p_dw_eintrags_nr INT64,
    IN p_error_type STRING, -- e.g., 'E' for Error
    IN p_error_nr INT64,
    IN p_error_arg STRING
)
BEGIN
    INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
        p_dw_eintrags_nr,
        (SELECT job_kennung FROM `my_project_id.my_dataset_id.job_status_table` WHERE job_nr = p_dw_eintrags_nr),
        CONCAT('Error (', p_error_type, '): ', CAST(p_error_nr AS STRING), '. Argument: ', p_error_arg),
        CURRENT_TIMESTAMP(),
        'ERROR'
    );
    -- Update status table to FAILED on specific errors
    UPDATE `my_project_id.my_dataset_id.job_status_table`
    SET status = 'FAILED', last_update_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = p_dw_eintrags_nr;
END;