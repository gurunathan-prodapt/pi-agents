-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure to set the job status to OK (success).
-- This procedure mimics the `DWMSG_SetzeStatusOK` shell function.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.DWMSG_SetzeStatusOK_SP`(
    IN p_dw_eintrags_nr INT64
)
BEGIN
    INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
        p_dw_eintrags_nr,
        (SELECT job_kennung FROM `my_project_id.my_dataset_id.job_status_table` WHERE job_nr = p_dw_eintrags_nr),
        'Job completed successfully.',
        CURRENT_TIMESTAMP(),
        'INFO'
    );
    UPDATE `my_project_id.my_dataset_id.job_status_table`
    SET status = 'SUCCESS', last_update_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = p_dw_eintrags_nr;
END;