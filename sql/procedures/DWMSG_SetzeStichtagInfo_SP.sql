-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure to set and log stichtag (reference date) information.
-- This procedure mimics the `DWMSG_SetzeStichtagInfo` shell function.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.DWMSG_SetzeStichtagInfo_SP`(
    IN p_dw_eintrags_nr INT64,
    IN p_stichtag STRING, -- Date string, e.g., 'DDMMYYYY'
    IN p_format STRING
)
BEGIN
    INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
        p_dw_eintrags_nr,
        (SELECT job_kennung FROM `my_project_id.my_dataset_id.job_status_table` WHERE job_nr = p_dw_eintrags_nr),
        CONCAT('Reference Date (Stichtag) set: ', p_stichtag, ' (Format: ', p_format, ')'),
        CURRENT_TIMESTAMP(),
        'INFO'
    );
END;