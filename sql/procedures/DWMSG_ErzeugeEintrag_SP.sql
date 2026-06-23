-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure to create an initial job entry in the log table.
-- This procedure mimics the `DWMSG_ErzeugeEintrag` shell function.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.DWMSG_ErzeugeEintrag_SP`(
    IN p_dw_eintrags_nr INT64,
    IN p_job_kennung STRING,
    IN p_script_type STRING,
    IN p_log_datei STRING
)
BEGIN
    INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (p_dw_eintrags_nr, p_job_kennung, CONCAT('Job started. Type: ', p_script_type, ', Log identifier: ', p_log_datei), CURRENT_TIMESTAMP(), 'INFO');

    INSERT INTO `my_project_id.my_dataset_id.job_status_table`(job_nr, job_kennung, status, last_update_ts)
    VALUES (p_dw_eintrags_nr, p_job_kennung, 'RUNNING', CURRENT_TIMESTAMP());
END;