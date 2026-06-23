-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- Replicates DWMSG_Logdateiname logic by inserting log messages into the job_log table.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_dwmsg_logdateiname`(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_message STRING,
    IN p_log_level STRING,
    IN p_script_name STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (
        job_kennung, entry_nr, log_timestamp, log_message, log_level, process_id, script_name
    )
    VALUES (
        p_job_kennung,
        p_entry_nr,
        CURRENT_TIMESTAMP(),
        p_message,
        p_log_level,
        GENERATE_UUID(), -- Placeholder for process ID
        p_script_name
    );
END;