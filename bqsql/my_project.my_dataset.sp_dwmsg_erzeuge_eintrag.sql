-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- Replicates DWMSG_ErzeugeEintrag for creating initial job entries in the job_control table.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_dwmsg_erzeuge_eintrag`(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_message STRING,
    IN p_pid STRING,
    IN p_hostname STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_control` (
        job_kennung, entry_nr, start_timestamp, status, last_modified, pid, hostname, message
    )
    VALUES (
        p_job_kennung,
        p_entry_nr,
        CURRENT_TIMESTAMP(),
        'RUNNING',
        CURRENT_TIMESTAMP(),
        p_pid,
        p_hostname,
        p_message
    );
END;