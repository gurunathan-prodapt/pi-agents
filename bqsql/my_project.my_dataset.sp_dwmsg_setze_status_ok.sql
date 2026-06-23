-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- Replicates DWMSG_SetzeStatusOK for updating job status to SUCCESS.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_dwmsg_setze_status_ok`(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_record_count INT64
)
BEGIN
    UPDATE `my_project.my_dataset.job_control`
    SET status = 'SUCCESS',
        end_timestamp = CURRENT_TIMESTAMP(),
        last_modified = CURRENT_TIMESTAMP(),
        record_count = p_record_count,
        message = CONCAT('Job completed successfully. Records processed: ', p_record_count)
    WHERE job_kennung = p_job_kennung AND entry_nr = p_entry_nr;

    CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(p_job_kennung, p_entry_nr, 'Job completed successfully.', 'INFO', 'sp_dwmsg_setze_status_ok');
END;