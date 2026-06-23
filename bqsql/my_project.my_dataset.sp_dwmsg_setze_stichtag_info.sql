-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- Replicates DWMSG_SetzeStichtagInfo for recording reference dates in the job_control table.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_dwmsg_setze_stichtag_info`(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_stichtag_date DATE
)
BEGIN
    UPDATE `my_project.my_dataset.job_control`
    SET message = CONCAT(COALESCE(message, ''), ' Stichtag: ', FORMAT_DATE('%Y-%m-%d', p_stichtag_date)),
        last_modified = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_job_kennung AND entry_nr = p_entry_nr;
END;