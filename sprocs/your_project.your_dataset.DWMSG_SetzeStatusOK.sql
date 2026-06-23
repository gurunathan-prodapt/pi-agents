-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery Stored Procedure to update the job status to OK.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_SetzeStatusOK`(
    IN p_job_key STRING
)
BEGIN
    CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
        p_job_key,
        'Job finished successfully.',
        'INFO',
        'SUCCESS'
    );
    -- Update the status of the initial 'RUNNING' entry to 'SUCCESS'
    UPDATE `your_project.your_dataset.job_logging_table`
    SET status = 'SUCCESS'
    WHERE job_key = p_job_key AND status = 'RUNNING';
END;