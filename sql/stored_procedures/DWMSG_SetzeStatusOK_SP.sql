-- BigQuery Stored Procedure to mark job as successful
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_SetzeStatusOK_SP`(
    IN p_DW_EintragsNr INT64
)
BEGIN
    UPDATE `your_project.your_dataset.job_log`
    SET status = 'SUCCESS'
    WHERE job_nr = p_DW_EintragsNr;

    INSERT INTO `your_project.your_dataset.job_log` (
        job_nr, log_level, log_message, log_ts
    )
    VALUES (
        p_DW_EintragsNr,
        'INFO',
        'Job completed successfully.',
        CURRENT_TIMESTAMP()
    );
END;