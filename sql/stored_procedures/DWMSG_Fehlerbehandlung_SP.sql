-- BigQuery Stored Procedure to handle errors, log details, and set job status to FAILED
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_Fehlerbehandlung_SP`(
    IN p_DW_EintragsNr INT64,
    IN p_ErrorMessage STRING
)
BEGIN
    UPDATE `your_project.your_dataset.job_log`
    SET status = 'FAILED'
    WHERE job_nr = p_DW_EintragsNr;

    INSERT INTO `your_project.your_dataset.job_log` (
        job_nr, log_level, log_message, log_ts
    )
    VALUES (
        p_DW_EintragsNr,
        'ERROR',
        FORMAT_BQM_TEXT("Job failed. Error: %s", p_ErrorMessage),
        CURRENT_TIMESTAMP()
    );
    -- Optionally, SIGNAL SQLSTATE '45000' to propagate the error immediately
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT_BQM_TEXT('Job %d failed: %s', p_DW_EintragsNr, p_ErrorMessage);
END;