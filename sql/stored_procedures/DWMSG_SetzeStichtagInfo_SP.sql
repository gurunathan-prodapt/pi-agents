-- BigQuery Stored Procedure to record reference date information
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh
-- This procedure updates the reference_date column for a given job_nr.

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_SetzeStichtagInfo_SP`(
    IN p_DW_EintragsNr INT64,
    IN p_DateValue STRING, -- e.g., '20231026'
    IN p_DateFormat STRING -- e.g., '%Y%m%d'
)
BEGIN
    UPDATE `your_project.your_dataset.job_log`
    SET reference_date = PARSE_DATE(p_DateFormat, p_DateValue)
    WHERE job_nr = p_DW_EintragsNr;

    INSERT INTO `your_project.your_dataset.job_log` (
        job_nr, log_level, log_message, log_ts
    )
    VALUES (
        p_DW_EintragsNr,
        'INFO',
        FORMAT_BQM_TEXT("Reference date set to %s.", PARSE_DATE(p_DateFormat, p_DateValue)),
        CURRENT_TIMESTAMP()
    );
END;