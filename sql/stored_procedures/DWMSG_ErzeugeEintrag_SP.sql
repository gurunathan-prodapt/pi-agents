-- BigQuery Stored Procedure to create an initial log entry
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_ErzeugeEintrag_SP`(
    IN p_DW_EintragsNr INT64,
    IN p_JobKennung STRING,
    IN p_ScriptName STRING,
    IN p_LogIdentifier STRING
)
BEGIN
    INSERT INTO `your_project.your_dataset.job_log` (
        job_nr, job_kennung, script_name, log_identifier, log_level, log_message, log_ts, status
    )
    VALUES (
        p_DW_EintragsNr,
        p_JobKennung,
        p_ScriptName,
        p_LogIdentifier,
        'INFO',
        FORMAT_BQM_TEXT("Job %s started for script %s.", p_JobKennung, p_ScriptName),
        CURRENT_TIMESTAMP(),
        'RUNNING'
    );
END;