-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure creates a new message/log entry in the bert_meldung table.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_erzeuge_eintrag`(
    IN p_eintrags_nr STRING,
    IN p_job_kennung STRING,
    IN p_programm_name STRING,
    IN p_log_datei STRING,
    IN p_typ STRING
)
BEGIN
    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_eintrags_nr cannot be empty or NULL.';
    END IF;

    INSERT INTO `my_project.my_dataset.bert_meldung` (
        eintrags_nr,
        job_kennung,
        programm_name,
        log_datei,
        typ,
        status,
        creation_timestamp,
        last_update_timestamp
    )
    VALUES (
        p_eintrags_nr,
        p_job_kennung,
        p_programm_name,
        p_log_datei,
        p_typ,
        'IN_PROGRESS', -- Initial status when creating an entry
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
END;