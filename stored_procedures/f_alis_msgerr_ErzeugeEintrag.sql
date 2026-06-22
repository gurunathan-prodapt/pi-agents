-- BigQuery Stored Procedure for DWMSG_ErzeugeEintrag
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Creates a new entry in the message_table for a job execution.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_ErzeugeEintrag(
    p_EintragsNr INT64,
    p_JobKennung STRING,
    p_Programmname STRING,
    p_LogDatei STRING
)
BEGIN
    IF p_EintragsNr IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_EintragsNr cannot be NULL.';
    END IF;

    INSERT INTO dw_is_error_management.message_table (
        eintragsnr,
        job_kennung,
        programmname,
        log_datei,
        status,
        created_at,
        updated_at
    )
    VALUES (
        p_EintragsNr,
        p_JobKennung,
        p_Programmname,
        p_LogDatei,
        'LAEUFT', -- Initial status for a new entry
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
END;