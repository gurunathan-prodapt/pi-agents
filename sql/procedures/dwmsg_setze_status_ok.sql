-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure sets the status of a bert_meldung entry to 'OK'.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_setze_status_ok`(
    IN p_eintrags_nr STRING
)
BEGIN
    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_eintrags_nr cannot be empty or NULL.';
    END IF;

    UPDATE `my_project.my_dataset.bert_meldung`
    SET
        status = 'OK',
        last_update_timestamp = CURRENT_TIMESTAMP()
    WHERE
        eintrags_nr = p_eintrags_nr;
END;