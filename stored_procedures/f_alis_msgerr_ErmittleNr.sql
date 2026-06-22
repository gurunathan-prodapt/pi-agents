-- BigQuery Stored Procedure for DWMSG_ErmittleNr
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Generates a unique entry number (EintragsNr) for a new job execution.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_ErmittleNr(
    OUT p_EintragsNr INT64
)
BEGIN
    -- Update the sequence table and retrieve the next value
    UPDATE dw_is_error_management.message_sequence
    SET next_val = next_val + 1
    WHERE sequence_name = 'eintragsnr_seq';

    -- Get the updated (new) sequence value
    SELECT next_val
    INTO p_EintragsNr
    FROM dw_is_error_management.message_sequence
    WHERE sequence_name = 'eintragsnr_seq';

    IF p_EintragsNr IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Failed to retrieve EintragsNr from sequence.';
    END IF;
END;