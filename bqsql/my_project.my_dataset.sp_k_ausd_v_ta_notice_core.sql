-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- This procedure acts as a control script, handling job parameters and orchestrating the core SQL logic.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_k_ausd_v_ta_notice_core`(
    IN p_JobKennung STRING,
    IN p_EintragsNr INT64
)
BEGIN
    DECLARE v_records_processed INT64;

    -- Parameter validation, equivalent to pruefeParameterGesetzt
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        CALL `my_project.my_dataset.sp_dwmsg_fehlerbehandlung`('UNKNOWN_JOB', COALESCE(p_EintragsNr, -1), 'JobKennung parameter is missing.', 'sp_k_ausd_v_ta_notice_core', 'PARAMETER_ERROR');
        RAISE;
    END IF;

    IF p_EintragsNr IS NULL THEN
        CALL `my_project.my_dataset.sp_dwmsg_fehlerbehandlung`(p_JobKennung, -1, 'EintragsNr parameter is missing.', 'sp_k_ausd_v_ta_notice_core', 'PARAMETER_ERROR');
        RAISE;
    END IF;

    CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(p_JobKennung, p_EintragsNr, CONCAT('Starting core processing for Job: ', p_JobKennung, ', Entry: ', p_EintragsNr), 'INFO', 'sp_k_ausd_v_ta_notice_core');

    BEGIN
        -- Execute the core SQL logic encapsulated in sp_d_ausd_v_ta_notice_sql
        CALL `my_project.my_dataset.sp_d_ausd_v_ta_notice_sql`(p_JobKennung, p_EintragsNr, v_records_processed);

        -- Update job control with success status and record count
        CALL `my_project.my_dataset.sp_dwmsg_setze_status_ok`(p_JobKennung, p_EintragsNr, v_records_processed);

    EXCEPTION WHEN ERROR THEN
        -- The error handling within sp_d_ausd_v_ta_notice_sql already calls sp_dwmsg_fehlerbehandlung.
        -- We re-raise here to propagate the error up to the wrapper procedure.
        RAISE;
    END;

END;