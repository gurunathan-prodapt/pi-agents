--
-- Legacy Source:
--   vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh
--   vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
--
-- BigQuery Stored Procedure for orchestration logic.
-- Combines parameter handling, date logic, and calls the core processing procedure.

CREATE OR REPLACE PROCEDURE `project.sof.sp_r_k_ausd_bp_ta_apn_vertrag`(
    p_jobkennung STRING,
    p_eintragsnr INT64,
    p_stichtag DATE,
    p_wiederanlaufwert INT64
)
BEGIN
    DECLARE v_sysdate_str STRING;
    DECLARE v_stichtag_actual DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_error_message STRING;

    -- Parameter validation (from k_ausd_bp_ta_apn_vertrag.ksh)
    IF p_jobkennung IS NULL OR TRIM(p_jobkennung) = '' THEN
        SET v_error_message = 'FEHLER: Jobkennung parameter is missing.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_stichtag IS NULL THEN
        SET v_error_message = 'FEHLER: Stichtag parameter is missing.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_eintragsnr IS NULL THEN
        SET v_error_message = 'FEHLER: EintragsNr parameter is missing.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Default p_wiederanlaufwert if not set (from r_ausd_bp_ta_apn_vertrag.ksh)
    IF p_wiederanlaufwert IS NULL THEN
        SET p_wiederanlaufwert = 0;
    END IF;

    -- Get current date (equivalent to DWDate_Gib_Zeitraum and sysdate in ksh)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Stichtag determination (if not set in ksh, default to v_sysdate - now v_datum_heute)
    -- In BigQuery, p_stichtag is already a DATE. If the input p_stichtag was NULL, it would have errored above.
    -- The original logic defaults p_stichtag to sysdate if not provided.
    SET v_stichtag_actual = p_stichtag;

    -- Log job start (placeholder for a logging table)
    -- INSERT INTO `project.isbert_schema.job_log` (job_kennung, eintragsnr, start_time, stichtag, wiederanlaufwert)
    -- VALUES (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), v_stichtag_actual, p_wiederanlaufwert);

    -- Call the core data processing procedure
    CALL `project.sof.sp_d_ausd_bp_ta_apn_vertrag`();

    -- Log job success (placeholder for a logging table)
    -- UPDATE `project.isbert_schema.job_log`
    -- SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS'
    -- WHERE job_kennung = p_jobkennung AND eintragsnr = p_eintragsnr;

    -- The commented-out sed/sort/join processing in k_ausd_bp_ta_apn_vertrag.ksh is omitted per design.

EXCEPTION WHEN ERROR THEN
    SET v_error_message = CONCAT('Job failed for DW.BERT_AUSD_BP_TA_APN_VERTRAG. Error: ', @@error.message);
    -- Log job failure (placeholder for a logging table)
    -- INSERT INTO `project.isbert_schema.error_log` (job_kennung, eintragsnr, error_time, error_message)
    -- VALUES (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), v_error_message);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
END;