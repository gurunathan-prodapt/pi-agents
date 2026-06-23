-- Migrated from: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

-- This BigQuery Stored Procedure orchestrates the APN contract processing,
-- handling parameters, validations, date derivations, and calling the
-- core data transformation procedure.

CREATE OR REPLACE PROCEDURE `dataset.k_ausd_bp_ta_apn_vertrag_sp`(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING, -- Expected format: DDMMYYYY
    p_wiederanlaufWert INT64,
    p_dataset_name STRING, -- The BigQuery dataset where all target tables and procedures reside.
    p_isbert_schema_dataset STRING -- The BigQuery dataset for source system lookups like dwtk_meldungen.
)
BEGIN
    DECLARE v_ErrNr INT64 DEFAULT 0;
    DECLARE v_ErrArg STRING DEFAULT '';
    DECLARE v_records INT64;
    DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt'; -- from original ksh script
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_effective_wiederanlaufWert INT64;

    -- Initialize wiederanlaufWert
    SET v_effective_wiederanlaufWert = COALESCE(p_wiederanlaufWert, 0);

    -- Parameter Validation: Pruefe, ob notwendige Parameter gesetzt worden sind
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_ErrNr = 193; -- Notwendiges Argument fehlt
        SET v_ErrArg = 'Jobkennung';
        RAISE USING MESSAGE FORMAT 'FEHLER: %d E %d %s - Parameter %s ist nicht gesetzt.', 0, v_ErrNr, v_ErrArg, v_ErrArg;
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
        SET v_ErrNr = 193; -- Notwendiges Argument fehlt
        SET v_ErrArg = 'Stichtag';
        RAISE USING MESSAGE FORMAT 'FEHLER: %d E %d %s - Parameter %s ist nicht gesetzt.', 0, v_ErrNr, v_ErrArg, v_ErrArg;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_ErrNr = 193; -- Notwendiges Argument fehlt
        SET v_ErrArg = 'EintragsNr';
        RAISE USING MESSAGE FORMAT 'FEHLER: %d E %d %s - Parameter %s ist nicht gesetzt.', 0, v_ErrNr, v_ErrArg, v_ErrArg;
    END IF;

    -- Date Validation: Pruefe ob Datum das richtige Format hat (DDMMYYYY)
    IF SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL THEN
        SET v_ErrNr = 194; -- Ungueltiges Datumsformat
        SET v_ErrArg = 'Stichtag';
        RAISE USING MESSAGE FORMAT 'FEHLER: %d E %d %s - Ungueltiges Datumsformat fuer Stichtag: %s', 0, v_ErrNr, v_ErrArg, p_Stichtag;
    END IF;

    -- Date Derivation: gestern.ksh equivalent
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Log job start
    INSERT INTO `dataset.job_audit` (job_name, start_time, status, job_kennung, eintrags_nr, stichtag)
    VALUES ('k_ausd_bp_ta_apn_vertrag', CURRENT_TIMESTAMP(), 'RUNNING', p_JobKennung, p_EintragsNr, SAFE.PARSE_DATE('%d%m%Y', p_Stichtag));

    -- DB-Script ausfuehren
    BEGIN
        CALL `dataset.d_ausd_bp_ta_apn_vertrag_proc`(p_dataset_name, p_isbert_schema_dataset);

        -- After the core logic, retrieve the record count directly from the target table.
        -- Assuming 'sof$ta_apn_vertrag' is the target table for record counting based on d_ausd_bp_ta_apn_vertrag.sql.
        EXECUTE IMMEDIATE FORMAT("""
            SELECT COUNT(*) FROM `%s.sof$ta_apn_vertrag`
        """, p_dataset_name) INTO v_records;

        -- Update job audit with success and record count
        UPDATE `dataset.job_audit`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'SUCCESS',
            record_count = v_records
        WHERE
            job_name = 'k_ausd_bp_ta_apn_vertrag' AND status = 'RUNNING';

    EXCEPTION WHEN ERROR THEN
        -- Log detailed error
        INSERT INTO `dataset.error_log` (job_name, error_message, error_stack, error_timestamp)
        VALUES ('k_ausd_bp_ta_apn_vertrag', @@error.message, @@error.stack_trace, CURRENT_TIMESTAMP());

        -- Update job audit with failure
        UPDATE `dataset.job_audit`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'FAILED',
            error_message = @@error.message
        WHERE
            job_name = 'k_ausd_bp_ta_apn_vertrag' AND status = 'RUNNING';

        RAISE; -- Re-raise the exception to stop execution
    END;

    SELECT '---------- ENDE Datenverarbeitung ----------' AS status_message, v_records AS records_processed;

END;