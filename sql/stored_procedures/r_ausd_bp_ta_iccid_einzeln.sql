-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- Description: Main orchestration BigQuery stored procedure for processing ICCID Einzeln.

CREATE OR REPLACE PROCEDURE your_project_id.your_dataset_id.r_ausd_bp_ta_iccid_einzeln(
    IN p_JobKennung STRING,
    IN p_EintragsNr INT64,
    IN p_Stichtag STRING, -- Format 'DDMMYYYY'
    IN p_wiederanlaufWert STRING
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records_processed INT64;
    DECLARE v_error_message STRING;

    -- Initialize values (replaces logic from gestern.ksh)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- 1. Parameter Validation (replaces pruefeParameterGesetzt and manual checks)
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
        RAISE USING MESSAGE = 'Jobkennung parameter must be set.';
    END IF;

    IF p_EintragsNr IS NULL THEN
        RAISE USING MESSAGE = 'EintragsNr parameter must be set.';
    END IF;

    IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
        RAISE USING MESSAGE = 'Stichtag parameter must be set.';
    END IF;

    -- Date Validation (replaces DWDate_Datum_Check)
    IF NOT your_project_id.your_dataset_id.f_is_date_check(p_Stichtag, '%d%m%Y') THEN
        RAISE USING MESSAGE = FORMAT('Invalid Stichtag date format: %s. Expected DDMMYYYY.', p_Stichtag);
    END IF;

    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

    -- Log job start
    CALL your_project_id.your_dataset_id.p_log_job_entry(
        p_JobKennung,
        p_EintragsNr,
        'PoolBasisprodukt', -- Derived from original ksh: v_TabName='PoolBasisprodukt'
        v_stichtag_date,
        NULL,
        'RUNNING',
        'Job started.'
    );

    -- Main processing logic
    BEGIN
        CALL your_project_id.your_dataset_id.p_process_iccid_einzeln(
            v_stichtag_date,
            v_records_processed
        );

        -- Log job success
        CALL your_project_id.your_dataset_id.p_log_job_entry(
            p_JobKennung,
            p_EintragsNr,
            'PoolBasisprodukt',
            v_stichtag_date,
            v_records_processed,
            'SUCCESS',
            FORMAT('Job completed. Processed %d records.', v_records_processed)
        );

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        -- Log job failure (replaces DWMSG_MeldeFehler)
        CALL your_project_id.your_dataset_id.p_log_job_entry(
            p_JobKennung,
            p_EintragsNr,
            'PoolBasisprodukt',
            v_stichtag_date,
            NULL,
            'FAILED',
            FORMAT('Job failed: %s', v_error_message)
        );
        RAISE; -- Re-raise the error for external orchestration if any
    END;
END;