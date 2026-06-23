-- BigQuery Stored Procedure for k_ausd_bp_ta_apn_carmen.ksh orchestration
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh

CREATE OR REPLACE PROCEDURE `default_project.default_dataset.r_ausd_bp_ta_apn_carmen`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING,
    IN p_stichtag STRING, -- Expected format DDMMYYYY
    IN p_wiederanlauf_wert STRING -- Can be empty, defaults to '0'
)
BEGIN
    -- Declare variables
    DECLARE v_stichtag_date DATE;
    DECLARE v_wiederanlauf_wert_int INT64;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records_processed INT64;
    DECLARE v_tab_name STRING DEFAULT 'PoolBasisprodukt'; -- Corresponding to the original v_TabName

    -- 1. Parameter Validation (replacing pruefeParameterGesetzt)
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        RAISE USING MESSAGE = 'FEHLER: Missing parameter p_JobKennung';
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        RAISE USING MESSAGE = 'FEHLER: Missing parameter p_EintragsNr';
    END IF;

    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
        RAISE USING MESSAGE = 'FEHLER: Missing parameter p_Stichtag';
    END IF;

    -- 2. Date Validation and Conversion (replacing DWDate_Datum_Check)
    IF NOT REGEXP_CONTAINS(p_stichtag, r'^[0-9]{8}$') THEN
        RAISE USING MESSAGE = 'FEHLER: Invalid date format for p_Stichtag. Expected DDMMYYYY.';
    END IF;

    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

    -- 3. Restart Value Initialization
    IF p_wiederanlauf_wert IS NULL OR TRIM(p_wiederanlauf_wert) = '' THEN
        SET v_wiederanlauf_wert_int = 0;
    ELSE
        -- Attempt to cast, raise error if not a valid integer
        BEGIN
            SET v_wiederanlauf_wert_int = CAST(p_wiederanlauf_wert AS INT64);
        EXCEPTION WHEN ERROR THEN
            RAISE USING MESSAGE = 'FEHLER: Invalid restart value. Expected integer, got ' || p_wiederanlauf_wert;
        END;
    END IF;


    -- 4. Date Determination (replacing gestern.ksh)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Log dates (optional, for debugging or verbose logging)
    -- SELECT FORMAT('Today: %t, Yesterday: %t', v_datum_heute, v_datum_gestern) AS debug_dates;

    -- 5. Execute Core Data Logic (calling d_ausd_bp_ta_apn_carmen stored procedure)
    -- The parameters passed to the sub-procedure should be derived from the orchestration script.
    -- Assuming d_ausd_bp_ta_apn_carmen uses the key date and restart value.
    CALL `default_project.default_dataset.d_ausd_bp_ta_apn_carmen`(
        v_stichtag_date,
        v_wiederanlauf_wert_int,
        v_records_processed
    );

    -- Check if the core procedure returned a valid count
    IF v_records_processed IS NULL THEN
        SET v_records_processed = 0; -- Default if the called procedure didn't set it
    END IF;


    -- 6. Job Logging (replacing FOSJobErzeugeEintrag)
    -- Note: The original FOSJobErzeugeEintrag was commented out, but the design suggests migrating it.
    INSERT INTO `default_project.default_dataset.job_log_table` (
        job_identifier,
        entry_number,
        status_code_1,
        status_code_2,
        key_date,
        restart_value,
        records_processed,
        log_timestamp
    )
    VALUES (
        p_job_kennung,
        p_eintrags_nr,
        'A', -- As per design 'A'
        'I', -- As per design 'I'
        v_stichtag_date,
        v_wiederanlauf_wert_int,
        v_records_processed,
        CURRENT_TIMESTAMP()
    );

    -- Optional: Success message
    SELECT FORMAT('Job %s for Entry %s completed successfully. Processed %d records for key date %t.',
                  p_job_kennung, p_eintrags_nr, v_records_processed, v_stichtag_date) AS job_status;

EXCEPTION WHEN ERROR THEN
    -- Generic error handler to catch any unhandled exceptions and log them or re-raise
    RAISE USING MESSAGE = FORMAT('An error occurred during execution: %s', @@error.message);
END;