-- BigQuery Stored Procedure for the orchestration logic
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE OR REPLACE PROCEDURE prod_dw_isrpt.k_ausd_bp_ta_bpr_apn_sp(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING,           -- Input format 'DDMMYYYY'
    p_wiederanlaufWert INT64     -- Optional, defaults to 0 if NULL
)
BEGIN
    -- Declare variables
    DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
    DECLARE v_stichtag_date DATE;
    DECLARE v_stichtag_YYYYMMDD STRING;
    DECLARE v_datum_heute_YYYYMMDD STRING;
    DECLARE v_datum_gestern_YYYYMMDD STRING;
    DECLARE v_records_processed INT64;
    DECLARE v_wiederanlaufWert_final INT64;

    -- --- Error Handling Block ---
    BEGIN
        -- Initialize v_wiederanlaufWert_final
        SET v_wiederanlaufWert_final = IFNULL(p_wiederanlaufWert, 0);

        -- Parameter Validation (pruefeParameterGesetzt equivalent)
        IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            INSERT INTO prod_dw_logs.error_log (log_timestamp, job_id, source_script, error_number, error_argument, error_message)
            VALUES (CURRENT_TIMESTAMP(), p_JobKennung, 'k_ausd_bp_ta_bpr_apn_sp', 193, 'Jobkennung', 'Required parameter Jobkennung is missing.');
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Required parameter Jobkennung is missing.';
        END IF;

        IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
            INSERT INTO prod_dw_logs.error_log (log_timestamp, job_id, source_script, error_number, error_argument, error_message)
            VALUES (CURRENT_TIMESTAMP(), p_JobKennung, 'k_ausd_bp_ta_bpr_apn_sp', 193, 'Stichtag', 'Required parameter Stichtag is missing.');
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Required parameter Stichtag is missing.';
        END IF;

        IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
            INSERT INTO prod_dw_logs.error_log (log_timestamp, job_id, source_script, error_number, error_argument, error_message)
            VALUES (CURRENT_TIMESTAMP(), p_JobKennung, 'k_ausd_bp_ta_bpr_apn_sp', 193, 'EintragsNr', 'Required parameter EintragsNr is missing.');
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Required parameter EintragsNr is missing.';
        END IF;

        -- Date Format Validation (DWDate_Datum_Check equivalent)
        -- Expects DDMMYYYY, convert to YYYY-MM-DD for BigQuery DATE type
        IF NOT REGEXP_CONTAINS(p_Stichtag, r'^\d{8}$') THEN
             INSERT INTO prod_dw_logs.error_log (log_timestamp, job_id, source_script, error_number, error_argument, error_message)
            VALUES (CURRENT_TIMESTAMP(), p_JobKennung, 'k_ausd_bp_ta_bpr_apn_sp', 194, p_Stichtag, 'Invalid Stichtag format. Expected DDMMYYYY.');
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Invalid Stichtag format. Expected DDMMYYYY.';
        END IF;

        BEGIN
            SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
            SET v_stichtag_YYYYMMDD = FORMAT_DATE('%Y%m%d', v_stichtag_date);
        EXCEPTION WHEN ERROR THEN
            INSERT INTO prod_dw_logs.error_log (log_timestamp, job_id, source_script, error_number, error_argument, error_message)
            VALUES (CURRENT_TIMESTAMP(), p_JobKennung, 'k_ausd_bp_ta_bpr_apn_sp', 195, p_Stichtag, 'Failed to parse Stichtag date.');
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Failed to parse Stichtag date.';
        END;

        -- Date Derivation (gestern.ksh equivalent)
        SET v_datum_heute_YYYYMMDD = FORMAT_DATE('%Y%m%d', CURRENT_DATE());
        SET v_datum_gestern_YYYYMMDD = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

        -- Call the core SQL processing stored procedure
        CALL prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp(
            p_EintragsNr,
            p_JobKennung,
            v_stichtag_YYYYMMDD,
            v_wiederanlaufWert_final,
            v_datum_heute_YYYYMMDD,
            v_datum_gestern_YYYYMMDD
        );

        -- Get number of processed records (v_records equivalent)
        SELECT COUNT(*) INTO v_records_processed
        FROM prod_dw_isrpt.PoolBasisprodukt;

        -- Optional: Job Tracking (FOSJobErzeugeEintrag equivalent, if needed)
        -- This functionality was commented out in the original script.
        -- Uncomment and customize if job tracking is required.
        INSERT INTO prod_dw_logs.job_tracking
        (
            track_timestamp,
            job_id,
            entry_number,
            table_name,
            status,
            start_date,
            end_date,
            record_count,
            notes
        )
        VALUES
        (
            CURRENT_TIMESTAMP(),
            p_JobKennung,
            p_EintragsNr,
            v_TabName,
            'COMPLETED',
            v_stichtag_date,
            v_stichtag_date,
            v_records_processed,
            'Initialbefuellung'
        );

        SELECT '---------- ENDE Datenverarbeitung ----------' AS message;

    EXCEPTION WHEN ERROR THEN
        -- Catch any unhandled errors within the procedure
        INSERT INTO prod_dw_logs.error_log (log_timestamp, job_id, source_script, error_number, error_argument, error_message)
        VALUES (CURRENT_TIMESTAMP(), p_JobKennung, 'k_ausd_bp_ta_bpr_apn_sp', -1, NULL, ERROR_MESSAGE());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Unhandled Error in k_ausd_bp_ta_bpr_apn_sp: ', ERROR_MESSAGE());
    END;
END;