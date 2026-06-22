-- BigQuery Stored Procedure for orchestration logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag_raw STRING, -- Stichtag as DDMMYYYY string
    IN p_wiederanlaufWert STRING
)
BEGIN
    DECLARE v_Stichtag DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records INT64;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_error_message = NULL;

    -- Parameter validation and error handling
    BEGIN
        -- Validate Jobkennung
        IF p_JobKennung IS NULL OR LENGTH(TRIM(p_JobKennung)) = 0 THEN
            SET v_status = 'FAILED';
            SET v_error_message = 'ERROR: Jobkennung parameter is missing or empty.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- Validate EintragsNr
        IF p_EintragsNr IS NULL OR LENGTH(TRIM(p_EintragsNr)) = 0 THEN
            SET v_status = 'FAILED';
            SET v_error_message = 'ERROR: EintragsNr parameter is missing or empty.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- Validate and parse Stichtag (DDMMYYYY)
        IF p_Stichtag_raw IS NULL OR LENGTH(TRIM(p_Stichtag_raw)) = 0 THEN
            SET v_status = 'FAILED';
            SET v_error_message = 'ERROR: Stichtag parameter is missing or empty.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        SET v_Stichtag = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag_raw);
        IF v_Stichtag IS NULL THEN
            SET v_status = 'FAILED';
            SET v_error_message = FORMAT('ERROR: Invalid Stichtag format: %s. Expected DDMMYYYY.', p_Stichtag_raw);
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- Derive today's and yesterday's dates
        SET v_datum_heute = CURRENT_DATE();
        SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        SELECT FORMAT("INFO: Parameters - Jobkennung: %s, EintragsNr: %s, Stichtag: %s (parsed: %t), wiederanlaufWert: %s",
                       p_JobKennung, p_EintragsNr, p_Stichtag_raw, v_Stichtag, p_wiederanlaufWert);
        SELECT FORMAT("INFO: Derived Dates - Heute: %t, Gestern: %t", v_datum_heute, v_datum_gestern);

        -- Call the core SQL processing stored procedure
        CALL `my_gcp_project.my_bq_dataset.sp_d_ausd_bp_ta_cntrct_dist`(
            p_JobKennung,
            p_EintragsNr,
            v_Stichtag,
            p_wiederanlaufWert
        );

        -- Get record count from the target table
        SELECT COUNT(*)
        INTO v_records
        FROM `my_gcp_project.my_bq_dataset.target_result_table`
        WHERE stichtag = v_Stichtag; -- Assuming target_result_table has a 'stichtag' column for filtering

        SELECT FORMAT("INFO: Processed records count: %d", v_records);

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;
        SELECT FORMAT("ERROR: Procedure failed with message: %s", v_error_message);
    END;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Log job execution details
    INSERT INTO `my_gcp_project.my_bq_dataset.job_tracking_table` (
        job_id,
        entry_number,
        key_date,
        record_count,
        status,
        start_timestamp,
        end_timestamp,
        error_message
    )
    VALUES (
        p_JobKennung,
        p_EintragsNr,
        v_Stichtag,
        v_records,
        v_status,
        v_start_timestamp,
        v_end_timestamp,
        v_error_message
    );

END;