-- BigQuery Stored Procedure to replace k_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag STRING, -- Expected format 'DDMMYYYY'
    IN p_wiederanlaufWert INT64 -- Defaulted to 0 if not provided
)
BEGIN
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'FAILED';
    DECLARE v_error_message STRING;
    DECLARE v_run_id STRING DEFAULT GENERATE_UUID();

    -- Initialize p_wiederanlaufWert if NULL
    IF p_wiederanlaufWert IS NULL THEN
        SET p_wiederanlaufWert = 0;
    END IF;

    -- Insert initial job log entry
    INSERT INTO `your_project_id.your_dataset_id.job_log`
    (
        job_name, entry_number, key_date, start_time, status, run_id, message
    )
    VALUES
    (
        'k_ausd_bp_ta_bpr_apn', p_EintragsNr, SAFE.PARSE_DATE('%d%m%Y', p_Stichtag), v_start_time, 'RUNNING', v_run_id, 'Job started'
    );

    BEGIN EXCEPTION WHEN ERROR THEN
        -- Error handling for the entire block
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;

        INSERT INTO `your_project_id.your_dataset_id.error_log`
        (
            job_name, entry_number, error_code, error_message, error_timestamp, run_id
        )
        VALUES
        (
            'k_ausd_bp_ta_bpr_apn', p_EintragsNr, 0, v_error_message, v_end_time, v_run_id
        );

        UPDATE `your_project_id.your_dataset_id.job_log`
        SET
            end_time = v_end_time,
            status = 'FAILED',
            message = 'Job failed: ' || v_error_message
        WHERE run_id = v_run_id;

        RAISE USING MESSAGE = v_error_message;
    END;

    -- Parameter validation (pruefeParameterGesetzt equivalent)
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        RAISE USING MESSAGE = 'Parameter p_JobKennung (j) is required.';
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        RAISE USING MESSAGE = 'Parameter p_EintragsNr (f) is required.';
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
        RAISE USING MESSAGE = 'Parameter p_Stichtag (s) is required.';
    END IF;

    -- Date format check (DWDate_Datum_Check equivalent)
    IF SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL THEN
        RAISE USING MESSAGE = 'Parameter p_Stichtag (s) has an invalid date format. Expected DDMMYYYY.';
    END IF;

    -- Date derivation (gestern.ksh equivalent)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Call the core SQL stored procedure
    CALL `your_project_id.your_dataset_id.sp_d_ausd_bp_ta_bpr_apn`(
        p_EintragsNr,
        p_JobKennung,
        p_Stichtag,
        v_records_processed
    );

    SET v_end_time = CURRENT_TIMESTAMP();
    SET v_status = 'SUCCESS';

    -- Update job log entry
    UPDATE `your_project_id.your_dataset_id.job_log`
    SET
        end_time = v_end_time,
        status = v_status,
        record_count = v_records_processed,
        message = 'Job completed successfully. Records processed: ' || CAST(v_records_processed AS STRING)
    WHERE run_id = v_run_id;

END;