-- Legacy source: k_ausd_bp_ta_iccid_einzeln.ksh
-- BigQuery migration for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- This stored procedure orchestrates the ICCID data processing,
-- including parameter validation, date handling, and execution of core logic.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset.r_ausd_bp_ta_iccid_einzeln`(
    p_jobkennung STRING,
    p_eintragsnr STRING,
    p_stichtag STRING, -- Expected format 'DDMMYYYY', can be NULL
    p_wiederanlaufwert STRING
)
BEGIN
    -- Declare variables
    DECLARE v_stichtag_formatted STRING; -- YYYYMMDD format for internal use
    DECLARE v_message STRING;
    DECLARE v_error_code STRING DEFAULT 'N/A';
    DECLARE v_records_processed INT64;
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_status STRING DEFAULT 'FAILED';
    DECLARE v_run_id STRING DEFAULT GENERATE_UUID(); -- Unique ID for this run

    -- Helper function for logging errors
    CALL `your_gcp_project_id.your_bigquery_dataset.log_error`(
        v_run_id,
        'r_ausd_bp_ta_iccid_einzeln',
        'Starting job',
        NULL,
        NULL,
        TO_JSON(STRUCT(p_jobkennung, p_eintragsnr, p_stichtag, p_wiederanlaufwert))
    );

    BEGIN
        -- 1. Parameter Validation (re-implementation of pruefeParameterGesetzt)
        IF p_jobkennung IS NULL OR TRIM(p_jobkennung) = '' THEN
            SET v_message = 'Parameter p_jobkennung must be set.';
            SET v_error_code = 'PARAM_JOBKENNUNG_MISSING';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
        END IF;

        IF p_eintragsnr IS NULL OR TRIM(p_eintragsnr) = '' THEN
            SET v_message = 'Parameter p_eintragsnr must be set.';
            SET v_error_code = 'PARAM_EINTRAGSNR_MISSING';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
        END IF;

        -- 2. Date Derivation and Validation (re-implementation of h_alis_date.ksh, gestern.ksh)
        IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
            -- If p_stichtag is not provided, use yesterday's date (gestern.ksh equivalent)
            SET v_stichtag_formatted = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));
            SET v_message = 'p_stichtag not provided, using yesterday: ' || v_stichtag_formatted;
            CALL `your_gcp_project_id.your_bigquery_dataset.log_info`(
                v_run_id,
                'r_ausd_bp_ta_iccid_einzeln',
                v_message,
                NULL,
                NULL
            );
        ELSE
            -- Validate p_stichtag format (DDMMYYYY)
            IF NOT SAFE.PARSE_DATE('%d%m%Y', p_stichtag) IS NULL THEN
                SET v_stichtag_formatted = FORMAT_DATE('%Y%m%d', PARSE_DATE('%d%m%Y', p_stichtag));
            ELSE
                SET v_message = 'Invalid p_stichtag format. Expected DDMMYYYY. Got: ' || p_stichtag;
                SET v_error_code = 'INVALID_DATE_FORMAT';
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
            END IF;
        END IF;

        -- For the Oracle script, v_datum could also be derived from DWTK_MELDUNGEN.
        -- Assuming for this migration, the explicit parameter or yesterday's date takes precedence.
        -- If a default `v_datum` from DWTK_MELDUNGEN is truly needed when p_stichtag is NULL,
        -- add logic here:
        /*
        IF v_stichtag_formatted IS NULL THEN
            SELECT NVL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
            INTO v_stichtag_formatted
            FROM `your_gcp_project_id.your_bigquery_dataset.DWTK_MELDUNGEN_BQ` AS m
            WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        END IF;
        */

        -- 3. Execute Core SQL Logic (d_ausd_bp_ta_iccid_einzeln_sp)
        CALL `your_gcp_project_id.your_bigquery_dataset.d_ausd_bp_ta_iccid_einzeln_sp`(v_stichtag_formatted);

        -- 4. Get record count
        SELECT COUNT(*)
        INTO v_records_processed
        FROM `your_gcp_project_id.your_bigquery_dataset.SOF_TA_ICCID_EINZELN_BQ`;

        SET v_status = 'SUCCESS';
        SET v_message = 'Job completed successfully.';

        CALL `your_gcp_project_id.your_bigquery_dataset.log_info`(
            v_run_id,
            'r_ausd_bp_ta_iccid_einzeln',
            v_message,
            NULL,
            TO_JSON(STRUCT(v_records_processed AS records_inserted))
        );

    EXCEPTION WHEN ERROR THEN
        SET v_message = @@ERROR_MESSAGE;
        CALL `your_gcp_project_id.your_bigquery_dataset.log_error`(
            v_run_id,
            'r_ausd_bp_ta_iccid_einzeln',
            v_message,
            v_error_code,
            NULL, -- No specific input_parameters to pass from here, they are in the log_info call
            TO_JSON(STRUCT(p_jobkennung, p_eintragsnr, p_stichtag, p_wiederanlaufwert))
        );
    END;

    -- 5. Log job tracking information
    INSERT INTO `your_gcp_project_id.your_bigquery_dataset.job_tracking_bq` (
        job_id, run_id, start_timestamp, end_timestamp, status, processed_records, input_parameters, output_details
    )
    VALUES (
        p_jobkennung,
        v_run_id,
        v_start_timestamp,
        CURRENT_TIMESTAMP(),
        v_status,
        IF(v_status = 'SUCCESS', v_records_processed, NULL),
        TO_JSON(STRUCT(p_jobkennung, p_eintragsnr, p_stichtag, p_wiederanlaufwert)),
        IF(v_status = 'SUCCESS', TO_JSON(STRUCT(v_records_processed AS records_inserted)), NULL)
    );

END;

-- Placeholder for error logging UDF (f_alis_msgerr.ksh equivalent)
-- In a real scenario, this would be a more robust error logging mechanism
-- potentially writing to a dedicated error table or Cloud Logging.
CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset.log_error`(
    run_id STRING,
    component STRING,
    error_message STRING,
    error_code STRING,
    severity STRING,
    parameters JSON
)
BEGIN
    INSERT INTO `your_gcp_project_id.your_bigquery_dataset.error_log_bq` (
        job_id, run_id, log_timestamp, error_message, error_code, severity, component, parameters
    )
    VALUES (
        NULL, -- Job ID might be derived from component or passed explicitly
        run_id,
        CURRENT_TIMESTAMP(),
        error_message,
        error_code,
        COALESCE(severity, 'ERROR'),
        component,
        parameters
    );
END;

-- Placeholder for info logging UDF
CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset.log_info`(
    run_id STRING,
    component STRING,
    message STRING,
    severity STRING,
    details JSON
)
BEGIN
    INSERT INTO `your_gcp_project_id.your_bigquery_dataset.error_log_bq` (
        job_id, run_id, log_timestamp, error_message, error_code, severity, component, parameters
    )
    VALUES (
        NULL, -- Job ID might be derived from component or passed explicitly
        run_id,
        CURRENT_TIMESTAMP(),
        message,
        'INFO', -- Using error_code for message type in this simple example
        COALESCE(severity, 'INFO'),
        component,
        details
    );
END;