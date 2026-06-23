-- BigQuery Stored Procedure: project.dataset.r_ausd_bp_ta_apn_vertrag_proc
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- This procedure orchestrates parameter handling, validation, date calculations,
-- and execution of the core data transformation.

CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_bp_ta_apn_vertrag_proc(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag_Str STRING, -- Input as STRING for validation
    p_wiederanlaufWert STRING
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_apn_vertrag.ksh';
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_reference_date DATE;
    DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
    DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    DECLARE v_processed_record_count INT64;
    DECLARE v_job_status STRING DEFAULT 'SUCCESS';
    DECLARE v_error_message STRING;

    -- 1. Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_error_message = 'Parameter "Job ID (p_JobKennung)" cannot be empty.';
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('MissingParameter' AS reason, 'p_JobKennung' AS parameter_name)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_error_message = 'Parameter "Entry Number (p_EintragsNr)" cannot be empty.';
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('MissingParameter' AS reason, 'p_EintragsNr' AS parameter_name)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END IF;

    IF p_Stichtag_Str IS NULL OR p_Stichtag_Str = '' THEN
        SET v_error_message = 'Parameter "Reference Date (p_Stichtag_Str)" cannot be empty.';
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('MissingParameter' AS reason, 'p_Stichtag_Str' AS parameter_name)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END IF;

    -- 2. Date Validation and Conversion
    BEGIN
        SET v_reference_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag_Str);
        IF v_reference_date IS NULL THEN
            SET v_error_message = FORMAT('Invalid date format for p_Stichtag_Str: "%s". Expected DDMMYYYY.', p_Stichtag_Str);
            INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
            VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('InvalidDateFormat' AS reason, 'p_Stichtag_Str' AS parameter_name, p_Stichtag_Str AS provided_value)));
            SET v_job_status = 'FAILED';
            RAISE EXCEPTION '%', v_error_message;
        END IF;
    EXCEPTION WHEN ERROR THEN
        -- This block catches parsing errors not caught by SAFE.PARSE_DATE returning NULL (e.g., if p_Stichtag_Str is a valid date but not DDMMYYYY)
        -- SAFE.PARSE_DATE handles most malformed dates by returning NULL. This is a safeguard.
        SET v_error_message = FORMAT('Unexpected error during date parsing for p_Stichtag_Str: "%s". Error: %s', p_Stichtag_Str, @@error.message);
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('DateParsingError' AS reason, 'p_Stichtag_Str' AS parameter_name, p_Stichtag_Str AS provided_value, 'SQL_Error' AS sql_error_type, @@error.message AS sql_error_message)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END;

    -- Log successful parameter validation (optional)
    -- INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
    -- VALUES (CURRENT_TIMESTAMP(), v_job_name, 'Parameters validated successfully.', 'INFO', NULL);

    -- 3. Execute Core Data Transformation Procedure
    BEGIN
        CALL project.dataset.d_ausd_bp_ta_apn_vertrag_proc(
            p_EintragsNr,
            p_JobKennung,
            v_reference_date,
            p_wiederanlaufWert,
            v_processed_record_count
        );
    EXCEPTION WHEN ERROR THEN
        SET v_error_message = FORMAT('Error executing d_ausd_bp_ta_apn_vertrag_proc: %s', @@error.message);
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('ProcedureExecutionError' AS reason, 'procedure_name' AS d_ausd_bp_ta_apn_vertrag_proc, 'SQL_Error' AS sql_error_type, @@error.message AS sql_error_message)));
        SET v_job_status = 'FAILED';
        RAISE EXCEPTION '%', v_error_message;
    END;

    -- 4. Job Audit
    INSERT INTO project.dataset.job_audit (
        audit_timestamp,
        job_name,
        job_id,
        entry_number,
        reference_date,
        target_table,
        processed_record_count,
        status,
        start_timestamp,
        end_timestamp
    )
    VALUES (
        CURRENT_TIMESTAMP(),
        v_job_name,
        p_JobKennung,
        p_EintragsNr,
        v_reference_date,
        'PoolBasisprodukt', -- As identified in the design document
        IFNULL(v_processed_record_count, 0), -- Use 0 if the procedure didn't return a count
        v_job_status,
        v_start_timestamp,
        CURRENT_TIMESTAMP()
    );

EXCEPTION WHEN ERROR THEN
    -- Catch any unhandled errors and log them before exiting
    SET v_error_message = IFNULL(v_error_message, FORMAT('An unhandled error occurred in %s: %s', v_job_name, @@error.message));
    IF v_job_status = 'SUCCESS' THEN -- Only log if it's not already logged as FAILED
        INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_name, v_error_message, 'ERROR', TO_JSON(STRUCT('UnhandledError' AS reason, 'SQL_Error' AS sql_error_type, @@error.message AS sql_error_message)));
    END IF;
    -- Ensure audit entry is written even for failure
    INSERT INTO project.dataset.job_audit (
        audit_timestamp,
        job_name,
        job_id,
        entry_number,
        reference_date,
        target_table,
        processed_record_count,
        status,
        start_timestamp,
        end_timestamp
    )
    VALUES (
        CURRENT_TIMESTAMP(),
        v_job_name,
        p_JobKennung,
        p_EintragsNr,
        v_reference_date,
        'PoolBasisprodukt',
        IFNULL(v_processed_record_count, 0),
        'FAILED',
        v_start_timestamp,
        CURRENT_TIMESTAMP()
    );
    RAISE; -- Re-raise the error for external orchestration systems to catch
END;