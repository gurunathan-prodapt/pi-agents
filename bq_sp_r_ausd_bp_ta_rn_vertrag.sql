-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_bp_ta_rn_vertrag(
    p_job_id STRING, -- Corresponds to -j option, e.g., 'k_ausd_bp_ta_rn_vertrag'
    p_date_today_str STRING, -- Corresponds to -f option, format YYYYMMDD
    p_date_yesterday_str STRING, -- Corresponds to -s option, format YYYYMMDD
    p_mandant STRING -- Corresponds to -l option
)
OPTIONS(
  description = "BigQuery stored procedure replacing k_ausd_bp_ta_rn_vertrag.ksh. Orchestrates parameter validation, date checks, and execution of core SQL logic."
)
BEGIN
    DECLARE v_return_code INT64 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_processed_records INT64;
    DECLARE v_date_today DATE;
    DECLARE v_date_yesterday DATE;
    DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Helper procedure for logging errors
    -- In a real scenario, this would be part of a common logging utility
    -- For this example, we'll inline a basic logging mechanism.
    -- Assuming job_error_log and job_audit_log tables exist:
    -- CREATE TABLE project.dataset.job_error_log (
    --     job_id STRING,
    --     log_timestamp TIMESTAMP,
    --     error_message STRING
    -- );
    -- CREATE TABLE project.dataset.job_audit_log (
    --     job_id STRING,
    --     log_timestamp TIMESTAMP,
    --     event_type STRING,
    --     details STRING
    -- );

    -- Log start of job
    INSERT INTO project.dataset.job_audit_log (job_id, log_timestamp, event_type, details)
    VALUES (p_job_id, v_current_timestamp, 'START', 'Procedure started with parameters: job_id=' || p_job_id || ', date_today=' || p_date_today_str || ', date_yesterday=' || p_date_yesterday_str || ', mandant=' || p_mandant);

    -- 1. Parameter Validation
    -- Check if mandatory parameters are set
    IF p_date_today_str IS NULL OR p_date_today_str = '' THEN
        SET v_error_message = 'Parameter -f (date_today) is mandatory.';
        INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
        VALUES (p_job_id, v_current_timestamp, v_error_message);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_date_yesterday_str IS NULL OR p_date_yesterday_str = '' THEN
        SET v_error_message = 'Parameter -s (date_yesterday) is mandatory.';
        INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
        VALUES (p_job_id, v_current_timestamp, v_error_message);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_mandant IS NULL OR p_mandant = '' THEN
        SET v_error_message = 'Parameter -l (mandant) is mandatory.';
        INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
        VALUES (p_job_id, v_current_timestamp, v_error_message);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Date format validation (YYYYMMDD)
    BEGIN
        SET v_date_today = SAFE.PARSE_DATE('%Y%m%d', p_date_today_str);
        IF v_date_today IS NULL THEN
            SET v_error_message = 'Invalid date format for -f (date_today). Expected YYYYMMDD.';
            INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
            VALUES (p_job_id, v_current_timestamp, v_error_message);
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
        END IF;

        SET v_date_yesterday = SAFE.PARSE_DATE('%Y%m%d', p_date_yesterday_str);
        IF v_date_yesterday IS NULL THEN
            SET v_error_message = 'Invalid date format for -s (date_yesterday). Expected YYYYMMDD.';
            INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
            VALUES (p_job_id, v_current_timestamp, v_error_message);
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
        END IF;
    EXCEPTION WHEN ERROR THEN
        -- Catch any unexpected parsing errors, though SAFE.PARSE_DATE handles most
        SET v_error_message = 'An unexpected error occurred during date parsing: ' || @@error.message;
        INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
        VALUES (p_job_id, v_current_timestamp, v_error_message);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;

    -- 2. Date Derivation (replacing gestern.ksh logic if needed, or validating inputs)
    -- The ksh script potentially derived dates if not provided. Here, we validate
    -- that p_date_yesterday_str is indeed yesterday's date relative to p_date_today_str.
    IF DATE_SUB(v_date_today, INTERVAL 1 DAY) != v_date_yesterday THEN
        SET v_error_message = 'Logical error: -s (date_yesterday) is not one day before -f (date_today).';
        INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
        VALUES (p_job_id, v_current_timestamp, v_error_message);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- 3. Execute Core SQL Logic
    -- This section replaces the call to starteSQLSkript and d_ausd_bp_ta_rn_vertrag.sql
    BEGIN
        -- Call the dedicated BigQuery SQL script, passing necessary parameters.
        -- The actual content of d_ausd_bp_ta_rn_vertrag.sql needs to be translated into
        -- BigQuery SQL and could be executed here, either as an inline query,
        -- or by calling another stored procedure.
        -- For this example, we assume `bq_d_ausd_bp_ta_rn_vertrag.sql` exists and
        -- returns the count of processed records.

        -- Placeholder for the actual execution of translated d_ausd_bp_ta_rn_vertrag.sql logic.
        -- The `bq_d_ausd_bp_ta_rn_vertrag.sql` content should be embedded or called here.
        -- Example of what the embedded/called SQL might look like:
        /*
        CREATE TEMPORARY TABLE temp_result AS
        SELECT
            -- ... your transformed data ...
        FROM
            your_source_table
        WHERE
            processing_date = v_date_today
            AND mandant_id = p_mandant;

        INSERT INTO project.dataset.your_target_table
        SELECT * FROM temp_result;

        SET v_processed_records = (SELECT COUNT(*) FROM temp_result);
        */

        -- Since the content of d_ausd_bp_ta_rn_vertrag.sql is not available,
        -- we will use a placeholder for its execution.
        -- In a real scenario, you would CALL another procedure or embed the SQL directly.

        -- Example of calling another procedure:
        -- CALL project.dataset.d_ausd_bp_ta_rn_vertrag_proc(
        --     v_date_today,
        --     v_date_yesterday,
        --     p_mandant,
        --     OUT v_processed_records
        -- );

        -- For now, setting a dummy value for processed records.
        SET v_processed_records = -1; -- Indicates that actual count was not performed.

        -- INSERT INTO project.dataset.job_audit_log (job_id, log_timestamp, event_type, details)
        -- VALUES (p_job_id, v_current_timestamp, 'SQL_EXECUTION', 'Core SQL logic executed.');

    EXCEPTION WHEN ERROR THEN
        SET v_return_code = 1;
        SET v_error_message = 'Error during core SQL execution: ' || @@error.message;
        INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
        VALUES (p_job_id, v_current_timestamp, v_error_message);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;

    -- 4. Record Count Capture & Logging
    IF v_return_code = 0 THEN
        INSERT INTO project.dataset.job_audit_log (job_id, log_timestamp, event_type, details)
        VALUES (p_job_id, v_current_timestamp, 'SUCCESS', 'Job completed. Processed records: ' || CAST(v_processed_records AS STRING));
    END IF;

EXCEPTION WHEN ERROR THEN
    -- Global error handler for any uncaught errors in the procedure
    SET v_return_code = 1;
    SET v_error_message = 'An unhandled error occurred in the procedure: ' || @@error.message;
    INSERT INTO project.dataset.job_error_log (job_id, log_timestamp, error_message)
    VALUES (p_job_id, v_current_timestamp, v_error_message);
    -- Re-raise the error
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
END;