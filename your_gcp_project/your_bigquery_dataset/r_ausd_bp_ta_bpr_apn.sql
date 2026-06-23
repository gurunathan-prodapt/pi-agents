-- BigQuery Stored Procedure for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- This procedure orchestrates the data processing logic, handles parameters,
-- performs validations, executes core SQL, and logs job status.
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bigquery_dataset.r_ausd_bp_ta_bpr_apn`(
    IN p_jobkennung STRING,
    IN p_eintragsnr STRING,
    IN p_stichtag STRING, -- Expected format DDMMYYYY
    IN p_wiederanlaufwert STRING DEFAULT NULL
)
BEGIN
    -- Declare variables
    DECLARE v_tab_name STRING DEFAULT 'PoolBasisprodukt';
    DECLARE v_records INT64;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_stichtag_date DATE;
    DECLARE v_error_message STRING;
    DECLARE v_sql_statement STRING;
    DECLARE v_job_status STRING DEFAULT 'SUCCESS';

    -- Set today's and yesterday's dates
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

    -- Error handling block
    BEGIN
        -- 1. Parameter Validation
        IF p_jobkennung IS NULL OR p_eintragsnr IS NULL OR p_stichtag IS NULL THEN
            SET v_error_message = 'ERROR: Required parameters (Jobkennung, EintragsNr, Stichtag) are missing.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- Validate p_stichtag format (DDMMYYYY)
        IF NOT REGEXP_CONTAINS(p_stichtag, r'^\d{2}\d{2}\d{4}$') THEN
            SET v_error_message = 'ERROR: Stichtag format invalid. Expected DDMMYYYY.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- Validate p_stichtag as a valid date
        SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_stichtag);
        IF v_stichtag_date IS NULL THEN
            SET v_error_message = 'ERROR: Stichtag is not a valid date.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- 2. Construct and Execute Core SQL Logic
        -- The original d_ausd_bp_ta_bpr_apn.sql content, converted to BigQuery Standard SQL,
        -- should be placed here. Parameters need to be properly substituted.
        SET v_sql_statement = FORMAT(
            """
            -- Placeholder for migrated d_ausd_bp_ta_bpr_apn.sql content
            -- Replace this with the actual BigQuery SQL logic.
            -- Example: INSERT INTO `your_gcp_project.your_bigquery_dataset.PoolBasisprodukt` (...) SELECT ...;
            -- Or UPDATE `your_gcp_project.your_bigquery_dataset.PoolBasisprodukt` SET ... WHERE ...;

            -- Example of how to use parameters in the SQL:
            -- SELECT
            --   '%s' AS job_kennung,
            --   '%s' AS eintragsnr,
            --   DATE '%s' AS stichtag_date,
            --   DATE '%s' AS datum_heute,
            --   DATE '%s' AS datum_gestern,
            --   '%s' AS wiederanlaufwert
            -- FROM `your_gcp_project.your_bigquery_dataset.some_source_table`;

            -- IMPORTANT: Ensure the migrated SQL handles these parameters safely and correctly.
            -- This example merely performs a dummy update for demonstration purposes.
            UPDATE `your_gcp_project.your_bigquery_dataset.PoolBasisprodukt`
            SET
                beschreibung = 'Processed on ' || CAST(CURRENT_TIMESTAMP() AS STRING),
                last_updated = CURRENT_TIMESTAMP()
            WHERE
                basis_datum = DATE '%s'; -- Using v_stichtag_date
            """,
            p_jobkennung,
            p_eintragsnr,
            v_stichtag_date,
            v_datum_heute,
            v_datum_gestern,
            IFNULL(p_wiederanlaufwert, 'NULL'),
            v_stichtag_date
        );

        EXECUTE IMMEDIATE v_sql_statement;

        -- 3. Record Counting
        -- Count records affected or processed. For this example, we count all records
        -- in PoolBasisprodukt for the given stichtag. Adjust WHERE clause as needed.
        SELECT COUNT(*)
        INTO v_records
        FROM `your_gcp_project.your_bigquery_dataset.PoolBasisprodukt`
        WHERE basis_datum = v_stichtag_date;

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = CONCAT('Stored Procedure failed: ', @@error.message);

        -- Log error
        INSERT INTO `your_gcp_project.your_bigquery_dataset.error_log` (
            job_name,
            error_nr,
            error_arg,
            error_message,
            created_at
        ) VALUES (
            'k_ausd_bp_ta_bpr_apn', -- Original job name
            -- Custom error number if applicable, else 0 or a generic number
            CASE
                WHEN p_stichtag IS NULL THEN 192 -- As per design document, example error numbers
                WHEN NOT REGEXP_CONTAINS(p_stichtag, r'^\d{2}\d{2}\d{4}$') OR SAFE.PARSE_DATE('%d%m%Y', p_stichtag) IS NULL THEN 193
                ELSE 999 -- Generic error
            END,
            p_stichtag, -- Or other relevant parameter
            v_error_message,
            CURRENT_TIMESTAMP()
        );
        RAISE USING MESSAGE v_error_message; -- Re-raise the error to propagate failure
    END;

    -- 4. Job Logging (if successful)
    INSERT INTO `your_gcp_project.your_bigquery_dataset.job_log` (
        tab_name,
        status,
        mode,
        stichtag_from,
        stichtag_to,
        job_type,
        restart_flag,
        record_count,
        description,
        job_kennung,
        eintragsnr,
        created_at
    ) VALUES (
        v_tab_name,
        v_job_status,
        'standard', -- Default mode, adjust if dynamic
        v_stichtag_date,
        v_stichtag_date, -- If processing a single stichtag, from and to are the same
        'BP_TA_BPR_APN', -- Specific job type
        IFNULL(p_wiederanlaufwert, 'N'),
        v_records,
        'Successfully processed Basisprodukt data',
        p_jobkennung,
        p_eintragsnr,
        CURRENT_TIMESTAMP()
    );

END;