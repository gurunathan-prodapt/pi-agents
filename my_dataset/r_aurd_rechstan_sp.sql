-- BigQuery Stored Procedure for r_aurd_rechstan
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh
-- and incorporates logic from d_aurd_rechstan.sql

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.r_aurd_rechstan`(
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    p_stichtag STRING, -- Expected format: 'DDMMYYYY'
    p_wiederanlauf_wert INT64 DEFAULT 0
)
BEGIN
    DECLARE v_job_run_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_reference_date DATE;
    DECLARE v_processed_records INT64 DEFAULT 0;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_target_table_name STRING DEFAULT 'RKopfStan';
    DECLARE v_heute_date DATE;
    DECLARE v_gestern_date DATE;

    -- Initialize job run ID and start time
    SET v_job_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Default for p_wiederanlauf_wert if not provided
    IF p_wiederanlauf_wert IS NULL THEN
        SET p_wiederanlauf_wert = 0;
    END IF;

    -- Log job start
    INSERT INTO `my_project.my_dataset.job_table` (job_id, entry_number, start_time, reference_date, status, target_table, restart_value)
    VALUES (v_job_run_id, p_eintrags_nr, v_start_time, PARSE_DATE('%d%m%Y', p_stichtag), 'RUNNING', v_target_table_name, p_wiederanlauf_wert);

    BEGIN -- Start of main logic block with error handling
        -- Parameter validation
        IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
            RAISE USING MESSAGE 'FEHLER: JobKennung (p_job_kennung) is missing.';
        END IF;
        IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
            RAISE USING MESSAGE 'FEHLER: EintragsNr (p_eintrags_nr) is missing.';
        END IF;
        IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
            RAISE USING MESSAGE 'FEHLER: Stichtag (p_stichtag) is missing.';
        END IF;

        -- Date validation and derivation
        SET v_reference_date = SAFE.PARSE_DATE('%d%m%Y', p_stichtag);
        IF v_reference_date IS NULL THEN
            RAISE USING MESSAGE FORMAT('FEHLER: Invalid date format for Stichtag (p_stichtag): %s. Expected DDMMYYYY.', p_stichtag);
        END IF;

        SET v_heute_date = CURRENT_DATE();
        SET v_gestern_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- Log derived dates for debugging/auditing if needed
        -- SELECT FORMAT('Today: %t, Yesterday: %t', v_heute_date, v_gestern_date);

        -- Core data processing logic from d_aurd_rechstan.sql
        -- NOTE: The original d_aurd_rechstan.sql content was not provided.
        -- This is a placeholder MERGE statement assuming data is loaded from
        -- a source table (e.g., `my_project.my_dataset.source_rechstan_data`)
        -- into `my_project.my_dataset.RKopfStan`.
        -- A common pattern for ETL is to MERGE new/updated data.

        MERGE INTO `my_project.my_dataset.RKopfStan` AS T
        USING (
            SELECT
                source_id AS rkopf_id,
                v_reference_date AS stichtag_date, -- Use the validated reference date
                source_attr_1 AS attribute_1,
                source_attr_2 AS attribute_2
            FROM `my_project.my_dataset.source_rechstan_data` -- Placeholder source table
            WHERE source_date = v_reference_date -- Example filter
            -- Add any specific filtering or transformation logic based on p_wiederanlauf_wert if needed
        ) AS S
        ON T.rkopf_id = S.rkopf_id AND T.stichtag_date = S.stichtag_date
        WHEN MATCHED THEN
            UPDATE SET
                attribute_1 = S.attribute_1,
                attribute_2 = S.attribute_2,
                creation_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (rkopf_id, stichtag_date, attribute_1, attribute_2)
            VALUES (S.rkopf_id, S.stichtag_date, S.attribute_1, S.attribute_2);

        -- Capture number of processed records
        -- This counts all rows in RKopfStan for the given stichtag_date that were just processed
        -- Or, if the MERGE statement allows, MERGE output can be used.
        -- For simplicity, re-counting specific to the job.
        SELECT COUNT(*)
        INTO v_processed_records
        FROM `my_project.my_dataset.RKopfStan`
        WHERE stichtag_date = v_reference_date;

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;

        INSERT INTO `my_project.my_dataset.error_log` (log_time, job_id, procedure_name, error_message, stack_trace, reference_date, additional_info)
        VALUES (CURRENT_TIMESTAMP(), v_job_run_id, 'r_aurd_rechstan', v_error_message, @@error.stack_trace, v_reference_date, JSON_OBJECT('job_kennung', p_job_kennung, 'entry_number', p_eintrags_nr));

        -- Re-raise the error to ensure external orchestrators are aware of the failure
        RAISE USING MESSAGE v_error_message;
    END;

    -- Update job status and end time
    SET v_end_time = CURRENT_TIMESTAMP();
    UPDATE `my_project.my_dataset.job_table`
    SET
        end_time = v_end_time,
        status = v_status,
        processed_records = v_processed_records,
        error_message = v_error_message
    WHERE job_id = v_job_run_id;

END;