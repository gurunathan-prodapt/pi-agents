-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh

-- This stored procedure handles the provisioning of basic product data for the BERT system.
-- It generates a snapshot extraction of contract cache/base product data for a given cutoff date
-- and loads it into a target table, supporting restart capabilities.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.r_ausd_bp_ta_bpr_optionen`(
    IN p_stichtag STRING,           -- Input cutoff date in DDMMYYYY format
    IN p_wiederanlaufWert INT64     -- Restart value, filters DWH_VERTRAG_ID > this value
)
BEGIN
    DECLARE v_sysdate DATE;
    DECLARE v_stichtag DATE;
    DECLARE v_datum DATE;
    DECLARE v_job_run_id STRING;
    DECLARE v_error_message STRING;
    DECLARE v_inserted_rows INT64;
    DECLARE v_start_time TIMESTAMP;

    -- Generate a unique run ID for logging purposes
    SET v_job_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Initialize v_sysdate
    SET v_sysdate = CURRENT_DATE();

    -- Determine v_stichtag: Use p_stichtag if provided and valid, otherwise use system date.
    BEGIN
        SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag);
    EXCEPTION WHEN ERROR THEN
        SET v_stichtag = v_sysdate; -- Default to system date if p_stichtag is invalid or null
    END;

    -- Log job start
    INSERT INTO `your_gcp_project.your_bq_dataset.job_audit_log` (
        job_name, run_id, start_time, status, message, parameters
    )
    VALUES (
        'r_ausd_bp_ta_bpr_optionen',
        v_job_run_id,
        v_start_time,
        'RUNNING',
        'Job started with parameters',
        TO_JSON(STRUCT(p_stichtag, p_wiederanlaufWert))
    );

    BEGIN
        -- Determine v_datum from metadata table (migrated from isbert_schema.dwtk_meldungen)
        -- Assuming 'value' column stores date as STRING in DDMMYYYY format
        SELECT
            PARSE_DATE('%d%m%Y', CAST(value AS STRING))
        INTO v_datum
        FROM
            `your_gcp_project.your_bq_dataset.dwtk_meldungen`
        WHERE
            job_kennung = 'BERT_DROP_TEMP_TABLE';

        -- Truncate the target table
        TRUNCATE TABLE `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen`;

        -- Insert data into target table from source table with restart logic
        INSERT INTO `your_gcp_project.your_bq_dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID)
        SELECT
            bp.CNTRCT_ID,
            bp.BPR_ID
        FROM
            `your_gcp_project.your_bq_dataset.sof_ta_bpr_instance` AS bp
        WHERE
            bp.DWH_VERTRAG_ID > p_wiederanlaufWert; -- Apply restart logic

        SET v_inserted_rows = ROW_COUNT();

        -- Log job success
        INSERT INTO `your_gcp_project.your_bq_dataset.job_audit_log` (
            job_name, run_id, end_time, status, message, inserted_rows
        )
        VALUES (
            'r_ausd_bp_ta_bpr_optionen',
            v_job_run_id,
            CURRENT_TIMESTAMP(),
            'SUCCESS',
            FORMAT('Job completed successfully. Inserted %d rows.', v_inserted_rows),
            v_inserted_rows
        );

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;

        -- Log job failure
        INSERT INTO `your_gcp_project.your_bq_dataset.job_audit_log` (
            job_name, run_id, end_time, status, message, parameters
        )
        VALUES (
            'r_ausd_bp_ta_bpr_optionen',
            v_job_run_id,
            CURRENT_TIMESTAMP(),
            'FAILED',
            FORMAT('Job failed with error: %s', v_error_message),
            TO_JSON(STRUCT(p_stichtag, p_wiederanlaufWert))
        );

        RAISE USING MESSAGE = v_error_message; -- Re-raise the error to propagate it
    END;
END;