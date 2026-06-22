-- Migrated BigQuery Stored Procedure from k_ausd_v_ta_notice.ksh
-- This procedure acts as the control wrapper, handling parameters, logging, and calling the core logic.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_control_ta_notice`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_script_name STRING DEFAULT 'k_ausd_v_ta_notice.ksh';
    DECLARE v_tab_name STRING DEFAULT 'ta_notice';
    DECLARE v_records_processed INT64;
    DECLARE v_process_date DATE;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack STRING;

    -- --- Step 1: Log job start ---
    INSERT INTO `your_project_id.your_dataset_id.job_run_log` (job_kennung, eintrags_nr, tab_name, script_name, status)
    VALUES (p_job_kennung, p_eintrags_nr, v_tab_name, v_script_name, 'STARTED');

    BEGIN
        -- --- Step 2: Parameter Validation ---
        IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
            RAISE USING MESSAGE = 'Parameter p_job_kennung cannot be NULL or empty.';
        END IF;

        IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
            RAISE USING MESSAGE = 'Parameter p_eintrags_nr cannot be NULL or empty.';
        END IF;

        -- --- Step 3: Determine process date (v_datum equivalent) ---
        -- Query to get the latest timecreated for 'BERT_DROP_TEMP_TABLE' from dwtk_meldungen
        -- Assuming `dwtk_meldungen` table exists and has `timecreated` (TIMESTAMP) and `job_kennung` (STRING) columns.
        SELECT
            COALESCE(MAX(DATE(m.timecreated)), DATE('1900-01-01'))
        INTO
            v_process_date
        FROM
            `your_project_id.your_dataset_id.dwtk_meldungen` AS m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        -- If no date is found, we might want to raise an error or use a default.
        -- Using '1900-01-01' as per original SQL's NVL default.

        -- --- Step 4: Call the core data transformation procedure ---
        CALL `your_project_id.your_dataset_id.sp_d_ausd_v_ta_notice`(
            p_process_date => v_process_date,
            p_records_processed => v_records_processed
        );

        -- --- Step 5: Log results and job completion ---
        INSERT INTO `your_project_id.your_dataset_id.job_run_result` (job_kennung, eintrags_nr, tab_name, record_count)
        VALUES (p_job_kennung, p_eintrags_nr, v_tab_name, v_records_processed);

        INSERT INTO `your_project_id.your_dataset_id.job_run_log` (job_kennung, eintrags_nr, tab_name, script_name, status)
        VALUES (p_job_kennung, p_eintrags_nr, v_tab_name, v_script_name, 'COMPLETED');

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_stack = @@error.stacktrace;

        -- Log error details
        INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg)
        VALUES (p_job_kennung, p_eintrags_nr, -1, v_error_message); -- Using -1 for generic BigQuery error

        -- Log job failure
        INSERT INTO `your_project_id.your_dataset_id.job_run_log` (job_kennung, eintrags_nr, tab_name, script_name, status)
        VALUES (p_job_kennung, p_eintrags_nr, v_tab_name, v_script_name, 'FAILED');

        -- Re-raise the error to propagate it to the caller/orchestrator
        RAISE USING MESSAGE = 'Job `sp_control_ta_notice` failed for JobKennung: ' || p_job_kennung || ', EintragsNr: ' || p_eintrags_nr || '. Error: ' || v_error_message;
    END;
END;