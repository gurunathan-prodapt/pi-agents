-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.bert_k_ausd_v_ta_cntrct_valid`(
    p_job_kennung STRING,
    p_eintrags_nr STRING
)
BEGIN
    DECLARE v_datum_str STRING;
    DECLARE v_processed_records INT64;
    DECLARE v_job_name STRING DEFAULT 'k_ausd_v_ta_cntrct_valid'; -- Derived from script name
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;

    -- Initialize start time
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Parameter Validation
    IF p_job_kennung IS NULL OR p_eintrags_nr IS NULL THEN
        INSERT INTO `project.dataset.error_log` (job_name, job_id, entry_number, error_timestamp, error_message, error_stack_trace)
        VALUES (v_job_name, p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), 'Required parameters p_job_kennung or p_eintrags_nr are missing.', @@error.stack_trace);
        RAISE SCRIPT_EXCEPTION('Required parameters p_job_kennung or p_eintrags_nr are missing.');
    END IF;

    -- Job Registration (Start)
    INSERT INTO `project.dataset.job_table` (job_name, job_id, entry_number, start_timestamp, status)
    VALUES (v_job_name, p_job_kennung, p_eintrags_nr, v_start_time, 'RUNNING');

    BEGIN EXCEPTION WHEN ERROR THEN
        -- Error handling for the main logic
        INSERT INTO `project.dataset.error_log` (job_name, job_id, entry_number, error_timestamp, error_message, error_stack_trace)
        VALUES (v_job_name, p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), CONCAT('Stored Procedure failed: ', @@error.message), @@error.stack_trace);

        -- Update job status to FAILED
        UPDATE `project.dataset.job_table`
        SET status = 'FAILED',
            end_timestamp = CURRENT_TIMESTAMP(),
            message = CONCAT('Failed: ', @@error.message)
        WHERE job_id = p_job_kennung AND entry_number = p_eintrags_nr;

        RAISE; -- Re-raise the exception to stop execution
    END;

    -- Derive v_datum_str (replaces Oracle SELECT NVL(TO_CHAR(MAX(m.timecreated)...)
    -- Assuming `dwtk_meldungen` table exists in BigQuery
    SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
    INTO v_datum_str
    FROM `project.dataset.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Truncate target table (replaces PL/SQL call to DWPA_UTIL_SKRIPT.runstatement)
    TRUNCATE TABLE `project.dataset.ta_cntrct_valid`;

    -- Core Data Insertion Logic (migrated from d_ausd_v_ta_cntrct_valid.sql)
    -- The source table 'cds$ta_cntrct_validity' is assumed to be `project.dataset.cds_ta_cntrct_validity`
    INSERT INTO `project.dataset.ta_cntrct_valid`(
        cntrct_validity_id,
        first_period_id,
        following_period_id,
        first_notice_period_id,
        follow_notice_period_id,
        bfc_age
    )
    SELECT
        CAST(cv.cntrct_validity_id AS STRING), -- Assuming IDs might be numeric in source
        CAST(cv.first_period_id AS STRING),
        CAST(cv.following_period_id AS STRING),
        CAST(cv.first_notice_period_id AS STRING),
        CAST(cv.follow_notice_period_id AS STRING),
        cv.insert_at
    FROM
        `project.dataset.cds_ta_cntrct_validity` AS cv
    WHERE
        cv.insert_at <= PARSE_DATE('%Y%m%d', v_datum_str)
    AND (cv.modified_at IS NULL
        OR cv.modified_at > PARSE_DATE('%Y%m%d', v_datum_str));

    SET v_processed_records = @@row_count;

    -- Job Completion and Result Logging
    SET v_end_time = CURRENT_TIMESTAMP();

    UPDATE `project.dataset.job_table`
    SET status = 'SUCCESS',
        end_timestamp = v_end_time,
        message = CONCAT('Successfully processed ', v_processed_records, ' records.')
    WHERE job_id = p_job_kennung AND entry_number = p_eintrags_nr;

    INSERT INTO `project.dataset.job_result_log` (job_name, job_id, entry_number, run_timestamp, records_processed, result_details)
    VALUES (v_job_name, p_job_kennung, p_eintrags_nr, v_end_time, v_processed_records, 'Data loaded successfully into ta_cntrct_valid.');

END;