-- BigQuery Stored Procedure for k_ausd_bp_ta_rn_vertrag.ksh
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
-- Core SQL: d_ausd_bp_ta_rn_vertrag.sql

CREATE OR REPLACE PROCEDURE my_project.my_dataset.r_ausd_bp_ta_rn_vertrag(
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    p_stichtag STRING, -- Expected format: DDMMYYYY
    p_wiederanlauf_wert STRING
)
BEGIN
    DECLARE v_run_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'SUCCESS';
    DECLARE v_error_message STRING;
    DECLARE v_error_stack_trace STRING;
    DECLARE v_records_processed INT64;
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_from_dwtk STRING; -- YYYYMMDD format

    -- Generate a unique run ID for logging purposes
    SET v_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Log job start
    INSERT INTO my_project.my_dataset.job_log (job_name, start_time, status, message, run_id)
    VALUES ('r_ausd_bp_ta_rn_vertrag', v_start_time, 'RUNNING', 'Job started', v_run_id);

    BEGIN
        -- 1. Parameter Validation
        IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
            RAISE USING MESSAGE = 'Parameter p_job_kennung cannot be NULL or empty.';
        END IF;
        IF p_eintrags_nr IS NULL OR p_eintrags_nr = '' THEN
            RAISE USING MESSAGE = 'Parameter p_eintrags_nr cannot be NULL or empty.';
        END IF;
        IF p_stichtag IS NULL OR p_stichtag = '' THEN
            RAISE USING MESSAGE = 'Parameter p_stichtag cannot be NULL or empty.';
        END IF;
        -- p_wiederanlauf_wert can be NULL/empty, so no validation here

        -- Validate and parse p_stichtag
        IF NOT REGEXP_CONTAINS(p_stichtag, r'^[0-9]{8}$') THEN
            RAISE USING MESSAGE = FORMAT('Parameter p_stichtag "%s" has invalid format. Expected DDMMYYYY.', p_stichtag);
        END IF;

        SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_stichtag);
        IF v_stichtag_date IS NULL THEN
            RAISE USING MESSAGE = FORMAT('Could not parse p_stichtag "%s" as a valid date.', p_stichtag);
        END IF;

        -- 2. Determine v_datum from dwtk_meldungen (equivalent to Step00)
        -- This simulates the Oracle NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
        INTO
            v_datum_from_dwtk
        FROM
            my_project.my_dataset.dwtk_meldungen AS m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        -- If no value found, default to '19000101' as in Oracle logic
        IF v_datum_from_dwtk IS NULL THEN
            SET v_datum_from_dwtk = '19000101';
        END IF;

        -- 3. Truncate target table (equivalent to Step01: DWPA_UTIL_SKRIPT.runstatement)
        TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_vertrag;

        -- 4. Insert data into target table (equivalent to Step05_b)
        INSERT INTO my_project.my_dataset.sof_ta_rn_vertrag
        (
            CNTRCT_ID, TN_MULTI_SINGLE, TN_TEL_MSISDN, TN_TEL_STATUS, TN_TEL_VALID_TO,
            TN_FAX_MSISDN, TN_FAX_STATUS, TN_FAX_VALID_TO, TN_DAT_MSISDN, TN_DAT_STATUS, TN_DAT_VALID_TO,
            TC_MULTI_SINGLE, TC_TEL_MSISDN, TC_TEL_STATUS, TC_TEL_VALID_TO,
            TC_FAX_MSISDN, TC_FAX_STATUS, TC_FAX_VALID_TO, TC_DAT_MSISDN, TC_DAT_STATUS, TC_DAT_VALID_TO,
            TB_MULTI_SINGLE, TB_TEL_MSISDN, TB_TEL_STATUS, TB_TEL_VALID_TO,
            TB_FAX_MSISDN, TB_FAX_STATUS, TB_FAX_VALID_TO, TB_DAT_MSISDN, TB_DAT_STATUS, TB_DAT_VALID_TO,
            MS_RN_1_MSISDN, MS_RN_1_STATUS, MS_RN_1_VALID_TO,
            MS_RN_2_MSISDN, MS_RN_2_STATUS, MS_RN_2_VALID_TO
        )
        SELECT
            cntrct_id,
            MAX(TN_multi_single) AS TN_multi_single,
            MAX(TN_TEL_msisdn) AS TN_TEL_msisdn,
            MAX(TN_TEL_status) AS TN_TEL_status,
            MAX(TN_TEL_valid_to) AS TN_TEL_valid_to,
            MAX(TN_FAX_msisdn) AS TN_FAX_msisdn,
            MAX(TN_FAX_status) AS TN_FAX_status,
            MAX(TN_FAX_valid_to) AS TN_FAX_valid_to,
            MAX(TN_DAT_msisdn) AS TN_DAT_msisdn,
            MAX(TN_DAT_status) AS TN_DAT_status,
            MAX(TN_DAT_valid_to) AS TN_DAT_valid_to,
            MAX(TC_multi_single) AS TC_multi_single,
            MAX(TC_TEL_msisdn) AS TC_TEL_msisdn,
            MAX(TC_TEL_status) AS TC_TEL_status,
            MAX(TC_TEL_valid_to) AS TC_TEL_valid_to,
            MAX(TC_FAX_msisdn) AS TC_FAX_msisdn,
            MAX(TC_FAX_status) AS TC_FAX_status,
            MAX(TC_FAX_valid_to) AS TC_FAX_valid_to,
            MAX(TC_DAT_msisdn) AS TC_DAT_msisdn,
            MAX(TC_DAT_status) AS TC_DAT_status,
            MAX(TC_DAT_valid_to) AS TC_DAT_valid_to,
            MAX(TB_multi_single) AS TB_multi_single,
            MAX(TB_TEL_msisdn) AS TB_TEL_msisdn,
            MAX(TB_TEL_status) AS TB_TEL_status,
            MAX(TB_TEL_valid_to) AS TB_TEL_valid_to,
            MAX(TB_FAX_msisdn) AS TB_FAX_msisdn,
            MAX(TB_FAX_status) AS TB_FAX_status,
            MAX(TB_FAX_valid_to) AS TB_FAX_valid_to,
            MAX(TB_DAT_msisdn) AS TB_DAT_msisdn,
            MAX(TB_DAT_status) AS TB_DAT_status,
            MAX(TB_DAT_valid_to) AS TB_DAT_valid_to,
            MAX(MS_RN_1_msisdn) AS MS_RN_1_msisdn,
            MAX(MS_RN_1_status) AS MS_RN_1_status,
            MAX(MS_RN_1_valid_to) AS MS_RN_1_valid_to,
            MAX(MS_RN_2_msisdn) AS MS_RN_2_msisdn,
            MAX(MS_RN_2_status) AS MS_RN_2_status,
            MAX(MS_RN_2_valid_to) AS MS_RN_2_valid_to
        FROM
            my_project.my_dataset.sof_ta_rn_einzeln AS rp
        GROUP BY
            cntrct_id;

        -- Get records processed count
        SET v_records_processed = @@row_count;

        SET v_end_time = CURRENT_TIMESTAMP();

        -- Log job success
        UPDATE my_project.my_dataset.job_log
        SET
            end_time = v_end_time,
            status = v_status,
            records_processed = v_records_processed,
            message = 'Job completed successfully'
        WHERE
            run_id = v_run_id;

    EXCEPTION WHEN ERROR THEN
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;
        SET v_error_stack_trace = @@error.stack_trace;

        -- Log job failure
        UPDATE my_project.my_dataset.job_log
        SET
            end_time = v_end_time,
            status = v_status,
            message = FORMAT('Job failed: %s', v_error_message)
        WHERE
            run_id = v_run_id;

        INSERT INTO my_project.my_dataset.error_log (job_name, error_time, error_message, stack_trace, run_id)
        VALUES ('r_ausd_bp_ta_rn_vertrag', v_end_time, v_error_message, v_error_stack_trace, v_run_id);

        RAISE USING MESSAGE = FORMAT('Job execution failed. Error: %s', v_error_message);
    END;
END;