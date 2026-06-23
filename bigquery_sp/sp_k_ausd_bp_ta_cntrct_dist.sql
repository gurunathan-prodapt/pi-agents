-- BigQuery Stored Procedure for k_ausd_bp_ta_cntrct_dist
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    p_stichtag STRING, -- Expected DDMMYYYY
    p_wiederanlauf_wert STRING -- Can be NULL or empty
)
OPTIONS(
    description="BigQuery Stored Procedure to orchestrate contract distribution processing, replacing k_ausd_bp_ta_cntrct_dist.ksh."
)
BEGIN
    -- Declare variables
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'FAILED';
    DECLARE v_error_message STRING;
    DECLARE v_run_id STRING DEFAULT GENERATE_UUID();
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records_processed INT64;

    -- Input parameter validation
    -- Equivalent to pruefeParameterGesetzt calls in KSH
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        RAISE USING MESSAGE = 'Parameter p_job_kennung is missing or empty.';
    END IF;
    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        RAISE USING MESSAGE = 'Parameter p_eintrags_nr is missing or empty.';
    END IF;
    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
        RAISE USING MESSAGE = 'Parameter p_stichtag is missing or empty.';
    END IF;

    -- Validate p_stichtag format (DDMMYYYY)
    -- Equivalent to DWDate_Datum_Check in KSH
    IF SAFE.PARSE_DATE('%d%m%Y', p_stichtag) IS NULL THEN
        RAISE USING MESSAGE = 'Parameter p_stichtag has an invalid format. Expected DDMMYYYY.';
    END IF;

    -- Date derivation (equivalent to gestern.ksh and related logic)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    BEGIN
        -- TRUNCATE existing data in the target table
        -- Replaces `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_dist REUSE STORAGE');`
        TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;

        -- Execute the core transformation logic
        -- Replaces `d_ausd_bp_ta_cntrct_dist.sql` execution
        INSERT INTO `project.dataset.sof_ta_cntrct_dist`
        (cntrct_id)
        SELECT
            DISTINCT cntrct_id
        FROM
            `project.dataset.sof_ta_bpr_basis`;

        -- Capture record count
        -- Replaces reading from $DW_DIR_UTL/bert_k_ausd_bp_ta_cntrct_dist.tmp
        SET v_records_processed = (SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_dist`);

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_status = 'FAILED';
    END;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Log job execution details
    -- Replaces FOSJobErzeugeEintrag (commented out in legacy, now implemented)
    INSERT INTO `project.dataset.job_audit_table` (
        job_name,
        run_id,
        start_timestamp,
        end_timestamp,
        status,
        error_message,
        input_params,
        output_records,
        duration_ms
    )
    VALUES (
        'k_ausd_bp_ta_cntrct_dist',
        v_run_id,
        v_start_timestamp,
        v_end_timestamp,
        v_status,
        v_error_message,
        TO_JSON(STRUCT(p_job_kennung AS job_kennung, p_eintrags_nr AS eintrags_nr, p_stichtag AS stichtag, p_wiederanlauf_wert AS wiederanlauf_wert, v_datum_heute AS datum_heute, v_datum_gestern AS datum_gestern)),
        v_records_processed,
        TIMESTAMP_DIFF(v_end_timestamp, v_start_timestamp, MILLISECOND)
    );

    IF v_status = 'FAILED' THEN
        RAISE USING MESSAGE = 'Job failed: ' || v_error_message;
    END IF;

END;