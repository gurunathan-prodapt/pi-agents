-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

-- This BigQuery Stored Procedure orchestrates the data extraction and transformation
-- for the 'r_ausd_bp_ta_msisdn' job. It replaces the main shell script and
-- integrates the core SQL logic.

CREATE OR REPLACE PROCEDURE `my-gcp-project.isbert_dataset.r_ausd_bp_ta_msisdn`(
    IN p_stichtag_str STRING,              -- Cutoff date in DDMMYYYY format, optional. e.g., '01012023'
    IN p_wiederanlaufwert_param INT64,     -- Restart value, optional. Default: 0
    IN p_job_kennung STRING,               -- Job identifier, e.g., 'FOS_BP_TA_MSISDN'
    IN p_eintrags_nr STRING                -- Entry number, optional
)
OPTIONS(
    description="Main BigQuery Stored Procedure for the r_ausd_bp_ta_msisdn job. Orchestrates data extraction and transformation."
)
BEGIN
    DECLARE v_job_id STRING DEFAULT 'r_ausd_bp_ta_msisdn';
    DECLARE v_run_id STRING DEFAULT GENERATE_UUID();
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufwert INT64;
    DECLARE v_record_count INT64;
    DECLARE v_bert_drop_temp_table_date DATE; -- Variable for BERT_DROP_TEMP_TABLE date from dwtk_meldungen

    -- Defaulting and parameter validation
    BEGIN
        -- p_stichtag: Defaults to CURRENT_DATE() if not provided
        IF p_stichtag_str IS NULL OR TRIM(p_stichtag_str) = '' THEN
            SET v_stichtag = CURRENT_DATE();
        ELSE
            CALL `my-gcp-project.isbert_dataset.validate_ddmmyyyy`(p_stichtag_str, v_stichtag);
        END IF;

        -- p_wiederanlaufWert: Defaults to 0 if not provided
        SET v_wiederanlaufwert = COALESCE(p_wiederanlaufwert_param, 0);

        -- Insert initial audit log entry (status 'RUNNING')
        INSERT INTO `my-gcp-project.isbert_dataset.job_audit`
            (job_id, run_id, start_timestamp, status, stichtag, wiederanlaufwert)
        VALUES
            (v_job_id, v_run_id, v_start_timestamp, 'RUNNING', v_stichtag, v_wiederanlaufwert);

        -- Derive v_bert_drop_temp_table_date from dwtk_meldungen,
        -- mimicking the Oracle script's DEFINE v_datum logic.
        -- This variable is defined but not directly used in the BigQuery
        -- translation of d_ausd_bp_ta_msisdn_logic.sql as table names are static.
        SELECT COALESCE(MAX(DATE(m.timecreated)), PARSE_DATE('%Y%m%d', '19000101'))
        INTO v_bert_drop_temp_table_date
        FROM `my-gcp-project.isbert_dataset.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        -- Core data extraction and transformation logic (from bq_d_ausd_bp_ta_msisdn_logic.sql)
        -- Step 01: Truncate the target table `sof_ta_msisdn`
        TRUNCATE TABLE `my-gcp-project.isbert_dataset.sof_ta_msisdn`;

        -- Step 02: Insert valid MSISDNs into the target table
        INSERT INTO `my-gcp-project.isbert_dataset.sof_ta_msisdn`
        (
          BPR_INSTANCE_ID,
          MSISDN,
          CALLNUMBER_ROLE_ID,
          VALID_TO
        )
        SELECT
            cn1.bpri_com_id,
            cn1.msisdn,
            cn1.callnumber_role_id,
            COALESCE(cn1.valid_to, PARSE_DATE('%Y%m%d', '47121231')) AS valid_to
        FROM
            (
                SELECT
                    cn.bpri_com_id,
                    cn.msisdn,
                    cn.callnumber_role_id,
                    cn.valid_to,
                    MAX(COALESCE(cn.valid_to, PARSE_DATE('%Y%m%d', '47121231'))) OVER (PARTITION BY cn.bpri_com_id) AS max_valid_to
                FROM
                    `my-gcp-project.isbert_dataset.sof_ta_msisdn_his` AS cn
            ) AS cn1
        WHERE
            COALESCE(cn1.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = cn1.max_valid_to;

        -- Get record count after insertion
        SELECT COUNT(*) INTO v_record_count FROM `my-gcp-project.isbert_dataset.sof_ta_msisdn`;

        -- Set success status
        SET v_status = 'SUCCESS';
        SET v_end_timestamp = CURRENT_TIMESTAMP();

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;
        SET v_end_timestamp = CURRENT_TIMESTAMP();

        -- Log error to job_audit. A separate INSERT is used for the error case
        -- to ensure error details are captured even if the final UPDATE fails.
        INSERT INTO `my-gcp-project.isbert_dataset.job_audit`
            (job_id, run_id, start_timestamp, end_timestamp, status, error_message, stichtag, wiederanlaufwert)
        VALUES
            (v_job_id, v_run_id, v_start_timestamp, v_end_timestamp, v_status, v_error_message, v_stichtag, v_wiederanlaufwert);
        RAISE; -- Re-raise the error to propagate it to the caller (e.g., Cloud Composer)
    END;

    -- Update final audit log entry with end_timestamp and status for success case
    UPDATE `my-gcp-project.isbert_dataset.job_audit`
    SET
        end_timestamp = v_end_timestamp,
        status = v_status,
        error_message = v_error_message -- Will be NULL for success
    WHERE
        run_id = v_run_id;

    -- Insert record counts if the job was successful
    IF v_status = 'SUCCESS' THEN
        INSERT INTO `my-gcp-project.isbert_dataset.job_result_counts`
            (job_id, run_id, stichtag, record_count, timestamp)
        VALUES
            (v_job_id, v_run_id, v_stichtag, v_record_count, CURRENT_TIMESTAMP());
    END IF;
END;