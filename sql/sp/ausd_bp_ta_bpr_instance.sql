-- BigQuery Stored Procedure for orchestration
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

CREATE OR REPLACE PROCEDURE project.dataset.ausd_bp_ta_bpr_instance(
    IN p_stichtag_str STRING OPTIONS(description="Processing date in DDMMYYYY format, defaults to system date"),
    IN p_wiederanlaufwert INT64 OPTIONS(description="Restart value, defaults to 0")
)
BEGIN
    DECLARE v_job_run_id STRING;
    DECLARE v_job_name STRING DEFAULT 'ausd_bp_ta_bpr_instance';
    DECLARE v_program_name STRING DEFAULT 'r_ausd_bp_ta_bpr_instance';
    DECLARE v_program_version STRING DEFAULT '1.0.0'; -- Placeholder, ideally fetched from a config table
    DECLARE v_job_kennung STRING DEFAULT 'BERT_TA_BPR'; -- Corresponds to JobKennung in source
    DECLARE v_dw_eintrags_nr STRING; -- Corresponds to DW_EintragsNr in source
    DECLARE v_stichtag DATE;
    DECLARE v_actual_wiederanlaufwert INT64;
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE v_parameters_json JSON;

    -- Generate a unique job run ID
    SET v_job_run_id = GENERATE_UUID();

    -- Set default for p_wiederanlaufwert if not provided
    SET v_actual_wiederanlaufwert = COALESCE(p_wiederanlaufwert, 0);

    -- Set default for p_stichtag_str if not provided, then parse
    IF p_stichtag_str IS NULL OR p_stichtag_str = '' THEN
        SET v_stichtag = v_sysdate;
    ELSE
        -- Attempt to parse p_stichtag_str, handle potential errors
        BEGIN
            SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_str);
        EXCEPTION WHEN ERROR THEN
            SIGNAL SQLSTATE '22000' SET MESSAGE_TEXT = 'Invalid p_stichtag_str format. Expected DDMMYYYY.';
        END;
    END IF;

    -- Simple parameter validation (e.g., for future extensions)
    IF v_stichtag IS NULL THEN
        SIGNAL SQLSTATE '22000' SET MESSAGE_TEXT = 'Processing date (p_stichtag) cannot be null after defaulting.';
    END IF;

    -- Placeholder for DW_EintragsNr generation/retrieval (if it's a unique entry ID)
    -- For now, generate a UUID or a simple timestamp-based ID
    SET v_dw_eintrags_nr = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()) || '_' || SUBSTR(GENERATE_UUID(), 1, 8);


    -- Capture parameters for logging
    SET v_parameters_json = TO_JSON(STRUCT(p_stichtag_str, p_wiederanlaufwert, v_stichtag, v_actual_wiederanlaufwert));

    -- Log job start
    INSERT INTO project.dataset.job_log (
        job_run_id, job_name, program_name, program_version, status, start_time, stichtag, wiederanlaufwert, log_message, parameters_json
    ) VALUES (
        v_job_run_id, v_job_name, v_program_name, v_program_version, 'STARTED', CURRENT_TIMESTAMP(), v_stichtag, v_actual_wiederanlaufwert, 'Job execution started.', v_parameters_json
    );

    BEGIN
        -- Call the kernel stored procedure
        CALL project.dataset.k_ausd_bp_ta_bpr_instance_sql(
            v_stichtag,
            v_actual_wiederanlaufwert,
            v_job_kennung,
            v_dw_eintrags_nr
        );

        -- Log job success
        INSERT INTO project.dataset.job_log (
            job_run_id, job_name, program_name, program_version, status, end_time, log_message
        ) VALUES (
            v_job_run_id, v_job_name, v_program_name, v_program_version, 'SUCCEEDED', CURRENT_TIMESTAMP(), 'Job completed successfully.'
        );

    EXCEPTION WHEN ERROR THEN
        -- Log job failure
        INSERT INTO project.dataset.job_log (
            job_run_id, job_name, program_name, program_version, status, end_time, log_message, error_details
        ) VALUES (
            v_job_run_id, v_job_name, v_program_name, v_program_version, 'FAILED', CURRENT_TIMESTAMP(), 'Job failed.', @@error.message
        );
        -- Re-raise the error to signal failure to the caller (e.g., Airflow)
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @@error.message;
    END;

END;