--
-- BigQuery Stored Procedure: ausd_bp_ta_apn_vertrag_wrapper
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh
-- Orchestrates the core logic, handles parameters, logging, and error management.
--
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`(
    IN p_stichtag STRING,           -- Stichtag (cutoff date) in 'DDMMYYYY' format
    IN p_wiederanlaufWert STRING    -- Wiederanlaufwert (restart value)
)
BEGIN
    DECLARE v_jobkennung STRING;
    DECLARE v_sysdate STRING;
    DECLARE v_restart_value STRING;
    DECLARE v_stichtag STRING;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'RUNNING';
    DECLARE v_error_message STRING DEFAULT NULL;
    DECLARE v_log_id STRING;

    -- Generate a unique job identifier
    SET v_jobkennung = GENERATE_UUID();
    SET v_start_timestamp = CURRENT_TIMESTAMP();

    -- Determine system date (DDMMYYYY)
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Parameter defaulting and validation
    IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN
      SET v_restart_value = '0';
    ELSE
      SET v_restart_value = p_wiederanlaufWert;
    END IF;

    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
      SET v_stichtag = v_sysdate;
    ELSE
      SET v_stichtag = p_stichtag;
    END IF;

    -- Basic validation: Check if stichtag is present after defaulting
    IF v_stichtag IS NULL OR TRIM(v_stichtag) = '' THEN
        SET v_error_message = 'ERROR: Stichtag parameter is missing or empty after defaulting.';
        SET v_status = 'FAILED';
        SET v_end_timestamp = CURRENT_TIMESTAMP();

        -- Log job failure in job_registry
        INSERT INTO `your_gcp_project.your_bq_dataset.job_registry`
        VALUES (
            v_jobkennung,
            'ausd_bp_ta_apn_vertrag',
            'r_ausd_bp_ta_apn_vertrag.ksh',
            v_start_timestamp,
            v_end_timestamp,
            v_status,
            TO_JSON(STRUCT(p_stichtag AS p_stichtag_raw, p_wiederanlaufWert AS p_wiederanlaufWert_raw, v_stichtag AS p_stichtag_actual, v_restart_value AS p_wiederanlaufWert_actual)),
            v_error_message
        );
        -- Log detailed error
        SET v_log_id = GENERATE_UUID();
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log`
        VALUES (v_log_id, v_jobkennung, CURRENT_TIMESTAMP(), 'ERROR', 'wrapper', v_error_message, NULL);
        
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Log job start in job_registry
    INSERT INTO `your_gcp_project.your_bq_dataset.job_registry`
    VALUES (
        v_jobkennung,
        'ausd_bp_ta_apn_vertrag',
        'r_ausd_bp_ta_apn_vertrag.ksh',
        v_start_timestamp,
        NULL, -- end_timestamp will be updated later
        v_status,
        TO_JSON(STRUCT(p_stichtag AS p_stichtag_raw, p_wiederanlaufWert AS p_wiederanlaufWert_raw, v_stichtag AS p_stichtag_actual, v_restart_value AS p_wiederanlaufWert_actual)),
        NULL
    );

    -- Log job start in job_log
    SET v_log_id = GENERATE_UUID();
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log`
    VALUES (v_log_id, v_jobkennung, CURRENT_TIMESTAMP(), 'INFO', 'wrapper', 'Job started with parameters.', TO_JSON(STRUCT(v_stichtag AS stichtag, v_restart_value AS wiederanlaufWert)));

    -- Error handling block for core logic
    BEGIN
        -- Call the core processing stored procedure
        CALL `your_gcp_project.your_bq_dataset.k_ausd_bp_ta_apn_vertrag`(
            v_jobkennung,
            v_stichtag,
            v_restart_value
        );

        SET v_status = 'SUCCESS';
        SET v_end_timestamp = CURRENT_TIMESTAMP();

        -- Log success in job_log
        SET v_log_id = GENERATE_UUID();
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log`
        VALUES (v_log_id, v_jobkennung, CURRENT_TIMESTAMP(), 'INFO', 'wrapper', 'Job completed successfully.', NULL);

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_status = 'FAILED';
        SET v_end_timestamp = CURRENT_TIMESTAMP();

        -- Log error in job_log
        SET v_log_id = GENERATE_UUID();
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log`
        VALUES (v_log_id, v_jobkennung, CURRENT_TIMESTAMP(), 'ERROR', 'wrapper', 'Job failed during core processing.', TO_JSON(STRUCT(v_error_message AS error_detail)));

        -- Re-raise the error to propagate it to the caller (e.g., Airflow)
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;

    END;

    -- Update job_registry with final status
    UPDATE `your_gcp_project.your_bq_dataset.job_registry`
    SET
        end_timestamp = v_end_timestamp,
        status = v_status,
        error_message = v_error_message
    WHERE job_id = v_jobkennung;

END;