-- BigQuery Stored Procedure for orchestration and control
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.r_ausd_vertrag_control`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_process_date DATE;
    DECLARE v_records_processed INT64;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_error_code INT64;
    DECLARE v_error_argument STRING;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_records_processed = 0;
    SET v_status = 'RUNNING';
    SET v_message = 'Control script started.';

    -- Log job start
    INSERT INTO `my_project.my_dataset.job_log`
    (job_kennung, eintrags_nr, start_timestamp, end_timestamp, status, records_processed, message)
    VALUES
    (p_job_kennung, p_eintrags_nr, v_start_timestamp, NULL, v_status, v_records_processed, v_message);

    -- Parameter Validation
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_code = 193; -- Equivalent to "Notwendiges Argument fehlt"
        SET v_error_argument = 'p_job_kennung';
        SET v_message = 'Parameter p_job_kennung is missing.';
        INSERT INTO `my_project.my_dataset.error_log` (job_kennung, eintrags_nr, error_code, error_argument, message)
        VALUES (p_job_kennung, p_eintrags_nr, v_error_code, v_error_argument, v_message);
        SET v_status = 'FAILURE';
        RAISE USING MESSAGE 'FEHLER: ' || v_error_code || ' ' || v_error_argument || ' - ' || v_message;
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SET v_error_code = 193; -- Equivalent to "Notwendiges Argument fehlt"
        SET v_error_argument = 'p_eintrags_nr';
        SET v_message = 'Parameter p_eintrags_nr is missing.';
        INSERT INTO `my_project.my_dataset.error_log` (job_kennung, eintrags_nr, error_code, error_argument, message)
        VALUES (p_job_kennung, p_eintrags_nr, v_error_code, v_error_argument, v_message);
        SET v_status = 'FAILURE';
        RAISE USING MESSAGE 'FEHLER: ' || v_error_code || ' ' || v_error_argument || ' - ' || v_message;
    END IF;

    -- Determine v_datum from dwtk_meldungen (as per original ksh script)
    -- Assuming dwtk_meldungen is migrated to my_project.my_dataset.dwtk_meldungen
    BEGIN
        SELECT PARSE_DATE('%Y%m%d', NVL(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101'))
        INTO v_process_date
        FROM `my_project.my_dataset.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        IF v_process_date IS NULL THEN
            SET v_error_code = -1; -- Custom error code for date derivation failure
            SET v_error_argument = 'v_process_date';
            SET v_message = 'Could not derive process date from dwtk_meldungen.';
            INSERT INTO `my_project.my_dataset.error_log` (job_kennung, eintrags_nr, error_code, error_argument, message)
            VALUES (p_job_kennung, p_eintrags_nr, v_error_code, v_error_argument, v_message);
            SET v_status = 'FAILURE';
            RAISE USING MESSAGE 'FEHLER: ' || v_error_code || ' ' || v_error_argument || ' - ' || v_message;
        END IF;

    EXCEPTION WHEN ERROR THEN
        SET v_error_code = -1;
        SET v_error_argument = 'DATE_DERIVATION_ERROR';
        SET v_message = 'Error deriving process date: ' || @@error.message;
        INSERT INTO `my_project.my_dataset.error_log` (job_kennung, eintrags_nr, error_code, error_argument, message)
        VALUES (p_job_kennung, p_eintrags_nr, v_error_code, v_error_argument, v_message);
        SET v_status = 'FAILURE';
        RAISE USING MESSAGE 'FEHLER: ' || v_error_code || ' ' || v_error_argument || ' - ' || v_message;
    END;

    -- Execute data processing SQL
    BEGIN
        CALL `my_project.my_dataset.d_ausd_v_ta_discount_rr`(p_eintrags_nr, p_job_kennung, v_process_date, v_records_processed);
        SET v_status = 'SUCCESS';
        SET v_message = 'Data processing completed.';
    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILURE';
        SET v_message = 'Data processing failed: ' || @@error.message;
        -- d_ausd_v_ta_discount_rr already logs detailed errors, but we catch and re-raise here for control flow
        RAISE; -- Re-raise the caught error
    END;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Update the job_log entry with final status and records processed
    UPDATE `my_project.my_dataset.job_log`
    SET
        end_timestamp = v_end_timestamp,
        status = v_status,
        records_processed = v_records_processed,
        message = v_message
    WHERE
        job_kennung = p_job_kennung AND eintrags_nr = p_eintrags_nr AND start_timestamp = v_start_timestamp;

    SELECT CONCAT('---------- ENDE Datenverarbeitung ---------- Records Processed: ', CAST(v_records_processed AS STRING));

END;