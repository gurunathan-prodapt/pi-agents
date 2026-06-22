--
-- BigQuery Stored Procedure for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
-- This procedure orchestrates the data reconciliation and update for the sof_ta_p_discount table.
--
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_v_ta_p_discount`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_datum STRING;
    DECLARE v_records INT64;
    DECLARE v_job_start_time TIMESTAMP;
    DECLARE v_job_end_time TIMESTAMP;
    DECLARE v_job_status STRING DEFAULT 'FAILED';
    DECLARE v_log_message STRING;
    DECLARE v_error_message STRING;
    DECLARE v_stack_trace STRING;

    SET v_job_start_time = CURRENT_TIMESTAMP();

    -- Log job start
    INSERT INTO `project.dataset.job_log` (log_id, job_name, start_time, status, message)
    VALUES (GENERATE_UUID(), 'r_ausd_v_ta_p_discount', v_job_start_time, 'STARTED', 'Job started with JobKennung: ' || p_job_kennung || ', EintragsNr: ' || p_eintrags_nr);

    BEGIN
        -- Derive v_datum from latest BERT_DROP_TEMP_TABLE entry in dw_meldungen
        SET v_datum = (
            SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated))), '19000101')
            FROM `project.dataset.dwtk_meldungen`
            WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
        );

        -- Log derived date
        SET v_log_message = 'Derived processing date: ' || v_datum;
        INSERT INTO `project.dataset.job_log` (log_id, job_name, start_time, message)
        VALUES (GENERATE_UUID(), 'r_ausd_v_ta_p_discount', CURRENT_TIMESTAMP(), v_log_message);

        -- Truncate target table
        TRUNCATE TABLE `project.dataset.sof_ta_p_discount`;

        -- Log truncate action
        INSERT INTO `project.dataset.job_log` (log_id, job_name, start_time, message)
        VALUES (GENERATE_UUID(), 'r_ausd_v_ta_p_discount', CURRENT_TIMESTAMP(), 'Truncated target table sof_ta_p_discount');

        -- Insert reconciled rows
        INSERT INTO `project.dataset.sof_ta_p_discount`
          (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle, contract_number)
        SELECT
          da.cntrct_id,
          da.disc_vector_ty,
          da.cntrct_obj_version,
          da.rabatt_alle,
          c.contract_number
        FROM `project.dataset.sof_ta_disc_zusgf` AS da
        JOIN `project.dataset.sof_ta_cntrct_crs` AS c
          ON da.cntrct_id = c.cntrct_id
         AND da.cntrct_obj_version = c.obj_version;

        SET v_records = @@row_count;
        SET v_job_status = 'COMPLETED';
        SET v_log_message = 'Successfully inserted ' || v_records || ' records into sof_ta_p_discount.';

        EXCEPTION WHEN ERROR THEN
            SET v_error_message = CONCAT('Error during data transformation: ', @@error.message);
            SET v_stack_trace = @@error.stack_trace;
            -- Log error
            INSERT INTO `project.dataset.job_error_log` (error_id, job_name, error_time, error_message, stack_trace, severity)
            VALUES (GENERATE_UUID(), 'r_ausd_v_ta_p_discount', CURRENT_TIMESTAMP(), v_error_message, v_stack_trace, 'ERROR');
            SET v_log_message = 'Job failed: ' || v_error_message;

    END;

    SET v_job_end_time = CURRENT_TIMESTAMP();

    -- Log job end
    INSERT INTO `project.dataset.job_log` (log_id, job_name, start_time, end_time, status, message, records_processed)
    VALUES (GENERATE_UUID(), 'r_ausd_v_ta_p_discount', v_job_start_time, v_job_end_time, v_job_status, v_log_message, v_records);

END;