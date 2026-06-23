-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Description: Migrated control logic from k_ausd_v_ta_p_vertrag.ksh to BigQuery stored procedure.
CREATE OR REPLACE PROCEDURE `my_dataset.r_ausd_vertrag_control`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING
)
BEGIN
    DECLARE v_job_run_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_error_detail STRING;

    SET v_job_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_message = 'Job started.';

    -- Initialize job run log
    INSERT INTO `my_dataset.job_run_log` (run_id, job_id, start_time, status, message)
    VALUES (v_job_run_id, p_JobKennung, v_start_time, v_status, v_message);

    BEGIN
        -- 1. Validate input parameters
        IF p_JobKennung IS NULL OR LENGTH(TRIM(p_JobKennung)) = 0 THEN
            SET v_status = 'FAILURE';
            SET v_message = 'Parameter p_JobKennung is missing or empty.';
            RAISE USING MESSAGE v_message;
        END IF;

        IF p_EintragsNr IS NULL OR LENGTH(TRIM(p_EintragsNr)) = 0 THEN
            SET v_status = 'FAILURE';
            SET v_message = 'Parameter p_EintragsNr is missing or empty.';
            RAISE USING MESSAGE v_message;
        END IF;

        -- 2. Job management: Deactivate old active jobs and register current job
        -- Deactivate old active jobs for this job_kennung
        UPDATE `my_dataset.job_table`
        SET status = 'INACTIVE', last_update_time = CURRENT_TIMESTAMP()
        WHERE job_id = p_JobKennung AND status = 'ACTIVE';

        -- Register current job
        INSERT INTO `my_dataset.job_table` (job_id, job_name, status, start_time, last_update_time)
        VALUES (p_JobKennung, 'k_ausd_v_ta_p_vertrag', 'ACTIVE', v_start_time, CURRENT_TIMESTAMP());

        -- 3. Execute the data processing SQL script (migrated to a stored procedure)
        CALL `my_dataset.p_ausd_v_ta_p_vertrag_data_process`(p_JobKennung, p_EintragsNr, v_records_processed);

        SET v_status = 'SUCCESS';
        SET v_message = FORMAT('Job completed successfully. Processed %d records.', v_records_processed);

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILURE';
        SET v_error_message = @@error.message;
        SET v_error_detail = @@error.stack_trace;
        SET v_message = 'Job failed: ' || v_error_message;

        -- Log error
        INSERT INTO `my_dataset.job_error_log` (job_id, run_id, error_message, error_detail)
        VALUES (p_JobKennung, v_job_run_id, v_error_message, v_error_detail);

    END;

    SET v_end_time = CURRENT_TIMESTAMP();

    -- Update job run log with final status
    UPDATE `my_dataset.job_run_log`
    SET
        end_time = v_end_time,
        status = v_status,
        records_processed = v_records_processed,
        message = v_message
    WHERE run_id = v_job_run_id;

    -- Update main job_table status if job failed or completed
    IF v_status = 'SUCCESS' THEN
        UPDATE `my_dataset.job_table`
        SET status = 'COMPLETED', end_time = v_end_time, last_update_time = CURRENT_TIMESTAMP()
        WHERE job_id = p_JobKennung AND status = 'ACTIVE';
    ELSE
        UPDATE `my_dataset.job_table`
        SET status = 'FAILED', end_time = v_end_time, last_update_time = CURRENT_TIMESTAMP()
        WHERE job_id = p_JobKennung AND status = 'ACTIVE';
    END IF;

    IF v_status = 'FAILURE' THEN
        RAISE USING MESSAGE v_message;
    END IF;

END;