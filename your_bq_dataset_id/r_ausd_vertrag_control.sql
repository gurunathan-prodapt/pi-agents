-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_id.r_ausd_vertrag_control`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING
)
BEGIN
    DECLARE v_processed_records INT64;
    DECLARE v_error_message STRING;
    DECLARE v_job_status STRING DEFAULT 'FAILED';

    -- Start job execution with error handling
    BEGIN
        -- 1. Parameter Validation
        IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
            SET v_error_message = 'Parameter p_JobKennung cannot be NULL or empty.';
            INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_error_log`
                (job_kennung, eintrags_nr, error_timestamp, error_message, severity)
            VALUES
                (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), v_error_message, 'ERROR');
            RAISE BQ.INVALID_ARGUMENT_TYPE(v_error_message);
        END IF;

        IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
            SET v_error_message = 'Parameter p_EintragsNr cannot be NULL or empty.';
            INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_error_log`
                (job_kennung, eintrags_nr, error_timestamp, error_message, severity)
            VALUES
                (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), v_error_message, 'ERROR');
            RAISE BQ.INVALID_ARGUMENT_TYPE(v_error_message);
        END IF;

        -- 2. Deactivate old active jobs and register new job run
        CALL `your_gcp_project_id.your_bq_dataset_id.register_job_start`(p_JobKennung, p_EintragsNr);

        -- 3. Call core data transformation procedure
        CALL `your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_dwh_proc`(v_processed_records);

        SET v_job_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        -- Log the error
        INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_error_log`
            (job_kennung, eintrags_nr, error_timestamp, error_message, severity)
        VALUES
            (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), v_error_message, 'CRITICAL');
        -- Re-raise the error to signal failure to the caller
        RAISE;

    FINALLY
        -- Update job_run_log and job_table with final status
        UPDATE `your_gcp_project_id.your_bq_dataset_id.job_run_log`
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = v_job_status,
            processed_records = v_processed_records
        WHERE
            job_kennung = p_JobKennung
            AND eintrags_nr = p_EintragsNr
            AND status = 'RUNNING'; -- Only update the currently running job entry

        UPDATE `your_gcp_project_id.your_bq_dataset_id.job_table`
        SET is_active = FALSE, last_update_timestamp = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr;

    END;
END;