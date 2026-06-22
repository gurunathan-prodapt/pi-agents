-- BigQuery Stored Procedure for Orchestration
-- Replaces legacy KornShell script vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

CREATE OR REPLACE PROCEDURE `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`(
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    OUT records_processed INT64
)
BEGIN
    DECLARE job_run_uuid STRING;
    DECLARE start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Generate a unique ID for this job run
    SET job_run_uuid = GENERATE_UUID();

    -- Log start of the job
    INSERT INTO `your-gcp-project.isbert_schema.job_run_log` (run_id, job_id, start_time, status, parameters)
    VALUES (job_run_uuid, 'k_ausd_v_ta_c_bfc', start_timestamp, 'RUNNING', TO_JSON(STRUCT(p_job_kennung, p_eintrags_nr)));

    -- Parameter Validation
    -- Corresponds to 'pruefeParameterGesetzt' calls in KSH
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        RAISE_ERROR(ERROR_MESSAGE => 'FEHLER: Parameter p_job_kennung must be set.');
    END IF;
    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        RAISE_ERROR(ERROR_MESSAGE => 'FEHLER: Parameter p_eintrags_nr must be set.');
    END IF;

    BEGIN
        -- Call the data transformation stored procedure
        -- Corresponds to 'starteSQLSkript' call in KSH
        CALL `your-gcp-project.isbert_schema.d_ausd_v_ta_c_bfc_sp`();

        -- Get number of processed records (from the target table)
        -- Corresponds to 'eval "v_records=`cat $tmpFile`"' in KSH
        SELECT COUNT(1) INTO records_processed FROM `your-gcp-project.isbert_schema.sof$ta_c_bfc`;

        -- Log successful completion
        UPDATE `your-gcp-project.isbert_schema.job_run_log`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'SUCCEEDED'
        WHERE run_id = job_run_uuid;

    EXCEPTION WHEN ERROR THEN
        -- Log job error
        INSERT INTO `your-gcp-project.isbert_schema.job_error_log` (job_id, error_message, severity)
        VALUES (p_job_kennung, @@error.message, 'ERROR');

        -- Update job run log with failure status
        UPDATE `your-gcp-project.isbert_schema.job_run_log`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'FAILED'
        WHERE run_id = job_run_uuid;
        RAISE; -- Re-raise the error to propagate
    END;
END;