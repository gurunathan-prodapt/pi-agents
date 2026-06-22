-- BigQuery Stored Procedure for control and orchestration, migrated from k_ausd_v_ta_bp_ref.ksh
-- Legacy Job: k_ausd_v_ta_bp_ref.ksh
CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag_control(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING,
    IN p_stichtag DATE -- Added stichtag as input for the business logic
)
BEGIN
    -- Legacy source: k_ausd_v_ta_bp_ref.ksh
    -- This procedure orchestrates the execution and handles logging and parameter validation.

    DECLARE v_tab_name STRING DEFAULT 'ta_bp_ref';
    DECLARE v_records_processed INT64;
    DECLARE v_error_message STRING;
    DECLARE v_error_code INT64;

    -- Parameter Validation (from pruefeParameterGesetzt in ksh)
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_message = 'FEHLER: JobKennung parameter is missing or empty.';
        INSERT INTO project.dataset.error_log (error_ts, error_code, error_arg, job_kennung, eintrags_nr, script_name, message)
        VALUES (CURRENT_TIMESTAMP(), 193, 'JobKennung', p_job_kennung, p_eintrags_nr, 'r_ausd_vertrag_control', v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SET v_error_message = 'FEHLER: EintragsNr parameter is missing or empty.';
        INSERT INTO project.dataset.error_log (error_ts, error_code, error_arg, job_kennung, eintrags_nr, script_name, message)
        VALUES (CURRENT_TIMESTAMP(), 193, 'EintragsNr', p_job_kennung, p_eintrags_nr, 'r_ausd_vertrag_control', v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- If p_stichtag is not provided, default to current date.
    IF p_stichtag IS NULL THEN
        SET p_stichtag = CURRENT_DATE();
    END IF;

    -- Placeholder for "Job Management Logic" (ignore active jobs, deactivate old jobs)
    -- This logic needs to be implemented based on the definition of "active jobs"
    -- and interaction with a dedicated BigQuery job metadata table.
    -- Example:
    /*
    IF EXISTS (SELECT 1 FROM project.dataset.job_metadata WHERE job_kennung = p_job_kennung AND status = 'ACTIVE') THEN
        SELECT 'INFO: Job ' || p_job_kennung || ' is already active. Ignoring this run.' AS message;
        RETURN;
    END IF;
    -- Logic to deactivate old jobs would go here
    */

    BEGIN
        -- Call the core business logic procedure
        CALL project.dataset.d_ausd_v_ta_bp_ref_logic(p_stichtag, p_job_kennung, p_eintrags_nr);

        -- After successful execution, get the count of processed records from the target table
        SELECT COUNT(*) INTO v_records_processed FROM project.dataset.sof_ta_bp_ref;

        -- Log successful job run
        INSERT INTO project.dataset.job_run_log (run_ts, job_kennung, eintrags_nr, tab_name, records_processed)
        VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, v_tab_name, v_records_processed);

        SELECT 'INFO: Processing finished successfully for JobKennung: ' || p_job_kennung || ', EintragsNr: ' || p_eintrags_nr || ' with ' || v_records_processed || ' records processed.' AS message;

    EXCEPTION WHEN ERROR THEN
        SET v_error_code = @@error.code;
        SET v_error_message = @@error.message;
        -- An error should already be logged by d_ausd_v_ta_bp_ref_logic.
        -- This catch block is for any errors specific to r_ausd_vertrag_control or unhandled by the child procedure.
        -- Check if an error message for this job/entry/date already exists to avoid duplicates.
        IF NOT EXISTS (SELECT 1 FROM project.dataset.error_log WHERE job_kennung = p_job_kennung AND eintrags_nr = p_eintrags_nr AND message LIKE '%' || v_error_message || '%' AND DATE(error_ts) = CURRENT_DATE()) THEN
            INSERT INTO project.dataset.error_log (error_ts, error_code, error_arg, job_kennung, eintrags_nr, script_name, message)
            VALUES (CURRENT_TIMESTAMP(), v_error_code, NULL, p_job_kennung, p_eintrags_nr, 'r_ausd_vertrag_control', 'Unhandled error during control procedure: ' || v_error_message);
        END IF;
        RAISE; -- Re-raise the error to propagate it to the caller (e.g., Airflow)
    END;

END;