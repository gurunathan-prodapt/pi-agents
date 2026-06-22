-- BigQuery Stored Procedure: project.dataset.r_ausd_vertrag_control
-- Replaces the orchestration logic from k_ausd_v_ta_vvl_upgrade.ksh
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh

CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag_control(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_job_run_id STRING;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_processed_records INT64;
    DECLARE v_job_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_code STRING;
    DECLARE v_is_active BOOL;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_job_run_id = GENERATE_UUID();
    SET v_job_status = 'RUNNING';

    -- 1. Parameter Validation
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_code = '193'; -- Notwendiges Argument fehlt
        SET v_error_message = 'Parameter p_JobKennung is missing or empty.';
        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_code, error_message)
        VALUES (COALESCE(p_job_kennung, 'UNKNOWN'), COALESCE(p_eintrags_nr, 'UNKNOWN'), v_error_code, v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SET v_error_code = '193'; -- Notwendiges Argument fehlt
        SET v_error_message = 'Parameter p_EintragsNr is missing or empty.';
        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_code, error_message)
        VALUES (COALESCE(p_job_kennung, 'UNKNOWN'), COALESCE(p_eintrags_nr, 'UNKNOWN'), v_error_code, v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- 2. Job Status Management: Check if this job instance is already active
    SELECT COUNT(1) > 0
    INTO v_is_active
    FROM project.dataset.job_table
    WHERE job_kennung = p_job_kennung
      AND eintrags_nr = p_eintrags_nr
      AND job_status = 'ACTIVE';

    IF v_is_active THEN
        SET v_job_status = 'SKIPPED';
        SET v_error_message = FORMAT('JobKennung: %s, EintragsNr: %s is already active. Skipping execution.', p_job_kennung, p_eintrags_nr);
        INSERT INTO project.dataset.job_run_log (
            job_run_id, job_kennung, eintrags_nr, start_timestamp, end_timestamp, status, processed_records
        ) VALUES (
            v_job_run_id, p_job_kennung, p_eintrags_nr, v_start_timestamp, CURRENT_TIMESTAMP(), v_job_status, NULL
        );
        -- Log to console as well
        SELECT v_error_message AS message;
        RETURN; -- Exit early if already active
    END IF;

    -- If not active, deactivate any previous active entries for this specific job instance
    -- This handles cases where a previous run might have failed to update its status to INACTIVE/COMPLETED
    UPDATE project.dataset.job_table
    SET job_status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_job_kennung
      AND eintrags_nr = p_eintrags_nr
      AND job_status = 'ACTIVE';

    -- Upsert/Insert the current job instance as ACTIVE
    MERGE INTO project.dataset.job_table AS T
    USING (SELECT p_job_kennung AS job_kennung, p_eintrags_nr AS eintrags_nr) AS S
    ON T.job_kennung = S.job_kennung AND T.eintrags_nr = S.eintrags_nr
    WHEN MATCHED THEN
        UPDATE SET T.job_status = 'ACTIVE', T.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintrags_nr, job_status)
        VALUES (S.job_kennung, S.eintrags_nr, 'ACTIVE');

    -- Initial logging of job start
    INSERT INTO project.dataset.job_run_log (
        job_run_id, job_kennung, eintrags_nr, start_timestamp, status
    ) VALUES (
        v_job_run_id, p_job_kennung, p_eintrags_nr, v_start_timestamp, 'RUNNING'
    );

    BEGIN
        -- 3. Execute the core SQL transformation procedure
        CALL project.dataset.d_ausd_v_ta_vvl_upgrade(p_eintrags_nr, p_job_kennung, v_processed_records);

        -- 4. Log completion and update job status
        SET v_job_status = 'COMPLETED';
        UPDATE project.dataset.job_run_log
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_job_status,
            processed_records = v_processed_records
        WHERE job_run_id = v_job_run_id;

        UPDATE project.dataset.job_table
        SET job_status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_job_kennung
          AND eintrags_nr = p_eintrags_nr;

        SELECT FORMAT('JobKennung: %s, EintragsNr: %s completed successfully. Processed records: %d.', p_job_kennung, p_eintrags_nr, v_processed_records) AS message;

    EXCEPTION WHEN ERROR THEN
        -- 5. Error Handling
        SET v_job_status = 'FAILED';
        SET v_error_message = @@error.message;
        SET v_error_code = 'GENERIC_SQL_ERROR'; -- Or parse @@error.message for more specific codes

        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_code, error_message)
        VALUES (p_job_kennung, p_eintrags_nr, v_error_code, v_error_message);

        UPDATE project.dataset.job_run_log
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_job_status
        WHERE job_run_id = v_job_run_id;

        UPDATE project.dataset.job_table
        SET job_status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_job_kennung
          AND eintrags_nr = p_eintrags_nr;

        RAISE USING MESSAGE FORMAT('JobKennung: %s, EintragsNr: %s failed with error: %s', p_job_kennung, p_eintrags_nr, v_error_message);
    END;
END;