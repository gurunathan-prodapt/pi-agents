--
-- BigQuery Stored Procedure: your_gcp_project.your_bq_dataset.r_ausd_vertrag_control
-- Migrates orchestration logic from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
--
CREATE OR REPLACE PROCEDURE your_gcp_project.your_bq_dataset.r_ausd_vertrag_control(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    -- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
    -- This procedure replaces the KornShell script's control flow, parameter handling,
    -- job status management, and orchestration of the core SQL logic.

    DECLARE v_error_message STRING;
    DECLARE v_records_processed INT64;

    -- Parameter Validation (replaces h_alis_parameter.ksh and pruefeParameterGesetzt)
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_error_message = 'Parameter p_JobKennung must not be empty.';
        INSERT INTO your_gcp_project.your_bq_dataset.error_log (error_ts, error_nr, error_arg, procedure_name)
        VALUES (CURRENT_DATETIME(), 1001, v_error_message, 'r_ausd_vertrag_control');
        RAISE BQ.ABORT_TRANSACTION(v_error_message);
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_error_message = 'Parameter p_EintragsNr must not be empty.';
        INSERT INTO your_gcp_project.your_bq_dataset.error_log (error_ts, error_nr, error_arg, procedure_name)
        VALUES (CURRENT_DATETIME(), 1002, v_error_message, 'r_ausd_vertrag_control');
        RAISE BQ.ABORT_TRANSACTION(v_error_message);
    END IF;

    -- Job Deactivation Logic (replaces part of k_ausd_v_ta_bp_ref.ksh job control)
    -- Deactivate any previously active jobs with the same JobKennung but different EintragsNr
    UPDATE your_gcp_project.your_bq_dataset.job_table
    SET
        active_flag = FALSE,
        end_time = CURRENT_DATETIME(),
        status = 'DEACTIVATED',
        message = 'Deactivated by new job instance'
    WHERE
        job_kennung = p_JobKennung
        AND eintrags_nr <> p_EintragsNr
        AND active_flag = TRUE;

    -- Mark current job as RUNNING or insert new entry
    MERGE INTO your_gcp_project.your_bq_dataset.job_table AS T
    USING (SELECT p_JobKennung AS job_kennung, p_EintragsNr AS eintrags_nr) AS S
    ON T.job_kennung = S.job_kennung AND T.eintrags_nr = S.eintrags_nr
    WHEN MATCHED THEN
        UPDATE SET
            active_flag = TRUE,
            start_time = CURRENT_DATETIME(),
            end_time = NULL,
            status = 'RUNNING',
            message = 'Job started'
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintrags_nr, active_flag, start_time, status, message)
        VALUES (S.job_kennung, S.eintrags_nr, TRUE, CURRENT_DATETIME(), 'RUNNING', 'Job started');

    -- Execute Core Business Logic (replaces invocation of d_ausd_v_ta_bp_ref.sql via starteSQLSkript/sqlplus)
    BEGIN
        CALL your_gcp_project.your_bq_dataset.d_ausd_v_ta_bp_ref(v_records_processed);

        -- Record the result of the core business logic (replaces temporary file usage)
        INSERT INTO your_gcp_project.your_bq_dataset.job_result (job_kennung, eintrags_nr, record_count, created_ts)
        VALUES (p_JobKennung, p_EintragsNr, v_records_processed, CURRENT_DATETIME());

        -- Update job status to COMPLETED
        UPDATE your_gcp_project.your_bq_dataset.job_table
        SET
            active_flag = FALSE,
            end_time = CURRENT_DATETIME(),
            status = 'COMPLETED',
            message = 'Job completed successfully'
        WHERE
            job_kennung = p_JobKennung
            AND eintrags_nr = p_EintragsNr;

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = CONCAT('Error executing d_ausd_v_ta_bp_ref: ', @@error.message);
        INSERT INTO your_gcp_project.your_bq_dataset.error_log (error_ts, error_nr, error_arg, procedure_name)
        VALUES (CURRENT_DATETIME(), 1003, v_error_message, 'r_ausd_vertrag_control');

        -- Update job status to FAILED
        UPDATE your_gcp_project.your_bq_dataset.job_table
        SET
            active_flag = FALSE,
            end_time = CURRENT_DATETIME(),
            status = 'FAILED',
            message = v_error_message
        WHERE
            job_kennung = p_JobKennung
            AND eintrags_nr = p_EintragsNr;

        RAISE BQ.ABORT_TRANSACTION(v_error_message);
    END;

END;