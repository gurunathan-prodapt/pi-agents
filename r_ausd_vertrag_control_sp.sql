-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
-- BigQuery Stored Procedure for orchestration and control

CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag_control_sp(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_job_status STRING DEFAULT 'STARTED';
    DECLARE v_error_message STRING;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_finish_ts TIMESTAMP;

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = 'JobKennung parameter cannot be empty.';
        CALL project.dataset.log_error(p_JobKennung, p_EintragsNr, '1001', 'PARAMETER_VALIDATION', v_error_message);
        RAISE USING MESSAGE 'ERROR: ' || v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = 'EintragsNr parameter cannot be empty.';
        CALL project.dataset.log_error(p_JobKennung, p_EintragsNr, '1002', 'PARAMETER_VALIDATION', v_error_message);
        RAISE USING MESSAGE 'ERROR: ' || v_error_message;
    END IF;

    -- Insert job start record
    INSERT INTO project.dataset.job_table (job_kennung, eintrags_nr, tab_name, status, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, 'target_table', v_job_status, v_start_ts);

    BEGIN
        -- Execute the core transformation logic
        -- NOTE: Replace this EXECUTE IMMEDIATE block with the actual invocation
        -- of your migrated SQL, potentially as a separate script or a series of statements.
        -- If d_ausd_v_ta_vvl_dwh_migrated.sql returns a count, capture it.
        EXECUTE IMMEDIATE FORMAT(
            """
            SELECT COUNT(*) FROM project.dataset.target_table; -- Placeholder for actual SQL logic that returns record count
            """
        ); -- Actual core SQL logic would go here. For now, it's a placeholder.

        -- Assuming the transformation logic updates v_records_processed
        -- For example, if it's an INSERT/MERGE, you could capture @@row_count if it were in a different context.
        -- For a true BigQuery equivalent, you might need to run a SELECT COUNT(*) after the DML
        -- or capture the result of the DML if it's part of a scripting block that allows it.
        SET v_records_processed = @@row_count; -- This won't work for the placeholder SELECT. Adjust based on actual DML.

        -- If the actual DML is complex or involves multiple steps, you'll need to capture the count differently.
        -- For example, if the d_ausd_v_ta_vvl_dwh_migrated.sql is another procedure or function that returns the count.

        SET v_job_status = 'COMPLETED';

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = @@error.message;
        -- Log detailed error information
        CALL project.dataset.log_error(p_JobKennung, p_EintragsNr, '9999', 'TRANSFORMATION_ERROR', v_error_message);
        RAISE USING MESSAGE 'Transformation failed: ' || v_error_message;
    END;

    SET v_finish_ts = CURRENT_TIMESTAMP();

    -- Update job completion record
    UPDATE project.dataset.job_table
    SET
        status = v_job_status,
        record_count = v_records_processed,
        finished_ts = v_finish_ts
    WHERE
        job_kennung = p_JobKennung
        AND eintrags_nr = p_EintragsNr
        AND created_ts = v_start_ts; -- Use created_ts to ensure we update the specific job instance

END;

-- Helper procedure for error logging (simulating DWMSG_MeldeFehler)
CREATE OR REPLACE PROCEDURE project.dataset.log_error(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_ErrNr STRING,
    p_ErrArg STRING,
    p_Message STRING
)
BEGIN
    INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, err_nr, err_arg, created_ts, message)
    VALUES (p_JobKennung, p_EintragsNr, p_ErrNr, p_ErrArg, CURRENT_TIMESTAMP(), p_Message);
END;