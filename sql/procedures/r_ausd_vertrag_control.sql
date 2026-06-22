-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control` (
    IN p_job_kennung STRING,
    IN p_eintragsnr STRING,
    IN p_carmen_project STRING,
    IN p_carmen_dataset STRING,
    IN p_as_of_date DATE DEFAULT NULL
)
BEGIN
    DECLARE v_run_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'FAILED';
    DECLARE v_error_message STRING;
    DECLARE v_processed_records INT64 DEFAULT 0;
    DECLARE v_log_details JSON;

    -- Generate a unique run_id for this execution
    SET v_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Initialize log entry
    INSERT INTO `project.dataset.job_audit` (
        job_id, run_id, start_time, status, job_kennung_param, eintragsnr_param, log_details
    )
    VALUES (
        'k_ausd_v_ta_period', v_run_id, v_start_time, 'RUNNING', p_job_kennung, p_eintragsnr,
        TO_JSON(STRUCT('Starting job execution' AS message))
    );

    BEGIN
        -- Parameter validation (simplified - original script had more complex sourcing)
        IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
            RAISE EXCEPTION 'JobKennung (p_job_kennung) cannot be NULL or empty.';
        END IF;

        IF p_eintragsnr IS NULL OR p_eintragsnr = '' THEN
            RAISE EXCEPTION 'EintragsNr (p_eintragsnr) cannot be NULL or empty.';
        END IF;

        -- Call the data transformation stored procedure
        CALL `project.dataset.d_ausd_v_ta_period`(
            p_job_kennung => p_job_kennung,
            p_target_table_name => 'sof$ta_period', -- Target table for this job
            p_carmen_project => p_carmen_project,
            p_carmen_dataset => p_carmen_dataset,
            p_as_of_date => p_as_of_date
        );

        -- Capture output (row count from the previous CALL, assuming it returns data via SELECT)
        -- In BigQuery, @@row_count refers to the last DML statement.
        -- If d_ausd_v_ta_period returns a SELECT, we need to capture that.
        -- Assuming d_ausd_v_ta_period returns results in a SELECT statement at the end,
        -- we can retrieve them here if the procedure is called in a specific way or if a temp table is used.
        -- For simplicity, let's assume the CALL sets @@row_count or passes it back via a parameter.
        -- If `d_ausd_v_ta_period` does not use an OUT parameter, this part would need adjustment.
        -- The previous tool call output suggests a SELECT statement as return, which requires
        -- a different calling pattern in BQ. For now, let's assume direct call and `@@row_count` captures.
        -- It's more robust to pass results via OUT parameters or temp tables for inter-procedure communication.
        -- Re-reading the `d_ausd_v_ta_period` output, it explicitly does `SELECT 'OK' ... v_rows_inserted AS rows_inserted;`
        -- This output needs to be captured.

        -- To capture the result of the CALL:
        DECLARE result STRUCT<status STRING, as_of_date DATE, target_table STRING, rows_inserted INT64>;
        FOR record IN (
            SELECT * FROM TABLE(
                `project.dataset.d_ausd_v_ta_period`(
                    p_job_kennung => p_job_kennung,
                    p_target_table_name => 'sof$ta_period',
                    p_carmen_project => p_carmen_project,
                    p_carmen_dataset => p_carmen_dataset,
                    p_as_of_date => p_as_of_date
                )
            )
        ) DO
            SET result = record;
        END FOR;

        IF result.status = 'OK' THEN
            SET v_status = 'SUCCESS';
            SET v_processed_records = result.rows_inserted;
            SET v_log_details = TO_JSON(STRUCT(
                'Job completed successfully' AS message,
                result.as_of_date AS as_of_date,
                result.target_table AS target_table,
                result.rows_inserted AS rows_inserted
            ));
        ELSE
            -- This branch should ideally be caught by the outer EXCEPTION handler if `d_ausd_v_ta_period` raises an exception.
            -- If it returns 'ERROR' status without raising, we handle it here.
            SET v_error_message = 'Data transformation procedure returned an error.';
            SET v_log_details = TO_JSON(STRUCT(
                'Data transformation procedure returned an error',
                'Procedure status: ' || result.status AS detail_message
            ));
        END IF;

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_log_details = TO_JSON(STRUCT(
            'Job failed due to exception' AS message,
            v_error_message AS error_detail,
            'SQLSTATE: ' || @@error.sqlstate AS sql_state,
            'SQLCODE: ' || @@error.code AS sql_code
        ));
    END;

    -- Finalize log entry
    UPDATE `project.dataset.job_audit`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = v_status,
        processed_records = v_processed_records,
        error_message = v_error_message,
        log_details = v_log_details
    WHERE run_id = v_run_id;

    -- Return final status
    SELECT
        v_run_id AS run_id,
        v_status AS status,
        v_processed_records AS processed_records,
        v_error_message AS error_message;

END;