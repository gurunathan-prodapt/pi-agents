--
-- Target BigQuery Stored Procedure project.dataset.r_ausd_v_ta_inv_assign
-- This procedure encapsulates the logic from:
-- - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
-- - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh
-- - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_inv_assign.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
--
CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_v_ta_inv_assign(
    p_JobKennung STRING,
    p_EintragsNr INT64
)
BEGIN
    DECLARE v_job_start_time TIMESTAMP;
    DECLARE v_job_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_record_count INT64;
    DECLARE v_datum TIMESTAMP;
    DECLARE v_log_id STRING;

    -- Generate a unique log ID for this execution
    SET v_log_id = GENERATE_UUID();

    -- Initialize job start time and log the beginning of the job
    SET v_job_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_record_count = 0;
    SET v_error_message = NULL; -- Initialize error message

    INSERT INTO project.dataset.job_log (
        log_id, job_name, job_kennung, eintrags_nr, start_time, status, log_timestamp
    )
    VALUES (
        v_log_id,
        'r_ausd_v_ta_inv_assign',
        p_JobKennung,
        p_EintragsNr,
        v_job_start_time,
        v_status,
        CURRENT_TIMESTAMP()
    );

    -- Basic parameter validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_status = 'FAILED';
        SET v_error_message = 'Parameter p_JobKennung cannot be NULL or empty.';
        -- Raise an error to stop execution
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Main logic block with error handling
    BEGIN
        -- Determine the cutoff date (v_datum) from the dwtk_meldungen table
        SELECT MAX(timecreated)
        INTO v_datum
        FROM project.dataset.dwtk_meldungen
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';

        -- If no valid cutoff date is found, raise an error
        IF v_datum IS NULL THEN
            SET v_status = 'FAILED';
            SET v_error_message = 'Cutoff date (v_datum) could not be determined from project.dataset.dwtk_meldungen for job_kennung ''BERT_DROP_TEMP_TABLE''.';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
        END IF;

        -- Truncate the target table to ensure a fresh load
        TRUNCATE TABLE project.dataset.sof_ta_inv_assign;

        -- Insert data into the target table with filtering conditions
        INSERT INTO project.dataset.sof_ta_inv_assign (
            assignment_id, insert_at, modified_at, valid_from, valid_to, is_production, some_value
        )
        SELECT
            ia.assignment_id,
            ia.insert_at,
            ia.modified_at,
            ia.valid_from,
            ia.valid_to,
            ia.is_production,
            ia.some_value
        FROM project.dataset.cds_ta_inv_assignment AS ia
        WHERE
            ia.insert_at <= v_datum -- Filter for records inserted on or before v_datum
            AND (ia.modified_at IS NULL OR ia.modified_at > v_datum) -- Filter for records modified after v_datum or never modified
            AND ia.valid_from <= v_datum -- Filter for records valid from on or before v_datum
            AND (ia.valid_to IS NULL OR ia.valid_to > v_datum) -- Filter for records valid until after v_datum or indefinitely
            AND ia.is_production = 1; -- Filter for production records

        -- Capture the number of inserted records using @@ROW_COUNT
        SET v_record_count = @@ROW_COUNT;

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = @@ERROR.MESSAGE; -- Capture the BigQuery error message
    END;

    -- Finalize job end time
    SET v_job_end_time = CURRENT_TIMESTAMP();

    -- Update the job_log with completion details
    UPDATE project.dataset.job_log
    SET
        end_time = v_job_end_time,
        status = v_status,
        record_count = v_record_count,
        error_message = v_error_message,
        log_timestamp = CURRENT_TIMESTAMP()
    WHERE log_id = v_log_id;

END;