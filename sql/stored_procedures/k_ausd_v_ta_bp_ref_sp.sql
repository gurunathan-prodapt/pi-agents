-- BigQuery Stored Procedure for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Replaces k_ausd_v_ta_bp_ref.ksh orchestration and d_ausd_v_ta_bp_ref.sql data transformation.
--
-- This stored procedure handles parameter parsing, validation, job state management,
-- date determination, data truncation, and the core INSERT...SELECT data transformation.
-- It logs job status, record counts, and errors to dedicated BigQuery tables.

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_bp_ref_sp`(
    p_JobKennung STRING,
    p_EintragsNr INT64
)
BEGIN
    DECLARE v_datum STRING;
    DECLARE v_records INT64;
    DECLARE job_start_timestamp TIMESTAMP;
    DECLARE job_end_timestamp TIMESTAMP;
    DECLARE error_message STRING;
    DECLARE active_jobs_count INT64;

    SET job_start_timestamp = CURRENT_TIMESTAMP();

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET error_message = 'Parameter p_JobKennung cannot be NULL or empty.';
        INSERT INTO `project.dataset.job_error_log` (job_id, entry_number, error_timestamp, error_message, script_name, log_level)
        VALUES ('UNKNOWN', COALESCE(p_EintragsNr, -1), CURRENT_TIMESTAMP(), error_message, 'k_ausd_v_ta_bp_ref_sp', 'ERROR');
        RAISE;
    END IF;

    IF p_EintragsNr IS NULL THEN
        SET error_message = 'Parameter p_EintragsNr cannot be NULL.';
        INSERT INTO `project.dataset.job_error_log` (job_id, entry_number, error_timestamp, error_message, script_name, log_level)
        VALUES (p_JobKennung, -1, CURRENT_TIMESTAMP(), error_message, 'k_ausd_v_ta_bp_ref_sp', 'ERROR');
        RAISE;
    END IF;

    -- Deactivate old 'RUNNING' jobs for the same job_id (older than 24 hours)
    -- This handles cases where a previous run might have failed to update its status.
    UPDATE `project.dataset.job_control`
    SET
        status = 'FAILED',
        end_timestamp = CURRENT_TIMESTAMP()
    WHERE
        job_id = p_JobKennung
        AND status = 'RUNNING'
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_timestamp, HOUR) >= 24;

    -- Check for actively running jobs with the same JobKennung
    -- If an active job exists, this run is ignored (as per legacy ksh script logic).
    SELECT COUNT(*)
    INTO active_jobs_count
    FROM `project.dataset.job_control`
    WHERE
        job_id = p_JobKennung
        AND status = 'RUNNING';

    IF active_jobs_count > 0 THEN
        -- Log that an active job was found and this run is being ignored.
        -- This mimics the "aktive Jobs werden ignoriert" behavior of the ksh script.
        INSERT INTO `project.dataset.job_error_log` (job_id, entry_number, error_timestamp, error_message, script_name, log_level)
        VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), 'Active job found for JobKennung. Current run ignored.', 'k_ausd_v_ta_bp_ref_sp', 'INFO');
        RETURN; -- Exit gracefully without further processing
    END IF;

    -- Insert initial job control entry for the current run
    INSERT INTO `project.dataset.job_control` (job_id, entry_number, start_timestamp, status, script_name)
    VALUES (p_JobKennung, p_EintragsNr, job_start_timestamp, 'RUNS_B_OK', 'k_ausd_v_ta_bp_ref_sp'); -- "RUNS_B_OK" for tracking progress, will be updated to RUNNING in the future.

    BEGIN
        -- Determine v_datum (processing date)
        -- Mimics: SELECT NVL(TO_CHAR(MAX(m.timecreated),...),...) FROM isbert_schema.dwtk_meldungen
        SELECT FORMAT_DATE('%Y%m%d', COALESCE(MAX(timecreated), CURRENT_DATE()))
        INTO v_datum
        FROM `project.dataset.dwtk_meldungen`;

        -- Truncate target table
        -- Mimics: begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bp_ref'); end;
        TRUNCATE TABLE `project.dataset.sof_ta_bp_ref`;

        -- Core data transformation: INSERT INTO ... SELECT FROM
        -- Migrated from d_ausd_v_ta_bp_ref.sql
        INSERT INTO `project.dataset.sof_ta_bp_ref` (cntrct_cp2_id, bp_id)
        SELECT
            br.cntrct_cp2_id,
            br.bp_id
        FROM
            `project.dataset.cds_ta_bp_ref` AS br
        WHERE
            br.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
            AND (br.modified_at IS NULL OR br.modified_at > PARSE_DATE('%Y%m%d', v_datum))
            AND br.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
            AND (br.valid_to IS NULL OR br.valid_to > PARSE_DATE('%Y%m%d', v_datum))
            AND br.is_production = 1
            AND br.bp_ref_ty = 4;

        -- Get record count
        -- Mimics: eval "v_records=`cat $tmpFile`"
        SELECT COUNT(*) INTO v_records FROM `project.dataset.sof_ta_bp_ref`;

        -- Final Success Update for job control
        SET job_end_timestamp = CURRENT_TIMESTAMP();
        UPDATE `project.dataset.job_control`
        SET
            end_timestamp = job_end_timestamp,
            status = 'SUCCESS',
            records_processed = v_records,
            processing_date = PARSE_DATE('%Y%m%d', v_datum)
        WHERE
            job_id = p_JobKennung
            AND entry_number = p_EintragsNr
            AND start_timestamp = job_start_timestamp; -- Ensures uniqueness for this specific run instance

    EXCEPTION WHEN ERROR THEN
        SET error_message = @@error.message;
        SET job_end_timestamp = CURRENT_TIMESTAMP();

        -- Update job control with FAILED status
        UPDATE `project.dataset.job_control`
        SET
            end_timestamp = job_end_timestamp,
            status = 'FAILED',
            records_processed = 0,
            processing_date = NULL
        WHERE
            job_id = p_JobKennung
            AND entry_number = p_EintragsNr
            AND start_timestamp = job_start_timestamp;

        -- Log the error to job_error_log table
        INSERT INTO `project.dataset.job_error_log` (job_id, entry_number, error_timestamp, error_message, script_name, log_level)
        VALUES (p_JobKennung, p_EintragsNr, job_end_timestamp, error_message, 'k_ausd_v_ta_bp_ref_sp', 'ERROR');

        RAISE; -- Re-raise the error to signal failure to any calling orchestration
    END;
END;