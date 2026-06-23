-- BigQuery Stored Procedure: sp_initial_befuellung_vertrags_cache_fos
-- Replaces legacy KornShell wrapper script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
--
-- This procedure orchestrates the initial provisioning and snapshot extraction
-- of the contract cache for the Forderungsscoring (FOS) system.
-- It handles parameter parsing, defaulting, validation, logging, and error management.
CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`(
    IN p_stichtag_str STRING,           -- Optional: Reference date in 'DDMMYYYY' format (e.g., '31122023')
    IN p_wiederanlaufWert_input INT64   -- Optional: Restart value for DWH_VERTRAG_ID threshold
)
BEGIN
    DECLARE v_job_kennung STRING DEFAULT 'r_ausd_geschaeftspartner';
    DECLARE v_job_nr INT64;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_job_status STRING DEFAULT 'STARTED';
    DECLARE v_error_message STRING;

    -- Generate a new job_nr
    SELECT COALESCE(MAX(job_nr), 0) + 1 INTO v_job_nr
    FROM `project_id.dataset_id.job_control`;

    -- Default and parse p_wiederanlaufWert
    SET v_wiederanlaufWert = COALESCE(p_wiederanlaufWert_input, 0);

    -- Default and parse p_stichtag
    IF p_stichtag_str IS NULL OR TRIM(p_stichtag_str) = '' THEN
        SET v_stichtag = CURRENT_DATE();
    ELSE
        BEGIN
            SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_str);
        EXCEPTION WHEN ERROR THEN
            SET v_job_status = 'FAILED';
            SET v_error_message = FORMAT("ERROR: Invalid Stichtag format: '%s'. Expected DDMMYYYY.", p_stichtag_str);
            INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
            VALUES (v_job_nr, v_job_kennung, 'ERROR', v_error_message, CURRENT_TIMESTAMP());
            INSERT INTO `project_id.dataset_id.job_control` (job_nr, job_kennung, stichtag, resume_value, status, created_at, finished_at)
            VALUES (v_job_nr, v_job_kennung, NULL, v_wiederanlaufWert, v_job_status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
            RAISE USING MESSAGE = v_error_message;
        END;
    END IF;

    -- Validate Stichtag (should not be NULL after parsing/defaulting)
    IF v_stichtag IS NULL THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = 'ERROR: Stichtag could not be determined or is invalid.';
        INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
        VALUES (v_job_nr, v_job_kennung, 'ERROR', v_error_message, CURRENT_TIMESTAMP());
        INSERT INTO `project_id.dataset_id.job_control` (job_nr, job_kennung, stichtag, resume_value, status, created_at, finished_at)
        VALUES (v_job_nr, v_job_kennung, NULL, v_wiederanlaufWert, v_job_status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        RAISE USING MESSAGE = v_error_message;
    END IF;

    -- Initialize job_control entry
    INSERT INTO `project_id.dataset_id.job_control` (job_nr, job_kennung, stichtag, resume_value, status, created_at)
    VALUES (v_job_nr, v_job_kennung, v_stichtag, v_wiederanlaufWert, v_job_status, CURRENT_TIMESTAMP());

    INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
    VALUES (v_job_nr, v_job_kennung, 'INFO', FORMAT("Job started with Stichtag: %T, Wiederanlaufwert: %d", v_stichtag, v_wiederanlaufWert), CURRENT_TIMESTAMP());

    BEGIN
        -- Call the core business logic procedure
        CALL `project_id.dataset_id.sp_ausd_geschaeftspartner`(v_job_nr, v_job_kennung, v_stichtag, v_wiederanlaufWert);

        -- Update job status to SUCCESS
        SET v_job_status = 'SUCCESS';
        INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
        VALUES (v_job_nr, v_job_kennung, 'INFO', 'Job completed successfully.', CURRENT_TIMESTAMP());

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = @@error.message;
        INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
        VALUES (v_job_nr, v_job_kennung, 'ERROR', FORMAT("Job failed: %s", v_error_message), CURRENT_TIMESTAMP());
        -- Propagate the error after logging
        RAISE USING MESSAGE = v_error_message;

    FINALLY
        -- Always update the job_control record with final status and finished_at timestamp
        UPDATE `project_id.dataset_id.job_control`
        SET
            status = v_job_status,
            finished_at = CURRENT_TIMESTAMP()
        WHERE
            job_nr = v_job_nr;
    END;

END;