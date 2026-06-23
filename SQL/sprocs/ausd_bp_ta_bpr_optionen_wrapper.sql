-- BigQuery Stored Procedure: project.dataset.ausd_bp_ta_bpr_optionen_wrapper
-- Replaces legacy KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
    IN p_stichtag_in STRING, -- Stichtag/cutoff date in DDMMYYYY format (optional)
    IN p_wiederanlaufWert_in INT64 -- Wiederanlaufwert/restart value (optional, defaults to 0)
)
BEGIN
    -- Declare variables for internal use
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_script_name STRING DEFAULT 'r_ausd_bp_ta_bpr_optionen.ksh';
    DECLARE v_job_kennung STRING;
    DECLARE v_dw_eintrags_nr INT64;
    DECLARE v_log_file STRING;
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_error_message STRING;

    -- Initialize parameters and apply defaults as per design document
    -- If p_stichtag_in is NULL, default to current system date formatted as DDMMYYYY
    SET v_stichtag = COALESCE(p_stichtag_in, FORMAT_DATE('%d%m%Y', v_sysdate));
    -- If p_wiederanlaufWert_in is NULL, default to 0
    SET v_wiederanlaufWert = COALESCE(p_wiederanlaufWert_in, 0);

    -- Generate a unique job identifier (JobKennung)
    -- This combines the script name with a timestamp for uniqueness, similar to how shell scripts often derive identifiers.
    SET v_job_kennung = CONCAT(REPLACE(v_script_name, '.ksh', ''), '_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', v_start_time, 'Europe/Berlin'));

    -- Determine the next job entry number (DW_EintragsNr) for job tracking
    -- NOTE: In a high-concurrency production environment, this MAX+1 approach is prone to race conditions.
    -- A dedicated sequence table or a different unique identifier generation strategy would be more robust.
    SELECT IFNULL(MAX(job_nr), 0) + 1 INTO v_dw_eintrags_nr FROM `project.dataset.job_control`;

    -- Construct a logical log file name for reference, even if logs are stored in a table
    SET v_log_file = CONCAT(v_job_kennung, '_', v_dw_eintrags_nr, '.log');

    -- Begin a transaction to ensure atomicity of job control and log updates
    BEGIN TRANSACTION;

    -- Insert initial job control entry with 'RUNNING' status
    INSERT INTO `project.dataset.job_control` (
        job_nr, job_kennung, script_name, log_file, stichtag_info, status, created_at, finished_at
    ) VALUES (
        v_dw_eintrags_nr, v_job_kennung, v_script_name, v_log_file, v_stichtag, 'RUNNING', v_start_time, NULL
    );

    -- Log the job start message
    INSERT INTO `project.dataset.job_log` (
        job_nr, job_kennung, log_level, message, created_at
    ) VALUES (
        v_dw_eintrags_nr, v_job_kennung, 'INFO', CONCAT('Job started. Stichtag: ', v_stichtag, ', Wiederanlaufwert: ', v_wiederanlaufWert), CURRENT_TIMESTAMP()
    );

    -- Main execution block with error handling (replaces shell 'trap' mechanism)
    BEGIN
        -- Call the core provisioning stored procedure
        -- This represents the invocation of 'k_ausd_bp_ta_bpr_optionen.ksh' from the original script.
        -- The arguments passed reflect those observed in the legacy script's invocation.
        CALL `project.dataset.k_ausd_bp_ta_bpr_optionen`(
            v_job_kennung,        -- Corresponding to '-j $JobKennung'
            v_stichtag,           -- Corresponding to '-s $p_stichtag'
            v_dw_eintrags_nr,     -- Corresponding to '-f ${DW_EintragsNr}'
            v_wiederanlaufWert    -- Corresponding to '-l ${p_wiederanlaufWert}'
        );

        -- If the core procedure completes successfully, update job status to 'OK'
        UPDATE `project.dataset.job_control`
        SET
            status = 'OK',
            finished_at = CURRENT_TIMESTAMP()
        WHERE
            job_nr = v_dw_eintrags_nr;

        -- Log successful completion
        INSERT INTO `project.dataset.job_log` (
            job_nr, job_kennung, log_level, message, created_at
        ) VALUES (
            v_dw_eintrags_nr, v_job_kennung, 'INFO', 'Job completed successfully.', CURRENT_TIMESTAMP()
        );

        -- Commit the transaction if everything was successful
        COMMIT TRANSACTION;

    EXCEPTION WHEN ERROR THEN
        -- Catch any errors during the execution of the core procedure or subsequent updates
        SET v_error_message = @@error.message;

        -- Update job status to 'FAILED'
        UPDATE `project.dataset.job_control`
        SET
            status = 'FAILED',
            finished_at = CURRENT_TIMESTAMP()
        WHERE
            job_nr = v_dw_eintrags_nr;

        -- Log the error details
        INSERT INTO `project.dataset.job_log` (
            job_nr, job_kennung, log_level, message, created_at
        ) VALUES (
            v_dw_eintrags_nr, v_job_kennung, 'ERROR', CONCAT('Job failed with error: ', v_error_message), CURRENT_TIMESTAMP()
        );

        -- Rollback the transaction to undo any partial changes within this wrapper's transaction
        ROLLBACK TRANSACTION;

        -- Re-raise the error to the caller, mimicking script exit with an error code
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;
END;