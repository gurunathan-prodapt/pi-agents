-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh

CREATE OR REPLACE PROCEDURE `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(
    IN p_stichtag_in STRING,    -- Corresponds to -s parameter for stichtag (reference date)
    IN p_log_level_in STRING,   -- Corresponds to -l parameter for log level
    IN p_show_help BOOL         -- Corresponds to -h parameter for help
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'sp_vertragsdatenabgleich';
    DECLARE v_job_version STRING DEFAULT '1.0';
    DECLARE v_job_entry_no INT64;
    DECLARE v_log_file_name STRING;
    DECLARE v_stichtag STRING;
    DECLARE v_stichtag_format STRING DEFAULT 'YYYYMMDD'; -- Default format, can be inferred or passed
    DECLARE v_log_level STRING DEFAULT 'INFO';
    DECLARE v_error_message STRING;
    DECLARE v_error_stack STRING;
    DECLARE v_error_no INT64 DEFAULT 0;

    -- Handle help request (corresponds to -h in ksh)
    IF p_show_help THEN
        SELECT 'Usage: CALL `PROJECT_ID.DATASET_ID.sp_vertragsdatenabgleich`(p_stichtag_in => "YYYYMMDD", p_log_level_in => "INFO|DEBUG|ERROR", p_show_help => FALSE)' AS Help_Message;
        SELECT '  p_stichtag_in: The reference date for the job (e.g., "20231026"). Defaults to current date if NULL.' AS Help_Detail;
        SELECT '  p_log_level_in: The desired logging level ("INFO", "DEBUG", "ERROR"). Defaults to "INFO".' AS Help_Detail;
        SELECT '  p_show_help: Set to TRUE to display this help message and exit.' AS Help_Detail;
        RETURN;
    END IF;

    -- Initialize parameters (corresponds to getopts and variable assignments in ksh)
    SET v_stichtag = IFNULL(p_stichtag_in, FORMAT_DATE('%Y%m%d', CURRENT_DATE()));
    SET v_log_level = IFNULL(p_log_level_in, 'INFO');

    -- Get a unique job entry number for this run (simulates DWMSG_ErmittleNr)
    -- This assumes job_entry_no is a sequence per job_name.
    SET v_job_entry_no = (SELECT IFNULL(MAX(job_entry_no), 0) + 1 FROM `PROJECT_ID.DATASET_ID.job_audit_log` WHERE job_name = v_job_name);
    
    -- Generate conceptual log file name (simulating DWMSG_Logdateiname)
    -- In BigQuery, logs are stored in job_audit_log table, so this is for descriptive purposes.
    SET v_log_file_name = CONCAT('BQ_LOG_', v_job_name, '_', CAST(v_job_entry_no AS STRING), '_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()), '.log');

    -- Log job start event (simulates DWMSG_ErzeugeEintrag for job start)
    INSERT INTO `PROJECT_ID.DATASET_ID.job_audit_log` (
        job_name,
        job_version,
        job_entry_no,
        log_file_name,
        event_type,
        event_message,
        stichtag,
        stichtag_format,
        event_ts
    )
    VALUES (
        v_job_name,
        v_job_version,
        v_job_entry_no,
        v_log_file_name,
        'START',
        CONCAT('Job started with stichtag: ', v_stichtag, ', log_level: ', v_log_level),
        v_stichtag,
        v_stichtag_format,
        CURRENT_TIMESTAMP()
    );

    -- Update or insert job control entry with 'RUNNING' status (simulates DWMSG_SetzeStichtagInfo and initial status)
    MERGE `PROJECT_ID.DATASET_ID.job_control` AS T
    USING (SELECT v_job_name AS job_name_key) AS S
    ON T.job_name = S.job_name_key
    WHEN MATCHED THEN
        UPDATE SET
            job_entry_no = v_job_entry_no,
            job_status = 'RUNNING',
            stichtag = v_stichtag,
            stichtag_format = v_stichtag_format,
            updated_ts = CURRENT_TIMESTAMP(),
            status_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_name, job_entry_no, job_status, stichtag, stichtag_format, updated_ts, status_ts)
        VALUES (v_job_name, v_job_entry_no, 'RUNNING', v_stichtag, v_stichtag_format, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    -- Main logic block with error handling (corresponds to `set -e` and trap ERR in ksh)
    BEGIN
        -- Call the core kernel stored procedure (corresponds to executing k_ausd_v_ta_vvl_upgrade.ksh)
        CALL `PROJECT_ID.DATASET_ID.sp_k_ausd_v_ta_vvl_upgrade`(v_job_name, v_job_entry_no, v_stichtag, v_stichtag_format);

        -- Log job completion event (simulates DWMSG_SetzeStatusOK and final log message)
        INSERT INTO `PROJECT_ID.DATASET_ID.job_audit_log` (
            job_name,
            job_entry_no,
            event_type,
            event_message,
            stichtag,
            stichtag_format,
            event_ts
        )
        VALUES (
            v_job_name,
            v_job_entry_no,
            'FINISH',
            'Job completed successfully.',
            v_stichtag,
            v_stichtag_format,
            CURRENT_TIMESTAMP()
        );

        -- Update job control entry with 'OK' status
        MERGE `PROJECT_ID.DATASET_ID.job_control` AS T
        USING (SELECT v_job_name AS job_name_key) AS S
        ON T.job_name = S.job_name_key
        WHEN MATCHED THEN
            UPDATE SET
                job_entry_no = v_job_entry_no,
                job_status = 'OK',
                updated_ts = CURRENT_TIMESTAMP(),
                status_ts = CURRENT_TIMESTAMP();

    EXCEPTION WHEN ERROR THEN
        -- Error handling (simulates DWMSG_Fehlerbehandlung)
        SET v_error_message = @@error.message;
        SET v_error_stack = @@error.stack_trace;
        SET v_error_no = 1; -- Generic error code for unhandled exceptions

        -- Log error event
        INSERT INTO `PROJECT_ID.DATASET_ID.job_audit_log` (
            job_name,
            job_entry_no,
            event_type,
            error_no,
            error_arg,
            event_message,
            stichtag,
            stichtag_format,
            event_ts
        )
        VALUES (
            v_job_name,
            v_job_entry_no,
            'ERROR',
            v_error_no,
            v_error_stack,
            CONCAT('Job failed: ', v_error_message),
            v_stichtag,
            v_stichtag_format,
            CURRENT_TIMESTAMP()
        );

        -- Update job control entry with 'FAILED' status
        MERGE `PROJECT_ID.DATASET_ID.job_control` AS T
        USING (SELECT v_job_name AS job_name_key) AS S
        ON T.job_name = S.job_name_key
        WHEN MATCHED THEN
            UPDATE SET
                job_entry_no = v_job_entry_no,
                job_status = 'FAILED',
                updated_ts = CURRENT_TIMESTAMP(),
                status_ts = CURRENT_TIMESTAMP();

        -- Re-raise the error to propagate it to the caller or external orchestrator
        RAISE;
    END;

END;