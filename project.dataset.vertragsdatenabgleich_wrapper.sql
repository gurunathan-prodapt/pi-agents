-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.vertragsdatenabgleich_wrapper`(
    IN p_stichtag STRING,     -- Expected format: DDMMYYYY
    IN p_log_level STRING,    -- Log level, currently not explicitly used in wrapper logic
    IN p_show_help BOOL       -- Flag to show help message
)
OPTIONS(
  description="Wrapper for synchronizing contract data in the ta_period table. Handles parameter parsing, job control, logging, and invokes core logic."
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'r_ausd_v_ta_period.ksh';
    DECLARE v_prog_version STRING DEFAULT '1.0';
    DECLARE v_job_kennung STRING;
    DECLARE v_err_nr INT64 DEFAULT 0;
    DECLARE v_err_arg STRING;
    DECLARE v_dw_eintrags_nr INT64;
    DECLARE v_stichtag_date DATE;
    DECLARE v_current_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_job_status STRING;
    DECLARE v_log_message STRING;

    -- Helper to insert into job_log
    SET v_job_kennung = v_prog_name; -- In original script, JobKennung is often ProgName

    -- Function to log messages
    -- NOTE: In BigQuery, UDFs for DML are not directly supported.
    -- This logic is inlined for clarity or could be a separate helper procedure if called frequently.
    INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
    SELECT v_job_kennung, v_dw_eintrags_nr, 'Job started: ' || v_prog_name || ' Version: ' || v_prog_version, v_current_ts;

    -- Parameter parsing and validation
    IF p_show_help THEN
        SET v_log_message = "Usage: CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_stichtag => 'DDMMYYYY', p_log_level => 'INFO', p_show_help => FALSE);";
        INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
        VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());
        SET v_err_nr = 0; -- Successful help display
        SET v_job_status = 'OK';
        -- Update job_control and exit. Since we don't have DW_EintragsNr yet, this is slightly different.
        -- In BigQuery SP, we'd exit here if help is the only purpose.
        RETURN;
    END IF;

    IF p_stichtag IS NULL OR p_stichtag = '' THEN
        SET v_err_nr = 193; -- Missing parameter value
        SET v_err_arg = '-s';
        SET v_log_message = 'ERROR: Parameter -s (Stichtag) is missing.';
        INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
        VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());
        RAISE BQ.ERROR(v_log_message); -- Immediately raise an error to trigger exception handler
    END IF;

    BEGIN
        SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);
    EXCEPTION WHEN ERROR THEN
        SET v_err_nr = 192; -- Invalid parameter format
        SET v_err_arg = '-s ' || p_stichtag;
        SET v_log_message = 'ERROR: Invalid Stichtag format. Expected DDMMYYYY, got ' || p_stichtag;
        INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
        VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());
        RAISE BQ.ERROR(v_log_message); -- Immediately raise an error
    END;

    -- Determine next job_entry_nr and insert into job_control
    SELECT COALESCE(MAX(job_entry_nr), 0) + 1 INTO v_dw_eintrags_nr FROM `project.dataset.job_control`;

    INSERT INTO `project.dataset.job_control` (job_entry_nr, job_name, script_name, log_file_name, stichtag, stichtag_format, status, created_ts)
    VALUES (v_dw_eintrags_nr, v_job_kennung, v_prog_name, v_job_kennung || '_' || p_stichtag || '.log', v_stichtag_date, '%d%m%Y', 'STARTED', v_current_ts);

    -- Log job start message
    SET v_log_message = 'Job started with DW_EintragsNr: ' || CAST(v_dw_eintrags_nr AS STRING) || ', Stichtag: ' || p_stichtag;
    INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
    VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());

    -- Main job execution block with error handling
    BEGIN
        -- Call the core business logic script
        SET v_log_message = 'Calling core procedure k_ausd_v_ta_period with JobKennung=' || v_job_kennung || ' and DW_EintragsNr=' || CAST(v_dw_eintrags_nr AS STRING);
        INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
        VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());

        CALL `project.dataset.k_ausd_v_ta_period`(v_job_kennung, v_dw_eintrags_nr);

        -- If core procedure returns without error, mark as OK
        SET v_job_status = 'OK';
        SET v_log_message = 'Core procedure k_ausd_v_ta_period completed successfully.';
        INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
        VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());

    EXCEPTION WHEN ERROR THEN
        SET v_err_nr = 1; -- Generic error for core logic failure
        SET v_err_arg = 'k_ausd_v_ta_period';
        SET v_job_status = 'ERROR';
        SET v_log_message = 'ERROR: Core procedure k_ausd_v_ta_period failed. Error: ' || @@error.message;
        INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
        VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());

        INSERT INTO `project.dataset.job_error_log` (job_name, job_entry_nr, error_nr, error_arg, error_message, created_ts)
        VALUES (v_job_kennung, v_dw_eintrags_nr, v_err_nr, v_err_arg, v_log_message, CURRENT_TIMESTAMP());

        -- Re-raise the error to propagate it if needed by orchestrator
        RAISE;
    END;

    -- Update job_control with final status and finished timestamp
    UPDATE `project.dataset.job_control`
    SET
        status = v_job_status,
        finished_ts = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = v_dw_eintrags_nr;

    IF v_job_status = 'OK' THEN
        SET v_log_message = 'Job ' || v_job_kennung || ' finished successfully (DW_EintragsNr: ' || CAST(v_dw_eintrags_nr AS STRING) || ').';
    ELSE
        SET v_log_message = 'Job ' || v_job_kennung || ' finished with ERROR (DW_EintragsNr: ' || CAST(v_dw_eintrags_nr AS STRING) || ').';
    END IF;

    INSERT INTO `project.dataset.job_log` (job_name, job_entry_nr, log_message, created_ts)
    VALUES (v_job_kennung, v_dw_eintrags_nr, v_log_message, CURRENT_TIMESTAMP());

END;