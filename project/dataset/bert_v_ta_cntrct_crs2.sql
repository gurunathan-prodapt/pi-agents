-- BigQuery Stored Procedure for the wrapper script
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh

CREATE OR REPLACE PROCEDURE project.dataset.BERT_V_TA_CNTRCT_CRS2(
    IN p_h BOOL,   -- Help flag
    IN p_s DATE,   -- Stichtag (reference date)
    IN p_l STRING  -- Log file name (placeholder for target, not used for actual file)
)
BEGIN
    DECLARE v_ProgName STRING DEFAULT 'BERT_V_TA_CNTRCT_CRS2';
    DECLARE v_ProgVersion STRING DEFAULT '1.0'; -- Placeholder for version
    DECLARE v_JobKennung STRING; -- Will be determined dynamically or passed
    DECLARE v_stichtag_formatted STRING;
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE v_DW_EintragsNr INT64;
    DECLARE v_ErrNr INT64;
    DECLARE v_ErrArg STRING;
    DECLARE v_LogDatei STRING;
    DECLARE v_Name_Kernskript STRING DEFAULT 'k_ausd_v_ta_cntrct_crs2';
    DECLARE v_status STRING;

    -- --- Help Parameter Handling (-h) ---
    IF p_h THEN
        SELECT 'Usage: CALL project.dataset.BERT_V_TA_CNTRCT_CRS2(p_h => [TRUE|FALSE], p_s => ''YYYY-MM-DD'', p_l => ''log_name'')' AS HelpMessage;
        SELECT '  -h: Display this help message.' AS HelpMessage;
        SELECT '  -s: Stichtag (Reference date in YYYY-MM-DD format).' AS HelpMessage;
        SELECT '  -l: Logfile name (optional, currently not used for file output).' AS HelpMessage;
        RETURN;
    END IF;

    -- --- Parameter Validation ---
    IF p_s IS NULL THEN
        SET v_ErrNr = 1; -- Example error code
        SET v_ErrArg = '-s (Stichtag) parameter is missing.';
        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_nr, error_arg, error_message, created_ts)
        VALUES ('N/A', 0, v_ErrNr, v_ErrArg, 'Missing required parameter.', CURRENT_TIMESTAMP());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Missing required parameter: -s';
    END IF;

    -- Derive JobKennung from program name
    SET v_JobKennung = v_ProgName;
    SET v_stichtag_formatted = FORMAT_DATE('%Y%m%d', p_s);

    -- --- Determine next DW_EintragsNr ---
    -- Assuming eintrags_nr is an auto-incrementing-like sequence managed by the control table
    SELECT IFNULL(MAX(eintrags_nr), 0) + 1 INTO v_DW_EintragsNr FROM project.dataset.job_control;

    -- Set LogDatei (even if not used for actual file redirection in BQ)
    SET v_LogDatei = IFNULL(p_l, 'bert_v_ta_cntrct_crs2_' || v_stichtag_formatted || '.log');

    -- --- Log start of job and insert initial entry into job_control ---
    INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
    VALUES (v_JobKennung, v_DW_EintragsNr, 'INFO', 'Job ' || v_ProgName || ' version ' || v_ProgVersion || ' started.', CURRENT_TIMESTAMP());

    INSERT INTO project.dataset.job_control (eintrags_nr, job_kennung, script_name, log_name, stichtag_info, status, created_ts, finished_ts)
    VALUES (v_DW_EintragsNr, v_JobKennung, v_ProgName, v_LogDatei, p_s, 'RUNNING', CURRENT_TIMESTAMP(), NULL);

    -- --- Main processing block with error handling ---
    BEGIN
        -- --- Update stichtag_info (equivalent to DWMSG_SetzeStichtagInfo) ---
        -- This implies updating a record specific to the current run/job_kennung
        -- Or, if job_control handles only one stichtag, this would be an UPDATE.
        -- For this migration, we'll assume job_control entry holds the stichtag for this run.
        -- The initial INSERT above already stores p_s, so no explicit UPDATE here unless
        -- there's a global stichtag to be set.
        -- For now, let's assume the stichtag in job_control is sufficient.

        -- --- Call the core processing script ---
        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (v_JobKennung, v_DW_EintragsNr, 'INFO', 'Calling core script: ' || v_Name_Kernskript, CURRENT_TIMESTAMP());

        CALL project.dataset.k_ausd_v_ta_cntrct_crs2(v_JobKennung, v_DW_EintragsNr);

        -- --- On successful completion ---
        SET v_status = 'OK';
        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (v_JobKennung, v_DW_EintragsNr, 'INFO', 'Job ' || v_ProgName || ' completed successfully.', CURRENT_TIMESTAMP());

        UPDATE project.dataset.job_control
        SET status = v_status, finished_ts = CURRENT_TIMESTAMP()
        WHERE eintrags_nr = v_DW_EintragsNr AND job_kennung = v_JobKennung;

    EXCEPTION WHEN ERROR THEN
        -- --- Error Handling (equivalent to DWMSG_Fehlerbehandlung) ---
        SET v_status = 'ERROR';
        SET v_ErrNr = @@error.code;
        SET v_ErrArg = @@error.message;

        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_nr, error_arg, error_message, created_ts)
        VALUES (v_JobKennung, v_DW_EintragsNr, v_ErrNr, v_ErrArg, 'Job ' || v_ProgName || ' failed.', CURRENT_TIMESTAMP());

        INSERT INTO project.dataset.job_log (job_kennung, eintrags_nr, log_level, message, created_ts)
        VALUES (v_JobKennung, v_DW_EintragsNr, 'ERROR', 'Job ' || v_ProgName || ' failed with error: ' || v_ErrArg, CURRENT_TIMESTAMP());

        UPDATE project.dataset.job_control
        SET status = v_status, finished_ts = CURRENT_TIMESTAMP()
        WHERE eintrags_nr = v_DW_EintragsNr AND job_kennung = v_JobKennung;

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Job ' || v_ProgName || ' failed. Refer to job_error_log.';
    END;

END;