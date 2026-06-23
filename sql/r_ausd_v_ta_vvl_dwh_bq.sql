-- BigQuery Script replacing vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh
-- This script orchestrates the data reconciliation process, handles parameters, logging, and error trapping.

-- Define script parameters based on original ksh script's getopts
DECLARE p_h BOOL DEFAULT FALSE; -- Help flag
DECLARE p_s STRING DEFAULT NULL; -- Generic parameter -s
DECLARE p_l STRING DEFAULT NULL; -- Generic parameter -l

-- Parse command-line arguments (assuming these are passed via execution environment or default values)
-- For execution via bq command or scheduling, these would be passed like:
-- bq query --use_legacy_sql=false --parameter='p_s:STRING:value_s' --parameter='p_l:STRING:value_l' path/to/r_ausd_v_ta_vvl_dwh_bq.sql

-- Declare script-level variables
DECLARE ProgName STRING DEFAULT 'r_ausd_v_ta_vvl_dwh_bq.sql';
DECLARE ProgVersion STRING DEFAULT '1.0';
DECLARE JobKennung STRING;
DECLARE DW_EintragsNr INT64;
DECLARE LogIdentifier STRING;
DECLARE v_sysdate STRING;
DECLARE BERT_DIR_ROOT STRING DEFAULT 'your_project.your_dataset'; -- Placeholder for configuration, e.g., schema for common utilities
DECLARE CORE_SCRIPT_SP_NAME STRING DEFAULT '`your_project.your_dataset.k_ausd_v_ta_vvl_dwh_sp`';

-- Error handling block
BEGIN
    -- Check for help flag
    IF p_h IS TRUE THEN
        SELECT FORMAT_BQM_TEXT(
            "Usage: %s [-h] [-s <param_s>] [-l <param_l>]\n" ||
            "  -h: Display this help message.\n" ||
            "  -s: Generic parameter s.\n" ||
            "  -l: Generic parameter l.\n" ||
            "Purpose: Orchestrates contract data reconciliation for ta_vvl_dwh.",
            ProgName
        );
        RETURN;
    END IF;

    -- Initialize JobKennung (example, could be passed as parameter or generated differently)
    SET JobKennung = UPPER(FORMAT_BQM_TEXT("JOB_%s_%s", ProgName, FORMAT_DATE('%Y%m%d_%H%M%S', CURRENT_DATE())));

    -- Get unique job entry number
    CALL `your_project.your_dataset.DWMSG_ErmittleNr_SP`(DW_EintragsNr);

    -- Determine Log Identifier
    CALL `your_project.your_dataset.DWMSG_Logdateiname_SP`(JobKennung, DW_EintragsNr, LogIdentifier);

    -- Create initial log entry
    CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag_SP`(DW_EintragsNr, JobKennung, ProgName, LogIdentifier);

    -- Get system date in desired format
    SET v_sysdate = FORMAT_DATE('%Y%m%d', CURRENT_DATE()); -- Corresponds to $(date +%Y%m%d)

    -- Set reference date information in log (if applicable)
    CALL `your_project.your_dataset.DWMSG_SetzeStichtagInfo_SP`(DW_EintragsNr, v_sysdate, '%Y%m%d');

    -- Log the start of execution
    INSERT INTO `your_project.your_dataset.job_log` (
        job_nr, job_kennung, log_level, log_message, log_ts
    )
    VALUES (
        DW_EintragsNr,
        JobKennung,
        'INFO',
        FORMAT_BQM_TEXT("Starting %s (Version: %s, JobKennung: %s, DW_EintragsNr: %d)", ProgName, ProgVersion, JobKennung, DW_EintragsNr),
        CURRENT_TIMESTAMP()
    );

    -- Log values of optional parameters if provided
    IF p_s IS NOT NULL THEN
        INSERT INTO `your_project.your_dataset.job_log` (
            job_nr, job_kennung, log_level, log_message, log_ts
        )
        VALUES (
            DW_EintragsNr, JobKennung, 'INFO', FORMAT_BQM_TEXT("Parameter -s: %s", p_s), CURRENT_TIMESTAMP()
        );
    END IF;

    IF p_l IS NOT NULL THEN
        INSERT INTO `your_project.your_dataset.job_log` (
            job_nr, job_kennung, log_level, log_message, log_ts
        )
        VALUES (
            DW_EintragsNr, JobKennung, 'INFO', FORMAT_BQM_TEXT("Parameter -l: %s", p_l), CURRENT_TIMESTAMP()
        );
    END IF;

    -- Call the core processing stored procedure
    -- The k_ausd_v_ta_vvl_dwh.ksh script is replaced by a BigQuery Stored Procedure
    CALL `your_project.your_dataset.k_ausd_v_ta_vvl_dwh_sp`(JobKennung, DW_EintragsNr);

    -- Mark job as successful
    CALL `your_project.your_dataset.DWMSG_SetzeStatusOK_SP`(DW_EintragsNr);

EXCEPTION WHEN ERROR THEN
    -- Get the error message
    DECLARE error_message STRING DEFAULT ERROR_MESSAGE();
    -- Handle the error and log it
    CALL `your_project.your_dataset.DWMSG_Fehlerbehandlung_SP`(DW_EintragsNr, error_message);
END;