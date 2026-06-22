-- BigQuery Stored Procedure for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
-- This procedure orchestrates and executes the core business logic.
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_opt_text`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag STRING, -- Expected format DDMMYYYY
    IN p_wiederanlaufWert STRING
)
BEGIN
    -- Declare variables
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_stack_trace STRING;

    -- Set current date variables
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- 1. Parameter Validation
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
        SET v_error_message = 'ERROR: Parameter p_JobKennung is missing or empty.';
        INSERT INTO `project.dataset.job_error_audit` (job_id, entry_number, key_date, error_timestamp, error_message, stack_trace)
        VALUES (COALESCE(p_JobKennung, 'UNKNOWN'), p_EintragsNr, v_stichtag_date, CURRENT_TIMESTAMP(), v_error_message, @@error.stack_trace);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
        SET v_error_message = 'ERROR: Parameter p_EintragsNr is missing or empty.';
        INSERT INTO `project.dataset.job_error_audit` (job_id, entry_number, key_date, error_timestamp, error_message, stack_trace)
        VALUES (p_JobKennung, COALESCE(p_EintragsNr, 'UNKNOWN'), v_stichtag_date, CURRENT_TIMESTAMP(), v_error_message, @@error.stack_trace);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
        SET v_error_message = 'ERROR: Parameter p_Stichtag is missing or empty.';
        INSERT INTO `project.dataset.job_error_audit` (job_id, entry_number, key_date, error_timestamp, error_message, stack_trace)
        VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, CURRENT_TIMESTAMP(), v_error_message, @@error.stack_trace);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_stichtag_date IS NULL THEN
        SET v_error_message = FORMAT('ERROR: Invalid date format for p_Stichtag: %s. Expected DDMMYYYY.', p_Stichtag);
        INSERT INTO `project.dataset.job_error_audit` (job_id, entry_number, key_date, error_timestamp, error_message, stack_trace)
        VALUES (p_JobKennung, p_EintragsNr, NULL, CURRENT_TIMESTAMP(), v_error_message, @@error.stack_trace);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Error handling block for main logic
    BEGIN
        -- Log job start
        INSERT INTO `project.dataset.job_run_audit` (job_id, entry_number, key_date, run_timestamp, status, start_time)
        VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, CURRENT_TIMESTAMP(), 'RUNNING', CURRENT_TIMESTAMP());

        -- 2. Truncate/Clear target table
        -- Oracle equivalent: TRUNCATE TABLE sof$ta_bpr_opt_text REUSE STORAGE
        DELETE FROM `project.dataset.target_bp_ta_bpr_opt_text`
        WHERE TRUE; -- Delete all rows

        -- 3. Main Business Logic (Migrated from d_ausd_bp_ta_bpr_opt_text.sql)
        -- Oracle equivalent: INSERT INTO sof$ta_bpr_opt_text ...
        INSERT INTO `project.dataset.target_bp_ta_bpr_opt_text`
        (CNTRCT_ID, BPR_ID, PDS_DESCRIPTION)
        SELECT
            bp.CNTRCT_ID,
            bp.BPR_ID,
            bs.PDS_DESCRIPTION
        FROM
            `project.dataset.sof_ta_bpr_optionen` AS bp
        INNER JOIN
            `project.dataset.sof_ta_bpr_beschr` AS bs
        ON
            bp.BPR_ID = bs.BPR_ID;

        SET v_records_processed = (SELECT COUNT(*) FROM `project.dataset.target_bp_ta_bpr_opt_text`);

        -- Log job success
        INSERT INTO `project.dataset.job_run_audit` (job_id, entry_number, key_date, run_timestamp, status, processed_records, end_time)
        VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, CURRENT_TIMESTAMP(), 'SUCCESS', v_records_processed, CURRENT_TIMESTAMP());

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_stack_trace = @@error.stack_trace;
        INSERT INTO `project.dataset.job_error_audit` (job_id, entry_number, key_date, error_timestamp, error_message, stack_trace)
        VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, CURRENT_TIMESTAMP(), v_error_message, v_stack_trace);
        -- Log job failure
        INSERT INTO `project.dataset.job_run_audit` (job_id, entry_number, key_date, run_timestamp, status, end_time, processed_records)
        VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, CURRENT_TIMESTAMP(), 'FAILED', CURRENT_TIMESTAMP(), 0);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;
END;