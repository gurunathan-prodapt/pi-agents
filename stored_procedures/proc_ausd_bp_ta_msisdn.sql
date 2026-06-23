-- BigQuery Stored Procedure for orchestration logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- This procedure replaces the KornShell script, handling parameter parsing, validation, date derivation,
-- execution of the core SQL logic, record counting, and error handling.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn`(
    p_job_kennung STRING,          -- Corresponds to Jobkennung
    p_stichtag_ddmmyyyy STRING,    -- Corresponds to Stichtag in DDMMYYYY format
    p_eintragsnr STRING,           -- Corresponds to EintragsNr
    p_wiederanlaufwert STRING      -- Corresponds to wiederanlaufWert (can be NULL)
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_current_date DATE;
    DECLARE v_yesterday_date DATE;
    DECLARE v_error_message STRING;
    DECLARE v_job_status STRING DEFAULT 'RUNNING';
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_run_id STRING;

    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_run_id = GENERATE_UUID();

    -- 1. Parameter Validation (replacing h_alis_parameter.ksh and explicit checks)
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_message = 'Parameter Jobkennung (JOBKENNUNG) is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_stichtag_ddmmyyyy IS NULL OR TRIM(p_stichtag_ddmmyyyy) = '' THEN
        SET v_error_message = 'Parameter Stichtag (STICHWERT) is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_eintragsnr IS NULL OR TRIM(p_eintragsnr) = '' THEN
        SET v_error_message = 'Parameter EintragsNr (EINTRAGSNR) is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- 2. Date Validation (replacing h_alis_date.ksh DWDate_Datum_Check)
    SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_stichtag_ddmmyyyy);
    IF v_stichtag_date IS NULL THEN
        SET v_error_message = FORMAT('Invalid Stichtag format. Expected DDMMYYYY, received: %s', p_stichtag_ddmmyyyy);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- 3. Date Derivation (replacing gestern.ksh logic)
    SET v_current_date = CURRENT_DATE();
    SET v_yesterday_date = DATE_SUB(v_current_date, INTERVAL 1 DAY);

    -- Optional Job Tracking: Initial entry (replacing FOSJobErzeugeEintrag)
    INSERT INTO `your_gcp_project.your_bq_dataset.job_tracking` (
        job_name, run_id, start_timestamp, status, stichtag
    )
    VALUES (
        p_job_kennung, v_run_id, v_start_time, v_job_status, v_stichtag_date
    );

    BEGIN
        -- Execute the core SQL logic (replacing d_ausd_bp_ta_msisdn.sql via starteSQLSkript)
        CALL `your_gcp_project.your_bq_dataset.proc_d_ausd_bp_ta_msisdn`(
            p_job_kennung,
            v_stichtag_date,
            p_eintragsnr,
            p_wiederanlaufwert
        );

        -- Record Counting (replacing `cat $tmpFile` and `eval "v_records="`)
        -- Assumes proc_d_ausd_bp_ta_msisdn writes to `target_bp_ta_msisdn`
        -- and that `target_bp_ta_msisdn` has a 'processing_date' column.
        SELECT COUNT(*)
        INTO v_records_processed
        FROM `your_gcp_project.your_bq_dataset.target_bp_ta_msisdn`
        WHERE processing_date = v_stichtag_date;

        SET v_job_status = 'SUCCEEDED';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_job_status = 'FAILED';

        -- Error Logging (replacing f_alis_msgerr.ksh)
        INSERT INTO `your_gcp_project.your_bq_dataset.job_error_log` (
            job_name, run_timestamp, severity, error_message, stack_trace, input_parameters
        )
        VALUES (
            p_job_kennung,
            CURRENT_TIMESTAMP(),
            'ERROR',
            v_error_message,
            @@error.stack_trace,
            TO_JSON_STRING(STRUCT(p_job_kennung, p_stichtag_ddmmyyyy, p_eintragsnr, p_wiederanlaufwert))
        );

        RAISE; -- Re-raise the error to propagate it to the caller
    END;

    -- Optional Job Tracking: Update final status (replacing FOSJobDeaktivate)
    UPDATE `your_gcp_project.your_bq_dataset.job_tracking`
    SET
        end_timestamp = CURRENT_TIMESTAMP(),
        status = v_job_status,
        records_processed = v_records_processed,
        error_details = v_error_message
    WHERE
        run_id = v_run_id;

END;