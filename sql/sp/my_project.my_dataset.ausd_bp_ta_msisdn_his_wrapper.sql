-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- BigQuery Stored Procedure for the wrapper logic.
CREATE OR REPLACE PROCEDURE my_project.my_dataset.ausd_bp_ta_msisdn_his_wrapper(
    p_stichtag DATE,
    p_wiederanlaufWert INT64
)
BEGIN
    DECLARE ProgName STRING DEFAULT 'r_ausd_bp_ta_msisdn_his_wrapper';
    DECLARE ProgVersion STRING DEFAULT '1.0';
    DECLARE JobKennung STRING DEFAULT 'AUSD_BP_TA_MSISDN_HIS';
    DECLARE v_sysdate DATE;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE DW_EintragsNr INT64; -- Unique identifier for this job run

    -- Initialize system date and parameters
    SET v_sysdate = CURRENT_DATE();
    SET v_stichtag = IFNULL(p_stichtag, v_sysdate);
    SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

    -- Generate a unique job entry number using UNIX_MICROS for BigQuery's lack of auto-incrementing INT64
    SET DW_EintragsNr = UNIX_MICROS(CURRENT_TIMESTAMP());

    -- Error Handling Block
    BEGIN
        -- Parameter validation
        IF v_stichtag IS NULL THEN
            -- Log error for missing Stichtag
            INSERT INTO my_project.my_dataset.job_error_log (job_name, job_entry_nr, error_nr, error_arg, error_message, created_at)
            VALUES (JobKennung, DW_EintragsNr, 1001, 'StichtagMissing', 'The cutoff date (Stichtag) cannot be NULL or empty after defaulting.', CURRENT_TIMESTAMP());
            RAISE EXCEPTION 'Stichtag cannot be NULL.';
        END IF;

        -- Record job start in the registry
        INSERT INTO my_project.my_dataset.job_registry (job_entry_nr, job_name, script_name, stichtag, created_at, status)
        VALUES (DW_EintragsNr, JobKennung, ProgName, v_stichtag, CURRENT_TIMESTAMP(), 'RUNNING');

        -- Log job start message
        INSERT INTO my_project.my_dataset.job_message_log (job_name, job_entry_nr, message_text, created_at)
        VALUES (JobKennung, DW_EintragsNr, FORMAT('Job %s (Version %s) started for Stichtag %s with Wiederanlaufwert %d. Job Entry Nr: %d',
                                                JobKennung, ProgVersion, FORMAT_DATE('%Y-%m-%d', v_stichtag), v_wiederanlaufWert, DW_EintragsNr), CURRENT_TIMESTAMP());

        -- Call the core kernel script
        CALL my_project.my_dataset.k_ausd_bp_ta_msisdn_his(JobKennung, v_stichtag, DW_EintragsNr, v_wiederanlaufWert);

        -- Log job completion message
        INSERT INTO my_project.my_dataset.job_message_log (job_name, job_entry_nr, message_text, created_at)
        VALUES (JobKennung, DW_EintragsNr, FORMAT('Job %s completed successfully. Job Entry Nr: %d', JobKennung, DW_EintragsNr), CURRENT_TIMESTAMP());

        -- Update job registry status to OK
        UPDATE my_project.my_dataset.job_registry
        SET status = 'OK', finished_at = CURRENT_TIMESTAMP()
        WHERE job_entry_nr = DW_EintragsNr;

    EXCEPTION WHEN ERROR THEN
        -- Log error details
        INSERT INTO my_project.my_dataset.job_error_log (job_name, job_entry_nr, error_nr, error_arg, error_message, created_at)
        VALUES (JobKennung, DW_EintragsNr, 9999, 'BigQueryExecutionError', ERROR_MESSAGE(), CURRENT_TIMESTAMP());

        -- Update job registry status to ERROR
        UPDATE my_project.my_dataset.job_registry
        SET status = 'ERROR', finished_at = CURRENT_TIMESTAMP()
        WHERE job_entry_nr = DW_EintragsNr;

        -- Re-raise the exception to propagate the error
        RAISE;
    END;
END;