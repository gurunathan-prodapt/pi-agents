-- BigQuery Stored Procedure: sp_bindefristcache_update
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This procedure orchestrates the update of the 'ta_c_bfc' table,
-- replacing the original KornShell wrapper script.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_bindefristcache_update`(
    p_help BOOLEAN DEFAULT FALSE
)
BEGIN
    -- Declare variables to mimic shell script environment and logging.
    DECLARE v_job_kennung STRING;
    DECLARE v_entry_nr INT64;
    DECLARE v_script_name STRING DEFAULT 'r_ausd_v_ta_c_bfc.ksh';
    DECLARE v_core_script_name STRING DEFAULT 'k_ausd_v_ta_c_bfc.ksh';
    DECLARE v_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_end_ts TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'FAILED'; -- Default status if an unexpected error occurs
    DECLARE v_message STRING;
    DECLARE v_log_file STRING DEFAULT 'N/A'; -- Original shell script had a log file, now logging to tables

    -- Error handling variables
    DECLARE v_error_message STRING;
    DECLARE v_error_stack STRING;
    DECLARE v_error_sqlstate STRING;
    DECLARE v_error_current_statement STRING;
    DECLARE v_error_constraint STRING;

    -- Handle help parameter
    IF p_help THEN
        SELECT '''
        Usage: CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`([p_help => TRUE]);

        This stored procedure orchestrates the update process for the Bindefristcache table.
        It replaces the legacy r_ausd_v_ta_c_bfc.ksh shell script.

        Arguments:
            p_help (BOOL, optional): If TRUE, displays this help message and exits. Defaults to FALSE.
        ''' AS usage_info;
        RETURN;
    END IF;

    -- Generate JobKennung (mimics dynamic generation in shell script)
    SET v_job_kennung = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', v_start_ts) || '_' || REPLACE(REPLACE(v_script_name, '.ksh', ''), '.', '_');

    -- Determine next entry number for the audit log
    SELECT IFNULL(MAX(entry_nr), 0) + 1
    INTO v_entry_nr
    FROM `your_project_id.your_dataset_id.job_audit_log`;

    -- Initial audit log entry (Job Start)
    INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
        entry_nr, job_kennung, script_name, log_file, stichtag, status, created_ts, message
    )
    VALUES (
        v_entry_nr, v_job_kennung, v_script_name, v_log_file, CURRENT_DATE(), 'STARTED', v_start_ts, 'Job execution started.'
    );

    BEGIN
        -- Call the core logic stored procedure
        -- The original script calls: ${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}
        CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc`(v_job_kennung, v_entry_nr);

        SET v_status = 'COMPLETED';
        SET v_message = 'Job execution completed successfully.';

    EXCEPTION WHEN ERROR THEN
        -- Capture error details
        SET v_error_sqlstate = @@error.sqlstate;
        SET v_error_message = @@error.message;
        SET v_error_stack = @@error.stack_trace;
        SET v_error_current_statement = @@error.statement_text;
        SET v_error_constraint = @@error.constraint_name;

        SET v_status = 'FAILED';
        SET v_message = 'Job execution failed.';

        -- Log the error to the job_error_log table
        INSERT INTO `your_project_id.your_dataset_id.job_error_log` (
            job_kennung, err_nr, err_arg, created_ts, message,
            error_sqlstate, error_message, error_stack, error_current_statement, error_constraint
        )
        VALUES (
            v_job_kennung, v_entry_nr, NULL, CURRENT_TIMESTAMP(), 'Error during core logic execution.',
            v_error_sqlstate, v_error_message, v_error_stack, v_error_current_statement, v_error_constraint
        );
    END;

    -- Final update to the audit log entry
    SET v_end_ts = CURRENT_TIMESTAMP();
    UPDATE `your_project_id.your_dataset_id.job_audit_log`
    SET
        status = v_status,
        end_ts = v_end_ts,
        message = v_message
    WHERE
        job_kennung = v_job_kennung AND entry_nr = v_entry_nr;

    -- Re-raise the error if the job failed, so external orchestrators are aware.
    IF v_status = 'FAILED' THEN
        RAISE BQ EXCEPTION FORMAT_TEXT('Job %s failed. Error: %s', v_job_kennung, IFNULL(v_error_message, 'Unknown error'));
    END IF;

END;