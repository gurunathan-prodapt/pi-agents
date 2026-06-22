-- Procedure: sp_bert_v_ta_period
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period`(
    IN p_help BOOLEAN DEFAULT FALSE, -- Represents -h parameter
    IN p_param_s STRING DEFAULT NULL, -- Represents -s parameter
    IN p_param_l STRING DEFAULT NULL  -- Represents -l parameter
)
OPTIONS(
  description="BigQuery wrapper for contract data reconciliation: ta_period. Replaces r_ausd_v_ta_period.ksh"
)
BEGIN
    -- Declarations similar to shell script variables
    DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_PERIOD';
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE DW_EintragsNr INT64;
    DECLARE LogDatei STRING; -- Simulated log file name or BQ job ID
    DECLARE v_message STRING;
    DECLARE v_exit_code INT64 DEFAULT 0;

    -- Simulate usage function
    IF p_help THEN
        SELECT FORMAT("""
            Programm: %s
            Version:  %s
            Aufruf:   CALL `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period`(p_help => [TRUE|FALSE], p_param_s => '...', p_param_l => '...');
            Parameter:
                p_help      : Displays this help message.
                p_param_s   : (Optional string parameter 's', passed to core script)
                p_param_l   : (Optional string parameter 'l', passed to core script)

            Beschreibung:
                Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.
                This procedure orchestrates the execution, parameter handling, and logging
                for the ta_period contract data reconciliation.
        """, 'Vertragsdatenabgleich', 'V1.0.0');
        RETURN;
    END IF;

    -- DWMSG_ErmittleNr: Determine next entry number
    -- This simulates the shell script's logic to get a unique entry number.
    SELECT IFNULL(MAX(job_entry_nr), 0) + 1 INTO DW_EintragsNr
    FROM `your_gcp_project.your_bq_dataset.job_log`
    WHERE job_name = JobKennung;

    -- DWMSG_Logdateiname: Simulate log file name
    -- In BigQuery, logs are typically structured in `job_log` table or sent to Cloud Logging.
    -- This 'LogDatei' string is for conceptual mapping and consistency with the original script.
    SET LogDatei = FORMAT('%s_%d_%s.log', JobKennung, DW_EintragsNr, FORMAT_DATE('%Y%m%d', v_sysdate));

    -- DWMSG_ErzeugeEintrag: Create job entry and initial status
    -- Log the start of the wrapper script execution.
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log`
    (job_name, job_entry_nr, log_level, message, log_file_name, business_date, created_at, updated_at)
    VALUES
    (JobKennung, DW_EintragsNr, 'I', FORMAT('Job wrapper for %s started.', JobKennung), LogDatei, v_sysdate, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    -- Set initial job status to RUNNING.
    INSERT INTO `your_gcp_project.your_bq_dataset.job_status`
    (job_name, job_entry_nr, status, updated_at)
    VALUES
    (JobKennung, DW_EintragsNr, 'RUNNING', CURRENT_TIMESTAMP());

    -- DWMSG_SetzeStichtagInfo: Set business date info
    -- Record the key date for the job run in the control table.
    INSERT INTO `your_gcp_project.your_bq_dataset.job_control`
    (job_name, job_entry_nr, stichtag, stichtag_format, created_at)
    VALUES
    (JobKennung, DW_EintragsNr, v_sysdate, 'YYYYMMDD', CURRENT_TIMESTAMP());

    -- Print job header (equivalent to shell 'print' statements)
    -- This output would typically go to standard output or a monitoring system in BQ.
    SELECT FORMAT("""
        ----------------- Job -----------------------
        Job-Nr    : '%d'
        JobKennung: '%s'
        Logdatei  : '%s'
        ---------------------------------------------
    """, DW_EintragsNr, JobKennung, LogDatei) AS job_header_info;

    -- Main execution block with error handling
    BEGIN
        -- Core logic invocation
        -- Original shell: ${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1
        -- Call the migrated core script, passing relevant parameters.
        CALL `your_gcp_project.your_bq_dataset.sp_k_ausd_v_ta_period`(JobKennung, DW_EintragsNr, p_param_s, p_param_l);

        -- If core script finishes without error, log success
        -- Original shell: print "Die Abarbeitung wurde ohne erkennbare Fehler beendet" | tee -a $LogDatei
        SET v_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet';
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log`
        (job_name, job_entry_nr, log_level, message, log_file_name, business_date, created_at, updated_at)
        VALUES
        (JobKennung, DW_EintragsNr, 'S', v_message, LogDatei, v_sysdate, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

        -- DWMSG_SetzeStatusOK: Update job status to success
        UPDATE `your_gcp_project.your_bq_dataset.job_status`
        SET status = 'SUCCESS', updated_at = CURRENT_TIMESTAMP()
        WHERE job_name = JobKennung AND job_entry_nr = DW_EintragsNr;

    EXCEPTION WHEN ERROR THEN
        -- DWMSG_Fehlerbehandlung (simulated via BigQuery EXCEPTION block)
        -- This block catches any SQL errors during the execution of the core logic.
        -- It simulates the 'trap' functionality for INT and ERR signals.
        SET v_message = FORMAT('Execution aborted due to error. SQLSTATE: %s, Message: %s', @@error.sqlstate, @@error.message);
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log`
        (job_name, job_entry_nr, log_level, error_nr, error_arg, message, log_file_name, business_date, created_at, updated_at)
        VALUES
        (JobKennung, DW_EintragsNr, 'E', 1, @@error.sqlstate, v_message, LogDatei, v_sysdate, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

        -- Update job status to FAILED.
        UPDATE `your_gcp_project.your_bq_dataset.job_status`
        SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP()
        WHERE job_name = JobKennung AND job_entry_nr = DW_EintragsNr;

        -- Re-raise the error to propagate the failure to the caller.
        RAISE;
    END;

END;