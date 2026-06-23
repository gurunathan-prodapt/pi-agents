-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- This is the top-level wrapper procedure for the r_ausd_v_ta_notice job.
-- It handles overall job setup, error logging, and orchestration of the core logic.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_r_ausd_v_ta_notice`(
    IN p_optional_job_kennung STRING -- Optional override for job_kennung (e.g., from scheduler)
    -- Additional parameters for configuration or debug could be added here
)
BEGIN
    DECLARE v_job_kennung STRING;
    DECLARE v_entry_nr INT64;
    DECLARE v_hostname STRING DEFAULT 'bigquery-host'; -- Placeholder for BigQuery execution environment
    DECLARE v_pid STRING DEFAULT GENERATE_UUID();     -- Placeholder for a unique process identifier

    -- Set or generate JobKennung
    IF p_optional_job_kennung IS NOT NULL AND p_optional_job_kennung != '' THEN
        SET v_job_kennung = p_optional_job_kennung;
    ELSE
        SET v_job_kennung = 'R_AUSD_V_TA_NOTICE'; -- Default JobKennung
    END IF;

    -- Generate a unique entry number for this run
    CALL `my_project.my_dataset.sp_dwmsg_ermittle_nr`(v_job_kennung, v_entry_nr);

    CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(v_job_kennung, v_entry_nr, 'Starting sp_r_ausd_v_ta_notice wrapper script.', 'INFO', 'sp_r_ausd_v_ta_notice');

    -- Create initial job entry in the job_control table
    CALL `my_project.my_dataset.sp_dwmsg_erzeuge_eintrag`(v_job_kennung, v_entry_nr, 'Job started.', v_pid, v_hostname);

    BEGIN
        -- Call the core control procedure, passing JobKennung and EntryNr
        CALL `my_project.my_dataset.sp_k_ausd_v_ta_notice_core`(v_job_kennung, v_entry_nr);

        CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(v_job_kennung, v_entry_nr, 'sp_r_ausd_v_ta_notice finished successfully.', 'INFO', 'sp_r_ausd_v_ta_notice');

    EXCEPTION WHEN ERROR THEN
        -- An error occurred in sp_k_ausd_v_ta_notice_core or sp_d_ausd_v_ta_notice_sql.
        -- Error details are already logged by sp_dwmsg_fehlerbehandlung.
        -- Log a final wrapper error and re-raise to signal failure to any calling orchestrator.
        CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(v_job_kennung, v_entry_nr, CONCAT('sp_r_ausd_v_ta_notice encountered a critical error: ', ERROR_MESSAGE()), 'ERROR', 'sp_r_ausd_v_ta_notice');
        RAISE;
    END;

END;