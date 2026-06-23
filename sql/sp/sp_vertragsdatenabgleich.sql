-- BigQuery Stored Procedure for wrapper script
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.Vertragsdatenabgleich`(
    p_h BOOL,
    p_s STRING,
    p_l STRING
)
BEGIN
    DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_BARRIER_ZUSGF';
    DECLARE v_sysdate STRING;
    DECLARE v_dw_eintrags_nr STRING; -- Using STRING for job_id to match UUID
    DECLARE v_log_message STRING;
    DECLARE v_error_message STRING;

    -- Set system date
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Parameter -h for usage display
    IF p_h THEN
        SET v_log_message = 'Programm: Vertragsdatenabgleich\nVersion:  V1.0.0\nAufruf:   my_project.my_dataset.Vertragsdatenabgleich(p_h => [BOOL], p_s => [STRING], p_l => [STRING])\nParameter:\n\t-h     zeigt diese Seite an\n\nBeschreibung:\n    Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_barrier_zusgf.';
        INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
        VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP(), 'INFO', v_log_message);
        RETURN;
    END IF;

    -- Generate a unique job ID
    SET v_dw_eintrags_nr = GENERATE_UUID();

    -- Log initial job entry to job_registry
    INSERT INTO `my_project.my_dataset.job_registry` (job_id, job_name, start_time, status)
    VALUES (v_dw_eintrags_nr, 'Vertragsdatenabgleich', CURRENT_TIMESTAMP(), 'RUNNING');

    SET v_log_message = FORMAT('Job started. Job-ID: %s, JobKennung: %s', v_dw_eintrags_nr, v_job_kennung);
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', v_log_message);

    BEGIN
        -- Log job details before calling core script (mimics DWMSG_ErzeugeEintrag)
        SET v_log_message = FORMAT('----------------- Job -----------------------\n Job-Nr    : \'%s\'\n JobKennung: \'%s\'\n Logdatei  : \'N/A (logging to BigQuery tables)\'\n ---------------------------------------------', v_dw_eintrags_nr, v_job_kennung);
        INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', v_log_message);

        -- Update Stichtag Info (mimics DWMSG_SetzeStichtagInfo) - not directly applicable for simple wrapper, but can be logged
        SET v_log_message = FORMAT('Stichtag Info: %s (DDMMYYYY)', v_sysdate);
        INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', v_log_message);

        -- Call the core business logic stored procedure
        -- The original script passed -j $JobKennung -f ${DW_EintragsNr}
        -- p_s and p_l from wrapper are not passed to core script in original ksh
        CALL `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(v_job_kennung, v_dw_eintrags_nr);

        -- If core logic completes without error
        SET v_log_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet';
        INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', v_log_message);

        -- Update job status to OK (mimics DWMSG_SetzeStatusOK)
        UPDATE `my_project.my_dataset.job_registry`
        SET status = 'OK', end_time = CURRENT_TIMESTAMP(), last_updated = CURRENT_TIMESTAMP()
        WHERE job_id = v_dw_eintrags_nr;

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;

        -- Log error to job_log
        INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'ERROR', FORMAT('Error during job execution: %s', v_error_message));

        -- Insert error details into job_error
        INSERT INTO `my_project.my_dataset.job_error` (job_id, error_time, error_code, error_message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), @@error.code, v_error_message);

        -- Update job status to FAILED in job_registry
        UPDATE `my_project.my_dataset.job_registry`
        SET status = 'FAILED', end_time = CURRENT_TIMESTAMP(), last_updated = CURRENT_TIMESTAMP(), error_message = v_error_message
        WHERE job_id = v_dw_eintrags_nr;

        -- Re-raise the error to indicate failure to the caller
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;
END;