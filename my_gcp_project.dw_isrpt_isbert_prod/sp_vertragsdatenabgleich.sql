-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`
(
    p_job_kennung STRING DEFAULT 'BERT_V_TA_VERTRAG_TMP',
    p_run_date DATE DEFAULT CURRENT_DATE(),
    p_show_help BOOL DEFAULT FALSE,
    p_s_param STRING DEFAULT NULL, -- Corresponds to -s parameter in original script
    p_l_param STRING DEFAULT NULL  -- Corresponds to -l parameter in original script
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'Vertragsdatenabgleich';
    DECLARE v_prog_version STRING DEFAULT 'V1.0.0';
    DECLARE v_job_run_id STRING;
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_error_code INT64;
    DECLARE v_error_message STRING;
    DECLARE v_error_detail STRING;
    DECLARE v_stichtag_info DATE;

    -- Helper procedure for logging audit messages
    DECLARE PROCEDURE LogAudit(log_level STRING, message STRING, component STRING, error_code INT64 DEFAULT NULL, error_args STRING DEFAULT NULL)
    BEGIN
        INSERT INTO `my_gcp_project.dw_isrpt_isbert_prod.job_audit_log`
        (log_id, job_run_id, log_timestamp, log_level, message, component, error_code, error_args)
        VALUES
        (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), log_level, message, component, error_code, error_args);
    END;

    -- Helper procedure for updating job status
    DECLARE PROCEDURE UpdateJobStatus(status STRING, message STRING, error_code INT64 DEFAULT NULL, error_message STRING DEFAULT NULL)
    BEGIN
        MERGE `my_gcp_project.dw_isrpt_isbert_prod.job_status` T
        USING (SELECT v_job_run_id AS job_run_id, p_job_kennung AS job_kennung,
                      CURRENT_TIMESTAMP() AS status_timestamp, status AS current_status,
                      message AS last_update_message, error_code AS error_code,
                      error_message AS error_message) S
        ON T.job_run_id = S.job_run_id
        WHEN MATCHED THEN
            UPDATE SET current_status = S.current_status,
                       status_timestamp = S.status_timestamp,
                       last_update_message = S.last_update_message,
                       error_code = S.error_code,
                       error_message = S.error_message
        WHEN NOT MATCHED THEN
            INSERT (job_run_id, job_kennung, status_timestamp, current_status, last_update_message, error_code, error_message)
            VALUES (S.job_run_id, S.job_kennung, S.status_timestamp, S.current_status, S.last_update_message, S.error_code, S.error_message);
    END;

    -- Handle help request (-h parameter)
    IF p_show_help THEN
        SELECT 'Programm: ' || v_prog_name || '\nVersion:  ' || v_prog_version || '\nAufruf:   CALL `project.dataset.sp_vertragsdatenabgleich`(...)\n\nBeschreibung:\n    Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_vertrag_tmp.\nParameter:\n\tp_job_kennung STRING: Job identifier (default: BERT_V_TA_VERTRAG_TMP)\n\tp_run_date DATE: Date for the job run (default: CURRENT_DATE())\n\tp_show_help BOOL: Display this help message\n\tp_s_param STRING: Value for the -s parameter (passed to core script)\n\tp_l_param STRING: Value for the -l parameter (passed to core script)' AS help_message;
        RETURN;
    END IF;

    -- Generate a unique run ID (replacing DW_EintragsNr)
    SET v_job_run_id = GENERATE_UUID();
    SET v_stichtag_info = p_run_date;

    -- Initialize job registry and status
    INSERT INTO `my_gcp_project.dw_isrpt_isbert_prod.job_registry`
    (job_run_id, job_kennung, program_name, program_path, start_time, stichtag_info, status)
    VALUES
    (v_job_run_id, p_job_kennung, v_prog_name, 'vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh', v_start_time, v_stichtag_info, 'RUNNING');

    CALL UpdateJobStatus('RUNNING', 'Job execution started.');
    CALL LogAudit('INFO', FORMAT('Job %s (Run ID: %s) started. Version: %s. Run Date: %tF', p_job_kennung, v_job_run_id, v_prog_version, v_stichtag_info), 'sp_vertragsdatenabgleich');
    CALL LogAudit('INFO', FORMAT('Parameters received: p_job_kennung=%s, p_run_date=%tF, p_s_param=%s, p_l_param=%s', p_job_kennung, p_run_date, COALESCE(p_s_param, 'NULL'), COALESCE(p_l_param, 'NULL')), 'sp_vertragsdatenabgleich');


    -- Begin main job execution block with error handling (replacing `trap` mechanism)
    BEGIN
        -- Log "StichtagInfo" (Key Date Info)
        CALL LogAudit('INFO', FORMAT('Set StichtagInfo for job: %tF', v_stichtag_info), 'sp_vertragsdatenabgleich');

        -- Mimic print statements
        CALL LogAudit('INFO', '----------------- Job -----------------------', 'sp_vertragsdatenabgleich');
        CALL LogAudit('INFO', FORMAT('Job-Nr    : \'%s\'', v_job_run_id), 'sp_vertragsdatenabgleich');
        CALL LogAudit('INFO', FORMAT('JobKennung: \'%s\'', p_job_kennung), 'sp_vertragsdatenabgleich');
        CALL LogAudit('INFO', FORMAT('Logdatei  : \'%s\'', v_job_run_id), 'sp_vertragsdatenabgleich'); -- LogDatei in BQ is effectively job_run_id
        CALL LogAudit('INFO', '---------------------------------------------', 'sp_vertragsdatenabgleich');

        -- Call the core processing script (k_ausd_v_ta_vertrag_tmp.ksh -> sp_k_ausd_v_ta_vertrag_tmp)
        CALL `my_gcp_project.dw_isrpt_isbert_prod.sp_k_ausd_v_ta_vertrag_tmp`(v_job_run_id, p_job_kennung, p_s_param, p_l_param);

        -- If execution reaches here, it means the core script completed without unhandled errors
        CALL LogAudit('INFO', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', 'sp_vertragsdatenabgleich');
        CALL UpdateJobStatus('OK', 'Job completed successfully.');

        UPDATE `my_gcp_project.dw_isrpt_isbert_prod.job_registry`
        SET end_time = CURRENT_TIMESTAMP(), status = 'OK'
        WHERE job_run_id = v_job_run_id;

    EXCEPTION WHEN ERROR THEN
        SET v_error_code = @@error.code;
        SET v_error_message = @@error.message;
        SET v_error_detail = @@error.stack_trace;

        CALL LogAudit('ERROR', FORMAT('Abnormal termination of job. Error: %s', v_error_message), 'sp_vertragsdatenabgleich', v_error_code, v_error_detail);
        CALL UpdateJobStatus('ERR', FORMAT('Job failed: %s', v_error_message), v_error_code, v_error_message);

        UPDATE `my_gcp_project.dw_isrpt_isbert_prod.job_registry`
        SET end_time = CURRENT_TIMESTAMP(), status = 'ERR', error_code = v_error_code, error_message = v_error_message
        WHERE job_run_id = v_job_run_id;
    END;

END;