-- BigQuery Stored Procedure for Vertragsdatenabgleich
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.Vertragsdatenabgleich`(
    IN p_job_kennung STRING,
    IN p_stichtag DATE,
    IN p_typ STRING
    -- p_start_time and p_log_level are declared in original ksh but not explicitly used.
    -- If they become relevant, add them here.
)
BEGIN
    DECLARE v_dw_eintrags_nr INT64;
    DECLARE v_log_dateiname STRING;
    DECLARE v_start_zeit TIMESTAMP;
    DECLARE v_ende_zeit TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack_trace STRING;
    DECLARE v_user_name STRING DEFAULT SESSION_USER(); -- BigQuery equivalent for whoami
    DECLARE v_pid INT64 DEFAULT CAST(GENERATE_UUID() AS BIGNUMERIC); -- Placeholder, BigQuery doesn't have direct PID
    DECLARE v_host_name STRING DEFAULT 'BigQuery';

    -- Initialize start time
    SET v_start_zeit = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';

    -- Simulate DWMSG_ErmittleNr
    SELECT IFNULL(MAX(eintrags_nr), 0) + 1 INTO v_dw_eintrags_nr
    FROM `your_project.your_dataset.job_audit_log`
    WHERE job_kennung = p_job_kennung;

    -- Simulate DWMSG_Logdateiname
    SET v_log_dateiname = CONCAT(p_job_kennung, '_', CAST(v_dw_eintrags_nr AS STRING), '.log');

    -- Simulate DWMSG_ErzeugeEintrag (initial entry)
    INSERT INTO `your_project.your_dataset.job_audit_log` (
        job_kennung, eintrags_nr, start_zeit, status, meldungs_text, log_dateiname, user_name, pid, host_name, referenz_datum
    ) VALUES (
        p_job_kennung, v_dw_eintrags_nr, v_start_zeit, v_status, 'Job started', v_log_dateiname, v_user_name, v_pid, v_host_name, p_stichtag
    );

    -- Simulate DWMSG_SetzeStichtagInfo
    INSERT INTO `your_project.your_dataset.job_reference_date` (
        job_kennung, eintrags_nr, referenz_datum, gueltig_ab_datum, gueltig_bis_datum
    ) VALUES (
        p_job_kennung, v_dw_eintrags_nr, p_stichtag, p_stichtag, NULL -- Assuming gueltig_ab = stichtag, gueltig_bis is open
    );

    BEGIN
        -- Core Logic Invocation: CALL the migrated k_ausd_v_ta_cntrct_templ procedure
        -- This procedure needs to be migrated separately as per the design document.
        CALL `your_project.your_dataset.k_ausd_v_ta_cntrct_templ`(p_job_kennung, v_dw_eintrags_nr);

        -- If core logic completes successfully
        SET v_status = 'OK';
        SET v_ende_zeit = CURRENT_TIMESTAMP();

        -- Simulate DWMSG_SetzeStatusOK
        UPDATE `your_project.your_dataset.job_audit_log`
        SET
            ende_zeit = v_ende_zeit,
            status = v_status,
            meldungs_text = 'Job completed successfully'
        WHERE job_kennung = p_job_kennung AND eintrags_nr = v_dw_eintrags_nr;

    EXCEPTION WHEN ERROR THEN
        -- Simulate DWMSG_Fehlerbehandlung
        SET v_status = 'ERROR';
        SET v_ende_zeit = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;
        SET v_error_stack_trace = @@error.stack_trace;

        INSERT INTO `your_project.your_dataset.job_error_log` (
            job_kennung, eintrags_nr, fehler_zeit, fehler_code, fehler_text, quell_prozedur, stack_trace
        ) VALUES (
            p_job_kennung, v_dw_eintrags_nr, v_ende_zeit, 199, v_error_message, 'Vertragsdatenabgleich', v_error_stack_trace
        );

        -- Update audit log with error status
        UPDATE `your_project.your_dataset.job_audit_log`
        SET
            ende_zeit = v_ende_zeit,
            status = v_status,
            meldungs_text = CONCAT('Job failed: ', v_error_message)
        WHERE job_kennung = p_job_kennung AND eintrags_nr = v_dw_eintrags_nr;

        -- Re-raise the error to signal failure to the caller
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;

END;