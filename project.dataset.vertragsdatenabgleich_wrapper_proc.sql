-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: BigQuery stored procedure acting as a wrapper for the contract reconciliation process.
-- Replaces the KornShell wrapper script r_ausd_v_ta_p_discount_rr.ksh.
CREATE OR REPLACE PROCEDURE project.dataset.vertragsdatenabgleich_wrapper(
    IN p_job_kennung STRING DEFAULT 'BERT_V_TA_P_DISCOUNT_RR',
    IN p_stichtag DATE DEFAULT CURRENT_DATE(),
    IN p_help BOOLEAN DEFAULT FALSE
)
BEGIN
    DECLARE v_entry_nr INT64;
    DECLARE v_sysdate_formatted STRING;
    DECLARE v_start_time TIMESTAMP;

    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_sysdate_formatted = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    IF p_help THEN
        SELECT 'Usage: CALL project.dataset.vertragsdatenabgleich_wrapper([p_job_kennung => <job_id>], [p_stichtag => <YYYY-MM-DD>], [p_help => TRUE])';
        SELECT '  p_job_kennung: Optional. Unique identifier for the job. Default is BERT_V_TA_P_DISCOUNT_RR.';
        SELECT '  p_stichtag: Optional. Key date for the job execution. Default is today''s date.';
        SELECT '  p_help: Optional. If TRUE, displays this help message and exits.';
        RETURN;
    END IF;

    -- 1. Ermittle Job Entry Number (DWMSG_ErmittleNr)
    CALL project.dataset.dwmsg_ermittlenr(p_job_kennung, v_entry_nr);

    -- Log job start
    CALL project.dataset.dwmsg_erzeugeeintrag(
        p_job_kennung,
        v_entry_nr,
        'Job start: ' || p_job_kennung || ' with entry_nr: ' || CAST(v_entry_nr AS STRING),
        'INFO'
    );
    CALL project.dataset.dwmsg_setzestatusok(
        p_job_kennung,
        v_entry_nr,
        'STARTED',
        'Job started with Stichtag: ' || FORMAT_DATE('%Y-%m-%d', p_stichtag)
    );

    -- 2. Setze Stichtag Info (DWMSG_SetzeStichtagInfo)
    CALL project.dataset.dwmsg_setzestichtaginfo(p_job_kennung, v_entry_nr, p_stichtag);

    BEGIN
        -- 3. Invoke Core Logic (k_ausd_v_ta_p_discount_rr)
        CALL project.dataset.k_ausd_v_ta_p_discount_rr(p_job_kennung, v_entry_nr, p_stichtag);

        -- Log successful completion
        CALL project.dataset.dwmsg_erzeugeeintrag(
            p_job_kennung,
            v_entry_nr,
            'Job completed successfully.',
            'INFO'
        );
        CALL project.dataset.dwmsg_setzestatusok(
            p_job_kennung,
            v_entry_nr,
            'COMPLETED',
            'Execution finished successfully.'
        );

    EXCEPTION WHEN ERROR THEN
        -- 4. Handle Errors (DWMSG_MeldeFehler and DWMSG_Fehlerbehandlung)
        CALL project.dataset.dwmsg_meldefehler(
            p_job_kennung,
            v_entry_nr,
            'Job failed: ' || ERROR_MESSAGE(),
            'WRAPPER',
            'BQ_WRAPPER_ERROR'
        );
        CALL project.dataset.dwmsg_setzestatusok(
            p_job_kennung,
            v_entry_nr,
            'FAILED',
            'Execution failed: ' || ERROR_MESSAGE()
        );
        -- Re-raise the error so that orchestrator can catch it
        RAISE;
    END;

END;