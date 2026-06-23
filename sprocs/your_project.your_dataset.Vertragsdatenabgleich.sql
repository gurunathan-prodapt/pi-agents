-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- Main BigQuery Orchestration Stored Procedure to replace r_ausd_v_ta_discount_rr.ksh.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.Vertragsdatenabgleich`(
    IN p_param_s STRING,
    IN p_param_l STRING,
    IN p_param_h BOOL -- Flag for displaying help, though direct help display is not standard in BQ SP.
)
BEGIN
    DECLARE v_job_key STRING;
    DECLARE v_log_file_name STRING;
    DECLARE v_bert_dir_root STRING;
    DECLARE v_stichtag_info STRING DEFAULT FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()); -- Default reference date

    -- Initialize job key
    SET v_job_key = CONCAT('r_ausd_v_ta_discount_rr_', GENERATE_UUID());

    -- Retrieve BERT_DIR_ROOT from configuration table (simulates environment variable sourcing)
    SELECT config_value INTO v_bert_dir_root
    FROM `your_project.your_dataset.configuration_table`
    WHERE config_key = 'BERT_DIR_ROOT';

    IF v_bert_dir_root IS NULL THEN
        -- Handle case where configuration is missing
        CALL `your_project.your_dataset.DWMSG_MeldeFehler`(v_job_key, 'BERT_DIR_ROOT configuration not found.');
        RAISE BQ.ABORTED ERROR 'Configuration error: BERT_DIR_ROOT is not set.';
    END IF;

    -- Create initial log entry for job start
    CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
        v_job_key,
        'Job `r_ausd_v_ta_discount_rr.ksh` started in BigQuery.',
        'INFO',
        'RUNNING'
    );

    -- Simulate `getopts -h`
    IF p_param_h THEN
        CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
            v_job_key,
            'Help requested. Parameters: -s (some_value), -l (another_value).',
            'INFO',
            'SUCCESS'
        );
        RETURN;
    END IF;

    -- Error handling block for the main job execution
    BEGIN
        -- Set reference date information
        CALL `your_project.your_dataset.DWMSG_SetzeStichtagInfo`(v_job_key, v_stichtag_info);

        -- Log parsed parameters
        CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
            v_job_key,
            CONCAT('Parameters received: -s="', p_param_s, '", -l="', p_param_l, '"'),
            'INFO',
            'RUNNING'
        );

        -- Determine conceptual log file name (for record-keeping)
        CALL `your_project.your_dataset.DWMSG_Logdateiname`(v_job_key, v_log_file_name);
        CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
            v_job_key,
            CONCAT('Conceptual Log File Name: ', v_log_file_name),
            'INFO',
            'RUNNING'
        );

        -- Invoke the migrated core script
        CALL `your_project.your_dataset.k_ausd_v_ta_discount_rr`(v_job_key, p_param_s, p_param_l);

        -- If core script completes without error, set status to OK
        CALL `your_project.your_dataset.DWMSG_SetzeStatusOK`(v_job_key);

    EXCEPTION WHEN ERROR THEN
        -- Handle any errors that occurred within the BEGIN block
        CALL `your_project.your_dataset.DWMSG_Fehlerbehandlung`(
            v_job_key,
            ERROR_CODE(),
            CONCAT('Execution failed. Error message: ', ERROR_MESSAGE(), ' Stack trace: ', ERROR_STACK_TRACE())
        );
        RAISE; -- Re-raise the error to indicate failure to the caller
    END;
END;