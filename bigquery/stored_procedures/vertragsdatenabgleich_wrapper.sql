-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

CREATE OR REPLACE PROCEDURE `my-gcp-project.my_dataset.vertragsdatenabgleich_wrapper`(
    IN p_job_kennung_param STRING, -- Corresponds to -l argument
    IN p_stichtag_param DATE,      -- Corresponds to -s argument
    IN p_show_help BOOL            -- Corresponds to -h argument
)
BEGIN
    DECLARE v_job_entry_number INT64;
    DECLARE v_job_kennung STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_script_name STRING DEFAULT 'vertragsdatenabgleich_wrapper';

    -- 1. Parameter Validation (equivalent to getopts and validation in r_ausd_v_ta_apn_ve.ksh)
    IF p_show_help THEN
        SELECT 'Usage: CALL `my-gcp-project.my_dataset.vertragsdatenabgleich_wrapper`(p_job_kennung_param => <job_kennung>, p_stichtag_param => <YYYY-MM-DD>, p_show_help => FALSE);' AS usage_info;
        RETURN;
    END IF;

    IF p_job_kennung_param IS NULL OR p_stichtag_param IS NULL THEN
        -- Simulates legacy error code 192 (Missing Parameters)
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR 192: Job Kennung (-l) and Stichtag (-s) parameters are mandatory.';
    END IF;

    SET v_job_kennung = p_job_kennung_param;
    SET v_stichtag = p_stichtag_param;

    -- 2. Job Initialization (equivalent to DWMSG_ErmittleNr, DWMSG_ErzeugeEintrag in r_ausd_v_ta_apn_ve.ksh)
    -- Determine next job_entry_number and insert initial job control record.
    SET v_job_entry_number = (SELECT IFNULL(MAX(job_entry_number), 0) + 1 FROM `my-gcp-project.my_dataset.job_control`);

    INSERT INTO `my-gcp-project.my_dataset.job_control` (
        job_entry_number,
        job_kennung,
        script_name,
        start_time,
        stichtag,
        status,
        updated_at
    )
    VALUES (
        v_job_entry_number,
        v_job_kennung,
        v_script_name,
        CURRENT_TIMESTAMP(),
        v_stichtag,
        'RUNNING',
        CURRENT_TIMESTAMP()
    );

    -- Log initial messages (equivalent to DWMSG_Logdateiname, initial log writes in r_ausd_v_ta_apn_ve.ksh)
    INSERT INTO `my-gcp-project.my_dataset.job_log` (job_entry_number, log_timestamp, log_level, message)
    VALUES (v_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Job started for JobKennung: %s, Stichtag: %t, Job Entry No: %d', v_job_kennung, v_stichtag, v_job_entry_number));
    INSERT INTO `my-gcp-project.my_dataset.job_log` (job_entry_number, log_timestamp, log_level, message)
    VALUES (v_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', 'Environment initialized and parameters validated.');


    -- 3. Core Logic Invocation (equivalent to executing k_ausd_v_ta_apn_ve.ksh)
    BEGIN
        CALL `my-gcp-project.my_dataset.k_ausd_v_ta_apn_ve`(v_job_kennung, v_job_entry_number);

        -- If core logic succeeds (equivalent to DWMSG_SetzeStatusOK)
        UPDATE `my-gcp-project.my_dataset.job_control`
        SET
            status = 'OK',
            end_time = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_entry_number = v_job_entry_number;

        INSERT INTO `my-gcp-project.my_dataset.job_log` (job_entry_number, log_timestamp, log_level, message)
        VALUES (v_job_entry_number, CURRENT_TIMESTAMP(), 'INFO', 'Core logic completed successfully. Job status set to OK.');

    EXCEPTION WHEN ERROR THEN
        -- 4. Error Handling (equivalent to trap INT ERR, DWMSG_Fehlerbehandlung, exit $ErrNr)
        DECLARE error_code_val INT64 DEFAULT ERROR_CODE();
        DECLARE error_message_val STRING DEFAULT ERROR_MESSAGE();
        DECLARE sql_state_val STRING DEFAULT SQLSTATE();
        DECLARE stack_trace_val STRING DEFAULT STACK_TRACE();

        -- Map specific legacy error codes if needed, otherwise use BigQuery's
        DECLARE mapped_error_code INT64;
        SET mapped_error_code = CASE
                                    WHEN error_message_val LIKE 'ERROR 192:%' THEN 192 -- Missing parameters
                                    WHEN error_message_val LIKE '%Simulated core logic error%' THEN 193 -- Simulate a potential core logic failure
                                    ELSE error_code_val
                                END;

        UPDATE `my-gcp-project.my_dataset.job_control`
        SET
            status = 'ERROR',
            end_time = CURRENT_TIMESTAMP(),
            error_code = mapped_error_code,
            error_message = error_message_val,
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_entry_number = v_job_entry_number;

        INSERT INTO `my-gcp-project.my_dataset.job_error_log` (
            job_entry_number,
            error_timestamp,
            script_name,
            error_code,
            error_message,
            sql_state,
            stack_trace
        )
        VALUES (
            v_job_entry_number,
            CURRENT_TIMESTAMP(),
            v_script_name,
            mapped_error_code,
            error_message_val,
            sql_state_val,
            stack_trace_val
        );

        INSERT INTO `my-gcp-project.my_dataset.job_log` (job_entry_number, log_timestamp, log_level, message)
        VALUES (v_job_entry_number, CURRENT_TIMESTAMP(), 'ERROR', FORMAT('Job failed with error: %s. Job status set to ERROR.', error_message_val));

        -- Re-raise the error for external orchestrators to catch
        RAISE;
    END;
END;