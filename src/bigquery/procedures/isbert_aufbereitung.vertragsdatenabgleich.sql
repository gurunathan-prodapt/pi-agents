-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh

CREATE SCHEMA IF NOT EXISTS isbert_aufbereitung;

CREATE OR REPLACE PROCEDURE isbert_aufbereitung.vertragsdatenabgleich(
    IN p_stichtag DATE DEFAULT CURRENT_DATE(),
    IN p_log_to_stdout_only BOOL DEFAULT FALSE, -- -l flag replacement (log file redirection)
    IN p_show_help BOOL DEFAULT FALSE
)
OPTIONS (
    description="Wrapper script for reconciliation of contract data: table ta_acc_ref. Migrated from r_ausd_v_ta_acc_ref.ksh."
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'vertragsdatenabgleich';
    DECLARE v_prog_version STRING DEFAULT '1.0.0'; -- Placeholder for version
    DECLARE v_job_kennung STRING;
    DECLARE v_core_script_name STRING DEFAULT 'k_ausd_v_ta_acc_ref';
    DECLARE v_log_file_path STRING DEFAULT CONCAT('isbert_logs/run_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()), '.log'); -- Default logical path
    DECLARE v_dw_eintrags_nr INT64;
    DECLARE v_stichtag_yyyymmdd STRING;
    DECLARE v_job_status STRING DEFAULT 'FAILED'; -- Default to FAILED, set to SUCCESS on completion
    DECLARE v_run_id STRING DEFAULT GENERATE_UUID();

    -- Help message (equivalent to -h)
    IF p_show_help THEN
        SELECT FORMAT(
            """
            Usage: CALL isbert_aufbereitung.vertragsdatenabgleich(p_stichtag => 'YYYY-MM-DD', p_log_to_stdout_only => TRUE, p_show_help => TRUE)

            Options:
              p_stichtag           (DATE): Processing date in 'YYYY-MM-DD' format. Default: CURRENT_DATE().
              p_log_to_stdout_only (BOOL): If TRUE, detailed logs might not be written to BigQuery tables (implementation specific).
                                            Original -l flag (redirect to log file) is handled by logging to BQ tables.
                                            This flag provides an option for more direct console output if needed.
              p_show_help          (BOOL): Display this help message and exit.

            Purpose: Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_acc_ref
                     (Wrapper script for the reconciliation of contract data: table ta_acc_ref)
            """, v_prog_name, v_prog_version
        ) AS help_message;
        RETURN;
    END IF;

    -- Initialize Job Kennung
    SET v_job_kennung = CONCAT('ISBERT_VTA_', FORMAT_DATE('%Y%m%d', p_stichtag));

    -- Get Stichtag in YYYYMMDD format
    CALL isbert_aufbereitung.h_alis_date_bq_placeholder(p_input_date => p_stichtag, p_output_format_yyyymmdd => v_stichtag_yyyymmdd);

    BEGIN
        -- Start transaction for logging
        BEGIN TRANSACTION;

        -- DWMSG_ErmittleNr (Simulated: get a unique entry number for this run)
        -- In a real scenario, this would likely come from a sequence or a logging table counter.
        SET v_dw_eintrags_nr = CAST(FORMAT_DATE('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()) AS INT64); -- Simple example

        -- DWMSG_ErzeugeEintrag (Log job start)
        INSERT INTO isbert_logs.job_log (
            job_kennung, entry_number, start_timestamp, status, message, job_name, program_version, run_id, stichtag_info
        )
        VALUES (
            v_job_kennung, v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'RUNNING',
            CONCAT('Job ', v_job_kennung, ' started for Stichtag: ', v_stichtag_yyyymmdd),
            v_prog_name, v_prog_version, v_run_id, v_stichtag_yyyymmdd
        );

        -- DWMSG_SetzeStichtagInfo (Log stichtag info)
        INSERT INTO isbert_logs.job_log_detail (
            job_kennung, entry_number, log_timestamp, log_level, message, run_id
        )
        VALUES (
            v_job_kennung, v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO',
            CONCAT('Stichtag for processing: ', FORMAT_DATE('%Y-%m-%d', p_stichtag), ' (YYYYMMDD: ', v_stichtag_yyyymmdd, ')'),
            v_run_id
        );

        -- Call the core script
        CALL isbert_aufbereitung.k_ausd_v_ta_acc_ref(
            p_job_kennung => v_job_kennung,
            p_dw_eintrags_nr => v_dw_eintrags_nr
        );

        -- If core script completes without error
        SET v_job_status = 'SUCCESS';

        -- DWMSG_SetzeStatusOK (Update job log with success status)
        UPDATE isbert_logs.job_log
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = v_job_status,
            message = CONCAT('Job ', v_job_kennung, ' completed successfully.')
        WHERE
            job_kennung = v_job_kennung AND entry_number = v_dw_eintrags_nr AND run_id = v_run_id;

        INSERT INTO isbert_logs.job_log_detail (
            job_kennung, entry_number, log_timestamp, log_level, message, run_id
        )
        VALUES (
            v_job_kennung, v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO',
            CONCAT('Job ', v_job_kennung, ' finished with status: ', v_job_status),
            v_run_id
        );

        COMMIT TRANSACTION;

    EXCEPTION WHEN ERROR THEN
        -- DWMSG_Fehlerbehandlung (Error handling equivalent to trap)
        DECLARE error_message STRING DEFAULT @@error.message;
        DECLARE error_stack STRING DEFAULT @@error.stack_trace;
        DECLARE error_code STRING DEFAULT @@error.code;

        ROLLBACK TRANSACTION;

        SET v_job_status = 'FAILED';

        -- Log error details
        CALL isbert_aufbereitung.f_alis_msgerr_bq_placeholder(
            p_job_kennung => v_job_kennung,
            p_entry_number => v_dw_eintrags_nr,
            p_error_code => error_code,
            p_error_message => error_message,
            p_program_name => v_prog_name,
            p_line_number => NULL -- BigQuery error stack doesn't easily map to a single line number
        );

        -- Update main job log to FAILED
        UPDATE isbert_logs.job_log
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = v_job_status,
            message = CONCAT('Job ', v_job_kennung, ' failed with error: ', error_message)
        WHERE
            job_kennung = v_job_kennung AND entry_number = v_dw_eintrags_nr AND run_id = v_run_id;

        INSERT INTO isbert_logs.job_log_detail (
            job_kennung, entry_number, log_timestamp, log_level, message, run_id
        )
        VALUES (
            v_job_kennung, v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'ERROR',
            CONCAT('Job ', v_job_kennung, ' finished with status: ', v_job_status, '. Error: ', error_message),
            v_run_id
        );

        -- Re-raise the error to indicate failure to the caller
        RAISE;
    END;

END;