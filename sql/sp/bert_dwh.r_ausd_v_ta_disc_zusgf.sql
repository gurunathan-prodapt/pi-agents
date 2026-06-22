-- Header: BigQuery Stored Procedure for wrapper logic
-- Legacy Source: r_ausd_v_ta_disc_zusgf.ksh
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE OR REPLACE PROCEDURE `bert_dwh.r_ausd_v_ta_disc_zusgf`(
    IN job_kennung_param STRING,
    IN eintrags_nr_param INT64
)
BEGIN
    DECLARE v_job_kennung STRING DEFAULT job_kennung_param;
    DECLARE v_eintrags_nr INT64 DEFAULT eintrags_nr_param;

    -- If eintrags_nr is not provided, generate a new one
    IF v_eintrags_nr IS NULL THEN
        SELECT COALESCE(MAX(eintrags_nr), 0) + 1
        INTO v_eintrags_nr
        FROM `bert_dwh.job_control`
        WHERE job_kennung = v_job_kennung;
    END IF;

    -- Initialize job control entry
    INSERT INTO `bert_dwh.job_control` (job_kennung, eintrags_nr, job_status, start_time, last_update_time)
    VALUES (v_job_kennung, v_eintrags_nr, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    -- Log the start of the wrapper script
    INSERT INTO `bert_dwh.job_log` (job_kennung, eintrags_nr, log_timestamp, message, log_level)
    VALUES (v_job_kennung, v_eintrags_nr, CURRENT_TIMESTAMP(), CONCAT('Starting wrapper procedure r_ausd_v_ta_disc_zusgf for Job: ', v_job_kennung, ' Entry: ', CAST(v_eintrags_nr AS STRING)), 'INFO');

    -- Call the control procedure
    CALL `bert_dwh.k_ausd_v_ta_disc_zusgf`(v_job_kennung, v_eintrags_nr);

    -- If k_ausd_v_ta_disc_zusgf call succeeds, update job_control status to SUCCEEDED
    -- This update will only happen if k_ausd_v_ta_disc_zusgf didn't already mark it FAILED
    UPDATE `bert_dwh.job_control`
    SET job_status = 'SUCCEEDED',
        end_time = CURRENT_TIMESTAMP(),
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_kennung = v_job_kennung AND eintrags_nr = v_eintrags_nr AND job_status = 'RUNNING';

    -- Log the completion of the wrapper script
    INSERT INTO `bert_dwh.job_log` (job_kennung, eintrags_nr, log_timestamp, message, log_level)
    VALUES (v_job_kennung, v_eintrags_nr, CURRENT_TIMESTAMP(), 'Wrapper procedure r_ausd_v_ta_disc_zusgf completed successfully', 'INFO');

EXCEPTION WHEN ERROR THEN
    -- Log the error
    INSERT INTO `bert_dwh.job_error_log` (job_kennung, eintrags_nr, error_timestamp, error_message, stack_trace)
    VALUES (v_job_kennung, v_eintrags_nr, CURRENT_TIMESTAMP(), ERROR_MESSAGE(), @@error.stack_trace);

    -- Update job control status to FAILED
    UPDATE `bert_dwh.job_control`
    SET job_status = 'FAILED',
        end_time = CURRENT_TIMESTAMP(),
        error_message = ERROR_MESSAGE(),
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_kennung = v_job_kennung AND eintrags_nr = v_eintrags_nr;

    -- Re-raise the error to propagate it
    RAISE;
END;