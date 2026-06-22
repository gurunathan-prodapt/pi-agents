-- Header: BigQuery Stored Procedure for control logic
-- Legacy Source: k_ausd_v_ta_disc_zusgf.ksh
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE OR REPLACE PROCEDURE `bert_dwh.k_ausd_v_ta_disc_zusgf`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr INT66
)
BEGIN
    -- Log the start of the control script
    INSERT INTO `bert_dwh.job_log` (job_kennung, eintrags_nr, log_timestamp, message, log_level)
    VALUES (p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), 'Starting control procedure k_ausd_v_ta_disc_zusgf', 'INFO');

    -- Deactivate any old active jobs for this job_kennung
    UPDATE `bert_dwh.job_control`
    SET job_status = 'CANCELLED',
        end_time = CURRENT_TIMESTAMP(),
        last_update_time = CURRENT_TIMESTAMP(),
        error_message = 'Cancelled by new run'
    WHERE job_kennung = p_job_kennung AND job_status = 'RUNNING' AND eintrags_nr < p_eintrags_nr;

    -- Call the core transformation procedure
    CALL `bert_dwh.d_ausd_v_ta_disc_zusgf`(p_eintrags_nr, p_job_kennung);

    -- If the d_ausd_v_ta_disc_zusgf call succeeds, update job_control status
    UPDATE `bert_dwh.job_control`
    SET job_status = 'SUCCEEDED',
        end_time = CURRENT_TIMESTAMP(),
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_job_kennung AND eintrags_nr = p_eintrags_nr;

    -- Log the completion of the control script
    INSERT INTO `bert_dwh.job_log` (job_kennung, eintrags_nr, log_timestamp, message, log_level)
    VALUES (p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), 'Control procedure k_ausd_v_ta_disc_zusgf completed successfully', 'INFO');

EXCEPTION WHEN ERROR THEN
    -- Log the error
    INSERT INTO `bert_dwh.job_error_log` (job_kennung, eintrags_nr, error_timestamp, error_message, stack_trace)
    VALUES (p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), ERROR_MESSAGE(), @@error.stack_trace);

    -- Update job control status to FAILED
    UPDATE `bert_dwh.job_control`
    SET job_status = 'FAILED',
        end_time = CURRENT_TIMESTAMP(),
        error_message = ERROR_MESSAGE(),
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_job_kennung AND eintrags_nr = p_eintrags_nr;

    -- Re-raise the error to propagate it
    RAISE;
END;