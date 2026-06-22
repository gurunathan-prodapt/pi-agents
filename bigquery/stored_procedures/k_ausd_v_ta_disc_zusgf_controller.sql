-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

CREATE OR REPLACE PROCEDURE `isbert_ds.k_ausd_v_ta_disc_zusgf_controller`(
    p_JobKennung STRING,
    p_EintragsNr INT64
)
BEGIN
    DECLARE v_record_count INT64;
    DECLARE current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_EintragsNr IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'JobKennung and EintragsNr must be provided.';
    END IF;

    -- Logging Start
    INSERT INTO `isbert_ds.job_message_log` (job_kennung, eintrags_nr, log_time, message, log_level)
    VALUES (p_JobKennung, p_EintragsNr, current_timestamp, 'Starting k_ausd_v_ta_disc_zusgf_controller', 'INFO');

    BEGIN
        -- Deactivate older active jobs
        UPDATE `isbert_ds.job_control`
        SET status = 'DEACTIVATED',
            end_time = current_timestamp,
            message = 'Deactivated by new job run'
        WHERE job_kennung = p_JobKennung
          AND status = 'RUNNING'
          AND eintrags_nr <> p_EintragsNr;

        -- Register current job as active
        INSERT INTO `isbert_ds.job_control` (job_kennung, eintrags_nr, start_time, status, message)
        VALUES (p_JobKennung, p_EintragsNr, current_timestamp, 'RUNNING', 'Job started successfully');

        -- Call core SQL logic
        CALL `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`();

        -- Get record count
        SELECT COUNT(*) INTO v_record_count FROM `isbert_ds.sof_ta_disc_zusgf`;

        -- Log record count
        INSERT INTO `isbert_ds.job_result_log` (job_kennung, eintrags_nr, log_time, metric_name, metric_value, table_name)
        VALUES (p_JobKennung, p_EintragsNr, current_timestamp, 'RECORD_COUNT', v_record_count, 'sof_ta_disc_zusgf');

        -- Update job status to SUCCESS
        UPDATE `isbert_ds.job_control`
        SET status = 'SUCCESS',
            end_time = current_timestamp,
            message = CONCAT('Job completed successfully. Records processed: ', CAST(v_record_count AS STRING))
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr;

        INSERT INTO `isbert_ds.job_message_log` (job_kennung, eintrags_nr, log_time, message, log_level)
        VALUES (p_JobKennung, p_EintragsNr, current_timestamp, 'k_ausd_v_ta_disc_zusgf_controller completed successfully', 'INFO');

    EXCEPTION WHEN ERROR THEN
        -- Log error
        INSERT INTO `isbert_ds.error_log` (job_kennung, eintrags_nr, log_time, error_message, stack_trace)
        VALUES (p_JobKennung, p_EintragsNr, current_timestamp, ERROR_MESSAGE(), @@error.stack_trace);

        -- Update job status to FAILED
        UPDATE `isbert_ds.job_control`
        SET status = 'FAILED',
            end_time = current_timestamp,
            message = CONCAT('Job failed: ', ERROR_MESSAGE())
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr;

        INSERT INTO `isbert_ds.job_message_log` (job_kennung, eintrags_nr, log_time, message, log_level)
        VALUES (p_JobKennung, p_EintragsNr, current_timestamp, CONCAT('k_ausd_v_ta_disc_zusgf_controller failed: ', ERROR_MESSAGE()), 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('k_ausd_v_ta_disc_zusgf_controller failed: ', ERROR_MESSAGE());
    END;
END;