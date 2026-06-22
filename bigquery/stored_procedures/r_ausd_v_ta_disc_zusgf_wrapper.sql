-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

CREATE OR REPLACE PROCEDURE `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`()
BEGIN
    DECLARE v_job_kennung STRING DEFAULT 'R_AUSD_V_TA_DISC_ZUSGF';
    DECLARE v_eintrags_nr INT64 DEFAULT UNIX_SECONDS(CURRENT_TIMESTAMP()); -- Unique run identifier
    DECLARE current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Top-level logging start
    INSERT INTO `isbert_ds.job_message_log` (job_kennung, eintrags_nr, log_time, message, log_level)
    VALUES (v_job_kennung, v_eintrags_nr, current_timestamp, 'Starting r_ausd_v_ta_disc_zusgf_wrapper', 'INFO');

    BEGIN
        -- Call the controller stored procedure
        CALL `isbert_ds.k_ausd_v_ta_disc_zusgf_controller`(v_job_kennung, v_eintrags_nr);

        -- Top-level logging success
        INSERT INTO `isbert_ds.job_message_log` (job_kennung, eintrags_nr, log_time, message, log_level)
        VALUES (v_job_kennung, v_eintrags_nr, current_timestamp, 'r_ausd_v_ta_disc_zusgf_wrapper completed successfully', 'INFO');

    EXCEPTION WHEN ERROR THEN
        -- Top-level logging error
        INSERT INTO `isbert_ds.error_log` (job_kennung, eintrags_nr, log_time, error_message, stack_trace)
        VALUES (v_job_kennung, v_eintrags_nr, current_timestamp, ERROR_MESSAGE(), @@error.stack_trace);

        INSERT INTO `isbert_ds.job_message_log` (job_kennung, eintrags_nr, log_time, message, log_level)
        VALUES (v_job_kennung, v_eintrags_nr, current_timestamp, CONCAT('r_ausd_v_ta_disc_zusgf_wrapper failed: ', ERROR_MESSAGE()), 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('r_ausd_v_ta_disc_zusgf_wrapper failed: ', ERROR_MESSAGE());
    END;
END;