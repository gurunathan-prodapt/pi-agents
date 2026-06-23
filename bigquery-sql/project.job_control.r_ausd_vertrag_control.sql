-- BigQuery Stored Procedure for Orchestration
-- Replaces KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE OR REPLACE PROCEDURE `project.job_control.r_ausd_vertrag_control`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_datum DATE;
    DECLARE job_status STRING;
    DECLARE error_message STRING;
    DECLARE record_count INT64;

    -- Initialize job status
    SET job_status = 'RUNNING';

    -- Log job start
    INSERT INTO `project.job_control.job_table` (job_kennung, eintrags_nr, status, start_time)
    VALUES (p_JobKennung, p_EintragsNr, job_status, CURRENT_TIMESTAMP());

    BEGIN EXCEPTION WHEN ERROR THEN
        SET error_message = CONCAT('Error during job initialization: ', @@error.message);
        INSERT INTO `project.job_control.error_log` (job_kennung, eintrags_nr, error_time, error_message)
        VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), error_message);
        SET job_status = 'FAILED';
        UPDATE `project.job_control.job_table`
        SET status = job_status, end_time = CURRENT_TIMESTAMP(), message = error_message
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr;
        RAISE; -- Re-raise the error for external handling
    END;

    -- Determine v_datum
    BEGIN
        SELECT IFNULL(MAX(DATE(m.timecreated)), DATE '1900-01-01')
        INTO v_datum
        FROM `project.isbert_schema.dwtk_meldungen` m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        IF v_datum IS NULL THEN
            SET error_message = 'Failed to determine v_datum from dwtk_meldungen.';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_message;
        END IF;

    EXCEPTION WHEN ERROR THEN
        SET error_message = CONCAT('Error determining v_datum: ', @@error.message);
        INSERT INTO `project.job_control.error_log` (job_kennung, eintrags_nr, error_time, error_message)
        VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), error_message);
        SET job_status = 'FAILED';
        UPDATE `project.job_control.job_table`
        SET status = job_status, end_time = CURRENT_TIMESTAMP(), message = error_message
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr;
        RAISE;
    END;

    -- Call the data transformation stored procedure
    BEGIN
        CALL `project.staging.d_ausd_v_ta_cntrct_crs`(v_datum);

        -- Get record count from target table
        SELECT COUNT(*)
        INTO record_count
        FROM `project.staging.sof_ta_cntrct_crs`;

        -- Log job result
        INSERT INTO `project.job_control.job_result_log` (job_kennung, eintrags_nr, result_time, record_count, status)
        VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), record_count, 'SUCCESS');

        SET job_status = 'COMPLETED';
        UPDATE `project.job_control.job_table`
        SET status = job_status, end_time = CURRENT_TIMESTAMP(), message = 'Successfully completed'
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr;

    EXCEPTION WHEN ERROR THEN
        SET error_message = CONCAT('Error during data transformation: ', @@error.message);
        INSERT INTO `project.job_control.error_log` (job_kennung, eintrags_nr, error_time, error_message)
        VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), error_message);
        SET job_status = 'FAILED';
        UPDATE `project.job_control.job_table`
        SET status = job_status, end_time = CURRENT_TIMESTAMP(), message = error_message
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr;
        RAISE;
    END;

END;