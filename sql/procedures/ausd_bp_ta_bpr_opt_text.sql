-- BigQuery Stored Procedure for r_ausd_bp_ta_bpr_opt_text.ksh
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- and core logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_opt_text.sql
--
-- This procedure provisions selected base products for the BERT system.
-- It handles parameter parsing, date calculations, restart logic, and
-- orchestrates the data loading into the target table.

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_opt_text`(
    IN p_stichtag_str STRING,  -- Input as DDMMYYYY string
    IN p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'ausd_bp_ta_bpr_opt_text';
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufwert_param INT64;

    -- Initialize parameters and start logging
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_wiederanlaufwert_param = IFNULL(p_wiederanlaufWert, 0);

    -- Parse stichtag; default to current date if not provided
    IF p_stichtag_str IS NULL OR p_stichtag_str = '' THEN
        SET v_stichtag = CURRENT_DATE();
        INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
        VALUES (CURRENT_TIMESTAMP(), 'INFO', v_job_name, 'p_stichtag_str not provided, defaulting to CURRENT_DATE().');
    ELSE
        BEGIN
            SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_str);
            INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
            VALUES (CURRENT_TIMESTAMP(), 'INFO', v_job_name, FORMAT('p_stichtag parsed as: %t', v_stichtag));
        EXCEPTION WHEN ERROR THEN
            SET v_status = 'FAILED';
            SET v_message = FORMAT('Error parsing p_stichtag_str: "%s". Expected format DDMMYYYY.', p_stichtag_str);
            INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
            VALUES (CURRENT_TIMESTAMP(), 'ERROR', v_job_name, v_message);
            INSERT INTO `project.dataset.job_audit` (job_name, start_time, end_time, status, message, stichtag, wiederanlaufwert)
            VALUES (v_job_name, v_start_time, CURRENT_TIMESTAMP(), v_status, v_message, NULL, v_wiederanlaufwert_param);
            RAISE USING MESSAGE v_message;
        END;
    END IF;

    INSERT INTO `project.dataset.job_audit` (job_name, start_time, status, stichtag, wiederanlaufwert)
    VALUES (v_job_name, v_start_time, 'RUNNING', v_stichtag, v_wiederanlaufwert_param);

    BEGIN
        -- Implement restart logic: delete existing records if wiederanlaufWert > 0
        IF v_wiederanlaufwert_param > 0 THEN
            INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
            VALUES (CURRENT_TIMESTAMP(), 'INFO', v_job_name, FORMAT('Restart logic active. Deleting records from `project.dataset.sof_ta_bpr_opt_text` with cntrct_id >= %d.', v_wiederanlaufwert_param));

            DELETE FROM `project.dataset.sof_ta_bpr_opt_text`
            WHERE cntrct_id >= v_wiederanlaufwert_param;

            INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
            VALUES (CURRENT_TIMESTAMP(), 'INFO', v_job_name, FORMAT('%d records deleted from `project.dataset.sof_ta_bpr_opt_text` for restart.', @@row_count));
        ELSE
             INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
             VALUES (CURRENT_TIMESTAMP(), 'INFO', v_job_name, 'No restart value provided, performing full load by truncating table.');
             TRUNCATE TABLE `project.dataset.sof_ta_bpr_opt_text`;
        END IF;

        -- Core transformation logic
        INSERT INTO `project.dataset.sof_ta_bpr_opt_text` (
            cntrct_id,
            bpr_id,
            pds_description
        )
        SELECT
            bp.cntrct_id,
            bp.bpr_id,
            bs.pds_description
        FROM
            `project.dataset.sof_ta_bpr_optionen` AS bp
        INNER JOIN
            `project.dataset.sof_ta_bpr_beschr` AS bs
            ON bp.bpr_id = bs.bpr_id;

        SET v_status = 'SUCCESS';
        SET v_message = FORMAT('%d records inserted into `project.dataset.sof_ta_bpr_opt_text`.', @@row_count);
        INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
        VALUES (CURRENT_TIMESTAMP(), 'INFO', v_job_name, v_message);

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_message = CONCAT('Job failed with error: ', @@error.message);
        INSERT INTO `project.dataset.job_log` (log_time, log_level, job_name, message)
        VALUES (CURRENT_TIMESTAMP(), 'ERROR', v_job_name, v_message);
    END;

    -- Final audit log update
    SET v_end_time = CURRENT_TIMESTAMP();
    UPDATE `project.dataset.job_audit`
    SET
        end_time = v_end_time,
        status = v_status,
        message = v_message
    WHERE
        job_name = v_job_name AND start_time = v_start_time;

    IF v_status = 'FAILED' THEN
        RAISE USING MESSAGE v_message;
    END IF;

END;