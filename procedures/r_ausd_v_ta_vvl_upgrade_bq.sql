--
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- BigQuery SQL script to re-implement the orchestration logic of r_ausd_v_ta_vvl_upgrade.ksh.
-- This script replaces the KornShell wrapper and orchestrates the call to the
-- migrated k_ausd_v_ta_vvl_upgrade BigQuery stored procedure.
--
-- Parameters:
--   p_stichtag_str: Optional date string for the stichtag (YYYY-MM-DD). If NULL, uses current date.
--   p_log_level: Optional string for logging level (e.g., 'INFO', 'DEBUG'). Not directly used by this script but can be passed.
--   p_job_kennung: Optional string to identify the job. If NULL, generates a default.
--
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_v_ta_vvl_upgrade_bq`(
    IN p_stichtag_str STRING,
    IN p_log_level STRING,
    IN p_job_kennung STRING
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'r_ausd_v_ta_vvl_upgrade_bq';
    DECLARE v_prog_version STRING DEFAULT '1.0'; -- Placeholder, update as needed
    DECLARE v_job_number INT64;
    DECLARE v_job_identifier STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_message STRING;
    DECLARE v_current_timestamp TIMESTAMP;
    DECLARE v_max_job_number INT64;

    -- Initialize current timestamp
    SET v_current_timestamp = CURRENT_TIMESTAMP();

    -- Determine stichtag
    IF p_stichtag_str IS NOT NULL THEN
        SET v_stichtag = PARSE_DATE('%Y-%m-%d', p_stichtag_str);
    ELSE
        SET v_stichtag = CURRENT_DATE();
    END IF;

    -- Determine job identifier
    IF p_job_kennung IS NOT NULL THEN
        SET v_job_identifier = p_job_kennung;
    ELSE
        SET v_job_identifier = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', v_current_timestamp) || '_' || v_prog_name;
    END IF;

    -- Generate a new job number
    SELECT COALESCE(MAX(job_number), 0) + 1 INTO v_max_job_number FROM `project.dataset.job_status`;
    SET v_job_number = v_max_job_number;

    -- Log initial job entry and status
    SET v_message = FORMAT('Starting %s (Version: %s) for Job: %s, Number: %d, Stichtag: %s',
                           v_prog_name, v_prog_version, v_job_identifier, v_job_number, FORMAT_DATE('%Y-%m-%d', v_stichtag));
    INSERT INTO `project.dataset.job_log` (log_timestamp, job_number, job_identifier, severity, message)
    VALUES (v_current_timestamp, v_job_number, v_job_identifier, 'INFO', v_message);

    INSERT INTO `project.dataset.job_status` (job_number, job_identifier, status, stichtag, updated_timestamp)
    VALUES (v_job_number, v_job_identifier, 'RUNNING', v_stichtag, v_current_timestamp);

    -- Main job logic with error handling
    BEGIN
        -- Log execution start of kernel script
        SET v_message = FORMAT('Calling kernel script k_ausd_v_ta_vvl_upgrade with JobKennung=%s, JobNr=%d', v_job_identifier, v_job_number);
        INSERT INTO `project.dataset.job_log` (log_timestamp, job_number, job_identifier, severity, message)
        VALUES (CURRENT_TIMESTAMP(), v_job_number, v_job_identifier, 'INFO', v_message);

        -- *** IMPORTANT: This is a placeholder call. ***
        -- The actual `k_ausd_v_ta_vvl_upgrade` stored procedure must be migrated and created first.
        -- Replace 'project.dataset' with your actual project and dataset.
        CALL `project.dataset.k_ausd_v_ta_vvl_upgrade`(v_job_identifier, v_job_number);

        -- If kernel script succeeds
        SET v_message = FORMAT('Kernel script k_ausd_v_ta_vvl_upgrade completed successfully for Job: %s, Number: %d.', v_job_identifier, v_job_number);
        INSERT INTO `project.dataset.job_log` (log_timestamp, job_number, job_identifier, severity, message)
        VALUES (CURRENT_TIMESTAMP(), v_job_number, v_job_identifier, 'INFO', v_message);

        UPDATE `project.dataset.job_status`
        SET status = 'OK', updated_timestamp = CURRENT_TIMESTAMP()
        WHERE job_number = v_job_number AND job_identifier = v_job_identifier;

    EXCEPTION WHEN ERROR THEN
        -- Error handling for the kernel script call
        SET v_message = FORMAT('ERROR: Kernel script k_ausd_v_ta_vvl_upgrade failed for Job: %s, Number: %d. Error: %s', v_job_identifier, v_job_number, @@error.message);
        INSERT INTO `project.dataset.job_log` (log_timestamp, job_number, job_identifier, severity, message)
        VALUES (CURRENT_TIMESTAMP(), v_job_number, v_job_identifier, 'ERROR', v_message);

        UPDATE `project.dataset.job_status`
        SET status = 'ERROR', updated_timestamp = CURRENT_TIMESTAMP()
        WHERE job_number = v_job_number AND job_identifier = v_job_identifier;

        RAISE; -- Re-raise the error for external monitoring
    END;

    SET v_message = FORMAT('Job %s (Number: %d) finished with status: %s', v_job_identifier, v_job_number,
                           (SELECT status FROM `project.dataset.job_status` WHERE job_number = v_job_number AND job_identifier = v_job_identifier));
    INSERT INTO `project.dataset.job_log` (log_timestamp, job_number, job_identifier, severity, message)
    VALUES (CURRENT_TIMESTAMP(), v_job_number, v_job_identifier, 'INFO', v_message);

END;