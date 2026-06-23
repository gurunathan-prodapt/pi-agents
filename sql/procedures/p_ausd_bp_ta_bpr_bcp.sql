-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.p_ausd_bp_ta_bpr_bcp`(
    in_stichtag STRING,
    in_wiederanlaufWert STRING,
    in_job_source STRING,
    in_run_mode STRING
)
BEGIN
    -- Declare variables for job execution and logging
    DECLARE v_job_run_id INT64;
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_bpr_bcp';
    DECLARE v_job_kennung STRING DEFAULT 'DW.BERT.BP_TA_BPR_BCP'; -- Placeholder, update if specific Kennung logic exists
    DECLARE v_stichtag_str STRING;
    DECLARE v_wiederanlaufwert INT64;
    DECLARE v_log_timestamp TIMESTAMP;
    DECLARE v_message STRING;
    DECLARE v_status STRING;

    -- Initialize a unique job run ID using current timestamp in microseconds
    SET v_job_run_id = UNIX_MICROS(CURRENT_TIMESTAMP());

    -- Main job execution block with comprehensive error handling
    BEGIN
        -- Log the start of the job
        SET v_log_timestamp = CURRENT_TIMESTAMP();
        SET v_message = 'Job started.';
        SET v_status = 'RUNNING';
        INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message)
        VALUES (v_job_run_id, v_job_name, v_job_kennung, v_log_timestamp, v_status, v_message);

        -- Process and validate 'Stichtag' parameter (cutoff date)
        IF in_stichtag IS NULL OR TRIM(in_stichtag) = '' THEN
            -- If Stichtag is not provided, default to the current system date in DDMMYYYY format
            SET v_stichtag_str = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
            SET v_message = 'Stichtag not provided, defaulting to current date: ' || v_stichtag_str;
            INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message)
            VALUES (v_job_run_id, v_job_name, v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', v_message);
        ELSE
            -- Validate the provided Stichtag to ensure it matches DDMMYYYY format
            IF SAFE.PARSE_DATE('%d%m%Y', in_stichtag) IS NULL THEN
                RAISE USING MESSAGE 'Invalid Stichtag format. Expected DDMMYYYY, but received: ' || in_stichtag;
            END IF;
            SET v_stichtag_str = in_stichtag;
            SET v_message = 'Stichtag provided: ' || v_stichtag_str;
            INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message)
            VALUES (v_job_run_id, v_job_name, v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', v_message);
        END IF;

        -- Process and validate 'Wiederanlaufwert' parameter (restart value)
        IF in_wiederanlaufWert IS NULL OR TRIM(in_wiederanlaufWert) = '' THEN
            -- If Wiederanlaufwert is not provided, default to 0
            SET v_wiederanlaufwert = 0;
            SET v_message = 'Wiederanlaufwert not provided, defaulting to 0.';
            INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message)
            VALUES (v_job_run_id, v_job_name, v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', v_message);
        ELSE
            -- Attempt to cast the provided Wiederanlaufwert to an INT64.
            -- BigQuery will raise an error if the cast fails, which will be caught by the EXCEPTION block.
            SET v_wiederanlaufwert = CAST(in_wiederanlaufWert AS INT64);
            SET v_message = 'Wiederanlaufwert provided: ' || CAST(v_wiederanlaufwert AS STRING);
            INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message)
            VALUES (v_job_run_id, v_job_name, v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', v_message);
        END IF;

        -- Log the impending call to the core kernel script
        SET v_message = 'Calling core kernel procedure p_k_ausd_bp_ta_bpr_bcp with Stichtag: ' || v_stichtag_str || ' and Wiederanlaufwert: ' || CAST(v_wiederanlaufwert AS STRING);
        INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message, stichtag, wiederanlaufwert)
        VALUES (v_job_run_id, v_job_name, v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', v_message, v_stichtag_str, v_wiederanlaufwert);

        -- Execute the migrated core kernel BigQuery Stored Procedure
        -- This procedure (p_k_ausd_bp_ta_bpr_bcp) is assumed to be migrated and available separately.
        CALL `project.dataset.p_k_ausd_bp_ta_bpr_bcp`(v_stichtag_str, v_wiederanlaufwert);

        -- Log successful completion of the job
        SET v_log_timestamp = CURRENT_TIMESTAMP();
        SET v_message = 'Job completed successfully.';
        SET v_status = 'SUCCESS';
        INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, message, stichtag, wiederanlaufwert)
        VALUES (v_job_run_id, v_job_name, v_job_kennung, v_log_timestamp, v_status, v_message, v_stichtag_str, v_wiederanlaufwert);

    EXCEPTION WHEN ERROR THEN
        -- Error handling: Log the failure details and re-raise the error to the caller
        SET v_log_timestamp = CURRENT_TIMESTAMP();
        SET v_status = 'FAILED';
        SET v_message = CONCAT('Job failed. Error: ', @@error.message);
        INSERT INTO `project.dataset.job_log` (job_run_id, job_name, job_kennung, log_timestamp, status, error_nr, error_arg, stichtag, wiederanlaufwert, message)
        VALUES (v_job_run_id, v_job_name, v_job_kennung, v_log_timestamp, v_status, -1, @@error.message, v_stichtag_str, v_wiederanlaufwert, v_message);
        RAISE; -- Re-raise the error to ensure failure propagation
    END;
END;