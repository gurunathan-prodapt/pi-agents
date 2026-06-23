-- BigQuery Stored Procedure for orchestration
-- Converts vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE OR REPLACE PROCEDURE my_project.my_dataset.sp_bereitstellung_basisprodukte_bert(
    IN p_stichtag STRING,
    IN p_wiederanlaufWert STRING
)
BEGIN
    -- Declare variables
    DECLARE v_job_run_id STRING;
    DECLARE v_effective_stichtag STRING;
    DECLARE v_effective_wiederanlaufWert STRING;
    DECLARE v_current_date STRING;
    DECLARE v_error_message STRING;

    -- Generate a unique job run ID
    SET v_job_run_id = GENERATE_UUID();

    -- Determine current system date in YYYYMMDD format for defaults
    SET v_current_date = FORMAT_DATE('%Y%m%d', CURRENT_DATE());

    -- Parameter Handling and Defaulting
    SET v_effective_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_current_date);
    SET v_effective_wiederanlaufWert = IFNULL(NULLIF(p_wiederanlaufWert, ''), '0');

    -- Log job start
    INSERT INTO my_project.my_dataset.job_audit_log (job_run_id, job_name, start_timestamp, status, message, parameter_stichtag, parameter_wiederanlaufwert)
    VALUES (v_job_run_id, 'sp_bereitstellung_basisprodukte_bert', CURRENT_TIMESTAMP(), 'RUNNING', 'Job started', v_effective_stichtag, v_effective_wiederanlaufWert);

    -- Parameter Validation (simplified - shell scripts have more complex checks)
    IF v_effective_stichtag IS NULL OR LENGTH(v_effective_stichtag) != 8 THEN
        SET v_error_message = 'ERROR: Parameter -s (Stichtag) is invalid or not provided.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Main logic block with error handling
    BEGIN
        -- Call the core data processing procedure
        CALL my_project.my_dataset.sp_ausd_bp_ta_rn_da_vda_tk(
            v_job_run_id,
            v_effective_stichtag,
            CAST(v_effective_wiederanlaufWert AS INT64), -- Pass as INT64 as per design
            0 -- Assuming a default for p_restart_threshold, as it's a placeholder
        );

        -- Log job success
        INSERT INTO my_project.my_dataset.job_audit_log (job_run_id, job_name, end_timestamp, status, message)
        VALUES (v_job_run_id, 'sp_bereitstellung_basisprodukte_bert', CURRENT_TIMESTAMP(), 'SUCCESS', 'Job completed successfully');

    EXCEPTION WHEN ERROR THEN
        -- Capture and log error
        SET v_error_message = CONCAT('Job failed: ', @@error.message);
        INSERT INTO my_project.my_dataset.job_audit_log (job_run_id, job_name, end_timestamp, status, message)
        VALUES (v_job_run_id, 'sp_bereitstellung_basisprodukte_bert', CURRENT_TIMESTAMP(), 'FAILED', v_error_message);
        -- Re-raise the error to notify the caller
        RAISE;
    END;
END;