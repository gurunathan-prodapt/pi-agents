-- Legacy Source: k_ausd_bp_ta_iccid_einzeln.ksh (kernel script invoked by r_ausd_bp_ta_iccid_einzeln.ksh)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_iccid_einzeln`(
    p_stichtag DATE,
    p_wiederanlaufWert INT64,
    p_wrapper_run_id STRING
)
OPTIONS(
    description="Core logic for extracting contract cache data for FOS based on a snapshot date and restart value."
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_iccid_einzeln';
    DECLARE v_run_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_extracted_rows INT64;

    -- Use the run_id passed from the wrapper
    SET v_run_id = p_wrapper_run_id;
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_message = FORMAT("Starting core logic for Stichtag: %t, Wiederanlaufwert: %d", p_stichtag, p_wiederanlaufWert);

    -- Log start of the kernel stored procedure
    INSERT INTO `project.dataset.job_audit_log` (job_name, run_id, start_time, status, message, parameters_json)
    VALUES (v_job_name, v_run_id, v_start_time, v_status, v_message,
            TO_JSON(STRUCT(p_stichtag AS stichtag, p_wiederanlaufWert AS wiederanlaufwert)));

    BEGIN
        -- Clear target table for idempotency if needed, or implement MERGE/UPSERT logic
        -- For now, assuming a simple INSERT, and previous runs handle their own cleanup
        -- This part needs to be reviewed based on actual requirements for the target table
        -- For demonstration, assuming a truncate-and-load for simplicity if run daily.
        -- If this is an incremental load, MERGE statement would be more appropriate.

        -- TRUNCATE TABLE `project.dataset.fos_contract_data`; -- Uncomment if full reload is desired per run

        INSERT INTO `project.dataset.fos_contract_data` (
            contract_id,
            product_id,
            customer_id,
            stichtag,
            load_timestamp
        )
        SELECT
            dcc.dwh_vertrag_id,
            dcc.product_id,
            dcc.customer_id,
            p_stichtag, -- The processing date is part of the output
            CURRENT_TIMESTAMP()
        FROM
            `project.dataset.dwh_contract_cache` AS dcc
        WHERE
            dcc.gueltig_von <= p_stichtag
            AND p_stichtag < dcc.gueltig_bis
            AND dcc.ladedatum < p_stichtag
            AND (p_wiederanlaufWert IS NULL OR dcc.dwh_vertrag_id > p_wiederanlaufWert);

        SET v_extracted_rows = @@row_count;
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'SUCCEEDED';
        SET v_message = FORMAT("Successfully extracted %d records for FOS.", v_extracted_rows);

        -- Update the audit log with success status
        UPDATE `project.dataset.job_audit_log`
        SET
            end_time = v_end_time,
            status = v_status,
            message = v_message
        WHERE
            run_id = v_run_id AND job_name = v_job_name AND status = 'RUNNING';

    EXCEPTION WHEN ERROR THEN
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'FAILED';
        SET v_message = FORMAT("Kernel stored procedure failed with error: %s", @@error.message);

        -- Update the audit log with failure status
        UPDATE `project.dataset.job_audit_log`
        SET
            end_time = v_end_time,
            status = v_status,
            message = v_message
        WHERE
            run_id = v_run_id AND job_name = v_job_name AND status = 'RUNNING';

        RAISE; -- Re-raise the error for the calling procedure to handle
    END;
END;