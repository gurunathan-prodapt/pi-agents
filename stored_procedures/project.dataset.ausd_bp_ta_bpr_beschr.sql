-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Description: BigQuery Stored Procedure to replace the KornShell wrapper script and its core logic.
-- This procedure handles parameter parsing, environment setup, logging, and error handling.
-- The core data processing logic is based on the description in the migration design document
-- for the (unresolved) k_ausd_bp_ta_bpr_beschr.ksh script.
CREATE OR REPLACE PROCEDURE project.dataset.ausd_bp_ta_bpr_beschr(
    IN p_stichtag_string STRING,
    IN p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_final_wiederanlaufWert INT64;

    -- Generate a unique job ID for this run
    SET v_job_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';

    -- Initialize parameters JSON for audit log
    DECLARE job_params JSON;
    SET job_params = TO_JSON(STRUCT(p_stichtag_string AS stichtag_input, p_wiederanlaufWert AS wiederanlauf_input));

    -- Insert initial job audit record
    INSERT INTO project.dataset.job_audit (job_id, job_name, start_time, status, parameters)
    VALUES (v_job_id, 'ausd_bp_ta_bpr_beschr', v_start_time, v_status, job_params);

    -- Log job start
    INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'Job ausd_bp_ta_bpr_beschr started.');
    INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Input parameters: stichtag_string=%s, wiederanlaufWert=%ld', p_stichtag_string, p_wiederanlaufWert));

    BEGIN
        -- 1. Parameter Handling and Defaulting
        -- Default p_wiederanlaufWert to 0 if not provided or invalid
        SET v_final_wiederanlaufWert = COALESCE(p_wiederanlaufWert, 0);

        -- Default p_stichtag_string to current date if not provided
        IF p_stichtag_string IS NULL OR TRIM(p_stichtag_string) = '' THEN
            SET v_stichtag = CURRENT_DATE();
            INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
            VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('p_stichtag_string defaulted to CURRENT_DATE(): %s', FORMAT_DATE('%d%m%Y', v_stichtag)));
        ELSE
            -- Validate and parse p_stichtag_string (assuming 'DDMMYYYY' format)
            BEGIN
                SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_string);
                INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
                VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('p_stichtag_string parsed to date: %s', FORMAT_DATE('%d%m%Y', v_stichtag)));
            EXCEPTION WHEN ERROR THEN
                SET v_error_message = 'Invalid date format for p_stichtag_string. Expected DDMMYYYY.';
                RAISE; -- Re-raise the error to be caught by the outer EXCEPTION block
            END;
        END IF;

        -- 2. Core Business Logic (Derived from k_ausd_bp_ta_bpr_beschr.ksh description)
        -- Delete logic for restart mechanism, if v_final_wiederanlaufWert is 0 (no active, uncollected contract cache).
        -- The design implies "deletion of already provisioned table content when no active, uncollected contract cache exists".
        -- This interpretation assumes that when v_final_wiederanlaufWert is 0, a full refresh/reprocessing might be intended.
        -- If v_final_wiederanlaufWert > 0, it acts as a filter for DWH_VERTRAG_ID, implying an incremental load or restart point.
        IF v_final_wiederanlaufWert = 0 THEN
            INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
            VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'v_final_wiederanlaufWert is 0. Performing full DELETE on target table.');

            DELETE FROM project.dataset.fos_tabelle
            WHERE TRUE; -- Delete all records if full refresh intended. Adjust WHERE clause if specific partitions/data need to be cleared.

            INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
            VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'DELETE operation completed.');
        END IF;

        -- Insert selected records into project.dataset.fos_tabelle
        INSERT INTO project.dataset.fos_tabelle (
            DWH_VERTRAG_ID,
            FOS_ATTRIBUTE_1, -- Placeholder for actual transformation logic
            FOS_PROVISION_DATE,
            FOS_PROVISION_VALUE
        )
        SELECT
            tvc.DWH_VERTRAG_ID,
            tvc.ATTRIBUTE_1 AS FOS_ATTRIBUTE_1, -- Example mapping
            v_stichtag AS FOS_PROVISION_DATE, -- Example: use stichtag as provision date
            tvc.PROVISION_VALUE AS FOS_PROVISION_VALUE -- Example mapping
        FROM
            project.dataset.ta_vertrag_cache AS tvc
        WHERE
            tvc.LADEDATUM = v_stichtag -- Filter by load date matching Stichtag
            AND tvc.Gueltig_von <= v_stichtag
            AND tvc.Gueltig_bis >= v_stichtag
            AND tvc.FOSHoleLadedatum IS NULL -- Example condition based on design: "if FOSHoleLadedatum was active"
            AND CAST(tvc.DWH_VERTRAG_ID AS INT64) > v_final_wiederanlaufWert; -- Restart logic

        INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
        VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'INSERT into project.dataset.fos_tabelle completed.');

        SET v_status = 'OK';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'ERROR';
        SET v_error_message = @@error.message;
        INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
        VALUES (v_job_id, CURRENT_TIMESTAMP(), 'ERROR', FORMAT('Job failed with error: %s', v_error_message));
        RAISE; -- Re-raise the error for external orchestration to catch
    END;

    -- Update final job audit record
    SET v_end_time = CURRENT_TIMESTAMP();
    UPDATE project.dataset.job_audit
    SET
        end_time = v_end_time,
        status = v_status,
        error_message = v_error_message
    WHERE job_id = v_job_id;

    INSERT INTO project.dataset.job_log (job_id, log_timestamp, log_level, message)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Job ausd_bp_ta_bpr_beschr finished with status: %s', v_status));

END;