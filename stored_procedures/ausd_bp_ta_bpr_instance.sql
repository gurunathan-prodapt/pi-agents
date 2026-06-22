-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_instance`(
    IN p_stichtag STRING,           -- Processing date in DDMMYYYY format
    IN p_wiederanlaufWert INT64     -- Restart value, 0 for full run, >0 for partial restart
)
BEGIN
    DECLARE v_sysdate STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_bpr_instance';
    DECLARE v_script_name STRING DEFAULT 'ausd_bp_ta_bpr_instance';
    DECLARE v_log_entry_nr INT64;

    -- Determine current system date in DDMMYYYY format
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Default p_wiederanlaufWert if not provided
    SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

    -- Default p_stichtag if not provided, else parse it
    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
        SET v_stichtag = PARSE_DATE('%d%m%Y', v_sysdate);
    ELSE
        SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag);
    END IF;

    -- Generate a unique entry number for this job run (simple approach, can be improved)
    SELECT IFNULL(MAX(entry_nr), 0) + 1 INTO v_log_entry_nr FROM `project.dataset.job_log`;

    -- Log job start and parameters
    INSERT INTO `project.dataset.job_log` (entry_nr, job_name, script_name, log_name, stichtag, status, created_at, error_message)
    VALUES (v_log_entry_nr, v_job_name, v_script_name, NULL, v_stichtag, 'START', CURRENT_TIMESTAMP(),
            FORMAT("Job started with Stichtag: %s, Wiederanlaufwert: %d", FORMAT_DATE('%d%m%Y', v_stichtag), v_wiederanlaufWert));

    -- Parameter validation: Ensure Stichtag is valid
    IF v_stichtag IS NULL THEN
        INSERT INTO `project.dataset.job_error_log` (job_name, error_nr, error_arg, error_timestamp, error_details)
        VALUES (v_job_name, 193, 'Stichtag', CURRENT_TIMESTAMP(), 'Stichtag parameter is invalid or missing.');
        
        UPDATE `project.dataset.job_log`
        SET status = 'FAILURE', error_message = 'Stichtag parameter is invalid or missing.'
        WHERE entry_nr = v_log_entry_nr;

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Stichtag parameter is invalid or missing.';
    END IF;

    BEGIN
        -- Restart logic: If Wiederanlaufwert is > 0, delete relevant records from target
        IF v_wiederanlaufWert > 0 THEN
            DELETE FROM `project.dataset.target_table`
            WHERE DWH_VERTRAG_ID >= v_wiederanlaufWert;

            INSERT INTO `project.dataset.job_log` (entry_nr, job_name, script_name, log_name, stichtag, status, created_at, error_message)
            VALUES (v_log_entry_nr + 0.1, v_job_name, v_script_name, NULL, v_stichtag, 'INFO', CURRENT_TIMESTAMP(),
                    FORMAT("Deleted records from target_table with DWH_VERTRAG_ID >= %d for restart.", v_wiederanlaufWert));
        END IF;

        -- Core Data Logic: MERGE statement
        -- This MERGE statement replaces the logic from the external kernel script (k_ausd_bp_ta_bpr_instance.ksh).
        -- The exact column mappings and transformation logic for UPDATE SET and INSERT must be
        -- completed after a detailed analysis of the kernel script's content.
        MERGE INTO `project.dataset.target_table` T
        USING (
            SELECT
                src.DWH_VERTRAG_ID,
                src.Gueltig_von,
                src.Gueltig_bis,
                src.LADEDATUM,
                src.example_column_1, -- Placeholder: Replace with actual source columns
                src.example_column_2  -- Placeholder: Replace with actual source columns
            FROM `project.dataset.source_contract_cache` src
            WHERE
                src.Gueltig_von <= v_stichtag
                AND v_stichtag < src.Gueltig_bis
                AND src.LADEDATUM < v_stichtag -- Note: Original comment was 'LADEDATUM < PARSE_DATE('%d%m%Y', v_stichtag)'
                                              -- Assuming v_stichtag is already DATE type, direct comparison is better.
                AND (v_wiederanlaufWert = 0 OR src.DWH_VERTRAG_ID > v_wiederanlaufWert)
        ) S
        ON T.DWH_VERTRAG_ID = S.DWH_VERTRAG_ID
        WHEN MATCHED THEN
            UPDATE SET
                T.target_column_1 = S.example_column_1, -- Placeholder: Map source to target columns
                T.target_column_2 = S.example_column_2, -- Placeholder: Map source to target columns
                T.last_updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                DWH_VERTRAG_ID,
                target_column_1, -- Placeholder: Insert all required target columns
                target_column_2,
                last_updated_at
            )
            VALUES (
                S.DWH_VERTRAG_ID,
                S.example_column_1, -- Placeholder: Map source to target columns
                S.example_column_2,
                CURRENT_TIMESTAMP()
            );

        -- Log job success
        UPDATE `project.dataset.job_log`
        SET status = 'SUCCESS', created_at = CURRENT_TIMESTAMP(), error_message = 'Job completed successfully.'
        WHERE entry_nr = v_log_entry_nr;

    EXCEPTION WHEN ERROR THEN
        -- Log any SQL errors during data processing
        INSERT INTO `project.dataset.job_error_log` (job_name, error_nr, error_arg, error_timestamp, error_details)
        VALUES (v_job_name, 192, 'SQL_EXECUTION_ERROR', CURRENT_TIMESTAMP(), ERROR_MESSAGE());

        UPDATE `project.dataset.job_log`
        SET status = 'FAILURE', error_message = ERROR_MESSAGE()
        WHERE entry_nr = v_log_entry_nr;

        RAISE; -- Re-raise the error to signal failure to the orchestrator
    END;

END;