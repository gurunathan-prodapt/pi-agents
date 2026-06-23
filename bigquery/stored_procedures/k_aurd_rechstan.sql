-- BigQuery Stored Procedure: project.dataset.k_aurd_rechstan
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh and its execution of D_AURD_RECHSTAN.SQL
CREATE OR REPLACE PROCEDURE `project.dataset.k_aurd_rechstan`(
    p_job_kennung STRING,
    p_stichtag DATE,
    p_fehler_nr INT64, -- Parameter from original script, kept for signature. Not directly used in current logic.
    p_wiederanlaufWert INT64
)
BEGIN
    -- Log the start of the core processing procedure
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
    VALUES (p_job_kennung, CURRENT_TIMESTAMP(), 'INFO', 'Starting core processing procedure k_aurd_rechstan.', p_stichtag, p_wiederanlaufWert, 'RUNNING_CORE');

    -- Restart logic: Delete records from the target table if p_wiederanlaufWert is greater than 0
    IF p_wiederanlaufWert > 0 THEN
        DELETE FROM `project.dataset.target_table`
        WHERE dwh_vertrag_id >= p_wiederanlaufWert;

        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
        VALUES (p_job_kennung, CURRENT_TIMESTAMP(), 'INFO', 'Deleted existing records from target_table for restart (dwh_vertrag_id >= ' || p_wiederanlaufWert || ').', p_stichtag, p_wiederanlaufWert, 'RESTART_DELETE_COMPLETED');
    END IF;

    -- Insert new/updated data into the target table from the source table
    -- NOTE: Replace `project.dataset.source_table` and `project.dataset.target_table` with actual table names.
    -- NOTE: Adjust column list based on actual schema of target and source tables.
    INSERT INTO `project.dataset.target_table` (
        dwh_vertrag_id,
        gueltig_von,
        gueltig_bis,
        ladedatum,
        -- Placeholder for other columns - replace with actual columns
        col1,
        col2,
        col3
    )
    SELECT
        s.dwh_vertrag_id,
        s.gueltig_von,
        s.gueltig_bis,
        s.ladedatum,
        -- Placeholder for other columns - replace with actual columns
        s.col1,
        s.col2,
        s.col3
    FROM
        `project.dataset.source_table` AS s
    WHERE
        s.gueltig_von <= p_stichtag
        AND p_stichtag < s.gueltig_bis
        AND s.ladedatum < p_stichtag
        AND (p_wiederanlaufWert = 0 OR s.dwh_vertrag_id > p_wiederanlaufWert); -- Apply restart filter for insert as well

    -- Log the successful completion of the core processing procedure
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
    VALUES (p_job_kennung, CURRENT_TIMESTAMP(), 'INFO', 'Core processing procedure k_aurd_rechstan completed successfully.', p_stichtag, p_wiederanlaufWert, 'CORE_COMPLETED');

EXCEPTION WHEN ERROR THEN
    -- Log any errors that occur during the core processing
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
    VALUES (p_job_kennung, CURRENT_TIMESTAMP(), 'ERROR', 'Error in k_aurd_rechstan: ' || ERROR_MESSAGE(), p_stichtag, p_wiederanlaufWert, 'CORE_FAILED');
    RAISE; -- Re-raise the error to propagate it.
END;