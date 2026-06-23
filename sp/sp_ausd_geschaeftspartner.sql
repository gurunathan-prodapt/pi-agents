-- BigQuery Stored Procedure: sp_ausd_geschaeftspartner
-- Replaces core business logic from k_ausd_geschaeftspartner.ksh, part of legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
--
-- This procedure extracts and loads data for the FOS contract cache.
-- It handles restart logic based on dwh_vertrag_id and filters records based on validity dates.
CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_ausd_geschaeftspartner`(
    IN p_job_nr INT64,
    IN p_job_kennung STRING,
    IN p_stichtag DATE,
    IN p_wiederanlaufWert INT64
)
BEGIN
    -- Log start of core processing
    INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
    VALUES (p_job_nr, p_job_kennung, 'INFO', FORMAT("Starting sp_ausd_geschaeftspartner for Stichtag: %T, Wiederanlaufwert: %d", p_stichtag, p_wiederanlaufWert), CURRENT_TIMESTAMP());

    -- Implement restart logic: Delete records above p_wiederanlaufWert if it's greater than 0
    IF p_wiederanlaufWert > 0 THEN
        DELETE FROM `project_id.dataset_id.fos_vertrags_cache`
        WHERE
            dwh_vertrag_id >= p_wiederanlaufWert;

        INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
        VALUES (p_job_nr, p_job_kennung, 'INFO', FORMAT("Deleted records from fos_vertrags_cache with dwh_vertrag_id >= %d (Wiederanlaufwert)", p_wiederanlaufWert), CURRENT_TIMESTAMP());
    END IF;

    -- Insert filtered records into the target cache table
    INSERT INTO `project_id.dataset_id.fos_vertrags_cache` (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum)
    SELECT
        src.dwh_vertrag_id,
        src.vertrags_nummer,
        src.gueltig_von,
        src.gueltig_bis,
        src.betrag,
        p_stichtag AS ladedatum -- Assuming 'ladedatum' in target means the Stichtag of this run.
    FROM
        `project_id.dataset_id.dwh_vertrag_cache_source` AS src
    WHERE
        src.gueltig_von <= p_stichtag
        AND p_stichtag < src.gueltig_bis
        AND src.ladedatum < p_stichtag -- The design specifies "LADEDATUM < Stichtag"
        AND (p_wiederanlaufWert = 0 OR src.dwh_vertrag_id > p_wiederanlaufWert); -- Apply wiederanlaufWert filter during insert as well

    INSERT INTO `project_id.dataset_id.job_log` (job_nr, job_kennung, log_level, message, created_at)
    VALUES (p_job_nr, p_job_kennung, 'INFO', 'Finished inserting data into fos_vertrags_cache.', CURRENT_TIMESTAMP());

END;