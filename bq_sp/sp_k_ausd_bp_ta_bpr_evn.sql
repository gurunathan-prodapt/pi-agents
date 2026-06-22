--
-- BigQuery Stored Procedure for the core data transformation logic.
-- This procedure encapsulates the logic originally found or implied within
-- k_ausd_bp_ta_bpr_evn.ksh, which is orchestrated by
-- vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh.
-- NOTE: The exact schema of source_contract_cache and fos_target_table is assumed
-- and requires detailed analysis of k_ausd_bp_ta_bpr_evn.ksh for a complete implementation.
--
CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_bp_ta_bpr_evn`(
    IN p_stichtag STRING,
    IN p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_stichtag_date DATE;

    -- Convert the input stichtag string (DDMMYYYY) to a DATE type
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

    -- Conditional deletion based on wiederanlaufWert
    -- The design document suggests deleting based on dwh_vertrag_id >= p_wiederanlaufWert
    -- If fos_target_table contains data for multiple stichtags, this DELETE might be too broad.
    -- Consider adding a WHERE clause for stichtag in the DELETE if the target table is partitioned or contains stichtag as a column.
    IF p_wiederanlaufWert IS NOT NULL AND p_wiederanlaufWert > 0 THEN
        DELETE FROM `project.dataset.fos_target_table`
        WHERE dwh_vertrag_id >= p_wiederanlaufWert;
    END IF;

    -- Insert data into the target table
    INSERT INTO `project.dataset.fos_target_table`
    -- Assuming column names and order match between source and target,
    -- or that SELECT * is acceptable for this initial migration.
    -- In a production scenario, explicit column lists should be used.
    SELECT
        * -- Placeholder: Replace with actual column list from source_contract_cache
    FROM
        `project.dataset.source_contract_cache`
    WHERE
        -- Date filtering based on Gueltig_von, Gueltig_bis, and LADEDATUM
        gueltig_von <= v_stichtag_date
        AND v_stichtag_date < gueltig_bis
        AND ladedatum < v_stichtag_date
        -- Apply DWH_VERTRAG_ID filter only if p_wiederanlaufWert is positive
        AND (p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = 0 OR dwh_vertrag_id > p_wiederanlaufWert);

END;