-- BigQuery Stored Procedure for Core Data Logic
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE PROCEDURE project.dataset.sp_d_ausd_bp_ta_apn_vertrag(
    IN p_stichtag DATE
)
BEGIN
    -- Legacy variable definition for v_carmen and v_datum based on DWTK_MELDUNGEN.
    -- The original script defined v_datum from MAX(m.timecreated) from DWTK_MELDUNGEN
    -- where job_kennung = 'BERT_DROP_TEMP_TABLE'. This was likely a control date.
    -- For this migration, we will assume p_stichtag is the primary control date,
    -- or if needed, a specific date from DWTK_MELDUNGEN can be queried.
    -- The original script also had 'v_carmen = "@pcrs1"' which seems like a database link,
    -- not directly translatable into BigQuery.

    -- Truncate the target table before insertion, as per original script's TRUNCATE TABLE command.
    TRUNCATE TABLE project.dataset.SOF_TA_APN_VERTRAG;

    -- Core logic: Aggregate access_point_name and cntrct_id_ref for each cntrct_id.
    -- The original PL/SQL used a loop with string concatenation and substr/rtrim.
    -- BigQuery supports STRING_AGG for this purpose, which is more efficient.
    INSERT INTO project.dataset.SOF_TA_APN_VERTRAG (
        cntrct_id,
        access_point_names_aggregated,
        cntrct_id_refs_aggregated,
        processing_stichtag
    )
    SELECT
        t1.cntrct_id,
        SUBSTR(STRING_AGG(DISTINCT t1.access_point_name, ', ' ORDER BY t1.access_point_name), 1, 100) AS access_point_names_aggregated,
        SUBSTR(STRING_AGG(DISTINCT t1.cntrct_id_ref, ', ' ORDER BY t1.cntrct_id_ref), 1, 100) AS cntrct_id_refs_aggregated,
        p_stichtag -- Use the passed stichtag as the processing date
    FROM
        project.dataset.SOF_TA_BPR_APN AS t1
    WHERE
        -- If the original d_ausd_bp_ta_apn_vertrag.sql had date filtering logic
        -- based on the 'v_datum' derived from DWTK_MELDUNGEN, it would go here.
        -- For now, we assume no direct date filtering on SOF_TA_BPR_APN based on v_datum
        -- because the PL/SQL loop did not use it for WHERE clause.
        TRUE -- Placeholder, add specific filtering if source data has a date column relevant to p_stichtag
    GROUP BY
        t1.cntrct_id;

    -- Original script had 'analyze table' and 'SPR$PA_ANALYZE.ANALYZE_OBJECTS'
    -- which are not needed in BigQuery as statistics are handled automatically.

    -- Log successful completion (optional, can be handled by the calling orchestration SP)
    -- INSERT INTO project.dataset.error_log (job_name, error_message, severity)
    -- VALUES ('sp_d_ausd_bp_ta_apn_vertrag', 'Core data logic completed successfully', 'INFO');

EXCEPTION WHEN ERROR THEN
    -- Log the error
    INSERT INTO project.dataset.error_log (job_name, error_code, error_message, severity)
    VALUES (
        'sp_d_ausd_bp_ta_apn_vertrag',
        CAST(BQ.RAISE_ERROR() AS STRING), -- Captures the error message
        'Failed to execute core data logic',
        'ERROR'
    );
    -- Re-raise the error to propagate it to the calling procedure
    RAISE;
END;