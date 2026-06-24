-- Target BigQuery SQL for DW.BERT_AUSD_BP_TA_APN_VERTRAG
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG

-- Step 1: Truncate the target table in BigQuery.
TRUNCATE TABLE `your_project.your_dataset.SOFTA_APN_VERTRAG`;

-- Step 2: Insert aggregated data into the target BigQuery table.
INSERT INTO `your_project.your_dataset.SOFTA_APN_VERTRAG` (
    cntrct_id,
    aggregated_apn,
    aggregated_cntrct_ref
)
SELECT
    cntrct_id,
    -- Concatenate APNs, ordering is important for consistent output if not guaranteed by source.
    -- SUBSTR and RTRIM mimic the original logic for length and trailing comma.
    SUBSTR(RTRIM(STRING_AGG(access_point_name, ', ' ORDER BY access_point_name), ', '), 1, 100) AS aggregated_apn,
    -- Concatenate contract references; casting to STRING is necessary for STRING_AGG if not already STRING.
    SUBSTR(RTRIM(STRING_AGG(CAST(cntrct_id_ref AS STRING), ', ' ORDER BY cntrct_id_ref), ', '), 1, 100) AS aggregated_cntrct_ref
FROM
    `your_project.your_dataset.SOFTA_BPR_APN`
GROUP BY
    cntrct_id;