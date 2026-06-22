-- BigQuery Stored Procedure for d_ausd_v_ta_inv_assign.sql logic
-- Replaces: d_ausd_v_ta_inv_assign.sql executed by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your-gcp-project.isbert_target_data.sp_d_ausd_v_ta_inv_assign`(
    p_job_kennung STRING, -- Parameter from original ksh script, not directly used by original SQL logic
    p_eintrags_nr STRING  -- Parameter from original ksh script, not directly used by original SQL logic
)
BEGIN
    DECLARE v_datum DATE;

    -- Determine v_datum based on the logic from the original SQL
    -- SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    SELECT
        COALESCE(CAST(MAX(m.timecreated) AS DATE), PARSE_DATE('%Y%m%d', '19000101'))
    INTO v_datum
    FROM
        `your-gcp-project.isbert_log_data.dwtk_meldungen_bq` AS m
    WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Truncate target table sof$ta_inv_assign (now ta_inv_assign)
    -- Original: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_inv_assign');
    TRUNCATE TABLE `your-gcp-project.isbert_target_data.ta_inv_assign`;

    -- Insert data into the target table
    -- Original INSERT statement from d_ausd_v_ta_inv_assign.sql
    INSERT INTO `your-gcp-project.isbert_target_data.ta_inv_assign` (
        cntrct_id,
        inv_definition_id,
        insert_at,
        modified_at,
        valid_from,
        valid_to,
        is_production
    )
    SELECT
        ia.cntrct_id,
        ia.inv_definition_id,
        ia.insert_at,
        ia.modified_at,
        ia.valid_from,
        ia.valid_to,
        CAST(ia.is_production AS BOOL) -- Assuming is_production is a numeric type (0/1) in source and needs cast to BOOL for BQ
    FROM
        `your-gcp-project.isbert_source_carmen.cds_ta_inv_assignment` AS ia -- Assuming source table is in a 'source_carmen' dataset
    WHERE
            ia.insert_at <= v_datum
        AND (ia.modified_at IS NULL OR ia.modified_at > v_datum)
        AND ia.valid_from <= v_datum
        AND (ia.valid_to IS NULL OR ia.valid_to > v_datum)
        AND ia.is_production = 1; -- Original filter was 'ia.is_production = 1'. Keep as is for source, then cast for target.

END;