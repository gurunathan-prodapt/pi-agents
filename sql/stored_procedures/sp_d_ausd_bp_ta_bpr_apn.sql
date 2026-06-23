-- BigQuery Stored Procedure to replace d_ausd_bp_ta_bpr_apn.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_d_ausd_bp_ta_bpr_apn`(
    IN p_EintragsNr STRING,
    IN p_JobKennung STRING,
    IN p_Stichtag_raw STRING, -- Stichtag as passed from ksh, e.g., 'DDMMYYYY'
    OUT records_processed INT64
)
BEGIN
    DECLARE v_datum DATE;

    -- Step00: Variable definition (v_datum equivalent)
    -- This section corresponds to the 'v_datum' derivation from dwtk_meldungen.
    -- The original script used MAX(m.timecreated) from dwtk_meldungen where job_kennung = 'BERT_DROP_TEMP_TABLE'.
    -- We assume p_Stichtag_raw is the primary date. If m.timecreated is meant as a fallback or a different date,
    -- this logic needs to be revisited. For now, we prioritize the passed Stichtag.
    -- If p_Stichtag_raw is NOT the source for v_datum, this section needs manual adjustment.
    SET v_datum = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag_raw);

    -- If v_datum is derived from dwtk_meldungen as in the original SQL:
    -- SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
    -- FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    -- The BigQuery equivalent for this would be:
    -- SET v_datum = COALESCE((SELECT MAX(DATE(m.timecreated)) FROM `your_project_id.your_dataset_id.dwtk_meldungen` m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'), PARSE_DATE('%Y%m%d', '19000101'));


    -- Step01: Delete/Truncate temporary tables
    -- Corresponds to: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_apn REUSE STORAGE');
    TRUNCATE TABLE `your_project_id.your_dataset_id.sof_ta_bpr_apn`;

    -- Step10: Insert into sof_ta_bpr_apn
    INSERT INTO `your_project_id.your_dataset_id.sof_ta_bpr_apn`
    (
        CNTRCT_ID,
        BPR_ID,
        CNTRCT_ID_REF,
        ACCESS_POINT_NAME
    )
    SELECT
        DISTINCT
        bp.cntrct_id,
        bp.bpr_id,
        bp.cntrct_id_ref,
        ap.access_point_name
    FROM
        `your_project_id.your_dataset_id.sof_ta_bpr_instance` AS bp
    INNER JOIN
        `your_project_id.your_dataset_id.sof_ta_apn_carmen` AS ap
    ON
        bp.cntrct_id_ref = ap.cntrct_id
    WHERE
        bp.bpr_id IN (2828, 2829, 2830, 2831, 2925, 2926, 2998, 2999, 3000);

    SET records_processed = ROW_COUNT();

END;