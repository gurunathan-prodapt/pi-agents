-- ===================================================================
-- Legacy Source: d_ausd_bp_ta_bpr_bcp.sql / k_ausd_bp_ta_bpr_bcp.ksh
-- Job          : ausd_bp_ta_bpr_bcp
-- Description  : Target BigQuery SQL Statement to process BCP contracts
-- ===================================================================

-- Step 1: Query legacy metadata variable v_datum (retained for backward compatibility and audits)
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Clear the target table
TRUNCATE TABLE `isbert_schema.sof_ta_bpr_bcp`;

-- Step 3: Insert valid distinct Business Card Package (BCP) contracts
INSERT INTO `isbert_schema.sof_ta_bpr_bcp` (
  CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF
)
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref
FROM `isbert_schema.sof_ta_bpr_instance` bp
WHERE bp.bpr_id = '3142';