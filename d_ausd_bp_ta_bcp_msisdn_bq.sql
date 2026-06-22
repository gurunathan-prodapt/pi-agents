-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql
-- Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

-- Step00: variable definition equivalent
DECLARE v_carmen STRING DEFAULT '@pcrs1'; -- Original variable, not directly used in transformation
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `isbert_schema.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step01: truncate target table
TRUNCATE TABLE `sof.ta_bcp_msisdn`;

-- Step11c: enrich data with MSISDN of BCP contract
INSERT INTO `sof.ta_bcp_msisdn`
  (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN)
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref,
  rn.tn_tel_msisdn
FROM `sof.ta_bpr_bcp` AS bp
JOIN `sof.ta_rn_vertrag` AS rn
  ON bp.cntrct_id_ref = rn.cntrct_id;