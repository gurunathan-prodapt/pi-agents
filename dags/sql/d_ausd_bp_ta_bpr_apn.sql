-- ===================================================================
-- Legacy Source: d_ausd_bp_ta_bpr_apn.sql
-- Job: ausd_bp_ta_bpr_apn
-- Target: dw_bert.sof_ta_bpr_apn
-- Source: dw_bert.sof_ta_bpr_instance & dw_bert.sof_ta_apn_carmen
-- Process: Provision BERT basis product mappings to APNs
-- ===================================================================

-- Step 1: Clean target table (Replaces legacy TRUNCATE REUSE STORAGE)
TRUNCATE TABLE `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`;

-- Step 2: Insert mapped enterprise products
INSERT INTO `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn` (
  CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  ACCESS_POINT_NAME
)
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref,
  ap.access_point_name
FROM
  `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_instance` bp
INNER JOIN
  `gcp-enterprise-dwh.dw_bert.sof_ta_apn_carmen` ap
ON
  bp.cntrct_id_ref = ap.cntrct_id
WHERE
  bp.bpr_id IN (
    2828, -- VPN
    2829, -- IV_VPN
    2830, -- WAP-Intranet
    2831, -- Telemetry
    2925, -- Mobile IP VPN (50% discount)
    2926, -- Mobile IP VPN (100% discount)
    2998, -- Blackberry Solution
    2999, -- Blackberry Solution (10% discount)
    3000  -- Blackberry Solution (20% discount)
  );