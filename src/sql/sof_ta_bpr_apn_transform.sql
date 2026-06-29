-- Legacy source: d_ausd_bp_ta_bpr_apn.sql
-- Job: ausd_bp_ta_bpr_apn
-- Purpose: Perform clean daily reload of target table by joining basic product instances with APN mappings

-- Step 1: Empty target table to prepare for clean daily reload
TRUNCATE TABLE `isbert_schema.sof_ta_bpr_apn`;

-- Step 2: Insert refined APN references from active instances
INSERT INTO `isbert_schema.sof_ta_bpr_apn` (
  cntrct_id,
  bpr_id,
  cntrct_id_ref,
  access_point_name
)
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref,
  ap.access_point_name
FROM `isbert_schema.sof_ta_bpr_instance` bp
INNER JOIN `isbert_schema.sof_ta_apn_carmen` ap
   ON bp.cntrct_id_ref = ap.cntrct_id
WHERE bp.bpr_id IN (
  2828, -- vpn
  2829, -- iv_vpn
  2830, -- wap-intranet
  2831, -- telemetrie
  2925, -- mobile ip vpn (50% discount)
  2926, -- mobile ip vpn (100% discount)
  2998, -- blackberry solution
  2999, -- blackberry solution (10% discount)
  3000  -- blackberry solution (20% discount)
);