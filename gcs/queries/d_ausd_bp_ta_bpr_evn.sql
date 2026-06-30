-- ===================================================================
-- Target Table: sof_ta_bpr_evn
-- Source Table: sof_ta_bpr_instance
-- Description : Truncates target and inserts filtered EVN basis products
--               derived from legacy d_ausd_bp_ta_bpr_evn.sql.
-- ===================================================================

-- Step 1: Truncate Target Table
TRUNCATE TABLE `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`;

-- Step 2: Insert Filtered EVN Basis Product Instances
INSERT INTO `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn` (cntrct_id, bpr_id)
SELECT
    bp.cntrct_id,
    bp.bpr_id
FROM `gcp-bert-prd.bert_dataset.sof_ta_bpr_instance` AS bp
WHERE bp.bpr_id IN (
    32,    -- standard-evn
    2506,  -- komfort-evn
    2839,  -- standard-evn separat
    2840,  -- komfort-evn separat
    3055,  -- komfort-plus-evn
    3056,  -- komfort-plus-evn separat
    3821   -- standard-plus-evn
);