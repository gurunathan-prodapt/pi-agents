-- BigQuery DDL for sof_ta_bcp_iccid
-- Legacy Source: Oracle table sof$ta_bcp_iccid
-- Job ID: DW.BERT_AUSD_BP_TA_BCP_ICCID

CREATE TABLE IF NOT EXISTS `<project>.<dataset>.sof_ta_bcp_iccid` (
    CNTRCT_ID STRING,
    BPR_ID STRING,
    CNTRCT_ID_REF STRING,
    TN_ICCID STRING,
    TN_IMSI_HLR STRING
)
-- Recommendation from design: Consider partitioning and clustering strategies for performance,
-- especially for the CNTRCT_ID_REF column used in the join.
-- Example:
-- PARTITION BY RANGE_BUCKET(CNTRCT_ID_REF, GENERATE_ARRAY(0, 1000000, 10000))
-- CLUSTER BY CNTRCT_ID_REF
;