-- BigQuery DDL for sof_ta_bpr_bcp
-- Legacy Source: Oracle table sof$ta_bpr_bcp
-- Job ID: DW.BERT_AUSD_BP_TA_BCP_ICCID

CREATE TABLE IF NOT EXISTS `<project>.<dataset>.sof_ta_bpr_bcp` (
    cntrct_id STRING,
    bpr_id STRING,
    cntrct_id_ref STRING
);