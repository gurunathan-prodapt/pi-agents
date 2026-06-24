-- DDL for BigQuery table your_project.your_dataset.SOFTA_BPR_APN
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.SOFTA_BPR_APN` (
    cntrct_id STRING,
    bpr_id STRING,
    cntrct_id_ref STRING,
    access_point_name STRING
);