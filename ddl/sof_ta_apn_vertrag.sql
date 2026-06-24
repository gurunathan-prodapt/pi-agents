-- DDL for BigQuery table your_project.your_dataset.SOFTA_APN_VERTRAG
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.SOFTA_APN_VERTRAG` (
    cntrct_id STRING,
    aggregated_apn STRING,
    aggregated_cntrct_ref STRING
);