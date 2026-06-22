--
-- Legacy Source: N/A (table definitions inferred from d_ausd_bp_ta_apn_vertrag.sql)
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
--
-- DDL for creating target BigQuery tables for DW.BERT_AUSD_BP_TA_APN_VERTRAG job.

-- Create dataset for isbert_schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `project.isbert_schema`;

-- Create dataset for sof if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `project.sof`;

-- Table: isbert_schema.dwtk_meldungen
-- Inferred schema from usage in d_ausd_bp_ta_apn_vertrag.sql
CREATE TABLE IF NOT EXISTS `project.isbert_schema.dwtk_meldungen` (
    timecreated TIMESTAMP,
    job_kennung STRING
);

-- Table: sof.ta_bpr_apn
-- Inferred schema from usage in d_ausd_bp_ta_apn_vertrag.sql
CREATE TABLE IF NOT EXISTS `project.sof.ta_bpr_apn` (
    cntrct_id_ref STRING(100), -- Max length 100 from substr in Oracle script
    bpr_id INT64,              -- Inferred as an ID type
    cntrct_id STRING(10),      -- Max length 10 from v_cntrct declaration
    access_point_name STRING(100) -- Max length 100 from substr in Oracle script
);

-- Table: sof.ta_apn_vertrag
-- Inferred schema from usage in d_ausd_bp_ta_apn_vertrag.sql
CREATE TABLE IF NOT EXISTS `project.sof.ta_apn_vertrag` (
    cntrct_id STRING(10),       -- Max length 10
    apn_list STRING(100),       -- Max length 100
    cntrct_ref_list STRING(100) -- Max length 100
);