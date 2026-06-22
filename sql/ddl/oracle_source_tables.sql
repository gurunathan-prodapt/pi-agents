-- DDL for BigQuery source tables for job EXIS_SD_APT_RABATT
-- This file defines the schema for tables that mirror the Oracle source.
-- Ensure the dataset `oracle_source` exists in your BigQuery project.

CREATE TABLE IF NOT EXISTS `{{ project_id }}.oracle_source.RPT_TA_S_D1_VERTRAG` (
    RAHMENVERTRAG_ID STRING,
    DWH_TARIFGR_TEXT STRING,
    SV_ID STRING,
    VERTRAG_ID_CARMEN STRING
);

CREATE TABLE IF NOT EXISTS `{{ project_id }}.oracle_source.RPT_TA_S_D1_DISCOUNT_RR` (
    CONTRACT_NUMBER STRING,
    CNTRCT_TEMPLATE_ID STRING,
    RABATTIERTE_RECH_POS STRING,
    DISC_INVOICE_ITEM_ID STRING,
    RABATTHOEHE NUMERIC
);

CREATE TABLE IF NOT EXISTS `{{ project_id }}.oracle_source.SOF_TA_BPR_OPTIONEN` (
    CNTRCT_ID STRING,
    BPR_ID STRING
);

CREATE TABLE IF NOT EXISTS `{{ project_id }}.oracle_source.SOF_VI_L_OPTIONZUORDNUNG` (
    OPTION_ID STRING
);