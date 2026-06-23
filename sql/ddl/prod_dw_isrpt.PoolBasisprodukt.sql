-- BigQuery DDL for the target table PoolBasisprodukt
-- Replaces output of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Based on the structure implied by d_ausd_bp_ta_bpr_apn.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE SCHEMA IF NOT EXISTS prod_dw_isrpt;

CREATE TABLE IF NOT EXISTS prod_dw_isrpt.PoolBasisprodukt
(
    CNTRCT_ID STRING,
    BPR_ID INT64,
    CNTRCT_ID_REF STRING,
    ACCESS_POINT_NAME STRING
)
OPTIONS(
    description="Target table for Basisprodukt data, migrated from Oracle/KornShell process."
);