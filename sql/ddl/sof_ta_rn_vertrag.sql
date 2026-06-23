-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
-- BigQuery DDL for source table sof$ta_rn_vertrag
-- Placeholder DDL based on usage in d_ausd_bp_ta_bcp_msisdn.sql

CREATE TABLE IF NOT EXISTS `dataset.sof_ta_rn_vertrag` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract ID"),
    tn_tel_msisdn STRING NOT NULL OPTIONS(description="Telephone MSISDN")
    -- Add other columns if known from source system
);