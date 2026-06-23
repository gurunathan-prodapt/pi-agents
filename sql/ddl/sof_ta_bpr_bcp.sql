-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
-- BigQuery DDL for source table sof$ta_bpr_bcp
-- Placeholder DDL based on usage in d_ausd_bp_ta_bcp_msisdn.sql

CREATE TABLE IF NOT EXISTS `dataset.sof_ta_bpr_bcp` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract ID"),
    bpr_id STRING NOT NULL OPTIONS(description="Business Partner ID"),
    cntrct_id_ref STRING NOT NULL OPTIONS(description="Reference Contract ID")
    -- Add other columns if known from source system
);