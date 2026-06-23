-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
-- BigQuery DDL for target table sof$ta_bcp_msisdn
-- Based on the INSERT statement in d_ausd_bp_ta_bcp_msisdn.sql

CREATE TABLE IF NOT EXISTS `dataset.sof_ta_bcp_msisdn` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract ID"),
    bpr_id STRING NOT NULL OPTIONS(description="Business Partner ID"),
    cntrct_id_ref STRING NOT NULL OPTIONS(description="Reference Contract ID"),
    tn_tel_msisdn STRING NOT NULL OPTIONS(description="Telephone MSISDN")
);