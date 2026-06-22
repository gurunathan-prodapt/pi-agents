--
-- BigQuery DDL for sof.ta_bcp_iccid
-- Replaces Oracle table in job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh
--
-- NOTE: Column data types are inferred. Please verify and adjust according to actual source system schema.
--
CREATE TABLE IF NOT EXISTS `sof.ta_bcp_iccid`
(
    cntrct_id STRING,
    bpr_id STRING,
    cntrct_id_ref STRING,
    tn_iccid STRING,
    tn_imsi_hlr STRING
);