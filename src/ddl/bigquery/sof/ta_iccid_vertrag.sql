--
-- BigQuery DDL for sof.ta_iccid_vertrag
-- Replaces Oracle table in job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh
--
-- NOTE: Column data types are inferred. Please verify and adjust according to actual source system schema.
--
CREATE TABLE IF NOT EXISTS `sof.ta_iccid_vertrag`
(
    cntrct_id STRING,       -- Inferred from JOIN condition
    tn_iccid STRING,        -- Inferred from SELECT list
    tn_imsi_hlr STRING      -- Inferred from SELECT list
);