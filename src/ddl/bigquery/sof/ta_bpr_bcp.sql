--
-- BigQuery DDL for sof.ta_bpr_bcp
-- Replaces Oracle table in job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh
--
-- NOTE: Column data types are inferred. Please verify and adjust according to actual source system schema.
--
CREATE TABLE IF NOT EXISTS `sof.ta_bpr_bcp`
(
    cntrct_id STRING,       -- Inferred from JOIN and SELECT list
    bpr_id STRING,          -- Inferred from SELECT list
    cntrct_id_ref STRING    -- Inferred from JOIN condition
);