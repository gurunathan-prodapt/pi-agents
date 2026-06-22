--
-- BigQuery DDL for isbert_schema.dwtk_meldungen
-- Replaces Oracle table in job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh
--
-- NOTE: Column data types are inferred. Please verify and adjust according to actual source system schema.
--
CREATE TABLE IF NOT EXISTS `isbert_schema.dwtk_meldungen`
(
    timecreated TIMESTAMP, -- Inferred from MAX(m.timecreated) usage, assuming Oracle DATE/TIMESTAMP
    job_kennung STRING     -- Inferred from WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);