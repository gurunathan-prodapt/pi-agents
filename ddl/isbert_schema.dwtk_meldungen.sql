-- BigQuery DDL for source table isbert_schema.dwtk_meldungen
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql

CREATE TABLE IF NOT EXISTS `isbert_schema.dwtk_meldungen`
(
    timecreated TIMESTAMP OPTIONS(description="Timestamp when the message was created."),
    job_kennung   STRING    OPTIONS(description="Identifier for the job type."),
    -- Additional columns as per source system if needed
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp in BigQuery.")
);