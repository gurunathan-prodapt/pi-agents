-- BigQuery DDL for pds$ta_pdp_context_assoc
-- Replaces Oracle table used by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pds$ta_pdp_context_assoc` (
    cntrct_id STRING,
    pdp_context_id INT64,
    insert_at DATE,
    modified_at DATE,
    valid_from DATE,
    valid_to DATE
);