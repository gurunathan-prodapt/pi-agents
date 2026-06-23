-- BigQuery DDL for raw_isbert.cds_ta_discount_bc_assoc
-- Legacy source: Oracle table inferred from d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `your_project.raw_isbert.cds_ta_discount_bc_assoc` (
    cntrct_id INT64,
    discount_id INT64,
    cntrct_obj_version INT64,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP
);