-- BigQuery DDL for raw_isbert.cds_ta_disc_vector
-- Legacy source: Oracle table inferred from d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `your_project.raw_isbert.cds_ta_disc_vector` (
    CALC_RULE_VALUE NUMERIC,
    discount_id INT64,
    disc_vector_ty STRING,
    discount_obj_version INT64,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP
);