-- BigQuery DDL for curated_rpt.sof_ta_discount_rr (Target Table)
-- Legacy source: sof$ta_discount_rr from d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `your_project.curated_rpt.sof_ta_discount_rr` (
    cntrct_id INT64,
    discount_id INT64,
    disc_vector_ty STRING,
    cntrct_obj_version INT64,
    cntrct_template_id INT64,
    disc_invoice_item_id INT64,
    rabatt STRING,
    rabatthoehe NUMERIC,
    rabattierte_rech_pos STRING
);