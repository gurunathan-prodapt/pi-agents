-- BigQuery DDL for raw_isbert.cds_ta_disc_invoice_item
-- Legacy source: Oracle table inferred from d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `your_project.raw_isbert.cds_ta_disc_invoice_item` (
    disc_invoice_item_id INT64,
    cds_description_id INT64,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP
);