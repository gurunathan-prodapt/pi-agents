-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh

-- This script performs data transformation for ta_p_discount_rr.
-- The original Oracle SQL*Plus commands and hints have been removed.
-- Replace `your_bigquery_project.your_bigquery_dataset` with your actual BigQuery project and dataset.

INSERT INTO `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr` (
        cntrct_id,
        discount_id,
        disc_vector_ty,
        cntrct_obj_version,
        cntrct_template_id,
        disc_invoice_item_id,
        rabatt,
        rabatthoehe,
        rabattierte_rech_pos,
        contract_number,
        std_vertrag)
SELECT
        da.cntrct_id,
        da.discount_id,
        da.disc_vector_ty,
        da.cntrct_obj_version,
        da.cntrct_template_id,
        da.disc_invoice_item_id,
        da.rabatt,
        da.rabatthoehe,
        da.rabattierte_rech_pos,
        c.contract_number,
        ct.cds_description AS std_vertrag
FROM
        `your_bigquery_project.your_bigquery_dataset.sof_ta_discount_rr`   da,
        `your_bigquery_project.your_bigquery_dataset.sof_ta_cntrct_crs`    c,
        `your_bigquery_project.your_bigquery_dataset.sof_ta_cntrct_templ`  ct
WHERE
        da.cntrct_id            = c.cntrct_id
AND     da.cntrct_obj_version   = c.obj_version
AND     da.cntrct_template_id   = ct.cntrct_template_id;

-- The original script had a section to derive a 'v_datum' from 'isbert_schema.dwtk_meldungen'.
-- This variable was not used in the provided INSERT statement.
-- If 'v_datum' is required for other parts of the overall process
-- (e.g., for data partitioning or other filtering), it should be handled
-- in the orchestrating Airflow DAG or as a separate BigQuery query/parameter.
-- Example of BigQuery equivalent for v_datum logic if needed elsewhere:
-- SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
-- FROM `your_bigquery_project.your_bigquery_dataset.dwtk_meldungen` m
-- WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';