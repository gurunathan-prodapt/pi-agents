-- BigQuery SQL transformation script
-- Replaces legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql
-- Job: DW.BERT_AUSD_V_TA_P_DISCOUNT_RR

-- Truncate the target table before inserting new data
TRUNCATE TABLE `your-gcp-project-id.your_bigquery_dataset.sof_ta_p_discount_rr`;

-- Insert data into the target table by joining source tables
INSERT INTO `your-gcp-project-id.your_bigquery_dataset.sof_ta_p_discount_rr` (
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
    std_vertrag
)
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
    `your-gcp-project-id.your_bigquery_dataset.sof_ta_discount_rr` AS da
JOIN
    `your-gcp-project-id.your_bigquery_dataset.sof_ta_cntrct_crs` AS c
ON
    da.cntrct_id = c.cntrct_id
    AND da.cntrct_obj_version = c.obj_version
JOIN
    `your-gcp-project-id.your_bigquery_dataset.sof_ta_cntrct_templ` AS ct
ON
    da.cntrct_template_id = ct.cntrct_template_id;