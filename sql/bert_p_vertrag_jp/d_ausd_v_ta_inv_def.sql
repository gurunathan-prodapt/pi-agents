DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_inv_def` AS
SELECT
  id.inv_definition_id,
  id.acc_ref_id,
  id.inv_pay_ty_cv,
  id.inv_media_cv,
  id.billcycle_id,
  id.sales_tax_freed,
  id.inv_cont_config_id,
  d.cds_description AS rechn_inh_konfig_text
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_inv_definition` id
LEFT OUTER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_inv_cont_config` icc
  ON id.inv_cont_config_id = icc.inv_cont_config_id
LEFT OUTER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_care_description` d
  ON icc.cds_description_id = d.cds_description_id
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', id.insert_at) <= v_datum
  AND (id.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', id.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', id.valid_from) <= v_datum
  AND (id.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', id.valid_to) > v_datum)
  AND id.is_production = 1
  AND (icc.insert_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', icc.insert_at) <= v_datum)
  AND COALESCE(FORMAT_TIMESTAMP('%Y%m%d', icc.modified_at), '99991231') > v_datum
  AND (icc.valid_from IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', icc.valid_from) <= v_datum)
  AND COALESCE(FORMAT_TIMESTAMP('%Y%m%d', icc.valid_to), '99991231') > v_datum
  AND COALESCE(icc.is_production, 1) = 1;