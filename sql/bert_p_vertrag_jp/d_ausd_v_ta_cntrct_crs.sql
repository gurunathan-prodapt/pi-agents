DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_cntrct_crs` AS
SELECT
  c.cntrct_id,
  c.obj_version,
  c.contract_number,
  c.cntrct_template_id,
  c.cntrct_validity_id,
  c.valid_from,
  c.com_per_ext_rea_cv,
  c.billcycle_id,
  c.vo_code,
  c.cntrct_start_date,
  c.cntrct_st,
  c.cntrct_parent,
  c.cntrct_ty,
  c.cost_centre,
  c.cost_centre_user,
  c.commitment_reference_date,
  c.order_number,
  c.insert_at AS bfc_age
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_cntrct` c
WHERE
  c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND FORMAT_TIMESTAMP('%Y%m%d', c.insert_at) <= v_datum
  AND (c.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', c.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', c.valid_from) <= v_datum
  AND (c.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', c.valid_to) > v_datum)
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL);