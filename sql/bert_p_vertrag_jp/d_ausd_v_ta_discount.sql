DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_discount` AS
SELECT
  da.cntrct_id,
  da.discount_id,
  d.disc_vector_ty,
  da.cntrct_obj_version,
  cd.cds_description AS rabatt,
  CAST(dv.CALC_RULE_VALUE AS STRING) AS rabatthoehe
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_discount_bc_assoc` da
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_discount` d
  ON da.discount_id = d.discount_id
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_care_description` cd
  ON cd.cds_description_id = d.cds_description_id
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_disc_vector` dv
  ON d.discount_id = dv.discount_id
 AND d.disc_vector_ty = dv.disc_vector_ty
 AND d.obj_version = dv.discount_obj_version
WHERE
  cd.LANGUAGE = 1
  AND FORMAT_TIMESTAMP('%Y%m%d', da.insert_at) <= v_datum
  AND (da.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', da.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', d.insert_at) <= v_datum
  AND (d.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', d.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', d.valid_from) <= v_datum
  AND (d.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', d.valid_to) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', dv.insert_at) <= v_datum
  AND (dv.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', dv.modified_at) > v_datum)
  AND d.is_production = 1;