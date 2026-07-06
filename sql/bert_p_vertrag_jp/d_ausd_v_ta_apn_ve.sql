DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_apn_ve` AS
SELECT
  pca.cntrct_id,
  ap.access_point_name
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.pds_ta_pdp_context_assoc` pca
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.pds_ta_pdp_context` pc
  ON pca.pdp_context_id = pc.pdp_context_id
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.pds_ta_access_point` ap
  ON pc.access_point_id = ap.access_point_id
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', pca.insert_at) <= v_datum
  AND (pca.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', pca.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', pca.valid_from) <= v_datum
  AND (pca.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', pca.valid_to) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', pc.insert_at) <= v_datum
  AND (pc.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', pc.modified_at) > v_datum)
  AND pc.is_production = 1
  AND FORMAT_TIMESTAMP('%Y%m%d', ap.insert_at) <= v_datum
  AND (ap.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', ap.modified_at) > v_datum)
  AND pca.cntrct_id IS NOT NULL;