DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_inv_assign` AS
SELECT
  ia.cntrct_id,
  ia.inv_definition_id
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_inv_assignment` ia
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', ia.insert_at) <= v_datum
  AND (ia.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', ia.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', ia.valid_from) <= v_datum
  AND (ia.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', ia.valid_to) > v_datum)
  AND ia.is_production = 1;