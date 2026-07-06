DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_acc_ref` AS
SELECT
  ar.acc_ref_id,
  ar.account_reference
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_acc_ref` ar
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', ar.insert_at) <= v_datum
  AND (ar.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', ar.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', ar.valid_from) <= v_datum
  AND (ar.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', ar.valid_to) > v_datum)
  AND ar.is_production = 1;