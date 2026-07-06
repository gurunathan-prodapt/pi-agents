DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_action_assoc` AS
SELECT
  ac.cntrct_id,
  ac.rv_action_id
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_action_assoc` ac
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', ac.insert_at) <= v_datum
  AND FORMAT_TIMESTAMP('%Y%m%d', ac.valid_from) <= v_datum
  AND ac.is_production = 1
  AND (ac.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', ac.modified_at) > v_datum)
  AND (ac.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', ac.valid_to) > v_datum);