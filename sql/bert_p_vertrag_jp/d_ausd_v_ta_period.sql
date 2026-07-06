DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_period` AS
SELECT
  p.period_id,
  p.number_time_measurement,
  p.time_meas_cv,
  d.description AS einheit,
  p.insert_at AS bfc_age
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_period` p
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_time_meas_cv` tm
  ON tm.time_meas_cv = p.time_meas_cv
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_description` d
  ON tm.description_id = d.description_id
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', p.insert_at) <= v_datum
  AND (p.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', p.modified_at) > v_datum);