DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_cntrct_valid` AS
SELECT
  cv.cntrct_validity_id,
  cv.first_period_id,
  cv.following_period_id,
  cv.first_notice_period_id,
  cv.follow_notice_period_id,
  cv.insert_at AS bfc_age
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_cntrct_validity` cv
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', cv.insert_at) <= v_datum
  AND (cv.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', cv.modified_at) > v_datum);