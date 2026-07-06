DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_bp_ref` AS
SELECT
  br.cntrct_cp2_id,
  br.bp_id
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_bp_ref` br
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', br.insert_at) <= v_datum
  AND (br.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', br.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', br.valid_from) <= v_datum
  AND (br.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', br.valid_to) > v_datum)
  AND br.is_production = 1
  AND br.bp_ref_ty = 4;