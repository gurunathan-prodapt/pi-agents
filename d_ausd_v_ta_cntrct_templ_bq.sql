-- This BigQuery SQL script replaces the legacy Oracle SQL script d_ausd_v_ta_cntrct_templ.sql
-- for job DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.

-- BigQuery SQL Script

DECLARE v_datum STRING DEFAULT (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `sof_ta_cntrct_templ`;

INSERT INTO `sof_ta_cntrct_templ`
(
  CNTRCT_TEMPLATE_ID,
  CDS_DESCRIPTION_ID,
  CDS_DESCRIPTION
)
SELECT
  ct.cntrct_template_id,
  ct.cds_description_id,
  cd.cds_description
FROM `cds_ta_cntrct_template` ct
JOIN `cds_ta_care_description` cd
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.is_production = 1
  AND cd.language = 1;