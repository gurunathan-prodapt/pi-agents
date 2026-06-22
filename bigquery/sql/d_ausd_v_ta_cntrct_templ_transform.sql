--
-- BigQuery SQL transformation logic for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: d_ausd_v_ta_cntrct_templ.sql (Oracle SQL*Plus)
--
-- Declare processing date variable
DECLARE v_datum STRING DEFAULT (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `your_project.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate target table
TRUNCATE TABLE `your_project.your_dataset.sof_ta_cntrct_templ`;

-- Insert data into target table
INSERT INTO `your_project.your_dataset.sof_ta_cntrct_templ`
(
  CNTRCT_TEMPLATE_ID,
  CDS_DESCRIPTION_ID,
  CDS_DESCRIPTION
)
SELECT
  ct.cntrct_template_id,
  ct.cds_description_id,
  cd.cds_description
FROM `your_project.your_dataset.cds_ta_cntrct_template` ct
JOIN `your_project.your_dataset.cds_ta_care_description` cd
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.is_production = 1
  AND cd.language = 1;