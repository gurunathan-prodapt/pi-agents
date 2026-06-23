-- BigQuery SQL for d_ausd_bp_ta_apn_carmen.sql
-- Replaces Oracle SQL script used by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

-- BigQuery Script

DECLARE v_carmen STRING DEFAULT '@pcrs1'; -- Placeholder for external system reference
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `your_project.your_dataset.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- step01: delete temporary table contents
TRUNCATE TABLE `your_project.your_dataset.sof$ta_apn_carmen`;

-- step10a: create local copy of carmen-apn table
INSERT INTO `your_project.your_dataset.sof$ta_apn_carmen`
  (CNTRCT_ID, ACCESS_POINT_NAME)
SELECT
  pca.cntrct_id,
  ap.access_point_name
FROM `your_project.your_dataset.pds$ta_pdp_context_assoc` pca
JOIN `your_project.your_dataset.pds$ta_pdp_context` pc
  ON pca.pdp_context_id = pc.pdp_context_id
JOIN `your_project.your_dataset.pds$ta_access_point` ap
  ON pc.access_point_id = ap.access_point_id
WHERE pca.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pca.modified_at IS NULL OR pca.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pca.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pca.valid_to IS NULL OR pca.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND pc.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pc.modified_at IS NULL OR pc.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pc.is_production = 1
  AND ap.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ap.modified_at IS NULL OR ap.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pca.cntrct_id IS NOT NULL;