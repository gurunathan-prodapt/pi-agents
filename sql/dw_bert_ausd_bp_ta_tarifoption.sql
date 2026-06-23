-- Migrated from Oracle SQL script: vobs/dw_source/isrpt/isbert/install_save/d_ausd_bp_ta_tarifoption.sql
-- Legacy job: DW.BERT_AUSD_BP_TA_TARIFOPTION

DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

DROP TABLE IF EXISTS `isbert_schema.sof$ta_bpr_opt_filter`;
DROP TABLE IF EXISTS `isbert_schema.sof$ta_tarifoption`;

CREATE TABLE `isbert_schema.sof$ta_bpr_opt_filter`
OPTIONS (
  description = 'nologging'
) AS
SELECT
  t.bpr_id,
  t.cntrct_id,
  t.pds_description,
  l.opt_kategorie
FROM `isbert_schema.sof$ta_l_bpr_optionen_filter` AS l,
     `isbert_schema.sof$ta_bpr_opt_text_` || v_datum AS t
WHERE t.bpr_id = l.bpr_id;

-- GRANT SELECT ON TABLE `isbert_schema.sof$ta_bpr_opt_filter` TO `isbert_schema`; -- BigQuery permissions are managed differently, usually via IAM roles.

CREATE TABLE `isbert_schema.sof$ta_tarifoption`
OPTIONS (
  description = 'nologging'
) AS
SELECT
  cntrct_id,
  RTRIM(SUBSTR(LTRIM(pds_des1, ', '), 1, 500)) AS business_option,
  RTRIM(SUBSTR(LTRIM(pds_des2, ', '), 1, 500)) AS sonstige_option,
  RTRIM(SUBSTR(LTRIM(pds_des3, ', '), 1, 500)) AS gprs_option
FROM (
  SELECT
    bpr_opt.cntrct_id,
    bpr_opt.bpr_id,
    LEAD(bpr_opt.cntrct_id, 1, -1) OVER (ORDER BY NULL) AS lagi,
    CASE
      WHEN bpr_opt.opt_kategorie = 'BUDGET' THEN CONCAT(bpr_opt.pds_description, CAST(bpr_opt.cntrct_id AS STRING))
      ELSE CONCAT(bpr_opt.pds_description, CAST(bpr_opt.cntrct_id AS STRING))
    END AS pds_des1,
    CASE
      WHEN bpr_opt.opt_kategorie = 'SONST' THEN CONCAT(bpr_opt.pds_description, CAST(bpr_opt.cntrct_id AS STRING))
      ELSE CONCAT(bpr_opt.pds_description, CAST(bpr_opt.cntrct_id AS STRING))
    END AS pds_des2,
    CASE
      WHEN bpr_opt.opt_kategorie = 'GPRS' THEN CONCAT(bpr_opt.pds_description, CAST(bpr_opt.cntrct_id AS STRING))
      ELSE CONCAT(bpr_opt.pds_description, CAST(bpr_opt.cntrct_id AS STRING))
    END AS pds_des3
  FROM (
    SELECT
      bpr_id,
      cntrct_id,
      pds_description,
      opt_kategorie
    FROM `isbert_schema.sof$ta_bpr_opt_filter`
    ORDER BY cntrct_id, pds_description
  ) AS bpr_opt
)
WHERE lagi > cntrct_id OR lagi = -1;

-- GRANT SELECT ON TABLE `isbert_schema.sof$ta_tarifoption` TO `isbert_schema`; -- BigQuery permissions are managed differently, usually via IAM roles.