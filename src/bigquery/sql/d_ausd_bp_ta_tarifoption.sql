-- Legacy Source: d_ausd_bp_ta_tarifoption.sql
-- Job: DW.BERT_AUSD_BP_TA_TARIFOPTION
DECLARE v_datum STRING;

-- Determine the dynamic date string for table naming and other purposes
-- This logic was originally in the Oracle script to define v_datum.
SET v_datum = (
  SELECT
    IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM
    `isbert_schema.dwtk_meldungen` m
  WHERE
    m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 01: Drop temporary tables if they exist
-- Original: drop table sof$ta_bpr_opt_filter;
-- Original: drop table sof$ta_tarifoption;
DROP TABLE IF EXISTS `isbert_schema.sof_ta_bpr_opt_filter`;
DROP TABLE IF EXISTS `isbert_schema.sof_ta_tarifoption`;

-- Step 09b: Create intermediate table sof$ta_bpr_opt_filter
-- Original: create table sof$ta_bpr_opt_filter ... as select ...
-- BigQuery tables are typically managed via `CREATE TABLE` rather than `CREATE TEMPORARY TABLE`
-- for persistent intermediate results or for results that might be queried later.
-- Dynamic table name `sof$ta_bpr_opt_text_&v_datum` will be handled by EXECUTE IMMEDIATE.
EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE `isbert_schema.sof_ta_bpr_opt_filter`
OPTIONS(
    description = 'Intermediate filter table for tarifoption processing.'
) AS
SELECT
       t.bpr_id,
       t.cntrct_id,
       t.pds_description,
       l.opt_kategorie
FROM
  `isbert_schema.sof_ta_l_bpr_optionen_filter` l,
  `isbert_schema.sof_ta_bpr_opt_text_%s` t -- Dynamic table name
WHERE
  t.bpr_id = l.bpr_id;
""", v_datum);


-- Step 09c: Populate final table sof$ta_tarifoption
-- Original: create table sof$ta_tarifoption ... as select ...
CREATE TABLE `isbert_schema.sof_ta_tarifoption`
OPTIONS(
    description = 'Target table for tariff options (SOF$TA_TARIFOPTION).'
) AS
SELECT
       cntrct_id,
       -- RTRIM(SUBSTR(LTRIM(pds_des1,', '),1,500)) as business_option
       RTRIM(SUBSTR(LTRIM(pds_des1, ', '), 1, 500)) AS business_option,
       -- RTRIM(SUBSTR(LTRIM(pds_des2,', '),1,500)) as sonstige_option
       RTRIM(SUBSTR(LTRIM(pds_des2, ', '), 1, 500)) AS sonstige_option,
       -- RTRIM(SUBSTR(LTRIM(pds_des3,', '),1,500)) as gprs_option
       RTRIM(SUBSTR(LTRIM(pds_des3, ', '), 1, 500)) AS gprs_option
FROM
(
       SELECT
              bpr_opt.cntrct_id,
              bpr_opt.bpr_id,
              -- LEAD(..., 1, -1) OVER (ORDER BY NULL) becomes LEAD(..., 1, -1) OVER ()
              LEAD(bpr_opt.cntrct_id, 1, -1) OVER () AS lagi,
              -- pds_des1 = business_option
              CASE WHEN bpr_opt.opt_kategorie = 'BUDGET'
                   THEN `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id)
                   ELSE `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id) -- Placeholder for concat1r
              END AS pds_des1,
              -- pds_des2 = sonstige_option
              CASE WHEN bpr_opt.opt_kategorie = 'SONST'
                   THEN `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id)
                   ELSE `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id) -- Placeholder for concat2r
              END AS pds_des2,
              -- pds_des3 = gprs_option
              CASE WHEN bpr_opt.opt_kategorie = 'GPRS'
                   THEN `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id)
                   ELSE `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id) -- Placeholder for concat3r
              END AS pds_des3
       FROM
       (
              SELECT
                     bpr_id,
                     cntrct_id,
                     pds_description,
                     opt_kategorie
              FROM
                     `isbert_schema.sof_ta_bpr_opt_filter`
              -- Sortiert Lesen aus Zwischentabelle für LEAD-Funktion (Gruppenwechsel)
              -- ORDER BY here implies a specific order for LEAD which needs to be verified
              ORDER BY cntrct_id, pds_description
       ) AS bpr_opt
)
WHERE lagi > cntrct_id OR lagi = -1;