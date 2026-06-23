-- BigQuery SQL transformation for d_ausd_v_ta_disc_zusgf.sql
-- Replaces logic in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
-- and orchestrated by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
--
-- NOTE: Replace `your_gcp_project_id` with your actual Google Cloud Project ID for all table references.

DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `your_gcp_project_id.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

INSERT INTO `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf`
  (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle)
WITH dzg AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
    disc_vector_ty
  FROM `your_gcp_project_id.raw_sof.sof$ta_discount`
),
con AS (
  SELECT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
    STRING_AGG(rabatt_text, ', ' ORDER BY rabatt_text) AS rabatt_alle
  FROM (
    SELECT DISTINCT
      CAST(cntrct_id AS INT64) AS cntrct_id,
      CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
      CONCAT(CAST(rabatt AS STRING), ' (', CAST(rabatthoehe AS STRING), '%)') AS rabatt_text
    FROM `your_gcp_project_id.raw_sof.sof$ta_discount`
  )
  GROUP BY cntrct_id, cntrct_obj_version
)
SELECT
  dzg.cntrct_id,
  dzg.cntrct_obj_version,
  dzg.disc_vector_ty,
  con.rabatt_alle
FROM dzg
LEFT JOIN con
  ON dzg.cntrct_id = con.cntrct_id
 AND dzg.cntrct_obj_version = con.cntrct_obj_version;