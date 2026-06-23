-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
-- Job: BERT_V_TA_DISC_ZUSGF
-- This script transforms discount data from source tables into a concatenated format
-- and populates the `sof$ta_disc_zusgf` table in BigQuery.
-- It replaces Oracle PL/SQL custom types and pipelined functions with standard BigQuery SQL.

CREATE OR REPLACE TABLE `sof$ta_disc_zusgf` AS
WITH discount_base AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
    disc_vector_ty
  FROM `sof$ta_discount`
),
discount_concat_source AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
    CONCAT(CAST(rabatt AS STRING), ' (', CAST(rabatthoehe AS STRING), '%)') AS rabatt_text
  FROM `sof$ta_discount`
),
discount_agg AS (
  SELECT
    cntrct_id,
    cntrct_obj_version,
    STRING_AGG(rabatt_text, ', ' ORDER BY rabatt_text) AS rabatt_alle
  FROM discount_concat_source
  GROUP BY cntrct_id, cntrct_obj_version
)
SELECT
  d.cntrct_id,
  d.cntrct_obj_version,
  d.disc_vector_ty,
  a.rabatt_alle
FROM discount_base d
LEFT JOIN discount_agg a
  ON d.cntrct_id = a.cntrct_id
 AND d.cntrct_obj_version = a.cntrct_obj_version;