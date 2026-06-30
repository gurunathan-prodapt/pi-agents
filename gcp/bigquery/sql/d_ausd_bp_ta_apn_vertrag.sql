-- gcp/bigquery/sql/d_ausd_bp_ta_apn_vertrag.sql
-- This SQL replaces the legacy Oracle PL/SQL cursor-loop logic.
-- It aggregates and concatenates active basic products (APN and contract reference values)
-- from sof_ta_bpr_apn and truncates the resulting strings to 100 characters, storing them
-- in sof_ta_apn_vertrag.

TRUNCATE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_apn_vertrag`;

INSERT INTO `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_apn_vertrag` (
  cntrct_id,
  access_point_names,
  cntrct_id_refs
)
WITH ordered_source AS (
  SELECT
    cntrct_id,
    cntrct_id_ref,
    bpr_id,
    access_point_name,
    ROW_NUMBER() OVER (
      PARTITION BY cntrct_id
      ORDER BY cntrct_id_ref, access_point_name, bpr_id
    ) AS rn
  FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_apn`
),
grouped AS (
  SELECT
    cntrct_id,
    STRING_AGG(access_point_name, ', ' ORDER BY rn) AS access_point_names,
    STRING_AGG(cntrct_id_ref, ', ' ORDER BY rn) AS cntrct_id_refs
  FROM ordered_source
  GROUP BY cntrct_id
)
SELECT
  cntrct_id,
  SUBSTR(RTRIM(access_point_names, ', '), 1, 100) AS access_point_names,
  SUBSTR(RTRIM(cntrct_id_refs, ', '), 1, 100) AS cntrct_id_refs
FROM grouped;