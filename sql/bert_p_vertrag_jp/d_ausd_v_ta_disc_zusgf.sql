CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_disc_zusgf` AS
WITH prepped_discounts AS (
  SELECT DISTINCT
    cntrct_id,
    cntrct_obj_version,
    CONCAT(rabatt, ' (', rabatthoehe, '%)') AS rabatt_agg_val
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_discount`
),
aggregated_discounts AS (
  SELECT
    cntrct_id,
    cntrct_obj_version,
    STRING_AGG(rabatt_agg_val, ', ' ORDER BY rabatt_agg_val) AS rabatt_alle
  FROM prepped_discounts
  GROUP BY cntrct_id, cntrct_obj_version
),
distinct_groups AS (
  SELECT DISTINCT
    cntrct_id,
    disc_vector_ty,
    cntrct_obj_version
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_discount`
)
SELECT
  dg.cntrct_id,
  dg.cntrct_obj_version,
  dg.disc_vector_ty,
  ad.rabatt_alle
FROM distinct_groups dg
LEFT JOIN aggregated_discounts ad
  ON dg.cntrct_id = ad.cntrct_id
 AND dg.cntrct_obj_version = ad.cntrct_obj_version;