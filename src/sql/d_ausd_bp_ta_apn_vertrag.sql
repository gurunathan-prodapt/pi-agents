-- Legacy Source File: d_ausd_bp_ta_apn_vertrag.sql
-- Legacy Job: ausd_bp_ta_apn_vertrag
-- Replaces: PL/SQL aggregation loop logic
--
-- This script performs the aggregation and pivoting of APN and contract references
-- per contract ID, using a Javascript UDF to strictly mirror the legacy PL/SQL cursor's
-- 100-character limit fitting behavior (Option B).

CREATE TEMP FUNCTION aggregate_limited(arr ARRAY<STRING>, delimiter STRING, max_len INT64)
RETURNS STRING
LANGUAGE js AS r"""
  if (!arr) return null;
  let result = "";
  for (let i = 0; i < arr.length; i++) {
    let item = arr[i];
    if (!item) continue;
    let next_str = result ? result + delimiter + item : item;
    if (next_str.length <= max_len) {
      result = next_str;
    } else {
      continue;
    }
  }
  return result;
""";

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.gcp_dataset }}.sof_ta_apn_vertrag` AS
SELECT
  cntrct_id,
  aggregate_limited(ARRAY_AGG(access_point_name ORDER BY access_point_name), ', ', 100) AS apn,
  aggregate_limited(ARRAY_AGG(cntrct_id_ref ORDER BY cntrct_id_ref), ', ', 100) AS cntrct_ref
FROM
  `{{ var.value.gcp_project }}.{{ var.value.gcp_dataset }}.sof_ta_bpr_apn`
GROUP BY
  cntrct_id;