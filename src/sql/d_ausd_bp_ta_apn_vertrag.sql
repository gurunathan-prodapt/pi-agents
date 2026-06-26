-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
-- Description: Replaces the procedural PL/SQL cursor loop with an optimized, set-based BigQuery query.
--              Utilizes a temporary JavaScript UDF to enforce the exact 100-character boundary aggregation.

-- 1. Temporary UDF for robust, length-constrained string aggregation
CREATE TEMP FUNCTION aggregate_strings_with_limit(arr ARRAY<STRING>, max_len INT64)
RETURNS STRING
LANGUAGE js AS """
  if (!arr) return null;
  let accum = "";
  for (let i = 0; i < arr.length; i++) {
    let val = arr[i];
    if (val === null || val === undefined) continue;
    // Check if adding the value and the separator exceeds the maximum length
    if ((accum + val + ", ").length <= max_len) {
      accum += val + ", ";
    }
  }
  // Trim the trailing comma and space
  if (accum.endsWith(", ")) {
    accum = accum.slice(0, -2);
  }
  return accum === "" ? null : accum;
""";

-- 2. Populate target table via a set-based query
CREATE OR REPLACE TABLE `isbert_schema.sof$ta_apn_vertrag` AS
SELECT
  cntrct_id,
  aggregate_strings_with_limit(
    ARRAY_AGG(access_point_name ORDER BY bpr_id, cntrct_id_ref, access_point_name IGNORE NULLS),
    100
  ) AS access_point_name,
  aggregate_strings_with_limit(
    ARRAY_AGG(cntrct_id_ref ORDER BY bpr_id, cntrct_id_ref, access_point_name IGNORE NULLS),
    100
  ) AS cntrct_id_ref
FROM `isbert_schema.sof$ta_bpr_apn`
GROUP BY cntrct_id;