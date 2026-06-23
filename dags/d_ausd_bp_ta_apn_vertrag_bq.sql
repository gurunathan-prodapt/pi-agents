-- BigQuery Standard SQL migration of d_ausd_bp_ta_apn_vertrag.sql
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
--
-- This script processes APN and contract reference data, aggregates them,
-- and inserts the results into a target BigQuery table.

-- Declare a variable for the processing date (v_datum), derived from `dwtk_meldungen`.
-- This mimics the Oracle SQL's `SELECT NVL(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101')` logic.
DECLARE v_datum STRING DEFAULT (
  SELECT
    COALESCE(
      FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))),
      '19000101'
    )
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate the target table `sof_ta_apn_vertrag` before inserting new data.
-- This replaces `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_apn_vertrag')`
TRUNCATE TABLE `sof_ta_apn_vertrag`;

-- Insert aggregated APN and contract reference data into the target table.
-- This replaces the cursor-based loop and row-by-row processing with set-based operations.
-- STRING_AGG is used for concatenation, and SUBSTR/TRIM for length constraints,
-- mirroring Oracle's SUBSTR(RTRIM(...), 1, 100).
INSERT INTO `sof_ta_apn_vertrag` (cntrct_id, apn_list, cntrct_ref_list)
SELECT
  cntrct_id,
  -- Aggregates access_point_name values into a comma-separated string,
  -- trims trailing commas and spaces, and truncates to 100 characters.
  SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(access_point_name, ', ' ORDER BY access_point_name)), 1, 100) AS apn_list,
  -- Aggregates cntrct_id_ref values into a comma-separated string,
  -- trims trailing commas and spaces, and truncates to 100 characters.
  SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref)), 1, 100) AS cntrct_ref_list
FROM `sof_ta_bpr_apn` -- Source table containing APN and contract data
GROUP BY cntrct_id      -- Group by contract ID to aggregate related APNs and references
ORDER BY cntrct_id;     -- Order for consistent output (optional, but good practice for determinism)