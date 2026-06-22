-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 05 - Populate the final regulierer table.
-- Replaces Oracle Step 05 section from original SQL*Plus script.

-- Parameter:
--   @stichtag_yyyymmdd: The snapshot date in 'YYYYMMDD' format.

INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_regulierer`
(
  INV_DEF_MOPREF_ID,
  MOP_BP_ID,
  MEANS_OF_PAYMENT_ID
)
SELECT
  bpr.inv_def_mopref_id,
  bpr.mop_bp_id,
  bpr.means_of_payment_id
FROM
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_bp_ref` AS bpr
WHERE
  (bpr.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
    AND (bpr.modified_at IS NULL
      OR bpr.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
  AND (bpr.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
    AND (bpr.valid_to IS NULL
      OR bpr.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 2
  AND bpr.mop_ref_ty = 1
;