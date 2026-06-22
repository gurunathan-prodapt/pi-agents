-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 04 - Part 1 - Populate business partner and related contract partner tables.
-- Replaces Oracle Step 04 (4a, 4b_nodp, 4b_final) sections from original SQL*Plus script.

-- Parameter:
--   @stichtag_yyyymmdd: The snapshot date in 'YYYYMMDD' format.

-- step04a: business_partner
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt`
(
  BP_ID,
  ORGANISATION_NAME,
  TITLE,
  SURNAME,
  FIRST_NAME,
  SALES_TAX_FREED,
  TM_CUSTOMERID
)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_bpd_business_partner` AS bp
WHERE
  (bp.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
    AND (bp.modified_at IS NULL
      OR bp.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
;


-- step04b: separate business-partner tables for contract partners (Geschaeftspartner) - nodp
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp`
(
  BP_ID
)
SELECT DISTINCT
  ref_gp.bp_id
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp` AS ref_gp
;


-- step04b: separate business-partner tables for contract partners (Geschaeftspartner) - final
INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_gp`
(
  BP_ID,
  ORGANISATION_NAME,
  TITLE,
  SURNAME,
  FIRST_NAME,
  SALES_TAX_FREED,
  TM_CUSTOMERID
)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp` AS br
JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt` AS bp
ON
  br.bp_id = bp.bp_id
;