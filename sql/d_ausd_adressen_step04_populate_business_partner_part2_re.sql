-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 04 - Part 3 - Populate business partner and related invoice recipient tables.
-- Replaces Oracle Step 04 (4d_nodp, 4d_final) sections from original SQL*Plus script.

-- Parameter:
--   @stichtag_yyyymmdd: The snapshot date in 'YYYYMMDD' format.

-- step04d: separate business-partner tables for invoice recipients (Rechnungsempfänger) - nodp
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re_nodp`
(
  BP_ID
)
SELECT DISTINCT
  ref_re.bp_id
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re` AS ref_re
;


-- step04d: separate business-partner tables for invoice recipients (Rechnungsempfänger) - final
INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_re`
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
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re_nodp` AS br
JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt` AS bp
ON
  br.bp_id = bp.bp_id
;