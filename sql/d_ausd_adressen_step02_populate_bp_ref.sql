-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 02 - Populate temporary bp_ref tables.
-- Replaces Oracle Step 02 (2a-2d) sections from original SQL*Plus script.

-- Parameter:
--   @stichtag_yyyymmdd: The snapshot date in 'YYYYMMDD' format.

-- step02a: contract partners (Vertragspartner 2)
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp`
(
  BP_ID,
  REACHABILITY_ID,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID
)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
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
  AND bpr.bp_ref_ty = 4
  AND bpr.address_ref_ty = 6
;


-- step02b: invoice recipients (Rechnungsempfänger)
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re`
(
  BP_ID,
  REACHABILITY_ID,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID
)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
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
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 5
UNION ALL
SELECT
  id.rdndnt_cp2_bp_id AS bp_id,
  id.rdndnt_cp2_reachability_id AS reachability_id,
  NULL AS cntrct_cp2_id,
  id.inv_definition_id AS inv_def_invrec_id,
  NULL AS bpr_inst_evnrec_id,
  NULL AS bpr_inst_srvusr_id
FROM
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_inv_definition` AS id
WHERE
  (id.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
    AND (id.modified_at IS NULL
      OR id.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
  AND (id.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
    AND (id.valid_to IS NULL
      OR id.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
  AND id.is_production = 1
  AND id.rdndant_invrec = 0
;


-- step02c: separate EVN recipients
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev`
(
  BP_ID,
  REACHABILITY_ID,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID
)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
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
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 7
;


-- step02d: service users (Dienstenutzer)
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn`
(
  BP_ID,
  REACHABILITY_ID,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID
)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
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
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 8
;