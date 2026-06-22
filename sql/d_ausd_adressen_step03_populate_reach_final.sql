-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 03 - Part 2 - Populate final reachability tables.
-- Replaces Oracle Step 03 (3f, 3g, 3h, 3i) sections from original SQL*Plus script.

-- Parameter:
--   @stichtag_yyyymmdd: The snapshot date in 'YYYYMMDD' format.

-- step03f: separate reachability tables for contract partners (Geschaeftspartner)
INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_gp`
(
  BP_ID,
  REACHABILITY_ID,
  OBJ_VERSION,
  COUNTRY_CODE,
  FOR_THE_ATTENTION_OF,
  ADDRESS_ATTACHMENT,
  ADDRESS_ATTACHMENT_ORG,
  CORP_UNIT,
  SURNAME_S,
  FIRST_NAME_G,
  ZIP_CODE,
  CITY,
  POBOX,
  STREET,
  HOUSE_NR,
  PUBLIC_AREA_A,
  PRIVATE_AREA_P,
  CORP_UNIT_OU1,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  REACHABLE_FROM,
  REACHABLE_THRU,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID,
  LAND_SD
)
SELECT
  re.BP_ID,
  re.REACHABILITY_ID,
  re.OBJ_VERSION,
  re.COUNTRY_CODE,
  re.FOR_THE_ATTENTION_OF,
  re.ADDRESS_ATTACHMENT,
  re.ADDRESS_ATTACHMENT_ORG,
  re.CORP_UNIT,
  re.SURNAME_S,
  re.FIRST_NAME_G,
  re.ZIP_CODE,
  re.CITY,
  re.POBOX,
  re.STREET,
  re.HOUSE_NR,
  re.PUBLIC_AREA_A,
  re.PRIVATE_AREA_P,
  re.CORP_UNIT_OU1,
  re.ADDRESS_LINE_1,
  re.ADDRESS_LINE_2,
  re.REACHABLE_FROM,
  re.REACHABLE_THRU,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp` AS br
JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
ON
  br.bp_id = re.bp_id
  AND br.reachability_id = re.reachability_id
LEFT JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
ON
  re.country_code = lk.country_code
;


-- step03g: separate reachability tables for invoice recipients (Rechnungsempfänger)
INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_re`
(
  BP_ID,
  REACHABILITY_ID,
  OBJ_VERSION,
  COUNTRY_CODE,
  FOR_THE_ATTENTION_OF,
  ADDRESS_ATTACHMENT,
  ADDRESS_ATTACHMENT_ORG,
  CORP_UNIT,
  SURNAME_S,
  FIRST_NAME_G,
  ZIP_CODE,
  CITY,
  POBOX,
  STREET,
  HOUSE_NR,
  PUBLIC_AREA_A,
  PRIVATE_AREA_P,
  CORP_UNIT_OU1,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  REACHABLE_FROM,
  REACHABLE_THRU,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID,
  LAND_SD
)
SELECT
  re.BP_ID,
  re.REACHABILITY_ID,
  re.OBJ_VERSION,
  re.COUNTRY_CODE,
  re.FOR_THE_ATTENTION_OF,
  re.ADDRESS_ATTACHMENT,
  re.ADDRESS_ATTACHMENT_ORG,
  re.CORP_UNIT,
  re.SURNAME_S,
  re.FIRST_NAME_G,
  re.ZIP_CODE,
  re.CITY,
  re.POBOX,
  re.STREET,
  re.HOUSE_NR,
  re.PUBLIC_AREA_A,
  re.PRIVATE_AREA_P,
  re.CORP_UNIT_OU1,
  re.ADDRESS_LINE_1,
  re.ADDRESS_LINE_2,
  re.REACHABLE_FROM,
  re.REACHABLE_THRU,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re` AS br
JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
ON
  br.bp_id = re.bp_id
  AND br.reachability_id = re.reachability_id
LEFT JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
ON
  re.country_code = lk.country_code
;


-- step03h: separate reachability tables for EVN recipients
INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_ev`
(
  BP_ID,
  REACHABILITY_ID,
  OBJ_VERSION,
  COUNTRY_CODE,
  FOR_THE_ATTENTION_OF,
  ADDRESS_ATTACHMENT,
  ADDRESS_ATTACHMENT_ORG,
  CORP_UNIT,
  SURNAME_S,
  FIRST_NAME_G,
  ZIP_CODE,
  CITY,
  POBOX,
  STREET,
  HOUSE_NR,
  PUBLIC_AREA_A,
  PRIVATE_AREA_P,
  CORP_UNIT_OU1,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  REACHABLE_FROM,
  REACHABLE_THRU,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID,
  LAND_SD
)
SELECT
  re.BP_ID,
  re.REACHABILITY_ID,
  re.OBJ_VERSION,
  re.COUNTRY_CODE,
  re.FOR_THE_ATTENTION_OF,
  re.ADDRESS_ATTACHMENT,
  re.ADDRESS_ATTACHMENT_ORG,
  re.CORP_UNIT,
  re.SURNAME_S,
  re.FIRST_NAME_G,
  re.ZIP_CODE,
  re.CITY,
  re.POBOX,
  re.STREET,
  re.HOUSE_NR,
  re.PUBLIC_AREA_A,
  re.PRIVATE_AREA_P,
  re.CORP_UNIT_OU1,
  re.ADDRESS_LINE_1,
  re.ADDRESS_LINE_2,
  re.REACHABLE_FROM,
  re.REACHABLE_THRU,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev` AS br
JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
ON
  br.bp_id = re.bp_id
  AND br.reachability_id = re.reachability_id
LEFT JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
ON
  re.country_code = lk.country_code
;


-- step03i: separate reachability tables for service users (Dienstenutzer)
INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_dn`
(
  BP_ID,
  REACHABILITY_ID,
  OBJ_VERSION,
  COUNTRY_CODE,
  FOR_THE_ATTENTION_OF,
  ADDRESS_ATTACHMENT,
  ADDRESS_ATTACHMENT_ORG,
  CORP_UNIT,
  SURNAME_S,
  FIRST_NAME_G,
  ZIP_CODE,
  CITY,
  POBOX,
  STREET,
  HOUSE_NR,
  PUBLIC_AREA_A,
  PRIVATE_AREA_P,
  CORP_UNIT_OU1,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  REACHABLE_FROM,
  REACHABLE_THRU,
  CNTRCT_CP2_ID,
  INV_DEF_INVREC_ID,
  BPR_INST_EVNREC_ID,
  BPR_INST_SRVUSR_ID,
  LAND_SD
)
SELECT
  re.BP_ID,
  re.REACHABILITY_ID,
  re.OBJ_VERSION,
  re.COUNTRY_CODE,
  re.FOR_THE_ATTENTION_OF,
  re.ADDRESS_ATTACHMENT,
  re.ADDRESS_ATTACHMENT_ORG,
  re.CORP_UNIT,
  re.SURNAME_S,
  re.FIRST_NAME_G,
  re.ZIP_CODE,
  re.CITY,
  re.POBOX,
  re.STREET,
  re.HOUSE_NR,
  re.PUBLIC_AREA_A,
  re.PRIVATE_AREA_P,
  re.CORP_UNIT_OU1,
  re.ADDRESS_LINE_1,
  re.ADDRESS_LINE_2,
  re.REACHABLE_FROM,
  re.REACHABLE_THRU,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn` AS br
JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
ON
  br.bp_id = re.bp_id
  AND br.reachability_id = re.reachability_id
LEFT JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
ON
  re.country_code = lk.country_code
;