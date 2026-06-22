-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 03 - Part 1 - Populate temporary country and reachability tables.
-- Replaces Oracle Step 03 (3a, 3b, 3c, 3e) sections from original SQL*Plus script.

-- Parameter:
--   @stichtag_yyyymmdd: The snapshot date in 'YYYYMMDD' format.

-- step03a: country
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country`
(
  COUNTRY_CODE,
  DESCRIPTION_ID,
  PARENT_COUNTRY_CODE,
  EU_INDICATOR,
  SAP_CODE,
  CORR_CODE,
  VALID
)
SELECT
  country.COUNTRY_CODE,
  country.DESCRIPTION_ID,
  country.PARENT_COUNTRY_CODE,
  country.EU_INDICATOR,
  country.SAP_CODE,
  country.CORR_CODE,
  country.VALID
FROM
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_glv_country` AS country
;


-- step03b: country description
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc`
(
  DESCRIPTION_ID,
  LANGUAGE,
  SHORT_DESCRIPTION,
  DESCRIPTION,
  LONG_DESCRIPTION
)
SELECT
  des.DESCRIPTION_ID,
  des.LANGUAGE,
  des.SHORT_DESCRIPTION,
  des.DESCRIPTION,
  des.LONG_DESCRIPTION
FROM
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_glv_description` AS des
;


-- step03c: country identification (Länderkennungen)
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng`
(
  COUNTRY_CODE,
  DESCRIPTION_ID,
  LANGUAGE,
  SHORT_DESCRIPTION,
  DESCRIPTION,
  LONG_DESCRIPTION
)
SELECT
  co.country_code,
  de.DESCRIPTION_ID,
  de.LANGUAGE,
  de.SHORT_DESCRIPTION,
  de.DESCRIPTION,
  de.LONG_DESCRIPTION
FROM
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country` AS co
JOIN
  `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc` AS de
ON
  co.description_id = de.description_id
WHERE
  co.valid = 1
;


-- step03e: reachability
INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability`
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
  REACHABLE_THRU
)
SELECT
  re.bp_id,
  re.reachability_id,
  re.obj_version,
  re.country_code,
  re.for_the_attention_of,
  re.address_attachment,
  re.address_attachment_org,
  re.corp_unit,
  re.surname_s,
  re.first_name_g,
  re.zip_code,
  re.city,
  re.pobox,
  re.street,
  re.house_nr,
  re.public_area_a,
  re.private_area_p,
  re.corp_unit_ou1,
  re.address_line_1,
  re.address_line_2,
  re.reachable_from,
  re.reachable_thru
FROM
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_bpd_reachability` AS re
WHERE
  (re.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
    AND (re.modified_at IS NULL
      OR re.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
  AND (re.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
    AND (re.valid_to IS NULL
      OR re.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
  AND re.is_production = 1
;