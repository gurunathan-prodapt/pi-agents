-- d_ausd_adressen.bq.sql
--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh
--
-- This BigQuery Stored Procedure encapsulates the core data processing logic
-- from the original Oracle SQL*Plus script.
--
-- Note: Replace `project.` with your actual Google Cloud Project ID.

CREATE OR REPLACE PROCEDURE `project.sof.d_ausd_adressen_proc`(p_stichtag STRING)
BEGIN
  -- Declare a DATE variable for the stichtag to be used in date comparisons.
  DECLARE v_stichtag_date DATE;

  -- Convert the input stichtag string (DDMMYYYY) to a DATE type.
  SET v_stichtag_date = PARSE_DATE('%Y%m%d', p_stichtag);

  -- Step01: Truncate all intermediate and final target tables for idempotency.
  -- This replaces the Oracle TRUNCATE TABLE ... REUSE STORAGE calls.
  TRUNCATE TABLE `project.sof.ta_bp_ref_gp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_re`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_ev`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_dn`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_gp_nodp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_re_nodp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_ev_nodp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_dn_nodp`;
  TRUNCATE TABLE `project.sof.ta_reachability`;
  TRUNCATE TABLE `project.sof.ta_business_pt`;
  TRUNCATE TABLE `project.sof.ta_country`;
  TRUNCATE TABLE `project.sof.ta_country_desc`;
  TRUNCATE TABLE `project.sof.ta_laender_kng`;
  TRUNCATE TABLE `project.sof.ta_e_reach_gp`;
  TRUNCATE TABLE `project.sof.ta_e_reach_re`;
  TRUNCATE TABLE `project.sof.ta_e_reach_dn`;
  TRUNCATE TABLE `project.sof.ta_e_reach_ev`;
  TRUNCATE TABLE `project.sof.ta_e_business_gp`;
  TRUNCATE TABLE `project.sof.ta_e_business_re`;
  TRUNCATE TABLE `project.sof.ta_e_business_dn`;
  TRUNCATE TABLE `project.sof.ta_e_business_ev`;
  TRUNCATE TABLE `project.sof.ta_e_regulierer`;

  -- Step02a: Populate sof.ta_bp_ref_gp for contract partners (BP_REF_TY = 4, ADDRESS_REF_TY = 6).
  INSERT INTO `project.sof.ta_bp_ref_gp`
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
  FROM `project.cds.ta_bp_ref` AS bpr
  WHERE DATE(bpr.insert_at) <= v_stichtag_date
    AND (bpr.modified_at IS NULL OR DATE(bpr.modified_at) > v_stichtag_date)
    AND DATE(bpr.valid_from) <= v_stichtag_date
    AND (bpr.valid_to IS NULL OR DATE(bpr.valid_to) > v_stichtag_date)
    AND bpr.is_production = 1
    AND bpr.bp_ref_ty = 4
    AND bpr.address_ref_ty = 6;

  -- Step02b: Populate sof.ta_bp_ref_re for invoice recipients (BP_REF_TY = 1, ADDRESS_REF_TY = 5)
  -- and redundant invoice recipients from inv_definition.
  INSERT INTO `project.sof.ta_bp_ref_re`
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
  FROM `project.cds.ta_bp_ref` AS bpr
  WHERE DATE(bpr.insert_at) <= v_stichtag_date
    AND (bpr.modified_at IS NULL OR DATE(bpr.modified_at) > v_stichtag_date)
    AND DATE(bpr.valid_from) <= v_stichtag_date
    AND (bpr.valid_to IS NULL OR DATE(bpr.valid_to) > v_stichtag_date)
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
  FROM `project.cds.ta_inv_definition` AS id
  WHERE DATE(id.insert_at) <= v_stichtag_date
    AND (id.modified_at IS NULL OR DATE(id.modified_at) > v_stichtag_date)
    AND DATE(id.valid_from) <= v_stichtag_date
    AND (id.valid_to IS NULL OR DATE(id.valid_to) > v_stichtag_date)
    AND id.is_production = 1
    AND id.rdndant_invrec = 0;

  -- Step02c: Populate sof.ta_bp_ref_ev for separate EVN recipients (BP_REF_TY = 1, ADDRESS_REF_TY = 7).
  INSERT INTO `project.sof.ta_bp_ref_ev`
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
  FROM `project.cds.ta_bp_ref` AS bpr
  WHERE DATE(bpr.insert_at) <= v_stichtag_date
    AND (bpr.modified_at IS NULL OR DATE(bpr.modified_at) > v_stichtag_date)
    AND DATE(bpr.valid_from) <= v_stichtag_date
    AND (bpr.valid_to IS NULL OR DATE(bpr.valid_to) > v_stichtag_date)
    AND bpr.is_production = 1
    AND bpr.bp_ref_ty = 1
    AND bpr.address_ref_ty = 7;

  -- Step02d: Populate sof.ta_bp_ref_dn for service users (BP_REF_TY = 1, ADDRESS_REF_TY = 8).
  INSERT INTO `project.sof.ta_bp_ref_dn`
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
  FROM `project.cds.ta_bp_ref` AS bpr
  WHERE DATE(bpr.insert_at) <= v_stichtag_date
    AND (bpr.modified_at IS NULL OR DATE(bpr.modified_at) > v_stichtag_date)
    AND DATE(bpr.valid_from) <= v_stichtag_date
    AND (bpr.valid_to IS NULL OR DATE(bpr.valid_to) > v_stichtag_date)
    AND bpr.is_production = 1
    AND bpr.bp_ref_ty = 1
    AND bpr.address_ref_ty = 8;

  -- Step03a: Populate sof.ta_country from glv.ta_country.
  INSERT INTO `project.sof.ta_country`
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
    country.country_code,
    country.description_id,
    country.parent_country_code,
    country.eu_indicator,
    country.sap_code,
    country.corr_code,
    country.valid
  FROM `project.glv.ta_country` AS country;

  -- Step03b: Populate sof.ta_country_desc from glv.ta_description.
  INSERT INTO `project.sof.ta_country_desc`
  (
    DESCRIPTION_ID,
    LANGUAGE,
    SHORT_DESCRIPTION,
    DESCRIPTION,
    LONG_DESCRIPTION
  )
  SELECT
    des.description_id,
    des.language,
    des.short_description,
    des.description,
    des.long_description
  FROM `project.glv.ta_description` AS des;

  -- Step03c: Populate sof.ta_laender_kng by joining country and country_desc.
  INSERT INTO `project.sof.ta_laender_kng`
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
    de.description_id,
    de.language,
    de.short_description,
    de.description,
    de.long_description
  FROM `project.sof.ta_country` AS co
  JOIN `project.sof.ta_country_desc` AS de
    ON co.description_id = de.description_id
  WHERE co.valid = 1;

  -- Step03e: Populate sof.ta_reachability from bpd.ta_reachability.
  INSERT INTO `project.sof.ta_reachability`
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
  FROM `project.bpd.ta_reachability` AS re
  WHERE DATE(re.insert_at) <= v_stichtag_date
    AND (re.modified_at IS NULL OR DATE(re.modified_at) > v_stichtag_date)
    AND DATE(re.valid_from) <= v_stichtag_date
    AND (re.valid_to IS NULL OR DATE(re.valid_to) > v_stichtag_date)
    AND re.is_production = 1;

  -- Step03f: Populate sof.ta_e_reach_gp by joining bp_ref_gp, reachability and laender_kng.
  INSERT INTO `project.sof.ta_e_reach_gp`
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
    re.reachable_thru,
    br.cntrct_cp2_id,
    br.inv_def_invrec_id,
    br.bpr_inst_evnrec_id,
    br.bpr_inst_srvusr_id,
    SUBSTR(lk.short_description, 1, 3) AS land_sd
  FROM `project.sof.ta_bp_ref_gp` AS br
  JOIN `project.sof.ta_reachability` AS re
    ON br.bp_id = re.bp_id
   AND br.reachability_id = re.reachability_id
  LEFT JOIN `project.sof.ta_laender_kng` AS lk
    ON re.country_code = lk.country_code;

  -- Step03g: Populate sof.ta_e_reach_re by joining bp_ref_re, reachability and laender_kng.
  INSERT INTO `project.sof.ta_e_reach_re`
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
    re.reachable_thru,
    br.cntrct_cp2_id,
    br.inv_def_invrec_id,
    br.bpr_inst_evnrec_id,
    br.bpr_inst_srvusr_id,
    SUBSTR(lk.short_description, 1, 3) AS land_sd
  FROM `project.sof.ta_bp_ref_re` AS br
  JOIN `project.sof.ta_reachability` AS re
    ON br.bp_id = re.bp_id
   AND br.reachability_id = re.reachability_id
  LEFT JOIN `project.sof.ta_laender_kng` AS lk
    ON re.country_code = lk.country_code;

  -- Step03h: Populate sof.ta_e_reach_ev by joining bp_ref_ev, reachability and laender_kng.
  INSERT INTO `project.sof.ta_e_reach_ev`
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
    re.reachable_thru,
    br.cntrct_cp2_id,
    br.inv_def_invrec_id,
    br.bpr_inst_evnrec_id,
    br.bpr_inst_srvusr_id,
    SUBSTR(lk.short_description, 1, 3) AS land_sd
  FROM `project.sof.ta_bp_ref_ev` AS br
  JOIN `project.sof.ta_reachability` AS re
    ON br.bp_id = re.bp_id
   AND br.reachability_id = re.reachability_id
  LEFT JOIN `project.sof.ta_laender_kng` AS lk
    ON re.country_code = lk.country_code;

  -- Step03i: Populate sof.ta_e_reach_dn by joining bp_ref_dn, reachability and laender_kng.
  INSERT INTO `project.sof.ta_e_reach_dn`
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
    re.reachable_thru,
    br.cntrct_cp2_id,
    br.inv_def_invrec_id,
    br.bpr_inst_evnrec_id,
    br.bpr_inst_srvusr_id,
    SUBSTR(lk.short_description, 1, 3) AS land_sd
  FROM `project.sof.ta_bp_ref_dn` AS br
  JOIN `project.sof.ta_reachability` AS re
    ON br.bp_id = re.bp_id
   AND br.reachability_id = re.reachability_id
  LEFT JOIN `project.sof.ta_laender_kng` AS lk
    ON re.country_code = lk.country_code;

  -- Step03j: Truncate temporary tables no longer needed.
  TRUNCATE TABLE `project.sof.ta_reachability`;
  TRUNCATE TABLE `project.sof.ta_country`;
  TRUNCATE TABLE `project.sof.ta_country_desc`;
  TRUNCATE TABLE `project.sof.ta_laender_kng`;

  -- Step04a: Populate sof.ta_business_pt from bpd.ta_business_partner.
  INSERT INTO `project.sof.ta_business_pt`
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
  FROM `project.bpd.ta_business_partner` AS bp
  WHERE DATE(bp.insert_at) <= v_stichtag_date
    AND (bp.modified_at IS NULL OR DATE(bp.modified_at) > v_stichtag_date);

  -- Step04b: Populate sof.ta_e_business_gp for contract partners.
  INSERT INTO `project.sof.ta_bp_ref_gp_nodp`
  (BP_ID)
  SELECT DISTINCT bp_id
  FROM `project.sof.ta_bp_ref_gp`;

  INSERT INTO `project.sof.ta_e_business_gp`
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
  FROM `project.sof.ta_bp_ref_gp_nodp` AS br
  JOIN `project.sof.ta_business_pt` AS bp
    ON br.bp_id = bp.bp_id;

  -- Step04c: Truncate temporary tables no longer needed.
  TRUNCATE TABLE `project.sof.ta_bp_ref_gp_nodp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_gp`;

  -- Step04d: Populate sof.ta_e_business_re for invoice recipients.
  INSERT INTO `project.sof.ta_bp_ref_re_nodp`
  (BP_ID)
  SELECT DISTINCT bp_id
  FROM `project.sof.ta_bp_ref_re`;

  INSERT INTO `project.sof.ta_e_business_re`
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
  FROM `project.sof.ta_bp_ref_re_nodp` AS br
  JOIN `project.sof.ta_business_pt` AS bp
    ON br.bp_id = bp.bp_id;

  -- Step04e: Truncate temporary tables no longer needed.
  TRUNCATE TABLE `project.sof.ta_bp_ref_re_nodp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_re`;

  -- Step04f: Populate sof.ta_e_business_ev for EVN recipients.
  INSERT INTO `project.sof.ta_bp_ref_ev_nodp`
  (BP_ID)
  SELECT DISTINCT bp_id
  FROM `project.sof.ta_bp_ref_ev`;

  INSERT INTO `project.sof.ta_e_business_ev`
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
  FROM `project.sof.ta_bp_ref_ev_nodp` AS br
  JOIN `project.sof.ta_business_pt` AS bp
    ON br.bp_id = bp.bp_id;

  -- Step04g: Truncate temporary tables no longer needed.
  TRUNCATE TABLE `project.sof.ta_bp_ref_ev_nodp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_ev`;

  -- Step04h: Populate sof.ta_e_business_dn for service users.
  INSERT INTO `project.sof.ta_bp_ref_dn_nodp`
  (BP_ID)
  SELECT DISTINCT bp_id
  FROM `project.sof.ta_bp_ref_dn`;

  INSERT INTO `project.sof.ta_e_business_dn`
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
  FROM `project.sof.ta_bp_ref_dn_nodp` AS br
  JOIN `project.sof.ta_business_pt` AS bp
    ON br.bp_id = bp.bp_id;

  -- Step04i: Truncate temporary tables no longer needed.
  TRUNCATE TABLE `project.sof.ta_business_pt`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_dn_nodp`;
  TRUNCATE TABLE `project.sof.ta_bp_ref_dn`;

  -- Step05: Populate sof.ta_e_regulierer for regulators.
  INSERT INTO `project.sof.ta_e_regulierer`
  (
    INV_DEF_MOPREF_ID,
    MOP_BP_ID,
    MEANS_OF_PAYMENT_ID
  )
  SELECT
    bpr.inv_def_mopref_id,
    bpr.mop_bp_id,
    bpr.means_of_payment_id
  FROM `project.cds.ta_bp_ref` AS bpr
  WHERE DATE(bpr.insert_at) <= v_stichtag_date
    AND (bpr.modified_at IS NULL OR DATE(bpr.modified_at) > v_stichtag_date)
    AND DATE(bpr.valid_from) <= v_stichtag_date
    AND (bpr.valid_to IS NULL OR DATE(bpr.valid_to) > v_stichtag_date)
    AND bpr.is_production = 1
    AND bpr.bp_ref_ty = 2
    AND bpr.mop_ref_ty = 1;
END;