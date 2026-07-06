-- BigQuery Script Parameters
DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM `gcp_project_id.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

DECLARE d_datum DATE DEFAULT PARSE_DATE('%Y%m%d', v_datum);

-- Step 01: Truncate target/temp tables
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_gp`;
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_re`;
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_ev`;
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_dn`;
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_gp_nodp`;
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_re_nodp`;
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_ev_nodp`;
TRUNCATE TABLE `gcp_project_id.sof.ta_bp_ref_dn_nodp`;
TRUNCATE TABLE `gcp_project_id.sof.ta_reachability`;
TRUNCATE TABLE `gcp_project_id.sof.ta_business_pt`;
TRUNCATE TABLE `gcp_project_id.sof.ta_country`;
TRUNCATE TABLE `gcp_project_id.sof.ta_country_desc`;
TRUNCATE TABLE `gcp_project_id.sof.ta_laender_kng`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_reach_gp`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_reach_re`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_reach_dn`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_reach_ev`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_business_gp`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_business_re`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_business_dn`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_business_ev`;
TRUNCATE TABLE `gcp_project_id.sof.ta_e_regulierer`;

-- Step 02a
INSERT INTO `gcp_project_id.sof.ta_bp_ref_gp`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `gcp_project_id.cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 4
  AND bpr.address_ref_ty = 6;

-- Step 02b
INSERT INTO `gcp_project_id.sof.ta_bp_ref_re`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `gcp_project_id.cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
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
FROM `gcp_project_id.cds.ta_inv_definition` id
WHERE id.insert_at <= d_datum
  AND (id.modified_at IS NULL OR id.modified_at > d_datum)
  AND id.valid_from <= d_datum
  AND (id.valid_to IS NULL OR id.valid_to > d_datum)
  AND id.is_production = 1
  AND id.rdndant_invrec = 0;

-- Step 02c
INSERT INTO `gcp_project_id.sof.ta_bp_ref_ev`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `gcp_project_id.cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 7;

-- Step 02d
INSERT INTO `gcp_project_id.sof.ta_bp_ref_dn`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `gcp_project_id.cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 8;

-- Step 03a
INSERT INTO `gcp_project_id.sof.ta_country`
  (COUNTRY_CODE, DESCRIPTION_ID, PARENT_COUNTRY_CODE, EU_INDICATOR, SAP_CODE, CORR_CODE, VALID)
SELECT
  country.country_code,
  country.description_id,
  country.parent_country_code,
  country.eu_indicator,
  country.sap_code,
  country.corr_code,
  country.valid
FROM `gcp_project_id.glv.ta_country` country;

-- Step 03b
INSERT INTO `gcp_project_id.sof.ta_country_desc`
  (DESCRIPTION_ID, LANGUAGE, SHORT_DESCRIPTION, DESCRIPTION, LONG_DESCRIPTION)
SELECT
  des.description_id,
  des.language,
  des.short_description,
  des.description,
  des.long_description
FROM `gcp_project_id.glv.ta_description` des;

-- Step 03c
INSERT INTO `gcp_project_id.sof.ta_laender_kng`
  (COUNTRY_CODE, DESCRIPTION_ID, LANGUAGE, SHORT_DESCRIPTION, DESCRIPTION, LONG_DESCRIPTION)
SELECT
  co.country_code,
  de.description_id,
  de.language,
  de.short_description,
  de.description,
  de.long_description
FROM `gcp_project_id.sof.ta_country` co
JOIN `gcp_project_id.sof.ta_country_desc` de
  ON co.description_id = de.description_id
WHERE co.valid = 1;

-- Step 03e
INSERT INTO `gcp_project_id.sof.ta_reachability`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU)
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
FROM `gcp_project_id.bpd.ta_reachability` re
WHERE re.insert_at <= d_datum
  AND (re.modified_at IS NULL OR re.modified_at > d_datum)
  AND re.valid_from <= d_datum
  AND (re.valid_to IS NULL OR re.valid_to > d_datum)
  AND re.is_production = 1;

-- Step 03f
INSERT INTO `gcp_project_id.sof.ta_e_reach_gp`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
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
FROM `gcp_project_id.sof.ta_bp_ref_gp` br
JOIN `gcp_project_id.sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `gcp_project_id.sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 03g
INSERT INTO `gcp_project_id.sof.ta_e_reach_re`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
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
FROM `gcp_project_id.sof.ta_bp_ref_re` br
JOIN `gcp_project_id.sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `gcp_project_id.sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 03h
INSERT INTO `gcp_project_id.sof.ta_e_reach_ev`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
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
FROM `gcp_project_id.sof.ta_bp_ref_ev` br
JOIN `gcp_project_id.sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `gcp_project_id.sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 03i
INSERT INTO `gcp_project_id.sof.ta_e_reach_dn`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
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
FROM `gcp_project_id.sof.ta_bp_ref_dn` br
JOIN `gcp_project_id.sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `gcp_project_id.sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 04a
INSERT INTO `gcp_project_id.sof.ta_business_pt`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `gcp_project_id.bpd.ta_business_partner` bp
WHERE bp.insert_at <= d_datum
  AND (bp.modified_at IS NULL OR bp.modified_at > d_datum);

-- Step 04b
INSERT INTO `gcp_project_id.sof.ta_bp_ref_gp_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `gcp_project_id.sof.ta_bp_ref_gp`;

INSERT INTO `gcp_project_id.sof.ta_e_business_gp`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `gcp_project_id.sof.ta_bp_ref_gp_nodp` br
JOIN `gcp_project_id.sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 04d
INSERT INTO `gcp_project_id.sof.ta_bp_ref_re_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `gcp_project_id.sof.ta_bp_ref_re`;

INSERT INTO `gcp_project_id.sof.ta_e_business_re`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `gcp_project_id.sof.ta_bp_ref_re_nodp` br
JOIN `gcp_project_id.sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 04f
INSERT INTO `gcp_project_id.sof.ta_bp_ref_ev_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `gcp_project_id.sof.ta_bp_ref_ev`;

INSERT INTO `gcp_project_id.sof.ta_e_business_ev`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `gcp_project_id.sof.ta_bp_ref_ev_nodp` br
JOIN `gcp_project_id.sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 04h
INSERT INTO `gcp_project_id.sof.ta_bp_ref_dn_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `gcp_project_id.sof.ta_bp_ref_dn`;

INSERT INTO `gcp_project_id.sof.ta_e_business_dn`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `gcp_project_id.sof.ta_bp_ref_dn_nodp` br
JOIN `gcp_project_id.sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 05
INSERT INTO `gcp_project_id.sof.ta_e_regulierer`
  (INV_DEF_MOPREF_ID, MOP_BP_ID, MEANS_OF_PAYMENT_ID)
SELECT
  bpr.inv_def_mopref_id,
  bpr.mop_bp_id,
  bpr.means_of_payment_id
FROM `gcp_project_id.cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 2
  AND bpr.mop_ref_ty = 1;