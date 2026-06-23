-- BigQuery SQL equivalent of d_ausd_rechempf.sql
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

DECLARE v_carmen STRING DEFAULT '@pcrs1'; -- Placeholder for external source reference
DECLARE v_datum STRING DEFAULT (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 02: truncate temp tables
TRUNCATE TABLE `fos_snapshots.sof_ta_means_of_pay`;
TRUNCATE TABLE `fos_snapshots.sof_ta_bank`;
TRUNCATE TABLE `fos_snapshots.sof_ta_bank_verb`;
TRUNCATE TABLE `fos_snapshots.sof_ta_bank_zuord`;
TRUNCATE TABLE `fos_snapshots.sof_ta_p_rech_empf`;
TRUNCATE TABLE `fos_snapshots.sof_ta_p_d1_vpn`;

-- Step 03: create temp. rechnungsdefinitionen (means_of_pay)
INSERT INTO `fos_snapshots.sof_ta_means_of_pay`
(
  BP_ID, MEANS_OF_PAYMENT_ID, OBJ_VERSION, INSERT_AT, MOP_TY, ACCOUNT_INT_BP_ID, ACCOUNT_INT_MOP_ID,
  BANK_ID_ACC, ACCOUNT_NUMBER_ACC, BANK_INTERNATIONAL_ID, MANDATE_VAR_CV, MANDATE_ST, MOP_ST, CHECK_ST,
  STATUS_REASON, IBAN, MANDATE_REFERENCE_NO, MANDATE_MIGRATED, MANDATE_CITY, MANDATE_DATE, VALID_FROM,
  VALID_TO, INSERT_BY, MODIFIED_AT, MODIFIED_BY, MODIFY_REASON, IS_IN_ARCHIVE, ROW_VERSION,
  REDUNDANT_RB_DOMAIN_PATH, REDUNDANT_RB_PROC_PATH, IS_PRODUCTION, RB_PARTITION_ID$
)
SELECT
  mop.BP_ID, mop.MEANS_OF_PAYMENT_ID, mop.OBJ_VERSION, mop.INSERT_AT, mop.MOP_TY, mop.ACCOUNT_INT_BP_ID, mop.ACCOUNT_INT_MOP_ID,
  mop.BANK_ID_ACC, mop.ACCOUNT_NUMBER_ACC, mop.BANK_INTERNATIONAL_ID, mop.MANDATE_VAR_CV, mop.MANDATE_ST, mop.MOP_ST, mop.CHECK_ST,
  mop.STATUS_REASON, mop.IBAN, mop.MANDATE_REFERENCE_NO, mop.MANDATE_MIGRATED, mop.MANDATE_CITY, mop.MANDATE_DATE, mop.VALID_FROM,
  mop.VALID_TO, mop.INSERT_BY, mop.MODIFIED_AT, mop.MODIFIED_BY, mop.MODIFY_REASON, mop.IS_IN_ARCHIVE, mop.ROW_VERSION,
  mop.REDUNDANT_RB_DOMAIN_PATH, mop.REDUNDANT_RB_PROC_PATH, mop.IS_PRODUCTION, mop.RB_PARTITION_ID$
FROM `carmen_bpd.ta_means_of_payment` AS mop -- Renamed from bpd$ta_means_of_payment
WHERE
  (DATE(mop.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (mop.modified_at IS NULL OR DATE(mop.modified_at) > PARSE_DATE('%Y%m%d', v_datum)))
  AND
  (DATE(mop.valid_from) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (mop.valid_to IS NULL OR DATE(mop.valid_to) > PARSE_DATE('%Y%m%d', v_datum)))
  AND mop.is_production = TRUE;

-- Step 03 continued: bank data
INSERT INTO `fos_snapshots.sof_ta_bank`
(
  BANK_ID, INSERT_AT, COUNTRY_CODE, BANK_SORT_NAME, BANK_NAME, INSERT_BY, MODIFIED_AT, MODIFIED_BY,
  MODIFY_REASON, IS_IN_ARCHIVE, ROW_VERSION, BIC, BANK_INTERNATIONAL_ID
)
SELECT
  ba.BANK_ID, ba.INSERT_AT, ba.COUNTRY_CODE, ba.BANK_SORT_NAME, ba.BANK_NAME, ba.INSERT_BY, ba.MODIFIED_AT, ba.MODIFIED_BY,
  ba.MODIFY_REASON, ba.IS_IN_ARCHIVE, ba.ROW_VERSION, CAST(NULL AS STRING) AS BIC, CAST(NULL AS STRING) AS BANK_INTERNATIONAL_ID
FROM `carmen_bpd.ta_bank` AS ba -- Renamed from BPD$TA_BANK
WHERE
  (DATE(ba.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (ba.modified_at IS NULL OR DATE(ba.modified_at) > PARSE_DATE('%Y%m%d', v_datum)))
UNION ALL
SELECT
  -99999 AS BANK_ID, bi.INSERT_AT, bi.COUNTRY_CODE, CAST(NULL AS STRING) AS BANK_SORT_NAME, bi.BANK_NAME,
  bi.INSERT_BY, bi.MODIFIED_AT, bi.MODIFIED_BY, bi.MODIFY_REASON, bi.IS_IN_ARCHIVE, bi.ROW_VERSION,
  bi.BIC, bi.BANK_INTERNATIONAL_ID
FROM `carmen_bpd.ta_bank_international` AS bi -- Renamed from BPD$TA_BANK_INTERNATIONAL
WHERE
  (DATE(bi.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (bi.modified_at IS NULL OR DATE(bi.modified_at) > PARSE_DATE('%Y%m%d', v_datum)));

-- Step 04: bank_verb
INSERT INTO `fos_snapshots.sof_ta_bank_verb`
(
  MEANS_OF_PAYMENT_ID, BP_ID, ACCOUNT_NUMBER_ACC, BANK_NAME, BANK_SORT_NAME, IBAN, BIC
)
SELECT
  mp.MEANS_OF_PAYMENT_ID, mp.BP_ID, mp.ACCOUNT_NUMBER_ACC, ba.BANK_NAME, ba.BANK_SORT_NAME, mp.IBAN, ba.BIC
FROM `fos_snapshots.sof_ta_means_of_pay` AS mp
JOIN `fos_snapshots.sof_ta_bank` AS ba
  ON mp.BANK_ID_ACC = ba.BANK_ID
  OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID;

-- Step 04 continued: bank_zuord
INSERT INTO `fos_snapshots.sof_ta_bank_zuord`
(
  INV_DEF_MOPREF_ID, ACCOUNT_NUMBER_ACC, BANK_NAME, BANK_SORT_NAME, IBAN, BIC
)
SELECT
  za.inv_def_mopref_id, ba.account_number_acc, ba.bank_name, ba.bank_sort_name, ba.iban, ba.bic
FROM `fos_snapshots.sof_ta_bank_verb` AS ba
JOIN `fos_source.sof_ta_e_regulierer` AS za
  ON za.means_of_payment_id = ba.means_of_payment_id
 AND za.mop_bp_id = ba.bp_id;

-- Step 05: rech_empf
INSERT INTO `fos_snapshots.sof_ta_p_rech_empf`
(
  KUNDENKONTO, RECHDEF_ID, DPPS_KONTONUMMER, RECHNUNGSEMPFAENGER, QUELLE, AKAD_TITEL, FIRMA,
  VORNAME, NACHNAME, ZUSATZ_1, ZUSATZ_2, STRASSE, PLZ, WOHNORT, LAND, BANKNAME,
  BANK_KONTONUMMER, BLZ, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN, KUN_NR_RECH_EMPF, IBAN, BIC
)
SELECT
  '0' AS kundenkonto,
  re.inv_def_invrec_id AS rechdef_id,
  '0' AS dpps_kontonummer,
  CASE
    WHEN re.corp_unit IS NULL AND bp.organisation_name IS NULL THEN
      CASE
        WHEN re.surname_s IS NULL THEN CONCAT(bp.first_name, ' ', bp.surname)
        ELSE CONCAT(re.first_name_g, ' ', re.surname_s)
      END
    ELSE
      CASE
        WHEN re.corp_unit IS NULL THEN bp.organisation_name
        ELSE re.corp_unit
      END
  END AS rechnungsempfaenger,
  'C' AS quelle,
  CASE
    WHEN re.surname_s IS NULL THEN bp.title
    ELSE ''
  END AS akad_titel,
  CASE
    WHEN re.corp_unit IS NULL THEN bp.organisation_name
    ELSE re.corp_unit
  END AS firma,
  CASE
    WHEN re.first_name_g IS NULL THEN bp.first_name
    ELSE re.first_name_g
  END AS vorname,
  CASE
    WHEN re.surname_s IS NULL THEN bp.surname
    ELSE bp.surname
  END AS nachname,
  re.for_the_attention_of AS zusatz_1,
  re.address_attachment AS zusatz_2,
  CASE
    WHEN re.street IS NULL THEN
      CASE
        WHEN re.pobox IS NULL THEN ''
        ELSE CONCAT('Postfach ', CAST(re.pobox AS STRING))
      END
    ELSE CONCAT(re.street, ' ', CAST(re.house_nr AS STRING))
  END AS strasse,
  re.zip_code AS plz,
  re.city AS wohnort,
  re.land_sd AS land,
  ba.bank_name AS bankname,
  ba.account_number_acc AS bank_kontonummer,
  ba.bank_sort_name AS blz,
  re.address_attachment_org AS organisationseinheit,
  bp.sales_tax_freed AS mwst_kennzeichen,
  bp.tm_customerid AS kun_nr_rech_empf,
  ba.iban,
  ba.bic
FROM `fos_source.sof_ta_e_reach_re` AS re
JOIN `fos_source.sof_ta_e_business_re` AS bp
  ON re.bp_id = bp.bp_id
LEFT JOIN `fos_snapshots.sof_ta_bank_zuord` AS ba
  ON re.inv_def_invrec_id = ba.inv_def_mopref_id;

-- Step 06: d1_vpn
INSERT INTO `fos_snapshots.sof_ta_p_d1_vpn`
(
  VERTRAGS_ID, VPN_ID
)
SELECT
  bp.vertrags_id, bp.vpn_id
FROM `dwh_view.vi_s_ibasisprodukt` AS bp
WHERE
  bp.vpn_id IS NOT NULL
  AND bp.basisprodukt_id IN (2828, 2831);