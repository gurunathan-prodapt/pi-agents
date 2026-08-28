-- ===================================================================
-- datei:  d_ausd_rechempf.sql
-- datum:  22.11.2001
-- autor:  andre loebbers (al)
-- ===================================================================
--
-- modifikationen
----------------------------------------------------------------------
-- version datum    autor dokumentation
-- 2.0.4   20011122 al    aufsetzend auf rel2.0.3 "dpps" entfernt
-- 2.0.8   20020521 al    kriterium in case angepasst
-- 2.0.9   20020612 al,sj organisationseinheit hinzugefuegt
-- 2.0.12  20020826 sj    erweiterung um postfach-ausgabe
-- 2.0.13  20020913 sj    umstellung auf crs und erweiterung um länderkennung
-- 3.1.0   20030109 sj    Tabellennamenerweiterung um das Tagesdatum
-- 7.0.0   20040503 Roh   Telemetriezusatzvertraege ergaenzt
-- 7.5.0   20040831 Roh   Umstellung auf parallel degree 4
-- 5.4.0   20050901 Roh   spool ins Unterverzeichnis ./tmp
-- 6.4.0   20061121 RR    Bestimmung Substitutions-Variable v_datum aus
--                        Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR    Überflüssige ANALYZE/STATISTICS Kommandos entfernt
-- 10.2.1  20100428 Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
-- 13.2.0  20130319 Markus Simon        Anpassung an Carmen Datenmodell, in Tabelle BPD$TA_MEANS_OF_PAYMENT fallen die Spalten BANK_ID_EC, ACCOUNT_NUMBER_EC, EC_CARD_NR weg
-- 13.3.1  20130409 Kornel Przybylski - Fields IBAN, BIC added to report generation as part of BERT SEPA@TDG CR 60 RV Admin
-- 14.1.0  20131204 Wojciech Szyba - BSP_SARAH_doppelte Anzeige der Rufnummern im Report (INM22722300)
----------------------------------------------------------------------

-- ========================= Step00 ==================================

SELECT 'step00: variablendefinition...' AS log_msg;

DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- ========================= Step01 ==================================

SELECT 'step01: prüfung, ob die benötigten ereignis-tabellen vorhanden sind...' AS log_msg;

-- Note: DESC commands are stripped as they are interactive SQL*Plus commands.
-- In BigQuery, table existence is validated at query compilation time.

-- ========================= Step02 ==================================

SELECT 'step02: löschen der temporären-tabellen...' AS log_msg;

-- löschen der aktuellen tabellen für den fall eines restarts am gleichen tag
TRUNCATE TABLE `sof$ta_means_of_pay`;
TRUNCATE TABLE `sof$ta_bank`;
TRUNCATE TABLE `sof$ta_bank_verb`;
TRUNCATE TABLE `sof$ta_bank_zuord`;
TRUNCATE TABLE `sof$ta_p_rech_empf`;
TRUNCATE TABLE `sof$ta_p_d1_vpn`;

-- ========================= Step03 ==================================

SELECT 'step03: erzeuge temp. rechnungsdefinitionen...' AS log_msg;

INSERT INTO `sof$ta_means_of_pay` (
  BP_ID,
  MEANS_OF_PAYMENT_ID,
  OBJ_VERSION,
  INSERT_AT,
  MOP_TY,
  ACCOUNT_INT_BP_ID,
  ACCOUNT_INT_MOP_ID,
  BANK_ID_ACC,
  ACCOUNT_NUMBER_ACC,
  BANK_INTERNATIONAL_ID,
  MANDATE_VAR_CV,
  MANDATE_ST,
  MOP_ST,
  CHECK_ST,
  STATUS_REASON,
  IBAN,
  MANDATE_REFERENCE_NO,
  MANDATE_MIGRATED,
  MANDATE_CITY,
  MANDATE_DATE,
  VALID_FROM,
  VALID_TO,
  INSERT_BY,
  MODIFIED_AT,
  MODIFIED_BY,
  MODIFY_REASON,
  IS_IN_ARCHIVE,
  ROW_VERSION,
  REDUNDANT_RB_DOMAIN_PATH,
  REDUNDANT_RB_PROC_PATH,
  IS_PRODUCTION,
  RB_PARTITION_ID$
)
SELECT
  mop.BP_ID,
  mop.MEANS_OF_PAYMENT_ID,
  mop.OBJ_VERSION,
  mop.INSERT_AT,
  mop.MOP_TY,
  mop.ACCOUNT_INT_BP_ID,
  mop.ACCOUNT_INT_MOP_ID,
  mop.BANK_ID_ACC,
  mop.ACCOUNT_NUMBER_ACC,
  mop.BANK_INTERNATIONAL_ID,
  mop.MANDATE_VAR_CV,
  mop.MANDATE_ST,
  mop.MOP_ST,
  mop.CHECK_ST,
  mop.STATUS_REASON,
  mop.IBAN,
  mop.MANDATE_REFERENCE_NO,
  mop.MANDATE_MIGRATED,
  mop.MANDATE_CITY,
  mop.MANDATE_DATE,
  mop.VALID_FROM,
  mop.VALID_TO,
  mop.INSERT_BY,
  mop.MODIFIED_AT,
  mop.MODIFIED_BY,
  mop.MODIFY_REASON,
  mop.IS_IN_ARCHIVE,
  mop.ROW_VERSION,
  mop.REDUNDANT_RB_DOMAIN_PATH,
  mop.REDUNDANT_RB_PROC_PATH,
  mop.IS_PRODUCTION,
  mop.RB_PARTITION_ID$
FROM `bpd$ta_means_of_payment` mop
WHERE (mop.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)
       AND (mop.modified_at IS NULL OR mop.modified_at > PARSE_DATETIME('%Y%m%d', v_datum)))
  AND (mop.valid_from <= PARSE_DATETIME('%Y%m%d', v_datum)
       AND (mop.valid_to IS NULL OR mop.valid_to > PARSE_DATETIME('%Y%m%d', v_datum)))
  AND mop.is_production = 1;

INSERT INTO `sof$ta_bank` (
  BANK_ID,
  INSERT_AT,
  COUNTRY_CODE,
  BANK_SORT_NAME,
  BANK_NAME,
  INSERT_BY,
  MODIFIED_AT,
  MODIFIED_BY,
  MODIFY_REASON,
  IS_IN_ARCHIVE,
  ROW_VERSION,
  BIC,
  BANK_INTERNATIONAL_ID
)
SELECT
  ba.BANK_ID,
  ba.INSERT_AT,
  ba.COUNTRY_CODE,
  ba.BANK_SORT_NAME,
  ba.BANK_NAME,
  ba.INSERT_BY,
  ba.MODIFIED_AT,
  ba.MODIFIED_BY,
  ba.MODIFY_REASON,
  ba.IS_IN_ARCHIVE,
  ba.ROW_VERSION,
  CAST(NULL AS STRING) AS BIC,
  CAST(NULL AS INT64) AS BANK_INTERNATIONAL_ID
FROM `BPD$TA_BANK` ba
WHERE ba.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)
  AND (ba.modified_at IS NULL OR ba.modified_at > PARSE_DATETIME('%Y%m%d', v_datum))

UNION ALL

SELECT
  -99999 AS BANK_ID,
  bi.INSERT_AT,
  bi.COUNTRY_CODE,
  CAST(NULL AS STRING) AS BANK_SORT_NAME,
  bi.BANK_NAME,
  bi.INSERT_BY,
  bi.MODIFIED_AT,
  bi.MODIFIED_BY,
  bi.MODIFY_REASON,
  bi.IS_IN_ARCHIVE,
  bi.ROW_VERSION,
  bi.BIC,
  bi.BANK_INTERNATIONAL_ID
FROM `BPD$TA_BANK_INTERNATIONAL` bi
WHERE bi.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)
  AND (bi.modified_at IS NULL OR bi.modified_at > PARSE_DATETIME('%Y%m%d', v_datum));

-- ========================= Step04 ==================================

SELECT 'step04: erzeuge tabelle sof$ta_bank_verb und sof$ta_bank_zuord...' AS log_msg;

INSERT INTO `sof$ta_bank_verb` (
  MEANS_OF_PAYMENT_ID,
  BP_ID,
  ACCOUNT_NUMBER_ACC,
  BANK_NAME,
  BANK_SORT_NAME,
  IBAN,
  BIC
)
SELECT
  mp.MEANS_OF_PAYMENT_ID,
  mp.BP_ID,
  mp.ACCOUNT_NUMBER_ACC,
  ba.BANK_NAME,
  ba.BANK_SORT_NAME,
  mp.IBAN,
  ba.BIC
FROM `sof$ta_means_of_pay` mp
INNER JOIN `sof$ta_bank` ba
   ON mp.BANK_ID_ACC = ba.BANK_ID
   OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID;

INSERT INTO `sof$ta_bank_zuord` (
  INV_DEF_MOPREF_ID,
  ACCOUNT_NUMBER_ACC,
  BANK_NAME,
  BANK_SORT_NAME,
  IBAN,
  BIC
)
SELECT
  za.inv_def_mopref_id,
  ba.account_number_acc,
  ba.bank_name,
  ba.bank_sort_name,
  ba.iban,
  ba.bic
FROM `sof$ta_bank_verb` ba
INNER JOIN `sof$ta_e_regulierer` za
  ON za.means_of_payment_id = ba.means_of_payment_id
 AND za.mop_bp_id           = ba.bp_id;

-- ========================= Step05 ==================================

SELECT 'step05: erzeuge tabelle sof$ta_p_rech_empf...' AS log_msg;

INSERT INTO `sof$ta_p_rech_empf` (
  KUNDENKONTO,
  RECHDEF_ID,
  DPPS_KONTONUMMER,
  RECHNUNGSEMPFAENGER,
  QUELLE,
  AKAD_TITEL,
  FIRMA,
  VORNAME,
  NACHNAME,
  ZUSATZ_1,
  ZUSATZ_2,
  STRASSE,
  PLZ,
  WOHNORT,
  LAND,
  BANKNAME,
  BANK_KONTONUMMER,
  BLZ,
  ORGANISATIONSEINHEIT,
  MWST_KENNZEICHEN,
  KUN_NR_RECH_EMPF,
  IBAN,
  BIC
)
SELECT
  '0' AS kundenkonto,
  re.inv_def_invrec_id AS rechdef_id,
  '0' AS dpps_kontonummer,
  CASE
     WHEN (re.corp_unit IS NULL AND bp.organisation_name IS NULL)
     THEN
         CASE
             WHEN (re.surname_s IS NULL)
             THEN CONCAT(COALESCE(bp.first_name, ''), ' ', COALESCE(bp.surname, ''))
             ELSE CONCAT(COALESCE(re.first_name_g, ''), ' ', COALESCE(re.surname_s, ''))
         END
     ELSE
         CASE
             WHEN (re.corp_unit IS NULL)
             THEN bp.organisation_name
             ELSE re.corp_unit
         END
  END AS rechnungsempfaenger,
  'C' AS quelle,
  CASE
     WHEN (re.surname_s IS NULL)
     THEN bp.title
     ELSE ''
  END AS akad_titel,
  CASE
     WHEN (re.corp_unit IS NULL)
     THEN bp.organisation_name
     ELSE re.corp_unit
  END AS firma,
  CASE
     WHEN (re.first_name_g IS NULL)
     THEN bp.first_name
     ELSE re.first_name_g
  END AS vorname,
  CASE
     WHEN (re.surname_s IS NULL)
     THEN bp.surname
     ELSE re.surname_s
  END AS nachname,
  re.for_the_attention_of AS zusatz_1,
  re.address_attachment AS zusatz_2,
  CASE
     WHEN (re.street IS NULL)
     THEN
        CASE
           WHEN (re.pobox IS NULL)
           THEN ''
           ELSE CONCAT('Postfach ', COALESCE(re.pobox, ''))
        END
     ELSE CONCAT(COALESCE(re.street, ''), ' ', COALESCE(re.house_nr, ''))
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
FROM `sof$ta_e_reach_re` re
INNER JOIN `sof$ta_e_business_re` bp
   ON re.bp_id = bp.bp_id
LEFT OUTER JOIN `sof$ta_bank_zuord` ba
   ON re.inv_def_invrec_id = ba.inv_def_mopref_id;

-- ========================= Step06 ==================================

SELECT 'step06: erzeuge die tabelle sof$ta_p_d1_vpn...' AS log_msg;

INSERT INTO `sof$ta_p_d1_vpn` (
  VERTRAGS_ID,
  VPN_ID
)
SELECT
  bp.vertrags_id,
  bp.vpn_id
FROM `dwh$vi_s_ibasisprodukt` bp
WHERE bp.vpn_id IS NOT NULL
  AND bp.basisprodukt_id IN (2828, 2831);

-- ========================= Step07 ==================================

SELECT 'step07: löschen der temporären zwischentabellen...' AS log_msg;

-- Note: These TRUNCATE operations were commented out in the original script.
-- TRUNCATE TABLE `sof$ta_means_of_pay`;
-- TRUNCATE TABLE `sof$ta_bank`;
-- TRUNCATE TABLE `sof$ta_bank_verb`;
-- TRUNCATE TABLE `sof$ta_bank_zuord`;

-- ========================= Step08 ==================================

SELECT 'step08: Verarbeitung von \'d_ausd_rechempf.sql\' fehlerfrei beendet.' AS log_msg;