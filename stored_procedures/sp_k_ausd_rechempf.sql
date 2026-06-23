-- BigQuery Stored Procedure for k_ausd_rechempf.ksh
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_rechempf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_rechempf(
  p_job_id STRING,
  p_job_kennung STRING,
  p_stichtag_str STRING, -- Stichtag in 'DDMMYYYY' format from original
  p_wiederanlauf_wert INT64
)
BEGIN
  DECLARE v_stichtag DATE;
  DECLARE v_datum_from_log DATE;
  DECLARE v_today DATE;
  DECLARE v_yesterday DATE;
  DECLARE v_record_count INT64;

  -- Convert p_stichtag_str to DATE
  SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_str);

  -- Get today's and yesterday's date (replacement for gestern.ksh)
  SET v_today = CURRENT_DATE();
  SET v_yesterday = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Replacement for 'SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') ...'
  -- Assuming job_log contains relevant information
  -- If 'BERT_DROP_TEMP_TABLE' is not found, default to '19000101'
  SELECT
    COALESCE(MAX(DATE(t2.end_time)), PARSE_DATE('%Y%m%d', '19000101'))
  INTO
    v_datum_from_log
  FROM
    project.dataset.job_log AS t2
  WHERE
    t2.job_id = 'BERT_DROP_TEMP_TABLE'; -- Or a derived job_kennung that makes sense in BQ

  -- Log message for starting the core processing
  INSERT INTO project.dataset.job_log_messages (job_id, log_time, message_type, message)
  VALUES (p_job_id, CURRENT_TIMESTAMP(), 'INFO', 'Starting core processing (sp_k_ausd_rechempf) for Stichtag: ' || p_stichtag_str);

  -- Step 02: Truncate temporary tables
  -- This replaces calls to isbert_schema.DWPA_UTIL_SKRIPT.runstatement
  TRUNCATE TABLE project.dataset.sof_ta_means_of_pay;
  TRUNCATE TABLE project.dataset.sof_ta_bank;
  TRUNCATE TABLE project.dataset.sof_ta_bank_verb;
  TRUNCATE TABLE project.dataset.sof_ta_bank_zuord;
  TRUNCATE TABLE project.dataset.sof_ta_p_rech_empf;
  TRUNCATE TABLE project.dataset.sof_ta_p_d1_vpn;

  -- Step 03: Erzeuge temp. rechnungsdefinitionen (Insert into sof_ta_means_of_pay)
  INSERT INTO project.dataset.sof_ta_means_of_pay
  (  BP_ID,
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
    RB_PARTITION_ID$)
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
  FROM
    source_project.source_dataset.bpd_ta_means_of_payment AS mop -- Assuming source table is available
  WHERE
    (DATE(mop.insert_at) <= v_datum_from_log
      AND (mop.modified_at IS NULL OR DATE(mop.modified_at) > v_datum_from_log))
    AND (mop.valid_from <= v_datum_from_log
      AND (mop.valid_to IS NULL OR mop.valid_to > v_datum_from_log))
    AND mop.is_production = 1;

  -- Insert into sof_ta_bank
  INSERT INTO project.dataset.sof_ta_bank
  (BANK_ID,
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
    BANK_INTERNATIONAL_ID)
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
    NULL AS BIC,
    NULL AS BANK_INTERNATIONAL_ID
  FROM
    source_project.source_dataset.bpd_ta_bank AS ba
  WHERE
    (DATE(ba.insert_at) <= v_datum_from_log
      AND (ba.MODIFIED_AT IS NULL OR DATE(ba.MODIFIED_AT) > v_datum_from_log))
  UNION ALL
  SELECT
    '-99999' AS BANK_ID, -- Changed from INT64 to STRING to match other BANK_ID
    bi.INSERT_AT,
    bi.COUNTRY_CODE,
    NULL AS BANK_SORT_NAME,
    bi.BANK_NAME,
    bi.INSERT_BY,
    bi.MODIFIED_AT,
    bi.MODIFIED_BY,
    bi.MODIFY_REASON,
    bi.IS_IN_ARCHIVE,
    bi.ROW_VERSION,
    bi.BIC,
    bi.BANK_INTERNATIONAL_ID
  FROM
    source_project.source_dataset.bpd_ta_bank_international AS bi
  WHERE
    (DATE(bi.insert_at) <= v_datum_from_log
      AND (bi.MODIFIED_AT IS NULL OR DATE(bi.MODIFIED_AT) > v_datum_from_log));

  -- Step 04: Erzeuge tabelle sof_ta_bank_verb und sof_ta_bank_zuord
  INSERT INTO project.dataset.sof_ta_bank_verb
  (MEANS_OF_PAYMENT_ID,
    BP_ID,
    ACCOUNT_NUMBER_ACC,
    BANK_NAME,
    BANK_SORT_NAME,
    IBAN,
    BIC)
  SELECT
    mp.MEANS_OF_PAYMENT_ID,
    mp.BP_ID,
    mp.ACCOUNT_NUMBER_ACC,
    ba.BANK_NAME,
    ba.BANK_SORT_NAME,
    mp.IBAN,
    ba.BIC
  FROM
    project.dataset.sof_ta_means_of_pay AS mp
  JOIN
    project.dataset.sof_ta_bank AS ba
  ON
    mp.BANK_ID_ACC = ba.BANK_ID
    OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID;

  INSERT INTO project.dataset.sof_ta_bank_zuord
  (INV_DEF_MOPREF_ID,
    ACCOUNT_NUMBER_ACC,
    BANK_NAME,
    BANK_SORT_NAME,
    IBAN,
    BIC)
  SELECT
    za.inv_def_mopref_id,
    ba.account_number_acc,
    ba.bank_name,
    ba.bank_sort_name,
    ba.iban,
    ba.bic
  FROM
    project.dataset.sof_ta_bank_verb AS ba
  JOIN
    source_project.source_dataset.sof_ta_e_regulierer AS za -- Assuming source table is available
  ON
    za.means_of_payment_id = ba.means_of_payment_id
    AND za.mop_bp_id           = ba.bp_id;

  -- Step 05: Erzeuge tabelle sof_ta_p_rech_empf
  INSERT INTO project.dataset.sof_ta_p_rech_empf
  (KUNDENKONTO,
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
    BIC)
  SELECT
    '0' AS kundenkonto,
    re.inv_def_invrec_id AS rechdef_id,
    '0' AS dpps_kontonummer,
    CASE
      WHEN (re.corp_unit IS NULL AND bp.organisation_name IS NULL)
        THEN
          CASE
            WHEN (re.surname_s IS NULL)
              THEN CONCAT(bp.first_name, ' ', bp.surname)
            ELSE CONCAT(re.first_name_g, ' ', re.surname_s)
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
            ELSE CONCAT('Postfach ', re.pobox)
          END
      ELSE CONCAT(re.street, ' ', re.house_nr)
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
  FROM
    project.dataset.sof_ta_bank_zuord AS ba
  JOIN
    source_project.source_dataset.sof_ta_e_reach_re AS re -- Assuming source table is available
    ON re.inv_def_invrec_id = ba.inv_def_mopref_id
  JOIN
    source_project.source_dataset.sof_ta_e_business_re AS bp -- Assuming source table is available
    ON re.bp_id = bp.bp_id;

  -- Step 06: Erzeuge die tabelle sof_ta_p_d1_vpn
  INSERT INTO project.dataset.sof_ta_p_d1_vpn
  (VERTRAGS_ID,
    VPN_ID )
  SELECT
    bp.vertrags_id,
    bp.vpn_id
  FROM
    source_project.source_dataset.dwh_vi_s_ibasisprodukt AS bp -- Assuming source table is available
  WHERE
    bp.vpn_id IS NOT NULL
    AND bp.basisprodukt_id IN ( 2828 , 2831 ); -- Telemetriezusatzvertraege

  -- Calculate record count (placeholder as original script used tmpFile for this)
  -- For now, just count rows in the main output table.
  SELECT COUNT(1) INTO v_record_count FROM project.dataset.sof_ta_p_rech_empf;

  INSERT INTO project.dataset.job_log_messages (job_id, log_time, message_type, message)
  VALUES (p_job_id, CURRENT_TIMESTAMP(), 'INFO', 'Core processing (sp_k_ausd_rechempf) completed. Records processed: ' || v_record_count);

END;