-- Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_geschaeftspartner.sql
-- For job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.d_ausd_geschaeftspartner_proc`(
  IN p_stichtag_str STRING,
  IN p_job_kennung STRING, -- This parameter is not explicitly used in the original SQL but is passed by the ksh. Included for completeness.
  OUT records_processed INT64
)
BEGIN

  -- Declare a variable for the processing date, derived from p_stichtag_str
  DECLARE v_stichtag DATE;
  DECLARE v_records_count INT64;

  -- Validate and parse the stichtag
  SET v_stichtag = SAFE.PARSE_DATE('%Y%m%d', p_stichtag_str);

  IF v_stichtag IS NULL THEN
    RAISE USING MESSAGE = CONCAT('Invalid date format for p_stichtag_str: ', p_stichtag_str, '. Expected YYYYMMDD.');
  END IF;

  -- ========================= Step00 - Variable Definitions ==================================
  -- Original:
  -- DEFINE v_carmen = "@pcrs1" -- Remote database link, assuming tables are local in BigQuery
  -- COLUMN s_datum new_value v_datum noprint
  -- SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
  -- In BigQuery, we use the p_stichtag_str parameter for the date logic, as per design document.
  -- The original logic for v_datum derivation from dwtk_meldungen is replaced by using p_stichtag_str.

  -- ========================= Step01 - Table Existence Check ==================================
  -- Original: DESC commands.
  -- In BigQuery, table existence is typically checked implicitly by query execution
  -- or managed by schema deployment. No explicit DESC command translation needed.

  -- ========================= Step02 - Truncating Temporary Tables ==================================
  -- Original: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...');
  -- Converting to direct TRUNCATE TABLE statements in BigQuery.

  TRUNCATE TABLE `your_project.your_dataset.sof_ta_segm_prem`;
  TRUNCATE TABLE `your_project.your_dataset.sof_ta_bpr_dn_evn_his`;
  TRUNCATE TABLE `your_project.your_dataset.sof_ta_bpr_dn_evn`;
  TRUNCATE TABLE `your_project.your_dataset.sof_ta_p_gesch_part`;
  TRUNCATE TABLE `your_project.your_dataset.sof_ta_p_dn_nutzer`;
  TRUNCATE TABLE `your_project.your_dataset.sof_ta_p_evn_empf`;


  -- ========================= Step03 - Populate sof_ta_segm_prem ==================================
  INSERT INTO `your_project.your_dataset.sof_ta_segm_prem`
    (BP_ID, SEGMENT_ID)
  SELECT
    BP_ID,
    SEGMENT_ID
  FROM
    `your_project.your_dataset.bpd_ta_bp_valueseg_assoc`;

  -- ========================= Step04 - Populate sof_ta_p_gesch_part ==================================
  INSERT INTO `your_project.your_dataset.sof_ta_p_gesch_part`
    (CNTRCT_ID,
      NAMENSZUSATZ,
      ADRESSZUSATZ,
      FIRMENNAME,
      AKAD_TITEL,
      NACHNAME,
      VORNAME,
      LAND,
      PLZ,
      WOHNORT,
      STRASSE,
      KUNDE_SEGMENT_ID,
      PREM_SEGMENT_ID,
      TM_KUNDENNUMMER,
      MWST_KENNZEICHEN,
      ORGANISATIONSEINHEIT)
  SELECT
    rg.cntrct_cp2_id,
    rg.for_the_attention_of,
    rg.address_attachment,
    COALESCE(rg.corp_unit, bp.organisation_name),
    CASE
      WHEN rg.surname_s IS NULL THEN bp.title
      ELSE ''
    END,
    COALESCE(rg.surname_s, bp.surname),
    COALESCE(rg.first_name_g, bp.first_name),
    rg.land_sd,
    rg.zip_code,
    rg.city,
    CASE
      WHEN rg.street IS NULL THEN
        CASE
          WHEN rg.pobox IS NULL THEN ''
          ELSE CONCAT('Postfach ', rg.pobox)
        END
      ELSE CONCAT(rg.street, ' ', rg.house_nr)
    END,
    CASE pr.segment_id
      WHEN 11 THEN 'SP'
      WHEN 12 THEN 'RV'
      WHEN 13 THEN 'MA'
      WHEN 14 THEN 'SO'
      WHEN 15 THEN 'VJ'
      WHEN 16 THEN 'IN'
      ELSE CAST(pr.segment_id AS STRING)
    END,
    0, -- prem_segment_id, original was commented out or hardcoded to 0
    bp.tm_customerid,
    bp.sales_tax_freed,
    rg.address_attachment_org
  FROM
    `your_project.your_dataset.sof_ta_e_reach_gp` AS rg
  INNER JOIN
    `your_project.your_dataset.sof_ta_e_business_gp` AS bp
    ON rg.bp_id = bp.bp_id
  LEFT JOIN -- Original used (+), which is an outer join
    `your_project.your_dataset.sof_ta_segm_prem` AS pr
    ON rg.bp_id = pr.bp_id;

  -- ========================= Step05 - Populate sof_ta_bpr_dn_evn_his and sof_ta_bpr_dn_evn ==================================

  INSERT INTO `your_project.your_dataset.sof_ta_bpr_dn_evn_his`
    (CNTRCT_ID, BPR_ID, BPRI_COM_ID, CNTRCT_ID_REF, VALID_FROM, VALID_TO, MODIFIED_AT, INSERT_AT)
  SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id,
    bp.cntrct_id_ref,
    bp.valid_from,
    bp.valid_to,
    bp.modified_at,
    bp.insert_at
  FROM
    `your_project.your_dataset.pds_ta_bpri_com` AS bp
  WHERE
    bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056)
    AND bp.insert_at <= v_stichtag
    AND (bp.modified_at IS NULL OR bp.modified_at > v_stichtag)
    AND bp.valid_from <= v_stichtag
    AND bp.is_production = 1;

  INSERT INTO `your_project.your_dataset.sof_ta_bpr_dn_evn`
    (CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, CNTRCT_ID_REF, COLUMN_5VALID_TO)
  SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id AS bpr_instance_id,
    bp.cntrct_id_ref,
    COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231')) AS valid_to
  FROM
    (
      SELECT
        bp1.*,
        MAX(COALESCE(bp1.valid_to, PARSE_DATE('%Y%m%d', '47121231'))) OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id) AS max_valid_to
      FROM
        `your_project.your_dataset.sof_ta_bpr_dn_evn_his` AS bp1
    ) AS bp
  WHERE
    COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = bp.max_valid_to;

  -- ========================= Step06 - Populate sof_ta_p_dn_nutzer ==================================

  INSERT INTO `your_project.your_dataset.sof_ta_p_dn_nutzer`
    (CNTRCT_ID,
      NAMENSZUSATZ,
      ADRESSZUSATZ,
      FIRMENNAME,
      AKAD_TITEL,
      NACHNAME,
      VORNAME,
      LAND,
      PLZ,
      WOHNORT,
      STRASSE,
      ORGANISATIONSEINHEIT,
      MWST_KENNZEICHEN)
  SELECT
    bi.cntrct_id,
    dn.for_the_attention_of,
    dn.address_attachment,
    COALESCE(dn.corp_unit, bp.organisation_name),
    CASE
      WHEN dn.surname_s IS NULL THEN bp.title
      ELSE ''
    END,
    COALESCE(dn.surname_s, bp.surname),
    COALESCE(dn.first_name_g, bp.first_name),
    dn.land_sd,
    dn.zip_code,
    dn.city,
    CASE
      WHEN dn.street IS NULL THEN
        CASE
          WHEN dn.pobox IS NULL THEN ''
          ELSE CONCAT('Postfach ', dn.pobox)
        END
      ELSE CONCAT(dn.street, ' ', dn.house_nr)
    END,
    dn.address_attachment_org,
    bp.sales_tax_freed
  FROM
    `your_project.your_dataset.sof_ta_e_reach_dn` AS dn
  INNER JOIN
    `your_project.your_dataset.sof_ta_e_business_dn` AS bp
    ON dn.bp_id = bp.bp_id
  INNER JOIN
    `your_project.your_dataset.sof_ta_bpr_dn_evn` AS bi
    ON dn.bpr_inst_srvusr_id = bi.bpr_instance_id;

  -- ========================= Step07 - Populate sof_ta_p_evn_empf ==================================

  INSERT INTO `your_project.your_dataset.sof_ta_p_evn_empf`
    (CNTRCT_ID,
      NAMENSZUSATZ,
      ADRESSZUSATZ,
      FIRMENNAME,
      AKAD_TITEL,
      NACHNAME,
      VORNAME,
      LAND,
      PLZ,
      WOHNORT,
      STRASSE,
      ORGANISATIONSEINHEIT,
      MWST_KENNZEICHEN)
  SELECT
    bi.cntrct_id,
    ev.for_the_attention_of,
    ev.address_attachment,
    COALESCE(ev.corp_unit, bp.organisation_name),
    CASE
      WHEN ev.surname_s IS NULL THEN bp.title
      ELSE ''
    END,
    COALESCE(ev.surname_s, bp.surname),
    COALESCE(ev.first_name_g, bp.first_name),
    ev.land_sd,
    ev.zip_code,
    ev.city,
    CASE
      WHEN ev.street IS NULL THEN
        CASE
          WHEN ev.pobox IS NULL THEN ''
          ELSE CONCAT('Postfach ', ev.pobox)
        END
      ELSE CONCAT(ev.street, ' ', ev.house_nr)
    END,
    ev.address_attachment_org,
    bp.sales_tax_freed
  FROM
    `your_project.your_dataset.sof_ta_e_reach_ev` AS ev
  INNER JOIN
    `your_project.your_dataset.sof_ta_e_business_ev` AS bp
    ON ev.bp_id = bp.bp_id
  INNER JOIN
    `your_project.your_dataset.sof_ta_bpr_dn_evn` AS bi
    ON ev.bpr_inst_evnrec_id = bi.bpr_instance_id;

  -- ========================= Step08 - Truncating Temporary Intermediate Tables ==================================
  -- The original script commented out these truncates, but for cleanliness and if they are truly temp tables,
  -- it's good practice to manage them. For this migration, we'll keep them consistent with the original's active parts.
  -- If these are permanent tables used by other processes, this needs re-evaluation.
  -- Original:
  -- --begin DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_segm_prem REUSE STORAGE');
  -- --begin DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_dn_evn REUSE STORAGE');

  -- Count the total records processed by the last INSERT or relevant operations
  -- For now, let's just count from one of the main target tables.
  -- In a real scenario, this would sum up rows from all relevant inserts.
  SELECT COUNT(1) INTO v_records_count FROM `your_project.your_dataset.sof_ta_p_gesch_part`;
  SET records_processed = v_records_count;

END;