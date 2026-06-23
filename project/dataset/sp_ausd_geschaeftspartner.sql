-- BigQuery Stored Procedure replacing vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- and executing the logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_geschaeftspartner.sql
-- This procedure extracts and transforms business partner data for the FOS system.

CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_geschaeftspartner`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_eintragsnr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_datum STRING;
  DECLARE v_records_inserted INT64;

  -- Validate p_stichtag format (DDMMYYYY)
  IF NOT REGEXP_CONTAINS(p_stichtag, '^[0-9]{8}$') THEN
    INSERT INTO `project.dataset.job_log`
      (job_kennung, eintragsnr, event_ts, level, errnr, errarg, message)
    VALUES
      (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'ERROR', 193, 'Stichtag', 'Invalid Stichtag format. Expected DDMMYYYY.');
    RAISE USING MESSAGE = 'Invalid Stichtag format: ' || p_stichtag;
  END IF;

  -- Determine v_datum from dwtk_meldungen, equivalent to Oracle's COLUMN ... NEW_VALUE and DEFINE
  -- Assumes `dwtk_meldungen` table exists in `project.dataset` and has `timecreated` and `job_kennung` columns.
  -- Default to '19000101' if no entry found.
  SET v_datum = (
    SELECT
      COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM
      `project.dataset.dwtk_meldungen` m
    WHERE
      m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Step00: Variable v_datum set to: ', v_datum));

  -- ========================= Step01 ==================================
  -- prompt step01: prfung, ob die bentigeten ereignis-tabellen vorhanden sind...
  -- In BigQuery, table existence is typically checked during query compilation or
  -- relies on the schema being correctly deployed. DESC statements are omitted.

  -- ========================= Step02 ==================================
  -- prompt step02: lschen der temporren-tabellen...
  -- Replacing DWPA_UTIL_SKRIPT.runstatement with direct TRUNCATE TABLE.
  TRUNCATE TABLE `project.dataset.sof$ta_segm_prem`;
  TRUNCATE TABLE `project.dataset.sof$ta_bpr_dn_evn`;
  TRUNCATE TABLE `project.dataset.sof$ta_bpr_dn_evn_his`;
  TRUNCATE TABLE `project.dataset.sof$ta_p_gesch_part`;
  TRUNCATE TABLE `project.dataset.sof$ta_p_dn_nutzer`;
  TRUNCATE TABLE `project.dataset.sof$ta_p_evn_empf`;

  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO', 'Step02: Temporary tables truncated.');

  -- ========================= Step03 ==================================
  -- prompt step03: erzeuge tabelle sof$ta_p_premium...
  INSERT INTO `project.dataset.sof$ta_segm_prem`
  (BP_ID, SEGMENT_ID)
  SELECT
    BP_ID,
    SEGMENT_ID
  FROM
    `project.dataset.bpd$ta_bp_valueseg_assoc`; -- &v_carmen removed as it was likely a DB link/alias

  SET v_records_inserted = @@row_count;
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Step03: Inserted ', CAST(v_records_inserted AS STRING), ' records into sof$ta_segm_prem.'));

  -- ========================= Step04 ==================================
  -- prompt step04: erzeuge tabelle sof$ta_p_geschaeftspartner...
  INSERT INTO `project.dataset.sof$ta_p_gesch_part`
  (CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL, NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, KUNDE_SEGMENT_ID, PREM_SEGMENT_ID, TM_KUNDENNUMMER, MWST_KENNZEICHEN, ORGANISATIONSEINHEIT)
  SELECT
    rg.cntrct_cp2_id,
    rg.for_the_attention_of,
    rg.address_attachment,
    COALESCE(rg.corp_unit, bp.organisation_name),
    CASE WHEN rg.surname_s IS NULL THEN bp.title ELSE '' END,
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
    0, -- prem_segment_id, original commented out and set to 0
    bp.tm_customerid,
    bp.sales_tax_freed,
    rg.address_attachment_org
  FROM
    `project.dataset.sof$ta_e_reach_gp` rg
  JOIN
    `project.dataset.sof$ta_e_business_gp` bp
    ON rg.bp_id = bp.bp_id
  LEFT JOIN
    `project.dataset.sof$ta_segm_prem` pr
    ON rg.bp_id = pr.bp_id;

  SET v_records_inserted = @@row_count;
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Step04: Inserted ', CAST(v_records_inserted AS STRING), ' records into sof$ta_p_gesch_part.'));

  -- ========================= Step05 ==================================
  -- prompt step05: erzeuge bpr_instance-Tabelle zur Ermittlung der Cntrct_ID der Dienstenutzer und EVN-Empfnger...
  INSERT INTO `project.dataset.sof$ta_bpr_dn_evn_his`
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
    `project.dataset.pds$ta_bpri_com` bp
  WHERE
    bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056)
    AND bp.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
    AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', v_datum))
    AND bp.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
    AND bp.is_production = 1;

  SET v_records_inserted = @@row_count;
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Step05: Inserted ', CAST(v_records_inserted AS STRING), ' records into sof$ta_bpr_dn_evn_his.'));

  INSERT INTO `project.dataset.sof$ta_bpr_dn_evn`
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
        `project.dataset.sof$ta_bpr_dn_evn_his` bp1
    ) bp
  WHERE
    COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = bp.max_valid_to;

  SET v_records_inserted = @@row_count;
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Step05: Inserted ', CAST(v_records_inserted AS STRING), ' records into sof$ta_bpr_dn_evn.'));

  -- Original comments indicate TRUNCATE TABLE sof$ta_bpr_dn_evn_his, but commented out.
  -- Following original script logic, not truncating here unless uncommented.

  -- ========================= Step06 ==================================
  -- prompt step06: erzeuge tabelle sof$ta_p_dienstenutzer...
  INSERT INTO `project.dataset.sof$ta_p_dn_nutzer`
  (CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL, NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN)
  SELECT
    bi.cntrct_id,
    dn.for_the_attention_of,
    dn.address_attachment,
    COALESCE(dn.corp_unit, bp.organisation_name),
    CASE WHEN dn.surname_s IS NULL THEN bp.title ELSE '' END,
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
    `project.dataset.sof$ta_e_reach_dn` dn
  JOIN
    `project.dataset.sof$ta_e_business_dn` bp
    ON dn.bp_id = bp.bp_id
  JOIN
    `project.dataset.sof$ta_bpr_dn_evn` bi
    ON dn.bpr_inst_srvusr_id = bi.bpr_instance_id;

  SET v_records_inserted = @@row_count;
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Step06: Inserted ', CAST(v_records_inserted AS STRING), ' records into sof$ta_p_dn_nutzer.'));

  -- ========================= Step07 ==================================
  -- prompt step07: erzeuge tabelle sof$ta_p_evn_empfnger...
  INSERT INTO `project.dataset.sof$ta_p_evn_empf`
  (CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL, NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN)
  SELECT
    bi.cntrct_id,
    ev.for_the_attention_of,
    ev.address_attachment,
    COALESCE(ev.corp_unit, bp.organisation_name),
    CASE WHEN ev.surname_s IS NULL THEN bp.title ELSE '' END,
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
    `project.dataset.sof$ta_e_reach_ev` ev
  JOIN
    `project.dataset.sof$ta_e_business_ev` bp
    ON ev.bp_id = bp.bp_id
  JOIN
    `project.dataset.sof$ta_bpr_dn_evn` bi
    ON ev.bpr_inst_evnrec_id = bi.bpr_instance_id;

  SET v_records_inserted = @@row_count;
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Step07: Inserted ', CAST(v_records_inserted AS STRING), ' records into sof$ta_p_evn_empf.'));

  -- ========================= Step08 ==================================
  -- prompt step08: lschen der temporren zwischentabelle...
  -- Original comments indicate TRUNCATE TABLE sof$ta_segm_prem and sof$ta_bpr_dn_evn, but commented out.
  -- Following original script logic, not truncating here unless uncommented.

  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'INFO', 'Verarbeitung von ''d_ausd_geschaeftspartner.sql'' fehlerfrei beendet.');

END;