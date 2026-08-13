-- BigQuery SQL Migration Script
-- Target: BigQuery Standard SQL (Procedural Scripting)
-- Source File: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql

BEGIN
  -- Declare variable to hold the processing date
  DECLARE v_datum STRING;

  SELECT 'variablendefinitionen' AS log_message;

  -- Step 1: Determine reporting date (Stichtag ermitteln)
  -- NVL() replaced with COALESCE()
  -- TO_CHAR() replaced with FORMAT_TIMESTAMP()
  SET v_datum = (
    SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `isbert_schema.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  SELECT 'tracing und settings' AS log_message;

  SELECT 'tabelle von vorherigem lauf leeren' AS log_message;

  -- Step 2: Empty the main target staging table from previous run
  TRUNCATE TABLE `sof.sof$ta_p_vertrag`;

  SELECT 'vertragstabelle sof$ta_p_vertrag (nachbearbeitung der twinbill vertraege)' AS log_message;

  -- Step 3: Populate destination contract table
  -- Oracle parallel hints are stripped.
  -- Oracle outer join (+) syntax converted to standard ANSI LEFT OUTER JOIN.
  INSERT INTO `sof.sof$ta_p_vertrag` (
    vertrag_id_carmen,
    partner_id_carmen,
    rechdef_id_carmen,
    kundenkonto,
    mwst_kennzeichen,
    rahmenvertrag_id,
    rechnungslauf,
    vo_kenn,
    geplant_kuend,
    eingang_kuend,
    vertragsbeginn,
    vertragsstatus,
    sperrart,
    sperrgrund,
    stillegungszeitraum,
    twincard,
    dwh_tarifgr_text,
    bindefrist,
    letztes_upgrade,
    vertragsbindung,
    vertragsbindungseinheit,
    rechnungszahlart,
    rechnungsmedium,
    twin_vertrag_id,
    upgradeberechtigt,
    apn,
    upgradegrund,
    sv_id,
    vda,
    cost_centre,
    cost_centre_user,
    cntrct_ty,
    segment_id,
    rv_action_id,
    rechn_inh_konfig_text,
    order_number,
    commitment_reference_date,
    cntrct_validity_id
  )
  SELECT
    v.vertrag_id_carmen,
    v.partner_id_carmen,
    v.rechdef_id_carmen,
    v.kundenkonto,
    v.mwst_kennzeichen,
    v.rahmenvertrag_id AS rahmenvertrag_id,
    v.rechnungslauf,
    v.vo_kenn AS vo_kenn,
    v.geplant_kuend,
    v.eingang_kuend,
    v.vertragsbeginn,
    v.vertragsstatus,
    v.sperrart,
    v.sperrgrund,
    v.stillegungszeitraum,
    v.twincard,
    v.dwh_tarifgr_text,
    v.bindefrist,
    v.letztes_upgrade,
    v.vertragsbindung,
    v.vertragsbindungseinheit,
    v.rechnungszahlart,
    v.rechnungsmedium,
    v.twin_vertrag_id,
    v.upgradeberechtigt,
    v.apn,
    v.upgradegrund,
    v.sv_id,
    v.vda,
    v.cost_centre,
    v.cost_centre_user,
    v.cntrct_ty,
    v.segment_id,
    v.rv_action_id,
    v.rechn_inh_konfig_text,
    v.order_number,
    v.commitment_reference_date,
    v.cntrct_validity_id
  FROM
    `sof.sof$ta_vertrag_tmp` AS v
  LEFT OUTER JOIN
    `sof.sof$ta_vertrag_tmp` AS pv
    ON v.twin_vertrag_id = pv.vertrag_id_carmen;

  SELECT 'leeren der temporaeren zwischentabellen' AS log_message;

  -- Step 4: Empty intermediate staging tables (Leeren der temporaeren Zwischentabellen)
  -- Oracle dynamic PL/SQL calls are replaced with direct static SQL.
  -- Storage options (DROP STORAGE, REUSE STORAGE) are stripped.
  TRUNCATE TABLE `sof.sof$ta_disc_zusgf`;
  TRUNCATE TABLE `sof.sof$ta_discount`;
  TRUNCATE TABLE `sof.sof$ta_barrier_zusgf`;
  TRUNCATE TABLE `sof.sof$ta_barrier`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_crs`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_templ`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_valid`;
  TRUNCATE TABLE `sof.sof$ta_period`;
  TRUNCATE TABLE `sof.sof$ta_bp_ref`;
  TRUNCATE TABLE `sof.sof$ta_inv_assign`;
  TRUNCATE TABLE `sof.sof$ta_inv_def`;
  TRUNCATE TABLE `sof.sof$ta_acc_ref`;
  TRUNCATE TABLE `sof.sof$ta_notice`;
  TRUNCATE TABLE `sof.sof$ta_apn_ve`;
  TRUNCATE TABLE `sof.sof$ta_discount_rr`;
  TRUNCATE TABLE `sof.sof$ta_vvl_dwh`;
  TRUNCATE TABLE `sof.sof$ta_vvl_upgrade`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_crs2`;
  TRUNCATE TABLE `sof.sof$ta_cntrct_crs3`;
  TRUNCATE TABLE `sof.sof$ta_inv_acc`;
  TRUNCATE TABLE `sof.sof$ta_vertrag_tmp`;
  TRUNCATE TABLE `sof.sof$ta_action_assoc`;

  SELECT 'Verarbeitung fehlerfrei beendet.' AS log_message;

END;