-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_geschaeftspartner.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE OR REPLACE PROCEDURE your_project.your_dataset_procs.d_ausd_geschaeftspartner_proc(
  p_EintragsNr STRING,
  p_JobKennung STRING,
  v_stichtag_date DATE,
  v_restart INT64,
  v_datum_heute DATE,
  v_datum_gestern DATE,
  OUT v_records INT64
)
OPTIONS(description="BigQuery Stored Procedure for core ETL logic of d_ausd_geschaeftspartner.sql")
BEGIN
  -- Variable Declaration
  DECLARE v_datum_str STRING;
  DECLARE insert_row_count INT64;

  SET v_records = 0; -- Initialize the OUT parameter

  -- Step 00: Variable Definition
  -- Determine v_datum from isbert_schema.dwtk_meldungen
  -- Original: SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
  SET v_datum_str = (
    SELECT
      IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM your_project.your_dataset_staging.dwtk_meldungen AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Step 01: Check for required event tables (no direct BigQuery equivalent for DESC check)
  -- This step is omitted as BigQuery schema validation occurs at query time.
  -- The presence of tables in the staging dataset is assumed based on the ingestion pipeline.

  -- Step 02: Truncate temporary/target tables
  TRUNCATE TABLE your_project.your_dataset_target.sof_ta_segm_prem;
  TRUNCATE TABLE your_project.your_dataset_target.sof_ta_bpr_dn_evn;
  TRUNCATE TABLE your_project.your_dataset_target.sof_ta_bpr_dn_evn_his;
  TRUNCATE TABLE your_project.your_dataset_target.sof_ta_p_gesch_part;
  TRUNCATE TABLE your_project.your_dataset_target.sof_ta_p_dn_nutzer;
  TRUNCATE TABLE your_project.your_dataset_target.sof_ta_p_evn_empf;

  -- Step 03: Populate target.sof_ta_segm_prem
  -- Original: FROM bpd$ta_bp_valueseg_assoc &v_carmen
  INSERT INTO your_project.your_dataset_target.sof_ta_segm_prem
  (BP_ID, SEGMENT_ID)
  SELECT
    BP_ID,
    SEGMENT_ID
  FROM your_project.your_dataset_staging.bpd_ta_bp_valueseg_assoc; -- Assuming ingestion into staging dataset

  SET insert_row_count = @@row_count;
  SET v_records = v_records + insert_row_count;

  -- Step 04: Populate target.sof_ta_p_gesch_part
  INSERT INTO your_project.your_dataset_target.sof_ta_p_gesch_part
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
    0, -- pr.premium, FD (21.05.2008) -> 0
    bp.tm_customerid,
    bp.sales_tax_freed,
    rg.address_attachment_org
  FROM
    your_project.your_dataset_staging.sof_ta_e_reach_gp AS rg
  INNER JOIN
    your_project.your_dataset_staging.sof_ta_e_business_gp AS bp
    ON rg.bp_id = bp.bp_id
  LEFT JOIN -- (pr.bp_id (+))
    your_project.your_dataset_target.sof_ta_segm_prem AS pr
    ON rg.bp_id = pr.bp_id;

  SET insert_row_count = @@row_count;
  SET v_records = v_records + insert_row_count;

  -- Step 05: Populate target.sof_ta_bpr_dn_evn_his and target.sof_ta_bpr_dn_evn
  INSERT INTO your_project.your_dataset_target.sof_ta_bpr_dn_evn_his
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
    your_project.your_dataset_staging.pds_ta_bpri_com AS bp -- Original: pds$ta_bpri_com &v_carmen bp
  WHERE
    bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056)
    AND bp.insert_at <= PARSE_DATE('%Y%m%d', v_datum_str) -- Original: TO_DATE('&v_datum','YYYYMMDD')
    AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', v_datum_str))
    AND bp.valid_from <= PARSE_DATE('%Y%m%d', v_datum_str)
    AND bp.is_production = 1;

  SET insert_row_count = @@row_count;
  SET v_records = v_records + insert_row_count;

  INSERT INTO your_project.your_dataset_target.sof_ta_bpr_dn_evn
  (CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, CNTRCT_ID_REF, COLUMN_5VALID_TO)
  SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id, -- AS bpr_instance_id
    bp.cntrct_id_ref,
    COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231'))
  FROM
    (
      SELECT
        bp1.*,
        MAX(COALESCE(bp1.valid_to, PARSE_DATE('%Y%m%d', '47121231'))) OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id) AS max_valid_to
      FROM
        your_project.your_dataset_target.sof_ta_bpr_dn_evn_his AS bp1
    ) AS bp
  WHERE
    COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = bp.max_valid_to;

  SET insert_row_count = @@row_count;
  SET v_records = v_records + insert_row_count;

  -- Step 06: Populate target.sof_ta_p_dn_nutzer
  INSERT INTO your_project.your_dataset_target.sof_ta_p_dn_nutzer
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
    your_project.your_dataset_staging.sof_ta_e_reach_dn AS dn
  INNER JOIN
    your_project.your_dataset_staging.sof_ta_e_business_dn AS bp
    ON dn.bp_id = bp.bp_id
  INNER JOIN
    your_project.your_dataset_target.sof_ta_bpr_dn_evn AS bi
    ON dn.bpr_inst_srvusr_id = bi.bpr_instance_id;

  SET insert_row_count = @@row_count;
  SET v_records = v_records + insert_row_count;

  -- Step 07: Populate target.sof_ta_p_evn_empf
  INSERT INTO your_project.your_dataset_target.sof_ta_p_evn_empf
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
    your_project.your_dataset_staging.sof_ta_e_reach_ev AS ev
  INNER JOIN
    your_project.your_dataset_staging.sof_ta_e_business_ev AS bp
    ON ev.bp_id = bp.bp_id
  INNER JOIN
    your_project.your_dataset_target.sof_ta_bpr_dn_evn AS bi
    ON ev.bpr_inst_evnrec_id = bi.bpr_instance_id;

  SET insert_row_count = @@row_count;
  SET v_records = v_records + insert_row_count;

  -- Step 08: Truncate temporary intermediate tables (original script had these commented out)
  -- Not explicitly re-truncating here as the target tables are already truncated at the start
  -- and intermediate tables `sof_ta_segm_prem` and `sof_ta_bpr_dn_evn` are used in subsequent steps.

END;