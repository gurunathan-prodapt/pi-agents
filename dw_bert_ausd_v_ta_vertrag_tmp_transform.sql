-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql
-- Job: DW.BERT_AUSD_V_TA_VERTRAG_TMP

-- BigQuery Script

DECLARE v_datum STRING DEFAULT (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `project.dataset.sof$ta_vertrag_tmp`; -- Placeholder for actual project.dataset.table

INSERT INTO `project.dataset.sof$ta_vertrag_tmp` ( -- Placeholder for actual project.dataset.table
  vertrag_id_carmen,
  partner_id_carmen,
  rechdef_id_carmen,
  kundenkonto,
  mwst_kennzeichen,
  rahmenvertrag_id,
  rechnungslauf,
  vo_kenn,
  order_number,
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
  SV_Id,
  VDA,
  cost_centre,
  cost_centre_user,
  cntrct_ty,
  segment_id,
  rv_action_id,
  rechn_inh_konfig_text,
  commitment_reference_date,
  cntrct_validity_id
)
SELECT
  c.cntrct_id AS vertrag_id_carmen,
  bp.bp_id AS partner_id_carmen,
  ia.inv_definition_id AS rechdef_id_carmen,
  ia.account_reference AS kundenkonto,
  ia.sales_tax_freed AS mwst_kennzeichen,
  c.rv_num AS rahmenvertrag_id,
  ia.billcycle_id AS rechnungslauf,
  c.vo_code AS vo_kenn,
  c.order_number AS order_number,
  n.valid_from AS geplant_kuend,
  n.entry_date_of_notice AS eingang_kuend,
  c.cntrct_start_date AS vertragsbeginn,
  CASE
    WHEN c.cntrct_st = 5 THEN 'A'
    WHEN c.cntrct_st = 6 THEN 'L'
    ELSE ''
  END AS vertragsstatus,
  b.sperrart_alle AS sperrart,
  b.sperrgrund_alle AS sperrgrund,
  b.stilllegungszeitraum_alle AS stillegungszeitraum,
  c.twinbill AS twincard,
  ct.cds_description AS dwh_tarifgr_text,
  bf.bindefrist AS bindefrist,
  vvl.upgradedatum AS letztes_upgrade,
  p.number_time_measurement AS vertragsbindung,
  p.einheit AS vertragsbindungseinheit,
  CASE
    WHEN ia.inv_pay_ty_cv = 1 THEN 'U'
    WHEN ia.inv_pay_ty_cv = 2 THEN 'E'
    WHEN ia.inv_pay_ty_cv = 3 THEN 'K'
    WHEN ia.inv_pay_ty_cv = 4 THEN 'B'
    ELSE ''
  END AS rechnungszahlart,
  CASE
    WHEN ia.inv_media_cv = 1 THEN 'Papier'
    WHEN ia.inv_media_cv = 2 THEN 'ELMO'
    WHEN ia.inv_media_cv = 3 THEN 'E-Mail'
    WHEN ia.inv_media_cv = 4 THEN 'Fax'
    WHEN ia.inv_media_cv = 5 THEN 'Inline/Papier'
    WHEN ia.inv_media_cv = 6 THEN 'ELMO/Papier'
    ELSE ''
  END AS rechnungsmedium,
  c.twin_vertrag_id AS twin_vertrag_id,
  CASE
    WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
      AND (
        b.sperrart_alle IS NULL
        OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
      )
    THEN 'J'
    WHEN p.number_time_measurement = 12
      AND DATE_DIFF(
            DATE(PARSE_DATE('%Y%m%d', v_datum)),
            DATE(COALESCE(c.commitment_reference_date, c.cntrct_start_date)),
            MONTH
          ) > 9
      AND (
        b.sperrart_alle IS NULL
        OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
      )
    THEN 'J'
    WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
      AND DATE_DIFF(
            DATE(PARSE_DATE('%Y%m%d', v_datum)),
            DATE(COALESCE(c.commitment_reference_date, c.cntrct_start_date)),
            MONTH
          ) > 23
      AND (
        b.sperrart_alle IS NULL
        OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
      )
    THEN 'J'
    ELSE 'N'
  END AS upgradeberechtigt,
  ap.access_point_name AS apn,
  vvl.upgradegrund AS upgradegrund,
  ct.cntrct_template_id AS SV_Id,
  CASE
    WHEN ct.cntrct_template_id IN (5104, 5105, 5106)
      OR (ct.cntrct_template_id BETWEEN 5155 AND 5161)
    THEN c.contract_number
    ELSE NULL
  END AS VDA,
  c.cost_centre AS cost_centre,
  c.cost_centre_user AS cost_centre_user,
  c.cntrct_ty AS cntrct_ty,
  rd.segment_id AS segment_id,
  ac.rv_action_id AS rv_action_id,
  ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
  c.commitment_reference_date AS commitment_reference_date,
  c.cntrct_validity_id AS cntrct_validity_id
FROM `sof$ta_cntrct_crs3` c -- Placeholder for actual project.dataset.table
JOIN `sof$ta_bp_ref` bp -- Placeholder for actual project.dataset.table
  ON bp.cntrct_cp2_id = c.cntrct_id
JOIN `sof$ta_inv_acc` ia -- Placeholder for actual project.dataset.table
  ON ia.cntrct_id = c.cntrct_id
JOIN `sof$ta_cntrct_templ` ct -- Placeholder for actual project.dataset.table
  ON ct.cntrct_template_id = c.cntrct_template_id
LEFT JOIN `sof$ta_notice` n -- Placeholder for actual project.dataset.table
  ON n.cntrct_id = c.cntrct_id
LEFT JOIN `sof$ta_barrier_zusgf` b -- Placeholder for actual project.dataset.table
  ON b.cntrct_id = c.cntrct_id
LEFT JOIN `sof$ta_cntrct_valid` cv -- Placeholder for actual project.dataset.table
  ON cv.cntrct_validity_id = c.cntrct_validity_id
LEFT JOIN `sof$ta_period` p -- Placeholder for actual project.dataset.table
  ON p.period_id = cv.first_period_id
LEFT JOIN `sof$ta_vvl_upgrade` vvl -- Placeholder for actual project.dataset.table
  ON vvl.vertrags_id = c.cntrct_id
LEFT JOIN `sof$ta_apn_ve` ap -- Placeholder for actual project.dataset.table
  ON ap.cntrct_id = c.cntrct_id
LEFT JOIN `dwh$vi_s_rd_segment` rd -- Placeholder for actual project.dataset.table
  ON ia.inv_definition_id = rd.rechdef_id_carmen
LEFT JOIN `sof$ta_action_assoc` ac -- Placeholder for actual project.dataset.table
  ON ac.cntrct_id = c.cntrct_id
LEFT JOIN `sof$vi_c_bfc` bf -- Placeholder for actual project.dataset.table
  ON bf.cntrct_id = c.cntrct_id
WHERE bp.cntrct_cp2_id = c.cntrct_id
  AND c.cntrct_ty <> 20

UNION ALL

SELECT
  c.cntrct_id AS vertrag_id_carmen,
  bp.bp_id AS partner_id_carmen,
  ia.inv_definition_id AS rechdef_id_carmen,
  ia.account_reference AS kundenkonto,
  ia.sales_tax_freed AS mwst_kennzeichen,
  c.rv_num AS rahmenvertrag_id,
  ia.billcycle_id AS rechnungslauf,
  c.vo_code AS vo_kenn,
  c.order_number AS order_number,
  n.valid_from AS geplant_kuend,
  n.entry_date_of_notice AS eingang_kuend,
  c.cntrct_start_date AS vertragsbeginn,
  CASE
    WHEN c.cntrct_st = 5 THEN 'A'
    WHEN c.cntrct_st = 6 THEN 'L'
    ELSE ''
  END AS vertragsstatus,
  b.sperrart_alle AS sperrart,
  b.sperrgrund_alle AS sperrgrund,
  b.stilllegungszeitraum_alle AS stillegungszeitraum,
  c.twinbill AS twincard,
  ct.cds_description AS dwh_tarifgr_text,
  bf.bindefrist AS bindefrist,
  vvl.upgradedatum AS letztes_upgrade,
  p.number_time_measurement AS vertragsbindung,
  p.einheit AS vertragsbindungseinheit,
  CASE
    WHEN ia.inv_pay_ty_cv = 1 THEN 'U'
    WHEN ia.inv_pay_ty_cv = 2 THEN 'E'
    WHEN ia.inv_pay_ty_cv = 3 THEN 'K'
    WHEN ia.inv_pay_ty_cv = 4 THEN 'B'
    ELSE ''
  END AS rechnungszahlart,
  CASE
    WHEN ia.inv_media_cv = 1 THEN 'Papier'
    WHEN ia.inv_media_cv = 2 THEN 'ELMO'
    WHEN ia.inv_media_cv = 3 THEN 'E-Mail'
    WHEN ia.inv_media_cv = 4 THEN 'Fax'
    WHEN ia.inv_media_cv = 5 THEN 'Inline/Papier'
    WHEN ia.inv_media_cv = 6 THEN 'ELMO/Papier'
    ELSE ''
  END AS rechnungsmedium,
  c.twin_vertrag_id AS twin_vertrag_id,
  CASE
    WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
      AND (
        b.sperrart_alle IS NULL
        OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
      )
    THEN 'J'
    WHEN p.number_time_measurement = 12
      AND DATE_DIFF(
            DATE(PARSE_DATE('%Y%m%d', v_datum)),
            DATE(COALESCE(c.commitment_reference_date, c.cntrct_start_date)),
            MONTH
          ) > 9
      AND (
        b.sperrart_alle IS NULL
        OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
      )
    THEN 'J'
    WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
      AND DATE_DIFF(
            DATE(PARSE_DATE('%Y%m%d', v_datum)),
            DATE(COALESCE(c.commitment_reference_date, c.cntrct_start_date)),
            MONTH
          ) > 23
      AND (
        b.sperrart_alle IS NULL
        OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
      )
    THEN 'J'
    ELSE 'N'
  END AS upgradeberechtigt,
  ap.access_point_name AS apn,
  vvl.upgradegrund AS upgradegrund,
  ct.cntrct_template_id AS SV_Id,
  CASE
    WHEN ct.cntrct_template_id IN (5104, 5105, 5106)
      OR (ct.cntrct_template_id BETWEEN 5155 AND 5161)
    THEN c.contract_number
    ELSE NULL
  END AS VDA,
  c.cost_centre AS cost_centre,
  c.cost_centre_user AS cost_centre_user,
  c.cntrct_ty AS cntrct_ty,
  rd.segment_id AS segment_id,
  ac.rv_action_id AS rv_action_id,
  ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
  c.commitment_reference_date AS commitment_reference_date,
  c.cntrct_validity_id AS cntrct_validity_id
FROM `sof$ta_cntrct_crs3` c -- Placeholder for actual project.dataset.table
JOIN `sof$ta_bp_ref` bp -- Placeholder for actual project.dataset.table
  ON bp.cntrct_cp2_id = c.cntrct_parent
JOIN `sof$ta_inv_acc` ia -- Placeholder for actual project.dataset.table
  ON ia.cntrct_id = c.cntrct_id
JOIN `sof$ta_cntrct_templ` ct -- Placeholder for actual project.dataset.table
  ON ct.cntrct_template_id = c.cntrct_template_id
LEFT JOIN `sof$ta_notice` n -- Placeholder for actual project.dataset.table
  ON n.cntrct_id = c.cntrct_id
LEFT JOIN `sof$ta_barrier_zusgf` b -- Placeholder for actual project.dataset.table
  ON b.cntrct_id = c.cntrct_id
LEFT JOIN `sof$ta_cntrct_valid` cv -- Placeholder for actual project.dataset.table
  ON cv.cntrct_validity_id = c.cntrct_validity_id
LEFT JOIN `sof$ta_period` p -- Placeholder for actual project.dataset.table
  ON p.period_id = cv.first_period_id
LEFT JOIN `sof$ta_vvl_upgrade` vvl -- Placeholder for actual project.dataset.table
  ON vvl.vertrags_id = c.cntrct_id
LEFT JOIN `sof$ta_apn_ve` ap -- Placeholder for actual project.dataset.table
  ON ap.cntrct_id = c.cntrct_id
LEFT JOIN `dwh$vi_s_rd_segment` rd -- Placeholder for actual project.dataset.table
  ON ia.inv_definition_id = rd.rechdef_id_carmen
LEFT JOIN `sof$ta_action_assoc` ac -- Placeholder for actual project.dataset.table
  ON ac.cntrct_id = c.cntrct_id
LEFT JOIN `sof$vi_c_bfc` bf -- Placeholder for actual project.dataset.table
  ON bf.cntrct_id = c.cntrct_id
WHERE c.cntrct_ty = 20;