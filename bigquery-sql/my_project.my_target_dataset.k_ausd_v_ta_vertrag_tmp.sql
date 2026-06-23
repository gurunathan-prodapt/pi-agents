-- BigQuery Stored Procedure for k_ausd_v_ta_vertrag_tmp
-- Encapsulates the core transformation logic from d_ausd_v_ta_vertrag_tmp.sql, originally executed by k_ausd_v_ta_vertrag_tmp.ksh
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_target_dataset.k_ausd_v_ta_vertrag_tmp`(
  IN v_datum STRING -- Expected format 'YYYYMMDD'
)
BEGIN
  TRUNCATE TABLE `my_project.my_target_dataset.ta_vertrag_tmp`;

  INSERT INTO `my_project.my_target_dataset.ta_vertrag_tmp` (
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
    CAST(c.cntrct_id AS STRING) AS vertrag_id_carmen,
    CAST(bp.bp_id AS STRING) AS partner_id_carmen,
    CAST(ia.inv_definition_id AS STRING) AS rechdef_id_carmen,
    ia.account_reference AS kundenkonto,
    CAST(ia.sales_tax_freed AS STRING) AS mwst_kennzeichen,
    CAST(c.rv_num AS STRING) AS rahmenvertrag_id,
    CAST(ia.billcycle_id AS STRING) AS rechnungslauf,
    c.vo_code AS vo_kenn,
    c.order_number AS order_number,
    DATE(n.valid_from) AS geplant_kuend,
    DATE(n.entry_date_of_notice) AS eingang_kuend,
    DATE(c.cntrct_start_date) AS vertragsbeginn,
    CASE c.cntrct_st
      WHEN 5 THEN 'A'
      WHEN 6 THEN 'L'
      ELSE ''
    END AS vertragsstatus,
    b.sperrart_alle AS sperrart,
    b.sperrgrund_alle AS sperrgrund,
    b.stillegungszeitraum_alle AS stillegungszeitraum,
    CAST(c.twinbill AS STRING) AS twincard,
    ct.cds_description AS dwh_tarifgr_text,
    CAST(bf.bindefrist AS INT64) AS bindefrist,
    DATE(vvl.upgradedatum) AS letztes_upgrade,
    CAST(p.number_time_measurement AS INT64) AS vertragsbindung,
    p.einheit AS vertragsbindungseinheit,
    CASE ia.inv_pay_ty_cv
      WHEN 1 THEN 'U'
      WHEN 2 THEN 'E'
      WHEN 3 THEN 'K'
      WHEN 4 THEN 'B'
      ELSE ''
    END AS rechnungszahlart,
    CASE ia.inv_media_cv
      WHEN 1 THEN 'Papier'
      WHEN 2 THEN 'ELMO'
      WHEN 3 THEN 'E-Mail'
      WHEN 4 THEN 'Fax'
      WHEN 5 THEN 'Inline/Papier'
      WHEN 6 THEN 'ELMO/Papier'
      ELSE ''
    END AS rechnungsmedium,
    CAST(c.twin_vertrag_id AS STRING) AS twin_vertrag_id,
    CASE
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
        AND (
          b.sperrart_alle IS NULL
          OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
        )
      THEN 'J'
      WHEN p.number_time_measurement = 12
        AND DATE_DIFF(
              PARSE_DATE('%Y%m%d', v_datum),
              COALESCE(DATE(c.commitment_reference_date), DATE(c.cntrct_start_date)),
              MONTH
            ) > 9
        AND (
          b.sperrart_alle IS NULL
          OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
        )
      THEN 'J'
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
        AND DATE_DIFF(
              PARSE_DATE('%Y%m%d', v_datum),
              COALESCE(DATE(c.commitment_reference_date), DATE(c.cntrct_start_date)),
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
    CAST(ct.cntrct_template_id AS INT64) AS SV_Id,
    CASE
      WHEN ct.cntrct_template_id IN (5104, 5105, 5106)
        OR (ct.cntrct_template_id BETWEEN 5155 AND 5161)
      THEN c.contract_number
      ELSE NULL
    END AS VDA,
    c.cost_centre AS cost_centre,
    c.cost_centre_user AS cost_centre_user,
    CAST(c.cntrct_ty AS INT64) AS cntrct_ty,
    CAST(rd.segment_id AS STRING) AS segment_id,
    CAST(ac.rv_action_id AS STRING) AS rv_action_id,
    ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
    DATE(c.commitment_reference_date) AS commitment_reference_date,
    CAST(c.cntrct_validity_id AS STRING) AS cntrct_validity_id
  FROM `my_project.source_dataset.sof$ta_cntrct_crs3` c
  JOIN `my_project.source_dataset.sof$ta_bp_ref` bp
    ON bp.cntrct_cp2_id = c.cntrct_id
  JOIN `my_project.source_dataset.sof$ta_inv_acc` ia
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$ta_notice` n
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$ta_barrier_zusgf` b
    ON b.cntrct_id = c.cntrct_id
  JOIN `my_project.source_dataset.sof$ta_cntrct_templ` ct
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN `my_project.source_dataset.sof$ta_cntrct_valid` cv
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN `my_project.source_dataset.sof$ta_period` p
    ON p.period_id = cv.first_period_id
  LEFT JOIN `my_project.source_dataset.sof$ta_vvl_upgrade` vvl
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$ta_apn_ve` ap
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.dwh$vi_s_rd_segment` rd
    ON rd.rechdef_id_carmen = ia.inv_definition_id
  LEFT JOIN `my_project.source_dataset.sof$ta_action_assoc` ac
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$vi_c_bfc` bf
    ON bf.cntrct_id = c.cntrct_id
  WHERE c.cntrct_ty <> 20

  UNION ALL

  SELECT
    CAST(c.cntrct_id AS STRING) AS vertrag_id_carmen,
    CAST(bp.bp_id AS STRING) AS partner_id_carmen,
    CAST(ia.inv_definition_id AS STRING) AS rechdef_id_carmen,
    ia.account_reference AS kundenkonto,
    CAST(ia.sales_tax_freed AS STRING) AS mwst_kennzeichen,
    CAST(c.rv_num AS STRING) AS rahmenvertrag_id,
    CAST(ia.billcycle_id AS STRING) AS rechnungslauf,
    c.vo_code AS vo_kenn,
    c.order_number AS order_number,
    DATE(n.valid_from) AS geplant_kuend,
    DATE(n.entry_date_of_notice) AS eingang_kuend,
    DATE(c.cntrct_start_date) AS vertragsbeginn,
    CASE c.cntrct_st
      WHEN 5 THEN 'A'
      WHEN 6 THEN 'L'
      ELSE ''
    END AS vertragsstatus,
    b.sperrart_alle AS sperrart,
    b.sperrgrund_alle AS sperrgrund,
    b.stillegungszeitraum_alle AS stillegungszeitraum,
    CAST(c.twinbill AS STRING) AS twincard,
    ct.cds_description AS dwh_tarifgr_text,
    CAST(bf.bindefrist AS INT64) AS bindefrist,
    DATE(vvl.upgradedatum) AS letztes_upgrade,
    CAST(p.number_time_measurement AS INT64) AS vertragsbindung,
    p.einheit AS vertragsbindungseinheit,
    CASE ia.inv_pay_ty_cv
      WHEN 1 THEN 'U'
      WHEN 2 THEN 'E'
      WHEN 3 THEN 'K'
      WHEN 4 THEN 'B'
      ELSE ''
    END AS rechnungszahlart,
    CASE ia.inv_media_cv
      WHEN 1 THEN 'Papier'
      WHEN 2 THEN 'ELMO'
      WHEN 3 THEN 'E-Mail'
      WHEN 4 THEN 'Fax'
      WHEN 5 THEN 'Inline/Papier'
      WHEN 6 THEN 'ELMO/Papier'
      ELSE ''
    END AS rechnungsmedium,
    CAST(c.twin_vertrag_id AS STRING) AS twin_vertrag_id,
    CASE
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
        AND (
          b.sperrart_alle IS NULL
          OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
        )
      THEN 'J'
      WHEN p.number_time_measurement = 12
        AND DATE_DIFF(
              PARSE_DATE('%Y%m%d', v_datum),
              COALESCE(DATE(c.commitment_reference_date), DATE(c.cntrct_start_date)),
              MONTH
            ) > 9
        AND (
          b.sperrart_alle IS NULL
          OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
        )
      THEN 'J'
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
        AND DATE_DIFF(
              PARSE_DATE('%Y%m%d', v_datum),
              COALESCE(DATE(c.commitment_reference_date), DATE(c.cntrct_start_date)),
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
    CAST(ct.cntrct_template_id AS INT64) AS SV_Id,
    CASE
      WHEN ct.cntrct_template_id IN (5104, 5105, 5106)
        OR (ct.cntrct_template_id BETWEEN 5155 AND 5161)
      THEN c.contract_number
      ELSE NULL
    END AS VDA,
    c.cost_centre AS cost_centre,
    c.cost_centre_user AS cost_centre_user,
    CAST(c.cntrct_ty AS INT64) AS cntrct_ty,
    CAST(rd.segment_id AS STRING) AS segment_id,
    CAST(ac.rv_action_id AS STRING) AS rv_action_id,
    ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
    DATE(c.commitment_reference_date) AS commitment_reference_date,
    CAST(c.cntrct_validity_id AS STRING) AS cntrct_validity_id
  FROM `my_project.source_dataset.sof$ta_cntrct_crs3` c
  JOIN `my_project.source_dataset.sof$ta_bp_ref` bp
    ON bp.cntrct_cp2_id = c.cntrct_parent
  JOIN `my_project.source_dataset.sof$ta_inv_acc` ia
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$ta_notice` n
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$ta_barrier_zusgf` b
    ON b.cntrct_id = c.cntrct_id
  JOIN `my_project.source_dataset.sof$ta_cntrct_templ` ct
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN `my_project.source_dataset.sof$ta_cntrct_valid` cv
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN `my_project.source_dataset.sof$ta_period` p
    ON p.period_id = cv.first_period_id
  LEFT JOIN `my_project.source_dataset.sof$ta_vvl_upgrade` vvl
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$ta_apn_ve` ap
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.dwh$vi_s_rd_segment` rd
    ON rd.rechdef_id_carmen = ia.inv_definition_id
  LEFT JOIN `my_project.source_dataset.sof$ta_action_assoc` ac
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN `my_project.source_dataset.sof$vi_c_bfc` bf
    ON bf.cntrct_id = c.cntrct_id
  WHERE c.cntrct_ty = 20;
END;