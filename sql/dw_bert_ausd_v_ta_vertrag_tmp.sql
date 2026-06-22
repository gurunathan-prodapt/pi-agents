-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql
-- Job: DW.BERT_AUSD_V_TA_VERTRAG_TMP

-- This SQL is intended to be executed as part of an Airflow BigQueryOperator task.
-- The `v_datum` variable is expected to be passed as a Jinja template from the Airflow context.

TRUNCATE TABLE `bert_staging.ta_vertrag_tmp`;

INSERT INTO `bert_staging.ta_vertrag_tmp` (
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
        cntrct_validity_id)
SELECT
       c.cntrct_id                      AS vertrag_id_carmen,
       bp.bp_id                         AS partner_id_carmen,
       ia.inv_definition_id             AS rechdef_id_carmen,
       ia.account_reference             AS kundenkonto,
       ia.sales_tax_freed               AS mwst_kennzeichen,
       c.rv_num                         AS rahmenvertrag_id,
       ia.billcycle_id                  AS rechnungslauf,
       c.vo_code                        AS vo_kenn,
       c.order_number                   AS order_number,
       n.valid_from                     AS geplant_kuend,
       n.entry_date_of_notice           AS eingang_kuend,
       c.cntrct_start_date              AS vertragsbeginn,
       CASE c.cntrct_st
                           WHEN 5 THEN 'A'
                           WHEN 6 THEN 'L'
                             ELSE ''
                             END    AS vertragsstatus,
       b.sperrart_alle                  AS sperrart,
       b.sperrgrund_alle                AS sperrgrund,
       b.stillegungszeitraum_alle      AS stillegungszeitraum,
       c.twinbill                       AS twincard,
       ct.cds_description               AS dwh_tarifgr_text,
       bf.bindefrist                    AS bindefrist,
       vvl.upgradedatum                 AS letztes_upgrade,
       p.number_time_measurement        AS vertragsbindung,
       p.einheit                        AS vertragsbindungseinheit,
       CASE ia.inv_pay_ty_cv
              WHEN 1 THEN 'U'
              WHEN 2 THEN 'E'
              WHEN 3 THEN 'K'
              WHEN 4 THEN 'B'
              ELSE ''
              END    AS rechnungszahlart,
       CASE ia.inv_media_cv
              WHEN 1 THEN 'Papier'
              WHEN 2 THEN 'ELMO'
              WHEN 3 THEN 'E-Mail'
              WHEN 4 THEN 'Fax'
              WHEN 5 THEN 'Inline/Papier'
              WHEN 6 THEN 'ELMO/Papier'
              ELSE ''
              END AS rechnungsmedium,
       c.twin_vertrag_id                AS twin_vertrag_id,
       CASE
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      p.number_time_measurement = 12
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ params.v_datum }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 9
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ params.v_datum }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 23
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         ELSE  'N'
       END  AS upgradeberechtigt,
        ap.access_point_name    AS apn,
        vvl.upgradegrund        AS upgradegrund,
        ct.cntrct_template_id   AS SV_Id,
        CASE
           WHEN (ct.cntrct_template_id in (5104,5105,5106) or
                (ct.cntrct_template_id >= 5155 and
                 ct.cntrct_template_id <= 5161)
                )
           THEN c.contract_number
           ELSE NULL
        END  AS VDA,
        c.cost_centre           AS cost_centre,
        c.cost_centre_user      AS cost_centre_user,
        c.cntrct_ty             AS cntrct_ty,
        rd.segment_id           AS segment_id,
        ac.rv_action_id         AS rv_action_id,
        ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
        c.commitment_reference_date AS commitment_reference_date,
        c.cntrct_validity_id    AS cntrct_validity_id
  FROM  `bert_source.ta_cntrct_crs3`     AS c
  JOIN  `bert_source.ta_bp_ref`          AS bp
    ON bp.cntrct_cp2_id = c.cntrct_id AND c.cntrct_ty <> 20
  JOIN  `bert_source.ta_inv_acc`         AS ia
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.ta_notice`          AS n
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.ta_barrier_zusgf`   AS b
    ON b.cntrct_id = c.cntrct_id
  JOIN  `bert_source.ta_cntrct_templ`    AS ct
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN  `bert_source.ta_cntrct_valid`    AS cv
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN  `bert_source.ta_period`          AS p
    ON p.period_id = cv.first_period_id
  LEFT JOIN  `bert_source.ta_vvl_upgrade`     AS vvl
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN  `bert_source.ta_apn_ve`          AS ap
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.vi_s_rd_segment`    AS rd
    ON ia.inv_definition_id = rd.rechdef_id_carmen
  LEFT JOIN  `bert_source.ta_action_assoc`    AS ac
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.vi_c_bfc`             AS bf
    ON bf.cntrct_id = c.cntrct_id
  WHERE c.cntrct_ty <> 20

UNION ALL

SELECT
       c.cntrct_id                      AS vertrag_id_carmen,
       bp.bp_id                         AS partner_id_carmen,
       ia.inv_definition_id             AS rechdef_id_carmen,
       ia.account_reference             AS kundenkonto,
       ia.sales_tax_freed               AS mwst_kennzeichen,
       c.rv_num                         AS rahmenvertrag_id,
       ia.billcycle_id                  AS rechnungslauf,
       c.vo_code                        AS vo_kenn,
       c.order_number                   AS order_number,
       n.valid_from                     AS geplant_kuend,
       n.entry_date_of_notice           AS eingang_kuend,
       c.cntrct_start_date              AS vertragsbeginn,
       CASE c.cntrct_st
                           WHEN 5 THEN 'A'
                           WHEN 6 THEN 'L'
                             ELSE ''
                             END    AS vertragsstatus,
       b.sperrart_alle                  AS sperrart,
       b.sperrgrund_alle                AS sperrgrund,
       b.stillegungszeitraum_alle      AS stillegungszeitraum,
       c.twinbill                       AS twincard,
       ct.cds_description               AS dwh_tarifgr_text,
       bf.bindefrist                    AS bindefrist,
       vvl.upgradedatum                 AS letztes_upgrade,
       p.number_time_measurement        AS vertragsbindung,
       p.einheit                        AS vertragsbindungseinheit,
       CASE ia.inv_pay_ty_cv
              WHEN 1 THEN 'U'
              WHEN 2 THEN 'E'
              WHEN 3 THEN 'K'
              WHEN 4 THEN 'B'
              ELSE ''
              END    AS rechnungszahlart,
       CASE ia.inv_media_cv
              WHEN 1 THEN 'Papier'
              WHEN 2 THEN 'ELMO'
              WHEN 3 THEN 'E-Mail'
              WHEN 4 THEN 'Fax'
              WHEN 5 THEN 'Inline/Papier'
              WHEN 6 THEN 'ELMO/Papier'
              ELSE ''
              END AS rechnungsmedium,
       c.twin_vertrag_id                AS twin_vertrag_id,
       CASE
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      p.number_time_measurement = 12
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ params.v_datum }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 9
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ params.v_datum }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 23
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         ELSE  'N'
       END  AS upgradeberechtigt,
        ap.access_point_name    AS apn,
        vvl.upgradegrund        AS upgradegrund,
        ct.cntrct_template_id   AS SV_Id,
        CASE
           WHEN (ct.cntrct_template_id in (5104,5105,5106) or
                (ct.cntrct_template_id >= 5155 and
                 ct.cntrct_template_id <= 5161)
                )
           THEN c.contract_number
           ELSE NULL
        END  AS VDA,
        c.cost_centre           AS cost_centre,
        c.cost_centre_user      AS cost_centre_user,
        c.cntrct_ty             AS cntrct_ty,
        rd.segment_id           AS segment_id,
        ac.rv_action_id         AS rv_action_id,
        ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
        c.commitment_reference_date AS commitment_reference_date,
        c.cntrct_validity_id    AS cntrct_validity_id
  FROM  `bert_source.ta_cntrct_crs3`     AS c
  JOIN  `bert_source.ta_bp_ref`          AS bp
    ON bp.cntrct_cp2_id = c.cntrct_parent AND c.cntrct_ty = 20
  JOIN  `bert_source.ta_inv_acc`         AS ia
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.ta_notice`          AS n
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.ta_barrier_zusgf`   AS b
    ON b.cntrct_id = c.cntrct_id
  JOIN  `bert_source.ta_cntrct_templ`    AS ct
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN  `bert_source.ta_cntrct_valid`    AS cv
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN  `bert_source.ta_period`          AS p
    ON p.period_id = cv.first_period_id
  LEFT JOIN  `bert_source.ta_vvl_upgrade`     AS vvl
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN  `bert_source.ta_apn_ve`          AS ap
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.vi_s_rd_segment`    AS rd
    ON ia.inv_definition_id = rd.rechdef_id_carmen
  LEFT JOIN  `bert_source.ta_action_assoc`    AS ac
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN  `bert_source.vi_c_bfc`             AS bf
    ON bf.cntrct_id = c.cntrct_id;