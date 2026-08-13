-- ===================================================================
-- Datei:  d_ausd_vertrag.sql
-- Datum:  16.10.2002
-- Autor:  Martin Buettner
-- ===================================================================
--
-- Modifikationen
----------------------------------------------------------------------
-- Version Datum    Autor  Dokumentation
-- 3.1.0   20030109 sj     Tabellennamenerweiterung um das tagesdatum
--         20030115 mb     Modifikationen und Erweiterung gemaess Protokoll:
--                         (u.a.: Ausgabe der Rabatthoehe, Sperrgruende, Sperrzeiten)
-- 7.5.0   20040831 Roh    Neues Attr. is_production in CDS$TA_DISCOUNT
--                         Umstellung auf parallel degree 4
-- 7.5.1   20041011 mb     Modifikationen fuer den neuen Rabattreport
-- ab 2005 neue Releasenummern
-- 5.1.1   20050204 Roh    neues Feld SEGMENT_ID auf Rechnugsdefiniton
-- 5.4.0   20090901 Roh    spool ins Unterverzeichnis ./tmp
-- 5.4.1   20051025 mb     Erweiterung fuer Pooling Report: Rechnungsinhaltskonfigurationstext
--                                                          (inv_cont_config_id)
-- 5.4.2   20051117 mb     Ersetzung der ANALYZE Kommandos durch GATHER_TABLE_STATS
-- 5.4.3   20051119 hs     Mergen auf dwh$ta_p_vertrag_<datum> zum Addiern von Stillegungstagen
-- 5.4.4   20051202 hs     Laufzeittuning zu Step 201b (merge via FTS)
-- 6.1.0   20051220 Roh    Neues Feld 0B-Nummer (CDS$TA_CNTRCT.ORDER_NUMBER)
-- 6.3.0   20060919 me/hs  Für Berechnung Bindefrist mit Sperren/Stillegungen: Erweiterung um Sperrklasse 7
-- 6.4.0   20061122 me     Umstellung wegen Datenmodell-Aenderung in Carmen:
--                         Tab. CDS$TA_BARRIER_KIND_CV -> CDS$TA_BARRIER_KIND,
--                         tab. CDS$TA_DESCRIPTION     -> CDS$TA_CARE_DESCRIPTION
-- 6.4.0   20061121 RR     Bestimmung Substitutions-Variable v_datum aus
--                         Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR     Überflüssige ANALYZE/STATISTICS Kommandos entfernt
-- 6.4.2   20061129 RR     Restartfähigkeit unter Verwendung bereits erzeugter Tabellen
-- 7.2.0   20070511 ME     NVL bei Setzen Bindefrist eingebaut (MERGE, step201b)
-- 7.4.0   20070814 AR/ME  Nach Umstellung 9i -> 10g:
--                         Steps 13a/b und 106a/b umgestellt auf TABLE-Function, hierzu neues Package
--                         Step 201b, MERGE: VALUES bei INSERT WHEN NOT MATCHED ergaenzt
-- 8.1.0   20071219 FD     Ausgliederung aus d_ausd_vertrag.sql
-- 10.2.1  20100428  Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
-- 12.2.0  20120629 Anna Kowalczuk Fix für INM12360473 - die Tabelle sof$ta_c_bfc durch die View sof$vi_c_bfc ersetzt
----------------------------------------------------------------------

-- prompt variablendefinitionen
SELECT "variablendefinitionen" AS log_msg;

-- In BigQuery, local variables are declared inside a scripting block.
-- We use a BEGIN...END; block for the entire script execution.

BEGIN
  DECLARE v_datum STRING;

  -- Stichtag ermitteln
  SET v_datum = (
    SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', CAST(MAX(m.timecreated) AS TIMESTAMP)), '19000101')
      FROM `isbert_schema.dwtk_meldungen` m
     WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- prompt tracing und settings
  SELECT "tracing und settings" AS log_msg;

  -- prompt tabelle von vorherigem lauf loeschen
  SELECT "tabelle von vorherigem lauf loeschen" AS log_msg;

  TRUNCATE TABLE `sof$ta_vertrag_tmp`;

  -- prompt zieltabelle anlegen: vertragstabelle sof$ta_vertrag_tmp_YYYYMMDD
  SELECT "zieltabelle anlegen: vertragstabelle sof$ta_vertrag_tmp_YYYYMMDD" AS log_msg;

  INSERT INTO `sof$ta_vertrag_tmp`(
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
         c.cntrct_id                      AS VERTRAG_ID_CARMEN,
         bp.bp_id                         AS PARTNER_ID_CARMEN,
         ia.inv_definition_id             AS RECHDEF_ID_CARMEN,
         ia.account_reference             AS KUNDENKONTO,
         ia.sales_tax_freed               AS MWST_KENNZEICHEN,
         c.rv_num                         AS RAHMENVERTRAG_ID,
         ia.billcycle_id                  AS RECHNUNGSLAUF,
         c.vo_code                        AS VO_KENN,
         c.order_number                   AS ORDER_NUMBER,
         n.valid_from                     AS GEPLANT_KUEND,
         n.entry_date_of_notice           AS EINGANG_KUEND,
         c.cntrct_start_date              AS VERTRAGSBEGINN,
         CASE c.cntrct_st
           WHEN 5 THEN 'A'
           WHEN 6 THEN 'L'
           ELSE NULL
         END                              AS VERTRAGSSTATUS,
         b.sperrart_alle                  AS SPERRART,
         b.sperrgrund_alle                AS SPERRGRUND,
         b.stilllegungszeitraum_alle      AS STILLEGUNGSZEITRAUM,
         c.twinbill                       AS TWINCARD,
         ct.cds_description               AS DWH_TARIFGR_TEXT,
         bf.bindefrist                    AS BINDEFRIST,
         vvl.upgradedatum                 AS LETZTES_UPGRADE,
         p.number_time_measurement        AS VERTRAGSBINDUNG,
         p.einheit                        AS VERTRAGSBINDUNGsEinheit,
         CASE ia.inv_pay_ty_cv
           WHEN 1 THEN 'U'
           WHEN 2 THEN 'E'
           WHEN 3 THEN 'K'
           WHEN 4 THEN 'B'
           ELSE NULL
         END                              AS RECHNUNGSZAHLART,
         CASE ia.inv_media_cv
           WHEN 1 THEN 'Papier'
           WHEN 2 THEN 'ELMO'
           WHEN 3 THEN 'E-Mail'
           WHEN 4 THEN 'Fax'
           WHEN 5 THEN 'Inline/Papier'
           WHEN 6 THEN 'ELMO/Papier'
           ELSE NULL
         END                              AS RECHNUNGSMEDIUM,
         c.twin_vertrag_id                AS TWIN_VERTRAG_ID,
         CASE
           WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
                 AND (
                          b.sperrart_alle IS NULL
                      OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                     )
           THEN 'J'
           WHEN p.number_time_measurement = 12
                 AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 9
                 AND (
                          b.sperrart_alle IS NULL
                      OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                     )
           THEN 'J'
           WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
                 AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 23
                 AND (
                          b.sperrart_alle IS NULL
                      OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                     )
           THEN 'J'
           ELSE 'N'
         END                              AS upgradeberechtigt,
         ap.access_point_name             AS apn,
         vvl.upgradegrund                 AS upgradegrund,
         ct.cntrct_template_id            AS SV_Id,
         CASE
            WHEN (ct.cntrct_template_id IN (5104, 5105, 5106) OR
                 (ct.cntrct_template_id >= 5155 AND
                  ct.cntrct_template_id <= 5161)
                 )
            THEN c.contract_number
            ELSE NULL
         END                              AS VDA,
         c.cost_centre,
         c.cost_centre_user,
         c.cntrct_ty,
         rd.segment_id,
         ac.rv_action_id,
         ia.rechn_inh_konfig_text,
         c.commitment_reference_date,
         c.cntrct_validity_id
    FROM `sof$ta_cntrct_crs3`     c
    JOIN `sof$ta_bp_ref`          bp ON bp.cntrct_cp2_id = c.cntrct_id AND c.cntrct_ty <> 20
    JOIN `sof$ta_inv_acc`         ia ON ia.cntrct_id = c.cntrct_id
    LEFT JOIN `sof$ta_notice`     n  ON n.cntrct_id = c.cntrct_id
    LEFT JOIN `sof$ta_barrier_zusgf` b ON b.cntrct_id = c.cntrct_id
    JOIN `sof$ta_cntrct_templ`    ct ON ct.cntrct_template_id = c.cntrct_template_id
    LEFT JOIN `sof$ta_cntrct_valid` cv ON cv.cntrct_validity_id = c.cntrct_validity_id
    LEFT JOIN `sof$ta_period`     p  ON p.period_id = cv.first_period_id
    LEFT JOIN `sof$ta_vvl_upgrade` vvl ON vvl.vertrags_id = c.cntrct_id
    LEFT JOIN `sof$ta_apn_ve`      ap ON ap.cntrct_id = c.cntrct_id
    LEFT JOIN `dwh$vi_s_rd_segment` rd ON ia.inv_definition_id = rd.rechdef_id_carmen
    LEFT JOIN `sof$ta_action_assoc` ac ON ac.cntrct_id = c.cntrct_id
    LEFT JOIN `sof$vi_c_bfc`      bf ON bf.cntrct_id = c.cntrct_id

  UNION ALL

  SELECT
         c.cntrct_id                      AS VERTRAG_ID_CARMEN,
         bp.bp_id                         AS PARTNER_ID_CARMEN,
         ia.inv_definition_id             AS RECHDEF_ID_CARMEN,
         ia.account_reference             AS KUNDENKONTO,
         ia.sales_tax_freed               AS MWST_KENNZEICHEN,
         c.rv_num                         AS RAHMENVERTRAG_ID,
         ia.billcycle_id                  AS RECHNUNGSLAUF,
         c.vo_code                        AS VO_KENN,
         c.order_number                   AS ORDER_NUMBER,
         n.valid_from                     AS GEPLANT_KUEND,
         n.entry_date_of_notice           AS EINGANG_KUEND,
         c.cntrct_start_date              AS VERTRAGSBEGINN,
         CASE c.cntrct_st
           WHEN 5 THEN 'A'
           WHEN 6 THEN 'L'
           ELSE NULL
         END                              AS VERTRAGSSTATUS,
         b.sperrart_alle                  AS SPERRART,
         b.sperrgrund_alle                AS SPERRGRUND,
         b.stilllegungszeitraum_alle      AS STILLEGUNGSZEITRAUM,
         c.twinbill                       AS TWINCARD,
         ct.cds_description               AS DWH_TARIFGR_TEXT,
         bf.bindefrist                    AS BINDEFRIST,
         vvl.upgradedatum                 AS LETZTES_UPGRADE,
         p.number_time_measurement        AS VERTRAGSBINDUNG,
         p.einheit                        AS VERTRAGSBINDUNGsEinheit,
         CASE ia.inv_pay_ty_cv
           WHEN 1 THEN 'U'
           WHEN 2 THEN 'E'
           WHEN 3 THEN 'K'
           WHEN 4 THEN 'B'
           ELSE NULL
         END                              AS RECHNUNGSZAHLART,
         CASE ia.inv_media_cv
           WHEN 1 THEN 'Papier'
           WHEN 2 THEN 'ELMO'
           WHEN 3 THEN 'E-Mail'
           WHEN 4 THEN 'Fax'
           WHEN 5 THEN 'Inline/Papier'
           WHEN 6 THEN 'ELMO/Papier'
           ELSE NULL
         END                              AS RECHNUNGSMEDIUM,
         c.twin_vertrag_id                AS TWIN_VERTRAG_ID,
         CASE
           WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
                 AND (
                          b.sperrart_alle IS NULL
                      OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                     )
           THEN 'J'
           WHEN p.number_time_measurement = 12
                 AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 9
                 AND (
                          b.sperrart_alle IS NULL
                      OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                     )
           THEN 'J'
           WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
                 AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 23
                 AND (
                          b.sperrart_alle IS NULL
                      OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                     )
           THEN 'J'
           ELSE 'N'
         END                              AS upgradeberechtigt,
         ap.access_point_name             AS apn,
         vvl.upgradegrund                 AS upgradegrund,
         ct.cntrct_template_id            AS SV_Id,
         CASE
            WHEN (ct.cntrct_template_id IN (5104, 5105, 5106) OR
                 (ct.cntrct_template_id >= 5155 AND
                  ct.cntrct_template_id <= 5161)
                 )
            THEN c.contract_number
            ELSE NULL
         END                              AS VDA,
         c.cost_centre,
         c.cost_centre_user,
         c.cntrct_ty,
         rd.segment_id,
         ac.rv_action_id,
         ia.rechn_inh_konfig_text,
         c.commitment_reference_date,
         c.cntrct_validity_id
    FROM `sof$ta_cntrct_crs3`     c
    JOIN `sof$ta_bp_ref`          bp ON bp.cntrct_cp2_id = c.cntrct_parent AND c.cntrct_ty = 20
    JOIN `sof$ta_inv_acc`         ia ON ia.cntrct_id = c.cntrct_id
    LEFT JOIN `sof$ta_notice`     n  ON n.cntrct_id = c.cntrct_id
    LEFT JOIN `sof$ta_barrier_zusgf` b ON b.cntrct_id = c.cntrct_id
    JOIN `sof$ta_cntrct_templ`    ct ON ct.cntrct_template_id = c.cntrct_template_id
    LEFT JOIN `sof$ta_cntrct_valid` cv ON cv.cntrct_validity_id = c.cntrct_validity_id
    LEFT JOIN `sof$ta_period`     p  ON p.period_id = cv.first_period_id
    LEFT JOIN `sof$ta_vvl_upgrade` vvl ON vvl.vertrags_id = c.cntrct_id
    LEFT JOIN `sof$ta_apn_ve`      ap ON ap.cntrct_id = c.cntrct_id
    LEFT JOIN `dwh$vi_s_rd_segment` rd ON ia.inv_definition_id = rd.rechdef_id_carmen
    LEFT JOIN `sof$ta_action_assoc` ac ON ac.cntrct_id = c.cntrct_id
    LEFT JOIN `sof$vi_c_bfc`      bf ON bf.cntrct_id = c.cntrct_id;

  -- prompt Verarbeitung fehlerfrei beendet.
  SELECT "Verarbeitung fehlerfrei beendet." AS log_msg;

END;