-- ===================================================================
-- Datei:  d_ausd_v_ta_period.sql
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
-- 6.3.0   20060919 me/hs  Fr Berechnung Bindefrist mit Sperren/Stillegungen: Erweiterung um Sperrklasse 7
-- 6.4.0   20061122 me     Umstellung wegen Datenmodell-Aenderung in Carmen:
--                         Tab. CDS$TA_BARRIER_KIND_CV -> CDS$TA_BARRIER_KIND,
--                         tab. CDS$TA_DESCRIPTION     -> CDS$TA_CARE_DESCRIPTION
-- 6.4.0   20061121 RR     Bestimmung Substitutions-Variable v_datum aus
--                         Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR     berflssige ANALYZE/STATISTICS Kommandos entfernt
-- 6.4.2   20061129 RR     Restartfhigkeit unter Verwendung bereits erzeugter Tabellen
-- 7.2.0   20070511 ME     NVL bei Setzen Bindefrist eingebaut (MERGE, step201b)
-- 7.4.0   20070814 AR/ME  Nach Umstellung 9i -> 10g:
--                         Steps 13a/b und 106a/b umgestellt auf TABLE-Function, hierzu neues Package
--                         Step 201b, MERGE: VALUES bei INSERT WHEN NOT MATCHED ergaenzt
-- 8.1.0   20071219 FD     Ausgliederung aus d_ausd_vertrag.sql
-- 10.2.1  20100428  Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
----------------------------------------------------------------------

SELECT 'variablendefinitionen' AS log_msg;

DECLARE v_datum STRING;

-- Retrieve cutting date
EXECUTE IMMEDIATE """
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `""" || @gcp_project || """.""" || @bq_dataset || """.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
""" INTO v_datum;

SELECT 'tracing und settings' AS log_msg;
-- Note: Session tracing and settings (trace.sql.cfg and spool) are retired in BigQuery environment.

SELECT 'tabelle von vorherigem lauf loeschen' AS log_msg;

EXECUTE IMMEDIATE """
  TRUNCATE TABLE `""" || @gcp_project || """.""" || @bq_dataset || """.sof$ta_period`
""";

SELECT 'zieltabelle anlegen: carmen-period-tabelle' AS log_msg;

EXECUTE IMMEDIATE """
  INSERT INTO `""" || @gcp_project || """.""" || @bq_dataset || """.sof$ta_period` (
    period_id,
    number_time_measurement,
    time_meas_cv,
    einheit,
    bfc_age
  )
  SELECT
    p.period_id,
    p.number_time_measurement,
    p.time_meas_cv,
    d.description,
    p.insert_at
  FROM
    `""" || @gcp_project || """.""" || @carmen_bq_dataset || """.cds$ta_period` p
  INNER JOIN
    `""" || @gcp_project || """.""" || @carmen_bq_dataset || """.CDS$TA_TIME_MEAS_CV` tm 
    ON tm.time_meas_cv = p.time_meas_cv
  INNER JOIN
    `""" || @gcp_project || """.""" || @carmen_bq_dataset || """.cds$ta_description` d 
    ON tm.DESCRIPTION_ID = d.DESCRIPTION_ID
  WHERE
    CAST(p.insert_at AS DATE) <= PARSE_DATE('%Y%m%d', @v_datum)
    AND (
      p.modified_at IS NULL
      OR CAST(p.modified_at AS DATE) > PARSE_DATE('%Y%m%d', @v_datum)
    )
""" USING v_datum AS v_datum;

SELECT 'Verarbeitung fehlerfrei beendet.' AS log_msg;