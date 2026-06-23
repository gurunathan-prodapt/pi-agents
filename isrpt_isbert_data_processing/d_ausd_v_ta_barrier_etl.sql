-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_barrier.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh

CREATE OR REPLACE PROCEDURE `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`()
BEGIN
  -- Stichtag ermitteln
  DECLARE v_datum STRING;
  SET v_datum = (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
                   FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m
                  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');

  -- Tabelle von vorherigem lauf leeren
  TRUNCATE TABLE `isrpt_isbert_data_processing.sof_ta_barrier`;

  -- Zieltabelle anlegen: lokale Kopie der Carmen-Sperr-Tabelle
  INSERT INTO `isrpt_isbert_data_processing.sof_ta_barrier`(
          cntrct_id,
          barrier_kind_id,
          sperrart,
          barrier_init_cv,
          barrier_reason_cv,
          sperr_beginn,
          sperr_ende,
          sperrgrund,
          bfc_age,
          ist_stillegung)
  SELECT
          b.cntrct_id,
          bc.barrier_kind_id,
          dk.cds_description AS SPERRART,
          bc.barrier_init_cv,
          bc.barrier_reason_cv,
          COALESCE(b.net_barr_on_date,  b.valid_from) AS SPERR_BEGINN,
          COALESCE(b.net_barr_off_date, b.valid_to) AS SPERR_ENDE,
          CASE bc.barrier_reason_cv
              WHEN 1 THEN 'Kartenverlust'
              WHEN 2 THEN 'Kundenwunsch'
              WHEN 3 THEN 'Betreiberinterne Sperre'
              WHEN 4 THEN 'Betreiberinterne Sperre'
              WHEN 7 THEN 'Betreiberinterne Sperre'
              WHEN 9 THEN 'wegen Kartenlieferung (LZE)'
              WHEN 10 THEN 'vorzeitige Aktivierung /Stillgelegt'
              WHEN 11 THEN 'Serviceproviderwunsch'
              WHEN 13 THEN 'Betreiberinterne Sperre'
              WHEN 14 THEN 'Betreiberinterne Sperre'
              WHEN 15 THEN 'Sterbefall/Stillgelegt'
              WHEN 16 THEN 'Telefonische Aktivierung'
              WHEN 17 THEN 'Betreiberinterne Sperre'
              WHEN 18 THEN 'Betreiberinterne Sperre'
              WHEN 19 THEN 'Stillgelegt'
              WHEN 20 THEN 'Verspaetete Endgeraetelieferung'
              WHEN 21 THEN 'Betreiberinterne Sperre'
              WHEN 22 THEN 'Kartenrcksendung'
              WHEN 23 THEN 'Betreiberinterne Sperre'
              WHEN 24 THEN 'Betreiberinterne Sperre'
              WHEN 25 THEN 'Betreiberinterne Sperre'
              WHEN 26 THEN 'Betreiberinterne Sperre'
              WHEN 27 THEN 'Aufhebung/Auslauf des Vertrages'
              WHEN 28 THEN 'Vertragsbernahme/ neuer Vertrag'
              WHEN 29 THEN 'Beauftragungsprozess'
              WHEN 30 THEN 'Endgeraet nicht zuordenbar'
              WHEN 31 THEN 'RV-Wunsch'
              WHEN 32 THEN 'Betreiberinterne Sperre'
              WHEN NULL THEN '' -- Special case for NULL as per Oracle DECODE behavior
              ELSE 'Betreiberinterne Sperre'
          END AS SPERRGRUND,
          GREATEST(b.insert_at, bc.insert_at) AS bfc_age,
          CASE WHEN bc.closure = 1 THEN 1 ELSE 0 END AS IST_STILLEGUNG
  FROM
          `isrpt_isbert_data_processing.cds_ta_barrier` AS b
  INNER JOIN `isrpt_isbert_data_processing.cds_ta_barrier_class` AS bc
          ON b.barrier_class_id = bc.barrier_class_id
  INNER JOIN `isrpt_isbert_data_processing.cds_ta_barrier_kind` AS bk
          ON bk.barrier_kind_id = bc.barrier_kind_id
  INNER JOIN `isrpt_isbert_data_processing.cds_ta_care_description` AS dk
          ON dk.cds_description_id = bk.cds_description_id
  WHERE
          b.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND     (b.modified_at IS NULL OR b.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND     b.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND     (b.valid_to IS NULL OR b.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND     b.is_production = 1
  AND     bk.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND     (bk.modified_at IS NULL OR bk.modified_at > PARSE_DATE('%Y%m%d', v_datum));

END;