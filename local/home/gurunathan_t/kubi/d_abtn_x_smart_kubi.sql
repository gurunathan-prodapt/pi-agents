-- Description : Aggregation Job to load data into dwh_ta_t_smart_kubi table
-- Erstellt    : Ankita Suvarna
-- Datum       : 18.09.2015
-- Language    : BigQuery Standard SQL Procedural Scripting
-- Version     : 16.1.0 (Migrated)
------------------------------------------------------

DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

-- Bind variables passed from the orchestrator
SET l_monats_id = @p_monats_id;
SET EintragsNr = @p_eintrags_nr;
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  -- Dynamic Truncate mapped to standard, direct DML execution
  SET lv_str = 'Truncate table dwh_ta_t_smart_kubi'; 
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  -- Wrap DML operations inside transaction blocks to replicate structural isolation
  BEGIN TRANSACTION;

  -- Primary INSERT statement
  INSERT INTO dwh_ta_t_smart_kubi
  ( 
      monats_id, 
      kundennummer, 
      tarif_id, 
      tarif_id_alt, 
      vo_kennung, 
      test_gp, 
      anzahl, 
      kennzahl_id 
  ) 
  WITH temp AS 
  ( 
      SELECT
          t.tarif_id,
          t.dwh_tarif_id,
          t.gueltig_von,
          t.gueltig_bis,
          tar.mp_geschaeftsfeld_id
      FROM dwh_vi_l_map_fa_tarif AS t
      INNER JOIN bl_d_tarif AS tar
         ON t.tarif_id = tar.tarif_id
      WHERE t.gueltig_bis = DATE '4712-12-31'
  )
  SELECT 
      l_monats_id AS monats_id,
      CASE 
          WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
          ELSE d.t_mobile_kundennummer 
      END AS kundennummer,
      COALESCE(t_new.tarif_id, 0) AS tarif_id,
      COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,
      CASE 
          WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn 
          WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
          ELSE fact.vo_kenn_bearb 
      END AS vo_kennung,
      d.test_gp, 
      SUM(fact.zugang) AS anzahl, 
      fact.kennzahl_id 
  FROM dwh_ta_f_d1_twvv_tn AS fact
  LEFT OUTER JOIN temp AS t_new
    ON fact.dwh_tarif_id_neu  = t_new.dwh_tarif_id 
  LEFT OUTER JOIN temp AS t_old
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id 
  LEFT OUTER JOIN dwh_ta_c_vertrag AS d
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
   AND l_monats_date > d.gueltig_von
   AND l_monats_date <= d.gueltig_bis
  WHERE FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  GROUP BY 
      CASE 
          WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
          ELSE d.t_mobile_kundennummer 
      END, 
      COALESCE(t_new.tarif_id, 0), 
      COALESCE(t_old.tarif_id, 0), 
      CASE 
          WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn 
          WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
          ELSE fact.vo_kenn_bearb 
      END, 
      d.test_gp, 
      fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count;
  COMMIT TRANSACTION;
    
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds);

EXCEPTION WHEN ERROR THEN
  -- unbekannte bzw. nicht erwartete Exception koennen auch
  -- behandelt werden. Die Fehlernummer ist immer die gleiche, nur
  -- der Zusatzfehlertext kann vorher ermittelt werden.
  ROLLBACK TRANSACTION;
  
  DECLARE ErrText STRING;
  DECLARE ErrC STRING;
  SET ErrText = @@error.message;
  SET ErrC = @@error.code;
  
  -- TODO: Manual intervention needed to replace Oracle package-level logging (dwpa_meldung.fehler).
  -- Insert metadata records to tracking tables or throw to standard orchestration logs:
  -- INSERT INTO control_table_log (eintrags_nr, error_code, error_msg, log_time) VALUES (EintragsNr, ErrC, ErrText, CURRENT_TIMESTAMP());
  
  ERROR(FORMAT('Error code: %s. Message: %s', ErrC, ErrText));
END;