-- Description: Aggregation Job to load data into DWH_TA_T_SMART_KUBI table
-- Erstellt  : Ankita Suvarna
-- Datum     : 18.09.2015
-- Language  : PL/SQL -> BigQuery SQL Scripting
-- Version   : 16.1.0.
------------------------------------------------------

DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64;
DECLARE EintragsNr  INT64;
DECLARE lv_str      STRING;
DECLARE l_monats_date DATE;

-- Emulated Parameter Initialization
SET l_monats_id = CAST(@p_monats_id AS INT64);
SET EintragsNr  = CAST(@p_eintragsnr AS INT64);

-- Calculate start of next month based on l_monats_id (format: YYYYMM)
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  -- Truncate target table (corresponds to dwpa_util_skript.runstatement)
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  -- Insert query with ANSI joins and CTEs
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
       FROM   dwh_vi_l_map_fa_tarif T
       INNER JOIN bl_d_tarif TAR
          ON t.tarif_id = tar.tarif_id
       WHERE  t.gueltig_bis = CAST('4712-12-31' AS DATETIME)
  )
  SELECT 
         l_monats_id                                    AS monats_id,
         CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END AS kundennummer,
         COALESCE(t_new.tarif_id, 0)                                                      AS tarif_id,
         COALESCE(t_old.tarif_id, 0)                                                      AS tarif_id_alt,
         CASE 
           WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' OR TRIM(fact.vo_kenn_bearb) = '#'
             THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END                                                                              AS vo_kennung,
         d.test_gp, 
         SUM(fact.zugang)                                                                 AS anzahl,
         fact.kennzahl_id 
  FROM     dwh_ta_f_d1_twvv_tn fact
  LEFT OUTER JOIN temp t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh_ta_c_vertrag d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
    AND CAST(l_monats_date AS DATETIME) > d.gueltig_von 
    AND CAST(l_monats_date AS DATETIME) <= d.gueltig_bis 
  WHERE    FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING) 
  AND      fact.kennzahl_id IN ('VVLREIN', 
                                'VVLTWC2C', 
                                'MIGP2CBF') 
  GROUP BY 
         CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END, 
         COALESCE(t_new.tarif_id, 0), 
         COALESCE(t_old.tarif_id, 0), 
         CASE 
           WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' OR TRIM(fact.vo_kenn_bearb) = '#'
             THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END, 
         d.test_gp, 
         fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count;

  SELECT CAST(v_anzahl_ds AS STRING) || ' rows inserted in DWH$TA_T_SMART_KUBI' AS log_message;

EXCEPTION WHEN ERROR THEN
  -- unbekannte bzw. nicht erwartete Exception koennen auch
  -- behandelt werden. Die Fehlernummer ist immer die gleiche, nur
  -- der Zusatzfehlertext kann vorher ermittelt werden.
  DECLARE ErrText  STRING;
  DECLARE ErrC     STRING;
  DECLARE FehlerNr INT64 DEFAULT -20001; -- Map to legacy dwpa_globals.k_alis_err_unknown equivalent if needed
  
  SET ErrText = @@error.message;
  SET ErrC = @@error.status;
  
  -- Simulate dwpa_meldung.fehler error logging call
  SELECT FORMAT('dwpa_meldung.fehler: F, %d, %d, %s, %s', EintragsNr, FehlerNr, ErrText, ErrC) AS error_log;
  
  RAISE USING message = ErrText;
END;