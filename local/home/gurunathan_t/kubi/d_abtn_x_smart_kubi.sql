-- Description: Aggregation Job to load data into DWH$TA_T_SMART_KUBI table
-- Language: BigQuery Standard SQL / Scripting

DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

-- Parse script run-time arguments
SET l_monats_id = @monats_id;
SET EintragsNr = @eintrags_nr;

-- Process Month Boundary Date
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  -- Start transactional execution
  BEGIN TRANSACTION;

  -- Truncate Target Table
  TRUNCATE TABLE dwh$ta_t_smart_kubi;

  -- Insert Logic
  INSERT INTO dwh$ta_t_smart_kubi 
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
      FROM dwh$vi_l_map_fa_tarif AS t
      INNER JOIN bl_d_tarif AS tar 
        ON t.tarif_id = tar.tarif_id
      WHERE CAST(t.gueltig_bis AS DATE) = DATE '4712-12-31'
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
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END AS vo_kennung,
      d.test_gp, 
      SUM(fact.zugang) AS anzahl, 
      fact.kennzahl_id 
  FROM dwh$ta_f_d1_twvv_tn AS fact
  LEFT OUTER JOIN temp AS t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp AS t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh$ta_c_vertrag AS d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
    AND l_monats_date > CAST(d.gueltig_von AS DATE)
    AND l_monats_date <= CAST(d.gueltig_bis AS DATE)
  WHERE FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE)) = CAST(l_monats_id AS STRING)
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  GROUP BY 
      CASE 
        WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
        ELSE d.t_mobile_kundennummer 
      END, 
      COALESCE(t_new.tarif_id, 0), 
      COALESCE(t_old.tarif_id, 0), 
      CASE 
        WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
        ELSE fact.vo_kenn_bearb
      END, 
      d.test_gp, 
      fact.kennzahl_id;

  -- Save Row Count
  SET v_anzahl_ds = @@row_count;

  COMMIT TRANSACTION;

  -- Log process output
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS execution_log;

EXCEPTION WHEN ERROR THEN
  -- unbekannte bzw. nicht erwartete Exception koennen auch
  -- behandelt werden. Die Fehlernummer ist immer die gleiche, nur
  -- der Zusatzfehlertext kann vorher ermittelt werden.
  ROLLBACK TRANSACTION;
  BEGIN
    DECLARE ErrText STRING;
    DECLARE ErrC INT64;
    DECLARE FehlerNr INT64;
    
    SET ErrText = @@error.message;
    SET ErrC = -1; -- Mock SQLCODE placeholder
    SET FehlerNr = -20001; -- Representing dwpa_globals.k_alis_err_unknown;

    -- Call custom enterprise logging routing
    CALL dwpa_meldung_fehler('F', EintragsNr, FehlerNr, ErrText, CAST(ErrC AS STRING));
    
    -- Raise application-specific error
    ERROR(ErrText);
  END;
END;