-- Description : Aggregation Job to load data into dwh_ta_t_smart_kubi table
-- Language    : BigQuery SQL (Scripting)
------------------------------------------------------

-- Declare internal working variables
DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE l_monats_date DATE;

-- Initialize variables from query parameters
SET l_monats_id = @p_monats_id;
SET EintragsNr = @p_eintrags_nr;

-- Calculate the first day of the next month
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  -- Start Transaction context for transactional execution safety
  BEGIN TRANSACTION;

  -- Truncate target table
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  -- Primary insert statement logic
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
  WITH temp AS ( 
      -- CTE resolved from Oracle inline view subquery
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
  LEFT JOIN temp AS t_new 
         ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT JOIN temp AS t_old 
         ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT JOIN dwh_ta_c_vertrag AS d 
         ON fact.dwh_vertrag_id = d.dwh_vertrag_id
        AND l_monats_date > d.gueltig_von
        AND l_monats_date <= d.gueltig_bis
  WHERE 
      FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE)) = CAST(l_monats_id AS STRING)
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

  -- Output results log for observability (equivalent to dbms_output.put_line)
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  -- Exception block equivalents to handle transactional rollbacks
  ROLLBACK TRANSACTION;
  
  -- Handle reporting log mechanism
  DECLARE err_msg STRING;
  DECLARE err_code STRING;
  SET err_msg = @@error.message;
  SET err_code = @@error.statement_text;
  
  -- Record Exception metadata log (emulates internal db logging behavior)
  SELECT 
      'F' AS error_severity,
      EintragsNr AS eintrags_nr,
      err_msg AS error_message,
      err_code AS statement_context;

  -- Re-throw the execution exception
  RAISE USING message = err_msg;
END;