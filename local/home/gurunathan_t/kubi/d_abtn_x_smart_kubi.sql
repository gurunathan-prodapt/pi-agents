DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

SET l_monats_id = CAST(@param_monats_id AS INT64);
SET EintragsNr = CAST(@param_eintrags_nr AS INT64);

SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

BEGIN
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  INSERT INTO dwh_ta_t_smart_kubi (
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
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp AS t_old
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh_ta_c_vertrag AS d
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id
    AND l_monats_date > CAST(d.gueltig_von AS DATE)
    AND l_monats_date <= CAST(d.gueltig_bis AS DATE)
  WHERE FORMAT_TIMESTAMP('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)
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

  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  BEGIN
    DECLARE err_text STRING;
    DECLARE err_code STRING;
    DECLARE fehler_nr INT64;

    SET err_text = @@error.message;
    SET err_code = @@error.statement_text;
    SET fehler_nr = -20001;

    INSERT INTO dwh_error_log (log_type, entry_nr, error_nr, error_msg, sql_code, log_time)
    VALUES ('F', EintragsNr, fehler_nr, err_text, err_code, CURRENT_TIMESTAMP());

    ERROR(err_text);
  END;
END;