DECLARE l_monats_id INT64 DEFAULT @l_monats_id;
DECLARE EintragsNr INT64 DEFAULT @EintragsNr;
DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_date DATETIME;
DECLARE lv_str STRING;

-- Exception block variables
DECLARE err_text STRING;
DECLARE err_code STRING;
DECLARE fehler_nr INT64;

-- Calculate target reporting month date offset
-- converted from ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)
SET l_monats_date = DATETIME_ADD(PARSE_DATETIME('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

-- Assign truncate statement variable
SET lv_str = 'Truncate table DWH$TA_T_SMART_KUBI';

BEGIN
  -- Execute Truncate Target Table
  -- converted from dwpa_util_skript.runstatement(eintragsnr, 'Truncate table DWH$TA_T_SMART_KUBI')
  TRUNCATE TABLE `dwh.dwh$ta_t_smart_kubi`;

  -- Load Target Table
  INSERT INTO `dwh.dwh$ta_t_smart_kubi`
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
    -- CTE matching business logic 
    SELECT
      t.tarif_id,
      t.dwh_tarif_id,
      t.gueltig_von,
      t.gueltig_bis,
      tar.mp_geschaeftsfeld_id
    FROM `dwh.dwh$vi_l_map_fa_tarif` AS t
    INNER JOIN `dwh.bl_d_tarif` AS tar
      ON t.tarif_id = tar.tarif_id
    WHERE t.gueltig_bis = DATETIME '4712-12-31 00:00:00'  -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT
    l_monats_id AS monats_id,
    -- converted from Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
    CASE 
      WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
      ELSE d.t_mobile_kundennummer 
    END AS kundennummer,
    -- converted from Nvl(t_new.tarif_id,0)
    COALESCE(t_new.tarif_id, 0) AS tarif_id,
    -- converted from Nvl(t_old.tarif_id,0)
    COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,
    -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)
    CASE 
      WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
      WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
      ELSE fact.vo_kenn_bearb 
    END AS vo_kennung,
    d.test_gp,
    SUM(fact.zugang) AS anzahl,
    fact.kennzahl_id
  FROM `dwh.dwh$ta_f_d1_twvv_tn` AS fact  -- stripped partition-specific suffix dwh$ta_f_d1_twvv_tn_&1
  LEFT JOIN temp AS t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id  -- converted from (+) outer join logic
  LEFT JOIN temp AS t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id  -- converted from (+) outer join logic
  LEFT JOIN `dwh.dwh$ta_c_vertrag` AS d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id      -- converted from (+) outer join logic
    AND l_monats_date > d.gueltig_von
    AND l_monats_date <= d.gueltig_bis
  WHERE FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)  -- converted from to_char(gueltigkeitszeitpunkt, 'yyyymm')
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF')
  GROUP BY 
    CASE 
      WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
      ELSE d.t_mobile_kundennummer 
    END,
    COALESCE(t_new.tarif_id, 0),
    COALESCE(t_old.tarif_id, 0),
    CASE 
      WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
      WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
      ELSE fact.vo_kenn_bearb 
    END,
    d.test_gp,
    fact.kennzahl_id;

  -- Capture execution rowcount
  SET v_anzahl_ds = @@row_count;  -- converted from SQL%ROWCOUNT

  -- Logging Execution Progress
  -- converted from dbms_output.put_line(...)
  SELECT CONCAT(CAST(v_anzahl_ds AS STRING), ' rows inserted in DWH$TA_T_SMART_KUBI') AS log_output;

EXCEPTION WHEN ERROR THEN
  -- Exception Block Handler
  -- Note: BigQuery does not support standard transaction rollback outside active dynamic multi-statement transactions.
  -- Error context details are captured using script environment status variables.
  SET err_text = @@error.message;
  SET err_code = @@error.statement_text;
  SET fehler_nr = -20001; -- converted from dwpa_globals.k_alis_err_unknown;

  -- Emulate custom logging package `dwpa_meldung.fehler` using selection logging
  SELECT 
    'F' AS severity,
    EintragsNr AS log_id,
    fehler_nr AS error_code,
    err_text AS error_desc,
    err_code AS failed_statement;

  ERROR(CONCAT('Execution failed with message: ', err_text));
END;