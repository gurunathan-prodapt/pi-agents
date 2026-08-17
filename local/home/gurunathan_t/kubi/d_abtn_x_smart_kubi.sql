-- Target: Load aggregated data into dwh_dataset.ta_t_smart_kubi table
-- Converted from Oracle PL/SQL script local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql

DECLARE monats_id_param INT64 DEFAULT @monats_id_param;
DECLARE eintragsnr_param INT64 DEFAULT @eintragsnr_param;

DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;
DECLARE l_monats_start_date DATE;
DECLARE l_monats_end_date DATE;
DECLARE ErrText STRING;
DECLARE ErrC STRING;

SET l_monats_id = monats_id_param;
SET EintragsNr = eintragsnr_param;

SET l_monats_start_date = PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING));
SET l_monats_date = DATE_ADD(l_monats_start_date, INTERVAL 1 MONTH);
SET l_monats_end_date = DATE_ADD(l_monats_start_date, INTERVAL 1 MONTH);

BEGIN
  TRUNCATE TABLE dwh_dataset.ta_t_smart_kubi;

  BEGIN TRANSACTION;

  INSERT INTO dwh_dataset.ta_t_smart_kubi 
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
             FROM   dwh_dataset.vi_l_map_fa_tarif T
             INNER JOIN dwh_dataset.bl_d_tarif TAR
                     ON t.tarif_id = tar.tarif_id
             WHERE  t.gueltig_bis = DATE '4712-12-31'
          )
  SELECT 
           l_monats_id                                                                          AS monats_id,
           CASE t_new.mp_geschaeftsfeld_id WHEN 2 THEN '-1' ELSE d.t_mobile_kundennummer END    AS kundennummer,
           COALESCE(t_new.tarif_id, 0)                                                          AS tarif_id,
           COALESCE(t_old.tarif_id, 0)                                                          AS tarif_id_alt,
           CASE 
              WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
              WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
              ELSE fact.vo_kenn_bearb 
           END                                                                                  AS vo_kennung,
           d.test_gp, 
           SUM(fact.zugang)                                                                     AS anzahl,
           fact.kennzahl_id 
  FROM     dwh_dataset.ta_f_d1_twvv_tn fact
  LEFT OUTER JOIN temp t_new
               ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp t_old
               ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN dwh_dataset.ta_c_vertrag d
               ON fact.dwh_vertrag_id = d.dwh_vertrag_id 
              AND l_monats_date > CAST(d.gueltig_von AS DATE) 
              AND l_monats_date <= CAST(d.gueltig_bis AS DATE) 
  WHERE    fact.gueltigkeitszeitpunkt >= l_monats_start_date
  AND      fact.gueltigkeitszeitpunkt < l_monats_end_date
  AND      fact.kennzahl_id IN ('VVLREIN', 
                                'VVLTWC2C', 
                                'MIGP2CBF') 
  GROUP BY CASE t_new.mp_geschaeftsfeld_id WHEN 2 THEN '-1' ELSE d.t_mobile_kundennummer END, 
           COALESCE(t_new.tarif_id, 0), 
           COALESCE(t_old.tarif_id, 0), 
           CASE 
              WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn 
              WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
              ELSE fact.vo_kenn_bearb 
           END, 
           d.test_gp, 
           fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count;
  
  COMMIT TRANSACTION;

  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  
  SET ErrText = @@error.message;
  SET ErrC = CAST(@@error.code AS STRING);

  SELECT FORMAT('EXCEPTION DETECTED. Error Code: %s, Message: %s', ErrC, ErrText) AS execution_error_log;
  
  RAISE USING MESSAGE = ErrText;
END;