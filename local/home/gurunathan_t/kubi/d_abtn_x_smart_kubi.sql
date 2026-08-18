DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;
DECLARE l_monats_date DATE;

BEGIN
  SET l_monats_id = @monats_id;
  SET EintragsNr = @eintragsnr;
  SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

  TRUNCATE TABLE `dwh$ta_t_smart_kubi`;

  INSERT INTO `dwh$ta_t_smart_kubi` 
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
             FROM   `dwh$vi_l_map_fa_tarif` t
             JOIN   `bl_d_tarif` tar
               ON   t.tarif_id = tar.tarif_id
             WHERE  t.gueltig_bis = DATE '4712-12-31'
         )
  SELECT 
           l_monats_id                                    AS monats_id,
           CASE 
             WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
             ELSE d.t_mobile_kundennummer 
           END                                            AS kundennummer,
           COALESCE(t_new.tarif_id, 0)                     AS tarif_id,
           COALESCE(t_old.tarif_id, 0)                     AS tarif_id_alt,
           CASE 
             WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '' THEN fact.vo_kenn 
             WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn 
             ELSE fact.vo_kenn_bearb 
           END                                            AS vo_kennung,
           d.test_gp, 
           sum(fact.zugang)                               AS anzahl, 
           fact.kennzahl_id 
  FROM     `dwh$ta_f_d1_twvv_tn` fact
  LEFT OUTER JOIN temp t_new 
               ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
  LEFT OUTER JOIN temp t_old 
               ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
  LEFT OUTER JOIN `dwh$ta_c_vertrag` d 
               ON fact.dwh_vertrag_id = d.dwh_vertrag_id
              AND l_monats_date > d.gueltig_von
              AND l_monats_date <= d.gueltig_bis
  WHERE    FORMAT_DATE('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)
  AND      fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  AND      _PARTITIONDATE = PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)) 
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
  SELECT FORMAT('Error: F, EintragsNr: %d, FehlerNr: %d, Message: %s, Statement: %s', EintragsNr, -20001, @@error.message, @@error.statement_text) AS error_log;
  ERROR(FORMAT('Error execution failed: %s - %s', @@error.statement_text, @@error.message));
END;