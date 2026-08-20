-- Declare all variables at the beginning of the BigQuery Scripting Block
DECLARE v_anzahl_ds INT64 DEFAULT 0;  -- converted from PLS_INTEGER
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING;  -- converted from VARCHAR2(300)
DECLARE l_monats_date DATE;  -- converted from Oracle DATE

-- Assign parameter values (to be substituted with migration session variables or procedure parameters)
SET l_monats_id = @l_monats_id;
SET EintragsNr = @EintragsNr;

-- Calculate month offset date using safe BigQuery functions
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH); 
-- converted from ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1)

BEGIN
  -- Replaces dynamic runstatement truncate call
  TRUNCATE TABLE `dwh$ta_t_smart_kubi`;

  -- Insert Logic utilizing explicit CTE definition
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
     FROM   `dwh$vi_l_map_fa_tarif` AS t
     INNER JOIN `bl_d_tarif` AS tar
       ON t.tarif_id = tar.tarif_id
     WHERE  CAST(t.gueltig_bis AS DATE) = DATE '4712-12-31'  
     -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
  )
  SELECT 
         l_monats_id AS monats_id,
         CASE 
           WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
           ELSE d.t_mobile_kundennummer 
         END AS kundennummer,  
         -- converted from Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)
         
         COALESCE(t_new.tarif_id, 0) AS tarif_id,  
         -- converted from Nvl(t_new.tarif_id,0)
         
         COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,  
         -- converted from Nvl(t_old.tarif_id,0)
         
         CASE 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) IS NULL THEN fact.vo_kenn 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) = '#' THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END AS vo_kennung,  
         -- converted from Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)
         
         d.test_gp, 
         SUM(fact.zugang) AS anzahl, 
         fact.kennzahl_id 
  FROM `dwh$ta_f_d1_twvv_tn` AS fact  -- stripped partition name partition(dwh$ta_f_d1_twvv_tn_&1)
  LEFT JOIN temp AS t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id  -- converted from (+) join
  LEFT JOIN temp AS t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id  -- converted from (+) join
  LEFT JOIN `dwh$ta_c_vertrag` AS d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id  -- converted from (+) join
    AND l_monats_date > CAST(d.gueltig_von AS DATE)  -- converted from (+) join & DATE type safety
    AND l_monats_date <= CAST(d.gueltig_bis AS DATE)  -- converted from (+) join & DATE type safety
  WHERE FORMAT_DATE('%Y%m', DATE(fact.gueltigkeitszeitpunkt)) = CAST(l_monats_id AS STRING)  
  -- converted from to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)
    AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF') 
  GROUP BY 
         CASE 
           WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
           ELSE d.t_mobile_kundennummer 
         END, 
         COALESCE(t_new.tarif_id, 0), 
         COALESCE(t_old.tarif_id, 0), 
         CASE 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) IS NULL THEN fact.vo_kenn 
           WHEN LTRIM(RTRIM(fact.vo_kenn_bearb)) = '#' THEN fact.vo_kenn 
           ELSE fact.vo_kenn_bearb 
         END, 
         d.test_gp, 
         fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count;  -- converted from SQL%ROWCOUNT
  
  -- Informational log outputs replacing dbms_output.put_line
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  -- unbekannte bzw. nicht erwartete Exception koennen auch
  -- behandelt werden. Die Fehlernummer ist immer die gleiche, nur
  -- der Zusatzfehlertext kann vorher ermittelt werden.
  DECLARE error_message STRING;
  SET error_message = @@error.message;
  
  -- Record failure parameters / details for debugging
  SELECT 
    'F' AS severity,
    EintragsNr AS entry_no,
    error_message AS oracle_error_text;
    
  RAISE USING message = error_message;
END;