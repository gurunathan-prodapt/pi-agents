-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh
-- This file deploys the BigQuery view and stored procedure to replace manual date logic.

-- Create or replace the view for dynamic date retrieval
CREATE OR REPLACE VIEW `isbert_aufbereitung.v_reporting_dates` AS 
SELECT
  FORMAT_DATE('%Y%m%d', CURRENT_DATE('Europe/Berlin')) AS Var_Datum_Heute,
  FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Europe/Berlin'), INTERVAL 1 DAY)) AS Var_Datum_Gestern,
  FORMAT_DATE('%Y%m', CURRENT_DATE('Europe/Berlin')) AS Var_Monat_Heute,
  FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE('Europe/Berlin'), INTERVAL 1 DAY)) AS Var_Monat_Gestern;

-- Backwards-Compatible Stored Procedure
CREATE OR REPLACE PROCEDURE `isbert_aufbereitung.get_reporting_dates`(
  OUT Var_Datum_Heute STRING,
  OUT Var_Datum_Gestern STRING,
  OUT Var_Monat_Heute STRING,
  OUT Var_Monat_Gestern STRING
)
BEGIN
  DECLARE today DATE;
  DECLARE yesterday DATE;
  
  -- Use local reporting timezone
  SET today = CURRENT_DATE('Europe/Berlin');
  SET yesterday = DATE_SUB(today, INTERVAL 1 DAY);
  
  SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', today);
  SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday);
  SET Var_Monat_Heute = FORMAT_DATE('%Y%m', today);
  SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday);
END;