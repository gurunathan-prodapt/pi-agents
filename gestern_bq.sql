-- Migrates vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

DECLARE Var_Datum_Heute STRING;
DECLARE Var_Monat_Heute STRING;
DECLARE Var_Datum_Gestern STRING;
DECLARE Var_Monat_Gestern STRING;

DECLARE current_date_value DATE DEFAULT CURRENT_DATE();
DECLARE yesterday_date_value DATE DEFAULT DATE_SUB(current_date_value, INTERVAL 1 DAY);

SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', current_date_value);
SET Var_Monat_Heute = FORMAT_DATE('%Y%m', current_date_value);

SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday_date_value);
SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday_date_value);

SELECT
  Var_Datum_Heute AS Var_Datum_Heute,
  Var_Datum_Gestern AS Var_Datum_Gestern,
  Var_Monat_Heute AS Var_Monat_Heute,
  Var_Monat_Gestern AS Var_Monat_Gestern;