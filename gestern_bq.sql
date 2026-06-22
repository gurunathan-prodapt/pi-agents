-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

DECLARE today_date DATE DEFAULT CURRENT_DATE();
DECLARE yesterday_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

DECLARE Var_Datum_Heute STRING DEFAULT FORMAT_DATE('%Y%m%d', today_date);
DECLARE Var_Datum_Gestern STRING DEFAULT FORMAT_DATE('%Y%m%d', yesterday_date);
DECLARE Var_Monat_Heute STRING DEFAULT FORMAT_DATE('%Y%m', today_date);
DECLARE Var_Monat_Gestern STRING DEFAULT FORMAT_DATE('%Y%m', yesterday_date);

SELECT
  Var_Datum_Heute AS today_ymd,
  Var_Datum_Gestern AS yesterday_ymd,
  Var_Monat_Heute AS today_ym,
  Var_Monat_Gestern AS yesterday_ym;