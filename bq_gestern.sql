-- This script replicates the functionality of the legacy vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh script
-- to calculate and format today's and yesterday's dates.

-- Declare variables to hold the formatted date strings
DECLARE Var_Datum_Heute STRING;
DECLARE Var_Monat_Heute STRING;
DECLARE Var_Datum_Gestern STRING;
DECLARE Var_Monat_Gestern STRING;

-- Get the current date (in UTC or specified timezone)
DECLARE today_date DATE DEFAULT CURRENT_DATE(); -- Use CURRENT_DATE('Your/Timezone') if needed
-- Calculate yesterday's date
DECLARE yesterday_date DATE DEFAULT DATE_SUB(today_date, INTERVAL 1 DAY);

-- Format today's date into YYYYMMDD and YYYYMM
SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', today_date);
SET Var_Monat_Heute = FORMAT_DATE('%Y%m', today_date);

-- Format yesterday's date into YYYYMMDD and YYYYMM
SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday_date);
SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday_date);

-- Output the results in the same order as the original shell script
SELECT
  Var_Datum_Heute AS today_date_yyyymmdd,
  Var_Datum_Gestern AS yesterday_date_yyyymmdd,
  Var_Monat_Heute AS today_month_yyyymm,
  Var_Monat_Gestern AS yesterday_month_yyyymm;