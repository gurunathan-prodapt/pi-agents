-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- This file contains BigQuery SQL for date utility functions.

-- 1. Create BigQuery Dataset (Schema)
CREATE SCHEMA IF NOT EXISTS dataset;

-- 2. Generate BigQuery UDFs

-- UDF: LetzterTagDesMonat
-- Legacy: Shell script function performing arithmetic and array lookup to determine if a given YYYYMMDD date is the last day of its month, including leap year logic.
-- BigQuery Target: Checks if the parsed date is the last day of its month.
CREATE OR REPLACE FUNCTION dataset.LetzterTagDesMonat(datum STRING)
RETURNS BOOL
AS (
  PARSE_DATE('%Y%m%d', datum) = LAST_DAY(PARSE_DATE('%Y%m%d', datum))
);

-- UDF: TageimMonat
-- Legacy: Shell script function performing arithmetic and array lookup to calculate the number of days in a given month and year, including leap year logic.
-- BigQuery Target: Extracts the day component of the last day of the month for the given year and month.
CREATE OR REPLACE FUNCTION dataset.TageimMonat(jahr INT64, monat INT64)
RETURNS INT64
AS (
  EXTRACT(DAY FROM LAST_DAY(DATE(jahr, monat, 1)))
);

-- UDF: AddiereDatum
-- Legacy: Shell script function manually adding days to a YYYYMMDD date, handling month and year rollovers through loops and calls to TageimMonat.
-- BigQuery Target: Adds a specified number of days to a given date and formats the result as YYYYMMDD.
CREATE OR REPLACE FUNCTION dataset.AddiereDatum(datum STRING, tage INT64)
RETURNS STRING
AS (
  FORMAT_DATE(
    '%Y%m%d',
    DATE_ADD(PARSE_DATE('%Y%m%d', datum), INTERVAL tage DAY)
  )
);

-- 3. Generate BigQuery Stored Procedures

-- Stored Procedure: DWDate_Vormonat
-- Legacy: Calls sqlplus to execute d_alis_vormonat.sql, which uses LAST_DAY(ADD_MONTHS(sysdate,-1)) to get the last day of the previous month.
-- BigQuery Target: Calculates the last day of the previous month from the current date and formats it.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Vormonat(
  IN v_dateformat STRING,
  OUT v_result STRING
)
BEGIN
  SET v_result = FORMAT_DATE(
    v_dateformat,
    LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
  );
END;

-- Stored Procedure: DWDate_Datum_Check
-- Legacy: Uses an inline sqlplus call with SELECT to_date('$wert','$format') FROM dual; to validate a date.
-- BigQuery Target: Attempts to parse a date string with a given format. BigQuery will raise an error if parsing fails.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_Check(
  IN wert STRING,
  IN format STRING
)
BEGIN
  DECLARE parsed_date DATE;
  -- If PARSE_DATE fails due to invalid format, BigQuery raises an error automatically.
  SET parsed_date = PARSE_DATE(format, wert);
END;

-- Stored Procedure: DWDate_Datum_LE
-- Legacy: Uses an inline sqlplus call with a PL/SQL block to compare two dates (datum1 <= datum2).
-- BigQuery Target: Compares two dates and raises an error if the first date is greater than the second.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_LE(
  IN datum1 STRING,
  IN datum2 STRING
)
BEGIN
  DECLARE d1 DATE;
  DECLARE d2 DATE;

  SET d1 = PARSE_DATE('%Y%m%d', datum1);
  SET d2 = PARSE_DATE('%Y%m%d', datum2);

  IF d1 > d2 THEN
    RAISE USING MESSAGE = CONCAT('Datum ', datum1, ' ist groesser als ', datum2);
  END IF;
END;

-- Stored Procedure: DWDate_Gib_Zeitraum
-- Legacy: Calls sqlplus to execute d_alis_datum_zeitraum.sql, which calculates a date range using SYSDATE, offset, and step.
-- BigQuery Target: Calculates a start and end date based on an offset and step ('D', 'M', 'Y') from the current date.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Gib_Zeitraum(
  IN offset_value INT64,
  IN stufe STRING,
  IN format STRING,
  OUT start_date STRING,
  OUT end_date STRING
)
BEGIN
  DECLARE base_start DATE;
  DECLARE base_end DATE;
  DECLARE calc_start DATE;
  DECLARE calc_end DATE;

  IF stufe = 'D' THEN
    SET base_start = CURRENT_DATE();
    SET base_end = CURRENT_DATE();
    SET calc_start = DATE_ADD(CURRENT_DATE(), INTERVAL offset_value DAY);
    SET calc_end = DATE_ADD(CURRENT_DATE(), INTERVAL offset_value DAY);

  ELSEIF stufe = 'M' THEN
    SET base_start = DATE_TRUNC(CURRENT_DATE(), MONTH);
    SET base_end = LAST_DAY(CURRENT_DATE());
    SET calc_start = DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL offset_value MONTH), MONTH);
    SET calc_end = LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL offset_value MONTH));

  ELSEIF stufe = 'Y' THEN
    SET base_start = DATE_TRUNC(CURRENT_DATE(), YEAR);
    SET base_end = DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 12 MONTH), YEAR), INTERVAL 1 DAY);
    SET calc_start = DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL offset_value * 12 MONTH), YEAR);
    SET calc_end = DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL (12 + offset_value * 12) MONTH), YEAR), INTERVAL 1 DAY);
  END IF;

  SET start_date = FORMAT_DATE(format, LEAST(base_start, calc_start));
  SET end_date = FORMAT_DATE(format, GREATEST(base_end, calc_end));
END;