-- Legacy Function: DWDate_Gib_Zeitraum() from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Calculates a date range (start_date, end_date) based on a current date,
-- an offset, and a unit (Days, Months, Years).
-- Returns a STRUCT with start_date and end_date strings in the specified output format.
-- Returns NULL for invalid inputs or unsupported units.
-- NOTE: The exact logic of the original 'd_alis_datum_zeitraum.sql' was not available.
-- This UDF implements common interpretations based on the design document's description.

CREATE OR REPLACE FUNCTION dw_utils.get_date_range(offset INT64, unit STRING, output_format STRING)
RETURNS STRUCT<start_date STRING, end_date STRING>
AS (
  CASE
    WHEN offset IS NULL OR unit IS NULL OR output_format IS NULL THEN NULL
    WHEN UPPER(unit) = 'D' THEN
      STRUCT(
        FORMAT_DATE(output_format, CURRENT_DATE()) AS start_date,
        FORMAT_DATE(output_format, DATE_ADD(CURRENT_DATE(), INTERVAL offset DAY)) AS end_date
      )
    WHEN UPPER(unit) = 'M' THEN
      STRUCT(
        FORMAT_DATE(output_format, DATE_TRUNC(CURRENT_DATE(), MONTH)) AS start_date,
        FORMAT_DATE(output_format, LAST_DAY(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL offset MONTH))) AS end_date
      )
    WHEN UPPER(unit) = 'Y' THEN
      STRUCT(
        FORMAT_DATE(output_format, DATE_TRUNC(CURRENT_DATE(), YEAR)) AS start_date,
        FORMAT_DATE(output_format, LAST_DAY(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL offset YEAR), MONTH)) AS end_date
      )
    ELSE NULL -- Unsupported unit
  END
);