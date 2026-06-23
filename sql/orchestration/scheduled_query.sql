-- BigQuery Scheduled Query SQL Content
-- This SQL statement is intended to be configured as a BigQuery Scheduled Query
-- to invoke the main orchestration stored procedure.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

-- Example call for the main orchestration procedure.
-- Parameters are set to NULL to use the default logic implemented within the stored procedure:
--   p_stichtag: defaults to current system date (DDMMYYYY) if NULL or empty.
--   p_wiederanlaufWert: defaults to 0 if NULL.

CALL `your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn`(
  NULL, -- p_stichtag (STRING) - pass NULL to use default (CURRENT_DATE())
  NULL  -- p_wiederanlaufWert (INT64) - pass NULL to use default (0)
);

-- To explicitly provide parameters, replace NULLs with desired values, e.g.:
-- CALL `your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn`('28022023', 1000);
--
-- For dynamic dates in scheduled queries, consider using scripting or external orchestrators
-- like Cloud Composer if complex date logic or external dependencies are required.
-- BigQuery Scheduled Queries do not directly support dynamic SQL parameters based on current date
-- within the `CALL` statement itself, but the stored procedure handles `CURRENT_DATE()`.
-- If `p_stichtag` needs to be an arbitrary historical date, pass it explicitly.