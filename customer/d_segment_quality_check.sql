-- d_segment_quality_check.sql
-- Computes the percentage of customers whose current segment version was
-- freshly created (i.e. re-versioned) on the given run date, so callers can
-- flag an implausibly large weekly shift.
-- Schema: ANALYTICS_SCHEMA

SELECT 
  ROUND(
    (
      SELECT COUNT(1) 
      FROM `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
      WHERE IS_CURRENT = 1 
        AND VALID_FROM = DATE_TRUNC(PARSE_DATE('%Y-%m-%d', @run_date), DAY)
    )
    /
    NULLIF(
      (
        SELECT COUNT(1) 
        FROM `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
        WHERE IS_CURRENT = 1
      ),
      0
    )
    * 100
  ) AS CHANGED_PCT;