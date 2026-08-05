-- d_gl_close_audit.sql
-- Writes the immutable close-audit record once aggregation has succeeded.
-- Params: @period_name = period name, @fiscal_year = fiscal year
-- Schema: analytics_schema

DECLARE period_name STRING DEFAULT @period_name;
DECLARE fiscal_year STRING DEFAULT @fiscal_year;

BEGIN
  BEGIN TRANSACTION;

  -- 1. Write the immutable close-audit record once aggregation has succeeded.
  INSERT INTO `analytics_schema.gl_close_audit`
      (PERIOD_NAME, FISCAL_YEAR, CLOSED_BY, CLOSED_AT)
  VALUES
      (
        period_name,
        fiscal_year,
        SESSION_USER(),
        CURRENT_TIMESTAMP()
      );

  -- 2. Update the system status tracker for the closed period.
  UPDATE `analytics_schema.gl_period_status`
  SET    CLOSE_STATUS = 'CLOSED',
         CLOSED_AT    = CURRENT_TIMESTAMP()
  WHERE  PERIOD_NAME  = period_name;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Rollback transaction in the event of an unexpected runtime failure
  ROLLBACK TRANSACTION;
  -- Re-raise error to alerting engine/orchestrator
  RAISE;
END;