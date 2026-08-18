-- d_gl_close_audit.sql
-- Writes the immutable close-audit record once aggregation has succeeded.
-- Params: @period_name = period name, @fiscal_year = fiscal year
-- Global Params: @gcp_project = GCP Project ID, @bq_dataset = BigQuery Dataset Name

DECLARE var_period_name STRING DEFAULT @period_name;
DECLARE var_fiscal_year STRING DEFAULT @fiscal_year;
DECLARE var_project_id STRING DEFAULT @gcp_project;
DECLARE var_dataset_name STRING DEFAULT @bq_dataset;

BEGIN
  BEGIN TRANSACTION;

  -- 1. Insert execution audit record
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.GL_CLOSE_AUDIT`
        (PERIOD_NAME, FISCAL_YEAR, CLOSED_BY, CLOSED_AT)
    VALUES
        (?, ?, SESSION_USER(), CURRENT_TIMESTAMP())
  """, var_project_id, var_dataset_name) USING var_period_name, var_fiscal_year;

  -- 2. Update status of the respective period status table record
  EXECUTE IMMEDIATE FORMAT("""
    UPDATE `%s.%s.GL_PERIOD_STATUS`
    SET    CLOSE_STATUS = 'CLOSED',
           CLOSED_AT    = CURRENT_TIMESTAMP()
    WHERE  PERIOD_NAME  = ?
  """, var_project_id, var_dataset_name) USING var_period_name;

  -- Commit changes atomically if no exception occurs
  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Fallback logic to roll back updates if any error during transaction blocks
  ROLLBACK TRANSACTION;
  -- Escalate exception to execution engine
  RAISE USING MESSAGE = CONCAT('Failure in GL Close Transaction Execution: ', @@error.message);
END;