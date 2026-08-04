-- d_historization_load.sql
-- SCD Type 2 merge of the weekly customer score/segment into the segment
-- dimension. A customer's prior current row is expired only when the
-- segment or score band actually changed.
-- Target Platform: Google Cloud BigQuery Standard SQL Scripting

BEGIN
  -- Declare variables
  DECLARE v_run_date_str STRING;
  DECLARE v_run_date DATE;
  DECLARE v_current_timestamp TIMESTAMP;

  -- Assign input run date string from orchestration parameter (represents '&1')
  SET v_run_date_str = @run_date_param;
  
  -- Convert input parameter to standard DATE format
  SET v_run_date = PARSE_DATE('%Y-%m-%d', v_run_date_str);
  
  -- Lock execution timestamp for consistent SCD transaction boundaries
  SET v_current_timestamp = CURRENT_TIMESTAMP();

  -- Start of multi-statement transactional block for safe SCD Type 2 execution
  BEGIN TRANSACTION;

  -- STEP 1: Expire old active customer records where values have changed, 
  -- and insert brand-new customers who do not yet exist.
  MERGE INTO `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` AS tgt
  USING (
      SELECT CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE
      FROM   `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`
      WHERE  RUN_DATE = v_run_date
  ) AS src
  ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = 1)
  
  WHEN MATCHED AND (
         tgt.SEGMENT_CODE <> src.SEGMENT_CODE
      OR tgt.SCORE_BAND   <> src.SCORE_BAND
     ) THEN
    UPDATE SET 
        tgt.IS_CURRENT = 0,
        tgt.VALID_TO   = v_current_timestamp
        
  WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
    VALUES (src.CUSTOMER_ID, src.SEGMENT_CODE, src.SCORE_BAND, src.SCORE_VALUE, 1, v_current_timestamp);

  -- STEP 2: Insert new active versions of records for customers whose prior active row 
  -- was just expired in the MERGE step. Joined based on matching timestamp from Step 1.
  INSERT INTO `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
      (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM)
  SELECT 
      src.CUSTOMER_ID, 
      src.SEGMENT_CODE, 
      src.SCORE_BAND, 
      src.SCORE_VALUE, 
      1, 
      v_current_timestamp
  FROM   `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` AS src
  INNER JOIN `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` AS tgt
    ON   tgt.CUSTOMER_ID = src.CUSTOMER_ID 
    AND  tgt.IS_CURRENT = 0 
    AND  tgt.VALID_TO = v_current_timestamp
  WHERE  src.RUN_DATE = v_run_date;

  -- Commit transaction to solidify all historical versions atomically
  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;