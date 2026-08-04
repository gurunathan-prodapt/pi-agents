-- d_product_master_load.sql
-- SCD Type 2 merge of the product master dimension.
-- Schema: ANALYTICS_SCHEMA

BEGIN
  -- Declare runtime tracking variable to ensure absolute consistency across DML operations
  DECLARE current_datetime_val DATETIME;
  SET current_datetime_val = CURRENT_DATETIME();

  -- Begin atomic transaction block to enforce consistency for the SCD Type 2 load sequence
  BEGIN TRANSACTION;

  -- Step 1: Expire changed records in target, and insert entirely new records
  MERGE INTO ANALYTICS_SCHEMA.DIM_PRODUCT tgt
  USING ANALYTICS_SCHEMA.STG_PRODUCT_MASTER src
  ON (tgt.PRODUCT_ID = src.PRODUCT_ID AND tgt.IS_CURRENT = 1)
  WHEN MATCHED AND (
           tgt.PRODUCT_NAME <> src.PRODUCT_NAME
        OR tgt.CATEGORY     <> src.CATEGORY
        OR tgt.UNIT_PRICE   <> src.UNIT_PRICE
       ) THEN
      UPDATE SET tgt.IS_CURRENT = 0,
                 tgt.VALID_TO   = current_datetime_val
  WHEN NOT MATCHED THEN
      INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, IS_CURRENT, VALID_FROM)
      VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, 1, current_datetime_val);

  -- Step 2: Insert new active versions of records that were expired in the preceding MERGE step
  INSERT INTO ANALYTICS_SCHEMA.DIM_PRODUCT
      (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, IS_CURRENT, VALID_FROM)
  SELECT 
      src.PRODUCT_ID, 
      src.PRODUCT_NAME, 
      src.CATEGORY, 
      src.UNIT_PRICE, 
      1, 
      current_datetime_val
  FROM ANALYTICS_SCHEMA.STG_PRODUCT_MASTER src
  INNER JOIN ANALYTICS_SCHEMA.DIM_PRODUCT tgt
    ON tgt.PRODUCT_ID = src.PRODUCT_ID 
    AND tgt.IS_CURRENT = 0 
    AND tgt.VALID_TO = current_datetime_val;

  -- Commit transaction block to apply all dimension changes atomically
  COMMIT TRANSACTION;
END;