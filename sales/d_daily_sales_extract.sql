DECLARE target_date_str STRING DEFAULT @input_date;
DECLARE target_date DATE;
DECLARE gcp_project STRING DEFAULT @gcp_project;
DECLARE bq_dataset STRING DEFAULT @bq_dataset;

SET target_date = PARSE_DATE('%Y-%m-%d', target_date_str);

BEGIN
  BEGIN TRANSACTION;

  -- Delete existing transactions for the execution date to ensure idempotent run
  EXECUTE IMMEDIATE FORMAT("""
    DELETE FROM `%s.%s.STG_DAILY_SALES`
    WHERE SALE_DATE = ?
  """, gcp_project, bq_dataset) USING target_date;

  -- Populate staging with source transaction joined to store dimension for region mappings
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.STG_DAILY_SALES`
        (SALE_ID, SALE_DATE, PRODUCT_ID, CUSTOMER_ID, STORE_ID, REGION_CODE, SALE_AMOUNT)
    SELECT
        p.SALE_ID,
        p.SALE_DATE,
        p.PRODUCT_ID,
        p.CUSTOMER_ID,
        p.STORE_ID,
        st.REGION_CODE,
        p.SALE_AMOUNT
    FROM `%s.%s.SRC_POS_TRANSACTIONS` p
    JOIN `%s.%s.DIM_STORE` st
      ON st.STORE_ID = p.STORE_ID
    WHERE p.SALE_DATE = ?
  """, gcp_project, bq_dataset, gcp_project, bq_dataset, gcp_project, bq_dataset) USING target_date;

  COMMIT TRANSACTION;
END;