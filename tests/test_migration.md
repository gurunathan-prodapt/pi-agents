As a senior QA engineer, I've reviewed the migration design document and the generated PySpark code. My goal is to ensure that the migration to BigQuery SQL/Stored Procedures (as per the design) will result in a robust, accurate, and performant data pipeline.

The generated PySpark code serves as a valuable blueprint for the transformation logic, even if the final target is BigQuery SQL. My test cases will leverage this PySpark structure for unit testing individual transformation functions and then translate to BigQuery SQL for data validation and integration scenarios, aligning with the migration design's ultimate target.

---

## Comprehensive Test Cases for `finaltestingrepo` Migration

### General Testing Principles

*   **Idempotency:** Running the same process multiple times with the same inputs should yield the same results without creating duplicates or altering previously processed data incorrectly.
*   **Data Integrity:** Ensure data types, nullability, uniqueness, and referential integrity are maintained or correctly transformed.
*   **Accuracy:** Verify all calculations, aggregations, filters, and joins produce correct results.
*   **Completeness:** All expected data from source systems should be present in the target, accounting for filters.
*   **Performance:** While not explicitly covered here, performance testing with large datasets would be a critical follow-up.
*   **Error Handling:** Verify that expected error conditions are handled gracefully (e.g., logging, alerting, retry mechanisms).

---

### 1. Unit Tests for Transformation Logic (PySpark Functions)

These tests focus on individual PySpark functions, providing sample input DataFrames and asserting expected outputs.

**A. Core Reusable Utilities (`core/`)**

*   **`core.common_transformations.add_load_metadata`**
    *   **Scenario 1: Basic Metadata Addition**
        *   Input: `df = spark.createDataFrame([("A", 1)], ["col1", "col2"])`, `load_date="2024-01-01"`, `batch_id="123"`, `source_file="test.csv"`
        *   Expected: `df` with `load_date`, `load_batch_id`, `source_file_name` columns correctly added.
    *   **Scenario 2: No Source File**
        *   Input: `df = spark.createDataFrame([("A", 1)], ["col1", "col2"])`, `load_date="2024-01-01"`, `batch_id="123"`
        *   Expected: `df` with `load_date`, `load_batch_id` but no `source_file_name`.
*   **`core.common_transformations.standardize_string_cols`**
    *   **Scenario 1: Trim Spaces**
        *   Input: `df = spark.createDataFrame([("  val1  ", "val2 ")], ["col1", "col2"])`, `cols=["col1", "col2"]`
        *   Expected: `df` with "val1", "val2" (trimmed).
    *   **Scenario 2: Handle Nulls**
        *   Input: `df = spark.createDataFrame([(None, " val2 ")], ["col1", "col2"])`, `cols=["col1", "col2"]`
        *   Expected: `df` with `None`, "val2".
*   **`core.common_transformations.safe_divide`**
    *   **Scenario 1: Normal Division**
        *   Input: `numerator=F.lit(10), denominator=F.lit(2)`
        *   Expected: `5.0`
    *   **Scenario 2: Division by Zero**
        *   Input: `numerator=F.lit(10), denominator=F.lit(0)`
        *   Expected: `0`
    *   **Scenario 3: Division by Null**
        *   Input: `numerator=F.lit(10), denominator=F.lit(None)`
        *   Expected: `0`
*   **`core.common_transformations.hash_key`**
    *   **Scenario 1: Consistent Hash for Same Inputs**
        *   Input: `F.lit("A"), F.lit("B")`
        *   Expected: Same SHA256 hash for "A||B".
    *   **Scenario 2: Different Hash for Different Inputs**
        *   Input: `F.lit("A"), F.lit("B")` vs `F.lit("B"), F.lit("A")`
        *   Expected: Different hashes.
    *   **Scenario 3: Handling Nulls**
        *   Input: `F.lit("A"), F.lit(None)`
        *   Expected: Hash for "A||".
*   **`core.dq_utils.dq_null_check`**
    *   **Scenario 1: No Nulls**
        *   Input: `df = spark.createDataFrame([("A", 1), ("B", 2)], ["col1", "col2"])`, `column_name="col1"`
        *   Expected: `failed_count=0`, `failed_pct=0.0`, `status="PASS"`.
    *   **Scenario 2: Some Nulls**
        *   Input: `df = spark.createDataFrame([("A", 1), (None, 2)], ["col1", "col2"])`, `column_name="col1"`
        *   Expected: `failed_count=1`, `failed_pct=50.0`, `status="FAIL"`.
    *   **Scenario 3: All Nulls**
        *   Input: `df = spark.createDataFrame([(None, 1), (None, 2)], ["col1", "col2"])`, `column_name="col1"`
        *   Expected: `failed_count=2`, `failed_pct=100.0`, `status="FAIL"`.
    *   **Scenario 4: Empty DataFrame**
        *   Input: `df = spark.createDataFrame([], ["col1", "col2"])`, `column_name="col1"`
        *   Expected: `total_count=0`, `failed_count=0`, `failed_pct=0.0`, `status="PASS"`.
*   **`core.dq_utils.dq_row_count_check`**
    *   **Scenario 1: Matching Counts**
        *   Input: `src_df` (2 rows), `tgt_df` (2 rows)
        *   Expected: `status="PASS"`.
    *   **Scenario 2: Mismatched Counts**
        *   Input: `src_df` (2 rows), `tgt_df` (1 row)
        *   Expected: `status="FAIL"`.
    *   **Scenario 3: Empty DataFrames**
        *   Input: `src_df` (0 rows), `tgt_df` (0 rows)
        *   Expected: `status="PASS"`.

**B. Sales Domain (`sales/`)**

*   **`sales.sales_extract.extract_sales_transactions`**
    *   **Scenario 1: Happy Path - Valid Transactions**
        *   Input: `SALES_TXN` with `txn_status_cd='POSTED'`, `sold_qty > 0`, `unit_sell_price > 0`, `disc_amount` present and null.
        *   Expected: Correctly filtered and transformed rows, `discount_pct` calculated, `currency_code` defaulted.
    *   **Scenario 2: Filtered Transactions**
        *   Input: `SALES_TXN` including `txn_status_cd` in ('VOID', 'CANCELLED', 'TEST'), `sold_qty <= 0`, `unit_sell_price < 0`.
        *   Expected: These rows should be excluded from the output.
    *   **Scenario 3: Discount Calculation Edge Cases**
        *   Input: `unit_sell_price = 0` for some rows.
        *   Expected: `discount_pct` should be 0 for these rows (no division by zero error).
    *   **Scenario 4: Null Currency**
        *   Input: `currency` is NULL for some rows.
        *   Expected: `currency_code` should default to 'GBP'.
*   **`sales.sales_rollup.build_sales_rollup`**
    *   **Scenario 1: Multiple Transactions for a Region/Date**
        *   Input: `FACT_DAILY_SALES` with several transactions for "UK", "2024-01-01".
        *   Expected: Correct `total_transactions`, `total_quantity`, `total_revenue`, `avg_basket_size`, `distinct_customers`, `distinct_products` for that group.
    *   **Scenario 2: Single Transaction**
        *   Input: `FACT_DAILY_SALES` with only one transaction for a region/date.
        *   Expected: Aggregations should reflect this single transaction.
    *   **Scenario 3: No Transactions for Filter Criteria**
        *   Input: `FACT_DAILY_SALES` where no rows match `load_date` and `region_code`.
        *   Expected: Empty DataFrame (no rollup row should be generated).
*   **`sales.sales_historization.scd2_merge_dimension` (Conceptual for PySpark, based on BQ MERGE logic)**
    *   **Scenario 1: New Product (INSERT)**
        *   Input: `current_dim_df` (empty), `staging_df` (new product).
        *   Expected: `action="INSERT"`.
    *   **Scenario 2: Existing Product, No Changes (NOOP)**
        *   Input: `current_dim_df` (product A, `is_current='Y'`), `staging_df` (product A, identical attributes).
        *   Expected: `action="NOOP"`.
    *   **Scenario 3: Existing Product, Changes Detected (EXPIRE_AND_INSERT)**
        *   Input: `current_dim_df` (product A, `is_current='Y'`, `list_price=10`), `staging_df` (product A, `list_price=12`).
        *   Expected: `action="EXPIRE_AND_INSERT"`.
    *   **Scenario 4: Handling Nulls in Comparison Columns**
        *   Input: `current_dim_df` (product A, `category_code=NULL`), `staging_df` (product A, `category_code='Electronics'`).
        *   Expected: `action="EXPIRE_AND_INSERT"`.
        *   Input: `current_dim_df` (product A, `category_code='Electronics'`), `staging_df` (product A, `category_code=NULL`).
        *   Expected: `action="EXPIRE_AND_INSERT"`.
        *   Input: `current_dim_df` (product A, `category_code=NULL`), `staging_df` (product A, `category_code=NULL`).
        *   Expected: `action="NOOP"`.
*   **`sales.sales_aggregation.build_sales_aggregation`**
    *   **Scenario 1: Basic Ranking**
        *   Input: `FACT_DAILY_SALES` with varied `net_amount` for a `txn_day`/`region_code`.
        *   Expected: `product_rank` assigned correctly (1, 2, 3...).
    *   **Scenario 2: Ties in Ranking**
        *   Input: `FACT_DAILY_SALES` with two products having the same `net_amount` within a partition.
        *   Expected: Both should receive the same rank (using `rank()` function).
    *   **Scenario 3: Empty Input**
        *   Input: Empty `FACT_DAILY_SALES`.
        *   Expected: Empty output DataFrame.
*   **`sales.sales_dq.run_sales_dq`**
    *   **Scenario 1: No Null Product Keys**
        *   Input: `FACT_DAILY_SALES` where `product_id` is never NULL.
        *   Expected: `DQ_RESULTS` row with `status="PASS"`, `failed_count=0`.
    *   **Scenario 2: Some Null Product Keys**
        *   Input: `FACT_DAILY_SALES` with some `product_id` as NULL.
        *   Expected: `DQ_RESULTS` row with `status="FAIL"`, `failed_count > 0`, `failed_pct` calculated correctly.

**C. Finance Domain (`finance/`)**

*   **`finance.gl_extract.extract_gl_transactions`**
    *   **Scenario 1: Happy Path - Debit and Credit**
        *   Input: `GL_JNL_LINES` with 'D' and 'C' flags, `AMOUNT > 0`, `STATUS='POSTED'`, `TXN_CURRENCY` present and null.
        *   Expected: `debit_amt`, `credit_amt`, `signed_amt`, `base_currency_amt` calculated correctly. `TXN_CURRENCY` defaulted to 'GBP' if null.
    *   **Scenario 2: Filtered Transactions**
        *   Input: `GL_JNL_LINES` with `AMOUNT=0` or `STATUS!='POSTED'`.
        *   Expected: These rows should be excluded.
    *   **Scenario 3: Exchange Rate Handling**
        *   Input: `STG_PERIOD_RATES` with and without matching rates for `TXN_CURRENCY`.
        *   Expected: `exchange_rate` should be applied if found, else default to 1. `base_currency_amt` calculated.
    *   **Scenario 4: Description Truncation**
        *   Input: `GL_JNL_LINES` with `DESCRIPTION` longer than 240 characters.
        *   Expected: `description` in output truncated to 240 characters.
*   **`finance.gl_transform.build_gl_balances`**
    *   **Scenario 1: Aggregation of Multiple Transactions**
        *   Input: `STG_GL_TRANSACTIONS` with multiple entries for the same `period_name`, `entity_code`, `account_segment`, `cc_segment`.
        *   Expected: Correct `total_debit`, `total_credit`, `net_amount`, `base_currency_amount` aggregated.
    *   **Scenario 2: Missing Dimension Account**
        *   Input: `STG_GL_TRANSACTIONS` with an `account_segment` not present in `DIM_ACCOUNT`.
        *   Expected: Left join should preserve the GL transaction, `DIM_ACCOUNT` fields would be null.
*   **`finance.gl_reconciliation.build_period_reconciliation`**
    *   **Scenario 1: Perfect Match**
        *   Input: `gl_balances_df` and `source_balances_df` with identical `net_amount` and `source_amount`.
        *   Expected: `variance=0`, `status="PASS"`.
    *   **Scenario 2: Mismatch**
        *   Input: `gl_balances_df` and `source_balances_df` with different amounts.
        *   Expected: `variance` calculated, `status="FAIL"`.
    *   **Scenario 3: Source Balance Missing**
        *   Input: `gl_balances_df` entry with no corresponding `source_balances_df` entry.
        *   Expected: `source_amount=0`, `variance=gl_amount`, `status="FAIL"`.
*   **`finance.account_processor.flatten_account_hierarchy`**
    *   **Scenario 1: Account with Parent**
        *   Input: `account_df` with `parent_account_code`.
        *   Expected: `hierarchy_path` correctly formed (e.g., "PARENT/CHILD").
    *   **Scenario 2: Root Account (No Parent)**
        *   Input: `account_df` with `parent_account_code=NULL`.
        *   Expected: `parent_account_code="ROOT"`, `hierarchy_path` correctly formed (e.g., "ROOT/ACCOUNT").
*   **`finance.account_processor.eliminate_intercompany`**
    *   **Scenario 1: Intercompany Accounts Present**
        *   Input: `df` with `account_code` like 'IC_123' and 'NON_IC_456'.
        *   Expected: Only 'NON_IC_456' rows remain.
    *   **Scenario 2: No Intercompany Accounts**
        *   Input: `df` with no `account_code` starting with 'IC_'.
        *   Expected: All rows remain.
*   **`finance.gl_aggregation.build_gl_aggregation`**
    *   **Scenario 1: Basic Aggregation**
        *   Input: `gl_balances_df` with multiple entries for same `period_name`, `entity_code`, `cc_segment`.
        *   Expected: Correct `ytd_balance`, `base_ytd_balance`, `variance_amount` (Spark's `variance` function).
    *   **Scenario 2: Single Entry per Group**
        *   Input: `gl_balances_df` where each group has only one entry.
        *   Expected: Aggregations should reflect the single entry.

**D. Customer Domain (`customer/`)**

*   **`customer.customer_extract.extract_customer_sources`**
    *   **Scenario 1: Data for Current and Past Dates**
        *   Input: `profile_df` with `load_date` matching `run_date`, `events_df` and `interactions_df` with dates `<= run_date`.
        *   Expected: All relevant data included in the output dictionary.
    *   **Scenario 2: Data for Future Dates**
        *   Input: `events_df` and `interactions_df` with dates `> run_date`.
        *   Expected: These rows should be excluded.
    *   **Scenario 3: Empty Source DataFrames**
        *   Input: Empty `profile_df`, `events_df`, `interactions_df`.
        *   Expected: Empty DataFrames in the output dictionary.
*   **`customer.customer_transform.build_customer_transform`**
    *   **Scenario 1: Full Customer Profile**
        *   Input: Customer with events (some converted), interactions, and retail spend.
        *   Expected: All rollup counts, `conversion_rate`, and `at_risk_flag` calculated correctly.
    *   **Scenario 2: Customer with No Events/Interactions/Spend**
        *   Input: Customer only in `profile_df`.
        *   Expected: `event_count=0`, `converted_count=0`, `interaction_count=0`, `conversion_rate=0`, `at_risk_flag` based on `days_since_last_purchase`.
    *   **Scenario 3: `conversion_rate` Division by Zero**
        *   Input: Customer with `event_count=0` but `converted_count=0`.
        *   Expected: `conversion_rate=0`.
    *   **Scenario 4: `at_risk_flag` Boundary Conditions**
        *   Input: `days_since_last_purchase` values around 30, 90, 180.
        *   Expected: `at_risk_flag` correctly set to 'Y' or 'N'.
*   **`customer.customer_historization.historize_customer_dim` (Conceptual for PySpark, based on BQ MERGE logic)**
    *   **Scenario 1: New Customer (INSERT)**
        *   Input: `current_dim_df` (empty), `staging_df` (new customer).
        *   Expected: `action="INSERT"`.
    *   **Scenario 2: Existing Customer, No Changes (NOOP)**
        *   Input: `current_dim_df` (customer A), `staging_df` (customer A, identical attributes).
        *   Expected: `action="NOOP"`.
    *   **Scenario 3: Existing Customer, Changes Detected (UPSERT)**
        *   Input: `current_dim_df` (customer A, `email='old@example.com'`), `staging_df` (customer A, `email='new@example.com'`).
        *   Expected: `action="UPSERT"`.
*   **`customer.customer_scoring.build_customer_scores`**
    *   **Scenario 1: Full Scoring Calculation**
        *   Input: `DIM_CUSTOMER_CRM` and `customer_transform_df` with `lifetime_value` and `last_purchase_date`.
        *   Expected: `value_score` (with capping) and `recency_score` calculated correctly.
    *   **Scenario 2: `lifetime_value` Capping**
        *   Input: `lifetime_value` > 10000 (e.g., 20000).
        *   Expected: `value_score` capped at 10.0.
    *   **Scenario 3: `last_purchase_date` Null**
        *   Input: `last_purchase_date` is NULL.
        *   Expected: `recency_score=0.95`.
    *   **Scenario 4: `last_purchase_date` Boundary Conditions**
        *   Input: `last_purchase_date` values resulting in `datediff` around 30, 90, 180 days.
        *   Expected: `recency_score` correctly assigned (0.20, 0.50, 0.80, 0.05).
*   **`customer.customer_segmentation.assign_customer_segments`**
    *   **Scenario 1: VIP Segment**
        *   Input: `scores_df` with `value_score=8.0`, `recency_score=8.0`.
        *   Expected: `composite_score=8.0`, `micro_segment="VIP"`.
    *   **Scenario 2: RETAIL Segment**
        *   Input: `scores_df` with `value_score=5.0`, `recency_score=5.0`.
        *   Expected: `composite_score=5.0`, `micro_segment="RETAIL"`.
    *   **Scenario 3: WHOLESALE Segment**
        *   Input: `scores_df` with `value_score=2.0`, `recency_score=2.0`.
        *   Expected: `composite_score=2.0`, `micro_segment="WHOLESALE"`.
    *   **Scenario 4: Segment Boundary Conditions**
        *   Input: `composite_score` values like 0.79, 0.80, 0.49, 0.50.
        *   Expected: `micro_segment` assigned correctly based on boundaries.
*   **`customer.lineage_tracker.build_lineage_metadata`**
    *   **Scenario 1: Basic Metadata Generation**
        *   Input: `job_name="test_job"`, `source_system="Oracle"`, `target_table="bq_table"`, `run_date="2024-01-01"`.
        *   Expected: DataFrame with these values and a `tracked_ts`.

---

### 2. Integration Test Stubs

These stubs outline the sequence of operations and key verification points for end-to-end flows within each domain, assuming the BigQuery stored procedures are called.

**A. Sales Domain Pipeline**

1.  **Setup:**
    *   Populate `project.source_ops.SALES_TXN`, `project.source_ops.CUSTOMER`, `project.source_ops.LOYALTY_PROFILE` with diverse test data for `load_date` (e.g., '2024-01-01') and `region_code` (e.g., 'UK').
    *   Ensure `project.sales.DIM_PRODUCT` and `project.sales.FACT_DAILY_SALES` are in a known state (e.g., empty or with baseline data).
2.  **Execution:**
    *   Call `CALL project.sales.sp_sales_extract('2024-01-01', 'UK', 'BATCH_123');`
    *   (Assume `FACT_DAILY_SALES` is populated by another process or directly for this test)
    *   Call `CALL project.sales.sp_sales_rollup('2024-01-01', 'UK');`
    *   Call `CALL project.sales.sp_load_dim_product('2024-01-01', 123);` (Requires `STG_PRODUCT_MASTER` to be populated).
    *   Call `CALL project.sales.sp_retail_dq('2024-01-01');`
3.  **Verification Points:**
    *   Check `project.sales.STG_SALES_TRANSACTIONS` for correct filtering and transformation.
    *   Check `project.sales.STG_CUSTOMER_SALES` for correct MERGE logic (inserts/updates).
    *   Check `project.sales.FACT_REGIONAL_SUMMARY` for accurate aggregations.
    *   Check `project.sales.DIM_PRODUCT` for correct SCD Type 2 behavior (new records, expired records, current flags).
    *   Check `project.audit.DQ_RESULTS` for sales DQ outcomes.

**B. Finance Domain Pipeline**

1.  **Setup:**
    *   Populate `project.source_fin.GL_JNL_LINES`, `project.source_fin.GL_LEDGERS`, `project.source_fin.LEGAL_ENTITIES` with test data for `period_name` (e.g., '2024-01') and `entity_code` (e.g., 'UK01').
    *   Populate `project.finance.STG_PERIOD_RATES` with various exchange rates, including 'GBP' and missing rates.
    *   Populate `project.finance.DIM_ACCOUNT` and `project.finance.SOURCE_FIN_AR_ACCOUNT_BALANCES`.
    *   Ensure `project.finance.DIM_PERIOD` is in a known state.
2.  **Execution:**
    *   Call `CALL project.finance.sp_gl_extract('2024-01', 'UK01', '2024-01-31');`
    *   Call `CALL project.finance.sp_close_period('2024-01', 'UK01', 'N');` (This procedure encapsulates `build_gl_balances` and `build_period_reconciliation`).
3.  **Verification Points:**
    *   Check `project.finance.STG_GL_TRANSACTIONS` for correct filtering, currency conversion, and description truncation.
    *   Check `project.finance.FACT_GL_BALANCES` for accurate aggregations of GL transactions.
    *   Check `project.finance.FACT_PERIOD_RECONCILIATION` for correct variance calculation and reconciliation status.
    *   Check `project.finance.DIM_PERIOD` for `IS_CLOSED='Y'` and `CLOSE_DATE` for '2024-01'.

**C. Customer Domain Pipeline**

1.  **Setup:**
    *   Populate `project.customer.STG_CUSTOMER_PROFILE`, `project.customer.STG_CAMPAIGN_EVENTS`, `project.customer.STG_CUSTOMER_INTERACTIONS` with test data for `run_date` (e.g., '2024-01-01').
    *   Populate `project.customer.DIM_CUSTOMER_CRM` and `project.sales.STG_CUSTOMER_SALES` (as `retail_spend` source).
2.  **Execution:**
    *   Call `CALL project.customer.sp_customer_scoring('2024-01-01', 'ALL', 'GLOBAL', 'v1');` (This procedure encapsulates the transformation, scoring, and segmentation logic).
3.  **Verification Points:**
    *   Check `project.customer.FACT_CUSTOMER_SCORES` for correct `value_score`, `recency_score`, `composite_score`, and `micro_segment` assignments.
    *   Verify `score_id` uniqueness.
    *   Verify `model_version` and `score_date`.

**D. Cross-Domain Orchestration (Conceptual)**

1.  **Setup:**
    *   Ensure all source systems are ready.
    *   Simulate `RETAIL_DAILY_COMPLETE` and `FINANCE_GL_CLOSE_COMPLETE` events (e.g., by inserting markers in a control table or publishing Pub/Sub messages).
2.  **Execution:**
    *   Trigger the main orchestration workflow (e.g., Cloud Composer DAG or Python runner).
3.  **Verification Points:**
    *   Verify the customer workflow waits for both sales and finance completion events before starting.
    *   Check `project.audit.JOB_AUDIT` for the sequence and status of all domain-specific jobs.
    *   Verify that customer tables correctly read data from sales and finance tables (e.g., `STG_CUSTOMER_SALES` for `lifetime_value`).

---

### 3. Data Validation Queries (BigQuery SQL)

These queries are designed to run directly against the BigQuery target tables after migration, verifying data integrity and transformation logic.

**A. Sales Domain**

*   **`project.sales.STG_SALES_TRANSACTIONS`**
    *   **Row Count & Filtering:**
        ```sql
        -- Compare with source count after applying filters
        SELECT COUNT(*) FROM `project.sales.STG_SALES_TRANSACTIONS`
        WHERE TRANSACTION_DATE = '2024-01-01' AND REGION_CODE = 'UK';

        -- Source count for comparison
        SELECT COUNT(*) FROM `project.source_ops.SALES_TXN`
        WHERE DATE(TXN_DATETIME) = '2024-01-01'
          AND STORE_REGION_CD = 'UK'
          AND TXN_STATUS_CD NOT IN ('VOID','CANCELLED','TEST')
          AND SOLD_QTY > 0
          AND UNIT_SELL_PRICE >= 0;
        ```
    *   **Discount Calculation:**
        ```sql
        -- Verify discount_pct calculation, especially for unit_price = 0
        SELECT TRANSACTION_ID, UNIT_PRICE, DISCOUNT_AMT, DISCOUNT_PCT
        FROM `project.sales.STG_SALES_TRANSACTIONS`
        WHERE TRANSACTION_DATE = '2024-01-01' AND REGION_CODE = 'UK'
          AND (
            (UNIT_PRICE > 0 AND ROUND((IFNULL(DISCOUNT_AMT,0) / UNIT_PRICE) * 100, 2) != DISCOUNT_PCT)
            OR (UNIT_PRICE = 0 AND DISCOUNT_PCT != 0)
          );
        ```
    *   **Currency Default:**
        ```sql
        SELECT COUNT(*) FROM `project.sales.STG_SALES_TRANSACTIONS`
        WHERE CURRENCY_CODE IS NULL OR CURRENCY_CODE = ''; -- Should be 0 if default 'GBP' is applied
        ```
*   **`project.sales.STG_CUSTOMER_SALES`**
    *   **Uniqueness:**
        ```sql
        SELECT CUSTOMER_ID, COUNT(*) FROM `project.sales.STG_CUSTOMER_SALES` GROUP BY 1 HAVING COUNT(*) > 1; -- Should be 0
        ```
    *   **Loyalty Tier Default:**
        ```sql
        SELECT CUSTOMER_ID, LOYALTY_TIER FROM `project.sales.STG_CUSTOMER_SALES` WHERE LOYALTY_TIER IS NULL; -- Should be 0 if default 'STANDARD' is applied
        ```
    *   **Last Purchase Date Logic:**
        ```sql
        -- Verify LAST_PURCHASE_DATE is the max transaction date for the customer on the load_date
        SELECT scs.CUSTOMER_ID, scs.LAST_PURCHASE_DATE, MAX(DATE(st.TXN_DATETIME)) AS Expected_Last_Purchase
        FROM `project.sales.STG_CUSTOMER_SALES` scs
        JOIN `project.source_ops.SALES_TXN` st ON scs.CUSTOMER_ID = st.CUST_ID
        WHERE DATE(st.TXN_DATETIME) = scs.LOAD_DATE AND scs.LOAD_DATE = '2024-01-01'
        GROUP BY 1, 2
        HAVING scs.LAST_PURCHASE_DATE != MAX(DATE(st.TXN_DATETIME));
        ```
*   **`project.sales.FACT_REGIONAL_SUMMARY`**
    *   **Uniqueness of Key:**
        ```sql
        SELECT SUMMARY_KEY, COUNT(*) FROM `project.sales.FACT_REGIONAL_SUMMARY` GROUP BY 1 HAVING COUNT(*) > 1; -- Should be 0
        ```
    *   **Aggregation Accuracy (Total Revenue):**
        ```sql
        SELECT frs.SUMMARY_DATE, frs.REGION_CODE, frs.TOTAL_REVENUE, SUM(fds.net_amount) AS Expected_Revenue
        FROM `project.sales.FACT_REGIONAL_SUMMARY` frs
        JOIN `project.sales.FACT_DAILY_SALES` fds
          ON frs.SUMMARY_DATE = fds.transaction_date AND frs.REGION_CODE = fds.region_code
        WHERE frs.SUMMARY_DATE = '2024-01-01' AND frs.REGION_CODE = 'UK'
        GROUP BY 1, 2, 3
        HAVING ABS(frs.TOTAL_REVENUE - SUM(fds.net_amount)) > 0.01; -- Allow for float precision
        ```
    *   **Distinct Counts:**
        ```sql
        SELECT frs.SUMMARY_DATE, frs.REGION_CODE, frs.DISTINCT_CUSTOMERS, COUNT(DISTINCT fds.dim_customer_key) AS Expected_Distinct_Customers
        FROM `project.sales.FACT_REGIONAL_SUMMARY` frs
        JOIN `project.sales.FACT_DAILY_SALES` fds
          ON frs.SUMMARY_DATE = fds.transaction_date AND frs.REGION_CODE = fds.region_code
        WHERE frs.SUMMARY_DATE = '2024-01-01' AND frs.REGION_CODE = 'UK'
        GROUP BY 1, 2, 3
        HAVING frs.DISTINCT_CUSTOMERS != COUNT(DISTINCT fds.dim_customer_key);
        ```
*   **`project.sales.DIM_PRODUCT` (SCD Type 2)**
    *   **Single Current Record per Product:**
        ```sql
        SELECT PRODUCT_ID, COUNT(*) FROM `project.sales.DIM_PRODUCT` WHERE IS_CURRENT = 'Y' GROUP BY 1 HAVING COUNT(*) > 1; -- Should be 0
        ```
    *   **Valid Date Ranges:**
        ```sql
        SELECT PRODUCT_ID, VALID_FROM, VALID_TO FROM `project.sales.DIM_PRODUCT` WHERE VALID_FROM >= VALID_TO; -- Should be 0
        ```
    *   **Current Record `VALID_TO`:**
        ```sql
        SELECT PRODUCT_ID, VALID_TO FROM `project.sales.DIM_PRODUCT` WHERE IS_CURRENT = 'Y' AND VALID_TO IS NOT NULL; -- Should be 0 (or max date if applicable)
        ```
    *   **Expired Record `VALID_TO`:**
        ```sql
        SELECT PRODUCT_ID, VALID_TO FROM `project.sales.DIM_PRODUCT` WHERE IS_CURRENT = 'N' AND VALID_TO IS NULL; -- Should be 0
        ```
*   **`project.audit.DQ_RESULTS`**
    *   **DQ Rule Verification:**
        ```sql
        SELECT * FROM `project.audit.DQ_RESULTS`
        WHERE rule_name = 'NULL_PRODUCT_KEY' AND load_date = '2024-01-01';

        -- Cross-check with source data
        SELECT COUNT(*) FROM `project.sales.FACT_DAILY_SALES`
        WHERE transaction_date = '2024-01-01' AND dim_product_key IS NULL;
        ```

**B. Finance Domain**

*   **`project.finance.STG_GL_TRANSACTIONS`**
    *   **Row Count & Filtering:**
        ```sql
        -- Compare with source count after applying filters
        SELECT COUNT(*) FROM `project.finance.STG_GL_TRANSACTIONS`
        WHERE PERIOD_NAME = '2024-01' AND ENTITY_CODE = 'UK01';

        -- Source count for comparison
        SELECT COUNT(*) FROM `project.source_fin.GL_JNL_LINES` g
        JOIN `project.source_fin.GL_LEDGERS` l ON l.LEDGER_ID = g.LEDGER_ID
        JOIN `project.source_fin.LEGAL_ENTITIES` le ON le.LEGAL_ENTITY_ID = l.LEGAL_ENTITY_ID
        WHERE g.PERIOD_NAME = '2024-01'
          AND le.ENTITY_SHORT_CODE = 'UK01'
          AND g.STATUS = 'POSTED'
          AND g.AMOUNT <> 0;
        ```
    *   **Debit/Credit Mutually Exclusive:**
        ```sql
        SELECT COUNT(*) FROM `project.finance.STG_GL_TRANSACTIONS` WHERE debit_amt > 0 AND credit_amt > 0; -- Should be 0
        ```
    *   **Signed Amount Calculation:**
        ```sql
        SELECT JOURNAL_LINE_ID, signed_amt, debit_amt, credit_amt
        FROM `project.finance.STG_GL_TRANSACTIONS`
        WHERE signed_amt != (debit_amt - credit_amt); -- Should be 0
        ```
    *   **Base Currency Amount & Exchange Rate:**
        ```sql
        SELECT JOURNAL_LINE_ID, TXN_CURRENCY, exchange_rate, base_currency_amt, ABS(debit_amt + credit_amt) AS Original_Amount
        FROM `project.finance.STG_GL_TRANSACTIONS`
        WHERE base_currency_amt != ABS(debit_amt + credit_amt) * exchange_rate; -- Should be 0
        ```
*   **`project.finance.FACT_GL_BALANCES`**
    *   **Uniqueness of Aggregation Key:**
        ```sql
        SELECT period_name, entity_code, account_segment, cc_segment, COUNT(*)
        FROM `project.finance.FACT_GL_BALANCES` GROUP BY 1,2,3,4 HAVING COUNT(*) > 1; -- Should be 0
        ```
    *   **Total Net Amount Balance:**
        ```sql
        SELECT SUM(net_amount) FROM `project.finance.FACT_GL_BALANCES`
        WHERE period_name = '2024-01' AND entity_code = 'UK01'; -- Should balance to 0 for a closed period if all accounts are included
        ```
*   **`project.finance.FACT_PERIOD_RECONCILIATION`**
    *   **Variance Check:**
        ```sql
        SELECT COUNT(*) FROM `project.finance.FACT_PERIOD_RECONCILIATION`
        WHERE ABS(variance) > 0.01 AND period_name = '2024-01' AND entity_code = 'UK01'; -- Identify significant variances
        ```
    *   **Status Accuracy:**
        ```sql
        SELECT COUNT(*) FROM `project.finance.FACT_PERIOD_RECONCILIATION`
        WHERE (ABS(variance) < 0.01 AND status = 'FAIL') OR (ABS(variance) >= 0.01 AND status = 'PASS'); -- Should be 0
        ```
*   **`project.finance.DIM_PERIOD`**
    *   **Period Closure:**
        ```sql
        SELECT IS_CLOSED, CLOSE_DATE FROM `project.finance.DIM_PERIOD` WHERE PERIOD_NAME = '2024-01'; -- Verify 'Y' and a valid date
        ```

**C. Customer Domain**

*   **`project.customer.FACT_CUSTOMER_SCORES`**
    *   **Uniqueness of Score ID:**
        ```sql
        SELECT score_id, COUNT(*) FROM `project.customer.FACT_CUSTOMER_SCORES` GROUP BY 1 HAVING COUNT(*) > 1; -- Should be 0
        ```
    *   **Value Score Capping:**
        ```sql
        SELECT customer_id, value_score FROM `project.customer.FACT_CUSTOMER_SCORES` WHERE value_score > 10.0; -- Should be 0
        ```
    *   **Recency Score Logic:**
        ```sql
        -- Verify recency score for customers with NULL last_purchase_date
        SELECT fcs.customer_id, fcs.recency_score, scs.LAST_PURCHASE_DATE
        FROM `project.customer.FACT_CUSTOMER_SCORES` fcs
        LEFT JOIN `project.sales.STG_CUSTOMER_SALES` scs ON fcs.customer_id = scs.customer_id
        WHERE scs.LAST_PURCHASE_DATE IS NULL AND fcs.recency_score != 0.95; -- Should be 0
        ```
    *   **Micro-segment Assignment:**
        ```sql
        SELECT customer_id, composite_score, micro_segment
        FROM `project.customer.FACT_CUSTOMER_SCORES`
        WHERE (composite_score >= 0.8 AND micro_segment != 'VIP')
           OR (composite_score >= 0.5 AND composite_score < 0.8 AND micro_segment != 'RETAIL')
           OR (composite_score < 0.5 AND micro_segment != 'WHOLESALE'); -- Should be 0
        ```

---

### 4. Edge Cases

These scenarios test the robustness and error handling of the migration.

**A. Data-Related Edge Cases**

1.  **Empty Source Tables:**
    *   What happens if `SALES_TXN`, `GL_JNL_LINES`, `CUSTOMER`, `LOYALTY_PROFILE`, `STG_PRODUCT_MASTER`, `STG_CAMPAIGN_EVENTS`, `STG_CUSTOMER_INTERACTIONS` are completely empty?
    *   Expected: Target tables should remain empty or procedures should complete successfully without errors, possibly logging "no data processed".
2.  **All Filtered Out:**
    *   `SALES_TXN`: All transactions are 'VOID'/'CANCELLED'/'TEST' or have `sold_qty <= 0`.
    *   `GL_JNL_LINES`: All transactions have `AMOUNT = 0` or `STATUS != 'POSTED'`.
    *   Expected: Staging tables should be empty for the given `load_date`/`period_name`.
3.  **Missing Dimension Keys:**
    *   `FACT_DAILY_SALES` contains `dim_product_key` or `dim_customer_key` that do not exist in `DIM_PRODUCT` or `DIM_CUSTOMER`.
    *   `STG_GL_TRANSACTIONS` contains `account_segment` not in `DIM_ACCOUNT`.
    *   Expected: Left joins should handle this gracefully (NULLs for dimension attributes), and downstream DQ checks should flag these as potential issues.
4.  **Null/Zero Values in Calculations:**
    *   `unit_sell_price = 0` when calculating `discount_pct`.
    *   `lifetime_value = 0` or `NULL` when calculating `value_score`.
    *   `last_purchase_date = NULL` when calculating `recency_score`.
    *   `event_count = 0` when calculating `conversion_rate`.
    *   Expected: Calculations should not fail (e.g., division by zero), and default/fallback logic should be applied correctly.
5.  **Extreme Values:**
    *   Very large `quantity`, `unit_price`, `amount` (check for overflow if data types are not `NUMERIC` or `BIGNUMERIC`).
    *   Very old or future dates (check date parsing and `DATE_DIFF` logic).
    *   Very long string fields (check truncation logic, e.g., `description` in GL).
6.  **Duplicate Source Data:**
    *   If source systems provide duplicate `TRANSACTION_ID` or `JNL_LINE_ID` for the same `load_date`/`period_name`.
    *   Expected: The `DELETE` and `INSERT` or `MERGE` logic should handle this idempotently, preventing duplicate final records in staging/fact tables where keys are defined.

**B. Orchestration & System-Related Edge Cases**

1.  **Upstream Dependency Failure:**
    *   `RETAIL_DAILY_COMPLETE` or `FINANCE_GL_CLOSE_COMPLETE` event is never published.
    *   Expected: Customer workflow should time out or remain in a waiting state, with appropriate logging/alerting.
2.  **Transient Failures:**
    *   A BigQuery job fails intermittently (e.g., network issue, temporary BigQuery service unavailability).
    *   Expected: `retry_command` (or Cloud Composer's retry mechanism) should re-attempt the step, and eventually succeed if the issue is transient.
3.  **Persistent Failures:**
    *   A BigQuery job consistently fails due to a logic error or invalid data.
    *   Expected: Retries should exhaust, the job should be marked as failed, and alerts should be triggered.
4.  **Concurrency Issues:**
    *   If the same pipeline is triggered multiple times for the same `load_date`/`period_name` (e.g., due to misconfiguration).
    *   Expected: The `DELETE` statements at the beginning of procedures should make them idempotent, preventing duplicate data.
5.  **Resource Exhaustion:**
    *   Running very large queries that might exceed BigQuery slot limits or memory.
    *   Expected: Queries should fail with resource errors, requiring optimization or scaling.
6.  **Security/Permissions:**
    *   Service account lacks necessary permissions to read from source tables, write to target tables, or execute stored procedures.
    *   Expected: Jobs should fail with permission errors.
7.  **Configuration Errors:**
    *   Incorrect dataset names, table names, or project IDs in `app_config.py` (or BigQuery config).
    *   Expected: Jobs should fail with "table not found" or similar errors.

---

This comprehensive set of test cases, covering unit logic, integration flows, data validation, and edge conditions, will provide a strong foundation for ensuring a successful and high-quality migration to BigQuery.