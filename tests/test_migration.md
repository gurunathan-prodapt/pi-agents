As a senior QA engineer, I've reviewed the migration design document and the generated PySpark code. Below is a comprehensive set of test cases, categorized as requested, to ensure the successful and accurate migration of the `finaltestingrepo` ETL pipeline to Google BigQuery.

---

## Comprehensive Test Cases for `finaltestingrepo` GCP Migration

### 0. General Testing Setup & Principles

*   **Test Environment:** Dedicated GCP project with BigQuery datasets (`stg_*`, `dw_*`, `audit_etl`) and GCS buckets (`your-temp-bucket`, raw data buckets) provisioned.
*   **Data Generation:** Use synthetic data that mimics production data characteristics (data types, distributions, null patterns, edge values) but is small enough for efficient testing.
*   **Baseline Comparison:** For integration and data validation, compare results against the legacy Oracle/Ab Initio/Spark outputs for a representative dataset.
*   **Automation:** All tests should be automatable (e.g., using `pytest` for unit tests, Airflow DAGs for integration tests, BigQuery scripting for data validation).
*   **Idempotency:** Ensure tests can be run multiple times without side effects or requiring manual cleanup (or include cleanup steps).

### 1. Unit Tests for Transformation Logic (PySpark Functions)

These tests focus on individual PySpark functions, verifying their logic in isolation using mock Spark DataFrames.

**Setup:**
*   Use `pytest` framework.
*   A fixture for a `SparkSession` in local mode.
*   Helper functions to create mock DataFrames with specified schemas and data.

```python
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from finaltestingrepo_gcp_migration import (
    build_spark, read_bigquery, write_bigquery, add_load_metadata,
    standardize_string_cols, safe_coalesce, prepare_scd2_source,
    scd2_merge_sql, build_dim_account_source, derive_period_dimension,
    build_fact_gl_balances, build_period_reconciliation, build_fin_account_hierarchy_snapshot,
    build_dim_product_source, build_dim_customer_retail_source, build_fact_daily_sales,
    build_fact_regional_summary, build_retail_product_rankings, build_retail_daily_analytical_summary,
    build_dim_customer_crm_source, build_customer_scores, build_crm_micro_segments,
    build_crm_region_segment_dist, build_crm_segment_weekly_snapshot, run_dq_rule, dq_null_check
)

@pytest.fixture(scope="module")
def spark_session():
    spark = build_spark("test_finaltestingrepo")
    yield spark
    spark.stop()

def create_df(spark, schema, data):
    return spark.createDataFrame([Row(**r) for r in data], schema)

# Helper for comparing dataframes (simplified, real impl would use assert_df_equal)
def assert_dfs_equal(df1, df2, sort_cols=None):
    if sort_cols:
        df1 = df1.sort(*sort_cols)
        df2 = df2.sort(*sort_cols)
    assert df1.count() == df2.count()
    assert df1.schema == df2.schema
    assert df1.collect() == df2.collect()
```

---

#### 1.1. General Utilities

**`test_add_load_metadata`**
*   **Input:** A simple DataFrame.
*   **Expected Output:** DataFrame with `load_date` (current date) and `load_batch_id` (timestamp) columns added.
*   **Edge Case:** Empty DataFrame.

**`test_standardize_string_cols`**
*   **Input:** DataFrame with string columns containing leading/trailing spaces and mixed case.
*   **Expected Output:** Selected string columns trimmed and upper-cased.
*   **Edge Case:** Null values in string columns, empty strings.

**`test_safe_coalesce`**
*   **Input:** DataFrame with columns containing various combinations of nulls and non-nulls.
*   **Expected Output:** Correctly coalesced values.

---

#### 1.2. SCD Type 2 Utilities

**`test_prepare_scd2_source`**
*   **Input:** DataFrame with natural keys and tracked columns.
*   **Expected Output:** DataFrame with a new `record_hash` column, ensuring deterministic hash generation for identical records.
*   **Edge Cases:**
    *   Changes in only one tracked column.
    *   Changes in multiple tracked columns.
    *   Null values in tracked columns (should be handled deterministically, e.g., `COALESCE(col, '')`).
    *   Empty DataFrame.

**`test_scd2_merge_sql`**
*   **Input:** Parameters for `target_table`, `source_view`, `natural_key_cols`, `tracked_cols`, `surrogate_key_col`, etc.
*   **Expected Output:** A correctly formatted BigQuery `MERGE` SQL string.
*   **Edge Cases:**
    *   Empty `tracked_cols` list.
    *   Different numbers of natural keys.
    *   Custom `valid_from`/`valid_to` column names.

---

#### 1.3. Finance Domain Transformations

**`test_build_dim_account_source`**
*   **Input:** Mock `stg_account_master` DataFrame.
*   **Expected Output:** DataFrame with `VALID_FROM`, `VALID_TO`, `IS_CURRENT`, `VERSION_NUM`, `CREATED_DATE`, `UPDATED_DATE` columns initialized correctly.
*   **Edge Cases:**
    *   `IS_ACTIVE` values ('Y', 'N').
    *   Null `PARENT_ACCOUNT_CODE`.

**`test_derive_period_dimension`**
*   **Input:** Mock `stg_period_rates` DataFrame with various `PERIOD_NAME` formats.
*   **Expected Output:** `dim_period` DataFrame with `PERIOD_YEAR`, `PERIOD_NUM` extracted correctly. `DIM_PERIOD_KEY` should be unique.
*   **Edge Cases:**
    *   `PERIOD_NAME` not matching regex pattern (should result in nulls or errors, depending on desired behavior).
    *   Duplicate `PERIOD_NAME` entries (should result in a single distinct entry).

**`test_build_fact_gl_balances`**
*   **Input:** Mock `stg_gl_transactions`, `dim_account` (current records), `dim_period`.
    *   Test 1: With `dim_cost_centre` (mock `dim_cost_centre` current records).
    *   Test 2: Without `dim_cost_centre` (pass `None`).
*   **Expected Output:** Correctly aggregated `PERIOD_DEBITS`, `PERIOD_CREDITS`, `FUNCTIONAL_BALANCE`, `CLOSING_BALANCE`. Correct joins to dimension keys.
*   **Edge Cases:**
    *   Transactions with no matching account/period/cost centre (should result in null dimension keys).
    *   All debit/credit amounts are null or zero.
    *   Mixed currencies (though `FUNCTIONAL_AMOUNT` is used, `CURRENCY_CODE` is retained).

**`test_build_period_reconciliation`**
*   **Input:** Mock `fact_gl_balances` and `subledger_df`.
*   **Expected Output:** Correct `VARIANCE_AMOUNT`, `VARIANCE_PCT`, `RECON_STATUS` (MATCH/MISMATCH).
*   **Edge Cases:**
    *   Perfect match.
    *   Significant variance.
    *   One side (GL or subledger) has no record for an account/period.
    *   `SUB_LEDGER_BALANCE` is zero (should handle division by zero for `VARIANCE_PCT`).

**`test_build_fin_account_hierarchy_snapshot`**
*   **Input:** Mock `dim_account` with various parent-child relationships.
*   **Expected Output:** `ROOT_ACCOUNT_CODE` and `LEVEL` derived correctly.
*   **Edge Cases:**
    *   Accounts with no parent (should be their own root).
    *   Multi-level hierarchies (though `LEVEL` is currently hardcoded to 1, this might be a future enhancement point).

---

#### 1.4. Retail Domain Transformations

**`test_build_dim_product_source`**
*   **Input:** Mock `stg_product_master` DataFrame.
*   **Expected Output:** `VALID_FROM`, `VALID_TO`, `IS_CURRENT`, `VERSION_NUM`, `CREATED_DATE`, `UPDATED_DATE` initialized. `IS_CURRENT` derived from `EXPIRY_DATE`.
*   **Edge Cases:**
    *   `EXPIRY_DATE` is null (should be `IS_CURRENT='Y'`).
    *   `EFFECTIVE_DATE` is in the future.
    *   `IS_ACTIVE` values ('Y', 'N').

**`test_build_dim_customer_retail_source`**
*   **Input:** Mock `stg_customer_sales` DataFrame.
*   **Expected Output:** `FULL_NAME` concatenated, `VALID_FROM`, `VALID_TO`, `IS_CURRENT`, `VERSION_NUM`, `CREATED_DATE`, `UPDATED_DATE` initialized.
*   **Edge Cases:**
    *   Null `FIRST_NAME` or `LAST_NAME`.
    *   `REGISTRATION_DATE` is null.

**`test_build_fact_daily_sales`**
*   **Input:** Mock `stg_sales_transactions`, `dim_product` (current records).
    *   Test 1: With `dim_customer` and `dim_store` (mock current records).
    *   Test 2: Without `dim_customer` and `dim_store` (pass `None`).
*   **Expected Output:** Correct `GROSS_AMOUNT`, `NET_AMOUNT`, `FISCAL_YEAR`, `FISCAL_MONTH`, `FISCAL_WEEK`. Correct joins to dimension keys.
*   **Edge Cases:**
    *   Transactions with no matching product/customer/store.
    *   Zero `QUANTITY` or `UNIT_PRICE`.
    *   Negative `DISCOUNT_AMT` (should be handled as a credit, or flagged as error).
    *   `TRANSACTION_DATE` at year/month boundaries.

**`test_build_fact_regional_summary`**
*   **Input:** Mock `fact_daily_sales` DataFrame.
*   **Expected Output:** Correct aggregations for `TOTAL_TRANSACTIONS`, `TOTAL_QUANTITY`, `TOTAL_REVENUE`, `AVG_BASKET_SIZE`, `DISTINCT_CUSTOMERS`, `DISTINCT_PRODUCTS` per region and date.
*   **Edge Cases:**
    *   Single transaction for a region/date.
    *   No transactions for a region/date.
    *   All customers/products are distinct.

**`test_build_retail_product_rankings`**
*   **Input:** Mock `fact_daily_sales` DataFrame.
*   **Expected Output:** Correct `REVENUE_RANK`, `REVENUE_DENSE_RANK`, `REVENUE_PCT_SHARE` within each `REGION_CODE`, `FISCAL_YEAR`, `FISCAL_MONTH` partition.
*   **Edge Cases:**
    *   Tied revenues (rank vs. dense_rank behavior).
    *   Single product in a partition.
    *   Zero revenue products.

**`test_build_retail_daily_analytical_summary`**
*   **Input:** Mock `fact_daily_sales` DataFrame.
*   **Expected Output:** Correct `RUNNING_REVENUE_YTD` and `REVENUE_VS_PREV_DAY`.
*   **Edge Cases:**
    *   First day of the year/month (YTD should be current day's revenue, prev_day_revenue should be null).
    *   Gaps in `TRANSACTION_DATE` for a region.
    *   Negative revenue days.

---

#### 1.5. Customer Domain Transformations

**`test_build_dim_customer_crm_source`**
*   **Input:** Mock `stg_customer_profile` DataFrame.
*   **Expected Output:** `FULL_NAME` concatenated, `VALID_FROM`, `VALID_TO`, `IS_CURRENT`, `VERSION_NUM`, `CREATED_DATE`, `UPDATED_DATE` initialized.
*   **Edge Cases:**
    *   Null `FIRST_NAME` or `LAST_NAME`.
    *   `REGISTRATION_DATE` is null.

**`test_build_customer_scores`**
*   **Input:** Mock `crm_customer_dim` DataFrame.
*   **Expected Output:** All score columns (`clv_score`, `churn_risk_score`, etc.) populated with default/hardcoded values. `segment_code` and `segment_label` derived.
*   **Edge Cases:**
    *   Null `customer_segment` (should default to 'UNKNOWN').
    *   Empty input DataFrame.
    *   (Note: `feature_config` is not currently used in the provided code, so no specific tests for its influence are needed unless the code is updated).

**`test_build_crm_micro_segments`**
*   **Input:** Mock `fact_customer_scores` DataFrame.
*   **Expected Output:** `composite_score` calculated, `micro_segment` and `intervention_recommended` derived based on score thresholds.
*   **Edge Cases:**
    *   Scores exactly on a threshold (e.g., `composite_score` = 75 or 50).
    *   All scores are very low/high.
    *   Null scores (should result in null composite score and potentially default micro-segment).

**`test_build_crm_region_segment_dist`**
*   **Input:** Mock `crm_micro_segments` DataFrame.
*   **Expected Output:** Correct `customer_count`, `avg_clv`, `avg_churn_risk`, `avg_composite_score`, `email_opted_in`, `avg_retail_lifetime_value`, and `pct_of_region`.
*   **Edge Cases:**
    *   Single customer in a region/segment.
    *   Region with no customers.
    *   All customers in a region opted in/out.
    *   Division by zero for `pct_of_region` (if `region_total` is 0, though `total_by_region` should prevent this if `crm_micro_segments` is not empty).

**`test_build_crm_segment_weekly_snapshot`**
*   **Input:** Mock `crm_micro_segments` DataFrame.
*   **Expected Output:** Correct aggregations for `customer_count`, `avg_clv_score`, `avg_churn_risk`, `high_value_count`, `at_risk_count`, `avg_retail_spend` per segment and region.
*   **Edge Cases:**
    *   No high-value or at-risk customers in a segment.
    *   Empty segment/region.

---

#### 1.6. Data Quality Framework

**`test_run_dq_rule`**
*   **Input:** DataFrame and a SQL expression string.
*   **Expected Output:** Tuple `(total_rows, failed_rows)` correctly calculated.
*   **Edge Cases:**
    *   Rule that passes all rows.
    *   Rule that fails all rows.
    *   Rule with complex logic.
    *   Empty DataFrame.

**`test_dq_null_check`**
*   **Input:** DataFrame with various null/non-null combinations in specified columns.
*   **Expected Output:** DataFrame with `dq_failed` column correctly marking rows with nulls in any of the specified columns.
*   **Edge Cases:**
    *   No nulls in specified columns.
    *   All nulls in specified columns.
    *   Empty list of columns to check.

---

### 2. Integration Test Stubs

Integration tests verify the interaction between different components and the end-to-end flow. These would typically be orchestrated by Cloud Composer DAGs in a test environment.

**General Integration Test Flow:**
1.  **Setup:**
    *   Provision mock source data in GCS (e.g., CSV files for `stg_customer_profile`, `stg_gl_transactions`).
    *   Ensure target BigQuery datasets and tables exist (or are created by the pipeline).
    *   Pre-load any necessary dimension tables (e.g., `dim_product`, `dim_account`) with known good data for join lookups.
2.  **Execution:**
    *   Trigger the relevant Cloud Composer DAG (e.g., `finance_daily_workflow`).
    *   Monitor DAG execution for success/failure.
3.  **Verification:**
    *   Check BigQuery target tables for data.
    *   Run data validation queries (see Section 3).
    *   Check `audit_etl.etl_job_audit` for job status and row counts.
    *   Check `audit_etl.dq_results` for DQ check outcomes.
4.  **Teardown:**
    *   Clean up temporary GCS files.
    *   Optionally truncate/delete data from target BigQuery tables for next run.

---

#### 2.1. Finance Domain Pipeline (`finance_daily_workflow`)

*   **Scenario:** Full daily GL processing, including dimension updates and fact loading.
*   **Components to Test:**
    *   GCS -> `stg_finance.stg_gl_transactions` (mock ingestion).
    *   `build_dim_account_source` -> `dw_finance.dim_account` (via `MERGE`).
    *   `derive_period_dimension` -> `dw_finance.dim_period` (via `MERGE`).
    *   `build_fact_gl_balances` -> `dw_finance.fact_gl_balances`.
    *   `build_period_reconciliation` -> `dw_finance.fact_period_reconciliation`.
    *   `build_fin_account_hierarchy_snapshot` -> `dw_finance.fin_account_hierarchy_snapshot`.
    *   Dataproc job execution (`account_processor.scala`, `gl_aggregation.py`).
*   **Key Test Cases:**
    *   **Initial Load:** Run with empty target DW tables.
    *   **Incremental Load (No Changes):** Run with source data identical to existing DW data.
    *   **Incremental Load (Updates):**
        *   New accounts in `stg_account_master` (should create new `dim_account` records).
        *   Changes to tracked attributes of existing accounts (should create new `dim_account` versions, mark old as `is_current='N'`).
        *   New GL transactions.
        *   Updates to existing GL transactions (if applicable, design implies append-only for facts).
    *   **Cross-domain dependency:** Verify `FINANCE_GL_CLOSE_COMPLETE` event is published.

#### 2.2. Retail Domain Pipeline (`retail_daily_workflow`)

*   **Scenario:** Daily sales processing, product/customer/store dimension updates, and sales fact/summary loading.
*   **Components to Test:**
    *   GCS -> `stg_retail.stg_sales_transactions`, `stg_retail.stg_product_master`, `stg_retail.stg_customer_sales`, `stg_retail.stg_store_master`.
    *   `build_dim_product_source` -> `dw_retail.dim_product` (via `MERGE`).
    *   `build_dim_customer_retail_source` -> `dw_retail.dim_customer` (via `MERGE`).
    *   `build_dim_store` -> `dw_retail.dim_store` (via `MERGE`).
    *   `build_fact_daily_sales` -> `dw_retail.fact_daily_sales`.
    *   `build_fact_regional_summary` -> `dw_retail.fact_regional_summary`.
    *   `build_retail_product_rankings` -> `dw_retail.retail_product_rankings`.
    *   `build_retail_daily_analytical_summary` -> `dw_retail.retail_daily_analytical_summary`.
    *   Dataproc job execution (`sales_aggregation.scala`, `retail_data_quality.py`).
*   **Key Test Cases:**
    *   **Initial Load:** Run with empty target DW tables.
    *   **Incremental Load (Updates):**
        *   New products/customers/stores.
        *   Changes to tracked attributes of existing products/customers/stores.
        *   New sales transactions.
    *   **Data Quality:** Verify `retail_data_quality.py` runs and populates `dq_results`.
    *   **Cross-domain dependency:** Verify `RETAIL_DAILY_COMPLETE` event is published.

#### 2.3. Customer Domain Pipeline (`crm_weekly_workflow`)

*   **Scenario:** Weekly customer scoring, segmentation, and campaign performance analysis.
*   **Components to Test:**
    *   `wait_finance_gl_close`, `wait_retail_daily_complete` (sensor functionality).
    *   GCS -> `stg_customer.stg_customer_profile`, `stg_customer.stg_campaign_events`, `stg_customer.stg_customer_interactions`.
    *   `build_dim_customer_crm_source` -> `dw_customer.dim_customer_crm` (via `MERGE`).
    *   `build_customer_scores` -> `dw_customer.fact_customer_scores`.
    *   `build_crm_micro_segments` -> `dw_customer.crm_micro_segments`.
    *   `build_crm_region_segment_dist` -> `dw_customer.crm_region_segment_dist`.
    *   `build_crm_segment_weekly_snapshot` -> `dw_customer.crm_segment_weekly_snapshot`.
    *   Dataproc job execution (`crm_customer_scoring.mp` (translated), `customer_segmentation.scala`).
*   **Key Test Cases:**
    *   **Dependency Fulfillment:** Ensure DAG only runs after finance and retail complete.
    *   **Customer Profile Changes:** Verify SCD Type 2 for `dim_customer_crm`.
    *   **Scoring Logic:** Verify scores are generated and segments assigned correctly based on the logic.
    *   **Aggregation Accuracy:** Check `crm_region_segment_dist` and `crm_segment_weekly_snapshot` for correct rollups.

---

### 3. Data Validation Queries (BigQuery SQL)

These queries are executed against the BigQuery target tables after the ETL pipeline runs to verify data quality and correctness.

**General Checks (Apply to all domains):**

1.  **Row Count Comparison (Source vs. Target Staging):**
    ```sql
    -- Example for stg_customer_profile
    SELECT COUNT(*) FROM `your-gcp-project-id.stg_customer.stg_customer_profile` WHERE load_date = CURRENT_DATE();
    -- Compare with source system extract count for the same period.
    ```

2.  **Row Count Comparison (Staging vs. DW Fact/Dimension):**
    ```sql
    -- Example for dim_customer_crm
    SELECT COUNT(*) FROM `your-gcp-project-id.dw_customer.dim_customer_crm` WHERE created_date >= CURRENT_DATE();
    -- Compare with number of new/updated records in stg_customer_profile.

    -- Example for fact_daily_sales
    SELECT COUNT(*) FROM `your-gcp-project-id.dw_retail.fact_daily_sales` WHERE load_date >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR;
    -- Compare with processed records from stg_sales_transactions.
    ```

3.  **Duplicate Check (Primary/Unique Keys):**
    ```sql
    -- Example for dim_customer_crm (natural key)
    SELECT customer_id, COUNT(*)
    FROM `your-gcp-project-id.dw_customer.dim_customer_crm`
    WHERE is_current = 'Y'
    GROUP BY customer_id
    HAVING COUNT(*) > 1;
    -- Expected: 0 rows

    -- Example for fact_gl_balances (composite key)
    SELECT DIM_ACCOUNT_KEY, DIM_PERIOD_KEY, ENTITY_CODE, PERIOD_NAME, COUNT(*)
    FROM `your-gcp-project-id.dw_finance.fact_gl_balances`
    WHERE DATE(LOAD_DATE) = CURRENT_DATE()
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1;
    -- Expected: 0 rows
    ```

4.  **Null Value Check (NOT NULL columns):**
    ```sql
    -- Example for dim_customer_crm
    SELECT COUNT(*) FROM `your-gcp-project-id.dw_customer.dim_customer_crm`
    WHERE dim_crm_customer_key IS NULL OR customer_id IS NULL OR valid_from IS NULL;
    -- Expected: 0 rows
    ```

5.  **Data Type Validation:**
    ```sql
    -- Implicitly handled by BigQuery schema, but can check for unexpected NULLs from type coercion
    SELECT COUNT(*) FROM `your-gcp-project-id.dw_retail.fact_daily_sales`
    WHERE QUANTITY IS NULL AND stg_quantity_source_was_not_null; -- Requires tracking source nullability
    ```

6.  **Referential Integrity (Foreign Keys):**
    ```sql
    -- Example: fact_daily_sales to dim_product
    SELECT COUNT(f.FACT_SALES_KEY)
    FROM `your-gcp-project-id.dw_retail.fact_daily_sales` f
    LEFT JOIN `your-gcp-project-id.dw_retail.dim_product` p ON f.DIM_PRODUCT_KEY = p.DIM_PRODUCT_KEY
    WHERE p.DIM_PRODUCT_KEY IS NULL AND f.DIM_PRODUCT_KEY IS NOT NULL;
    -- Expected: 0 rows (or acceptable number of unmatched records if business logic allows)
    ```

**SCD Type 2 Specific Checks:**

7.  **Current Record Check:**
    ```sql
    -- For any SCD Type 2 dimension (e.g., dim_customer_crm)
    SELECT customer_id, COUNT(*)
    FROM `your-gcp-project-id.dw_customer.dim_customer_crm`
    WHERE is_current = 'Y'
    GROUP BY customer_id
    HAVING COUNT(*) > 1;
    -- Expected: 0 rows (only one current record per natural key)
    ```

8.  **Version Number Increment:**
    ```sql
    -- For a specific customer_id with multiple versions
    SELECT customer_id, valid_from, valid_to, version_num
    FROM `your-gcp-project-id.dw_customer.dim_customer_crm`
    WHERE customer_id = <test_customer_id>
    ORDER BY valid_from;
    -- Expected: version_num should increment sequentially.
    ```

9.  **Date Overlap/Gap Check:**
    ```sql
    -- For any SCD Type 2 dimension
    WITH ranked_versions AS (
        SELECT
            customer_id,
            valid_from,
            valid_to,
            LEAD(valid_from, 1) OVER (PARTITION BY customer_id ORDER BY valid_from) as next_valid_from
        FROM `your-gcp-project-id.dw_customer.dim_customer_crm`
    )
    SELECT COUNT(*)
    FROM ranked_versions
    WHERE DATE_ADD(valid_to, INTERVAL 1 DAY) <> next_valid_from
      AND valid_to <> '9999-12-31'
      AND next_valid_from IS NOT NULL;
    -- Expected: 0 rows (no gaps or overlaps in validity periods)
    ```

**Transformation Logic Specific Checks:**

10. **Finance: `fact_gl_balances` Aggregation:**
    ```sql
    -- Compare sum of debits/credits in fact table with sum in staging for a given period/entity
    SELECT SUM(PERIOD_DEBITS), SUM(PERIOD_CREDITS), SUM(FUNCTIONAL_BALANCE)
    FROM `your-gcp-project-id.dw_finance.fact_gl_balances`
    WHERE PERIOD_NAME = 'JAN2024' AND ENTITY_CODE = 'ENT1';

    SELECT SUM(DEBIT_AMOUNT), SUM(CREDIT_AMOUNT), SUM(FUNCTIONAL_AMOUNT)
    FROM `your-gcp-project-id.stg_finance.stg_gl_transactions`
    WHERE PERIOD_NAME = 'JAN2024' AND ENTITY_CODE = 'ENT1';
    -- Expected: Values should match (within acceptable precision).
    ```

11. **Finance: `fact_period_reconciliation` Variance:**
    ```sql
    -- Verify variance calculation for a specific account
    SELECT GL_BALANCE, SUB_LEDGER_BALANCE, VARIANCE_AMOUNT, VARIANCE_PCT, RECON_STATUS
    FROM `your-gcp-project-id.dw_finance.fact_period_reconciliation`
    WHERE ENTITY_CODE = 'ENT1' AND PERIOD_NAME = 'JAN2024' AND ACCOUNT_CODE = '10010';
    -- Manually calculate and compare.
    ```

12. **Retail: `fact_daily_sales` Amount Calculations:**
    ```sql
    -- Verify NET_AMOUNT calculation
    SELECT
        QUANTITY, UNIT_PRICE, DISCOUNT_AMT, GROSS_AMOUNT, NET_AMOUNT,
        (QUANTITY * UNIT_PRICE) AS expected_gross,
        (QUANTITY * UNIT_PRICE - COALESCE(DISCOUNT_AMT, 0)) AS expected_net
    FROM `your-gcp-project-id.dw_retail.fact_daily_sales`
    WHERE TRANSACTION_DATE = '2024-01-15'
    LIMIT 10;
    -- Expected: GROSS_AMOUNT = expected_gross, NET_AMOUNT = expected_net
    ```

13. **Retail: `fact_regional_summary` Aggregations:**
    ```sql
    -- Verify total revenue for a region/date
    SELECT SUM(NET_AMOUNT) FROM `your-gcp-project-id.dw_retail.fact_daily_sales`
    WHERE REGION_CODE = 'EAST' AND TRANSACTION_DATE = '2024-01-15';

    SELECT TOTAL_REVENUE FROM `your-gcp-project-id.dw_retail.fact_regional_summary`
    WHERE REGION_CODE = 'EAST' AND SUMMARY_DATE = '2024-01-15';
    -- Expected: Values should match.
    ```

14. **Retail: `retail_product_rankings` Ranks:**
    ```sql
    -- Verify rankings for a specific region/year/month
    SELECT REGION_CODE, DIM_PRODUCT_KEY, TOTAL_REVENUE, REVENUE_RANK, REVENUE_DENSE_RANK
    FROM `your-gcp-project-id.dw_retail.retail_product_rankings`
    WHERE REGION_CODE = 'WEST' AND FISCAL_YEAR = 2024 AND FISCAL_MONTH = 1
    ORDER BY TOTAL_REVENUE DESC;
    -- Manually verify rank logic.
    ```

15. **Customer: `crm_micro_segments` Score Derivation:**
    ```sql
    -- Verify composite_score and micro_segment for specific customers
    SELECT
        customer_id, clv_score, churn_risk_score, engagement_score,
        composite_score, micro_segment, intervention_recommended
    FROM `your-gcp-project-id.dw_customer.crm_micro_segments`
    WHERE load_date = CURRENT_DATE()
    LIMIT 10;
    -- Manually verify calculations and derivations based on thresholds.
    ```

16. **Data Quality: `dq_results` Verification:**
    ```sql
    SELECT * FROM `your-gcp-project-id.audit_etl.dq_results`
    WHERE LOAD_DATE = CURRENT_DATE() AND STATUS = 'FAILED';
    -- Expected: Review failed rules and ensure they are correctly identified.
    ```

---

### 4. Edge Cases

These scenarios test the robustness of the migration logic under unusual or boundary conditions.

#### 4.1. Data Content Edge Cases

*   **Empty Source Data:**
    *   **Scenario:** An entire source table (e.g., `stg_gl_transactions`) is empty for a given processing run.
    *   **Expected Behavior:** The pipeline should run successfully without errors. Target tables should either remain unchanged or have no new records inserted. Aggregations should result in zero or nulls where appropriate.
*   **All Null Values:**
    *   **Scenario:** A source column expected to be numeric contains all nulls, or a string column contains all nulls.
    *   **Expected Behavior:** Numeric columns should correctly handle nulls (e.g., `SUM` should ignore, `AVG` should return null or ignore). String columns should propagate nulls or default to empty strings/specific values as per `COALESCE` logic.
*   **Zero Values:**
    *   **Scenario:** `QUANTITY` or `UNIT_PRICE` is 0 in sales. `DEBIT_AMOUNT` or `CREDIT_AMOUNT` is 0 in GL.
    *   **Expected Behavior:** Calculations like `GROSS_AMOUNT`, `NET_AMOUNT`, `FUNCTIONAL_BALANCE` should correctly result in 0. Division by zero (e.g., `VARIANCE_PCT` when `SUB_LEDGER_BALANCE` is 0) should be handled gracefully (e.g., return `NULL` or 0, not error).
*   **Negative Values:**
    *   **Scenario:** Negative `QUANTITY`, `UNIT_PRICE`, `DISCOUNT_AMT`, `DEBIT_AMOUNT`, `CREDIT_AMOUNT`.
    *   **Expected Behavior:** Calculations should correctly reflect negative values. If negative values are invalid per business rules, they should be flagged by DQ checks.
*   **Extreme Values:**
    *   **Scenario:** Very large numbers (e.g., `BIGNUMERIC` limits), very small numbers, very long strings.
    *   **Expected Behavior:** Data types should accommodate these values without overflow or truncation.
*   **Special Characters in Strings:**
    *   **Scenario:** Source string data contains special characters, emojis, or non-ASCII characters.
    *   **Expected Behavior:** BigQuery `STRING` type should handle these correctly. `standardize_string_cols` should not corrupt them (though `UPPER` might change some non-ASCII chars).
*   **Date/Time Boundaries:**
    *   **Scenario:** Transactions on `1900-01-01`, `2000-02-29` (leap year), `2024-12-31`, `2025-01-01`.
    *   **Expected Behavior:** Date/time functions (`year`, `month`, `weekofyear`, `DATE_SUB`) should work correctly across these boundaries.

#### 4.2. SCD Type 2 Specific Edge Cases

*   **First Load of a Record:**
    *   **Scenario:** A natural key appears for the first time in the source.
    *   **Expected Behavior:** A new record should be inserted with `is_current='Y'`, `valid_from` set to the effective date, `valid_to='9999-12-31'`, `version_num=1`.
*   **Update to a Non-Tracked Column:**
    *   **Scenario:** A column not included in `tracked_cols` changes for an existing natural key.
    *   **Expected Behavior:** No new version should be created. The existing `is_current='Y'` record should remain unchanged (or be updated if the column is part of the `UPDATE SET` clause, which it shouldn't be for SCD2).
*   **Update to a Tracked Column:**
    *   **Scenario:** A column included in `tracked_cols` changes for an existing natural key.
    *   **Expected Behavior:** The old record should be updated: `is_current='N'`, `valid_to` set to `effective_date - 1 day`. A new record should be inserted with `is_current='Y'`, new `valid_from`, `valid_to='9999-12-31'`, and `version_num` incremented.
*   **Multiple Updates in One Batch:**
    *   **Scenario:** The same natural key has multiple changes within a single source batch (e.g., `stg_customer_profile` has two entries for the same customer_id with different `registration_date` or `customer_segment` in the same `load_date`).
    *   **Expected Behavior:** The `prepare_scd2_source` should generate a hash for each distinct state. The `MERGE` logic should correctly identify the *latest* change and apply SCD2 based on the `valid_from` date. This might require pre-processing the source to get the latest state per natural key for the current batch. The current `scd2_merge_sql` assumes the `source_view` provides the *new* state for a natural key.
*   **Record Deleted from Source:**
    *   **Scenario:** A record that previously existed in the source (and thus in the DW) is no longer present in the current source extract.
    *   **Expected Behavior:** The design doesn't explicitly cover deletions. Common approaches:
        *   Do nothing (record remains `is_current='Y'`).
        *   Soft delete: Update `is_current='N'` and `valid_to` to `CURRENT_DATE()` for the missing record. This requires a `WHEN NOT MATCHED BY SOURCE` clause in BigQuery `MERGE`. The current `scd2_merge_sql` does not include this. This is a critical gap to address if soft deletes are required.
*   **Out-of-Order `valid_from` Dates:**
    *   **Scenario:** Source data provides updates for a record with `valid_from` dates that are not strictly increasing (e.g., a late-arriving historical correction).
    *   **Expected Behavior:** The current `scd2_merge_sql` relies on `S.valid_from` for setting `valid_to`. If `S.valid_from` is older than the current `valid_from` in the target, it could lead to incorrect `valid_to` dates or overlaps. This requires more sophisticated SCD Type 2 handling (e.g., using a temporary table to build the full history and then merging).

#### 4.3. Transformation Logic Specific Edge Cases

*   **Joins with No Matches:**
    *   **Scenario:** A fact record has a dimension key that does not exist in the corresponding dimension table (e.g., `PRODUCT_ID` in `stg_sales_transactions` not found in `dim_product`).
    *   **Expected Behavior:** The `LEFT JOIN`s in `build_fact_gl_balances` and `build_fact_daily_sales` should result in `NULL` for the dimension keys. This should be acceptable, or a DQ rule should flag these unmatched records.
*   **Dynamic SQL Generation Failures:**
    *   **Scenario:** The metadata for dynamic SQL (e.g., `scoring_feature_config`, `dq_rules`) contains malformed SQL fragments or invalid configurations.
    *   **Expected Behavior:** The Python script generating the SQL should catch these errors and fail gracefully, logging the issue.
*   **Configuration Errors:**
    *   **Scenario:** Missing or incorrect environment variables (`conf/env_retail.properties`), incorrect BigQuery project/dataset IDs.
    *   **Expected Behavior:** The pipeline should fail early with clear error messages.
*   **Concurrency Issues:**
    *   **Scenario:** Multiple instances of the same pipeline or dependent pipelines attempt to write to the same table simultaneously.
    *   **Expected Behavior:** BigQuery's transactional capabilities (for `MERGE`) and Spark's write modes (`append`, `overwrite`) should prevent data corruption. Airflow's task concurrency limits and dependencies should manage this at an orchestration level.

#### 4.4. Data Quality Rule Edge Cases

*   **DQ Rule Thresholds:**
    *   **Scenario:** A DQ rule has a threshold (e.g., `THRESHOLD_PCT`). Test data should be crafted to be exactly at, just below, and just above the threshold to ensure correct status reporting.
*   **Conflicting DQ Rules:**
    *   **Scenario:** Two DQ rules apply to the same data, one passes, one fails.
    *   **Expected Behavior:** Both results should be recorded in `dq_results`.
*   **DQ Rules with Complex SQL:**
    *   **Scenario:** A `SQL_FRAGMENT` in `dq_rules` is very complex or performs aggregations.
    *   **Expected Behavior:** `run_dq_rule` should correctly execute the SQL and return accurate counts.

---

This comprehensive set of test cases, covering unit logic, integration points, data validation, and edge conditions, will provide a robust testing strategy for the `finaltestingrepo` migration to GCP.