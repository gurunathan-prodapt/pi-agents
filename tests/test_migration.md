As a senior data-migration QA engineer, I've analyzed the `CRM_ABINITIO_TRANSFORM` migration design and the provided PySpark and Airflow code. The migration involves re-platforming two complex Ab Initio graphs (`customer_transform.xfr` and `gl_transform.xfr`) to PySpark on Dataproc Serverless, with BigQuery as the data warehouse and GCS for file outputs. Orchestration is handled by Cloud Composer.

The following test cases are designed to ensure behavioral equivalence, transformation correctness, proper external system integration, and data quality/schema integrity.

---

## Migration Validation Tests: CRM_ABINITIO_TRANSFORM

### Test Setup Prerequisites

Before running any tests, the following setup must be in place:

1.  **GCP Environment**: A GCP project with BigQuery, Dataproc Serverless, GCS, and Cloud Composer enabled.
2.  **BigQuery Datasets**:
    *   `stg_crm`
    *   `stg_finance`
    *   `crm_warehouse`
    *   `finance_warehouse`
3.  **GCS Buckets**:
    *   `gs://crm_alerts`
    *   `gs://finance_errors`
    *   `gs://your-dataproc-code-bucket` (for PySpark scripts)
4.  **PySpark Scripts Deployed**: `customer_transform_pyspark.py` and `gl_transform_pyspark.py` uploaded to `gs://your-dataproc-code-bucket/pyspark/`.
5.  **Airflow DAG Deployed**: `crm_abinitio_transform_dag.py` deployed to Cloud Composer.
6.  **Legacy Output Baseline**: For each test scenario, a corresponding "legacy output" (from the original Ab Initio job run with identical input data) must be available. This includes:
    *   Oracle table dumps (for `FACT_CUSTOMER_SCORES`, `FACT_CUSTOMER_SEGMENT_SUMMARY`, `FACT_GL_BALANCES`).
    *   Flat file contents (for `crm_at_risk_*.dat`, `finance_unmatched_gl_*.dat`).
    *   These legacy outputs will serve as the ground truth for comparison.

---

### 1. End-to-End Output Parity - `customer_transform`

*   **Purpose**: To verify that the `customer_transform_pyspark.py` job, when executed with a representative set of input data, produces identical final outputs (BigQuery tables and GCS files) as the legacy `customer_transform.xfr` Ab Initio job. This is the primary validation for behavioral equivalence.
*   **Setup**:
    1.  **Legacy Input Data**: Prepare a comprehensive set of sample data for `STG_CUSTOMER_PROFILE`, `DW_OWNER.STG_CUSTOMER_SALES`, and `STG_CAMPAIGN_EVENTS` in the legacy Oracle system. This data should cover typical scenarios, edge cases (e.g., customers with no sales, multiple campaigns, NULL values in non-key fields), and a range of values for score calculations.
    2.  **Legacy Output Baseline**: Run the legacy `customer_transform.xfr` job with the prepared input data. Extract the resulting `FACT_CUSTOMER_SCORES`, `FACT_CUSTOMER_SEGMENT_SUMMARY` from Oracle, and the `crm_at_risk_${RUN_DATE}.dat` flat file. Store these as the "legacy baseline".
    3.  **BigQuery Staging Data**: Load the *exact same* input data into the BigQuery staging tables: `stg_crm.stg_customer_profile`, `stg_crm.stg_customer_sales`, `stg_crm.stg_campaign_events`. Ensure data types and NULLability match the legacy source.
    4.  **Parameters**: Define a `RUN_DATE` (e.g., '2023-10-26') that will be used for both legacy and new system runs.
*   **Action**:
    1.  Trigger the `crm_abinitio_transform_dag` in Cloud Composer, ensuring the `customer_transform_pyspark_job` task executes with the specified `RUN_DATE`.
    2.  Wait for the job to complete successfully.
*   **Pass/Fail Criterion**:
    1.  **BigQuery `crm_warehouse.fact_customer_scores`**: The row count and all column values in the migrated BigQuery table must exactly match the legacy `FACT_CUSTOMER_SCORES` table.
    2.  **BigQuery `crm_warehouse.fact_customer_segment_summary`**: The row count and all column values in the migrated BigQuery table must exactly match the legacy `FACT_CUSTOMER_SEGMENT_SUMMARY` table.
    3.  **GCS `gs://crm_alerts/crm_at_risk_${RUN_DATE}.csv`**: The content (including row count, column order, and values) of the generated CSV file in GCS must exactly match the legacy `crm_at_risk_${RUN_DATE}.dat` flat file.
    *   **SQL Assertion Example (for `fact_customer_scores`):**
        ```sql
        -- Compare row counts
        SELECT
            (SELECT COUNT(*) FROM `your-gcp-project-id.crm_warehouse.fact_customer_scores`) AS new_count,
            (SELECT COUNT(*) FROM `legacy_baseline_dataset.fact_customer_scores_legacy`) AS legacy_count
        WHERE
            (SELECT COUNT(*) FROM `your-gcp-project-id.crm_warehouse.fact_customer_scores`) =
            (SELECT COUNT(*) FROM `legacy_baseline_dataset.fact_customer_scores_legacy`);

        -- Compare data content (example for a few columns, extend for all)
        SELECT
            COUNT(*)
        FROM (
            SELECT
                customer_id,
                clv_score,
                churn_risk_score,
                engagement_score,
                propensity_score,
                composite_score,
                segment_code,
                region_code
            FROM
                `your-gcp-project-id.crm_warehouse.fact_customer_scores`
            EXCEPT DISTINCT
            SELECT
                customer_id,
                clv_score,
                churn_risk_score,
                engagement_score,
                propensity_score,
                composite_score,
                segment_code,
                region_code
            FROM
                `legacy_baseline_dataset.fact_customer_scores_legacy`
        ) AS diff_new_vs_legacy;

        -- And vice-versa for legacy_vs_new to catch missing rows
        SELECT
            COUNT(*)
        FROM (
            SELECT
                customer_id,
                clv_score,
                churn_risk_score,
                engagement_score,
                propensity_score,
                composite_score,
                segment_code,
                region_code
            FROM
                `legacy_baseline_dataset.fact_customer_scores_legacy`
            EXCEPT DISTINCT
            SELECT
                customer_id,
                clv_score,
                churn_risk_score,
                engagement_score,
                propensity_score,
                composite_score,
                segment_code,
                region_code
            FROM
                `your-gcp-project-id.crm_warehouse.fact_customer_scores`
        ) AS diff_legacy_vs_new;
        ```
        *Pass if all `COUNT(*)` queries return 0, and initial row counts match.*
    *   **Python/Pytest Assertion Example (for GCS file):**
        ```python
        import pytest
        from google.cloud import storage

        def test_crm_at_risk_file_parity(run_date="2023-10-26"):
            bucket_name = "crm_alerts"
            gcs_file_path = f"crm_at_risk_{run_date}.csv"
            legacy_file_path = f"/path/to/legacy_baseline/crm_at_risk_{run_date}.dat" # Local path to legacy file

            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(gcs_file_path)

            assert blob.exists(), f"GCS file {gcs_file_path} does not exist."

            gcs_content = blob.download_as_text()
            with open(legacy_file_path, 'r') as f:
                legacy_content = f.read()

            # Normalize line endings and remove potential trailing newlines for robust comparison
            gcs_lines = [line.strip() for line in gcs_content.splitlines() if line.strip()]
            legacy_lines = [line.strip() for line in legacy_content.splitlines() if line.strip()]

            assert len(gcs_lines) == len(legacy_lines), \
                f"Row count mismatch: GCS has {len(gcs_lines)} rows, Legacy has {len(legacy_lines)} rows."

            # For exact content comparison, line by line
            for i, (gcs_line, legacy_line) in enumerate(zip(gcs_lines, legacy_lines)):
                assert gcs_line == legacy_line, f"Content mismatch at line {i+1}:\nGCS: {gcs_line}\nLegacy: {legacy_line}"

            print(f"GCS file {gcs_file_path} content matches legacy baseline.")
        ```

### 2. End-to-End Output Parity - `gl_transform`

*   **Purpose**: To verify that the `gl_transform_pyspark.py` job, when executed with a representative set of input data, produces identical final outputs (BigQuery tables and GCS files) as the legacy `gl_transform.xfr` Ab Initio job.
*   **Setup**:
    1.  **Legacy Input Data**: Prepare a comprehensive set of sample data for `STG_GL_TRANSACTIONS`, `DIM_ACCOUNT`, and `STG_PERIOD_RATES` in the legacy Oracle system. This data should cover typical scenarios, edge cases (e.g., unmatched accounts, various journal types, NULL amounts), and a range of values for balance calculations.
    2.  **Legacy Output Baseline**: Run the legacy `gl_transform.xfr` job with the prepared input data. Extract the resulting `FACT_GL_BALANCES` from Oracle, and the `finance_unmatched_gl_${ENTITY_CODE}_${PERIOD_NAME}.dat` flat file. Store these as the "legacy baseline".
    3.  **BigQuery Staging Data**: Load the *exact same* input data into the BigQuery staging tables: `stg_finance.stg_gl_transactions`, `stg_finance.dim_account`, `stg_finance.stg_period_rates`. Ensure data types and NULLability match the legacy source.
    4.  **Parameters**: Define a `PERIOD_NAME` (e.g., '202310') and `ENTITY_CODE` (e.g., 'GL_ENTITY_A') that will be used for both legacy and new system runs.
*   **Action**:
    1.  Trigger the `crm_abinitio_transform_dag` in Cloud Composer, ensuring the `gl_transform_pyspark_job` task executes with the specified `PERIOD_NAME` and `ENTITY_CODE`.
    2.  Wait for the job to complete successfully.
*   **Pass/Fail Criterion**:
    1.  **BigQuery `finance_warehouse.fact_gl_balances`**: The row count and all column values in the migrated BigQuery table must exactly match the legacy `FACT_GL_BALANCES` table.
    2.  **GCS `gs://finance_errors/finance_unmatched_gl_${ENTITY_CODE}_${PERIOD_NAME}.csv`**: The content (including row count, column order, and values) of the generated CSV file in GCS must exactly match the legacy `finance_unmatched_gl_${ENTITY_CODE}_${PERIOD_NAME}.dat` flat file.
    *   **SQL Assertion Example (for `fact_gl_balances`):**
        ```sql
        -- Compare row counts
        SELECT
            (SELECT COUNT(*) FROM `your-gcp-project-id.finance_warehouse.fact_gl_balances`) AS new_count,
            (SELECT COUNT(*) FROM `legacy_baseline_dataset.fact_gl_balances_legacy`) AS legacy_count
        WHERE
            (SELECT COUNT(*) FROM `your-gcp-project-id.finance_warehouse.fact_gl_balances`) =
            (SELECT COUNT(*) FROM `legacy_baseline_dataset.fact_gl_balances_legacy`);

        -- Compare data content (example for a few columns, extend for all)
        SELECT
            COUNT(*)
        FROM (
            SELECT
                account_code,
                entity_code,
                period_name,
                opening_balance,
                period_debits,
                period_credits,
                closing_balance
            FROM
                `your-gcp-project-id.finance_warehouse.fact_gl_balances`
            EXCEPT DISTINCT
            SELECT
                account_code,
                entity_code,
                period_name,
                opening_balance,
                period_debits,
                period_credits,
                closing_balance
            FROM
                `legacy_baseline_dataset.fact_gl_balances_legacy`
        ) AS diff_new_vs_legacy;

        -- And vice-versa for legacy_vs_new
        SELECT
            COUNT(*)
        FROM (
            SELECT
                account_code,
                entity_code,
                period_name,
                opening_balance,
                period_debits,
                period_credits,
                closing_balance
            FROM
                `legacy_baseline_dataset.fact_gl_balances_legacy`
            EXCEPT DISTINCT
            SELECT
                account_code,
                entity_code,
                period_name,
                opening_balance,
                period_debits,
                period_credits,
                closing_balance
            FROM
                `your-gcp-project-id.finance_warehouse.fact_gl_balances`
        ) AS diff_legacy_vs_new;
        ```
        *Pass if all `COUNT(*)` queries return 0, and initial row counts match.*
    *   **Python/Pytest Assertion Example (for GCS file):**
        ```python
        import pytest
        from google.cloud import storage

        def test_finance_unmatched_gl_file_parity(period_name="202310", entity_code="GL_ENTITY_A"):
            bucket_name = "finance_errors"
            gcs_file_path = f"finance_unmatched_gl_{entity_code}_{period_name}.csv"
            legacy_file_path = f"/path/to/legacy_baseline/finance_unmatched_gl_{entity_code}_{period_name}.dat"

            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(gcs_file_path)

            assert blob.exists(), f"GCS file {gcs_file_path} does not exist."

            gcs_content = blob.download_as_text()
            with open(legacy_file_path, 'r') as f:
                legacy_content = f.read()

            gcs_lines = [line.strip() for line in gcs_content.splitlines() if line.strip()]
            legacy_lines = [line.strip() for line in legacy_content.splitlines() if line.strip()]

            assert len(gcs_lines) == len(legacy_lines), \
                f"Row count mismatch: GCS has {len(gcs_lines)} rows, Legacy has {len(legacy_lines)} rows."

            for i, (gcs_line, legacy_line) in enumerate(zip(gcs_lines, legacy_lines)):
                assert gcs_line == legacy_line, f"Content mismatch at line {i+1}:\nGCS: {gcs_line}\nLegacy: {legacy_line}"

            print(f"GCS file {gcs_file_path} content matches legacy baseline.")
        ```

### 3. Transformation Correctness - `gl_transform`: Normalization and Journal Type Categorization

*   **Purpose**: To specifically validate the `reformat_normalise` component's logic, including `upper()`, `trim()`, `abs()`, `coalesce()`, and the `journal_type` `when().otherwise()` logic.
*   **Setup**:
    1.  **BigQuery Staging Data**: Load `stg_finance.stg_gl_transactions` with data specifically designed to test:
        *   Mixed-case `entity_code`, `account_code`, `cost_centre_code` (with leading/trailing spaces).
        *   Negative `debit_amount` and `credit_amount`.
        *   NULL `currency` and `exchange_rate`.
        *   Various `journal_type` values (e.g., 'MANUAL', 'ACCRUAL', 'REVERSAL', 'INTERCO', 'STANDARD', 'OTHER').
        *   `period_name` in 'YYYYMM' format.
    2.  **Parameters**: `PERIOD_NAME='202310'`, `ENTITY_CODE='TEST_ENT'`.
*   **Action**:
    1.  Run the `gl_transform_pyspark.py` job (can be run in isolation if the PySpark script is modular, or via the DAG).
    2.  Query the `joined_gl_df` (or an intermediate table if saved) to inspect the normalized fields.
*   **Pass/Fail Criterion**:
    *   `entity_code`, `account_code`, `cost_centre_code`: Must be uppercase and trimmed. `cost_centre_code` should default to 'DEFAULT' if NULL.
    *   `debit_amount`, `credit_amount`: Must be positive (absolute value).
    *   `currency_code`: Must be 'GBP' if original `currency` was NULL.
    *   `exchange_rate`: Must be 1.0 if original `exchange_rate` was NULL.
    *   `journal_type`: Must correctly map according to the `when().otherwise()` logic (e.g., 'MANUAL' -> 'ADJUSTING', 'REVERSAL' -> 'REVERSING', 'INTERCO' -> 'INTERCOMPANY', others -> 'STANDARD').
    *   `period_year`, `period_num`: Must be correctly derived from `period_name`.
    *   `net_amount`: Must be `debit_amount - credit_amount`.
    *   **SQL Assertion Example (against `finance_warehouse.fact_gl_balances` or an intermediate table):**
        ```sql
        -- Test journal_type mapping
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances`
        WHERE
            (original_journal_type = 'MANUAL' AND journal_type != 'ADJUSTING') OR
            (original_journal_type = 'REVERSAL' AND journal_type != 'REVERSING') OR
            (original_journal_type = 'INTERCO' AND journal_type != 'INTERCOMPANY') OR
            (original_journal_type NOT IN ('MANUAL', 'REVERSAL', 'INTERCO') AND journal_type != 'STANDARD');
        -- (Requires adding original_journal_type to the output for direct comparison, or inferring from input)

        -- Test upper/trim/abs/coalesce
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances`
        WHERE
            NOT (
                UPPER(TRIM(original_entity_code)) = entity_code AND
                ABS(original_debit_amount) = period_debits AND
                COALESCE(original_currency, 'GBP') = currency_code
                -- ... and so on for other fields
            );
        ```
        *Pass if all `COUNT(*)` queries return 0.*

### 4. Transformation Correctness - `gl_transform`: Account Dimension Join & Unmatched Records

*   **Purpose**: To validate the `join_account_dim` logic, specifically the left outer join behavior, the handling of unmatched records, and the coalescing of dimension attributes.
*   **Setup**:
    1.  **BigQuery Staging Data**:
        *   `stg_finance.stg_gl_transactions`: Include transactions with:
            *   Matching `account_code` and `entity_code` in `dim_account`.
            *   `account_code` or `entity_code` that *do not* exist in `dim_account`.
            *   NULL values for `account_code` or `entity_code` in `stg_gl_transactions`.
        *   `stg_finance.dim_account`: Include valid dimension records.
    2.  **Parameters**: `PERIOD_NAME='202310'`, `ENTITY_CODE='TEST_ENT'`.
*   **Action**:
    1.  Run the `gl_transform_pyspark.py` job.
    2.  Query `finance_warehouse.fact_gl_balances` and inspect the GCS output `gs://finance_errors/finance_unmatched_gl_TEST_ENT_202310.csv`.
*   **Pass/Fail Criterion**:
    *   Records in `fact_gl_balances` must have correctly populated `account_name`, `account_type`, `cost_centre_name`, `department_code` from `dim_account` where a match occurred.
    *   For records where `dim_account` was unmatched, `account_name` should be 'UNMAPPED', `account_type` 'UNKNOWN', `account_subtype` 'UNKNOWN', `cost_centre_name` 'DEFAULT', and `department_code` 'NONE'.
    *   The `finance_unmatched_gl_TEST_ENT_202310.csv` file in GCS must contain *only* the records from `stg_gl_transactions` that failed to find a match in `dim_account`, with `error_reason` correctly set to 'Account Dimension Mismatch'.
    *   **SQL Assertion Example (for `fact_gl_balances`):**
        ```sql
        -- Verify matched records have correct dimension attributes
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances` f
        JOIN
            `stg_finance.dim_account` d
            ON f.account_code = d.account_code AND f.entity_code = d.entity_code
        WHERE
            f.account_name != d.account_name OR
            f.account_type != d.account_type; -- ... and other dim fields

        -- Verify unmatched records have default dimension attributes
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances` f
        LEFT JOIN
            `stg_finance.dim_account` d
            ON f.account_code = d.account_code AND f.entity_code = d.entity_code
        WHERE
            d.account_code IS NULL AND
            (f.account_name != 'UNMAPPED' OR f.account_type != 'UNKNOWN' OR f.cost_centre_name != 'DEFAULT');
        ```
        *Pass if all `COUNT(*)` queries return 0.*
    *   **Python/Pytest Assertion Example (for GCS unmatched file):**
        ```python
        import pytest
        from google.cloud import storage
        import pandas as pd
        import io

        def test_finance_unmatched_gl_content(period_name="202310", entity_code="TEST_ENT"):
            bucket_name = "finance_errors"
            gcs_file_path = f"finance_unmatched_gl_{entity_code}_{period_name}.csv"
            
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(gcs_file_path)
            
            assert blob.exists(), f"GCS file {gcs_file_path} does not exist."
            
            gcs_content = blob.download_as_text()
            df_unmatched_gcs = pd.read_csv(io.StringIO(gcs_content), sep='|')
            
            # Assuming you have a way to get the expected unmatched records from your test data
            # For example, by running a SQL query against stg_gl_transactions and dim_account
            expected_unmatched_data = [
                {'gl_txn_id': 'TXN005', 'entity_code': 'TEST_ENT', 'period_name': '202310', 'account_code': 'ACC999', 'error_reason': 'Account Dimension Mismatch'},
                # ... add all expected unmatched records
            ]
            df_expected_unmatched = pd.DataFrame(expected_unmatched_data)

            # Compare dataframes (requires sorting and handling potential column order differences)
            pd.testing.assert_frame_equal(
                df_unmatched_gcs.sort_values(by=['gl_txn_id']).reset_index(drop=True),
                df_expected_unmatched.sort_values(by=['gl_txn_id']).reset_index(drop=True),
                check_dtype=False, check_like=True # check_like=True ignores column order
            )
            print("GCS unmatched GL file content is correct.")
        ```

### 5. Transformation Correctness - `gl_transform`: Rollup Balances

*   **Purpose**: To validate the `rollup_period_balances` logic, including `groupBy` keys, aggregation functions (`sum`, `count`), and the calculation of `closing_balance`.
*   **Setup**:
    1.  **BigQuery Staging Data**: Load `stg_finance.stg_gl_transactions` with multiple transactions for the same `(entity_code, period_name, account_code, cost_centre_code, currency_code)` group, including various debit/credit amounts, functional amounts, and transaction IDs. Ensure some transactions are 'STANDARD' or 'INTERCOMPANY' journal types.
    2.  **BigQuery Staging Data**: Load `stg_finance.dim_account` with matching account details for the GL transactions.
    3.  **Parameters**: `PERIOD_NAME='202310'`, `ENTITY_CODE='TEST_ENT'`.
*   **Action**:
    1.  Run the `gl_transform_pyspark.py` job.
    2.  Query `finance_warehouse.fact_gl_balances`.
*   **Pass/Fail Criterion**:
    *   Each row in `fact_gl_balances` must represent a unique combination of the `groupBy` keys.
    *   `period_debits`, `period_credits`, `net_period_amount`, `functional_balance`, `transaction_count` must be the correct sum/count of the underlying transactions for that group.
    *   `opening_balance` must be 0.0 as per the code.
    *   `closing_balance` must be `opening_balance + period_debits - period_credits`.
    *   `balance_type` must be 'ACTUAL'.
    *   **SQL Assertion Example:**
        ```sql
        -- Verify aggregation sums
        SELECT
            COUNT(*)
        FROM (
            SELECT
                entity_code,
                period_name,
                account_code,
                SUM(debit_amount) AS expected_debits,
                SUM(credit_amount) AS expected_credits,
                SUM(debit_amount - credit_amount) AS expected_net_amount,
                COUNT(gl_txn_id) AS expected_txn_count
            FROM
                `stg_finance.stg_gl_transactions` s
            JOIN
                `stg_finance.dim_account` d
                ON s.account_code = d.account_code AND s.entity_code = d.entity_code
            WHERE
                s.period_name = '202310' AND s.entity_code = 'TEST_ENT'
                AND s.journal_type IN ('STANDARD', 'INTERCO') -- Only standard journals are rolled up
            GROUP BY
                entity_code, period_name, account_code
        ) AS expected
        JOIN
            `your-gcp-project-id.finance_warehouse.fact_gl_balances` actual
            ON expected.entity_code = actual.entity_code
            AND expected.period_name = actual.period_name
            AND expected.account_code = actual.account_code
        WHERE
            expected.expected_debits != actual.period_debits OR
            expected.expected_credits != actual.period_credits OR
            expected.expected_net_amount != (actual.period_debits - actual.period_credits) OR -- Re-derive net_period_amount for comparison
            expected.expected_txn_count != actual.transaction_count;

        -- Verify closing_balance calculation
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances`
        WHERE
            closing_balance != (opening_balance + period_debits - period_credits);
        ```
        *Pass if all `COUNT(*)` queries return 0.*

### 6. Data Type & NULL Handling (General)

*   **Purpose**: To ensure that data types are correctly mapped and handled during the migration, and that NULL values propagate or are defaulted as expected, preventing data loss or corruption.
*   **Setup**:
    1.  **BigQuery Staging Data**: For both `customer_transform` and `gl_transform` inputs, create test data that includes:
        *   NULL values in various non-key columns (e.g., `description`, `debit_amount`, `credit_amount`, `currency`, `exchange_rate` for GL; `sales_amount`, `campaign_type` for CRM).
        *   Values that might cause type coercion issues (e.g., string '123.45' into a float, or a very long string into a shorter column).
        *   Boundary values for numeric types (min/max).
    2.  **Parameters**: Use standard `RUN_DATE`, `PERIOD_NAME`, `ENTITY_CODE`.
*   **Action**:
    1.  Run both `customer_transform_pyspark.py` and `gl_transform_pyspark.py` jobs.
    2.  Query the final BigQuery output tables (`fact_customer_scores`, `fact_customer_segment_summary`, `fact_gl_balances`) and GCS files.
*   **Pass/Fail Criterion**:
    *   All columns in the output tables/files must have the correct data types as defined in the BigQuery DDL.
    *   NULL values must be preserved where expected (e.g., if a source `description` is NULL and no transformation changes it, it should remain NULL in the target).
    *   Default values (e.g., `coalesce` in `gl_transform` for `currency_code`, `exchange_rate`, `account_name` for unmatched) must be applied correctly.
    *   No data truncation or unexpected type conversion errors should occur.
    *   **SQL Assertion Example (for NULL handling):**
        ```sql
        -- Example for GL: Verify currency_code defaults to 'GBP' when source currency is NULL
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances` f
        JOIN
            `stg_finance.stg_gl_transactions` s
            ON f.account_code = s.account_code AND f.entity_code = s.entity_code -- simplified join for example
        WHERE
            s.currency IS NULL AND f.currency_code != 'GBP';

        -- Example for GL: Verify account_name is 'UNMAPPED' for truly unmatched records
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances` f
        LEFT JOIN
            `stg_finance.dim_account` d
            ON f.account_code = d.account_code AND f.entity_code = d.entity_code
        WHERE
            d.account_code IS NULL AND f.account_name != 'UNMAPPED';
        ```
        *Pass if all `COUNT(*)` queries return 0.*

### 7. External System Replacement - GCS Output File Format

*   **Purpose**: To ensure that the GCS output files (`crm_at_risk_*.csv`, `finance_unmatched_gl_*.csv`) are generated with the correct format (CSV, delimiter, headers, compression if specified) and naming convention.
*   **Setup**:
    1.  **BigQuery Staging Data**: Populate staging tables with data that will result in records being written to both GCS output files.
    2.  **Parameters**: Use a specific `RUN_DATE` (e.g., '2023-10-26'), `PERIOD_NAME` ('202310'), and `ENTITY_CODE` ('TEST_ENT').
*   **Action**:
    1.  Run both `customer_transform_pyspark.py` and `gl_transform_pyspark.py` jobs.
    2.  Inspect the GCS buckets `gs://crm_alerts` and `gs://finance_errors`.
*   **Pass/Fail Criterion**:
    *   Files must exist at `gs://crm_alerts/crm_at_risk_2023-10-26.csv` and `gs://finance_errors/finance_unmatched_gl_TEST_ENT_202310.csv`.
    *   The files must be in CSV format.
    *   `crm_at_risk_*.csv` should use the default comma delimiter (as `option("delimiter", "|")` is not specified in the customer job, but it is for GL).
    *   `finance_unmatched_gl_*.csv` must use `|` as the delimiter, as specified in the code.
    *   No extra headers or footers unless explicitly configured.
    *   The file names must correctly incorporate the dynamic parameters (`RUN_DATE`, `ENTITY_CODE`, `PERIOD_NAME`).
    *   **Python/Pytest Assertion Example (for delimiter and content):**
        ```python
        import pytest
        from google.cloud import storage
        import io
        import csv

        def test_gcs_file_format_and_delimiter(period_name="202310", entity_code="TEST_ENT"):
            bucket_name = "finance_errors"
            gcs_file_path = f"finance_unmatched_gl_{entity_code}_{period_name}.csv"
            
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(gcs_file_path)
            
            assert blob.exists(), f"GCS file {gcs_file_path} does not exist."
            
            gcs_content = blob.download_as_text()
            
            # Check delimiter for finance_unmatched_gl
            reader = csv.reader(io.StringIO(gcs_content), delimiter='|')
            first_row = next(reader, None)
            assert first_row is not None, "GCS file is empty or has no rows."
            assert len(first_row) > 1, "Delimiter '|' not correctly applied, or only one column found."
            
            # Check for crm_at_risk (assuming default comma delimiter)
            crm_run_date = "2023-10-26"
            crm_bucket_name = "crm_alerts"
            crm_gcs_file_path = f"crm_at_risk_{crm_run_date}.csv"
            crm_blob = client.get_bucket(crm_bucket_name).blob(crm_gcs_file_path)
            crm_content = crm_blob.download_as_text()
            crm_reader = csv.reader(io.StringIO(crm_content), delimiter=',')
            crm_first_row = next(crm_reader, None)
            assert crm_first_row is not None, "CRM GCS file is empty or has no rows."
            assert len(crm_first_row) > 1, "Default delimiter ',' not correctly applied, or only one column found."

            print("GCS output file formats and delimiters are correct.")
        ```

### 8. Parameter Handling

*   **Purpose**: To ensure that dynamic parameters (`RUN_DATE`, `PERIOD_NAME`, `ENTITY_CODE`) are correctly passed from Airflow to the PySpark jobs and used for filtering and file naming.
*   **Setup**:
    1.  **BigQuery Staging Data**: Populate staging tables with data spanning multiple `RUN_DATE`s, `PERIOD_NAME`s, and `ENTITY_CODE`s.
    2.  **Airflow DAG**: Ensure `crm_abinitio_transform_dag.py` is configured to pass these parameters.
*   **Action**:
    1.  Manually trigger the `crm_abinitio_transform_dag` in Cloud Composer with specific `execution_date` (which influences `ds` and `ds_nodash`).
    2.  Override `DEFAULT_GL_ENTITY_CODE` in the DAG definition or via Airflow variables for specific test runs if needed.
    3.  Observe the Dataproc Serverless job logs for the PySpark script arguments.
    4.  Query the output BigQuery tables and inspect GCS files.
*   **Pass/Fail Criterion**:
    *   The `filter` conditions in PySpark (e.g., `F.col("period_name") == F.lit(period_name)`) must correctly filter the input data based on the passed parameters.
    *   The GCS output file names (`crm_at_risk_${RUN_DATE}.csv`, `finance_unmatched_gl_${ENTITY_CODE}_${PERIOD_NAME}.csv`) must accurately reflect the parameters used for that specific run.
    *   **SQL Assertion Example (for GL filtering):**
        ```sql
        -- Assuming a run with PERIOD_NAME='202310' and ENTITY_CODE='GL_ENTITY_A'
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances`
        WHERE
            period_name != '202310' OR entity_code != 'GL_ENTITY_A';
        ```
        *Pass if `COUNT(*)` returns 0.*
    *   **GCS File Naming Check**: Visually inspect GCS bucket or use `gsutil ls` to confirm file names match expected parameters.

### 9. Schema Validation (BigQuery Outputs)

*   **Purpose**: To ensure that the schema (column names, data types, NULLability) of the output BigQuery tables (`fact_customer_scores`, `fact_customer_segment_summary`, `fact_gl_balances`) matches the defined target DDL and the legacy output schema.
*   **Setup**:
    1.  **BigQuery DDL**: Have the target BigQuery DDLs for the output tables readily available.
    2.  **Legacy Schema**: Document the schema of the legacy Oracle output tables.
    3.  **BigQuery Staging Data**: Populate staging tables with minimal valid data.
*   **Action**:
    1.  Run both PySpark jobs.
    2.  Query BigQuery's `INFORMATION_SCHEMA` for the generated table schemas.
*   **Pass/Fail Criterion**:
    *   The column names, data types, and NULLability of the generated BigQuery tables must exactly match the target DDL and the legacy Oracle table schemas.
    *   **SQL Assertion Example:**
        ```sql
        -- Compare schema for fact_gl_balances
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM
            `your-gcp-project-id.finance_warehouse.INFORMATION_SCHEMA.COLUMNS`
        WHERE
            table_name = 'fact_gl_balances'
        EXCEPT DISTINCT
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM
            `legacy_baseline_dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE
            table_name = 'fact_gl_balances_legacy';

        -- And vice-versa
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM
            `legacy_baseline_dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE
            table_name = 'fact_gl_balances_legacy'
        EXCEPT DISTINCT
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM
            `your-gcp-project-id.finance_warehouse.INFORMATION_SCHEMA.COLUMNS`
        WHERE
            table_name = 'fact_gl_balances';
        ```
        *Pass if both `EXCEPT DISTINCT` queries return 0 rows.*

### 10. Row Count Validation (BigQuery Outputs)

*   **Purpose**: To ensure that the total number of records processed and outputted by the migrated jobs matches the legacy system, indicating no accidental data loss or duplication.
*   **Setup**:
    1.  **BigQuery Staging Data**: Populate staging tables with a known number of records.
    2.  **Legacy Output Baseline**: Record the exact row counts of the legacy output tables/files.
*   **Action**:
    1.  Run both PySpark jobs.
    2.  Query the row counts of the output BigQuery tables.
*   **Pass/Fail Criterion**:
    *   The row count of `crm_warehouse.fact_customer_scores` must match the legacy `FACT_CUSTOMER_SCORES`.
    *   The row count of `crm_warehouse.fact_customer_segment_summary` must match the legacy `FACT_CUSTOMER_SEGMENT_SUMMARY`.
    *   The row count of `finance_warehouse.fact_gl_balances` must match the legacy `FACT_GL_BALANCES`.
    *   The row count of records in GCS `crm_at_risk_*.csv` must match the legacy `crm_at_risk_*.dat`.
    *   The row count of records in GCS `finance_unmatched_gl_*.csv` must match the legacy `finance_unmatched_gl_*.dat`.
    *   **SQL Assertion Example:**
        ```sql
        -- For fact_customer_scores
        SELECT
            (SELECT COUNT(*) FROM `your-gcp-project-id.crm_warehouse.fact_customer_scores`) =
            (SELECT COUNT(*) FROM `legacy_baseline_dataset.fact_customer_scores_legacy`) AS row_count_match;

        -- For fact_gl_balances
        SELECT
            (SELECT COUNT(*) FROM `your-gcp-project-id.finance_warehouse.fact_gl_balances`) =
            (SELECT COUNT(*) FROM `legacy_baseline_dataset.fact_gl_balances_legacy`) AS row_count_match;
        ```
        *Pass if `row_count_match` returns `TRUE` for all comparisons.*

### 11. Edge Case: Empty Input Tables

*   **Purpose**: To verify that the PySpark jobs handle scenarios where one or more input staging tables are empty without failing or producing erroneous output.
*   **Setup**:
    1.  **BigQuery Staging Data**: Ensure one or more of the input tables (`stg_crm.stg_customer_profile`, `stg_crm.stg_customer_sales`, `stg_crm.stg_campaign_events`, `stg_finance.stg_gl_transactions`, `stg_finance.dim_account`, `stg_finance.stg_period_rates`) are completely empty. Test each combination (e.g., only `stg_gl_transactions` empty, only `dim_account` empty, all empty).
    2.  **Parameters**: Use standard `RUN_DATE`, `PERIOD_NAME`, `ENTITY_CODE`.
*   **Action**:
    1.  Run the relevant PySpark job (or the full DAG).
    2.  Observe job logs for errors.
    3.  Query the output BigQuery tables and inspect GCS files.
*   **Pass/Fail Criterion**:
    *   The PySpark job must complete successfully (not fail).
    *   Output BigQuery tables should either be empty or contain only records derived from non-empty inputs, as expected by the transformation logic (e.g., if `stg_gl_transactions` is empty, `fact_gl_balances` should be empty).
    *   GCS output files should either be empty or not generated, or contain only headers if that's the default behavior for empty dataframes.
    *   **SQL Assertion Example:**
        ```sql
        -- If stg_gl_transactions was empty
        SELECT COUNT(*) FROM `your-gcp-project-id.finance_warehouse.fact_gl_balances`;
        ```
        *Pass if `COUNT(*)` returns 0.*

### 12. Edge Case: All NULLs in Critical Fields

*   **Purpose**: To test the robustness of the transformation logic when critical fields (e.g., join keys, aggregation fields, calculation inputs) contain only NULL values.
*   **Setup**:
    1.  **BigQuery Staging Data**: Populate staging tables such that critical fields (e.g., `customer_id`, `account_code`, `debit_amount`, `churn_risk_score`) are exclusively NULL for a subset of records or all records.
    2.  **Parameters**: Use standard `RUN_DATE`, `PERIOD_NAME`, `ENTITY_CODE`.
*   **Action**:
    1.  Run the relevant PySpark job.
    2.  Query the output BigQuery tables and inspect GCS files.
*   **Pass/Fail Criterion**:
    *   The job must complete successfully.
    *   Join operations should correctly handle NULL keys (typically resulting in no match for inner joins, or NULLs on the non-matching side for outer joins).
    *   Aggregations on NULLs should result in NULL or 0 depending on the function (e.g., `SUM(NULL)` is NULL, `COUNT(NULL)` is 0).
    *   Calculations involving NULLs should propagate NULLs or be handled by `coalesce` as designed.
    *   **SQL Assertion Example (for GL rollup with NULL amounts):**
        ```sql
        -- Assuming stg_gl_transactions has records with NULL debit_amount and credit_amount
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project-id.finance_warehouse.fact_gl_balances`
        WHERE
            period_debits IS NOT NULL OR period_credits IS NOT NULL
            AND (SELECT COUNT(*) FROM `stg_finance.stg_gl_transactions` WHERE debit_amount IS NOT NULL AND credit_amount IS NOT NULL) = 0;
        ```
        *Pass if `COUNT(*)` returns 0 (meaning if all inputs were NULL, sums are also NULL).*

---

These tests provide a comprehensive framework for validating the `CRM_ABINITIO_TRANSFORM` migration. The emphasis on comparing against a legacy baseline, detailed transformation checks, and edge case handling ensures a high degree of confidence in the migrated solution's behavioral equivalence and correctness.