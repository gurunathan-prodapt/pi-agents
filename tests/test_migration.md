The migration of `k_ausd_bp_ta_msisdn.ksh` to Google Cloud Platform (GCP) involves a significant shift in technology stack and architectural patterns. The following test cases are designed to validate the behavioral equivalence of the migrated solution, focusing on the orchestration, parameter handling, date logic, and data processing aspects. Due to the unknown content of `d_ausd_bp_ta_msisdn.sql`, several tests for transformation correctness are presented as templates, highlighting what needs to be verified once the SQL logic is fully understood.

---

## Migration Validation Tests for `dag_k_ausd_bp_ta_msisdn`

### 1. Parameter Validation - Happy Path

*   **Purpose**: Verify that the `validate_parameters_func` correctly parses and validates all expected input parameters when valid values are provided, and pushes them to XCom.
*   **Setup**:
    1.  Create a mock Airflow `TaskInstance` object (`ti`) to simulate XCom interactions.
    2.  Prepare a `dag_run.conf` dictionary with valid parameters.
*   **Action**:
    1.  Call `utils.validate_parameters_func` with the mocked `ti` and `dag_run.conf`.
*   **Pass/Fail Criterion**:
    *   No `AirflowException` is raised.
    *   The mocked `ti.xcom_push` method is called with the correct keys (`job_name`, `reference_date_str`, `source_system`, `log_level`) and their corresponding valid values.

```python
# pytest test for utils.py
import pytest
from unittest.mock import MagicMock
from airflow.exceptions import AirflowException
import datetime
from dags import utils # Assuming utils.py is importable from 'dags'

def test_validate_parameters_func_happy_path():
    mock_ti = MagicMock()
    test_params = {
        'j': 'MSISDN_PROCESSING_JOB',
        'f': '20231026',
        's': 'SOURCE_A',
        'l': 'DEBUG'
    }
    mock_dag_run = MagicMock()
    mock_dag_run.conf = test_params

    kwargs = {'ti': mock_ti, 'dag_run': mock_dag_run}

    try:
        utils.validate_parameters_func(**kwargs)
    except AirflowException as e:
        pytest.fail(f"validate_parameters_func raised an unexpected exception: {e}")

    mock_ti.xcom_push.assert_any_call(key='job_name', value=test_params['j'])
    mock_ti.xcom_push.assert_any_call(key='reference_date_str', value=test_params['f'])
    mock_ti.xcom_push.assert_any_call(key='source_system', value=test_params['s'])
    mock_ti.xcom_push.assert_any_call(key='log_level', value=test_params['l'])
    print("Test Passed: validate_parameters_func handled valid parameters correctly.")

```

### 2. Parameter Validation - Missing Required Parameter

*   **Purpose**: Verify that `validate_parameters_func` raises an `AirflowException` when a required parameter is missing, mimicking the `pruefeParameterGesetzt` logic.
*   **Setup**:
    1.  Create a mock Airflow `TaskInstance` object (`ti`).
    2.  Prepare a `dag_run.conf` dictionary with one required parameter (e.g., `j`) omitted.
*   **Action**:
    1.  Call `utils.validate_parameters_func` with the mocked `ti` and `dag_run.conf`.
*   **Pass/Fail Criterion**:
    *   An `AirflowException` is raised.
    *   The exception message clearly indicates that a required parameter is missing.

```python
# pytest test for utils.py
import pytest
from unittest.mock import MagicMock
from airflow.exceptions import AirflowException
from dags import utils

def test_validate_parameters_func_missing_param():
    mock_ti = MagicMock()
    test_params = {
        'f': '20231026',
        's': 'SOURCE_A',
        'l': 'DEBUG'
    } # 'j' is missing
    mock_dag_run = MagicMock()
    mock_dag_run.conf = test_params

    kwargs = {'ti': mock_ti, 'dag_run': mock_dag_run}

    with pytest.raises(AirflowException) as excinfo:
        utils.validate_parameters_func(**kwargs)

    assert "Missing one or more required parameters (j, f, s, l)." in str(excinfo.value)
    print("Test Passed: validate_parameters_func correctly identified missing parameter.")

```

### 3. Parameter Validation - Invalid Date Format

*   **Purpose**: Verify that `validate_parameters_func` correctly validates the date format (`YYYYMMDD`) and raises an `AirflowException` for invalid formats, mimicking `DWDate_Datum_Check`.
*   **Setup**:
    1.  Create a mock Airflow `TaskInstance` object (`ti`).
    2.  Prepare a `dag_run.conf` dictionary with an invalid `f` (reference date) parameter (e.g., `2023-10-26` or `26102023`).
*   **Action**:
    1.  Call `utils.validate_parameters_func` with the mocked `ti` and `dag_run.conf`.
*   **Pass/Fail Criterion**:
    *   An `AirflowException` is raised.
    *   The exception message clearly indicates an invalid date format for `f`.

```python
# pytest test for utils.py
import pytest
from unittest.mock import MagicMock
from airflow.exceptions import AirflowException
from dags import utils

def test_validate_parameters_func_invalid_date_format():
    mock_ti = MagicMock()
    test_params = {
        'j': 'MSISDN_PROCESSING_JOB',
        'f': '2023-10-26', # Invalid format, expected YYYYMMDD
        's': 'SOURCE_A',
        'l': 'DEBUG'
    }
    mock_dag_run = MagicMock()
    mock_dag_run.conf = test_params

    kwargs = {'ti': mock_ti, 'dag_run': mock_dag_run}

    with pytest.raises(AirflowException) as excinfo:
        utils.validate_parameters_func(**kwargs)

    assert "Invalid reference date format for 'f': 2023-10-26. Expected YYYYMMDD." in str(excinfo.value)
    print("Test Passed: validate_parameters_func correctly identified invalid date format.")

```

### 4. Date Derivation - Correctness

*   **Purpose**: Verify that `derive_dates_func` correctly calculates `today_date` and `yesterday_date` based on the `reference_date` pulled from XCom, replacing `gestern.ksh` functionality.
*   **Setup**:
    1.  Create a mock Airflow `TaskInstance` object (`ti`).
    2.  Configure `ti.xcom_pull` to return a specific `reference_date_str` (e.g., `20231026`).
*   **Action**:
    1.  Call `utils.derive_dates_func` with the mocked `ti`.
*   **Pass/Fail Criterion**:
    *   No `AirflowException` is raised.
    *   The mocked `ti.xcom_push` method is called with `today_date` as the `reference_date_str` and `yesterday_date` as the day before the `reference_date_str`, both in `YYYYMMDD` format.

```python
# pytest test for utils.py
import pytest
from unittest.mock import MagicMock
from airflow.exceptions import AirflowException
from dags import utils
import datetime

def test_derive_dates_func_correctness():
    mock_ti = MagicMock()
    reference_date_str = '20231026'
    mock_ti.xcom_pull.return_value = reference_date_str # Simulates pulling from 'validate_parameters'

    kwargs = {'ti': mock_ti}

    try:
        utils.derive_dates_func(**kwargs)
    except AirflowException as e:
        pytest.fail(f"derive_dates_func raised an unexpected exception: {e}")

    expected_today = '20231026'
    expected_yesterday = '20231025'

    mock_ti.xcom_pull.assert_called_with(task_ids='validate_parameters', key='reference_date_str')
    mock_ti.xcom_push.assert_any_call(key='today_date', value=expected_today)
    mock_ti.xcom_push.assert_any_call(key='yesterday_date', value=expected_yesterday)
    print("Test Passed: derive_dates_func calculated dates correctly.")

```

### 5. End-to-End DAG Execution - Happy Path

*   **Purpose**: Verify that the entire Airflow DAG runs successfully with valid inputs, orchestrating all tasks as designed, from parameter validation to record count capture.
*   **Setup**:
    1.  Deploy the `dag_k_ausd_bp_ta_msisdn.py` DAG to a Cloud Composer environment.
    2.  Ensure the `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt` table exists in BigQuery with the expected schema (as defined in `sql/pool_basisprodukt_ddl.sql`).
    3.  Ensure `sql/d_ausd_bp_ta_msisdn_bq.sql` contains valid BigQuery SQL that performs an operation (e.g., `INSERT` or `MERGE`) into `PoolBasisprodukt` and can be executed without errors. For this test, it can be a simple `SELECT 1` or an `INSERT` of a few dummy rows.
    4.  Provide valid `dag_run.conf` parameters for the DAG trigger (e.g., `{"j": "E2E_TEST_JOB", "f": "20231027", "s": "E2E_SRC", "l": "INFO"}`).
*   **Action**:
    1.  Trigger the Airflow DAG manually via the Airflow UI or `gcloud composer environments run` command with the specified `dag_run.conf`.
*   **Pass/Fail Criterion**:
    *   The DAG run completes successfully (all tasks show a "success" status in the Airflow UI).
    *   No Airflow or BigQuery errors are reported in the task logs.
    *   The `capture_record_count` task reports a non-negative record count (if the SQL inserted data).

### 6. BigQuery SQL Transformation - Output Parity (Placeholder)

*   **Purpose**: Prove that the migrated `d_ausd_bp_ta_msisdn_bq.sql` produces the exact same output data in BigQuery as the legacy `d_ausd_bp_ta_msisdn.sql` did in Oracle for a given set of identical inputs. This is the most critical "transformation correctness" test.
*   **Setup**:
    1.  **Legacy Baseline**:
        *   Identify a specific `p_Stichtag` (e.g., `20231020`).
        *   Ensure the Oracle source tables that `d_ausd_bp_ta_msisdn.sql` reads from are in a known, controlled state for this `p_Stichtag`.
        *   Run `k_ausd_bp_ta_msisdn.ksh` with this `p_Stichtag`.
        *   After execution, extract the data from the Oracle `PoolBasisprodukt` table (or the final output table) for `p_Stichtag`. Export this data to a canonical format (e.g., CSV, Parquet). This is your "legacy golden dataset."
    2.  **Migrated Execution**:
        *   Ingest the *exact same* input data from the Oracle source tables into the corresponding BigQuery source tables. Ensure data types and values are preserved.
        *   Trigger the `dag_k_ausd_bp_ta_msisdn` DAG with `dag_run.conf` parameters including `f: 20231020`.
*   **Action**:
    1.  After the migrated DAG completes, query the `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt` table in BigQuery for `YOUR_DATE_COLUMN = '2023-10-20'`.
    2.  Export this BigQuery data to the same canonical format.
    3.  Compare the exported BigQuery data with the "legacy golden dataset."
*   **Pass/Fail Criterion**:
    *   The two datasets (legacy Oracle output vs. migrated BigQuery output) are identical in terms of row count, column names, data types, and values. This comparison should be robust to order differences (e.g., sort both datasets by primary key before comparison).

```python
# Example Python (pytest) comparison script (after data export)
import pandas as pd
import os

def test_output_parity_with_legacy_data():
    legacy_output_path = "path/to/legacy_poolbasisprodukt_20231020.csv"
    migrated_output_path = "path/to/migrated_poolbasisprodukt_20231020.csv"

    if not os.path.exists(legacy_output_path):
        pytest.skip(f"Legacy output file not found: {legacy_output_path}")
    if not os.path.exists(migrated_output_path):
        pytest.fail(f"Migrated output file not found: {migrated_output_path}. Ensure DAG ran and data was exported.")

    legacy_df = pd.read_csv(legacy_output_path, dtype=str) # Read as string to avoid type conversion issues initially
    migrated_df = pd.read_csv(migrated_output_path, dtype=str)

    # Define columns to sort by for consistent comparison (e.g., primary keys)
    # This assumes MSISDN and ACTIVATION_DATE form a unique key or are sufficient for ordering
    sort_cols = ['MSISDN', 'ACTIVATION_DATE']
    
    # Ensure all expected columns are present in both dataframes
    assert set(legacy_df.columns) == set(migrated_df.columns), "Column sets do not match"

    # Sort and reset index for robust comparison
    legacy_df_sorted = legacy_df.sort_values(by=sort_cols).reset_index(drop=True)
    migrated_df_sorted = migrated_df.sort_values(by=sort_cols).reset_index(drop=True)

    # Perform a deep comparison
    try:
        pd.testing.assert_frame_equal(legacy_df_sorted, migrated_df_sorted,
                                      check_dtype=False, # Data types might differ slightly between Oracle/BQ exports
                                      check_exact=False, # Allow for floating point inaccuracies if applicable
                                      atol=1e-6) # Absolute tolerance for numeric comparisons
        print("Test Passed: Migrated BigQuery output is identical to legacy Oracle output.")
    except AssertionError as e:
        pytest.fail(f"Test Failed: Dataframes are not equal. Differences: {e}")

```

### 7. BigQuery SQL Transformation - Specific Logic (Placeholder)

*   **Purpose**: Verify specific transformation logic (joins, filters, aggregations, type handling, NULL handling, edge cases) within `d_ausd_bp_ta_msisdn_bq.sql`. This requires detailed knowledge of the original SQL.
*   **Setup**:
    1.  Create a small, controlled dataset in BigQuery source tables that specifically targets a particular transformation rule (e.g., a row that should be filtered out, a NULL value that should be handled, a specific join condition, a specific aggregation scenario).
    2.  Trigger the `dag_k_ausd_bp_ta_msisdn` DAG with this test data and a unique `reference_date_str` (e.g., `20231028`).
*   **Action**:
    1.  After the DAG completes, query the `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt` table (or intermediate tables if the SQL creates them) for the `reference_date_str`.
    2.  Assert the expected outcome for the test data.
*   **Pass/Fail Criterion**:
    *   The data in `PoolBasisprodukt` (or intermediate tables) for the test `reference_date_str` matches the expected result based on the specific transformation rule being tested.

```sql
-- Example SQL assertions (to be run against BigQuery after DAG execution)

-- Test Case 7.1: Verify Filtering Logic (e.g., only 'ACTIVE' status records are processed)
-- Assuming the original SQL filters for status = 'ACTIVE'
SELECT COUNT(*)
FROM `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt`
WHERE YOUR_DATE_COLUMN = PARSE_DATE('%Y%m%d', '20231028')
  AND STATUS != 'ACTIVE';
-- Expected Result: 0 (if only active records should be present)

-- Test Case 7.2: Verify NULL Handling (e.g., MSISDNs that are NULL in source are excluded or defaulted)
-- Assuming MSISDN should never be NULL in the target table
SELECT COUNT(*)
FROM `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt`
WHERE YOUR_DATE_COLUMN = PARSE_DATE('%Y%m%d', '20231028')
  AND MSISDN IS NULL;
-- Expected Result: 0

-- Test Case 7.3: Verify Aggregation/Deduplication Logic (if the SQL performs such operations)
-- Assuming the SQL ensures unique MSISDNs per product code for a given date
SELECT PRODUCT_CODE, COUNT(MSISDN) AS total_msisdns, COUNT(DISTINCT MSISDN) AS distinct_msisdns
FROM `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt`
WHERE YOUR_DATE_COLUMN = PARSE_DATE('%Y%m%d', '20231028')
GROUP BY PRODUCT_CODE;
-- Expected Result: For each PRODUCT_CODE, total_msisdns should equal distinct_msisdns
-- (if the logic is to ensure uniqueness)

-- Test Case 7.4: Verify Data Type Conversion (e.g., a numeric field from Oracle is correctly stored as INT64/NUMERIC in BQ)
SELECT
    MSISDN,
    PRODUCT_CODE,
    ACTIVATION_DATE, -- Should be DATE type
    LAST_UPDATE_TIMESTAMP -- Should be TIMESTAMP type
FROM `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt`
WHERE YOUR_DATE_COLUMN = PARSE_DATE('%Y%m%d', '20231028')
LIMIT 1;
-- Manual inspection of data types and values to ensure correct conversion.
```

### 8. Record Count Capture - Correctness

*   **Purpose**: Verify that `capture_record_count_func` accurately retrieves the number of records processed/inserted into `PoolBasisprodukt` for the given `reference_date`, replacing the legacy `$tmpFile` mechanism.
*   **Setup**:
    1.  Ensure `sql/d_ausd_bp_ta_msisdn_bq.sql` is configured to insert a known number of records (e.g., 100) for a specific `reference_date_str` (e.g., `20231029`).
    2.  Create a mock Airflow `TaskInstance` object (`ti`).
    3.  Configure `ti.xcom_pull` to return `20231029` for `reference_date_str`.
    4.  Mock `google.cloud.bigquery.Client` and its `query().result()` method to return a predefined count (e.g., `[(100,)]`) for the expected count query.
*   **Action**:
    1.  Call `utils.capture_record_count_func` with the mocked `ti` and `dag` (for `project_id`).
*   **Pass/Fail Criterion**:
    *   No `AirflowException` is raised.
    *   The mocked `ti.xcom_push` for `processed_record_count` contains the expected number of records (e.g., 100).

```python
# pytest test for utils.py
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from dags import utils

@patch('google.cloud.bigquery.Client')
def test_capture_record_count_func_correctness(mock_bigquery_client):
    mock_ti = MagicMock()
    mock_dag = MagicMock()
    mock_dag.default_args = {'project_id': 'test-gcp-project'}
    reference_date_str = '20231029'
    expected_record_count = 123

    mock_ti.xcom_pull.return_value = reference_date_str

    # Configure the mock BigQuery client to return a specific count
    mock_client_instance = mock_bigquery_client.return_value
    mock_query_job = MagicMock()
    mock_client_instance.query.return_value = mock_query_job
    mock_query_job.result.return_value = iter([(expected_record_count,)]) # Simulate query result

    kwargs = {'ti': mock_ti, 'dag': mock_dag}

    try:
        utils.capture_record_count_func(**kwargs)
    except AirflowException as e:
        pytest.fail(f"capture_record_count_func raised an unexpected exception: {e}")

    mock_ti.xcom_pull.assert_called_with(task_ids='validate_parameters', key='reference_date_str')
    mock_ti.xcom_push.assert_called_with(key='processed_record_count', value=expected_record_count)
    mock_client_instance.query.assert_called_once() # Verify query was made
    print("Test Passed: capture_record_count_func retrieved correct record count.")

```

### 9. Data Quality - Schema Assertion

*   **Purpose**: Verify that the `PoolBasisprodukt` table in BigQuery adheres to the expected schema (column names, data types, nullability) as derived from the Oracle source and optimized for BigQuery.
*   **Setup**:
    1.  Ensure the `sql/pool_basisprodukt_ddl.sql` has been executed to create the table.
    2.  Define the expected schema for `PoolBasisprodukt` in a structured format (e.g., a Python dictionary or list of tuples).
*   **Action**:
    1.  Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `PoolBasisprodukt` table.
    2.  Compare the retrieved schema with the predefined expected schema.
*   **Pass/Fail Criterion**:
    *   The queried schema (column names, data types, and nullability) exactly matches the expected schema definition.

```sql
-- SQL assertion to retrieve BigQuery schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your-gcp-project-id.your_bigquery_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'PoolBasisprodukt'
ORDER BY
    ordinal_position;

-- Expected Schema (example, based on DDL placeholder):
-- [
--     ('MSISDN', 'STRING', 'NO'),
--     ('SUBSCRIPTION_ID', 'STRING', 'YES'),
--     ('PRODUCT_CODE', 'STRING', 'YES'),
--     ('ACTIVATION_DATE', 'DATE', 'YES'),
--     ('STATUS', 'STRING', 'YES'),
--     ('LAST_UPDATE_TIMESTAMP', 'TIMESTAMP', 'YES'),
--     ('YOUR_DATE_COLUMN', 'DATE', 'NO')
-- ]
```

### 10. Data Quality - Row Count Assertion

*   **Purpose**: Verify that the total number of rows in `PoolBasisprodukt` for a given `reference_date` in BigQuery matches the expected count (e.g., from the legacy system or source system).
*   **Setup**:
    1.  Establish a baseline row count from the legacy Oracle `PoolBasisprodukt` table for a specific `p_Stichtag` (e.g., `20231020`).
    2.  Run the migrated DAG with the corresponding `reference_date_str` (`20231020`), ensuring the `d_ausd_bp_ta_msisdn_bq.sql` has processed data for this date.
*   **Action**:
    1.  Query the `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt` table in BigQuery for `YOUR_DATE_COLUMN = '2023-10-20'`.
*   **Pass/Fail Criterion**:
    *   The `COUNT(*)` from BigQuery for the specified date exactly matches the baseline count obtained from Oracle.

```sql
-- SQL assertion to check row count in BigQuery
SELECT COUNT(*)
FROM `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt`
WHERE YOUR_DATE_COLUMN = PARSE_DATE('%Y%m%d', '20231020');

-- Expected Result: [Baseline count from Oracle for 20231020]
```

### 11. Error Handling - DAG Failure

*   **Purpose**: Verify that the Airflow DAG correctly handles task failures and triggers the specified error handling mechanisms (e.g., `on_failure_callback` if configured). This replaces `f_alis_msgerr.ksh` for critical errors.
*   **Setup**:
    1.  Modify `sql/d_ausd_bp_ta_msisdn_bq.sql` to intentionally cause a BigQuery error (e.g., `SELECT 1/0;` or referencing a non-existent table).
    2.  Ensure `on_failure_callback=utils.custom_error_handler` is uncommented and correctly configured in the DAG's `default_args`.
    3.  Ensure `utils.custom_error_handler` logs or sends a notification.
*   **Action**:
    1.  Trigger the DAG with valid parameters.
*   **Pass/Fail Criterion**:
    *   The `execute_bigquery_sql` task fails.
    *   The DAG run is marked as "failed" in the Airflow UI.
    *   The `custom_error_handler` function is invoked, and its expected actions (e.g., specific log messages, notification attempts) are observed in the Airflow logs or external systems.

### 12. External System Replacement - Oracle to BigQuery Data Ingestion (Implicit)

*   **Purpose**: While the DAG itself doesn't perform the initial Oracle to BigQuery ingestion, it relies on this data being available and correct. This test ensures the source data for the DAG is correctly loaded into BigQuery.
*   **Setup**:
    1.  Identify all source tables in Oracle that `d_ausd_bp_ta_msisdn.sql` reads from.
    2.  Set up the data ingestion pipeline (e.g., Dataflow, Fivetran, custom script) to bring this data into BigQuery.
    3.  Load a known, representative dataset from Oracle into the BigQuery source tables.
*   **Action**:
    1.  Perform data validation checks between the Oracle source tables and their BigQuery counterparts. This can include:
        *   **Row Count Comparison**: `SELECT COUNT(*) FROM Oracle_Table` vs. `SELECT COUNT(*) FROM BigQuery_Table`.
        *   **Schema Comparison**: Verify column names, data types, and nullability match or are appropriately mapped.
        *   **Data Sample Comparison**: Select a random sample of rows from both systems and compare values.
        *   **Checksum/Hash Comparison**: If feasible, calculate checksums of entire tables or specific columns.
*   **Pass/Fail Criterion**:
    *   Row counts for all relevant source tables match between Oracle and BigQuery (within acceptable delta if streaming).
    *   Schema definitions are compatible, and data types are correctly mapped.
    *   Sample data comparisons show no discrepancies, indicating accurate data transfer.