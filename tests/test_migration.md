As a senior data-migration QA engineer, I've analyzed the provided design document for the `DW.BERT_AUSD_V_TA_P_VERTRAG` job. The migration involves re-platforming a UC4-orchestrated KSH script to an Airflow DAG running a PySpark application on Dataproc, with BigQuery as the target data platform.

A significant challenge highlighted in the design is the lack of detailed information about the `r_ausd_v_ta_p_vertrag.ksh` script's internal logic and the functionality of UC4 includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`). Therefore, the tests below will provide a robust framework and concrete examples, but specific table names, column names, and transformation logic placeholders will need to be replaced once the KSH script has been reverse-engineered.

The core assumption is that the PySpark script will read data from BigQuery (migrated from legacy Oracle sources), perform transformations, and update a BigQuery target table.

---

## Migration Validation Tests for DW.BERT_AUSD_V_TA_P_VERTRAG

### Test Environment Setup (Pre-requisite for all tests)

Before running any tests, ensure the following:

1.  **Legacy Environment**:
    *   Access to the legacy UC4 system and the `r_ausd_v_ta_p_vertrag.ksh` script.
    *   Access to the Oracle database(s) that the KSH script reads from and writes to.
    *   Tools to extract data from Oracle (e.g., SQL Developer, `sqlplus`, `expdp`).
    *   Ability to run the legacy UC4 job on demand.
2.  **Migrated Environment**:
    *   A functional Airflow Composer environment with the `dw_bert_ausd_v_ta_p_vertrag` DAG deployed.
    *   A Dataproc cluster configured and accessible by Airflow.
    *   The `r_ausd_v_ta_p_vertrag.py` PySpark script uploaded to the specified GCS bucket.
    *   A GCP project with BigQuery datasets and tables for source and target data.
    *   Appropriate IAM permissions for Airflow, Dataproc, and BigQuery.
    *   Tools to query BigQuery (e.g., `bq` CLI, BigQuery UI, Python client).
3.  **Data Synchronization**:
    *   A mechanism to snapshot legacy Oracle source tables and load them into corresponding BigQuery source tables for testing. This is crucial for ensuring identical inputs.
    *   A mechanism to snapshot legacy Oracle target tables for comparison.

---

### 1. Output Parity: End-to-End Data Comparison

**Purpose**: To verify that the migrated PySpark job, when executed with identical input data, produces an output in BigQuery that is byte-for-byte equivalent to the output produced by the legacy KSH script in Oracle. This is the most critical test for behavioral equivalence.

**Setup**:
1.  Identify all source tables (`LEGACY_SOURCE_TABLE_1`, `LEGACY_SOURCE_TABLE_2`, etc.) and the primary target table (`LEGACY_TARGET_TABLE`) that `r_ausd_v_ta_p_vertrag.ksh` interacts with.
2.  Create a dedicated test dataset in BigQuery (e.g., `test_dataset_parity`).
3.  **Snapshot Legacy Data**: Before running *any* job, capture the state of all relevant legacy Oracle source tables and the target table.
    *   Export `LEGACY_SOURCE_TABLE_1`, `LEGACY_SOURCE_TABLE_2`, etc., to CSV/Parquet files.
    *   Export `LEGACY_TARGET_TABLE` (pre-job state) to a CSV/Parquet file.
4.  **Load Migrated Source Data**: Load the exported legacy source data into corresponding BigQuery tables (e.g., `GCP_PROJECT.test_dataset_parity.migrated_source_table_1`, `GCP_PROJECT.test_dataset_parity.migrated_source_table_2`).
5.  **Load Migrated Target Initial State**: Load the exported `LEGACY_TARGET_TABLE` (pre-job state) into `GCP_PROJECT.test_dataset_parity.migrated_target_table`. This ensures the migrated job starts with the same target data as the legacy job.
6.  **Configure PySpark**: Ensure the `r_ausd_v_ta_p_vertrag.py` script is configured to read from `GCP_PROJECT.test_dataset_parity.migrated_source_table_X` and write/update `GCP_PROJECT.test_dataset_parity.migrated_target_table`.

**Action**:
1.  **Run Legacy Job**: Execute the `DW.BERT_AUSD_V_TA_P_VERTRAG` job in the legacy UC4 environment.
2.  **Capture Legacy Output**: After the legacy job completes, export the final state of `LEGACY_TARGET_TABLE` to a CSV/Parquet file (e.g., `legacy_target_output.csv`).
3.  **Run Migrated Job**: Trigger the `dw_bert_ausd_v_ta_p_vertrag` Airflow DAG.
4.  **Capture Migrated Output**: After the Airflow DAG completes, export the final state of `GCP_PROJECT.test_dataset_parity.migrated_target_table` to a CSV/Parquet file (e.g., `migrated_target_output.csv`).
5.  **Compare Outputs**: Use a data comparison tool or script to compare `legacy_target_output.csv` and `migrated_target_output.csv`.

**Pass/Fail Criterion**:
The two output files (`legacy_target_output.csv` and `migrated_target_output.csv`) must be identical in terms of content (all rows and columns, including data types and NULLs). Any difference (missing rows, extra rows, differing values, differing data types, NULL vs. empty string) constitutes a failure.

**Example Python (Pytest) Comparison (assuming CSVs are loaded into Pandas DataFrames):**

```python
import pandas as pd
import pytest

def test_output_parity_full_table():
    legacy_output_path = "path/to/legacy_target_output.csv"
    migrated_output_path = "path/to/migrated_target_output.csv"

    try:
        df_legacy = pd.read_csv(legacy_output_path, dtype=str, keep_default_na=True)
        df_migrated = pd.read_csv(migrated_output_path, dtype=str, keep_default_na=True)
    except FileNotFoundError as e:
        pytest.fail(f"Output file not found: {e}")
    except Exception as e:
        pytest.fail(f"Error reading CSV files: {e}")

    # Sort DataFrames to ensure row order doesn't affect comparison
    # Identify primary key columns for the target table
    pk_columns = ['contract_id', 'version_id'] # Placeholder: Replace with actual PK columns
    if not all(col in df_legacy.columns for col in pk_columns):
        pytest.fail(f"Primary key columns {pk_columns} not found in legacy output.")
    if not all(col in df_migrated.columns for col in pk_columns):
        pytest.fail(f"Primary key columns {pk_columns} not found in migrated output.")

    df_legacy_sorted = df_legacy.sort_values(by=pk_columns).reset_index(drop=True)
    df_migrated_sorted = df_migrated.sort_values(by=pk_columns).reset_index(drop=True)

    # Compare shapes (rows, columns)
    assert df_legacy_sorted.shape == df_migrated_sorted.shape, \
        f"Shape mismatch: Legacy {df_legacy_sorted.shape}, Migrated {df_migrated_sorted.shape}"

    # Compare column names and order
    assert list(df_legacy_sorted.columns) == list(df_migrated_sorted.columns), \
        "Column names or order mismatch."

    # Compare content
    pd.testing.assert_frame_equal(
        df_legacy_sorted,
        df_migrated_sorted,
        check_dtype=True,
        check_exact=True, # For exact value comparison
        check_less_precise=False,
        check_like=True # Allow columns to be in different order if not explicitly sorted
    )
    print("Full table output parity test passed.")
```

---

### 2. Output Parity: Incremental Data Comparison

**Purpose**: To verify that the migrated job correctly handles incremental updates or inserts, producing the same *changes* to the target table as the legacy job. This is particularly useful if the job is designed for incremental processing.

**Setup**:
1.  Same as Test 1, but focus on a scenario where only a subset of data is expected to change or new data is added.
2.  Prepare source data such that it includes new records or updates to existing records that the job should process.
3.  Ensure the target table has a known initial state.

**Action**:
1.  **Run Legacy Job**: Execute the `DW.BERT_AUSD_V_TA_P_VERTRAG` job.
2.  **Capture Legacy Changes**:
    *   Export `LEGACY_TARGET_TABLE` *before* the job run (`legacy_target_pre.csv`).
    *   Export `LEGACY_TARGET_TABLE` *after* the job run (`legacy_target_post.csv`).
    *   Calculate the delta: `legacy_delta = legacy_target_post - legacy_target_pre`. This might involve identifying new rows, updated rows (based on PK and changed columns), and deleted rows.
3.  **Reset Migrated Target**: Restore `GCP_PROJECT.test_dataset_parity.migrated_target_table` to the same state as `legacy_target_pre.csv`.
4.  **Run Migrated Job**: Trigger the `dw_bert_ausd_v_ta_p_vertrag` Airflow DAG.
5.  **Capture Migrated Changes**:
    *   Export `GCP_PROJECT.test_dataset_parity.migrated_target_table` *after* the job run (`migrated_target_post.csv`).
    *   Calculate the delta: `migrated_delta = migrated_target_post - migrated_target_pre`.
6.  **Compare Deltas**: Compare `legacy_delta` and `migrated_delta`.

**Pass/Fail Criterion**:
The calculated deltas (new, updated, deleted records) from both the legacy and migrated runs must be identical.

**Example SQL for Delta Comparison (assuming a `MERGE` or `INSERT OVERWRITE` strategy):**

```sql
-- Assuming target_table has a primary key (e.g., contract_id) and a last_updated_timestamp
-- This example compares the final state, which implicitly checks the delta if initial states were identical.
-- For explicit delta, you'd need to capture before/after and compare.

-- Check for records present in legacy but not in migrated
SELECT 'Missing in Migrated' AS issue_type, *
FROM `GCP_PROJECT.test_dataset_parity.legacy_target_post`
EXCEPT DISTINCT
SELECT * FROM `GCP_PROJECT.test_dataset_parity.migrated_target_post`;

-- Check for records present in migrated but not in legacy
SELECT 'Extra in Migrated' AS issue_type, *
FROM `GCP_PROJECT.test_dataset_parity.migrated_target_post`
EXCEPT DISTINCT
SELECT * FROM `GCP_PROJECT.test_dataset_parity.legacy_target_post`;

-- Check for records with differing values (assuming PK matches)
SELECT 'Differing Values' AS issue_type,
       L.contract_id,
       L.status AS legacy_status, M.status AS migrated_status,
       L.customer_id AS legacy_customer_id, M.customer_id AS migrated_customer_id
       -- Add all relevant columns for comparison
FROM `GCP_PROJECT.test_dataset_parity.legacy_target_post` L
JOIN `GCP_PROJECT.test_dataset_parity.migrated_target_post` M
  ON L.contract_id = M.contract_id -- Placeholder: Use actual PK
WHERE NOT (L.status IS NOT DISTINCT FROM M.status AND
           L.customer_id IS NOT DISTINCT FROM M.customer_id
           -- Add all relevant column comparisons
          );

-- Pass if all queries return 0 rows.
```

---

### 3. Transformation Correctness: Join Logic Validation

**Purpose**: To verify that any join operations performed by the PySpark script (e.g., joining contract details with option data) produce the same results as the legacy KSH script.

**Setup**:
1.  Identify all tables involved in join operations within `r_ausd_v_ta_p_vertrag.ksh`.
2.  Create specific test data sets for these tables in BigQuery that cover:
    *   Matching keys in both tables.
    *   Non-matching keys (left-only, right-only).
    *   Duplicate keys in one or both tables (if applicable to join type).
    *   NULL keys (if allowed and handled).
3.  Configure the PySpark script to use these test data sets.

**Action**:
1.  **Run Legacy Job**: Execute the KSH script with the prepared test data in Oracle.
2.  **Capture Legacy Output**: Extract the relevant joined data or the final target table state.
3.  **Run Migrated Job**: Execute the PySpark script with the same prepared test data in BigQuery.
4.  **Capture Migrated Output**: Extract the relevant joined data or the final target table state.
5.  **Compare Outputs**: Compare the specific columns affected by the join.

**Pass/Fail Criterion**:
The output data resulting from the join operations in the migrated job must be identical to the legacy job's output for the same input. This can be verified as part of the overall output parity (Test 1) or by isolating the join logic if possible.

**Example Pytest (conceptual, assuming a function `run_pyspark_join_logic` can be tested in isolation):**

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Assume a helper function to simulate the join logic
def simulate_join_logic(spark, df_source1, df_source2):
    # Placeholder: Replace with actual join logic from r_ausd_v_ta_p_vertrag.py
    df_joined = df_source1.join(df_source2,
                                 df_source1.contract_id == df_source2.contract_id,
                                 "left_outer") \
                          .select(df_source1.contract_id, df_source1.status, df_source2.option_name)
    return df_joined

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.appName("JoinTest").getOrCreate()
    yield spark
    spark.stop()

def test_join_logic_correctness(spark_session):
    # Test Case 1: All matching keys
    data1_match = [("C1", "Active"), ("C2", "Inactive")]
    schema1 = ["contract_id", "status"]
    df1_match = spark_session.createDataFrame(data1_match, schema=schema1)

    data2_match = [("C1", "OptionA"), ("C2", "OptionB")]
    schema2 = ["contract_id", "option_name"]
    df2_match = spark_session.createDataFrame(data2_match, schema=schema2)

    expected_match = spark_session.createDataFrame([
        ("C1", "Active", "OptionA"),
        ("C2", "Inactive", "OptionB")
    ], schema=["contract_id", "status", "option_name"])

    result_match = simulate_join_logic(spark_session, df1_match, df2_match)
    assert result_match.count() == expected_match.count()
    assert result_match.exceptAll(expected_match).count() == 0
    assert expected_match.exceptAll(result_match).count() == 0

    # Test Case 2: Left-only keys
    data1_left = [("C1", "Active"), ("C3", "Pending")]
    df1_left = spark_session.createDataFrame(data1_left, schema=schema1)

    data2_left = [("C1", "OptionA")]
    df2_left = spark_session.createDataFrame(data2_left, schema=schema2)

    expected_left = spark_session.createDataFrame([
        ("C1", "Active", "OptionA"),
        ("C3", "Pending", None)
    ], schema=["contract_id", "status", "option_name"])

    result_left = simulate_join_logic(spark_session, df1_left, df2_left)
    assert result_left.count() == expected_left.count()
    assert result_left.exceptAll(expected_left).count() == 0
    assert expected_left.exceptAll(result_left).count() == 0

    print("Join logic correctness test passed.")
```

---

### 4. Transformation Correctness: Aggregation Logic Validation

**Purpose**: To verify that any aggregation operations (e.g., `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`, `GROUP BY`) performed by the PySpark script produce the same results as the legacy KSH script.

**Setup**:
1.  Identify any aggregation logic within `r_ausd_v_ta_p_vertrag.ksh`.
2.  Create specific test data sets in BigQuery that cover:
    *   Various group keys.
    *   NULL values in aggregated columns.
    *   Empty groups.
    *   Large numbers for `SUM`/`AVG`.
3.  Configure the PySpark script to use these test data sets.

**Action**:
1.  **Run Legacy Job**: Execute the KSH script with the prepared test data in Oracle.
2.  **Capture Legacy Output**: Extract the aggregated results or the final target table state.
3.  **Run Migrated Job**: Execute the PySpark script with the same prepared test data in BigQuery.
4.  **Capture Migrated Output**: Extract the aggregated results or the final target table state.
5.  **Compare Outputs**: Compare the specific aggregated values.

**Pass/Fail Criterion**:
The aggregated values in the migrated job's output must be identical to the legacy job's output for the same input.

**Example SQL Assertion (conceptual):**

```sql
-- Assuming the job aggregates contract counts by customer_id
SELECT customer_id, COUNT(contract_id) AS contract_count, SUM(contract_value) AS total_value
FROM `GCP_PROJECT.test_dataset_parity.migrated_target_table`
GROUP BY customer_id
ORDER BY customer_id;

-- Compare this output with the equivalent aggregation from the legacy system.
-- If the legacy system produces a separate aggregation table, compare that.
-- If it's part of the main target table, then Test 1 covers it.
```

---

### 5. Transformation Correctness: Filter Logic Validation

**Purpose**: To verify that any filtering conditions (e.g., `WHERE` clauses) applied by the PySpark script correctly select/exclude records, matching the legacy KSH script's behavior.

**Setup**:
1.  Identify all filtering conditions within `r_ausd_v_ta_p_vertrag.ksh`.
2.  Create specific test data sets in BigQuery that include records that should:
    *   Definitely pass the filter.
    *   Definitely fail the filter.
    *   Be edge cases (e.g., boundary values, NULLs in filter columns).
3.  Configure the PySpark script to use these test data sets.

**Action**:
1.  **Run Legacy Job**: Execute the KSH script with the prepared test data in Oracle.
2.  **Capture Legacy Output**: Extract the filtered records or the final target table state.
3.  **Run Migrated Job**: Execute the PySpark script with the same prepared test data in BigQuery.
4.  **Capture Migrated Output**: Extract the filtered records or the final target table state.
5.  **Compare Outputs**: Compare the specific records that should have been filtered in/out.

**Pass/Fail Criterion**:
The set of records included/excluded by the filter in the migrated job must be identical to the legacy job's output for the same input.

**Example Pytest (conceptual):**

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Assume a helper function to simulate the filter logic
def simulate_filter_logic(spark, df_source):
    # Placeholder: Replace with actual filter logic from r_ausd_v_ta_p_vertrag.py
    df_filtered = df_source.filter(F.col("status") == "Active") \
                           .filter(F.col("contract_value") > 1000)
    return df_filtered

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.appName("FilterTest").getOrCreate()
    yield spark
    spark.stop()

def test_filter_logic_correctness(spark_session):
    data = [
        ("C1", "Active", 1500),
        ("C2", "Inactive", 2000),
        ("C3", "Active", 500),
        ("C4", "Active", 1000), # Boundary
        ("C5", None, 3000),     # NULL status
        ("C6", "Active", None)  # NULL value
    ]
    schema = ["contract_id", "status", "contract_value"]
    df_source = spark_session.createDataFrame(data, schema=schema)

    expected_filtered = spark_session.createDataFrame([
        ("C1", "Active", 1500)
    ], schema=schema)

    result_filtered = simulate_filter_logic(spark_session, df_source)

    assert result_filtered.count() == expected_filtered.count()
    assert result_filtered.exceptAll(expected_filtered).count() == 0
    assert expected_filtered.exceptAll(result_filtered).count() == 0

    print("Filter logic correctness test passed.")
```

---

### 6. Transformation Correctness: Data Type and NULL Handling

**Purpose**: To verify that data types are correctly mapped and handled between Oracle and BigQuery, and that NULL values are consistently processed (e.g., NULLs in Oracle remain NULLs in BigQuery, or are transformed as per design).

**Setup**:
1.  Identify all columns in source and target tables, noting their Oracle data types and expected BigQuery data types.
2.  Create specific test data sets that include:
    *   All relevant data types (e.g., `VARCHAR`, `NUMBER`, `DATE`, `TIMESTAMP`, `BOOLEAN`).
    *   NULL values in various columns.
    *   Empty strings vs. NULLs (Oracle often treats empty strings as NULLs for `VARCHAR2`).
    *   Boundary values for numeric and date types.
3.  Configure the PySpark script to use these test data sets.

**Action**:
1.  **Run Legacy Job**: Execute the KSH script with the prepared test data in Oracle.
2.  **Capture Legacy Output**: Extract the final target table state.
3.  **Run Migrated Job**: Execute the PySpark script with the same prepared test data in BigQuery.
4.  **Capture Migrated Output**: Extract the final target table state.
5.  **Compare Outputs**: Perform a detailed comparison of data types and NULL/empty string handling for all columns.

**Pass/Fail Criterion**:
1.  All column data types in the migrated target table must match the expected BigQuery types.
2.  NULL values in the migrated target table must correspond exactly to NULLs (or designed transformations of NULLs) in the legacy target table.
3.  Empty strings in the migrated target table must correspond exactly to empty strings (or designed transformations of empty strings/NULLs) in the legacy target table.

**Example Pytest (for schema and data type comparison):**

```python
import pytest
from google.cloud import bigquery

def test_schema_and_type_handling():
    client = bigquery.Client()
    target_table_id = "YOUR_GCP_PROJECT_ID.test_dataset_parity.migrated_target_table"

    # Define expected schema based on legacy Oracle types and migration design
    expected_schema = [
        bigquery.SchemaField("contract_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("contract_value", "BIGNUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("start_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("last_updated_ts", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("is_active", "BOOL", mode="NULLABLE"),
        # Add all other columns
    ]

    table = client.get_table(target_table_id)
    actual_schema = table.schema

    # Compare field names and types
    assert len(actual_schema) == len(expected_schema), \
        f"Schema length mismatch: Expected {len(expected_schema)}, Got {len(actual_schema)}"

    for expected_field in expected_schema:
        found = False
        for actual_field in actual_schema:
            if expected_field.name == actual_field.name:
                assert expected_field.field_type == actual_field.field_type, \
                    f"Type mismatch for column '{expected_field.name}': Expected {expected_field.field_type}, Got {actual_field.field_type}"
                assert expected_field.mode == actual_field.mode, \
                    f"Mode mismatch for column '{expected_field.name}': Expected {expected_field.mode}, Got {actual_field.mode}"
                found = True
                break
        assert found, f"Column '{expected_field.name}' not found in migrated table schema."

    print("Schema and data type handling test passed.")

# NULL/Empty string comparison is covered by the full output parity test (Test 1)
# if `check_exact=True` and `dtype=str` are used for Pandas comparison,
# or by `IS NOT DISTINCT FROM` in SQL.
```

---

### 7. Transformation Correctness: Edge Case Handling

**Purpose**: To verify that the migrated job correctly handles specific edge cases that might not be covered by general data sets, such as:
*   Empty source tables.
*   Source tables with only NULLs.
*   Source tables with extremely large/small values.
*   Specific business rule edge cases identified during KSH script analysis.

**Setup**:
1.  Identify specific edge cases from the KSH script analysis or business requirements.
2.  Create minimal, targeted test data sets in BigQuery for each edge case.
3.  Configure the PySpark script to use these test data sets.

**Action**:
1.  For each edge case:
    *   **Run Legacy Job**: Execute the KSH script with the specific edge case data in Oracle.
    *   **Capture Legacy Output**: Extract the final target table state.
    *   **Run Migrated Job**: Execute the PySpark script with the same edge case data in BigQuery.
    *   **Capture Migrated Output**: Extract the final target table state.
    *   **Compare Outputs**: Compare the outputs.

**Pass/Fail Criterion**:
The output for each edge case in the migrated job must be identical to the legacy job's output.

**Example Test Case: Empty Source Table**

**Setup**:
1.  Ensure `GCP_PROJECT.test_dataset_parity.migrated_source_table_1` is empty.
2.  Ensure `GCP_PROJECT.test_dataset_parity.migrated_target_table` has a known initial state (e.g., empty or with some base data).

**Action**:
1.  **Run Legacy Job**: Execute the KSH script with `LEGACY_SOURCE_TABLE_1` empty.
2.  **Capture Legacy Output**: Export `LEGACY_TARGET_TABLE`.
3.  **Run Migrated Job**: Trigger the Airflow DAG.
4.  **Capture Migrated Output**: Export `GCP_PROJECT.test_dataset_parity.migrated_target_table`.
5.  **Compare Outputs**: Compare the two exported files.

**Pass/Fail Criterion**: The target tables must be identical. If the job is designed to do nothing with empty sources, the target table should remain unchanged. If it's designed to clear the target, it should be empty.

---

### 8. External-System Replacements: Source Data Read Verification (Oracle -> BigQuery)

**Purpose**: To verify that the PySpark job correctly reads data from the BigQuery source tables, which are the replacements for the legacy Oracle source tables. This is a sanity check that the PySpark script can connect to and query BigQuery.

**Setup**:
1.  Ensure BigQuery source tables (e.g., `GCP_PROJECT.DATASET.source_contract_details`) are populated with representative data.
2.  Configure the PySpark script to read from these tables.

**Action**:
1.  Manually run the PySpark script (or the Airflow DAG) in a debug mode or with logging enabled to show the count of records read from each source table.
2.  Alternatively, add explicit logging within the PySpark script to print `df_source.count()` after reading each source.
3.  Query the corresponding legacy Oracle tables to get their row counts.

**Pass/Fail Criterion**:
The number of records read by the PySpark script from each BigQuery source table must match the number of records in the corresponding legacy Oracle source table (assuming a full migration of source data). If the PySpark script applies initial filters during read, then the counts should match after those filters.

**Example PySpark Snippet for verification:**

```python
# In r_ausd_v_ta_p_vertrag.py
# ...
df_source_contract = spark.read.format("bigquery") \
    .option("table", "your_project:your_dataset.source_contract_details") \
    .load()
print(f"Records read from source_contract_details: {df_source_contract.count()}")

df_source_options = spark.read.format("bigquery") \
    .option("table", "your_project:your_dataset.source_options") \
    .load()
print(f"Records read from source_options: {df_source_options.count()}")
# ...
```

---

### 9. External-System Replacements: Target Data Write Verification (BigQuery)

**Purpose**: To verify that the PySpark job successfully writes/updates data to the designated BigQuery target table, and that the write mode (e.g., `overwrite`, `append`, `merge`) is correctly implemented.

**Setup**:
1.  Ensure the BigQuery target table (`GCP_PROJECT.DATASET.target_contract_info`) exists.
2.  Configure the PySpark script to write to this table.
3.  Populate source tables with data that will result in a clear change in the target table.

**Action**:
1.  Record the initial row count and a checksum/hash of the target table in BigQuery.
2.  Trigger the `dw_bert_ausd_v_ta_p_vertrag` Airflow DAG.
3.  After completion, query the BigQuery target table to check its final state.

**Pass/Fail Criterion**:
1.  The job must complete without errors.
2.  The row count of the target table must match the expected count based on the transformation logic.
3.  The data in the target table must reflect the transformations performed by the PySpark script.
4.  If the write mode is `overwrite`, the table should contain only the new data. If `append`, new data should be added. If `merge`, updates and inserts should be correctly applied.

**Example SQL for verification:**

```sql
-- Before job run:
SELECT COUNT(*) FROM `GCP_PROJECT.DATASET.target_contract_info`;
SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) FROM `GCP_PROJECT.DATASET.target_contract_info` AS t ORDER BY 1; -- Checksum

-- After job run:
SELECT COUNT(*) FROM `GCP_PROJECT.DATASET.target_contract_info`;
-- Verify count matches expected.
-- If the checksum changes as expected, it indicates data modification.
```

---

### 10. External-System Replacements: Logging and Monitoring (UC4 Includes Replacement)

**Purpose**: To verify that the logging and monitoring functionalities previously provided by UC4 includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`) and `$HOME/.dw_init` are adequately replaced in the migrated environment. This includes job status, error logging, and any custom metrics.

**Setup**:
1.  Understand the exact functionality of `DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`, and `$HOME/.dw_init`.
    *   `DW.HOLE_PFAD`: Likely path definitions.
    *   `DW.BERT_LESE_LOG`: Likely logging mechanisms.
    *   `$HOME/.dw_init`: Environment setup, potentially more logging.
2.  Ensure the PySpark script and Airflow DAG have implemented equivalent logging (e.g., Python `logging` module, `print` statements captured by Dataproc/Cloud Logging).
3.  Ensure Airflow's native logging and monitoring (e.g., task status, logs in Cloud Logging, email alerts) are configured.

**Action**:
1.  **Run Legacy Job**: Execute the KSH script and observe its logging output (e.g., log files, UC4 reports).
2.  **Run Migrated Job (Success Scenario)**: Trigger the Airflow DAG for a successful run.
    *   Check Airflow UI for task status (should be 'success').
    *   Review Dataproc job logs in Cloud Logging for PySpark script output.
    *   Verify any custom metrics or status updates (if implemented).
3.  **Run Migrated Job (Failure Scenario)**: Introduce an intentional error in the PySpark script (e.g., divide by zero, invalid table name) and trigger the Airflow DAG.
    *   Check Airflow UI for task status (should be 'failed').
    *   Review Dataproc job logs in Cloud Logging for error messages and stack traces.
    *   Verify any configured failure notifications (e.g., email alerts).

**Pass/Fail Criterion**:
1.  All critical log messages and error details from the PySpark script must be visible in Cloud Logging.
2.  Airflow task status and logs must accurately reflect job execution (success/failure).
3.  Any custom logging or monitoring (e.g., job start/end timestamps, record counts processed) must be present and accurate.
4.  Failure notifications (if configured) must be triggered correctly.
5.  The functionality of `DW.HOLE_PFAD` (e.g., correct paths being used) and `DW.BERT_LESE_LOG` (e.g., log messages being written) must be demonstrably replicated.

---

### 11. Data Quality / Row Count / Schema Assertions: Row Count Parity

**Purpose**: To verify that the total number of rows in the target table after migration is consistent with the legacy system's output, and that intermediate row counts (if applicable) are as expected.

**Setup**:
1.  Identify the target table (`target_contract_info`) and any significant intermediate tables/views.
2.  Ensure the test environment is set up as per Test 1 (identical inputs).

**Action**:
1.  **Run Legacy Job**: Execute the KSH script.
2.  **Capture Legacy Counts**: Record the final row count of `LEGACY_TARGET_TABLE`. If possible, record intermediate row counts at key transformation steps.
3.  **Run Migrated Job**: Trigger the Airflow DAG.
4.  **Capture Migrated Counts**: Record the final row count of `GCP_PROJECT.test_dataset_parity.migrated_target_table`. If possible, add logging to the PySpark script to output intermediate row counts.

**Pass/Fail Criterion**:
The final row count of the migrated target table must exactly match the final row count of the legacy target table. Intermediate row counts should also match if they can be reliably captured and compared.

**Example SQL Assertion:**

```sql
-- Compare final row counts
SELECT
    (SELECT COUNT(*) FROM `GCP_PROJECT.test_dataset_parity.legacy_target_post`) AS legacy_row_count,
    (SELECT COUNT(*) FROM `GCP_PROJECT.test_dataset_parity.migrated_target_table`) AS migrated_row_count,
    (SELECT COUNT(*) FROM `GCP_PROJECT.test_dataset_parity.legacy_target_post`) =
    (SELECT COUNT(*) FROM `GCP_PROJECT.test_dataset_parity.migrated_target_table`) AS counts_match;

-- Pass if 'counts_match' is TRUE.
```

---

### 12. Data Quality / Row Count / Schema Assertions: Schema Parity

**Purpose**: To verify that the schema (column names, data types, nullability) of the migrated target table in BigQuery is identical to the schema of the legacy target table in Oracle, after accounting for necessary type conversions.

**Setup**:
1.  Obtain the schema definition of `LEGACY_TARGET_TABLE` from Oracle.
2.  Define the expected BigQuery schema based on the migration design (Oracle to BigQuery type mapping).

**Action**:
1.  Query the schema of `GCP_PROJECT.DATASET.target_contract_info` in BigQuery.
2.  Compare this actual BigQuery schema against the expected BigQuery schema.

**Pass/Fail Criterion**:
1.  All column names must match.
2.  All column data types must match the expected BigQuery types.
3.  All column nullability constraints (REQUIRED/NULLABLE) must match.
4.  Column order should ideally match, but if not, ensure the job is robust to order changes.

**Example Pytest (similar to Test 6, but focused purely on schema):**

```python
import pytest
from google.cloud import bigquery

def test_target_schema_parity():
    client = bigquery.Client()
    target_table_id = "YOUR_GCP_PROJECT_ID.YOUR_DATASET.target_contract_info"

    # Define expected schema based on the migration design document's type mapping
    # This should be derived from the legacy Oracle schema.
    expected_schema = [
        bigquery.SchemaField("contract_id", "STRING", mode="REQUIRED", description="Unique identifier for the contract"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED", description="Identifier for the customer"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE", description="Current status of the contract"),
        bigquery.SchemaField("start_date", "DATE", mode="NULLABLE", description="Contract start date"),
        bigquery.SchemaField("end_date", "DATE", mode="NULLABLE", description="Contract end date"),
        bigquery.SchemaField("contract_value", "BIGNUMERIC", mode="NULLABLE", description="Monetary value of the contract"),
        bigquery.SchemaField("last_updated_timestamp", "TIMESTAMP", mode="NULLABLE", description="Last update timestamp"),
        # ... add all other columns with their expected BigQuery types and modes
    ]

    try:
        table = client.get_table(target_table_id)
        actual_schema = table.schema
    except Exception as e:
        pytest.fail(f"Could not retrieve BigQuery table schema for {target_table_id}: {e}")

    # Convert actual schema to a dictionary for easier comparison by name
    actual_schema_dict = {field.name: field for field in actual_schema}

    assert len(actual_schema) == len(expected_schema), \
        f"Schema length mismatch: Expected {len(expected_schema)} fields, Got {len(actual_schema)} fields."

    for expected_field in expected_schema:
        assert expected_field.name in actual_schema_dict, \
            f"Expected column '{expected_field.name}' not found in BigQuery target table."

        actual_field = actual_schema_dict[expected_field.name]

        assert expected_field.field_type == actual_field.field_type, \
            f"Data type mismatch for column '{expected_field.name}': Expected '{expected_field.field_type}', Got '{actual_field.field_type}'."

        assert expected_field.mode == actual_field.mode, \
            f"Nullability mode mismatch for column '{expected_field.name}': Expected '{expected_field.mode}', Got '{actual_field.mode}'."

        # Optional: Compare descriptions if they are part of the migration
        # assert expected_field.description == actual_field.description, \
        #     f"Description mismatch for column '{expected_field.name}'."

    print("Target schema parity test passed.")
```

---

### 13. Data Quality / Row Count / Schema Assertions: Data Quality Checks

**Purpose**: To verify that the migrated data adheres to defined data quality rules (e.g., uniqueness, non-null constraints, referential integrity, domain validity) that were implicitly or explicitly enforced in the legacy system.

**Setup**:
1.  Identify key data quality rules for the target table (e.g., `contract_id` is unique, `status` is one of 'Active', 'Inactive', 'Pending', `customer_id` is never NULL).
2.  Ensure the test environment is set up as per Test 1 (identical inputs).

**Action**:
1.  **Run Migrated Job**: Trigger the Airflow DAG.
2.  **Perform Data Quality Checks**: Execute SQL queries against the BigQuery target table to validate data quality rules.

**Pass/Fail Criterion**:
All data quality checks must pass (i.e., queries should return 0 rows for violations, or counts within acceptable thresholds).

**Example SQL Assertions:**

```sql
-- Check 1: Uniqueness of Primary Key (e.g., contract_id)
SELECT 'PK Uniqueness Violation' AS dq_check, contract_id, COUNT(*)
FROM `GCP_PROJECT.test_dataset_parity.migrated_target_table`
GROUP BY contract_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Check 2: Non-null constraint for critical columns (e.g., customer_id)
SELECT 'Non-Null Violation - customer_id' AS dq_check, COUNT(*)
FROM `GCP_PROJECT.test_dataset_parity.migrated_target_table`
WHERE customer_id IS NULL;
-- Expected: 0 rows

-- Check 3: Domain validity for 'status' column
SELECT 'Domain Violation - status' AS dq_check, status, COUNT(*)
FROM `GCP_PROJECT.test_dataset_parity.migrated_target_table`
WHERE status NOT IN ('Active', 'Inactive', 'Pending'); -- Placeholder: Replace with actual valid statuses
-- Expected: 0 rows

-- Check 4: Referential Integrity (if contract_id refers to a master contract table)
-- This requires the master table to also be migrated and populated.
SELECT 'Referential Integrity Violation - contract_id' AS dq_check, T.contract_id
FROM `GCP_PROJECT.test_dataset_parity.migrated_target_table` T
LEFT JOIN `GCP_PROJECT.DATASET.master_contract_table` M ON T.contract_id = M.contract_id
WHERE M.contract_id IS NULL;
-- Expected: 0 rows (unless design allows for orphaned records)
```