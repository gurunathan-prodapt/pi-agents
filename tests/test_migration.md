As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `DW.BERT_AUSD_V_TA_C_BFC` job migration. These tests aim to ensure the migrated BigQuery/Airflow solution is behaviourally equivalent to the legacy Oracle/KornShell system.

**General Assumptions for Test Execution:**
*   **Golden Dataset Availability**: A representative "golden dataset" (or multiple smaller, targeted datasets) is available. This dataset, when processed by the legacy system, produces known and verifiable output in `sof$ta_c_bfc`.
*   **Environment Access**: Secure access to both the legacy Oracle database and the target Google BigQuery environment is established for data loading, extraction, and comparison.
*   **Prerequisite Migrations**: All source tables (`dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) have been successfully migrated to BigQuery with identical schemas and data, and are available for the migrated job.
*   **`bfc_get_bindefrist` UDF/SP**: The BigQuery User-Defined Function (UDF) or Stored Procedure replacing `Cds$vr_BindeFrist.GetBindeFrist` is assumed to have been implemented and deployed, and its internal logic is functionally identical to the Oracle original. This is a critical assumption given the "Missing PL/SQL Source Code" risk.
*   **Airflow DAG and Python Wrapper**: The Airflow DAG (`dw_bert_ausd_v_ta_c_bfc.py`) and the Python wrapper script (`r_ausd_v_ta_c_bfc.py`) are deployed and runnable in the target GCP environment.

---

### Test Case 1: Output Parity - End-to-End Data Comparison (Golden Dataset)

*   **Purpose**: To verify that the migrated job, when run with identical input data, produces an identical final output in the target table (`sof$ta_c_bfc`) compared to the legacy job. This is the ultimate validation of behavioural equivalence.
*   **Setup**:
    1.  Identify a "golden dataset" that represents typical production data, including various scenarios (new contracts, updated contracts, contracts triggering `bfc_age`/`bfc_count` changes, contracts with outdated `bfc_procedure`).
    2.  Load this golden dataset into the source tables in both the legacy Oracle environment and the target BigQuery environment. Ensure data types, precision, and values are exactly matched.
    3.  Ensure both legacy `sof$ta_c_bfc` and target `sof_ta_c_bfc_bq` tables are in a clean, known state (e.g., empty or pre-populated with a baseline).
    4.  Record the state of the legacy `sof$ta_c_bfc` table *before* running the legacy job, if the job is incremental.
*   **Action**:
    1.  Execute the legacy `DW.BERT_AUSD_V_TA_C_BFC` job in the Oracle environment.
    2.  Execute the migrated `dw_bert_ausd_v_ta_c_bfc` Airflow DAG in the GCP environment.
    3.  After both jobs complete successfully, extract the entire contents of the legacy `sof$ta_c_bfc` table and the target `sof_ta_c_bfc_bq` table.
*   **Pass/Fail Criterion**:
    *   The number of rows in `LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc` must be identical to `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_bq`.
    *   All columns in `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_bq` must exactly match their corresponding columns in `LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc` for all rows, after accounting for any expected type conversions (e.g., Oracle `DATE` to BigQuery `DATE` or `TIMESTAMP`). A row-by-row comparison (e.g., using a hash of each row or a full `EXCEPT` query) should yield no differences.

```python
# Example pytest for output parity
import pandas as pd
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for Oracle connection

# Configuration
ORACLE_CONN_STR = "user/password@host:port/service_name"
BQ_PROJECT = "my-gcp-project"
BQ_DATASET = "my_dataset"
LEGACY_TARGET_TABLE = "isbert_schema.sof$ta_c_bfc"
BQ_TARGET_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.sof_ta_c_bfc_bq"

def fetch_oracle_data(query):
    with cx_Oracle.connect(ORACLE_CONN_STR) as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)

def fetch_bigquery_data(query):
    client = bigquery.Client(project=BQ_PROJECT)
    query_job = client.query(query)
    return query_job.to_dataframe()

def test_output_parity_full_run():
    print("--- Running Output Parity Test (Full Run) ---")

    # 1. Trigger legacy job (manual or via API/script)
    #    Placeholder: Assume legacy job is run and completes.
    #    e.g., subprocess.run(["ssh", "legacy_host", "/path/to/legacy_job.ksh"])
    print("Ensure legacy job has completed successfully.")

    # 2. Trigger migrated job (via Airflow API or direct execution)
    #    Placeholder: Assume Airflow DAG is triggered and completes.
    #    e.g., airflow_client.trigger_dag("dw_bert_ausd_v_ta_c_bfc")
    print("Ensure migrated Airflow DAG has completed successfully.")

    # 3. Fetch data from both systems
    print(f"Fetching data from Oracle: {LEGACY_TARGET_TABLE}")
    df_oracle = fetch_oracle_data(f"SELECT * FROM {LEGACY_TARGET_TABLE} ORDER BY CNTRCT_ID, BFC_DATE") # Order for consistent comparison

    print(f"Fetching data from BigQuery: {BQ_TARGET_TABLE}")
    df_bigquery = fetch_bigquery_data(f"SELECT * FROM `{BQ_TARGET_TABLE}` ORDER BY CNTRCT_ID, BFC_DATE") # Order for consistent comparison

    # 4. Perform comparison
    print(f"Oracle row count: {len(df_oracle)}")
    print(f"BigQuery row count: {len(df_bigquery)}")

    assert len(df_oracle) == len(df_bigquery), "Row counts do not match between Oracle and BigQuery."

    # Standardize column names and types for comparison (e.g., uppercase, handle date types)
    df_oracle.columns = [col.upper() for col in df_oracle.columns]
    df_bigquery.columns = [col.upper() for col in df_bigquery.columns]

    # Convert date/timestamp columns to a common format (e.g., string 'YYYY-MM-DD')
    for col in ['BFC_DATE', 'CREATED_DATE', 'LAST_UPDATED_DATE']: # Example date columns
        if col in df_oracle.columns:
            df_oracle[col] = pd.to_datetime(df_oracle[col]).dt.strftime('%Y-%m-%d')
        if col in df_bigquery.columns:
            df_bigquery[col] = pd.to_datetime(df_bigquery[col]).dt.strftime('%Y-%m-%d')

    # Sort both DataFrames to ensure row-by-row comparison is meaningful
    df_oracle = df_oracle.sort_values(by=list(df_oracle.columns)).reset_index(drop=True)
    df_bigquery = df_bigquery.sort_values(by=list(df_bigquery.columns)).reset_index(drop=True)

    # Compare DataFrames
    pd.testing.assert_frame_equal(df_oracle, df_bigquery, check_dtype=False, check_exact=True) # check_dtype=False if types might differ but values are same

    print("Output parity test passed: Oracle and BigQuery target tables are identical.")

```

---

### Test Case 2: Transformation Correctness - `bfc_get_bindefrist` UDF/SP Logic

*   **Purpose**: To rigorously validate that the re-implemented BigQuery UDF/Stored Procedure (`bfc_get_bindefrist_udf`) produces identical results to the original Oracle PL/SQL function (`Cds$vr_BindeFrist.GetBindeFrist`) for a wide range of inputs. This is crucial as it's a core piece of business logic.
*   **Setup**:
    1.  Create a comprehensive set of test cases for the `Cds$vr_BindeFrist.GetBindeFrist` function. These test cases should cover:
        *   Typical valid inputs.
        *   Edge cases (e.g., NULL inputs, boundary dates, specific `cntrct_id` values known to have complex logic).
        *   Inputs that might trigger different branches of logic within the function.
    2.  For each test case, record the input parameters (`i_cntrct_id`, `i_commitment_reference_date`, `i_cntrct_validity_id`) and the expected output `DATE` from the Oracle function.
*   **Action**:
    1.  For each test case:
        *   Call the Oracle `Cds$vr_BindeFrist.GetBindeFrist` function with the test inputs and record its output.
        *   Call the BigQuery `bfc_get_bindefrist_udf` with the same test inputs and record its output.
*   **Pass/Fail Criterion**:
    *   For every test case, the output `DATE` from the BigQuery UDF/SP must exactly match the output `DATE` from the Oracle PL/SQL function.

```python
# Example pytest for UDF/SP correctness
import pytest
from google.cloud import bigquery
import cx_Oracle
from datetime import date

# Configuration
ORACLE_CONN_STR = "user/password@host:port/service_name"
BQ_PROJECT = "my-gcp-project"
BQ_DATASET = "my_dataset"
BQ_UDF_NAME = f"{BQ_PROJECT}.{BQ_DATASET}.bfc_get_bindefrist_udf" # Assuming a SQL UDF

# Test cases: (i_cntrct_id, i_commitment_reference_date, i_cntrct_validity_id, expected_output_date)
# NOTE: These are example values. Actual values should come from analysis of the Oracle function.
TEST_CASES = [
    (1001, date(2023, 1, 15), 1, date(2023, 2, 15)),
    (1002, date(2022, 12, 1), 2, date(2023, 1, 1)),
    (1003, None, 3, None), # Test NULL commitment_reference_date
    (1004, date(2024, 5, 10), None, date(2024, 6, 10)), # Test NULL cntrct_validity_id
    (1005, date(1900, 1, 1), 1, date(1900, 2, 1)), # Edge case: very old date
    (1006, date(2050, 1, 1), 1, date(2050, 2, 1)), # Edge case: future date
    # Add more test cases covering specific logic branches, boundary conditions, etc.
]

def call_oracle_function(cntrct_id, commitment_ref_date, cntrct_validity_id):
    with cx_Oracle.connect(ORACLE_CONN_STR) as connection:
        cursor = connection.cursor()
        # Example: Calling a PL/SQL function. Syntax might vary based on package/function signature.
        # This assumes a function returning DATE.
        result = cursor.callfunc(
            "SPR_SCHEMA.CDS$VR_BINDEFRIST.GETBINDEFRIST",
            cx_Oracle.DATETIME, # Expected return type
            [cntrct_id, commitment_ref_date, cntrct_validity_id]
        )
        return result.date() if result else None # Extract date part

def call_bigquery_udf(cntrct_id, commitment_ref_date, cntrct_validity_id):
    client = bigquery.Client(project=BQ_PROJECT)
    # Format dates for SQL
    commitment_ref_date_str = f"DATE '{commitment_ref_date}'" if commitment_ref_date else "NULL"
    cntrct_validity_id_str = str(cntrct_validity_id) if cntrct_validity_id is not None else "NULL"

    query = f"""
    SELECT {BQ_UDF_NAME}(
        {cntrct_id},
        {commitment_ref_date_str},
        {cntrct_validity_id_str}
    ) AS bindefrist_date
    """
    query_job = client.query(query)
    result = query_job.result().to_dataframe()
    return result['bindefrist_date'].iloc[0] if not result.empty else None

@pytest.mark.parametrize("cntrct_id, commitment_ref_date, cntrct_validity_id, expected_output", TEST_CASES)
def test_bfc_get_bindefrist_udf_correctness(cntrct_id, commitment_ref_date, cntrct_validity_id, expected_output):
    print(f"\n--- Testing UDF with inputs: cntrct_id={cntrct_id}, ref_date={commitment_ref_date}, valid_id={cntrct_validity_id} ---")

    # Call Oracle function
    oracle_result = call_oracle_function(cntrct_id, commitment_ref_date, cntrct_validity_id)
    print(f"Oracle Result: {oracle_result}")

    # Call BigQuery UDF
    bigquery_result = call_bigquery_udf(cntrct_id, commitment_ref_date, cntrct_validity_id)
    print(f"BigQuery Result: {bigquery_result}")

    # Compare results
    assert oracle_result == bigquery_result, \
        f"Mismatch for inputs ({cntrct_id}, {commitment_ref_date}, {cntrct_validity_id}): " \
        f"Oracle={oracle_result}, BigQuery={bigquery_result}"

    print("UDF correctness test passed for this case.")

```

---

### Test Case 3: Transformation Correctness - `sof$ta_c_bfc_akt` Population Logic

*   **Purpose**: To verify that the intermediate temporary table (`sof$ta_c_bfc_akt`) is populated identically in BigQuery as it is in Oracle. This validates the core aggregation, join, filter, and date conversion logic.
*   **Setup**:
    1.  Load a specific, controlled dataset into the source tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) in both environments. This dataset should include scenarios for:
        *   Records that should join successfully.
        *   Records that result in `LEFT JOIN` NULLs.
        *   Records that are filtered out.
        *   Data that tests date conversions (`NVL`, `TO_CHAR`, `TRUNC`).
    2.  Ensure both `LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc_akt` and `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_akt_bq` are empty before execution.
*   **Action**:
    1.  Execute only the `TRUNCATE` and `INSERT INTO sof$ta_c_bfc_akt` part of the legacy Oracle SQL script.
    2.  Execute only the corresponding BigQuery SQL for populating `sof_ta_c_bfc_akt_bq` (or the CTE that represents it).
    3.  Extract the full contents of both temporary tables.
*   **Pass/Fail Criterion**:
    *   The number of rows in `LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc_akt` must be identical to `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_akt_bq`.
    *   All columns in `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_akt_bq` must exactly match their corresponding columns in `LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc_akt` for all rows.

```sql
-- Example SQL for comparison (to be executed after running the respective population steps)
-- Oracle:
SELECT
    CNTRCT_ID,
    BFC_DATE,
    BFC_AGE,
    BFC_COUNT,
    BFC_PROCEDURE,
    TO_CHAR(CREATED_DATE, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_DATE_STR
FROM LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc_akt
ORDER BY CNTRCT_ID, BFC_DATE;

-- BigQuery:
SELECT
    CNTRCT_ID,
    BFC_DATE,
    BFC_AGE,
    BFC_COUNT,
    BFC_PROCEDURE,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CREATED_DATE) AS CREATED_DATE_STR
FROM `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_akt_bq`
ORDER BY CNTRCT_ID, BFC_DATE;

-- Comparison logic (e.g., in Python or a data comparison tool):
-- 1. Fetch results from both queries.
-- 2. Compare row counts.
-- 3. Compare row by row, column by column.
--    Example Python using pandas:
--    df_oracle_akt = fetch_oracle_data("SELECT ... FROM sof$ta_c_bfc_akt")
--    df_bigquery_akt = fetch_bigquery_data("SELECT ... FROM sof_ta_c_bfc_akt_bq")
--    pd.testing.assert_frame_equal(df_oracle_akt, df_bigquery_akt, check_dtype=False)
```

---

### Test Case 4: Transformation Correctness - Initial `INSERT` and `MERGE` Logic

*   **Purpose**: To validate the conditional initial `INSERT` into `sof$ta_c_bfc` (if empty) and the subsequent `MERGE` statement's `WHEN MATCHED` and `WHEN NOT MATCHED` clauses. This covers incremental updates based on `bfc_age` or `bfc_count` and new record insertion.
*   **Setup**:
    1.  **Scenario A (Initial Insert)**: Ensure `sof$ta_c_bfc` (Oracle) and `sof_ta_c_bfc_bq` (BigQuery) are completely empty. Load a small, controlled dataset into the source tables that will result in records being populated into `sof$ta_c_bfc_akt`.
    2.  **Scenario B (Merge - Updates & Inserts)**:
        *   Populate `sof$ta_c_bfc` and `sof_ta_c_bfc_bq` with a baseline set of records.
        *   Load source tables with data such that `sof$ta_c_bfc_akt` will contain:
            *   Records matching existing `sof$ta_c_bfc` records but with changes in `bfc_age` or `bfc_count` (to trigger `WHEN MATCHED THEN UPDATE`).
            *   Records not present in `sof$ta_c_bfc` (to trigger `WHEN NOT MATCHED THEN INSERT`).
            *   Records matching existing `sof$ta_c_bfc` records with no changes (to ensure they are not updated unnecessarily).
*   **Action**:
    1.  **Scenario A**: Run the full job (or relevant DML steps) in both environments.
    2.  **Scenario B**: Run the full job (or relevant DML steps) in both environments.
    3.  After each scenario, extract the contents of `sof$ta_c_bfc` and `sof_ta_c_bfc_bq`.
*   **Pass/Fail Criterion**:
    *   **Scenario A**: The final `sof$ta_c_bfc` tables must be identical in row count and data.
    *   **Scenario B**: The final `sof$ta_c_bfc` tables must be identical in row count and data, reflecting the correct updates and inserts from the `MERGE` logic.

```sql
-- Example SQL for comparison (after running the job for each scenario)
-- Oracle:
SELECT
    CNTRCT_ID,
    BFC_DATE,
    BFC_AGE,
    BFC_COUNT,
    BFC_PROCEDURE,
    TO_CHAR(CREATED_DATE, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_DATE_STR,
    TO_CHAR(LAST_UPDATED_DATE, 'YYYY-MM-DD HH24:MI:SS') AS LAST_UPDATED_DATE_STR
FROM LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc
ORDER BY CNTRCT_ID, BFC_DATE;

-- BigQuery:
SELECT
    CNTRCT_ID,
    BFC_DATE,
    BFC_AGE,
    BFC_COUNT,
    BFC_PROCEDURE,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CREATED_DATE) AS CREATED_DATE_STR,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', LAST_UPDATED_DATE) AS LAST_UPDATED_DATE_STR
FROM `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_bq`
ORDER BY CNTRCT_ID, BFC_DATE;

-- Comparison logic as in Test Case 1.
```

---

### Test Case 5: Transformation Correctness - `UPDATE` Outdated `bfc_procedure` Logic

*   **Purpose**: To verify the logic for updating records in `sof$ta_c_bfc` where `bfc_procedure` is outdated, including the `ROWNUM`/`LIMIT` conversion.
*   **Setup**:
    1.  Populate `sof$ta_c_bfc` (Oracle) and `sof_ta_c_bfc_bq` (BigQuery) with a specific dataset where some records have `bfc_procedure` values that would be considered "outdated" based on the job's logic (e.g., older than the `created` date of the `Cds$vr_BindeFrist` package).
    2.  Ensure the `v_max_update` variable (or its BigQuery equivalent) is set to a value that allows some, but potentially not all, outdated records to be updated, to test the `LIMIT` clause.
*   **Action**:
    1.  Execute only the `UPDATE` statement for outdated `bfc_procedure` in the legacy Oracle environment.
    2.  Execute the corresponding `UPDATE` statement in the BigQuery environment.
    3.  Extract the contents of both `sof$ta_c_bfc` and `sof_ta_c_bfc_bq`.
*   **Pass/Fail Criterion**:
    *   The number of updated rows reported by both systems must be identical.
    *   The final `sof$ta_c_bfc` tables must be identical in row count and data, with the correct records having their `bfc_procedure` and `LAST_UPDATED_DATE` fields updated as expected.

```sql
-- Example SQL for comparison (after running the update step)
-- Use the same comparison queries as in Test Case 4.
```

---

### Test Case 6: External System Replacement - `all_objects` Date Sourcing

*   **Purpose**: To verify that the `created` date of the `Cds$vr_BindeFrist` package, which was sourced from `all_objects@PCRS1` in Oracle, is correctly and consistently determined/used in the BigQuery environment.
*   **Setup**:
    1.  Identify the exact `created` timestamp of the `SPR_SCHEMA.CDS$VR_BINDEFRIST` package in the legacy Oracle environment.
    2.  Identify how this value is obtained or represented in the BigQuery environment (e.g., a hardcoded value in the Python script, a lookup from a metadata table, or derived from BigQuery's own UDF creation timestamp).
*   **Action**:
    1.  Query the Oracle `all_objects` view to get the `created` date for `SPR_SCHEMA.CDS$VR_BINDEFRIST`.
    2.  Inspect the BigQuery SQL or Python wrapper script to determine the value being used for this date. If it's dynamically sourced, query that source.
*   **Pass/Fail Criterion**:
    *   The `created` date value used in the BigQuery job (e.g., in the `DECLARE` statement or Python variable) must exactly match the `created` date of `SPR_SCHEMA.CDS$VR_BINDEFRIST` from the Oracle `all_objects` view.

```python
# Example pytest for external system replacement
import cx_Oracle
from google.cloud import bigquery
from datetime import datetime

# Configuration
ORACLE_CONN_STR = "user/password@host:port/service_name"
BQ_PROJECT = "my-gcp-project"
BQ_DATASET = "my_dataset" # Assuming the BQ UDF is in this dataset

def get_oracle_package_creation_date():
    with cx_Oracle.connect(ORACLE_CONN_STR) as connection:
        cursor = connection.cursor()
        # Query all_objects for the package creation date
        cursor.execute("""
            SELECT CREATED
            FROM ALL_OBJECTS
            WHERE OWNER = 'SPR_SCHEMA'
            AND OBJECT_NAME = 'CDS$VR_BINDEFRIST'
            AND OBJECT_TYPE = 'PACKAGE'
        """)
        result = cursor.fetchone()
        return result[0] if result else None

def get_bigquery_udf_creation_date_or_configured_value():
    # This function needs to reflect how the BQ job gets this date.
    # Option 1: Hardcoded in Python script (e.g., from a config file)
    # return datetime.strptime("2023-10-26 10:00:00", "%Y-%m-%d %H:%M:%S")

    # Option 2: From BigQuery UDF metadata (if UDF creation date is used)
    client = bigquery.Client(project=BQ_PROJECT)
    query = f"""
    SELECT creation_time
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.ROUTINES`
    WHERE routine_name = 'bfc_get_bindefrist_udf'
    AND routine_type = 'SCALAR_FUNCTION'
    """
    query_job = client.query(query)
    result = query_job.result().to_dataframe()
    if not result.empty:
        # BigQuery creation_time is in microseconds since epoch, convert to datetime
        return datetime.fromtimestamp(result['creation_time'].iloc[0] / 1000000)
    return None

def test_package_creation_date_sourcing():
    print("--- Testing Package Creation Date Sourcing ---")

    oracle_date = get_oracle_package_creation_date()
    print(f"Oracle Package Creation Date: {oracle_date}")

    bigquery_date = get_bigquery_udf_creation_date_or_configured_value()
    print(f"BigQuery Configured/Derived Date: {bigquery_date}")

    assert oracle_date is not None, "Could not retrieve Oracle package creation date."
    assert bigquery_date is not None, "Could not retrieve BigQuery configured/derived date."

    # Compare dates, potentially truncating to day if only day matters for logic
    # Or compare exact timestamps if precision is critical
    assert oracle_date.date() == bigquery_date.date(), \
        f"Mismatch in package creation date: Oracle={oracle_date.date()}, BigQuery={bigquery_date.date()}"

    print("Package creation date sourcing test passed.")

```

---

### Test Case 7: Data Quality - Schema Validation

*   **Purpose**: To ensure that the schema (column names, data types, nullability) of the target BigQuery table (`sof_ta_c_bfc_bq`) precisely matches the legacy Oracle table (`sof$ta_c_bfc`).
*   **Setup**: None, this test directly queries metadata.
*   **Action**:
    1.  Extract the schema definition (column name, data type, nullability, length/precision) for `LEGACY_ORACLE_SCHEMA.sof$ta_c_bfc`.
    2.  Extract the schema definition for `GCP_BQ_PROJECT.GCP_BQ_DATASET.sof_ta_c_bfc_bq`.
*   **Pass/Fail Criterion**:
    *   Both schemas must have the same number of columns.
    *   For each column, the name, data type (with appropriate BigQuery equivalents for Oracle types), and nullability must match. Precision/scale for numeric types and length for string types should also be compared.

```python
# Example pytest for schema validation
import pytest
from google.cloud import bigquery
import cx_Oracle

# Configuration
ORACLE_CONN_STR = "user/password@host:port/service_name"
BQ_PROJECT = "my-gcp-project"
BQ_DATASET = "my_dataset"
LEGACY_TARGET_TABLE = "isbert_schema.sof$ta_c_bfc"
BQ_TARGET_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.sof_ta_c_bfc_bq"

# Mapping Oracle types to expected BigQuery types (customize as needed)
TYPE_MAPPING = {
    "NUMBER": "NUMERIC", # Or INT64, FLOAT64 depending on precision
    "VARCHAR2": "STRING",
    "DATE": "DATE", # Or TIMESTAMP if time component is critical
    "TIMESTAMP(6)": "TIMESTAMP",
    # Add more mappings as per actual schema
}

def get_oracle_schema(table_name):
    schema = {}
    with cx_Oracle.connect(ORACLE_CONN_STR) as connection:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = '{table_name.split('.')[0].upper()}'
            AND TABLE_NAME = '{table_name.split('.')[1].upper()}'
            ORDER BY COLUMN_ID
        """)
        for col_name, data_type, nullable, data_length, data_precision, data_scale in cursor:
            schema[col_name.upper()] = {
                "data_type": data_type,
                "nullable": (nullable == 'Y'),
                "length": data_length,
                "precision": data_precision,
                "scale": data_scale
            }
    return schema

def get_bigquery_schema(project_id, dataset_id, table_id):
    schema = {}
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id.split('.')[-1])
    table = client.get_table(table_ref)
    for field in table.schema:
        schema[field.name.upper()] = {
            "data_type": field.field_type,
            "nullable": (field.mode == 'NULLABLE'),
            "description": field.description # Can be useful for comparison
        }
    return schema

def test_target_table_schema_parity():
    print("--- Running Schema Validation Test ---")

    oracle_schema = get_oracle_schema(LEGACY_TARGET_TABLE)
    bigquery_schema = get_bigquery_schema(BQ_PROJECT, BQ_DATASET, BQ_TARGET_TABLE)

    print(f"Oracle Schema ({LEGACY_TARGET_TABLE}): {oracle_schema}")
    print(f"BigQuery Schema ({BQ_TARGET_TABLE}): {bigquery_schema}")

    assert len(oracle_schema) == len(bigquery_schema), \
        f"Column count mismatch: Oracle has {len(oracle_schema)} columns, BigQuery has {len(bigquery_schema)}."

    for col_name, oracle_col_def in oracle_schema.items():
        assert col_name in bigquery_schema, f"Column '{col_name}' missing in BigQuery schema."
        bigquery_col_def = bigquery_schema[col_name]

        # Compare data types
        expected_bq_type = TYPE_MAPPING.get(oracle_col_def["data_type"].upper(), oracle_col_def["data_type"].upper())
        assert bigquery_col_def["data_type"].upper() == expected_bq_type, \
            f"Data type mismatch for column '{col_name}': Oracle '{oracle_col_def['data_type']}' -> Expected BQ '{expected_bq_type}', Got BQ '{bigquery_col_def['data_type']}'"

        # Compare nullability
        assert oracle_col_def["nullable"] == bigquery_col_def["nullable"], \
            f"Nullability mismatch for column '{col_name}': Oracle '{oracle_col_def['nullable']}', BigQuery '{bigquery_col_def['nullable']}'"

        # Further checks for precision/scale/length can be added here if relevant for specific types
        # e.g., if oracle_col_def["data_type"] == "NUMBER":
        #    assert oracle_col_def["precision"] == bigquery_col_def["precision"]

    print("Schema validation test passed: Target table schemas are equivalent.")

```

---

### Test Case 8: Orchestration and Error Handling

*   **Purpose**: To verify that the Airflow DAG correctly orchestrates the job, handles parameter passing, and gracefully manages expected and unexpected errors.
*   **Setup**:
    1.  Deploy the Airflow DAG (`dw_bert_ausd_v_ta_c_bfc.py`) to Cloud Composer.
    2.  Ensure Cloud Logging is configured for the Airflow environment.
    3.  Prepare a test BigQuery SQL script that can be intentionally broken (e.g., syntax error, table not found).
*   **Action**:
    1.  **Successful Run**: Trigger the Airflow DAG with valid parameters (if any, e.g., `p_JobKennung`, `p_EintragsNr`).
    2.  **Invalid Parameter Handling**: Trigger the Airflow DAG with missing or invalid parameters (if the Python wrapper script handles them).
    3.  **BigQuery SQL Error**: Replace the valid BigQuery SQL script with the intentionally broken one, then trigger the DAG.
    4.  **Source Data Unavailability**: Temporarily rename a source table in BigQuery to simulate unavailability, then trigger the DAG.
*   **Pass/Fail Criterion**:
    *   **Successful Run**: The Airflow DAG completes with a "success" status, and logs indicate all steps (Python script execution, BigQuery queries) ran without errors.
    *   **Invalid Parameter Handling**: The Airflow DAG fails early (e.g., in the Python wrapper script), the task status is "failed", and logs clearly indicate the parameter validation error.
    *   **BigQuery SQL Error**: The Airflow DAG task corresponding to the BigQuery execution fails, the task status is "failed", and Cloud Logging contains detailed BigQuery error messages (e.g., syntax error, table not found).
    *   **Source Data Unavailability**: The Airflow DAG task fails, and logs clearly indicate the BigQuery error related to the missing table.
    *   In all failure scenarios, no partial or incorrect data should be committed to the target `sof_ta_c_bfc_bq` table.

```python
# Example pytest for orchestration (conceptual, as it interacts with Airflow/GCP APIs)
import pytest
from airflow.api.client.local_client import Client # For local testing or Airflow API client
from google.cloud import logging_v2
import time

# Configuration
AIRFLOW_DAG_ID = "dw_bert_ausd_v_ta_c_bfc"
GCP_PROJECT = "my-gcp-project"
# Assuming Airflow client setup or direct API calls

def trigger_airflow_dag(dag_id, conf=None):
    # This would typically use Airflow's REST API or a client library
    # For demonstration, a placeholder:
    print(f"Triggering DAG '{dag_id}' with conf: {conf}")
    # Example using Airflow's local client (for local testing setup)
    # client = Client(None, None)
    # dr = client.trigger_dag(dag_id, conf=conf)
    # return dr.run_id
    return f"test_run_{int(time.time())}" # Mock run_id

def get_airflow_dag_run_status(dag_id, run_id):
    # Query Airflow API for DAG run status
    # For demonstration, a placeholder:
    # return "success" or "failed"
    return "success" # Mock success

def check_cloud_logs(run_id, expected_error_message=None):
    client = logging_v2.Client(project=GCP_PROJECT)
    filter_str = f'resource.type="cloud_composer_environment" AND labels.dag_id="{AIRFLOW_DAG_ID}" AND labels.run_id="{run_id}"'
    if expected_error_message:
        filter_str += f' AND textPayload:"{expected_error_message}"'

    entries = list(client.list_entries(filter_=filter_str, order_by=logging_v2.DESCENDING))
    return any(entries) # True if logs found matching filter

@pytest.mark.orchestration
def test_airflow_successful_run():
    print("\n--- Testing Airflow Successful Run ---")
    run_id = trigger_airflow_dag(AIRFLOW_DAG_ID, conf={"p_JobKennung": "TEST", "p_EintragsNr": 123})
    # Wait for DAG to complete (in real test, poll Airflow API)
    time.sleep(60) # Placeholder
    status = get_airflow_dag_run_status(AIRFLOW_DAG_ID, run_id)
    assert status == "success", f"DAG run {run_id} failed unexpectedly."
    assert check_cloud_logs(run_id, "Job completed successfully"), "Success message not found in logs."
    print("Airflow successful run test passed.")

@pytest.mark.orchestration
def test_airflow_invalid_parameters():
    print("\n--- Testing Airflow Invalid Parameters ---")
    # Assuming Python wrapper validates parameters and fails if missing
    run_id = trigger_airflow_dag(AIRFLOW_DAG_ID, conf={}) # Missing required parameters
    time.sleep(60) # Placeholder
    status = get_airflow_dag_run_status(AIRFLOW_DAG_ID, run_id)
    assert status == "failed", f"DAG run {run_id} did not fail with invalid parameters."
    assert check_cloud_logs(run_id, "Parameter validation failed"), "Parameter validation error not found in logs."
    print("Airflow invalid parameters test passed.")

@pytest.mark.orchestration
def test_airflow_bigquery_sql_error():
    print("\n--- Testing Airflow BigQuery SQL Error ---")
    # Pre-requisite: Deploy a broken BQ SQL script for this test run
    # This would involve a setup step to replace the SQL file in GCS
    print("Pre-requisite: Ensure a broken BQ SQL script is deployed for this test.")
    run_id = trigger_airflow_dag(AIRFLOW_DAG_ID, conf={"p_JobKennung": "ERROR_TEST"})
    time.sleep(60) # Placeholder
    status = get_airflow_dag_run_status(AIRFLOW_DAG_ID, run_id)
    assert status == "failed", f"DAG run {run_id} did not fail with BigQuery SQL error."
    assert check_cloud_logs(run_id, "BigQuery job failed"), "BigQuery job failure message not found in logs."
    # Further checks for specific BQ error messages in logs
    print("Airflow BigQuery SQL error test passed.")

```