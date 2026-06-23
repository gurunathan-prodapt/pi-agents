As a senior data-migration QA engineer, I've developed a comprehensive suite of validation tests for the migrated BigQuery job `k_ausd_bp_ta_apn_vertrag_sp`. These tests aim to ensure behavioral equivalence, data integrity, and adherence to the migration design.

The tests are categorized to cover output parity, transformation correctness, external system replacements (logging/auditing), and data quality/schema assertions.

**Assumptions for Test Execution:**

*   A Google Cloud Project (`PROJECT_ID`) is set up.
*   Dedicated BigQuery datasets for testing (`DATASET_ID`, `ISBERT_SCHEMA_DATASET_ID`) exist.
*   The BigQuery Stored Procedures (`k_ausd_bp_ta_apn_vertrag_sp`, `d_ausd_bp_ta_apn_vertrag_proc`) are deployed to `PROJECT_ID.DATASET_ID`.
*   The `sof$ta_bpr_apn` table (source) exists in `PROJECT_ID.DATASET_ID` (or `ISBERT_SCHEMA_DATASET_ID` if configured differently for source data).
*   The `sof$ta_apn_vertrag` table (target), `job_audit`, and `error_log` tables exist in `PROJECT_ID.DATASET_ID` with the specified schemas.
*   A Python environment with `pytest` and `google-cloud-bigquery` library is configured for running the test code.
*   The BigQuery client used by `pytest` has appropriate permissions to create/truncate tables and execute stored procedures.

---

### Test Setup (Pytest Fixtures)

The following `pytest` fixtures provide a clean and consistent environment for each test case:

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta
import time # For potential delays if needed

# --- Configuration ---
# Replace with your actual GCP Project ID and BigQuery Dataset IDs
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "test_isbert_data" # Use a dedicated test dataset
ISBERT_SCHEMA_DATASET_ID = "test_isbert_source" # For source tables like sof$ta_bpr_apn

# --- Pytest Fixtures ---

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def setup_teardown_tables(bq_client):
    """Ensures tables exist and are clean before/after each test."""
    # Create schemas if they don't exist
    bq_client.query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}` OPTIONS(description='Test dataset for k_ausd_bp_ta_apn_vertrag migration');").result()
    bq_client.query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{ISBERT_SCHEMA_DATASET_ID}` OPTIONS(description='Test source dataset for k_ausd_bp_ta_apn_vertrag migration');").result()

    # Create tables if they don't exist (using the DDL from the migration design)
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.sof$ta_bpr_apn` (
            cntrct_id STRING,
            bpr_id STRING,
            cntrct_id_ref STRING,
            access_point_name STRING
        );
    """).result()

    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.sof$ta_apn_vertrag` (
            contract_id STRING,
            access_point_names STRING,
            contract_refs STRING
        );
    """).result()

    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.job_audit` (
            job_audit_id INT64,
            job_name STRING NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status STRING NOT NULL,
            job_kennung STRING,
            eintrags_nr STRING,
            stichtag DATE,
            wiederanlauf_wert INT64,
            record_count INT64,
            error_message STRING,
            creation_time TIMESTAMP
        );
    """).result()

    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.error_log` (
            error_id INT64,
            job_name STRING NOT NULL,
            error_message STRING,
            error_stack STRING,
            error_severity STRING,
            error_code STRING,
            error_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            additional_info JSON
        );
    """).result()

    # Clean tables before each test
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_bpr_apn`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_apn_vertrag`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_audit`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()

    yield # Run the test

    # Clean tables after test (optional, but good for isolation)
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_bpr_apn`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_apn_vertrag`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_audit`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()

# --- Helper Functions for Tests ---

def call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, dataset_id=DATASET_ID, isbert_schema_dataset_id=ISBERT_SCHEMA_DATASET):
    """Helper function to call the main orchestration stored procedure."""
    query = f"""
        CALL `{PROJECT_ID}.{dataset_id}.k_ausd_bp_ta_apn_vertrag_sp`(
            p_JobKennung => @job_kennung,
            p_EintragsNr => @eintrags_nr,
            p_Stichtag => @stichtag,
            p_wiederanlaufWert => @wiederanlauf_wert,
            p_dataset_name => '{dataset_id}',
            p_isbert_schema_dataset => '{isbert_schema_dataset_id}'
        );
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr),
            bigquery.ScalarQueryParameter("stichtag", "STRING", stichtag),
            bigquery.ScalarQueryParameter("wiederanlauf_wert", "INT64", wiederanlauf_wert),
        ]
    )
    # Execute the query and return the job object.
    # .result() will raise an exception if the SP raises one.
    return bq_client.query(query, job_config=job_config)

def get_table_rows(bq_client, table_name, dataset_id=DATASET_ID, order_by_col=None):
    """Helper to fetch all rows from a table."""
    order_clause = f"ORDER BY {order_by_col}" if order_by_col else ""
    query = f"SELECT * FROM `{PROJECT_ID}.{dataset_id}.{table_name}` {order_clause}"
    return [dict(row) for row in bq_client.query(query).result()]

def get_single_audit_entry(bq_client, dataset_id=DATASET_ID):
    """Helper to fetch the single audit entry (assuming one job run per test)."""
    query = f"SELECT * FROM `{PROJECT_ID}.{dataset_id}.job_audit` ORDER BY start_time DESC LIMIT 1"
    rows = list(bq_client.query(query).result())
    return dict(rows[0]) if rows else None

def get_single_error_log_entry(bq_client, dataset_id=DATASET_ID):
    """Helper to fetch the single error log entry (assuming one error per test)."""
    query = f"SELECT * FROM `{PROJECT_ID}.{dataset_id}.error_log` ORDER BY error_timestamp DESC LIMIT 1"
    rows = list(bq_client.query(query).result())
    return dict(rows[0]) if rows else None

def insert_into_bpr_apn(bq_client, data):
    """Helper to insert data into sof$ta_bpr_apn."""
    rows_to_insert = [
        bigquery.Row(row_data) for row_data in data
    ]
    errors = bq_client.insert_rows_from_json(f"{PROJECT_ID}.{DATASET_ID}.sof$ta_bpr_apn", rows_to_insert)
    assert not errors, f"Errors inserting data: {errors}"

def insert_into_apn_vertrag(bq_client, data):
    """Helper to insert data into sof$ta_apn_vertrag."""
    rows_to_insert = [
        bigquery.Row(row_data) for row_data in data
    ]
    errors = bq_client.insert_rows_from_json(f"{PROJECT_ID}.{DATASET_ID}.sof$ta_apn_vertrag", rows_to_insert)
    assert not errors, f"Errors inserting data: {errors}"

```

---

### 1. Output Parity & Transformation Correctness

#### Test Case 1.1: Happy Path - Basic Data Transformation and Aggregation

*   **Purpose:** Verify that the core data transformation logic in `d_ausd_bp_ta_apn_vertrag_proc` (called by `k_ausd_bp_ta_apn_vertrag_sp`) correctly aggregates and inserts data into the target table `sof$ta_apn_vertrag` when provided with valid inputs. This covers basic joins (implicit in the aggregation), aggregations (`STRING_AGG`), and overall data flow.
*   **Setup:**
    1.  Ensure `sof$ta_bpr_apn` is empty.
    2.  Insert sample data into `sof$ta_bpr_apn` that includes multiple entries for the same `cntrct_id` to test aggregation.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with valid parameters.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully without raising an exception.
    *   The `sof$ta_apn_vertrag` table contains the expected number of rows (distinct `cntrct_id`s).
    *   Each row in `sof$ta_apn_vertrag` has `access_point_names` and `contract_refs` correctly aggregated, comma-separated, and sorted alphabetically.
    *   The `job_audit` table has one entry with `status = 'SUCCESS'` and `record_count` matching the number of rows in `sof$ta_apn_vertrag`.
    *   The `error_log` table is empty.

```python
def test_happy_path_basic_transformation(bq_client, setup_teardown_tables):
    # Setup: Insert sample data into sof$ta_bpr_apn
    sample_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "CR1", "access_point_name": "APN_X"},
        {"cntrct_id": "C1", "bpr_id": "B2", "cntrct_id_ref": "CR2", "access_point_name": "APN_Y"},
        {"cntrct_id": "C1", "bpr_id": "B3", "cntrct_id_ref": "CR3", "access_point_name": "APN_Z"},
        {"cntrct_id": "C2", "bpr_id": "B4", "cntrct_id_ref": "CR4", "access_point_name": "APN_A"},
        {"cntrct_id": "C2", "bpr_id": "B5", "cntrct_id_ref": "CR5", "access_point_name": "APN_B"},
        {"cntrct_id": "C3", "bpr_id": "B6", "cntrct_id_ref": "CR6", "access_point_name": "APN_C"},
    ]
    insert_into_bpr_apn(bq_client, sample_data)

    # Action: Call the stored procedure
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB1", "ENTRY1", today_str, 0).result()

    # Assertions
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag", order_by_col="contract_id")
    assert len(target_rows) == 3 # Expect 3 distinct contract_ids

    # Expected aggregated data (sorted)
    expected_c1_apns = "APN_X, APN_Y, APN_Z"
    expected_c1_refs = "CR1, CR2, CR3"
    expected_c2_apns = "APN_A, APN_B"
    expected_c2_refs = "CR4, CR5"
    expected_c3_apns = "APN_C"
    expected_c3_refs = "CR6"

    # Verify C1
    c1_row = next((row for row in target_rows if row['contract_id'] == 'C1'), None)
    assert c1_row is not None
    assert c1_row['access_point_names'] == expected_c1_apns
    assert c1_row['contract_refs'] == expected_c1_refs

    # Verify C2
    c2_row = next((row for row in target_rows if row['contract_id'] == 'C2'), None)
    assert c2_row is not None
    assert c2_row['access_point_names'] == expected_c2_apns
    assert c2_row['contract_refs'] == expected_c2_refs

    # Verify C3
    c3_row = next((row for row in target_rows if row['contract_id'] == 'C3'), None)
    assert c3_row is not None
    assert c3_row['access_point_names'] == expected_c3_apns
    assert c3_row['contract_refs'] == expected_c3_refs

    # Verify audit log
    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['record_count'] == 3
    assert audit_entry['job_kennung'] == 'JOB1'
    assert audit_entry['eintrags_nr'] == 'ENTRY1'
    assert audit_entry['stichtag'] == datetime.strptime(today_str, "%d%m%Y").date()

    # Verify error log is empty
    error_logs = get_table_rows(bq_client, "error_log")
    assert len(error_logs) == 0
```

#### Test Case 1.2: Empty Source Table (`sof$ta_bpr_apn`)

*   **Purpose:** Verify the job handles an empty source table gracefully, resulting in an empty target table and correct record count in the audit log.
*   **Setup:** Ensure `sof$ta_bpr_apn` is empty.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with valid parameters.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully.
    *   The `sof$ta_apn_vertrag` table is empty.
    *   The `job_audit` table has one entry with `status = 'SUCCESS'` and `record_count = 0`.
    *   The `error_log` table is empty.

```python
def test_empty_source_table(bq_client, setup_teardown_tables):
    # Setup: sof$ta_bpr_apn is already empty by fixture

    # Action: Call the stored procedure
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_EMPTY", "ENTRY_EMPTY", today_str, 0).result()

    # Assertions
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag")
    assert len(target_rows) == 0

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['record_count'] == 0

    error_logs = get_table_rows(bq_client, "error_log")
    assert len(error_logs) == 0
```

#### Test Case 1.3: NULL Values in Source Columns

*   **Purpose:** Verify `STRING_AGG` correctly handles `NULL` values in `access_point_name` and `cntrct_id_ref` by ignoring them, preventing `NULL` or extra delimiters in the aggregated string.
*   **Setup:** Insert sample data into `sof$ta_bpr_apn` including rows with `NULL` for `access_point_name` or `cntrct_id_ref`.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully.
    *   The aggregated strings in `sof$ta_apn_vertrag` do not contain `NULL` values or leading/trailing/double commas due to `NULL`s.

```python
def test_null_values_in_source_columns(bq_client, setup_teardown_tables):
    # Setup: Insert data with NULLs
    sample_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "CR1", "access_point_name": "APN_X"},
        {"cntrct_id": "C1", "bpr_id": "B2", "cntrct_id_ref": None, "access_point_name": "APN_Y"}, # NULL ref
        {"cntrct_id": "C1", "bpr_id": "B3", "cntrct_id_ref": "CR3", "access_point_name": None},    # NULL apn
        {"cntrct_id": "C2", "bpr_id": "B4", "cntrct_id_ref": None, "access_point_name": None},     # Both NULL
        {"cntrct_id": "C3", "bpr_id": "B5", "cntrct_id_ref": "CR5", "access_point_name": "APN_Z"},
    ]
    insert_into_bpr_apn(bq_client, sample_data)

    # Action: Call the stored procedure
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_NULLS", "ENTRY_NULLS", today_str, 0).result()

    # Assertions
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag", order_by_col="contract_id")
    assert len(target_rows) == 3 # C1, C2, C3

    c1_row = next((row for row in target_rows if row['contract_id'] == 'C1'), None)
    assert c1_row['access_point_names'] == "APN_X, APN_Y" # APN_Z (NULL) is ignored
    assert c1_row['contract_refs'] == "CR1, CR3" # CR2 (NULL) is ignored

    c2_row = next((row for row in target_rows if row['contract_id'] == 'C2'), None)
    assert c2_row['access_point_names'] is None # Both were NULL
    assert c2_row['contract_refs'] is None # Both were NULL

    c3_row = next((row for row in target_rows if row['contract_id'] == 'C3'), None)
    assert c3_row['access_point_names'] == "APN_Z"
    assert c3_row['contract_refs'] == "CR5"

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['record_count'] == 3
```

#### Test Case 1.4: Max Length Truncation (100 characters)

*   **Purpose:** Verify that the `SUBSTR(..., 1, 100)` function correctly truncates aggregated strings that exceed 100 characters.
*   **Setup:** Insert sample data into `sof$ta_bpr_apn` such that the aggregated `access_point_name` or `cntrct_id_ref` for a `cntrct_id` would naturally exceed 100 characters.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully.
    *   The `access_point_names` and `contract_refs` columns in `sof$ta_apn_vertrag` are exactly 100 characters long where truncation was expected.

```python
def test_max_length_truncation(bq_client, setup_teardown_tables):
    # Setup: Create long strings for aggregation
    long_apn_part = "A" * 40
    long_ref_part = "R" * 40
    sample_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": long_ref_part + "1", "access_point_name": long_apn_part + "1"},
        {"cntrct_id": "C1", "bpr_id": "B2", "cntrct_id_ref": long_ref_part + "2", "access_point_name": long_apn_part + "2"},
        {"cntrct_id": "C1", "bpr_id": "B3", "cntrct_id_ref": long_ref_part + "3", "access_point_name": long_apn_part + "3"},
    ]
    insert_into_bpr_apn(bq_client, sample_data)
    # Expected aggregated string length: 3 * 41 (part) + 2 * 2 (commas) = 123 + 4 = 127. Should be truncated to 100.

    # Action: Call the stored procedure
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_TRUNC", "ENTRY_TRUNC", today_str, 0).result()

    # Assertions
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag")
    assert len(target_rows) == 1
    row = target_rows[0]

    expected_apns_full = f"{long_apn_part}1, {long_apn_part}2, {long_apn_part}3"
    expected_refs_full = f"{long_ref_part}1, {long_ref_part}2, {long_ref_part}3"

    assert len(row['access_point_names']) == 100
    assert row['access_point_names'] == expected_apns_full[:100]

    assert len(row['contract_refs']) == 100
    assert row['contract_refs'] == expected_refs_full[:100]

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['record_count'] == 1
```

#### Test Case 1.5: Ordering of Aggregated Strings

*   **Purpose:** Verify that the `ORDER BY` clause within `STRING_AGG` ensures the aggregated elements are sorted alphabetically.
*   **Setup:** Insert sample data into `sof$ta_bpr_apn` with multiple `access_point_name` and `cntrct_id_ref` values for a single `cntrct_id` in a non-alphabetical order.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully.
    *   The aggregated strings in `sof$ta_apn_vertrag` are comma-separated and alphabetically ordered.

```python
def test_ordering_of_aggregated_strings(bq_client, setup_teardown_tables):
    # Setup: Insert data in non-alphabetical order
    sample_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "CR_Z", "access_point_name": "APN_C"},
        {"cntrct_id": "C1", "bpr_id": "B2", "cntrct_id_ref": "CR_A", "access_point_name": "APN_B"},
        {"cntrct_id": "C1", "bpr_id": "B3", "cntrct_id_ref": "CR_M", "access_point_name": "APN_A"},
    ]
    insert_into_bpr_apn(bq_client, sample_data)

    # Action: Call the stored procedure
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_ORDER", "ENTRY_ORDER", today_str, 0).result()

    # Assertions
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag")
    assert len(target_rows) == 1
    row = target_rows[0]

    assert row['contract_id'] == 'C1'
    assert row['access_point_names'] == "APN_A, APN_B, APN_C" # Should be sorted
    assert row['contract_refs'] == "CR_A, CR_M, CR_Z" # Should be sorted

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['record_count'] == 1
```

---

### 2. Parameter Handling & Validation

#### Test Case 2.1: Missing `p_JobKennung`

*   **Purpose:** Verify the `p_JobKennung` parameter validation (legacy `pruefeParameterGesetzt`) is correctly migrated.
*   **Setup:** None.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with `p_JobKennung` as `NULL`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call raises a BigQuery exception.
    *   The `job_audit` table has one entry with `status = 'FAILED'` and `error_message` containing "Parameter Jobkennung ist nicht gesetzt."
    *   The `error_log` table has one entry with `error_message` containing "Parameter Jobkennung ist nicht gesetzt." and `error_code` (or similar) reflecting `v_ErrNr = 193`.

```python
def test_missing_jobkennung_parameter(bq_client, setup_teardown_tables):
    # Action: Call the stored procedure with missing Jobkennung
    today_str = datetime.now().strftime("%d%m%Y")
    with pytest.raises(Exception) as excinfo:
        call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, None, "ENTRY1", today_str, 0).result()

    # Assertions for the raised exception
    assert "Parameter Jobkennung ist nicht gesetzt." in str(excinfo.value)

    # Assertions for audit and error logs
    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'FAILED'
    assert "Parameter Jobkennung ist nicht gesetzt." in audit_entry['error_message']

    error_log_entry = get_single_error_log_entry(bq_client)
    assert "Parameter Jobkennung ist nicht gesetzt." in error_log_entry['error_message']
    # Note: The current SP doesn't explicitly set error_code in error_log,
    # but the message should reflect the original ErrNr.
```

#### Test Case 2.2: Missing `p_Stichtag`

*   **Purpose:** Verify the `p_Stichtag` parameter validation (legacy `pruefeParameterGesetzt`) is correctly migrated.
*   **Setup:** None.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with `p_Stichtag` as `NULL`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call raises a BigQuery exception.
    *   The `job_audit` table has one entry with `status = 'FAILED'` and `error_message` containing "Parameter Stichtag ist nicht gesetzt."
    *   The `error_log` table has one entry with `error_message` containing "Parameter Stichtag ist nicht gesetzt." and `error_code` (or similar) reflecting `v_ErrNr = 193`.

```python
def test_missing_stichtag_parameter(bq_client, setup_teardown_tables):
    # Action: Call the stored procedure with missing Stichtag
    with pytest.raises(Exception) as excinfo:
        call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB1", "ENTRY1", None, 0).result()

    assert "Parameter Stichtag ist nicht gesetzt." in str(excinfo.value)

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'FAILED'
    assert "Parameter Stichtag ist nicht gesetzt." in audit_entry['error_message']

    error_log_entry = get_single_error_log_entry(bq_client)
    assert "Parameter Stichtag ist nicht gesetzt." in error_log_entry['error_message']
```

#### Test Case 2.3: Missing `p_EintragsNr`

*   **Purpose:** Verify the `p_EintragsNr` parameter validation (legacy `pruefeParameterGesetzt`) is correctly migrated.
*   **Setup:** None.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with `p_EintragsNr` as `NULL`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call raises a BigQuery exception.
    *   The `job_audit` table has one entry with `status = 'FAILED'` and `error_message` containing "Parameter EintragsNr ist nicht gesetzt."
    *   The `error_log` table has one entry with `error_message` containing "Parameter EintragsNr ist nicht gesetzt." and `error_code` (or similar) reflecting `v_ErrNr = 193`.

```python
def test_missing_eintragsnr_parameter(bq_client, setup_teardown_tables):
    # Action: Call the stored procedure with missing EintragsNr
    today_str = datetime.now().strftime("%d%m%Y")
    with pytest.raises(Exception) as excinfo:
        call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB1", None, today_str, 0).result()

    assert "Parameter EintragsNr ist nicht gesetzt." in str(excinfo.value)

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'FAILED'
    assert "Parameter EintragsNr ist nicht gesetzt." in audit_entry['error_message']

    error_log_entry = get_single_error_log_entry(bq_client)
    assert "Parameter EintragsNr ist nicht gesetzt." in error_log_entry['error_message']
```

#### Test Case 2.4: Invalid `p_Stichtag` Format

*   **Purpose:** Verify the `p_Stichtag` date format validation (legacy `DWDate_Datum_Check`) is correctly migrated using `SAFE.PARSE_DATE`.
*   **Setup:** None.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with `p_Stichtag = '2023-01-01'` (incorrect format) or '32132023' (invalid day).
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call raises a BigQuery exception.
    *   The `job_audit` table has one entry with `status = 'FAILED'` and `error_message` containing "Ungueltiges Datumsformat fuer Stichtag".
    *   The `error_log` table has one entry with `error_message` containing "Ungueltiges Datumsformat fuer Stichtag" and `error_code` (or similar) reflecting `v_ErrNr = 194`.

```python
def test_invalid_stichtag_format(bq_client, setup_teardown_tables):
    # Action: Call the stored procedure with an invalid Stichtag format
    with pytest.raises(Exception) as excinfo:
        call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB1", "ENTRY1", "2023-01-01", 0).result() # YYYY-MM-DD instead of DDMMYYYY

    assert "Ungueltiges Datumsformat fuer Stichtag" in str(excinfo.value)

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'FAILED'
    assert "Ungueltiges Datumsformat fuer Stichtag" in audit_entry['error_message']

    error_log_entry = get_single_error_log_entry(bq_client)
    assert "Ungueltiges Datumsformat fuer Stichtag" in error_log_entry['error_message']
```

#### Test Case 2.5: `p_wiederanlaufWert` Defaulting

*   **Purpose:** Verify that `p_wiederanlaufWert` correctly defaults to `0` if `NULL` is passed, mimicking the `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi` logic.
*   **Setup:** Insert sample data into `sof$ta_bpr_apn` for a successful run.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with `p_wiederanlaufWert = NULL`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully.
    *   The `job_audit` entry shows `wiederanlauf_wert` as `0` (if the audit table DDL and SP were updated to log this parameter, which they currently are not). The primary criterion is successful execution, indicating the `COALESCE` handled the `NULL` without error.

```python
def test_wiederanlaufwert_defaulting(bq_client, setup_teardown_tables):
    # Setup: Insert sample data for a successful run
    insert_into_bpr_apn(bq_client, [{"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "CR1", "access_point_name": "APN_X"}])

    # Action: Call the stored procedure with p_wiederanlaufWert as NULL
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_DEF", "ENTRY_DEF", today_str, None).result()

    # Assertions
    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    # Note: The current job_audit table DDL and SP do not explicitly log p_wiederanlaufWert.
    # If they did, we would assert audit_entry['wiederanlauf_wert'] == 0.
    # For now, successful execution is the primary indicator.
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag")
    assert len(target_rows) == 1 # Ensure data was processed
```

---

### 3. External System Replacements (Audit/Error Logging)

#### Test Case 3.1: `job_audit` Table Population (Start and Success)

*   **Purpose:** Verify that the `job_audit` table is correctly populated at the start and updated upon successful completion of the job, replacing the legacy `FOSJobErzeugeEintrag` functionality.
*   **Setup:** Insert sample data into `sof$ta_bpr_apn` for a successful run.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp` with valid parameters.
*   **Pass/Fail Criterion:**
    *   Before the call completes, if we could query the `job_audit` table, there would be an entry with `status = 'RUNNING'`.
    *   After the call completes, the `job_audit` table contains exactly one entry.
    *   This entry has `status = 'SUCCESS'`, `start_time` and `end_time` are populated, `job_kennung`, `eintrags_nr`, `stichtag` match the input parameters, and `record_count` is accurate.
    *   `error_message` is `NULL`.

```python
def test_job_audit_success_logging(bq_client, setup_teardown_tables):
    # Setup: Insert sample data
    insert_into_bpr_apn(bq_client, [{"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "CR1", "access_point_name": "APN_X"}])

    # Action: Call the stored procedure
    job_kennung = "AUDIT_JOB"
    eintrags_nr = "AUDIT_ENTRY"
    stichtag_str = datetime.now().strftime("%d%m%Y")
    stichtag_date = datetime.strptime(stichtag_str, "%d%m%Y").date()

    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, job_kennung, eintrags_nr, stichtag_str, 0).result()

    # Assertions
    audit_entries = get_table_rows(bq_client, "job_audit")
    assert len(audit_entries) == 1
    audit_entry = audit_entries[0]

    assert audit_entry['job_name'] == 'k_ausd_bp_ta_apn_vertrag'
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['start_time'] is not None
    assert audit_entry['end_time'] is not None
    assert audit_entry['end_time'] >= audit_entry['start_time']
    assert audit_entry['job_kennung'] == job_kennung
    assert audit_entry['eintrags_nr'] == eintrags_nr
    assert audit_entry['stichtag'] == stichtag_date
    assert audit_entry['record_count'] == 1 # One distinct contract_id
    assert audit_entry['error_message'] is None

    error_logs = get_table_rows(bq_client, "error_log")
    assert len(error_logs) == 0
```

#### Test Case 3.2: `job_audit` and `error_log` Table Population (Failure)

*   **Purpose:** Verify that `job_audit` and `error_log` tables are correctly populated when the job fails, replacing legacy `DWMSG_MeldeFehler` and `exit $ErrNr` behavior.
*   **Setup:** Create a scenario that causes `d_ausd_bp_ta_apn_vertrag_proc` to fail. For this test, we'll simulate by dropping the source table `sof$ta_bpr_apn` before the call.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call raises a BigQuery exception.
    *   The `job_audit` table has one entry with `status = 'FAILED'`, `start_time` and `end_time` populated, and `error_message` containing details about the failure.
    *   The `error_log` table has one entry with `job_name`, `error_message`, and `error_stack` populated.

```python
def test_job_audit_failure_logging(bq_client, setup_teardown_tables):
    # Setup: Cause d_ausd_bp_ta_apn_vertrag_proc to fail by dropping its source table
    # Note: This is a destructive setup for testing failure scenarios.
    # In a real environment, you might mock the called procedure or use a test version.
    bq_client.query(f"DROP TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_bpr_apn`").result()

    # Action: Call the stored procedure
    job_kennung = "FAIL_JOB"
    eintrags_nr = "FAIL_ENTRY"
    stichtag_str = datetime.now().strftime("%d%m%Y")

    with pytest.raises(Exception) as excinfo:
        call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, job_kennung, eintrags_nr, stichtag_str, 0).result()

    # Assertions for the raised exception
    assert "Not found: Table" in str(excinfo.value) or "Table not found" in str(excinfo.value)

    # Assertions for audit log
    audit_entries = get_table_rows(bq_client, "job_audit")
    assert len(audit_entries) == 1
    audit_entry = audit_entries[0]

    assert audit_entry['job_name'] == 'k_ausd_bp_ta_apn_vertrag'
    assert audit_entry['status'] == 'FAILED'
    assert audit_entry['start_time'] is not None
    assert audit_entry['end_time'] is not None
    assert audit_entry['end_time'] >= audit_entry['start_time']
    assert audit_entry['job_kennung'] == job_kennung
    assert audit_entry['eintrags_nr'] == eintrags_nr
    assert audit_entry['stichtag'] == datetime.strptime(stichtag_str, "%d%m%Y").date()
    assert audit_entry['record_count'] is None # No records processed on failure
    assert "Not found: Table" in audit_entry['error_message'] or "Table not found" in audit_entry['error_message']

    # Assertions for error log
    error_logs = get_table_rows(bq_client, "error_log")
    assert len(error_logs) == 1
    error_log_entry = error_logs[0]

    assert error_log_entry['job_name'] == 'k_ausd_bp_ta_apn_vertrag'
    assert "Not found: Table" in error_log_entry['error_message'] or "Table not found" in error_log_entry['error_message']
    assert error_log_entry['error_stack'] is not None
    assert error_log_entry['error_timestamp'] is not None
```

---

### 4. Data Quality / Row-Count / Schema Assertions

#### Test Case 4.1: Record Count Accuracy

*   **Purpose:** Verify that the `record_count` logged in the `job_audit` table accurately reflects the number of rows inserted into the target table `sof$ta_apn_vertrag`. This replaces the legacy temporary file (`.tmp`) record count mechanism.
*   **Setup:** Insert sample data into `sof$ta_bpr_apn` that will result in a known number of distinct `cntrct_id`s (and thus rows in `sof$ta_apn_vertrag`).
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully.
    *   The `record_count` in the `job_audit` entry equals the actual `COUNT(*)` from `sof$ta_apn_vertrag`.

```python
def test_record_count_accuracy(bq_client, setup_teardown_tables):
    # Setup: Insert data leading to 5 distinct contract_ids
    sample_data = [
        {"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "CR1", "access_point_name": "APN_X"},
        {"cntrct_id": "C1", "bpr_id": "B2", "cntrct_id_ref": "CR2", "access_point_name": "APN_Y"},
        {"cntrct_id": "C2", "bpr_id": "B3", "cntrct_id_ref": "CR3", "access_point_name": "APN_Z"},
        {"cntrct_id": "C3", "bpr_id": "B4", "cntrct_id_ref": "CR4", "access_point_name": "APN_A"},
        {"cntrct_id": "C4", "bpr_id": "B5", "cntrct_id_ref": "CR5", "access_point_name": "APN_B"},
        {"cntrct_id": "C5", "bpr_id": "B6", "cntrct_id_ref": "CR6", "access_point_name": "APN_C"},
    ]
    insert_into_bpr_apn(bq_client, sample_data)
    expected_target_rows = 5

    # Action: Call the stored procedure
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_COUNT", "ENTRY_COUNT", today_str, 0).result()

    # Assertions
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag")
    assert len(target_rows) == expected_target_rows

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['record_count'] == expected_target_rows
```

#### Test Case 4.2: Schema Conformance of Target Table (`sof$ta_apn_vertrag`)

*   **Purpose:** Verify that the schema of the target table `sof$ta_apn_vertrag` matches the expected structure (column names and data types).
*   **Setup:** A successful run of the job.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the target table.
*   **Pass/Fail Criterion:**
    *   The table `sof$ta_apn_vertrag` exists.
    *   It contains the columns `contract_id`, `access_point_names`, and `contract_refs`.
    *   All these columns are of type `STRING`.

```python
def test_target_table_schema_conformance(bq_client, setup_teardown_tables):
    # Setup: Ensure a successful run to create/populate the table if it didn't exist
    insert_into_bpr_apn(bq_client, [{"cntrct_id": "C1", "bpr_id": "B1", "cntrct_id_ref": "CR1", "access_point_name": "APN_X"}])
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_SCHEMA", "ENTRY_SCHEMA", today_str, 0).result()

    # Action: Query INFORMATION_SCHEMA
    query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'sof$ta_apn_vertrag'
        ORDER BY column_name
    """
    schema_info = {row.column_name: row.data_type for row in bq_client.query(query).result()}

    # Assertions
    expected_schema = {
        'contract_id': 'STRING',
        'access_point_names': 'STRING',
        'contract_refs': 'STRING'
    }
    assert schema_info == expected_schema
```

#### Test Case 4.3: `TRUNCATE` Behavior

*   **Purpose:** Verify that `d_ausd_bp_ta_apn_vertrag_proc` correctly truncates the target table `sof$ta_apn_vertrag` before inserting new data, ensuring idempotency and preventing duplicate data.
*   **Setup:**
    1.  Insert some dummy data into `sof$ta_apn_vertrag`.
    2.  Insert new, distinct sample data into `sof$ta_bpr_apn`.
*   **Action:** Call `k_ausd_bp_ta_apn_vertrag_sp`.
*   **Pass/Fail Criterion:**
    *   The `k_ausd_bp_ta_apn_vertrag_sp` call completes successfully.
    *   The `sof$ta_apn_vertrag` table contains only the data generated by the current run, and none of the initial dummy data.

```python
def test_truncate_behavior(bq_client, setup_teardown_tables):
    # Setup: Insert dummy data into target table
    dummy_data = [
        {"contract_id": "OLD_C1", "access_point_names": "OLD_APN1", "contract_refs": "OLD_CR1"},
        {"contract_id": "OLD_C2", "access_point_names": "OLD_APN2", "contract_refs": "OLD_CR2"},
    ]
    insert_into_apn_vertrag(bq_client, dummy_data)

    # Setup: Insert new source data
    new_source_data = [
        {"cntrct_id": "NEW_C1", "bpr_id": "B1", "cntrct_id_ref": "NEW_CR1", "access_point_name": "NEW_APN1"},
    ]
    insert_into_bpr_apn(bq_client, new_source_data)

    # Action: Call the stored procedure
    today_str = datetime.now().strftime("%d%m%Y")
    call_k_ausd_bp_ta_apn_vertrag_sp(bq_client, "JOB_TRUNCATE", "ENTRY_TRUNCATE", today_str, 0).result()

    # Assertions
    target_rows = get_table_rows(bq_client, "sof$ta_apn_vertrag", order_by_col="contract_id")
    assert len(target_rows) == 1 # Only the new data should be present

    row = target_rows[0]
    assert row['contract_id'] == 'NEW_C1'
    assert row['access_point_names'] == 'NEW_APN1'
    assert row['contract_refs'] == 'NEW_CR1'

    # Ensure old data is gone
    assert not any(r['contract_id'].startswith('OLD_') for r in target_rows)

    audit_entry = get_single_audit_entry(bq_client)
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['record_count'] == 1
```

---

### 5. Airflow Orchestration (Conceptual Tests)

These tests are conceptual as they require a running Airflow environment and cannot be directly executed as `pytest` code blocks without significant mocking or integration with an Airflow test harness.

#### Test Case 5.1: Airflow DAG Execution - Happy Path

*   **Purpose:** Verify that the Airflow DAG successfully triggers the BigQuery Stored Procedure and completes without Airflow-level errors. This replaces the legacy shell-based job scheduling.
*   **Setup:**
    1.  Deploy the `k_ausd_bp_ta_apn_vertrag_workflow` DAG to an Airflow environment.
    2.  Ensure the BigQuery connection (`google_cloud_default`) is correctly configured and has necessary permissions.
    3.  Ensure `sof$ta_bpr_apn` contains valid data for a successful run.
*   **Action:** Manually trigger the `k_ausd_bp_ta_apn_vertrag_workflow` DAG with default parameters.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes successfully (status `success`).
    *   The `call_k_ausd_bp_ta_apn_vertrag_sp` task within the DAG completes successfully.
    *   The `job_audit` table in BigQuery shows a successful entry for the job.
    *   The `error_log` table in BigQuery is empty.

#### Test Case 5.2: Airflow DAG Parameter Passing

*   **Purpose:** Verify that parameters defined in the Airflow DAG (e.g., `params` or `ds_nodash`) are correctly passed to the BigQuery Stored Procedure.
*   **Setup:**
    1.  Deploy the `k_ausd_bp_ta_apn_vertrag_workflow` DAG.
    2.  Ensure `sof$ta_bpr_apn` contains valid data.
*   **Action:** Manually trigger the DAG, overriding `job_kennung`, `eintrags_nr`, `stichtag`, and `wiederanlauf_wert` with specific, non-default values via the Airflow UI or CLI.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes successfully.
    *   The `job_audit` entry in BigQuery reflects the *overridden* `job_kennung`, `eintrags_nr`, `stichtag`, and `wiederanlauf_wert` (if logged) parameters, not the DAG's default values.

---

### Potential Improvements / Further Considerations

*   **`job_audit_id` and `error_id`:** The current DDL for `job_audit` and `error_log` tables includes `INT64` IDs, but the BigQuery SPs do not populate them. In BigQuery, these would typically be generated using `GENERATE_UUID()` or a sequence-like mechanism if a unique identifier is required. This is a minor discrepancy between DDL and implementation.
*   **`wiederanlauf_wert` in `job_audit`:** The `job_audit` DDL includes `wiederanlauf_wert`, but the SP does not insert this value. It would be beneficial to log this parameter for full auditability.
*   **`v_datum_heute` and `v_datum_gestern` Logging:** The derived dates are not explicitly logged. If their exact values are critical for auditing or debugging, the `job_audit` table could be extended to include them.
*   **Commented-out Legacy Logic:** The design explicitly states that commented-out `sed`, `sort`, `join` logic is assumed inactive. If this assumption changes, new tests would be required to validate their BigQuery equivalents (e.g., Dataflow or complex SQL).
*   **`d_ausd_bp_ta_apn_vertrag.sql` Content:** The tests focus on the orchestration script. A separate, detailed test plan for the migrated `d_ausd_bp_ta_apn_vertrag.sql` (now `d_ausd_bp_ta_apn_vertrag_proc`) would be crucial to cover all its specific business logic, joins, filters, and edge cases. This document assumes the provided `d_ausd_bp_ta_apn_vertrag_proc` is the complete and correct migration of that SQL.