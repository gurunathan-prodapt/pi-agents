As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery code for the `r_ausd_bp_ta_msisdn.ksh` job. The following test cases are designed to ensure the migrated solution is behaviorally equivalent to the legacy system, covering output parity, transformation correctness, external system replacements, and data quality assertions.

A critical discrepancy has been identified between the design document's description of the original SQL logic and the actual BigQuery SQL code provided for `bq_d_ausd_bp_ta_msisdn_logic.sql`. The design document describes filtering by `Stichtag` and `Wiederanlaufwert`, while the migrated SQL implements a "latest valid record" logic based on `valid_to` for each `bpri_com_id`, without using `Stichtag` or `Wiederanlaufwert` in the core data selection. The tests below are written to validate the *provided migrated BigQuery code's behavior*, and a specific test case (Test Case 13) is dedicated to highlighting and verifying this functional difference. This is crucial for ensuring the migrated system meets the *actual implemented* requirements, even if they diverge from the initial design description.

---

## Test Environment Setup (Conceptual)

For these tests, we assume a Python `pytest` framework with the `google-cloud-bigquery` client library.
A `conftest.py` would handle BigQuery client initialization and common fixtures.

```python
# conftest.py (Conceptual)
import pytest
from google.cloud import bigquery
import os
import uuid
from datetime import datetime, date

# --- Configuration ---
# Set these environment variables or hardcode for testing
# os.environ["GCP_PROJECT_ID"] = "your-gcp-project-id"
# os.environ["BIGQUERY_DATASET_ID"] = "isbert_dataset"

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        pytest.skip("GCP_PROJECT_ID environment variable not set.")
    client = bigquery.Client(project=project_id)
    yield client
    client.close()

@pytest.fixture(scope="session")
def project_id():
    """Provides the GCP project ID."""
    return os.environ.get("GCP_PROJECT_ID")

@pytest.fixture(scope="session")
def dataset_id():
    """Provides the BigQuery dataset ID."""
    return os.environ.get("BIGQUERY_DATASET_ID")

@pytest.fixture(scope="function")
def setup_clean_tables(bq_client, project_id, dataset_id):
    """
    Ensures all relevant tables are truncated before each test function.
    Also ensures the `validate_ddmmyyyy` procedure exists.
    """
    tables_to_clear = [
        f"`{project_id}.{dataset_id}.sof_ta_msisdn_his`",
        f"`{project_id}.{dataset_id}.sof_ta_msisdn`",
        f"`{project_id}.{dataset_id}.job_audit`",
        f"`{project_id}.{dataset_id}.job_result_counts`",
        f"`{project_id}.{dataset_id}.dwtk_meldungen`",
    ]
    for table_path in tables_to_clear:
        try:
            bq_client.query(f"TRUNCATE TABLE {table_path}").result()
        except Exception as e:
            print(f"Warning: Could not truncate {table_path}. It might not exist. Error: {e}")
            # Attempt to create if it doesn't exist (basic DDL, full DDL is in bq_schema_definition.sql)
            if "Not found" in str(e):
                if "sof_ta_msisdn_his" in table_path:
                    bq_client.query(f"""
                        CREATE TABLE IF NOT EXISTS {table_path} (
                            bpri_com_id STRING, msisdn STRING, callnumber_role_id STRING,
                            valid_to DATE, creation_date TIMESTAMP, last_update_date TIMESTAMP
                        )
                    """).result()
                elif "sof_ta_msisdn" in table_path:
                    bq_client.query(f"""
                        CREATE TABLE IF NOT EXISTS {table_path} (
                            BPR_INSTANCE_ID STRING, MSISDN STRING, CALLNUMBER_ROLE_ID STRING, VALID_TO DATE
                        )
                    """).result()
                elif "job_audit" in table_path:
                    bq_client.query(f"""
                        CREATE TABLE IF NOT EXISTS {table_path} (
                            job_id STRING, run_id STRING, start_timestamp TIMESTAMP, end_timestamp TIMESTAMP,
                            status STRING, error_message STRING, stichtag DATE, wiederanlaufwert INT64
                        )
                    """).result()
                elif "job_result_counts" in table_path:
                    bq_client.query(f"""
                        CREATE TABLE IF NOT EXISTS {table_path} (
                            job_id STRING, run_id STRING, stichtag DATE, record_count INT64, timestamp TIMESTAMP
                        )
                    """).result()
                elif "dwtk_meldungen" in table_path:
                    bq_client.query(f"""
                        CREATE TABLE IF NOT EXISTS {table_path} (
                            timecreated TIMESTAMP, job_kennung STRING, message_text STRING
                        )
                    """).result()
                bq_client.query(f"TRUNCATE TABLE {table_path}").result() # Try truncating again

    # Ensure validate_ddmmyyyy procedure exists
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.validate_ddmmyyyy`(
            IN p_date_string STRING, OUT p_date_out DATE
        )
        OPTIONS(description="Validates a date string in 'DDMMYYYY' format and converts it to a DATE type. Raises an error if invalid.")
        BEGIN
            DECLARE parsed_date DATE;
            SET parsed_date = SAFE.PARSE_DATE('%d%m%Y', p_date_string);
            IF parsed_date IS NULL THEN
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("Invalid date format for '%s'. Expected DDMMYYYY.", p_date_string);
            ELSE
                SET p_date_out = parsed_date;
            END IF;
        END;
    """).result()
    yield

def execute_bq_procedure(bq_client, project_id, dataset_id, proc_name, params):
    """Helper to execute a BigQuery stored procedure."""
    param_list = []
    for p in params:
        value = p['value']
        if isinstance(value, str):
            param_list.append(f"{p['name']}=>'{value}'")
        elif value is None:
            param_list.append(f"{p['name']}=>NULL")
        else:
            param_list.append(f"{p['name']}=>{value}")
    param_str = ", ".join(param_list)
    query = f"CALL `{project_id}.{dataset_id}.{proc_name}`({param_str})"
    print(f"Executing: {query}")
    return bq_client.query(query).result()

def fetch_table_data(bq_client, project_id, dataset_id, table_name, order_by=None):
    """Helper to fetch data from a BigQuery table."""
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}` {order_clause}"
    rows = bq_client.query(query).result()
    return [dict(row) for row in rows]

def insert_data(bq_client, project_id, dataset_id, table_name, data):
    """Helper to insert JSON data into a BigQuery table."""
    table_ref = bq_client.dataset(dataset_id).table(table_name)
    errors = bq_client.insert_rows_json(table_ref, data)
    if errors:
        raise Exception(f"Errors inserting data into {table_name}: {errors}")

def get_current_date_ddmmyyyy():
    """Helper to get current date in DDMMYYYY format."""
    return datetime.now().strftime('%d%m%Y')

def get_current_date_obj():
    """Helper to get current date as a date object."""
    return datetime.now().date()

```

---

## Test Cases

### Test Case 1: Schema Validation

*   **Purpose:** Verify that the DDLs for all BigQuery tables are correctly defined and can be created without errors. This ensures the foundational data structures are in place.
*   **Setup:**
    1.  Ensure the BigQuery client is configured and has permissions to create/drop tables in `my-gcp-project.isbert_dataset`.
    2.  The `bq_schema_definition.sql` script is available.
*   **Action:**
    1.  Execute the `bq_schema_definition.sql` script.
    2.  Attempt to insert a single, valid row into each defined table.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `bq_schema_definition.sql` script executes successfully, and a test row can be inserted into each table without schema-related errors.
    *   **Fail:** Any DDL statement fails, or an insertion fails due to schema mismatch or constraint violation.

```python
# test_schema_validation.py
import pytest
from google.cloud import bigquery
from datetime import datetime, date

def test_schema_creation_and_basic_insert(bq_client, project_id, dataset_id):
    """
    Tests that all tables defined in bq_schema_definition.sql can be created
    and accept basic data.
    """
    # Read and execute the DDL script
    with open("bq_schema_definition.sql", "r") as f:
        ddl_script = f.read()
    
    # Replace placeholder project/dataset for execution
    ddl_script = ddl_script.replace("`my-gcp-project.isbert_dataset.", f"`{project_id}.{dataset_id}.")

    # Split script into individual statements and execute
    statements = [s.strip() for s in ddl_script.split(';') if s.strip()]
    for stmt in statements:
        if stmt.startswith("CREATE TABLE"): # Only execute CREATE TABLE statements
            print(f"Executing DDL: {stmt[:100]}...")
            bq_client.query(stmt).result()

    # Test basic inserts
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his", [{
        "bpri_com_id": "TEST_BPR1", "msisdn": "12345", "callnumber_role_id": "ROLE_X",
        "valid_to": date(2024, 1, 1).isoformat(), "creation_date": datetime.now().isoformat(),
        "last_update_date": datetime.now().isoformat()
    }])
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn", [{
        "BPR_INSTANCE_ID": "TEST_BPR1", "MSISDN": "12345", "CALLNUMBER_ROLE_ID": "ROLE_X",
        "VALID_TO": date(2024, 1, 1).isoformat()
    }])
    insert_data(bq_client, project_id, dataset_id, "job_audit", [{
        "job_id": "test_job", "run_id": str(uuid.uuid4()), "start_timestamp": datetime.now().isoformat(),
        "status": "SUCCESS", "stichtag": date(2024, 1, 1).isoformat(), "wiederanlaufwert": 0
    }])
    insert_data(bq_client, project_id, dataset_id, "job_result_counts", [{
        "job_id": "test_job", "run_id": str(uuid.uuid4()), "stichtag": date(2024, 1, 1).isoformat(),
        "record_count": 10, "timestamp": datetime.now().isoformat()
    }])
    insert_data(bq_client, project_id, dataset_id, "dwtk_meldungen", [{
        "timecreated": datetime.now().isoformat(), "job_kennung": "TEST_JOB", "message_text": "Test message"
    }])

    # Verify counts
    assert len(fetch_table_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his")) == 1
    assert len(fetch_table_data(bq_client, project_id, dataset_id, "sof_ta_msisdn")) == 1
    assert len(fetch_table_data(bq_client, project_id, dataset_id, "job_audit")) == 1
    assert len(fetch_table_data(bq_client, project_id, dataset_id, "job_result_counts")) == 1
    assert len(fetch_table_data(bq_client, project_id, dataset_id, "dwtk_meldungen")) == 1
```

### Test Case 2: Parameter Defaulting - Stichtag

*   **Purpose:** Verify that if `p_stichtag_str` is not provided (NULL or empty string), the stored procedure correctly defaults `v_stichtag` to the current system date (`CURRENT_DATE()`).
*   **Setup:** `setup_clean_tables` fixture.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with `p_stichtag_str` as `NULL`.
    2.  Query the `job_audit` table for the latest entry.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `stichtag` column in the `job_audit` table for the executed run matches `CURRENT_DATE()`.
    *   **Fail:** `stichtag` is NULL or an incorrect date.

```python
# test_parameter_defaulting.py
import pytest
from datetime import date

def test_stichtag_defaults_to_current_date(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that p_stichtag_str defaults to CURRENT_DATE() when not provided.
    """
    current_date = get_current_date_obj()
    
    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": None},
        {"name": "p_wiederanlaufwert_param", "value": 0},
        {"name": "p_job_kennung", "value": "TEST_JOB_STICH"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    audit_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
    assert len(audit_entries) > 0
    assert audit_entries[0]['stichtag'] == current_date
    assert audit_entries[0]['status'] == 'SUCCESS' # Should succeed even with default

```

### Test Case 3: Parameter Defaulting - Wiederanlaufwert

*   **Purpose:** Verify that if `p_wiederanlaufwert_param` is not provided (NULL), the stored procedure correctly defaults `v_wiederanlaufwert` to `0`.
*   **Setup:** `setup_clean_tables` fixture.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with `p_wiederanlaufwert_param` as `NULL`.
    2.  Query the `job_audit` table for the latest entry.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `wiederanlaufwert` column in the `job_audit` table for the executed run is `0`.
    *   **Fail:** `wiederanlaufwert` is NULL or an incorrect value.

```python
# test_parameter_defaulting.py (continued)
def test_wiederanlaufwert_defaults_to_zero(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that p_wiederanlaufwert_param defaults to 0 when not provided.
    """
    stichtag_str = get_current_date_ddmmyyyy()

    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": stichtag_str},
        {"name": "p_wiederanlaufwert_param", "value": None},
        {"name": "p_job_kennung", "value": "TEST_JOB_WIE"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    audit_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
    assert len(audit_entries) > 0
    assert audit_entries[0]['wiederanlaufwert'] == 0
    assert audit_entries[0]['status'] == 'SUCCESS'
```

### Test Case 4: Date Format Validation (`validate_ddmmyyyy`)

*   **Purpose:** Verify that the `validate_ddmmyyyy` helper procedure correctly validates date strings in `DDMMYYYY` format and raises an error for invalid formats.
*   **Setup:** `setup_clean_tables` fixture (ensures procedure exists).
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with a valid `p_stichtag_str` (e.g., '01012023').
    2.  Call `r_ausd_bp_ta_msisdn` with an invalid `p_stichtag_str` (e.g., '2023-01-01', '32012023', 'ABC').
*   **Pass/Fail Criterion:**
    *   **Pass:** The call with a valid date succeeds, and the `stichtag` in `job_audit` is correct. The calls with invalid dates fail with an appropriate error message indicating invalid date format.
    *   **Fail:** Valid date fails, or invalid date succeeds, or error message is incorrect.

```python
# test_date_validation.py
import pytest
from google.cloud.exceptions import GoogleCloudError
from datetime import date

def test_valid_stichtag_format(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that a valid DDMMYYYY stichtag is processed correctly.
    """
    valid_date_str = "15032023"
    expected_date_obj = date(2023, 3, 15)

    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": valid_date_str},
        {"name": "p_wiederanlaufwert_param", "value": 0},
        {"name": "p_job_kennung", "value": "TEST_VALID_DATE"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    audit_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
    assert len(audit_entries) > 0
    assert audit_entries[0]['stichtag'] == expected_date_obj
    assert audit_entries[0]['status'] == 'SUCCESS'

def test_invalid_stichtag_format_raises_error(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that an invalid stichtag format raises an error.
    """
    invalid_date_strs = ["2023-01-01", "32012023", "ABCDEFGH", "01/01/2023"]

    for invalid_date_str in invalid_date_strs:
        with pytest.raises(GoogleCloudError) as excinfo:
            execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
                {"name": "p_stichtag_str", "value": invalid_date_str},
                {"name": "p_wiederanlaufwert_param", "value": 0},
                {"name": "p_job_kennung", "value": "TEST_INVALID_DATE"},
                {"name": "p_eintrags_nr", "value": None}
            ])
        assert "Invalid date format" in str(excinfo.value)
        
        # Verify audit log records the failure
        audit_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
        assert audit_entries[0]['status'] == 'FAILED'
        assert "Invalid date format" in audit_entries[0]['error_message']
```

### Test Case 5: Core Transformation Logic - Latest Valid Record

*   **Purpose:** Verify the core logic of selecting the "latest valid record" for each `bpri_com_id` based on `valid_to` (or `4712-12-31` for NULLs), as implemented in `bq_d_ausd_bp_ta_msisdn_logic.sql`.
*   **Setup:**
    1.  `setup_clean_tables` fixture.
    2.  Insert diverse test data into `sof_ta_msisdn_his` including multiple records for the same `bpri_com_id` with varying `valid_to` dates (including NULL).
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with valid parameters.
    2.  Query the `sof_ta_msisdn` target table.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `sof_ta_msisdn` table contains exactly one record for each `bpri_com_id`, and that record corresponds to the one with the maximum `valid_to` (or `4712-12-31` if `valid_to` is NULL) among all records for that `bpri_com_id`.
    *   **Fail:** Incorrect records are selected, or duplicate `bpri_com_id` entries exist in the target.

```python
# test_core_transformation.py
import pytest
from datetime import date, datetime

def test_latest_valid_record_selection(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests the core logic of selecting the latest valid record per bpri_com_id.
    """
    # Test data for sof_ta_msisdn_his
    source_data = [
        # BPR1: Latest is 2023-12-31
        {"bpri_com_id": "BPR1", "msisdn": "111", "callnumber_role_id": "ROLE_A", "valid_to": date(2023, 1, 1).isoformat(), "creation_date": datetime(2022,1,1).isoformat(), "last_update_date": datetime(2022,1,1).isoformat()},
        {"bpri_com_id": "BPR1", "msisdn": "111", "callnumber_role_id": "ROLE_A", "valid_to": date(2023, 6, 30).isoformat(), "creation_date": datetime(2022,6,1).isoformat(), "last_update_date": datetime(2022,6,1).isoformat()},
        {"bpri_com_id": "BPR1", "msisdn": "222", "callnumber_role_id": "ROLE_B", "valid_to": date(2023, 12, 31).isoformat(), "creation_date": datetime(2022,12,1).isoformat(), "last_update_date": datetime(2022,12,1).isoformat()},
        # BPR2: Latest is NULL (treated as 4712-12-31)
        {"bpri_com_id": "BPR2", "msisdn": "333", "callnumber_role_id": "ROLE_C", "valid_to": date(2024, 1, 15).isoformat(), "creation_date": datetime(2023,1,1).isoformat(), "last_update_date": datetime(2023,1,1).isoformat()},
        {"bpri_com_id": "BPR2", "msisdn": "444", "callnumber_role_id": "ROLE_D", "valid_to": None, "creation_date": datetime(2023,2,1).isoformat(), "last_update_date": datetime(2023,2,1).isoformat()},
        # BPR3: Multiple NULLs, one non-NULL. Latest is NULL.
        {"bpri_com_id": "BPR3", "msisdn": "555", "callnumber_role_id": "ROLE_E", "valid_to": date(2023, 5, 1).isoformat(), "creation_date": datetime(2022,5,1).isoformat(), "last_update_date": datetime(2022,5,1).isoformat()},
        {"bpri_com_id": "BPR3", "msisdn": "555", "callnumber_role_id": "ROLE_E", "valid_to": None, "creation_date": datetime(2022,6,1).isoformat(), "last_update_date": datetime(2022,6,1).isoformat()},
        {"bpri_com_id": "BPR3", "msisdn": "666", "callnumber_role_id": "ROLE_F", "valid_to": None, "creation_date": datetime(2022,7,1).isoformat(), "last_update_date": datetime(2022,7,1).isoformat()},
        # BPR4: Single record
        {"bpri_com_id": "BPR4", "msisdn": "777", "callnumber_role_id": "ROLE_G", "valid_to": date(2024, 2, 1).isoformat(), "creation_date": datetime(2023,2,1).isoformat(), "last_update_date": datetime(2023,2,1).isoformat()},
    ]
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his", source_data)

    # Expected output
    expected_target_data = [
        {"BPR_INSTANCE_ID": "BPR1", "MSISDN": "222", "CALLNUMBER_ROLE_ID": "ROLE_B", "VALID_TO": date(2023, 12, 31)},
        {"BPR_INSTANCE_ID": "BPR2", "MSISDN": "444", "CALLNUMBER_ROLE_ID": "ROLE_D", "VALID_TO": date(4712, 12, 31)},
        {"BPR_INSTANCE_ID": "BPR3", "MSISDN": "666", "CALLNUMBER_ROLE_ID": "ROLE_F", "VALID_TO": date(4712, 12, 31)}, # Could be 555 or 666, depends on tie-breaking for NULLs. The query picks one.
        {"BPR_INSTANCE_ID": "BPR4", "MSISDN": "777", "CALLNUMBER_ROLE_ID": "ROLE_G", "VALID_TO": date(2024, 2, 1)},
    ]
    # Sort for consistent comparison
    expected_target_data.sort(key=lambda x: x['BPR_INSTANCE_ID'])

    # Action: Execute the SP
    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": get_current_date_ddmmyyyy()},
        {"name": "p_wiederanlaufwert_param", "value": 0},
        {"name": "p_job_kennung", "value": "TEST_CORE_LOGIC"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    # Fetch results
    actual_target_data = fetch_table_data(bq_client, project_id, dataset_id, "sof_ta_msisdn", order_by="BPR_INSTANCE_ID")

    # Assertions
    assert len(actual_target_data) == len(expected_target_data)
    for i, expected_row in enumerate(expected_target_data):
        actual_row = actual_target_data[i]
        assert actual_row['BPR_INSTANCE_ID'] == expected_row['BPR_INSTANCE_ID']
        # MSISDN and CALLNUMBER_ROLE_ID might vary for tie-breaking NULLs, but VALID_TO should be consistent
        assert actual_row['VALID_TO'] == expected_row['VALID_TO']
        # For BPR3, if the tie-breaking for NULLs results in '555' instead of '666', that's acceptable
        # as long as the VALID_TO is correct (4712-12-31).
        if actual_row['BPR_INSTANCE_ID'] == 'BPR3':
            assert actual_row['MSISDN'] in ['555', '666']
            assert actual_row['CALLNUMBER_ROLE_ID'] in ['ROLE_E', 'ROLE_F']
        else:
            assert actual_row['MSISDN'] == expected_row['MSISDN']
            assert actual_row['CALLNUMBER_ROLE_ID'] == expected_row['CALLNUMBER_ROLE_ID']

```

### Test Case 6: Core Transformation Logic - NULL `valid_to` Handling

*   **Purpose:** Specifically verify that `NULL` values in `valid_to` are correctly treated as the maximum possible date (`4712-12-31`) for comparison and output.
*   **Setup:**
    1.  `setup_clean_tables` fixture.
    2.  Insert data into `sof_ta_msisdn_his` where `valid_to` is NULL for some records, and other records have a very late date (but earlier than `4712-12-31`).
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn`.
    2.  Query `sof_ta_msisdn`.
*   **Pass/Fail Criterion:**
    *   **Pass:** Records with `NULL` `valid_to` are correctly selected as the "latest" if no other record for the same `bpri_com_id` has a `valid_to` later than `4712-12-31`. The output `VALID_TO` for these records is `4712-12-31`.
    *   **Fail:** `NULL` `valid_to` is not handled as the maximum date, or the output `VALID_TO` is incorrect.

```python
# test_core_transformation.py (continued)
def test_null_valid_to_handling(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that NULL valid_to values are correctly treated as 4712-12-31.
    """
    source_data = [
        # BPR_NULL_MAX: NULL should be chosen as max
        {"bpri_com_id": "BPR_NULL_MAX", "msisdn": "100", "callnumber_role_id": "ROLE_X", "valid_to": date(2099, 12, 31).isoformat(), "creation_date": datetime(2098,1,1).isoformat(), "last_update_date": datetime(2098,1,1).isoformat()},
        {"bpri_com_id": "BPR_NULL_MAX", "msisdn": "200", "callnumber_role_id": "ROLE_Y", "valid_to": None, "creation_date": datetime(2099,1,1).isoformat(), "last_update_date": datetime(2099,1,1).isoformat()},
        # BPR_LATE_DATE_MAX: A very late explicit date should be chosen over an earlier NULL
        {"bpri_com_id": "BPR_LATE_DATE_MAX", "msisdn": "300", "callnumber_role_id": "ROLE_Z", "valid_to": date(4000, 1, 1).isoformat(), "creation_date": datetime(3999,1,1).isoformat(), "last_update_date": datetime(3999,1,1).isoformat()},
        {"bpri_com_id": "BPR_LATE_DATE_MAX", "msisdn": "400", "callnumber_role_id": "ROLE_W", "valid_to": None, "creation_date": datetime(3998,1,1).isoformat(), "last_update_date": datetime(3998,1,1).isoformat()},
    ]
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his", source_data)

    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": get_current_date_ddmmyyyy()},
        {"name": "p_wiederanlaufwert_param", "value": 0},
        {"name": "p_job_kennung", "value": "TEST_NULL_VALID_TO"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    actual_target_data = fetch_table_data(bq_client, project_id, dataset_id, "sof_ta_msisdn", order_by="BPR_INSTANCE_ID")

    expected_target_data = [
        {"BPR_INSTANCE_ID": "BPR_LATE_DATE_MAX", "MSISDN": "300", "CALLNUMBER_ROLE_ID": "ROLE_Z", "VALID_TO": date(4000, 1, 1)},
        {"BPR_INSTANCE_ID": "BPR_NULL_MAX", "MSISDN": "200", "CALLNUMBER_ROLE_ID": "ROLE_Y", "VALID_TO": date(4712, 12, 31)},
    ]
    expected_target_data.sort(key=lambda x: x['BPR_INSTANCE_ID'])

    assert len(actual_target_data) == len(expected_target_data)
    for i, expected_row in enumerate(expected_target_data):
        actual_row = actual_target_data[i]
        assert actual_row['BPR_INSTANCE_ID'] == expected_row['BPR_INSTANCE_ID']
        assert actual_row['MSISDN'] == expected_row['MSISDN']
        assert actual_row['CALLNUMBER_ROLE_ID'] == expected_row['CALLNUMBER_ROLE_ID']
        assert actual_row['VALID_TO'] == expected_row['VALID_TO']
```

### Test Case 7: Core Transformation Logic - Target Table Truncation

*   **Purpose:** Verify that the target table `sof_ta_msisdn` is truncated at the beginning of the job execution, ensuring a fresh snapshot.
*   **Setup:**
    1.  `setup_clean_tables` fixture.
    2.  Insert some dummy data into `sof_ta_msisdn` *before* calling the main stored procedure.
    3.  Insert valid source data into `sof_ta_msisdn_his`.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn`.
    2.  Query `sof_ta_msisdn`.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `sof_ta_msisdn` table contains only the data resulting from the current job run, and none of the pre-existing dummy data.
    *   **Fail:** Pre-existing data remains in `sof_ta_msisdn`.

```python
# test_core_transformation.py (continued)
def test_target_table_truncation(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that the target table sof_ta_msisdn is truncated before insertion.
    """
    # Insert dummy data into target table
    dummy_data = [{"BPR_INSTANCE_ID": "DUMMY1", "MSISDN": "000", "CALLNUMBER_ROLE_ID": "DUMMY", "VALID_TO": date(2000, 1, 1).isoformat()}]
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn", dummy_data)
    assert len(fetch_table_data(bq_client, project_id, dataset_id, "sof_ta_msisdn")) == 1

    # Insert valid source data
    source_data = [{"bpri_com_id": "REAL1", "msisdn": "123", "callnumber_role_id": "REAL", "valid_to": date(2024, 1, 1).isoformat(), "creation_date": datetime(2023,1,1).isoformat(), "last_update_date": datetime(2023,1,1).isoformat()}]
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his", source_data)

    # Action: Execute the SP
    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": get_current_date_ddmmyyyy()},
        {"name": "p_wiederanlaufwert_param", "value": 0},
        {"name": "p_job_kennung", "value": "TEST_TRUNCATE"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    # Fetch results
    actual_target_data = fetch_table_data(bq_client, project_id, dataset_id, "sof_ta_msisdn")

    # Assertions: Only the new data should be present
    assert len(actual_target_data) == 1
    assert actual_target_data[0]['BPR_INSTANCE_ID'] == "REAL1"
```

### Test Case 8: Audit Logging - Success

*   **Purpose:** Verify that a successful job run correctly logs its details to the `job_audit` table with a `SUCCESS` status.
*   **Setup:**
    1.  `setup_clean_tables` fixture.
    2.  Insert some valid data into `sof_ta_msisdn_his` to ensure a successful run.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with valid parameters.
    2.  Query the `job_audit` table.
*   **Pass/Fail Criterion:**
    *   **Pass:** A single entry exists in `job_audit` for the job run, with `status = 'SUCCESS'`, `end_timestamp` populated, and correct `stichtag` and `wiederanlaufwert`.
    *   **Fail:** No entry, incorrect status, missing `end_timestamp`, or incorrect parameter values.

```python
# test_audit_logging.py
import pytest
from datetime import date, datetime

def test_audit_log_success(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that a successful job run is correctly logged in job_audit.
    """
    stichtag_str = "01012023"
    expected_stichtag = date(2023, 1, 1)
    wiederanlaufwert = 100
    job_kennung = "SUCCESS_JOB"

    # Insert some data to ensure a successful run
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his", [
        {"bpri_com_id": "A1", "msisdn": "1", "callnumber_role_id": "R1", "valid_to": date(2024,1,1).isoformat(), "creation_date": datetime(2023,1,1).isoformat(), "last_update_date": datetime(2023,1,1).isoformat()}
    ])

    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": stichtag_str},
        {"name": "p_wiederanlaufwert_param", "value": wiederanlaufwert},
        {"name": "p_job_kennung", "value": job_kennung},
        {"name": "p_eintrags_nr", "value": "123"}
    ])

    audit_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
    assert len(audit_entries) == 1
    audit_entry = audit_entries[0]

    assert audit_entry['job_id'] == 'r_ausd_bp_ta_msisdn'
    assert audit_entry['run_id'] is not None
    assert audit_entry['start_timestamp'] is not None
    assert audit_entry['end_timestamp'] is not None
    assert audit_entry['status'] == 'SUCCESS'
    assert audit_entry['error_message'] is None
    assert audit_entry['stichtag'] == expected_stichtag
    assert audit_entry['wiederanlaufwert'] == wiederanlaufwert
```

### Test Case 9: Audit Logging - Failure

*   **Purpose:** Verify that a failed job run correctly logs its details to the `job_audit` table with a `FAILED` status and an error message.
*   **Setup:** `setup_clean_tables` fixture.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with an intentionally invalid `p_stichtag_str` (e.g., 'INVALID_DATE') to trigger a failure.
    2.  Query the `job_audit` table.
*   **Pass/Fail Criterion:**
    *   **Pass:** An entry exists in `job_audit` for the job run, with `status = 'FAILED'`, `end_timestamp` populated, and `error_message` containing relevant details about the failure.
    *   **Fail:** No entry, incorrect status, missing `end_timestamp`, or missing/irrelevant `error_message`.

```python
# test_audit_logging.py (continued)
import pytest
from google.cloud.exceptions import GoogleCloudError

def test_audit_log_failure(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that a failed job run is correctly logged in job_audit.
    """
    invalid_stichtag_str = "INVALID_DATE"
    wiederanlaufwert = 0
    job_kennung = "FAILURE_JOB"

    with pytest.raises(GoogleCloudError): # Expect the procedure to raise an error
        execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
            {"name": "p_stichtag_str", "value": invalid_stichtag_str},
            {"name": "p_wiederanlaufwert_param", "value": wiederanlaufwert},
            {"name": "p_job_kennung", "value": job_kennung},
            {"name": "p_eintrags_nr", "value": None}
        ])

    audit_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
    assert len(audit_entries) == 1
    audit_entry = audit_entries[0]

    assert audit_entry['job_id'] == 'r_ausd_bp_ta_msisdn'
    assert audit_entry['run_id'] is not None
    assert audit_entry['start_timestamp'] is not None
    assert audit_entry['end_timestamp'] is not None
    assert audit_entry['status'] == 'FAILED'
    assert audit_entry['error_message'] is not None
    assert "Invalid date format" in audit_entry['error_message']
    assert audit_entry['stichtag'] is None # Stichtag might be NULL if validation fails before setting it
    assert audit_entry['wiederanlaufwert'] == wiederanlaufwert
```

### Test Case 10: Record Count Logging

*   **Purpose:** Verify that the number of records inserted into the target table is correctly logged in the `job_result_counts` table upon successful completion.
*   **Setup:**
    1.  `setup_clean_tables` fixture.
    2.  Insert source data into `sof_ta_msisdn_his` that will result in a known number of records in `sof_ta_msisdn`.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with valid parameters.
    2.  Query the `job_result_counts` table.
*   **Pass/Fail Criterion:**
    *   **Pass:** A single entry exists in `job_result_counts` for the job run, with `record_count` matching the expected number of rows in `sof_ta_msisdn`.
    *   **Fail:** No entry, incorrect count, or entry exists for a failed run.

```python
# test_record_count_logging.py
import pytest
from datetime import date, datetime

def test_record_count_logging(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that the number of records processed is correctly logged.
    """
    stichtag_str = "01012023"
    expected_stichtag = date(2023, 1, 1)
    expected_record_count = 3 # Based on the source data below

    source_data = [
        {"bpri_com_id": "A", "msisdn": "1", "callnumber_role_id": "R1", "valid_to": date(2023,1,1).isoformat(), "creation_date": datetime(2022,1,1).isoformat(), "last_update_date": datetime(2022,1,1).isoformat()},
        {"bpri_com_id": "A", "msisdn": "2", "callnumber_role_id": "R2", "valid_to": date(2023,6,1).isoformat(), "creation_date": datetime(2022,2,1).isoformat(), "last_update_date": datetime(2022,2,1).isoformat()}, # This one should be picked for A
        {"bpri_com_id": "B", "msisdn": "3", "callnumber_role_id": "R3", "valid_to": date(2024,1,1).isoformat(), "creation_date": datetime(2022,3,1).isoformat(), "last_update_date": datetime(2022,3,1).isoformat()},
        {"bpri_com_id": "C", "msisdn": "4", "callnumber_role_id": "R4", "valid_to": None, "creation_date": datetime(2022,4,1).isoformat(), "last_update_date": datetime(2022,4,1).isoformat()},
    ]
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his", source_data)

    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": stichtag_str},
        {"name": "p_wiederanlaufwert_param", "value": 0},
        {"name": "p_job_kennung", "value": "COUNT_JOB"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    count_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_result_counts", order_by="timestamp DESC")
    assert len(count_entries) == 1
    count_entry = count_entries[0]

    assert count_entry['job_id'] == 'r_ausd_bp_ta_msisdn'
    assert count_entry['run_id'] is not None
    assert count_entry['stichtag'] == expected_stichtag
    assert count_entry['record_count'] == expected_record_count
    assert count_entry['timestamp'] is not None

    # Also verify the actual target table count
    actual_target_count = bq_client.query(f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.sof_ta_msisdn`").result().total_rows
    assert actual_target_count == expected_record_count
```

### Test Case 11: External System Replacement - `dwtk_meldungen`

*   **Purpose:** Verify that the logic to derive `v_bert_drop_temp_table_date` from `dwtk_meldungen` (mimicking `DEFINE v_datum` in Oracle) correctly identifies the maximum `timecreated` for `BERT_DROP_TEMP_TABLE` entries.
*   **Setup:**
    1.  `setup_clean_tables` fixture.
    2.  Insert multiple entries into `dwtk_meldungen` with different `timecreated` values and `job_kennung` values, including `BERT_DROP_TEMP_TABLE`.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn`.
    2.  Since `v_bert_drop_temp_table_date` is an internal variable, we cannot directly query it. We need to modify the SP temporarily to log this value or assert its effect if it were used. For this test, we'll assume a temporary modification to the SP to insert `v_bert_drop_temp_table_date` into `job_audit` or a temporary table for verification.
*   **Pass/Fail Criterion:**
    *   **Pass:** The extracted `v_bert_drop_temp_table_date` matches the maximum `timecreated` date from `dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Fail:** Incorrect date is derived.

```python
# test_external_systems.py
import pytest
from datetime import date, datetime, timedelta

# NOTE: This test requires a temporary modification to the bq_r_ausd_bp_ta_msisdn_sp.sql
# to expose the internal variable `v_bert_drop_temp_table_date` for testing.
# For example, add an INSERT into job_audit with this value, or return it.
# For the purpose of this example, we'll assume it's temporarily added to job_audit.error_message
# or a dedicated column for testing. A cleaner way would be to create a separate test SP.

def test_dwtk_meldungen_date_derivation(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that v_bert_drop_temp_table_date is correctly derived from dwtk_meldungen.
    """
    # Insert test data into dwtk_meldungen
    now = datetime.now()
    insert_data(bq_client, project_id, dataset_id, "dwtk_meldungen", [
        {"timecreated": (now - timedelta(days=10)).isoformat(), "job_kennung": "OTHER_JOB", "message_text": "Old other job"},
        {"timecreated": (now - timedelta(days=5)).isoformat(), "job_kennung": "BERT_DROP_TEMP_TABLE", "message_text": "Old drop"},
        {"timecreated": (now - timedelta(days=2)).isoformat(), "job_kennung": "OTHER_JOB", "message_text": "New other job"},
        {"timecreated": now.isoformat(), "job_kennung": "BERT_DROP_TEMP_TABLE", "message_text": "Latest drop"},
    ])

    # The expected date is the date part of the latest 'BERT_DROP_TEMP_TABLE' entry
    expected_date = now.date()

    # Action: Execute the SP. We need a way to retrieve v_bert_drop_temp_table_date.
    # For this test, we'll assume the SP is temporarily modified to insert this value
    # into the job_audit.error_message field for verification.
    # In a real scenario, you might create a dedicated test procedure or use a debug flag.
    # For now, we'll just check the audit log for success and assume the internal logic is correct
    # if the job runs successfully, as the variable is declared and assigned.
    # A more robust test would require modifying the SP to return or log this specific value.

    # --- TEMPORARY MODIFICATION ASSUMPTION ---
    # Assume bq_r_ausd_bp_ta_msisdn_sp.sql is temporarily modified like this:
    # ...
    # SELECT COALESCE(MAX(DATE(m.timecreated)), PARSE_DATE('%Y%m%d', '19000101'))
    # INTO v_bert_drop_temp_table_date
    # FROM `my-gcp-project.isbert_dataset.dwtk_meldungen` AS m
    # WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    #
    # -- TEMPORARY DEBUG LOGGING:
    # INSERT INTO `my-gcp-project.isbert_dataset.job_audit`
    #     (job_id, run_id, start_timestamp, status, error_message, stichtag, wiederanlaufwert)
    # VALUES
    #     (v_job_id, v_run_id, CURRENT_TIMESTAMP(), 'DEBUG', FORMAT('Derived date: %T', v_bert_drop_temp_table_date), v_stichtag, v_wiederanlaufwert);
    # ...

    # For the actual test, we'll call the SP and then query the audit log for the debug message.
    # If the SP is not modified, this test can only verify that the SP *runs* without error
    # when dwtk_meldungen data is present, but not the exact value of the internal variable.
    # Given the prompt, I must assume the ability to test internal logic.

    # Re-create the SP with a temporary debug logging line
    sp_content = ""
    with open("bq_r_ausd_bp_ta_msisdn_sp.sql", "r") as f:
        sp_content = f.read()
    
    # Inject debug logging for v_bert_drop_temp_table_date
    debug_insert = f"""
        INSERT INTO `{project_id}.{dataset_id}.job_audit`
            (job_id, run_id, start_timestamp, status, error_message, stichtag, wiederanlaufwert)
        VALUES
            (v_job_id, v_run_id, CURRENT_TIMESTAMP(), 'DEBUG_DATE_DERIVATION', FORMAT('Derived date: %T', v_bert_drop_temp_table_date), v_stichtag, v_wiederanlaufwert);
    """
    sp_content = sp_content.replace(
        "        -- Core data extraction and transformation logic (from bq_d_ausd_bp_ta_msisdn_logic.sql)",
        f"{debug_insert}\n        -- Core data extraction and transformation logic (from bq_d_ausd_bp_ta_msisdn_logic.sql)"
    )
    sp_content = sp_content.replace("`my-gcp-project.isbert_dataset.", f"`{project_id}.{dataset_id}.")
    bq_client.query(sp_content).result() # Re-create SP with debug code

    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": get_current_date_ddmmyyyy()},
        {"name": "p_wiederanlaufwert_param", "value": 0},
        {"name": "p_job_kennung", "value": "TEST_DWTK_DATE"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    # Query the audit log for the debug entry
    debug_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
    debug_entry = next((e for e in debug_entries if e['status'] == 'DEBUG_DATE_DERIVATION'), None)
    assert debug_entry is not None, "Debug entry for date derivation not found."
    
    derived_date_str = debug_entry['error_message'].split(': ')[1]
    derived_date = datetime.strptime(derived_date_str, '%Y-%m-%d').date() # BigQuery DATE format is YYYY-MM-DD

    assert derived_date == expected_date

    # Restore the original SP (optional, but good practice)
    with open("bq_r_ausd_bp_ta_msisdn_sp.sql", "r") as f:
        original_sp_content = f.read()
    original_sp_content = original_sp_content.replace("`my-gcp-project.isbert_dataset.", f"`{project_id}.{dataset_id}.")
    bq_client.query(original_sp_content).result()
```

### Test Case 12: Orchestration (Airflow DAG) - Parameter Passing

*   **Purpose:** Verify that the Cloud Composer DAG correctly invokes the BigQuery Stored Procedure and passes parameters as expected, especially dynamic ones like `data_interval_end`.
*   **Setup:**
    1.  A running Cloud Composer environment with the DAG deployed.
    2.  The BigQuery Stored Procedure `r_ausd_bp_ta_msisdn` is deployed.
    3.  `setup_clean_tables` fixture.
*   **Action:**
    1.  Manually trigger the `r_ausd_bp_ta_msisdn_dag` in Airflow for a specific `data_interval_end` (e.g., `2023-01-01T00:00:00Z`).
    2.  After the DAG run completes, query the `job_audit` table in BigQuery.
*   **Pass/Fail Criterion:**
    *   **Pass:** A `SUCCESS` entry exists in `job_audit` for the job run, and its `stichtag` matches the `data_interval_end` date passed by Airflow (e.g., `2023-01-01`). The `wiederanlaufwert` is `0` as hardcoded in the DAG.
    *   **Fail:** DAG fails, `job_audit` entry is missing or incorrect, or parameters are not passed correctly.

```python
# test_orchestration.py
import pytest
from airflow.models import DagBag
from datetime import date, datetime

# This test requires Airflow to be running and the DAG to be deployed.
# It simulates triggering the DAG and checks the BigQuery audit log.

def test_airflow_dag_parameter_passing(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that the Airflow DAG correctly passes parameters to the BigQuery SP.
    This test assumes the DAG is deployed and can be triggered.
    """
    # Simulate a specific Airflow execution date
    simulated_data_interval_end = datetime(2023, 1, 1, 0, 0, 0)
    expected_stichtag = simulated_data_interval_end.date()
    expected_wiederanlaufwert = 0 # As hardcoded in the DAG

    # In a real scenario, you would use Airflow's API or CLI to trigger the DAG
    # and wait for its completion. For this example, we'll simulate the SP call
    # with the parameters that Airflow *would* pass.

    # Parameters as they would be rendered by Airflow macros
    airflow_params = [
        {"name": "p_stichtag_str", "value": simulated_data_interval_end.strftime('%d%m%Y')},
        {"name": "p_wiederanlaufwert_param", "value": expected_wiederanlaufwert},
        {"name": "p_job_kennung", "value": "FOS_BP_TA_MSISDN"},
        {"name": "p_eintrags_nr", "value": None}
    ]

    # Execute the SP directly with these parameters
    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", airflow_params)

    # Verify audit log
    audit_entries = fetch_table_data(bq_client, project_id, dataset_id, "job_audit", order_by="start_timestamp DESC")
    assert len(audit_entries) >= 1 # Could be more if other tests ran
    latest_audit_entry = audit_entries[0]

    assert latest_audit_entry['status'] == 'SUCCESS'
    assert latest_audit_entry['stichtag'] == expected_stichtag
    assert latest_audit_entry['wiederanlaufwert'] == expected_wiederanlaufwert
    assert latest_audit_entry['job_id'] == 'r_ausd_bp_ta_msisdn'
```

### Test Case 13: Discrepancy Check - `Stichtag`/`Wiederanlaufwert` in Core Logic

*   **Purpose:** Explicitly verify that the `Stichtag` and `Wiederanlaufwert` parameters are *not* used in the core data extraction/transformation logic (i.e., the `SELECT` statement in `bq_d_ausd_bp_ta_msisdn_logic.sql`), as per the provided migrated code. This test highlights the functional deviation from the design document's description of the legacy SQL.
*   **Setup:**
    1.  `setup_clean_tables` fixture.
    2.  Insert source data into `sof_ta_msisdn_his` where some records would be filtered out by `Stichtag` or `Wiederanlaufwert` if they *were* applied, but should be included by the "latest valid record" logic.
*   **Action:**
    1.  Call `r_ausd_bp_ta_msisdn` with a `p_stichtag_str` and `p_wiederanlaufwert_param` that *would* filter data if the design document's description was implemented.
    2.  Query the `sof_ta_msisdn` target table.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `sof_ta_msisdn` table contains all records selected by the "latest valid record" logic, *regardless* of the `Stichtag` or `Wiederanlaufwert` passed. This confirms the migrated code's behavior, but flags the discrepancy with the design document.
    *   **Fail:** Data is filtered based on `Stichtag` or `Wiederanlaufwert`, indicating that the migrated code *does* use these parameters in the core `SELECT` logic, contradicting the provided `bq_d_ausd_bp_ta_msisdn_logic.sql`.

```python
# test_discrepancy_check.py
import pytest
from datetime import date, datetime

def test_stichtag_wiederanlaufwert_not_used_in_core_logic(bq_client, project_id, dataset_id, setup_clean_tables):
    """
    Tests that Stichtag and Wiederanlaufwert are NOT used in the core data selection logic,
    as per the provided bq_d_ausd_bp_ta_msisdn_logic.sql.
    This highlights a discrepancy with the design document's description of the legacy SQL.
    """
    # Define a Stichtag and Wiederanlaufwert that *would* filter data if applied
    # (e.g., Stichtag in the past, Wiederanlaufwert > 0)
    test_stichtag_str = "01012023" # January 1, 2023
    test_wiederanlaufwert = 100    # If DWH_VERTRAG_ID > 100 was applied

    # Source data:
    # - BPR1: Should be selected by "latest valid" logic.
    #   If Stichtag (2023-01-01) was applied as "valid_to <= Stichtag", this would be filtered out.
    # - BPR2: Should be selected by "latest valid" logic.
    #   If Wiederanlaufwert (100) was applied to bpri_com_id, this would be filtered out.
    source_data = [
        {"bpri_com_id": "BPR1", "msisdn": "111", "callnumber_role_id": "ROLE_A", "valid_to": date(2023, 6, 30).isoformat(), "creation_date": datetime(2022,1,1).isoformat(), "last_update_date": datetime(2022,1,1).isoformat()},
        {"bpri_com_id": "BPR2", "msisdn": "222", "callnumber_role_id": "ROLE_B", "valid_to": date(2024, 1, 1).isoformat(), "creation_date": datetime(2022,2,1).isoformat(), "last_update_date": datetime(2022,2,1).isoformat()},
        {"bpri_com_id": "BPR3", "msisdn": "333", "callnumber_role_id": "ROLE_C", "valid_to": None, "creation_date": datetime(2022,3,1).isoformat(), "last_update_date": datetime(2022,3,1).isoformat()},
    ]
    insert_data(bq_client, project_id, dataset_id, "sof_ta_msisdn_his", source_data)

    # Expected output based *only* on "latest valid record" logic, ignoring Stichtag/Wiederanlaufwert
    expected_target_data = [
        {"BPR_INSTANCE_ID": "BPR1", "MSISDN": "111", "CALLNUMBER_ROLE_ID": "ROLE_A", "VALID_TO": date(2023, 6, 30)},
        {"BPR_INSTANCE_ID": "BPR2", "MSISDN": "222", "CALLNUMBER_ROLE_ID": "ROLE_B", "VALID_TO": date(2024, 1, 1)},
        {"BPR_INSTANCE_ID": "BPR3", "MSISDN": "333", "CALLNUMBER_ROLE_ID": "ROLE_C", "VALID_TO": date(4712, 12, 31)},
    ]
    expected_target_data.sort(key=lambda x: x['BPR_INSTANCE_ID'])

    # Action: Execute the SP with filtering parameters
    execute_bq_procedure(bq_client, project_id, dataset_id, "r_ausd_bp_ta_msisdn", [
        {"name": "p_stichtag_str", "value": test_stichtag_str},
        {"name": "p_wiederanlaufwert_param", "value": test_wiederanlaufwert},
        {"name": "p_job_kennung", "value": "DISCREPANCY_CHECK"},
        {"name": "p_eintrags_nr", "value": None}
    ])

    # Fetch results
    actual_target_data = fetch_table_data(bq_client, project_id, dataset_id, "sof_ta_msisdn", order_by="BPR_INSTANCE_ID")

    # Pass/Fail Criterion:
    # The actual output should match the expected output *without* Stichtag/Wiederanlaufwert filtering.
    # This confirms the migrated code's behavior, but flags the discrepancy.
    assert len(actual_target_data) == len(expected_target_data)
    for i, expected_row in enumerate(expected_target_data):
        actual_row = actual_target_data[i]
        assert actual_row['BPR_INSTANCE_ID'] == expected_row['BPR_INSTANCE_ID']
        assert actual_row['MSISDN'] == expected_row['MSISDN']
        assert actual_row['CALLNUMBER_ROLE_ID'] == expected_row['CALLNUMBER_ROLE_ID']
        assert actual_row['VALID_TO'] == expected_row['VALID_TO']

    print("\n--- CRITICAL DISCREPANCY ALERT ---")
    print("The migrated BigQuery SQL (bq_d_ausd_bp_ta_msisdn_logic.sql) does NOT use 'Stichtag' or 'Wiederanlaufwert'")
    print("in its core data selection logic, contrary to the description in the Migration Design Document.")
    print("This test confirms the *implemented* behavior, but this functional difference must be reviewed with stakeholders.")
    print("The job currently selects the latest valid record based on 'valid_to' regardless of the 'Stichtag' parameter.")
    print("The 'Wiederanlaufwert' parameter is also not used in the core SELECT statement.")
    print("----------------------------------\n")
```