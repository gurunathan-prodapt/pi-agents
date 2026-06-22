Here are migration validation tests for the `k_ausd_v_ta_cntrct_valid.ksh` job, migrated to Google BigQuery. These tests are designed to cover output parity, transformation correctness, external system replacements, and data quality assertions, as requested.

The tests are structured using `pytest` for orchestration and direct BigQuery SQL for assertions. Placeholders like `your_project_id` and `your_dataset` should be replaced with your actual BigQuery project and dataset IDs.

**Assumptions:**
*   All BigQuery DDLs (`job_control`, `job_log`) and procedures (`log_message`, `d_ausd_v_ta_cntrct_valid_bq`, `r_ausd_vertrag_control`) have been deployed to `your_project_id.your_dataset`.
*   Source tables (`DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`) and target tables (`SOF$TA_CNTRCT_VALID`, `VIA`) have been migrated to BigQuery and are accessible.
*   Test data can be loaded into the BigQuery source tables to simulate various scenarios.
*   A `pytest` environment with the `google-cloud-bigquery` client library is set up for test orchestration.
*   The `d_ausd_v_ta_cntrct_valid_bq` procedure, while a placeholder in the provided code, is assumed to be implemented with the actual translated SQL logic for tests related to data transformation. For now, its `p_records_processed` output is simulated.

---

```python
import pytest
from google.cloud import bigquery
import uuid
import time
from datetime import datetime, timedelta

# --- Configuration for BigQuery ---
PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset"
JOB_CONTROL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_control"
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
R_AUSD_VERTRAG_CONTROL_PROC = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control"
D_AUSD_V_TA_CNTRCT_VALID_BQ_PROC = f"{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_cntrct_valid_bq"
SOF_TA_CNTRCT_VALID_TABLE = f"{PROJECT_ID}.{DATASET_ID}.SOF$TA_CNTRCT_VALID"
VIA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.VIA"
DWTK_MELDUNGEN_TABLE = f"{PROJECT_ID}.{DATASET_ID}.DWTK_MELDUNGEN"
CDS_TA_CNTRCT_VALIDITY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.CDS$TA_CNTRCT_VALIDITY"

bq_client = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions ---
def execute_bq_query(query):
    """Helper to execute a BigQuery SQL query."""
    query_job = bq_client.query(query)
    return query_job.result()

def call_bq_procedure(procedure_name, *args):
    """Helper to call a BigQuery stored procedure."""
    arg_str = ", ".join([f"'{arg}'" if isinstance(arg, str) else str(arg) for arg in args])
    query = f"CALL {procedure_name}({arg_str});"
    print(f"Executing: {query}")
    # BigQuery procedure calls can raise exceptions on error, which pytest will catch
    return execute_bq_query(query)

def get_latest_job_control_entry(job_name, job_kennung, eintrags_nr):
    """Fetches the latest job control entry for specific parameters."""
    query = f"""
        SELECT *
        FROM `{JOB_CONTROL_TABLE}`
        WHERE job_name = '{job_name}'
          AND job_kennung = '{job_kennung}'
          AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC
        LIMIT 1
    """
    rows = list(execute_bq_query(query))
    return rows[0] if rows else None

def get_job_log_entries(job_run_id):
    """Fetches all log entries for a given job_run_id."""
    query = f"""
        SELECT log_level, message, error_code, error_argument
        FROM `{JOB_LOG_TABLE}`
        WHERE job_run_id = '{job_run_id}'
        ORDER BY log_timestamp ASC
    """
    return list(execute_bq_query(query))

def setup_test_data():
    """
    Sets up minimal test data in source tables and clears target/log/control tables.
    This is a placeholder and should be expanded with realistic data.
    """
    print("Setting up test data...")
    # Clear target tables
    execute_bq_query(f"TRUNCATE TABLE `{SOF_TA_CNTRCT_VALID_TABLE}`")
    execute_bq_query(f"TRUNCATE TABLE `{VIA_TABLE}`")
    execute_bq_query(f"TRUNCATE TABLE `{JOB_CONTROL_TABLE}`")
    execute_bq_query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`")

    # Insert sample data into source tables (if not already present or for specific tests)
    # These inserts are illustrative and should match your actual table schemas.
    # For d_ausd_v_ta_cntrct_valid_bq to return 100 records, ensure enough data is present
    # or modify the procedure to simulate this.
    execute_bq_query(f"""
        CREATE OR REPLACE TABLE `{DWTK_MELDUNGEN_TABLE}` (id INT64, contract_id STRING, status STRING, value INT64, date_col DATE);
        INSERT INTO `{DWTK_MELDUNGEN_TABLE}` (id, contract_id, status, value, date_col) VALUES
        (1, 'C1', 'ACTIVE', 100, '2023-01-01'),
        (2, 'C2', 'INACTIVE', 200, '2023-01-05'),
        (3, 'C3', 'ACTIVE', NULL, '2023-01-10'),
        (4, 'C4', 'ACTIVE', 400, NULL),
        (5, 'C5', 'ACTIVE', 500, '2023-01-15');
    """)
    execute_bq_query(f"""
        CREATE OR REPLACE TABLE `{CDS_TA_CNTRCT_VALIDITY_TABLE}` (contract_id STRING, valid_from DATE, valid_to DATE, type STRING);
        INSERT INTO `{CDS_TA_CNTRCT_VALIDITY_TABLE}` (contract_id, valid_from, valid_to, type) VALUES
        ('C1', '2022-12-01', '2023-12-31', 'TYPEA'),
        ('C3', '2023-01-01', '2023-06-30', 'TYPEB'),
        ('C5', '2023-02-01', '2023-05-31', 'TYPEC'),
        ('C6', '2023-03-01', '2023-04-30', 'TYPED'); -- No match in DWTK_MELDUNGEN
    """)
    # Create target tables with placeholder schemas if they don't exist
    execute_bq_query(f"""
        CREATE OR REPLACE TABLE `{SOF_TA_CNTRCT_VALID_TABLE}` (
            contract_id STRING,
            status STRING,
            value INT64,
            processed_date DATE,
            source_table STRING
        );
    """)
    execute_bq_query(f"""
        CREATE OR REPLACE TABLE `{VIA_TABLE}` (
            via_id STRING,
            related_contract_id STRING,
            validity_type STRING
        );
    """)

    print("Test data setup complete.")

# Fixture to run setup before each test module or session
@pytest.fixture(scope="module", autouse=True)
def setup_module():
    setup_test_data()
    yield
    # Optional: Teardown logic if needed, e.g., clearing all tables again
    print("Test module teardown complete.")

# --- Test Cases ---

# --- 1. Output Parity & Transformation Correctness (Combined for core logic) ---

### Test Case 1.1: Successful Execution and Output Parity (Conceptual)

**Purpose:** To verify that the `r_ausd_vertrag_control` procedure correctly accepts valid parameters, orchestrates the `d_ausd_v_ta_cntrct_valid_bq` procedure, and that the core data transformation produces expected results in target tables. This test also covers the replacement of temporary file record count with `OUT` parameters.

**Setup:**
1.  Ensure `job_control` and `job_log` tables are empty.
2.  Populate `DWTK_MELDUNGEN` and `CDS$TA_CNTRCT_VALIDITY` with a predefined "golden" test dataset.
3.  **Crucially, the `d_ausd_v_ta_cntrct_valid_bq` procedure must be fully implemented with the translated SQL logic.** For this test, we assume it performs a simple insert into `SOF$TA_CNTRCT_VALID` and `VIA` and returns the total count of inserted rows.
    *   **Example `d_ausd_v_ta_cntrct_valid_bq` implementation for this test:**
        ```sql
        CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset.d_ausd_v_ta_cntrct_valid_bq`(
            IN p_eintrags_nr STRING,
            IN p_job_kennung STRING,
            OUT p_records_processed INT64
        )
        BEGIN
            DECLARE rows_sof INT64;
            DECLARE rows_via INT64;

            INSERT INTO `your_project_id.your_dataset.SOF$TA_CNTRCT_VALID` (contract_id, status, value, processed_date, source_table)
            SELECT contract_id, status, IFNULL(value, 0), IFNULL(date_col, CURRENT_DATE()), 'DWTK_MELDUNGEN'
            FROM `your_project_id.your_dataset.DWTK_MELDUNGEN`
            WHERE status = 'ACTIVE';
            SET rows_sof = @@row_count;

            INSERT INTO `your_project_id.your_dataset.VIA` (via_id, related_contract_id, validity_type)
            SELECT GENERATE_UUID(), c.contract_id, c.type
            FROM `your_project_id.your_dataset.CDS$TA_CNTRCT_VALIDITY` c
            JOIN `your_project_id.your_dataset.DWTK_MELDUNGEN` d ON c.contract_id = d.contract_id
            WHERE d.status = 'ACTIVE';
            SET rows_via = @@row_count;

            SET p_records_processed = rows_sof + rows_via;
        END;
        ```
4.  Clear `SOF$TA_CNTRCT_VALID` and `VIA` tables.

**Action:**
Execute the `r_ausd_vertrag_control` BigQuery stored procedure with valid parameters.

```python
def test_successful_execution_and_output_parity():
    job_kennung = "TEST_JOB_SUCCESS"
    eintrags_nr = "ENTRY_001"

    # Clear target tables before run
    execute_bq_query(f"TRUNCATE TABLE `{SOF_TA_CNTRCT_VALID_TABLE}`")
    execute_bq_query(f"TRUNCATE TABLE `{VIA_TABLE}`")

    # Call the main control procedure
    call_bq_procedure(R_AUSD_VERTRAG_CONTROL_PROC, job_kennung, eintrags_nr)

    # --- Assertions for Job Control and Logging ---
    job_entry = get_latest_job_control_entry("k_ausd_v_ta_cntrct_valid_bq", job_kennung, eintrags_nr)
    assert job_entry is not None, "Job control entry should exist."
    assert job_entry.status == "COMPLETED", f"Expected status 'COMPLETED', got '{job_entry.status}'"
    assert job_entry.end_timestamp is not None, "End timestamp should be set."

    log_entries = get_job_log_entries(job_entry.job_run_id)
    assert any("Job execution started." in log.message for log in log_entries), "Start log message missing."
    assert any("Job registered as ACTIVE." in log.message for log in log_entries), "Active registration log missing."
    assert any("Starting core data processing" in log.message for log in log_entries), "Core processing start log missing."
    assert any("Finished core data processing" in log.message for log in log_entries), "Core processing end log missing."
    assert any("Job completed successfully." in log.message for log in log_entries), "Completion log message missing."
    assert not any(log.log_level == "ERROR" for log in log_entries), "No error logs expected for successful run."

    # --- Assertions for Output Parity (assuming the example d_ausd_v_ta_cntrct_valid_bq) ---
    # Expected rows based on the example d_ausd_v_ta_cntrct_valid_bq and setup_test_data:
    # DWTK_MELDUNGEN has C1, C2, C3, C4, C5. Active: C1, C3, C4, C5 (4 rows)
    # CDS_TA_CNTRCT_VALIDITY has C1, C3, C5, C6.
    # SOF$TA_CNTRCT_VALID (WHERE status = 'ACTIVE'): C1, C3, C4, C5 (4 rows)
    # VIA (JOIN DWTK_MELDUNGEN ON status='ACTIVE'): C1, C3, C5 (3 rows)
    # Total records processed = 4 + 3 = 7

    expected_sof_count = 4
    expected_via_count = 3
    expected_total_processed = expected_sof_count + expected_via_count

    bq_sof_count = list(execute_bq_query(f"SELECT COUNT(*) FROM `{SOF_TA_CNTRCT_VALID_TABLE}`"))[0][0]
    bq_via_count = list(execute_bq_query(f"SELECT COUNT(*) FROM `{VIA_TABLE}`"))[0][0]

    assert bq_sof_count == expected_sof_count, f"Row count mismatch for {SOF_TA_CNTRCT_VALID_TABLE}"
    assert bq_via_count == expected_via_count, f"Row count mismatch for {VIA_TABLE}"
    assert job_entry.records_processed == expected_total_processed, \
        f"Records processed count mismatch in job_control. Expected {expected_total_processed}, got {job_entry.records_processed}"

    # Detailed data comparison (example for SOF$TA_CNTRCT_VALID)
    sof_data = list(execute_bq_query(f"SELECT contract_id, status, value, processed_date FROM `{SOF_TA_CNTRCT_VALID_TABLE}` ORDER BY contract_id"))
    assert len(sof_data) == 4
    assert sof_data[0].contract_id == 'C1' and sof_data[0].status == 'ACTIVE' and sof_data[0].value == 100
    assert sof_data[1].contract_id == 'C3' and sof_data[1].status == 'ACTIVE' and sof_data[1].value == 0 # NULL handled by IFNULL
    assert sof_data[2].contract_id == 'C4' and sof_data[2].status == 'ACTIVE' and sof_data[2].value == 400
    assert sof_data[3].contract_id == 'C5' and sof_data[3].status == 'ACTIVE' and sof_data[3].value == 500

    # Detailed data comparison (example for VIA)
    via_data = list(execute_bq_query(f"SELECT related_contract_id, validity_type FROM `{VIA_TABLE}` ORDER BY related_contract_id"))
    assert len(via_data) == 3
    assert via_data[0].related_contract_id == 'C1' and via_data[0].validity_type == 'TYPEA'
    assert via_data[1].related_contract_id == 'C3' and via_data[1].validity_type == 'TYPEB'
    assert via_data[2].related_contract_id == 'C5' and via_data[2].validity_type == 'TYPEC'
```

**Pass/Fail Criterion:**
*   The `job_control` table contains exactly one entry for the executed job with `status = 'COMPLETED'`, a non-NULL `end_timestamp`, and `records_processed` matching the expected total from `d_ausd_v_ta_cntrct_valid_bq`.
*   The `job_log` table contains INFO-level entries indicating job start, active registration, core processing start/end, and successful completion, with no ERROR-level entries.
*   The row counts in `SOF$TA_CNTRCT_VALID` and `VIA` BigQuery tables exactly match the expected counts based on the test data and transformation logic.
*   A detailed comparison of data content in `SOF$TA_CNTRCT_VALID` and `VIA` shows exact matches with expected values, demonstrating correct join, filter, and NULL handling.

---

### Test Case 2.1: Parameter Handling - Missing `p_job_kennung`

**Purpose:** To verify that the `r_ausd_vertrag_control` procedure correctly identifies and handles a missing or empty `p_job_kennung` parameter, logging an error and terminating.

**Setup:**
1.  Ensure `job_control` and `job_log` tables are empty.

**Action:**
Attempt to execute `r_ausd_vertrag_control` with `p_job_kennung` as an empty string.

```python
def test_missing_job_kennung_parameter():
    eintrags_nr = "ENTRY_002"
    job_kennung = "" # Simulate missing/empty parameter

    # Expect the procedure call to raise an error
    with pytest.raises(Exception) as excinfo:
        call_bq_procedure(R_AUSD_VERTRAG_CONTROL_PROC, job_kennung, eintrags_nr)

    assert "Required parameter \"Jobkennung\" (-j) is missing or empty." in str(excinfo.value)

    # Verify log entries (querying by job_name and eintrags_nr as job_run_id isn't registered for failed validation)
    query = f"""
        SELECT log_level, message, error_code, error_argument
        FROM `{JOB_LOG_TABLE}`
        WHERE job_name = 'k_ausd_v_ta_cntrct_valid_bq'
          AND eintrags_nr = '{eintrags_nr}'
          AND log_level = 'ERROR'
        ORDER BY log_timestamp DESC
        LIMIT 1
    """
    error_log = list(execute_bq_query(query))
    assert len(error_log) == 1, "Expected one error log entry."
    assert error_log[0].log_level == "ERROR"
    assert "Required parameter \"Jobkennung\" (-j) is missing or empty." in error_log[0].message
    assert error_log[0].error_code == 193
    assert error_log[0].error_argument == "-j"

    # No job control entry should be created for a failed parameter validation before registration
    query_control = f"""
        SELECT COUNT(*) FROM `{JOB_CONTROL_TABLE}`
        WHERE job_name = 'k_ausd_v_ta_cntrct_valid_bq'
          AND eintrags_nr = '{eintrags_nr}'
    """
    control_count = list(execute_bq_query(query_control))[0][0]
    assert control_count == 0, "No job control entry should be created for parameter validation failure."
```

**Pass/Fail Criterion:**
*   The procedure call raises an exception containing the expected error message.
*   The `job_log` table contains an `ERROR`-level entry with `message` indicating the missing `Jobkennung`, `error_code = 193`, and `error_argument = '-j'`.
*   No entry is created in the `job_control` table for this execution.

---

### Test Case 2.2: Parameter Handling - Missing `p_eintrags_nr`

**Purpose:** To verify that the procedure correctly identifies and handles a missing or empty `p_eintrags_nr` parameter, logging an error and terminating.

**Setup:**
1.  Ensure `job_control` and `job_log` tables are empty.

**Action:**
Attempt to execute `r_ausd_vertrag_control` with `p_eintrags_nr` as an empty string.

```python
def test_missing_eintrags_nr_parameter():
    job_kennung = "TEST_JOB_3"
    eintrags_nr = "" # Simulate missing/empty parameter

    with pytest.raises(Exception) as excinfo:
        call_bq_procedure(R_AUSD_VERTRAG_CONTROL_PROC, job_kennung, eintrags_nr)

    assert "Required parameter \"EintragsNr\" (-f) is missing or empty." in str(excinfo.value)

    query = f"""
        SELECT log_level, message, error_code, error_argument
        FROM `{JOB_LOG_TABLE}`
        WHERE job_name = 'k_ausd_v_ta_cntrct_valid_bq'
          AND job_kennung = '{job_kennung}'
          AND log_level = 'ERROR'
        ORDER BY log_timestamp DESC
        LIMIT 1
    """
    error_log = list(execute_bq_query(query))
    assert len(error_log) == 1, "Expected one error log entry."
    assert error_log[0].log_level == "ERROR"
    assert "Required parameter \"EintragsNr\" (-f) is missing or empty." in error_log[0].message
    assert error_log[0].error_code == 193
    assert error_log[0].error_argument == "-f"

    query_control = f"""
        SELECT COUNT(*) FROM `{JOB_CONTROL_TABLE}`
        WHERE job_name = 'k_ausd_v_ta_cntrct_valid_bq'
          AND job_kennung = '{job_kennung}'
    """
    control_count = list(execute_bq_query(query_control))[0][0]
    assert control_count == 0, "No job control entry should be created for parameter validation failure."
```

**Pass/Fail Criterion:**
*   The procedure call raises an exception containing the expected error message.
*   The `job_log` table contains an `ERROR`-level entry with `message` indicating the missing `EintragsNr`, `error_code = 193`, and `error_argument = '-f'`.
*   No entry is created in the `job_control` table for this execution.

---

### Test Case 3.1: Job Control - Ignoring Concurrent Job Execution

**Purpose:** To verify that if an instance of the job is already `ACTIVE`, subsequent calls with the same `job_name` are `IGNORED` as per the legacy script's behavior ("aktive Jobs werden ignoriert").

**Setup:**
1.  Ensure `job_control` and `job_log` tables are empty.
2.  Manually insert an `ACTIVE` entry into `job_control` for `k_ausd_v_ta_cntrct_valid_bq`.

**Action:**
1.  Insert an `ACTIVE` job entry into `job_control`.
2.  Execute `r_ausd_vertrag_control` with valid parameters.

```python
def test_ignoring_concurrent_job_execution():
    job_kennung = "TEST_JOB_CONCURRENT"
    eintrags_nr = "ENTRY_CONC_001"
    job_name = "k_ausd_v_ta_cntrct_valid_bq"
    active_job_run_id = str(uuid.uuid4())

    # 1. Manually insert an ACTIVE job entry
    execute_bq_query(f"""
        INSERT INTO `{JOB_CONTROL_TABLE}` (job_run_id, job_name, job_kennung, eintrags_nr, start_timestamp, status, process_id)
        VALUES ('{active_job_run_id}', '{job_name}', '{job_kennung}', '{eintrags_nr}', CURRENT_TIMESTAMP(), 'ACTIVE', 12345);
    """)

    # 2. Call the main control procedure (this should be ignored)
    call_bq_procedure(R_AUSD_VERTRAG_CONTROL_PROC, job_kennung, eintrags_nr)

    # Retrieve the latest job control entry for the *new* execution
    new_job_entry = get_latest_job_control_entry(job_name, job_kennung, eintrags_nr)
    assert new_job_entry is not None, "A job control entry for the new execution should exist."
    assert new_job_entry.job_run_id != active_job_run_id, "New job run ID should be different."
    assert new_job_entry.status == "IGNORED", f"Expected status 'IGNORED', got '{new_job_entry.status}'"
    assert "Another instance is active." in new_job_entry.message, "Expected message for ignored job."

    # Verify log entries for the new (ignored) run
    log_entries = get_job_log_entries(new_job_entry.job_run_id)
    assert any(log.log_level == "WARNING" and "Another instance of this job is currently active." in log.message for log in log_entries), "Warning log for ignored job missing."
    assert not any("Starting core data processing" in log.message for log in log_entries), "Core processing should not start for ignored job."
    assert not any(log.log_level == "ERROR" for log in log_entries), "No error logs expected for ignored run."

    # Verify the original active job is still active
    original_job_entry = execute_bq_query(f"SELECT status FROM `{JOB_CONTROL_TABLE}` WHERE job_run_id = '{active_job_run_id}'").to_dataframe().iloc[0]['status']
    assert original_job_entry == "ACTIVE", "Original active job should remain active."
```

**Pass/Fail Criterion:**
*   A new entry is created in `job_control` for the second execution with `status = 'IGNORED'` and an appropriate message.
*   The `job_log` table contains a `WARNING`-level entry indicating the job was ignored due to an active instance.
*   No `INFO` logs related to core data processing are present for the ignored job.
*   The original `ACTIVE` job entry remains unchanged.

---

### Test Case 3.2: Job Control - Deactivating Older Active Jobs

**Purpose:** To verify that the procedure correctly identifies and marks older `ACTIVE` jobs as `FAILED_TIMEOUT`.

**Setup:**
1.  Ensure `job_control` and `job_log` tables are empty.
2.  Insert an `ACTIVE` entry into `job_control` with a `start_timestamp` older than the defined timeout (e.g., 2 hours ago).
3.  Insert another `ACTIVE` entry that is *not* older than the timeout (e.g., 5 minutes ago).

**Action:**
Execute `r_ausd_vertrag_control` with valid parameters.

```python
def test_deactivating_older_active_jobs():
    job_kennung_old = "TEST_JOB_OLD_ACTIVE"
    eintrags_nr_old = "ENTRY_OLD_001"
    job_kennung_recent = "TEST_JOB_RECENT_ACTIVE"
    eintrags_nr_recent = "ENTRY_RECENT_001"
    job_name = "k_ausd_v_ta_cntrct_valid_bq"

    # Insert an old active job (should be deactivated)
    old_active_job_run_id = str(uuid.uuid4())
    old_start_time = datetime.utcnow() - timedelta(hours=2) # Older than 1 hour threshold
    execute_bq_query(f"""
        INSERT INTO `{JOB_CONTROL_TABLE}` (job_run_id, job_name, job_kennung, eintrags_nr, start_timestamp, status, process_id)
        VALUES ('{old_active_job_run_id}', '{job_name}', '{job_kennung_old}', '{eintrags_nr_old}', TIMESTAMP('{old_start_time.isoformat()}'), 'ACTIVE', 54321);
    """)

    # Insert a recent active job (should remain active)
    recent_active_job_run_id = str(uuid.uuid4())
    recent_start_time = datetime.utcnow() - timedelta(minutes=5) # Not older than 1 hour threshold
    execute_bq_query(f"""
        INSERT INTO `{JOB_CONTROL_TABLE}` (job_run_id, job_name, job_kennung, eintrags_nr, start_timestamp, status, process_id)
        VALUES ('{recent_active_job_run_id}', '{job_name}', '{job_kennung_recent}', '{eintrags_nr_recent}', TIMESTAMP('{recent_start_time.isoformat()}'), 'ACTIVE', 67890);
    """)

    # Execute the procedure (this will also create a new COMPLETED entry)
    call_bq_procedure(R_AUSD_VERTRAG_CONTROL_PROC, "NEW_JOB_RUN", "NEW_ENTRY")

    # Verify the old active job is now FAILED_TIMEOUT
    old_job_entry = execute_bq_query(f"SELECT status, message FROM `{JOB_CONTROL_TABLE}` WHERE job_run_id = '{old_active_job_run_id}'").to_dataframe().iloc[0]
    assert old_job_entry['status'] == "FAILED_TIMEOUT", f"Expected old job status 'FAILED_TIMEOUT', got '{old_job_entry['status']}'"
    assert "Job marked as FAILED_TIMEOUT" in old_job_entry['message'], "Expected message for failed timeout job."

    # Verify the recent active job is still ACTIVE
    recent_job_entry = execute_bq_query(f"SELECT status FROM `{JOB_CONTROL_TABLE}` WHERE job_run_id = '{recent_active_job_run_id}'").to_dataframe().iloc[0]['status']
    assert recent_job_entry == "ACTIVE", "Recent active job should still be 'ACTIVE'."

    # Verify log entries for the deactivation attempt (from the current run)
    current_run_entry = get_latest_job_control_entry("k_ausd_v_ta_cntrct_valid_bq", "NEW_JOB_RUN", "NEW_ENTRY")
    log_entries = get_job_log_entries(current_run_entry.job_run_id)
    assert any("Attempted to deactivate older active jobs." in log.message for log in log_entries), "Log message for deactivation attempt missing."
```

**Pass/Fail Criterion:**
*   The `job_control` entry for the "old active job" is updated to `status = 'FAILED_TIMEOUT'` with a relevant message and `end_timestamp`.
*   The `job_control` entry for the "recent active job" remains `status = 'ACTIVE'`.
*   The `job_log` table contains an `INFO`-level entry from the current run indicating that older active jobs were attempted to be deactivated.

---

### Test Case 3.3: Job Control - Job Failure During Core Processing

**Purpose:** To verify that if the `d_ausd_v_ta_cntrct_valid_bq` procedure encounters an error, the main control procedure correctly logs the error, updates the `job_control` status to `FAILED`, and re-raises the exception.

**Setup:**
1.  Ensure `job_control` and `job_log` tables are empty.
2.  **Temporarily modify `d_ausd_v_ta_cntrct_valid_bq` to unconditionally `RAISE` an error.**
    *   **Example `d_ausd_v_ta_cntrct_valid_bq` for this test:**
        ```sql
        CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset.d_ausd_v_ta_cntrct_valid_bq`(
            IN p_eintrags_nr STRING,
            IN p_job_kennung STRING,
            OUT p_records_processed INT64
        )
        BEGIN
            RAISE USING MESSAGE = 'Simulated core processing error';
        END;
        ```

**Action:**
Execute `r_ausd_vertrag_control` with valid parameters.

```python
# NOTE: This test requires temporarily modifying d_ausd_v_ta_cntrct_valid_bq to raise an error.
# In a real CI/CD pipeline, this might involve deploying a specific test version of the procedure.

def test_job_failure_during_core_processing():
    job_kennung = "TEST_JOB_FAIL"
    eintrags_nr = "ENTRY_FAIL_001"

    with pytest.raises(Exception) as excinfo:
        call_bq_procedure(R_AUSD_VERTRAG_CONTROL_PROC, job_kennung, eintrags_nr)

    assert "Job failed during core processing." in str(excinfo.value)
    assert "Simulated core processing error" in str(excinfo.value)

    job_entry = get_latest_job_control_entry("k_ausd_v_ta_cntrct_valid_bq", job_kennung, eintrags_nr)
    assert job_entry is not None, "Job control entry should exist."
    assert job_entry.status == "FAILED", f"Expected status 'FAILED', got '{job_entry.status}'"
    assert job_entry.end_timestamp is not None, "End timestamp should be set."
    assert "Failed during core processing: Simulated core processing error" in job_entry.message

    log_entries = get_job_log_entries(job_entry.job_run_id)
    assert any(log.log_level == "ERROR" and "Job failed during core processing." in log.message for log in log_entries), "Error log for core processing failure missing."
    assert any("Simulated core processing error" in log.message for log in log_entries), "Specific error message missing from log."
    assert not any("Job completed successfully." in log.message for log in log_entries), "No completion log expected for failed run."
```

**Pass/Fail Criterion:**
*   The procedure call raises an exception indicating a core processing failure.
*   The `job_control` table contains an entry for the job with `status = 'FAILED'`, a non-NULL `end_timestamp`, and a message detailing the error.
*   The `job_log` table contains an `ERROR`-level entry with a message describing the core processing failure and the specific error from `d_ausd_v_ta_cntrct_valid_bq`.

---

### Test Case 4.1: Data Quality / Schema Assertions

**Purpose:** To ensure that the schema of the target tables (`SOF$TA_CNTRCT_VALID`, `VIA`) in BigQuery matches the expected schema (including data types, nullability) and that data quality constraints (if any were migrated) are maintained.

**Setup:**
1.  Ensure `SOF$TA_CNTRCT_VALID` and `VIA` tables exist in BigQuery.
2.  Run `r_ausd_vertrag_control` with a representative dataset to populate the tables (e.g., using the successful execution test case).

**Action:**
1.  Query the schema of the BigQuery target tables.
2.  Perform data quality checks on the populated tables (e.g., check for unexpected NULLs, out-of-range values, referential integrity if applicable).

```python
def test_schema_and_data_quality_assertions():
    # Ensure a run has occurred to populate tables (e.g., by calling the successful test case)
    # This ensures tables are populated with data to check data quality.
    test_successful_execution_and_output_parity() # Re-run the successful test to populate tables

    # 1. Schema Assertions for SOF$TA_CNTRCT_VALID
    table_sof = bq_client.get_table(SOF_TA_CNTRCT_VALID_TABLE)
    # Define expected schema based on the example d_ausd_v_ta_cntrct_valid_bq
    expected_sof_schema = [
        bigquery.SchemaField("contract_id", "STRING", mode="NULLABLE"), # Assuming it could be NULL if source allows
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("value", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("processed_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("source_table", "STRING", mode="NULLABLE"),
    ]
    # Compare field by field, ignoring order
    assert len(table_sof.schema) == len(expected_sof_schema), "SOF$TA_CNTRCT_VALID schema field count mismatch."
    for expected_field in expected_sof_schema:
        found = False
        for actual_field in table_sof.schema:
            if (actual_field.name == expected_field.name and
                actual_field.field_type == expected_field.field_type and
                actual_field.mode == expected_field.mode):
                found = True
                break
        assert found, f"Field {expected_field.name} with type {expected_field.field_type} and mode {expected_field.mode} not found or mismatched in SOF$TA_CNTRCT_VALID."

    # 2. Schema Assertions for VIA
    table_via = bq_client.get_table(VIA_TABLE)
    expected_via_schema = [
        bigquery.SchemaField("via_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("related_contract_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("validity_type", "STRING", mode="NULLABLE"),
    ]
    assert len(table_via.schema) == len(expected_via_schema), "VIA schema field count mismatch."
    for expected_field in expected_via_schema:
        found = False
        for actual_field in table_via.schema:
            if (actual_field.name == expected_field.name and
                actual_field.field_type == expected_field.field_type and
                actual_field.mode == expected_field.mode):
                found = True
                break
        assert found, f"Field {expected_field.name} with type {expected_field.field_type} and mode {expected_field.mode} not found or mismatched in VIA."

    # 3. Data Quality Checks (examples based on the example d_ausd_v_ta_cntrct_valid_bq)
    # Check for unexpected NULLs in critical columns (if they were REQUIRED in legacy)
    # For example, if contract_id should never be NULL in SOF$TA_CNTRCT_VALID
    null_check_sof_contract_id = list(execute_bq_query(f"SELECT COUNT(*) FROM `{SOF_TA_CNTRCT_VALID_TABLE}` WHERE contract_id IS NULL"))[0][0]
    assert null_check_sof_contract_id == 0, "Unexpected NULLs in 'contract_id' in SOF$TA_CNTRCT_VALID."

    # Check for referential integrity (if applicable and migrated)
    # Example: All related_contract_id in VIA must exist in SOF$TA_CNTRCT_VALID
    missing_refs = list(execute_bq_query(f"""
        SELECT COUNT(DISTINCT v.related_contract_id)
        FROM `{VIA_TABLE}` v
        LEFT JOIN `{SOF_TA_CNTRCT_VALID_TABLE}` s ON v.related_contract_id = s.contract_id
        WHERE s.contract_id IS NULL
    """))[0][0]
    assert missing_refs == 0, "Referential integrity violation: related_contract_id in VIA not found in SOF$TA_CNTRCT_VALID."

    # Check for value ranges or patterns (e.g., status values)
    invalid_status_count = list(execute_bq_query(f"SELECT COUNT(*) FROM `{SOF_TA_CNTRCT_VALID_TABLE}` WHERE status NOT IN ('ACTIVE', 'INACTIVE') AND status IS NOT NULL"))[0][0]
    assert invalid_status_count == 0, "Invalid status values found in SOF$TA_CNTRCT_VALID."
```

**Pass/Fail Criterion:**
*   The BigQuery target tables (`SOF$TA_CNTRCT_VALID`, `VIA`) have schemas that precisely match the expected schemas (field names, data types, nullability).
*   All defined data quality checks (e.g., no unexpected NULLs in critical fields, referential integrity, value range constraints) pass.

---