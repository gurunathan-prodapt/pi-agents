The migration of `r_ausd_bp_ta_msisdn_his.ksh` to a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_msisdn_his_wrapper`) primarily involves translating orchestration logic, parameter handling, and logging. The following tests focus on ensuring behavioral equivalence in these areas, using a mock BigQuery Stored Procedure for the core logic (`k_ausd_bp_ta_msisdn_his`) to isolate the wrapper's functionality.

**Pre-requisites for all tests:**

1.  **BigQuery Project and Dataset:** Ensure `my_project.my_dataset` exists. Replace `my_project` and `my_dataset` with your actual project and dataset IDs.
2.  **`dwmsg_job_audit` Table:** The DDL for this table (`dwmsg_job_audit_table.sql`) must be executed.
3.  **Mock Core Stored Procedure:** A mock version of `project.dataset.k_ausd_bp_ta_msisdn_his` is required to simulate the core script's behavior without depending on its actual migration. This mock procedure will log its received parameters to the `dwmsg_job_audit` table and can be configured to succeed or fail.

    ```sql
    -- Mock k_ausd_bp_ta_msisdn_his stored procedure for testing the wrapper
    CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_msisdn_his`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      -- Log the parameters received by the core script for verification
      INSERT INTO `my_project.my_dataset.dwmsg_job_audit` (job_id, job_name, script_name, log_file, stichtag, status, error_message, created_at)
      VALUES (p_eintragsnr, p_jobkennung, 'k_ausd_bp_ta_msisdn_his_mock', 'mock_log.log', p_stichtag, 'CALLED',
              CONCAT('Params: stichtag=', p_stichtag, ', wiederanlaufWert=', CAST(p_wiederanlaufWert AS STRING)), CURRENT_TIMESTAMP());

      -- Special value to trigger a simulated failure for testing error handling
      IF p_wiederanlaufWert = -1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated failure in k_ausd_bp_ta_msisdn_his_mock due to p_wiederanlaufWert = -1';
      END IF;

      -- Simulate some work (e.g., a successful SELECT)
      SELECT 'Core script mock executed successfully' AS message;
    END;
    ```

The tests are provided in a `pytest` framework, using the `google-cloud-bigquery` client library for interaction with BigQuery.

---

## Test Case 1: `dwmsg_job_audit` Table Schema Validation

*   **Purpose:** Verify that the `dwmsg_job_audit` table is created with the correct schema, including column names, data types, nullability, partitioning, and clustering, as specified in the migration design. This ensures the foundation for logging is correctly established.
*   **Setup:**
    *   Ensure the BigQuery project and dataset (`my_project.my_dataset`) exist.
    *   The `dwmsg_job_audit_table.sql` DDL script should have been executed at least once.
*   **Action:** Query the BigQuery `INFORMATION_SCHEMA` for the table details.
*   **Pass/Fail Criterion:** The queried schema (column names, data types, nullability, partitioning, and clustering fields) exactly matches the expected DDL.
*   **Runnable Test Code (Pytest):**

```python
import pytest
from google.cloud import bigquery
from datetime import datetime

# --- Configuration ---
PROJECT_ID = "my_project"  # Replace with your BigQuery Project ID
DATASET_ID = "my_dataset"  # Replace with your BigQuery Dataset ID
AUDIT_TABLE_ID = "dwmsg_job_audit"
WRAPPER_SP_ID = "ausd_bp_ta_msisdn_his_wrapper"
CORE_SP_ID = "k_ausd_bp_ta_msisdn_his"

# --- Pytest Fixtures for BigQuery Client and Test Setup ---
@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test session."""
    client = bigquery.Client(project=PROJECT_ID)
    # Ensure dataset exists
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref))
    return client

@pytest.fixture(autouse=True)
def setup_and_teardown_audit_table(bq_client):
    """
    Ensures the audit table exists and is truncated before and after each test.
    Also creates the mock core SP.
    """
    # Create the audit table if it doesn't exist (idempotent)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}` (
      job_id INT64 NOT NULL OPTIONS(description="Unique identifier for the job run, derived from max(job_id) + 1 per job_name."),
      job_name STRING NOT NULL OPTIONS(description="Identifier for the type of job (e.g., 'AUSD_BP_TA_MSISDN_HIS')."),
      script_name STRING OPTIONS(description="Name of the script or stored procedure executing the job."),
      log_file STRING OPTIONS(description="Virtual log file name for historical reference."),
      stichtag STRING OPTIONS(description="Cutoff date for the job in DDMMYYYY format."),
      status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'STARTED', 'COMPLETED', 'FAILED')."),
      error_message STRING OPTIONS(description="Detailed error message if the job failed."),
      created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the audit entry was created.")
    )
    PARTITION BY
      DATE(created_at)
    CLUSTER BY
      job_name, job_id;
    """
    bq_client.query(create_table_sql).result()

    # Create the mock core SP
    mock_sp_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.{CORE_SP_ID}`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}` (job_id, job_name, script_name, log_file, stichtag, status, error_message, created_at)
      VALUES (p_eintragsnr, p_jobkennung, '{CORE_SP_ID}_mock', 'mock_log.log', p_stichtag, 'CALLED',
              CONCAT('Params: stichtag=', p_stichtag, ', wiederanlaufWert=', CAST(p_wiederanlaufWert AS STRING)), CURRENT_TIMESTAMP());

      IF p_wiederanlaufWert = -1 THEN -- Special value to trigger failure
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated failure in {CORE_SP_ID}_mock due to p_wiederanlaufWert = -1';
      END IF;
    END;
    """
    bq_client.query(mock_sp_sql).result()

    # Truncate table before each test
    truncate_table_sql = f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}`;"
    bq_client.query(truncate_table_sql).result()
    yield
    # Truncate table after each test (optional, but good for isolation)
    bq_client.query(truncate_table_sql).result()
    # Drop mock SP after all tests in the module
    # bq_client.query(f"DROP PROCEDURE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.{CORE_SP_ID}`;").result()


def test_audit_table_schema(bq_client, setup_and_teardown_audit_table):
    """Verifies the schema of the dwmsg_job_audit table."""
    expected_schema = {
        "job_id": {"data_type": "INT64", "is_nullable": "NO"},
        "job_name": {"data_type": "STRING", "is_nullable": "NO"},
        "script_name": {"data_type": "STRING", "is_nullable": "YES"},
        "log_file": {"data_type": "STRING", "is_nullable": "YES"},
        "stichtag": {"data_type": "STRING", "is_nullable": "YES"},
        "status": {"data_type": "STRING", "is_nullable": "NO"},
        "error_message": {"data_type": "STRING", "is_nullable": "YES"},
        "created_at": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
    }

    query = f"""
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = '{AUDIT_TABLE_ID}'
    ORDER BY
        column_name
    """
    query_job = bq_client.query(query)
    results = query_job.result()

    actual_schema = {row.column_name: {"data_type": row.data_type, "is_nullable": row.is_nullable} for row in results}

    assert len(actual_schema) == len(expected_schema), "Number of columns mismatch"
    for col_name, expected_props in expected_schema.items():
        assert col_name in actual_schema, f"Column {col_name} missing"
        assert actual_schema[col_name]["data_type"] == expected_props["data_type"], \
            f"Data type mismatch for {col_name}: Expected {expected_props['data_type']}, Got {actual_schema[col_name]['data_type']}"
        assert actual_schema[col_name]["is_nullable"] == expected_props["is_nullable"], \
            f"Nullability mismatch for {col_name}: Expected {expected_props['is_nullable']}, Got {actual_schema[col_name]['is_nullable']}"

    # Verify partitioning and clustering
    table = bq_client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}")
    assert table.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
    assert table.time_partitioning.field == "created_at"
    assert table.clustering_fields == ["job_name", "job_id"]

```

---

## Test Case 2: Default Parameter Handling (No Inputs)

*   **Purpose:** Verify that when `p_stichtag_in` and `p_wiederanlaufWert_in` are not provided (i.e., `NULL`), the wrapper correctly defaults `v_stichtag` to the current system date (`DDMMYYYY` format) and `v_wiederanlaufWert` to `0`. It also checks for the correct 'STARTED' and 'COMPLETED' log entries.
*   **Setup:**
    *   `dwmsg_job_audit` table is empty.
    *   The mock `k_ausd_bp_ta_msisdn_his` SP exists and is configured to succeed.
*   **Action:** Call `project.dataset.ausd_bp_ta_msisdn_his_wrapper(NULL, NULL)`.
*   **Pass/Fail Criterion:**
    1.  Exactly three entries are found in `dwmsg_job_audit` for `job_name = 'AUSD_BP_TA_MSISDN_HIS'` (STARTED, CALLED, COMPLETED).
    2.  The `stichtag` in the 'STARTED' and 'COMPLETED' entries matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    3.  The 'CALLED' entry (from the mock core SP) confirms `p_wiederanlaufWert = 0` and `p_stichtag` matching the current date were passed.
*   **Runnable Test Code (Pytest):**

```python
def test_default_parameter_handling(bq_client, setup_and_teardown_audit_table):
    """Tests that the wrapper correctly defaults parameters when none are provided."""
    # Action
    call_sp_sql = f"CALL `{PROJECT_ID}.{DATASET_ID}.{WRAPPER_SP_ID}`(NULL, NULL);"
    bq_client.query(call_sp_sql).result()

    # Pass/Fail Criterion
    today_ddmmyyyy = datetime.now().strftime('%d%m%Y')

    query_audit = f"""
    SELECT job_id, job_name, stichtag, status, error_message
    FROM `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}`
    WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS'
    ORDER BY created_at
    """
    audit_entries = list(bq_client.query(query_audit).result())

    assert len(audit_entries) == 3, "Expected 3 audit entries (STARTED, CALLED, COMPLETED)"
    
    # Verify STARTED entry
    started_entry = audit_entries[0]
    assert started_entry.status == 'STARTED'
    assert started_entry.stichtag == today_ddmmyyyy

    # Verify CALLED entry (from mock SP)
    called_entry = audit_entries[1]
    assert called_entry.status == 'CALLED'
    assert called_entry.stichtag == today_ddmmyyyy
    assert 'wiederanlaufWert=0' in called_entry.error_message # error_message used to log params

    # Verify COMPLETED entry
    completed_entry = audit_entries[2]
    assert completed_entry.status == 'COMPLETED'
    assert completed_entry.stichtag == today_ddmmyyyy
    assert completed_entry.error_message is None
```

---

## Test Case 3: Explicit Parameter Handling

*   **Purpose:** Verify that the stored procedure correctly accepts and uses explicitly provided `p_stichtag_in` and `p_wiederanlaufWert_in` values, passing them accurately to the core script.
*   **Setup:**
    *   `dwmsg_job_audit` table is empty.
    *   The mock `k_ausd_bp_ta_msisdn_his` SP exists and is configured to succeed.
*   **Action:** Call `project.dataset.ausd_bp_ta_msisdn_his_wrapper('01012023', 12345)`.
*   **Pass/Fail Criterion:**
    1.  Exactly three entries are found in `dwmsg_job_audit` for `job_name = 'AUSD_BP_TA_MSISDN_HIS'` (STARTED, CALLED, COMPLETED).
    2.  The `stichtag` in the 'STARTED' and 'COMPLETED' entries is '01012023'.
    3.  The 'CALLED' entry confirms `p_wiederanlaufWert = 12345` and `p_stichtag = '01012023'` were passed to the mock core SP.
*   **Runnable Test Code (Pytest):**

```python
def test_explicit_parameter_handling(bq_client, setup_and_teardown_audit_table):
    """Tests that the wrapper correctly handles explicitly provided parameters."""
    # Action
    stichtag_param = '01012023'
    wiederanlauf_param = 12345
    call_sp_sql = f"CALL `{PROJECT_ID}.{DATASET_ID}.{WRAPPER_SP_ID}`('{stichtag_param}', {wiederanlauf_param});"
    bq_client.query(call_sp_sql).result()

    # Pass/Fail Criterion
    query_audit = f"""
    SELECT job_id, job_name, stichtag, status, error_message
    FROM `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}`
    WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS'
    ORDER BY created_at
    """
    audit_entries = list(bq_client.query(query_audit).result())

    assert len(audit_entries) == 3, "Expected 3 audit entries (STARTED, CALLED, COMPLETED)"
    
    # Verify STARTED entry
    started_entry = audit_entries[0]
    assert started_entry.status == 'STARTED'
    assert started_entry.stichtag == stichtag_param

    # Verify CALLED entry (from mock SP)
    called_entry = audit_entries[1]
    assert called_entry.status == 'CALLED'
    assert called_entry.stichtag == stichtag_param
    assert f'wiederanlaufWert={wiederanlauf_param}' in called_entry.error_message

    # Verify COMPLETED entry
    completed_entry = audit_entries[2]
    assert completed_entry.status == 'COMPLETED'
    assert completed_entry.stichtag == stichtag_param
    assert completed_entry.error_message is None
```

---

## Test Case 4: `Stichtag` Validation (Empty String)

*   **Purpose:** Verify that if `p_stichtag_in` is provided as an empty string, the wrapper's validation logic (replacing `pruefeParameterGesetzt`) correctly identifies it as an invalid parameter, logs a 'FAILED' status, and raises an error.
*   **Setup:**
    *   `dwmsg_job_audit` table is empty.
    *   The mock `k_ausd_bp_ta_msisdn_his` SP exists.
*   **Action:** Call `project.dataset.ausd_bp_ta_msisdn_his_wrapper('', 123)`.
*   **Pass/Fail Criterion:**
    1.  The call to the SP raises a `GoogleAPICallError` containing the message 'Required parameter Stichtag is missing or empty.'.
    2.  Exactly two entries are found in `dwmsg_job_audit` (STARTED, FAILED).
    3.  The 'FAILED' entry's `error_message` contains the expected validation message.
*   **Runnable Test Code (Pytest):**

```python
def test_stichtag_validation_empty_string(bq_client, setup_and_teardown_audit_table):
    """Tests that the wrapper raises an error and logs failure if Stichtag is an empty string."""
    # Action: Call with empty string for stichtag
    call_sp_sql = f"CALL `{PROJECT_ID}.{DATASET_ID}.{WRAPPER_SP_ID}`('', 123);"
    
    with pytest.raises(bigquery.exceptions.GoogleAPICallError) as excinfo:
        bq_client.query(call_sp_sql).result()

    # Pass/Fail Criterion 1: Error message check
    assert "Required parameter Stichtag is missing or empty." in str(excinfo.value)

    # Pass/Fail Criterion 2 & 3: Audit table entries
    query_audit = f"""
    SELECT job_id, job_name, stichtag, status, error_message
    FROM `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}`
    WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS'
    ORDER BY created_at
    """
    audit_entries = list(bq_client.query(query_audit).result())

    assert len(audit_entries) == 2, "Expected 2 audit entries (STARTED, FAILED)"
    
    started_entry = audit_entries[0]
    assert started_entry.status == 'STARTED'
    assert started_entry.stichtag == '' # Stichtag was passed as empty string

    failed_entry = audit_entries[1]
    assert failed_entry.status == 'FAILED'
    assert "Required parameter Stichtag is missing or empty." in failed_entry.error_message
```

---

## Test Case 5: Error Handling in Core Script Invocation

*   **Purpose:** Verify that if the invoked core stored procedure (`k_ausd_bp_ta_msisdn_his`) fails, the wrapper correctly catches the error (replacing `trap` and `DWMSG_Fehlerbehandlung`), logs a 'FAILED' status, and re-raises the error to its caller.
*   **Setup:**
    *   `dwmsg_job_audit` table is empty.
    *   The mock `k_ausd_bp_ta_msisdn_his` SP exists and is configured to fail when `p_wiederanlaufWert = -1`.
*   **Action:** Call `project.dataset.ausd_bp_ta_msisdn_his_wrapper('01012023', -1)`.
*   **Pass/Fail Criterion:**
    1.  The call to the SP raises a `GoogleAPICallError`.
    2.  Exactly three entries are found in `dwmsg_job_audit` (STARTED, CALLED, FAILED).
    3.  The 'FAILED' entry's `error_message` contains the simulated error message from the mock core SP.
*   **Runnable Test Code (Pytest):**

```python
def test_core_script_failure_handling(bq_client, setup_and_teardown_audit_table):
    """Tests that the wrapper correctly handles errors from the invoked core script."""
    # Action
    stichtag_param = '01012023'
    wiederanlauf_param = -1 # This value triggers failure in mock SP
    call_sp_sql = f"CALL `{PROJECT_ID}.{DATASET_ID}.{WRAPPER_SP_ID}`('{stichtag_param}', {wiederanlauf_param});"
    
    with pytest.raises(bigquery.exceptions.GoogleAPICallError) as excinfo:
        bq_client.query(call_sp_sql).result()

    # Pass/Fail Criterion 1: Error message check
    assert "Simulated failure in k_ausd_bp_ta_msisdn_his_mock due to p_wiederanlaufWert = -1" in str(excinfo.value)

    # Pass/Fail Criterion 2 & 3: Audit table entries
    query_audit = f"""
    SELECT job_id, job_name, stichtag, status, error_message
    FROM `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}`
    WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS'
    ORDER BY created_at
    """
    audit_entries = list(bq_client.query(query_audit).result())

    assert len(audit_entries) == 3, "Expected 3 audit entries (STARTED, CALLED, FAILED)"
    
    started_entry = audit_entries[0]
    assert started_entry.status == 'STARTED'
    assert started_entry.stichtag == stichtag_param

    called_entry = audit_entries[1]
    assert called_entry.status == 'CALLED'
    assert called_entry.stichtag == stichtag_param
    assert f'wiederanlaufWert={wiederanlauf_param}' in called_entry.error_message

    failed_entry = audit_entries[2]
    assert failed_entry.status == 'FAILED'
    assert "Simulated failure in k_ausd_bp_ta_msisdn_his_mock due to p_wiederanlaufWert = -1" in failed_entry.error_message
```

---

## Test Case 6: Job ID Generation and Log File Naming

*   **Purpose:** Verify that `job_id` is correctly incremented for subsequent runs of the same job (`JobKennung`) and that `log_file` names are generated consistently based on the `job_id`. This tests the BigQuery equivalents of `DWMSG_ErmittleNr` and `DWMSG_Logdateiname`.
*   **Setup:**
    *   `dwmsg_job_audit` table is empty.
    *   The mock `k_ausd_bp_ta_msisdn_his` SP exists and is configured to succeed.
*   **Action:**
    1.  Call `project.dataset.ausd_bp_ta_msisdn_his_wrapper(NULL, NULL)` (first run).
    2.  Call `project.dataset.ausd_bp_ta_msisdn_his_wrapper('02022023', 100)` (second run).
*   **Pass/Fail Criterion:**
    1.  The first run's 'STARTED' entry has `job_id = 1` and `log_file` like `job_1_AUSD_BP_TA_MSISDN_HIS.log`.
    2.  The second run's 'STARTED' entry has `job_id = 2` and `log_file` like `job_2_AUSD_BP_TA_MSISDN_HIS.log`.
    3.  A total of six entries (2 STARTED, 2 CALLED, 2 COMPLETED) are found in `dwmsg_job_audit`.
*   **Runnable Test Code (Pytest):**

```python
def test_job_id_and_log_file_generation(bq_client, setup_and_teardown_audit_table):
    """Tests that job_id increments and log_file names are generated correctly across multiple runs."""
    # Action 1: First run
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{WRAPPER_SP_ID}`(NULL, NULL);").result()

    # Action 2: Second run
    stichtag_second_run = '02022023'
    wiederanlauf_second_run = 100
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{WRAPPER_SP_ID}`('{stichtag_second_run}', {wiederanlauf_second_run});").result()

    # Pass/Fail Criterion
    query_audit = f"""
    SELECT job_id, job_name, log_file, stichtag, status, created_at
    FROM `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}`
    WHERE job_name = 'AUSD_BP_TA_MSISDN_HIS'
    ORDER BY created_at
    """
    audit_entries = list(bq_client.query(query_audit).result())

    assert len(audit_entries) == 6, "Expected 6 audit entries (2 STARTED, 2 CALLED, 2 COMPLETED)"

    # First run checks
    first_run_started = audit_entries[0]
    first_run_called = audit_entries[1]
    first_run_completed = audit_entries[2]

    assert first_run_started.job_id == 1
    assert first_run_started.log_file == 'job_1_AUSD_BP_TA_MSISDN_HIS.log'
    assert first_run_started.status == 'STARTED'
    assert first_run_called.job_id == 1
    assert first_run_called.status == 'CALLED'
    assert first_run_completed.job_id == 1
    assert first_run_completed.status == 'COMPLETED'

    # Second run checks
    second_run_started = audit_entries[3]
    second_run_called = audit_entries[4]
    second_run_completed = audit_entries[5]

    assert second_run_started.job_id == 2
    assert second_run_started.log_file == 'job_2_AUSD_BP_TA_MSISDN_HIS.log'
    assert second_run_started.status == 'STARTED'
    assert second_run_started.stichtag == stichtag_second_run
    assert second_run_called.job_id == 2
    assert second_run_called.status == 'CALLED'
    assert f'wiederanlaufWert={wiederanlauf_second_run}' in second_run_called.error_message
    assert second_run_completed.job_id == 2
    assert second_run_completed.status == 'COMPLETED'
    assert second_run_completed.stichtag == stichtag_second_run
```

---

## Test Case 7: `dwmsg_job_audit` Table Creation Idempotency

*   **Purpose:** Verify that executing the `dwmsg_job_audit_table.sql` DDL script multiple times does not cause errors or unintended schema changes if the table already exists. This ensures robustness in deployment.
*   **Setup:** The `dwmsg_job_audit` table is created once (e.g., by the `setup_and_teardown_audit_table` fixture).
*   **Action:** Execute the `dwmsg_job_audit_table.sql` DDL script again.
*   **Pass/Fail Criterion:** The DDL execution completes without error, and the table schema remains identical to its initial state.
*   **Runnable Test Code (Pytest):**

```python
def test_audit_table_creation_idempotency(bq_client, setup_and_teardown_audit_table):
    """Verifies that creating the audit table multiple times is idempotent."""
    # Get initial schema
    initial_schema_query = f"SELECT column_name, data_type, is_nullable FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{AUDIT_TABLE_ID}' ORDER BY column_name;"
    initial_schema_results = list(bq_client.query(initial_schema_query).result())

    # Action: Execute DDL again
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}` (
      job_id INT64 NOT NULL, job_name STRING NOT NULL, script_name STRING, log_file STRING,
      stichtag STRING, status STRING NOT NULL, error_message STRING, created_at TIMESTAMP NOT NULL
    )
    PARTITION BY DATE(created_at) CLUSTER BY job_name, job_id;
    """
    try:
        bq_client.query(create_table_sql).result()
        # If it reaches here, it means no error was raised
        assert True, "DDL executed idempotently without error."
    except Exception as e:
        pytest.fail(f"DDL execution failed on second run: {e}")

    # Pass/Fail Criterion: Schema remains unchanged
    final_schema_query = f"SELECT column_name, data_type, is_nullable FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{AUDIT_TABLE_ID}' ORDER BY column_name;"
    final_schema_results = list(bq_client.query(final_schema_query).result())

    assert initial_schema_results == final_schema_results, "Table schema changed after idempotent DDL execution."
```