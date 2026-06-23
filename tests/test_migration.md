The migration of `r_ausd_v_ta_barrier_zusgf.ksh` to a BigQuery Stored Procedure (`project.dataset.Vertragsdatenabgleich`) primarily involves translating shell orchestration logic, parameter handling, logging, and error trapping into BigQuery SQL. The original script is a wrapper, so the tests focus on its control flow and interaction with the logging framework and the core business logic stored procedure.

**Prerequisites for all Tests:**

Before running any tests, ensure the following BigQuery tables and schemas exist. These tables are used by the migrated stored procedure for logging and auditing.

```sql
-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `my_project.my_dataset`;

-- DDL for job_registry table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_registry` (
    job_id STRING NOT NULL,
    job_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    last_updated TIMESTAMP,
    error_message STRING
);

-- DDL for job_log table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
    job_id STRING,
    log_time TIMESTAMP,
    level STRING,
    message STRING
);

-- DDL for job_error table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_error` (
    job_id STRING,
    error_time TIMESTAMP,
    error_code INT64,
    error_message STRING
);
```

---

### Test Case 1: Output Parity - Successful Execution & Logging

**Purpose:**
Verify that a successful execution of the BigQuery Stored Procedure (`Vertragsdatenabgleich`) produces equivalent log entries and updates the job status correctly, mirroring the legacy script's successful run. This covers the `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `print` statements, and `DWMSG_SetzeStatusOK` functionalities.

**Setup:**
1.  **Clean Logging Tables:** Clear all data from `job_registry`, `job_log`, and `job_error` to ensure a clean state for the test.
2.  **Mock Core SP (Success):** Create a mock version of `project.dataset.k_ausd_v_ta_barrier_zusgf` that always succeeds and logs its invocation.

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- Mock k_ausd_v_ta_barrier_zusgf to always succeed
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - SUCCESS', p_job_kennung, p_dw_eintrags_nr));
END;
```

**Action:**
Execute the migrated wrapper stored procedure with default parameters (no specific flags).

```sql
CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);
```

**Pass/Fail Criterion:**
1.  **Job Registry:** Exactly one entry in `job_registry` with `status = 'OK'`, `job_name = 'Vertragsdatenabgleich'`, and `end_time` populated.
2.  **Job Log:**
    *   Multiple `INFO` level entries corresponding to job start, job details, stichtag info, core SP invocation, and successful completion message.
    *   The `job_id` in `job_log` entries must match the `job_id` in `job_registry`.
    *   The `message` content should closely match the legacy script's output (e.g., "Job started.", "----------------- Job -----------------------", "Mock core SP...", "Die Abarbeitung wurde ohne erkennbare Fehler beendet").
3.  **Job Error:** No entries in `job_error`.

```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_successful_execution_and_logging():
    # Action: Call the SP
    client.query(f"CALL `{project_id}.{dataset_id}.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);").result()

    # Get job_id from the latest job_registry entry
    job_registry_query = f"""
        SELECT job_id, job_name, status, error_message
        FROM `{project_id}.{dataset_id}.job_registry`
        ORDER BY start_time DESC
        LIMIT 1
    """
    job_registry_result = list(client.query(job_registry_query).result())
    assert len(job_registry_result) == 1, "Expected exactly one job_registry entry."
    job_entry = job_registry_result[0]
    assert job_entry.status == 'OK', f"Expected job status 'OK', got '{job_entry.status}'."
    assert job_entry.job_name == 'Vertragsdatenabgleich', f"Expected job_name 'Vertragsdatenabgleich', got '{job_entry.job_name}'."
    assert job_entry.error_message is None, "Expected no error_message for a successful job."

    job_id = job_entry.job_id

    # Verify job_log entries
    job_log_query = f"""
        SELECT message, level
        FROM `{project_id}.{dataset_id}.job_log`
        WHERE job_id = '{job_id}'
        ORDER BY log_time ASC
    """
    job_log_results = [row.message for row in client.query(job_log_query).result()]

    expected_log_messages_substrings = [
        "Job started. Job-ID:",
        "----------------- Job -----------------------",
        "JobKennung: 'BERT_V_TA_BARRIER_ZUSGF'",
        "Logdatei  : 'N/A (logging to BigQuery tables)'",
        "Stichtag Info:",
        "Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: BERT_V_TA_BARRIER_ZUSGF",
        "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
    ]

    for expected_substring in expected_log_messages_substrings:
        assert any(expected_substring in msg for msg in job_log_results), \
            f"Expected log message containing '{expected_substring}' not found in job_log."

    # Verify no job_error entries
    job_error_query = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.job_error` WHERE job_id = '{job_id}'"
    error_count = client.query(job_error_query).result().scalar_iterator().next()
    assert error_count == 0, "Expected no job_error entries for a successful job."
```

---

### Test Case 2: Output Parity - Usage Display (`-h` / `p_h`)

**Purpose:**
Verify that when the `p_h` parameter is set to `TRUE`, the stored procedure correctly logs the usage message and exits without performing any other job processing, similar to the legacy script's `-h` flag.

**Setup:**
1.  **Clean Logging Tables:** Clear all data from `job_registry`, `job_log`, and `job_error`.
2.  **Mock Core SP (Success):** Ensure the mock `k_ausd_v_ta_barrier_zusgf` is in place (though it shouldn't be called in this scenario).

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- (Re-create if needed, but should be from previous test)
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - SUCCESS', p_job_kennung, p_dw_eintrags_nr));
END;
```

**Action:**
Execute the migrated wrapper stored procedure with `p_h` set to `TRUE`.

```sql
CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => TRUE, p_s => NULL, p_l => NULL);
```

**Pass/Fail Criterion:**
1.  **Job Log:** Exactly one `INFO` level entry in `job_log` containing the full usage message. The `job_id` for this entry will be a newly generated UUID, but it won't be linked to a `job_registry` entry.
2.  **Job Registry:** No entries in `job_registry`.
3.  **Job Error:** No entries in `job_error`.
4.  **Core SP Invocation:** The mock `k_ausd_v_ta_barrier_zusgf` should *not* have been called (verified by absence of its specific log message).

```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_usage_display():
    # Action: Call the SP with p_h => TRUE
    client.query(f"CALL `{project_id}.{dataset_id}.Vertragsdatenabgleich`(p_h => TRUE, p_s => NULL, p_l => NULL);").result()

    # Verify job_registry is empty
    job_registry_count_query = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.job_registry`"
    registry_count = client.query(job_registry_count_query).result().scalar_iterator().next()
    assert registry_count == 0, "Expected no job_registry entries when p_h is TRUE."

    # Verify job_error is empty
    job_error_count_query = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.job_error`"
    error_count = client.query(job_error_count_query).result().scalar_iterator().next()
    assert error_count == 0, "Expected no job_error entries when p_h is TRUE."

    # Verify job_log contains the usage message and nothing else related to job execution
    job_log_query = f"""
        SELECT message, level
        FROM `{project_id}.{dataset_id}.job_log`
        ORDER BY log_time ASC
    """
    job_log_results = list(client.query(job_log_query).result())

    assert len(job_log_results) == 1, "Expected exactly one log entry for usage display."
    log_entry = job_log_results[0]
    assert log_entry.level == 'INFO', f"Expected log level 'INFO', got '{log_entry.level}'."
    assert "Programm: Vertragsdatenabgleich" in log_entry.message, "Usage message not found in log."
    assert "zeigt diese Seite an" in log_entry.message, "Usage message not found in log."
    assert "Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_barrier_zusgf." in log_entry.message, "Usage message not found in log."

    # Verify core SP was not called (by checking for its specific log message)
    core_sp_log_query = f"""
        SELECT COUNT(*)
        FROM `{project_id}.{dataset_id}.job_log`
        WHERE message LIKE 'Mock core SP k_ausd_v_ta_barrier_zusgf called%'
    """
    core_sp_log_count = client.query(core_sp_log_query).result().scalar_iterator().next()
    assert core_sp_log_count == 0, "Core SP should not be called when p_h is TRUE."
```

---

### Test Case 3: Transformation Correctness - Error Handling (Core SP Failure)

**Purpose:**
Verify that if the invoked core business logic stored procedure (`k_ausd_v_ta_barrier_zusgf`) fails, the wrapper SP correctly catches the error, logs it to `job_log` and `job_error`, updates `job_registry` to `FAILED`, and re-raises the error using `SIGNAL SQLSTATE`. This mimics the `trap ERR` and `DWMSG_Fehlerbehandlung` behavior of the legacy script.

**Setup:**
1.  **Clean Logging Tables:** Clear all data from `job_registry`, `job_log`, and `job_error`.
2.  **Mock Core SP (Failure):** Create a mock version of `project.dataset.k_ausd_v_ta_barrier_zusgf` that always fails with a specific error message.

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- Mock k_ausd_v_ta_barrier_zusgf to always fail
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - INTENTIONAL FAILURE', p_job_kennung, p_dw_eintrags_nr));
    
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from k_ausd_v_ta_barrier_zusgf';
END;
```

**Action:**
Execute the migrated wrapper stored procedure with default parameters.

```sql
-- This call is expected to fail and raise an error
CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);
```

**Pass/Fail Criterion:**
1.  **Error Propagation:** The `CALL` statement for `Vertragsdatenabgleich` must raise an error (e.g., `45000` SQLSTATE) with a message indicating the failure.
2.  **Job Registry:** Exactly one entry in `job_registry` with `status = 'FAILED'`, `job_name = 'Vertragsdatenabgleich'`, `end_time` populated, and `error_message` containing the error details.
3.  **Job Log:**
    *   Entries for job start, job details, stichtag info, core SP invocation, and an `ERROR` level entry indicating the failure.
    *   The `job_id` in `job_log` entries must match the `job_id` in `job_registry`.
4.  **Job Error:** Exactly one entry in `job_error` with the correct `job_id`, `error_time`, `error_code`, and `error_message` matching the simulated error.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_error_handling_core_sp_failure():
    # Action: Call the SP, expecting it to fail
    with pytest.raises(BadRequest) as excinfo:
        client.query(f"CALL `{project_id}.{dataset_id}.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);").result()
    
    assert "Simulated error from k_ausd_v_ta_barrier_zusgf" in str(excinfo.value), \
        "Expected error message from core SP not found in wrapper's error."

    # Get job_id from the latest job_registry entry
    job_registry_query = f"""
        SELECT job_id, job_name, status, error_message
        FROM `{project_id}.{dataset_id}.job_registry`
        ORDER BY start_time DESC
        LIMIT 1
    """
    job_registry_result = list(client.query(job_registry_query).result())
    assert len(job_registry_result) == 1, "Expected exactly one job_registry entry."
    job_entry = job_registry_result[0]
    assert job_entry.status == 'FAILED', f"Expected job status 'FAILED', got '{job_entry.status}'."
    assert job_entry.job_name == 'Vertragsdatenabgleich', f"Expected job_name 'Vertragsdatenabgleich', got '{job_entry.job_name}'."
    assert "Simulated error from k_ausd_v_ta_barrier_zusgf" in job_entry.error_message, \
        "Expected error_message in job_registry not found."

    job_id = job_entry.job_id

    # Verify job_log entries
    job_log_query = f"""
        SELECT message, level
        FROM `{project_id}.{dataset_id}.job_log`
        WHERE job_id = '{job_id}'
        ORDER BY log_time ASC
    """
    job_log_results = list(client.query(job_log_query).result())

    expected_log_messages_substrings = [
        "Job started. Job-ID:",
        "Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: BERT_V_TA_BARRIER_ZUSGF",
        "INTENTIONAL FAILURE",
        "Error during job execution: Simulated error from k_ausd_v_ta_barrier_zusgf"
    ]
    for expected_substring in expected_log_messages_substrings:
        assert any(expected_substring in msg.message for msg in job_log_results), \
            f"Expected log message containing '{expected_substring}' not found in job_log."
    
    assert any(row.level == 'ERROR' for row in job_log_results), "Expected at least one ERROR level log entry."

    # Verify job_error entries
    job_error_query = f"""
        SELECT error_message, error_code
        FROM `{project_id}.{my_dataset}.job_error`
        WHERE job_id = '{job_id}'
    """
    job_error_results = list(client.query(job_error_query).result())
    assert len(job_error_results) == 1, "Expected exactly one job_error entry."
    error_entry = job_error_results[0]
    assert "Simulated error from k_ausd_v_ta_barrier_zusgf" in error_entry.error_message, \
        "Expected error_message in job_error not found."
    assert error_entry.error_code is not None, "Expected error_code to be populated."
```

---

### Test Case 4: Transformation Correctness - Parameter Handling (Unused `p_s`, `p_l`)

**Purpose:**
Confirm that the presence of `p_s` and `p_l` parameters in the BigQuery Stored Procedure does not alter the core logic or cause errors, mirroring the legacy script's behavior where these were parsed by `getopts` but not actually used within the provided script snippet.

**Setup:**
1.  **Clean Logging Tables:** Clear all data from `job_registry`, `job_log`, and `job_error`.
2.  **Mock Core SP (Success):** Ensure the mock `k_ausd_v_ta_barrier_zusgf` is set to always succeed.

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- Mock k_ausd_v_ta_barrier_zusgf to always succeed
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - SUCCESS', p_job_kennung, p_dw_eintrags_nr));
END;
```

**Action:**
Execute the migrated wrapper stored procedure, providing arbitrary values for `p_s` and `p_l`.

```sql
CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => 'some_value_s', p_l => 'some_value_l');
```

**Pass/Fail Criterion:**
1.  **Successful Execution:** The `CALL` statement must complete without raising an error.
2.  **Job Registry:** Exactly one entry in `job_registry` with `status = 'OK'`.
3.  **Job Log:** All expected log messages for a successful run should be present, and no error messages related to `p_s` or `p_l` should appear.
4.  **Core SP Invocation:** The mock `k_ausd_v_ta_barrier_zusgf` should have been called and succeeded, indicating the wrapper's core logic was unaffected.

```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_unused_parameters_handling():
    # Action: Call the SP with p_s and p_l values
    client.query(f"CALL `{project_id}.{dataset_id}.Vertragsdatenabgleich`(p_h => FALSE, p_s => 'some_value_s', p_l => 'some_value_l');").result()

    # Verify job_registry status
    job_registry_query = f"""
        SELECT status
        FROM `{project_id}.{dataset_id}.job_registry`
        ORDER BY start_time DESC
        LIMIT 1
    """
    job_status = client.query(job_registry_query).result().scalar_iterator().next()
    assert job_status == 'OK', f"Expected job status 'OK' even with p_s and p_l, got '{job_status}'."

    # Verify no error messages related to p_s or p_l in job_log
    job_log_query = f"""
        SELECT message
        FROM `{project_id}.{dataset_id}.job_log`
        WHERE message LIKE '%p_s%' OR message LIKE '%p_l%' OR level = 'ERROR'
    """
    error_logs = list(client.query(job_log_query).result())
    assert len(error_logs) == 0, "Found unexpected log entries related to p_s/p_l or errors."

    # Verify core SP was called and succeeded (implicitly checked by job_registry status 'OK' and absence of errors)
    # This test primarily ensures the parameters don't break the wrapper.
```

---

### Test Case 5: Data Quality / Schema Assertions - `job_registry`

**Purpose:**
Verify that the `job_registry` table correctly records job metadata, including schema adherence, non-null constraints for critical fields, and accurate status/timestamp updates for a successful job.

**Setup:**
1.  **Clean Logging Tables:** Clear `job_registry`.
2.  **Mock Core SP (Success):** Ensure the mock `k_ausd_v_ta_barrier_zusgf` is set to always succeed.
3.  **Execute SP:** Run `Vertragsdatenabgleich` once successfully to populate `job_registry`.

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`; -- Also clean for clarity, though not strictly needed for this test
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- Mock k_ausd_v_ta_barrier_zusgf to always succeed (if not already)
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - SUCCESS', p_job_kennung, p_dw_eintrags_nr));
END;

-- Action to populate job_registry
CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);
```

**Pass/Fail Criterion:**
1.  **Row Count:** Exactly one row in `job_registry`.
2.  **Schema & Data Types:** All columns (`job_id`, `job_name`, `start_time`, `end_time`, `status`, `last_updated`, `error_message`) exist and have the expected BigQuery data types.
3.  **Non-NULL Constraints:** `job_id`, `job_name`, `start_time`, `status`, `last_updated` are not NULL. `end_time` is not NULL for a completed job. `error_message` is NULL for a successful job.
4.  **Content Accuracy:**
    *   `job_id` is a valid UUID string.
    *   `job_name` is 'Vertragsdatenabgleich'.
    *   `status` is 'OK'.
    *   `start_time` is before `end_time` and `last_updated`.

```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_job_registry_data_quality_and_schema():
    # Query job_registry
    job_registry_query = f"""
        SELECT job_id, job_name, start_time, end_time, status, last_updated, error_message
        FROM `{project_id}.{dataset_id}.job_registry`
    """
    job_registry_results = list(client.query(job_registry_query).result())

    assert len(job_registry_results) == 1, "Expected exactly one entry in job_registry."
    job_entry = job_registry_results[0]

    # Check non-NULL constraints and data types (BigQuery client handles type conversion)
    assert job_entry.job_id is not None and isinstance(job_entry.job_id, str), "job_id is invalid."
    assert job_entry.job_name is not None and isinstance(job_entry.job_name, str), "job_name is invalid."
    assert job_entry.start_time is not None, "start_time is NULL."
    assert job_entry.end_time is not None, "end_time is NULL."
    assert job_entry.status is not None and isinstance(job_entry.status, str), "status is invalid."
    assert job_entry.last_updated is not None, "last_updated is NULL."
    assert job_entry.error_message is None, "error_message should be NULL for a successful job."

    # Check content accuracy
    assert job_entry.job_name == 'Vertragsdatenabgleich', f"Expected job_name 'Vertragsdatenabgleich', got '{job_entry.job_name}'."
    assert job_entry.status == 'OK', f"Expected status 'OK', got '{job_entry.status}'."
    assert job_entry.start_time < job_entry.end_time, "start_time is not before end_time."
    assert job_entry.end_time == job_entry.last_updated, "end_time and last_updated should be the same for a completed job."

    # Verify job_id format (UUID)
    import re
    assert re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', job_entry.job_id), \
        f"job_id '{job_entry.job_id}' is not a valid UUID format."
```

---

### Test Case 6: Data Quality / Schema Assertions - `job_log`

**Purpose:**
Verify that the `job_log` table correctly records log messages, including schema adherence, non-null constraints for critical fields, and accurate content for both successful and failed job executions.

**Setup:**
1.  **Clean Logging Tables:** Clear `job_log`.
2.  **Mock Core SPs:** Ensure both success and failure mocks for `k_ausd_v_ta_barrier_zusgf` are available.
3.  **Execute SPs:** Run `Vertragsdatenabgleich` once successfully and once with a simulated failure to generate diverse log entries.

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- Mock k_ausd_v_ta_barrier_zusgf to always succeed
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - SUCCESS', p_job_kennung, p_dw_eintrags_nr));
END;

-- Action: Run a successful job
CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);

-- Mock k_ausd_v_ta_barrier_zusgf to always fail
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - INTENTIONAL FAILURE', p_job_kennung, p_dw_eintrags_nr));
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from k_ausd_v_ta_barrier_zusgf';
END;

-- Action: Run a failed job (expecting error)
BEGIN
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);
EXCEPTION WHEN ERROR THEN
    -- Expected error, do nothing
END;
```

**Pass/Fail Criterion:**
1.  **Row Count:** At least 5-7 rows for a successful run, and additional rows for a failed run.
2.  **Schema & Data Types:** All columns (`job_id`, `log_time`, `level`, `message`) exist and have the expected BigQuery data types.
3.  **Non-NULL Constraints:** `log_time`, `level`, `message` are not NULL. `job_id` is not NULL for job-related logs (it can be NULL for usage display if `p_h` is true, but this test focuses on job execution logs).
4.  **Content Accuracy:**
    *   `level` contains 'INFO' and 'ERROR' entries.
    *   `message` content is meaningful and reflects the job's progress or errors.
    *   `job_id` matches the corresponding `job_registry` entry for each job.

```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_job_log_data_quality_and_schema():
    # Get job_ids from job_registry for verification
    job_ids_query = f"SELECT job_id FROM `{project_id}.{dataset_id}.job_registry` ORDER BY start_time ASC"
    job_ids = [row.job_id for row in client.query(job_ids_query).result()]
    assert len(job_ids) == 2, "Expected two job_ids (one success, one failure) in job_registry."

    # Query job_log
    job_log_query = f"""
        SELECT job_id, log_time, level, message
        FROM `{project_id}.{dataset_id}.job_log`
        ORDER BY log_time ASC
    """
    job_log_results = list(client.query(job_log_query).result())

    assert len(job_log_results) >= 10, "Expected a significant number of log entries (at least 10 for two runs)."

    info_count = 0
    error_count = 0
    for log_entry in job_log_results:
        # Check non-NULL constraints and data types
        assert log_entry.log_time is not None, "log_time is NULL."
        assert log_entry.level is not None and isinstance(log_entry.level, str), "level is invalid."
        assert log_entry.message is not None and isinstance(log_entry.message, str), "message is invalid."
        assert log_entry.job_id is not None and isinstance(log_entry.job_id, str), "job_id is NULL or invalid."

        # Check content accuracy
        assert log_entry.level in ['INFO', 'ERROR'], f"Unexpected log level: {log_entry.level}"
        if log_entry.level == 'INFO':
            info_count += 1
        elif log_entry.level == 'ERROR':
            error_count += 1
            assert "Error during job execution" in log_entry.message, "Error log message missing expected content."
        
        # Verify job_id linkage
        assert log_entry.job_id in job_ids, f"Log entry job_id '{log_entry.job_id}' does not match any registered job_id."

    assert info_count >= 8, "Expected multiple INFO log entries." # 4-5 per run
    assert error_count >= 1, "Expected at least one ERROR log entry from the failed run."
```

---

### Test Case 7: Data Quality / Schema Assertions - `job_error`

**Purpose:**
Verify that the `job_error` table correctly records error details, including schema adherence, non-null constraints for critical fields, and accurate content when a job fails.

**Setup:**
1.  **Clean Logging Tables:** Clear `job_error`.
2.  **Mock Core SP (Failure):** Ensure the mock `k_ausd_v_ta_barrier_zusgf` is set to always fail.
3.  **Execute SP:** Run `Vertragsdatenabgleich` with a simulated failure.

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- Mock k_ausd_v_ta_barrier_zusgf to always fail
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called with JobKennung: %s, DW_EintragsNr: %s - INTENTIONAL FAILURE', p_job_kennung, p_dw_eintrags_nr));
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from k_ausd_v_ta_barrier_zusgf';
END;

-- Action: Run a failed job (expecting error)
BEGIN
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);
EXCEPTION WHEN ERROR THEN
    -- Expected error, do nothing
END;
```

**Pass/Fail Criterion:**
1.  **Row Count:** Exactly one row in `job_error`.
2.  **Schema & Data Types:** All columns (`job_id`, `error_time`, `error_code`, `error_message`) exist and have the expected BigQuery data types.
3.  **Non-NULL Constraints:** All columns are not NULL.
4.  **Content Accuracy:**
    *   `job_id` matches the `job_id` of the failed job in `job_registry`.
    *   `error_message` contains the expected error text ("Simulated error from k_ausd_v_ta_barrier_zusgf").
    *   `error_code` is populated with a relevant integer.

```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_job_error_data_quality_and_schema():
    # Get job_id from the failed job in job_registry
    failed_job_id_query = f"""
        SELECT job_id
        FROM `{project_id}.{dataset_id}.job_registry`
        WHERE status = 'FAILED'
        ORDER BY start_time DESC
        LIMIT 1
    """
    failed_job_id = client.query(failed_job_id_query).result().scalar_iterator().next()
    assert failed_job_id is not None, "Failed job ID not found in job_registry."

    # Query job_error
    job_error_query = f"""
        SELECT job_id, error_time, error_code, error_message
        FROM `{project_id}.{dataset_id}.job_error`
        WHERE job_id = '{failed_job_id}'
    """
    job_error_results = list(client.query(job_error_query).result())

    assert len(job_error_results) == 1, "Expected exactly one entry in job_error for the failed job."
    error_entry = job_error_results[0]

    # Check non-NULL constraints and data types
    assert error_entry.job_id is not None and isinstance(error_entry.job_id, str), "job_id is invalid."
    assert error_entry.error_time is not None, "error_time is NULL."
    assert error_entry.error_code is not None and isinstance(error_entry.error_code, int), "error_code is invalid."
    assert error_entry.error_message is not None and isinstance(error_entry.error_message, str), "error_message is invalid."

    # Check content accuracy
    assert error_entry.job_id == failed_job_id, f"job_id in job_error '{error_entry.job_id}' does not match failed job_id '{failed_job_id}'."
    assert "Simulated error from k_ausd_v_ta_barrier_zusgf" in error_entry.error_message, \
        "Expected error_message content not found in job_error."
```

---

### Test Case 8: External System Replacements - Core SP Invocation

**Purpose:**
Verify that the `project.dataset.k_ausd_v_ta_barrier_zusgf` stored procedure is called exactly once by the wrapper SP, and that it receives the correct `JobKennung` and `DW_EintragsNr` parameters, replacing the legacy shell script's invocation of `k_ausd_v_ta_barrier_zusgf.ksh`.

**Setup:**
1.  **Clean Logging Tables:** Clear `job_registry`, `job_log`, and `job_error`.
2.  **Mock Core SP (Parameter Recording):** Create a special mock version of `project.dataset.k_ausd_v_ta_barrier_zusgf` that records the parameters it receives into a dedicated temporary table.

```sql
-- Cleanup
TRUNCATE TABLE `my_project.my_dataset.job_registry`;
TRUNCATE TABLE `my_project.my_dataset.job_log`;
TRUNCATE TABLE `my_project.my_dataset.job_error`;

-- DDL for mock call recording table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.mock_k_ausd_calls` (
    call_time TIMESTAMP,
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
);
TRUNCATE TABLE `my_project.my_dataset.mock_k_ausd_calls`;

-- Mock k_ausd_v_ta_barrier_zusgf to record parameters
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_barrier_zusgf`(
    p_job_kennung STRING,
    p_dw_eintrags_nr STRING
)
BEGIN
    INSERT INTO `my_project.my_dataset.mock_k_ausd_calls` (call_time, p_job_kennung, p_dw_eintrags_nr)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_dw_eintrags_nr);
    
    INSERT INTO `my_project.my_dataset.job_log` (job_id, log_time, level, message)
    VALUES (p_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Mock core SP k_ausd_v_ta_barrier_zusgf called and parameters recorded: JobKennung: %s, DW_EintragsNr: %s', p_job_kennung, p_dw_eintrags_nr));
END;
```

**Action:**
Execute the migrated wrapper stored procedure.

```sql
CALL `my_project.my_dataset.Vertragsdatenabgleich`(p_h => FALSE, p_s => NULL, p_l => NULL);
```

**Pass/Fail Criterion:**
1.  **Call Count:** Exactly one entry in `my_project.my_dataset.mock_k_ausd_calls`.
2.  **Parameter Accuracy:** The `p_job_kennung` recorded in `mock_k_ausd_calls` must be 'BERT_V_TA_BARRIER_ZUSGF'. The `p_dw_eintrags_nr` recorded must match the `job_id` generated and stored in `job_registry` for this execution.

```python
import pytest
from google.cloud import bigquery

client = bigquery.Client()
project_id = 'my_project'
dataset_id = 'my_dataset'

def test_core_sp_invocation_parameters():
    # Get the job_id generated by the wrapper SP
    job_registry_query = f"""
        SELECT job_id
        FROM `{project_id}.{dataset_id}.job_registry`
        ORDER BY start_time DESC
        LIMIT 1
    """
    wrapper_job_id = client.query(job_registry_query).result().scalar_iterator().next()
    assert wrapper_job_id is not None, "Wrapper job_id not found in job_registry."

    # Query the mock_k_ausd_calls table
    mock_calls_query = f"""
        SELECT p_job_kennung, p_dw_eintrags_nr
        FROM `{project_id}.{dataset_id}.mock_k_ausd_calls`
    """
    mock_calls_results = list(client.query(mock_calls_query).result())

    assert len(mock_calls_results) == 1, "Expected exactly one call to the mock core SP."
    mock_call_entry = mock_calls_results[0]

    # Verify parameters
    expected_job_kennung = 'BERT_V_TA_BARRIER_ZUSGF'
    assert mock_call_entry.p_job_kennung == expected_job_kennung, \
        f"Expected JobKennung '{expected_job_kennung}', got '{mock_call_entry.p_job_kennung}'."
    assert mock_call_entry.p_dw_eintrags_nr == wrapper_job_id, \
        f"Expected DW_EintragsNr '{wrapper_job_id}', got '{mock_call_entry.p_dw_eintrags_nr}'."
```