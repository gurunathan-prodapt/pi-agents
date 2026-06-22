As a senior data-migration QA engineer, I've designed a suite of validation tests for the migrated BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_beschr`, which replaces the legacy KornShell script `r_ausd_bp_ta_bpr_beschr.ksh`. These tests aim to ensure behavioral equivalence across the key areas outlined.

The core data processing logic, residing in `k_ausd_bp_ta_bpr_beschr.ksh` (migrated to `project.dataset.k_ausd_bp_ta_bpr_beschr`), is a critical dependency. For these tests, `project.dataset.k_ausd_bp_ta_bpr_beschr` is treated as a mockable component that can either succeed or fail as required by the test case.

**General Setup for all Tests:**

*   **BigQuery Environment:** A Google Cloud Project and BigQuery Dataset (`project.dataset`) must be available.
*   **Table Creation:** The `job_control` and `job_error_log` tables must be created using the provided DDL:
    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
        job_entry_nr INT64 NOT NULL,
        job_name STRING NOT NULL,
        script_name STRING,
        log_reference STRING,
        stichtag STRING,
        status STRING,
        created_at TIMESTAMP,
        finished_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
        job_name STRING NOT NULL,
        job_entry_nr INT64,
        error_number INT64,
        error_argument STRING,
        error_message STRING,
        created_at TIMESTAMP
    );
    ```
*   **Core Processing SP (Mock/Placeholder):** The `project.dataset.k_ausd_bp_ta_bpr_beschr` stored procedure must be deployed. For testing purposes, it will be temporarily modified or assumed to behave in a specific way (success/failure). The default successful version is:
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_beschr`(
      IN p_job_kennung STRING,
      IN p_stichtag STRING,
      IN p_dw_eintrags_nr INT64,
      IN p_wiederanlauf_wert INT64
    )
    BEGIN
      -- Log successful invocation for testing purposes, indicating parameters received
      INSERT INTO `project.dataset.job_error_log` (job_name, job_entry_nr, error_message, created_at)
      VALUES (p_job_kennung, p_dw_eintrags_nr, CONCAT('k_ausd_bp_ta_bpr_beschr called successfully with stichtag=', p_stichtag, ' and wiederanlauf_wert=', CAST(p_wiederanlauf_wert AS STRING)), CURRENT_TIMESTAMP());
    END;
    ```
    *(Note: The `job_error_log` insert here is an enhancement for testing to capture parameters passed to the core SP. The original design only had a placeholder comment.)*
*   **Wrapper SP:** The `project.dataset.ausd_bp_ta_bpr_beschr` stored procedure must be deployed as provided in the migration design.
*   **Test Framework:** The provided test code snippets use a `pytest`-like structure with `google.cloud.bigquery` client for execution and assertions.

---

## Test Case 1: Happy Path - Default Parameters

*   **Purpose:** Verify the wrapper SP executes successfully when no parameters are explicitly provided, correctly defaulting `stichtag` to the current system date (`DDMMYYYY`) and `p_wiederanlaufWert` to `0`. It should record a 'STARTED' and then 'OK' status in `job_control`.
*   **Setup:**
    *   Ensure `job_control` and `job_error_log` tables are empty.
    *   `project.dataset.k_ausd_bp_ta_bpr_beschr` is configured to succeed (as per the default mock above).
*   **Action:**
    *   Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_beschr` with `NULL` for both parameters.
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_beschr`(NULL, NULL);
    ```
*   **Pass/Fail Criterion:**
    *   One entry in `project.dataset.job_control` with `job_name = 'AUSD_BP_TA_BPR_BESCHR'` and `status = 'OK'`.
    *   The `stichtag` in `job_control` should match `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` at the time of execution.
    *   `job_entry_nr` should be `1`.
    *   `created_at` and `finished_at` timestamps should be populated, with `finished_at` being after `created_at`.
    *   No entries in `project.dataset.job_error_log` with `error_number` populated.
    *   One entry in `project.dataset.job_error_log` (from the mock `k_ausd_bp_ta_bpr_beschr` SP) indicating successful invocation with `wiederanlauf_wert=0`.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.fixture(autouse=True)
def cleanup_tables(bq_client):
    """Truncates tables before and after each test."""
    bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`").result()
    yield
    bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`").result()

def test_happy_path_default_parameters(bq_client):
    # Action
    bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_beschr`(NULL, NULL);").result()

    # Assertions for job_control table
    job_control_query = """
        SELECT job_entry_nr, job_name, stichtag, status, created_at, finished_at
        FROM `project.dataset.job_control`
        WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
        ORDER BY created_at DESC
        LIMIT 1
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == 1, "Expected one entry in job_control table."
    job_entry = job_control_rows[0]

    assert job_entry.job_entry_nr == 1, "job_entry_nr should be 1 for the first run."
    assert job_entry.job_name == 'AUSD_BP_TA_BPR_BESCHR', "Job name mismatch."
    
    # Stichtag should be current date in DDMMYYYY format
    expected_stichtag = datetime.now().strftime('%d%m%Y')
    assert job_entry.stichtag == expected_stichtag, f"Stichtag mismatch. Expected {expected_stichtag}, got {job_entry.stichtag}."
    assert job_entry.status == 'OK', "Job status should be 'OK'."
    assert job_entry.created_at is not None, "created_at should be populated."
    assert job_entry.finished_at is not None, "finished_at should be populated."
    assert job_entry.finished_at >= job_entry.created_at, "finished_at should be after created_at."

    # Assertions for job_error_log (only for k_ausd_bp_ta_bpr_beschr success log)
    error_log_query = "SELECT COUNT(*) FROM `project.dataset.job_error_log` WHERE error_number IS NOT NULL"
    error_count = bq_client.query(error_log_query).result().scalar_iterator().next()
    assert error_count == 0, "No error entries expected in job_error_log."

    k_core_log_query = """
        SELECT error_message FROM `project.dataset.job_error_log`
        WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
        AND error_message LIKE '%k_ausd_bp_ta_bpr_beschr called successfully with stichtag=% and wiederanlauf_wert=0%'
    """
    k_core_log_rows = list(bq_client.query(k_core_log_query).result())
    assert len(k_core_log_rows) == 1, "Expected one success log entry from k_ausd_bp_ta_bpr_beschr."
    assert f"stichtag={expected_stichtag}" in k_core_log_rows[0].error_message
    assert "wiederanlauf_wert=0" in k_core_log_rows[0].error_message
```

---

## Test Case 2: Happy Path - Explicit Parameters

*   **Purpose:** Verify the wrapper SP correctly uses provided `stichtag` and `wiederanlaufWert` and completes successfully.
*   **Setup:**
    *   Ensure `job_control` and `job_error_log` tables are empty.
    *   `project.dataset.k_ausd_bp_ta_bpr_beschr` is configured to succeed.
*   **Action:**
    *   Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_beschr` with specific `stichtag` (`'01012023'`) and `wiederanlaufWert` (`12345`).
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_beschr`('01012023', 12345);
    ```
*   **Pass/Fail Criterion:**
    *   One entry in `project.dataset.job_control` with `job_name = 'AUSD_BP_TA_BPR_BESCHR'` and `status = 'OK'`.
    *   The `stichtag` in `job_control` should be `'01012023'`.
    *   `job_entry_nr` should be `1`.
    *   No entries in `project.dataset.job_error_log` with `error_number` populated.
    *   One entry in `project.dataset.job_error_log` (from the mock `k_ausd_bp_ta_bpr_beschr` SP) indicating successful invocation with `stichtag='01012023'` and `wiederanlauf_wert=12345`.

```python
def test_happy_path_explicit_parameters(bq_client):
    # Action
    bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_beschr`('01012023', 12345);").result()

    # Assertions for job_control table
    job_control_query = """
        SELECT job_entry_nr, job_name, stichtag, status, created_at, finished_at
        FROM `project.dataset.job_control`
        WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
        ORDER BY created_at DESC
        LIMIT 1
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == 1, "Expected one entry in job_control table."
    job_entry = job_control_rows[0]

    assert job_entry.job_entry_nr == 1, "job_entry_nr should be 1 for the first run."
    assert job_entry.job_name == 'AUSD_BP_TA_BPR_BESCHR', "Job name mismatch."
    assert job_entry.stichtag == '01012023', "Stichtag mismatch."
    assert job_entry.status == 'OK', "Job status should be 'OK'."
    assert job_entry.created_at is not None, "created_at should be populated."
    assert job_entry.finished_at is not None, "finished_at should be populated."

    # Assertions for job_error_log (only for k_ausd_bp_ta_bpr_beschr success log)
    error_log_query = "SELECT COUNT(*) FROM `project.dataset.job_error_log` WHERE error_number IS NOT NULL"
    error_count = bq_client.query(error_log_query).result().scalar_iterator().next()
    assert error_count == 0, "No error entries expected in job_error_log."

    k_core_log_query = """
        SELECT error_message FROM `project.dataset.job_error_log`
        WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
        AND error_message LIKE '%k_ausd_bp_ta_bpr_beschr called successfully with stichtag=01012023 and wiederanlauf_wert=12345%'
    """
    k_core_log_rows = list(bq_client.query(k_core_log_query).result())
    assert len(k_core_log_rows) == 1, "Expected one success log entry from k_ausd_bp_ta_bpr_beschr."
```

---

## Test Case 3: Error Path - Missing Stichtag (Parameter Validation)

*   **Purpose:** Verify the wrapper SP correctly handles the case where `p_stichtag` is explicitly an empty string or effectively `NULL` after `NULLIF`, triggering the parameter validation error (`ErrNr=193`). It should record an entry in `job_error_log` and `SIGNAL SQLSTATE`. The `job_control` table should *not* have a 'STARTED' entry as the error occurs before it.
*   **Setup:**
    *   Ensure `job_control` and `job_error_log` tables are empty.
*   **Action:**
    *   Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_beschr` with an empty string for `p_stichtag`.
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_beschr`('', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The `CALL` statement should raise a BigQuery error (due to `SIGNAL SQLSTATE '45000'`) with a message containing "Parameter error: Stichtag, ErrNr=193".
    *   No entries in `project.dataset.job_control` for `job_name = 'AUSD_BP_TA_BPR_BESCHR'`.
    *   One entry in `project.dataset.job_error_log` with `job_name = 'AUSD_BP_TA_BPR_BESCHR'`, `error_number = 193`, `error_argument = 'Stichtag'`, and `error_message` matching the `SIGNAL SQLSTATE` message.

```python
def test_error_path_missing_stichtag(bq_client):
    # Action: Expecting a BigQuery error due to SIGNAL SQLSTATE
    with pytest.raises(Exception) as excinfo:
        bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_beschr`('', 0);").result()
    
    # Assert that the error message contains the expected text
    assert "Parameter error: Stichtag, ErrNr=193" in str(excinfo.value), "Expected specific parameter error message."

    # Assertions for job_control table (should be empty for this job)
    job_control_query_count = "SELECT COUNT(*) FROM `project.dataset.job_control` WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'"
    job_control_count = bq_client.query(job_control_query_count).result().scalar_iterator().next()
    assert job_control_count == 0, "No job_control entry expected for parameter validation errors."

    # Assertions for job_error_log table
    error_log_query = """
        SELECT job_name, error_number, error_argument, error_message
        FROM `project.dataset.job_error_log`
        WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
        ORDER BY created_at DESC
        LIMIT 1
    """
    error_log_rows = list(bq_client.query(error_log_query).result())
    assert len(error_log_rows) == 1, "Expected one error entry in job_error_log."
    error_entry = error_log_rows[0]

    assert error_entry.job_name == 'AUSD_BP_TA_BPR_BESCHR', "Job name mismatch in error log."
    assert error_entry.error_number == 193, "Error number mismatch."
    assert error_entry.error_argument == 'Stichtag', "Error argument mismatch."
    assert "Parameter error: Stichtag, ErrNr=193" in error_entry.error_message, "Error message mismatch."
```

---

## Test Case 4: Error Path - Core Script Failure

*   **Purpose:** Verify the wrapper SP correctly handles errors originating from the invoked core processing script (`k_ausd_bp_ta_bpr_beschr`). It should record a 'STARTED' and then 'ERROR' status in `job_control` and an entry in `job_error_log`.
*   **Setup:**
    *   Ensure `job_control` and `job_error_log` tables are empty.
    *   Temporarily modify `project.dataset.k_ausd_bp_ta_bpr_beschr` to `SIGNAL SQLSTATE` an error immediately upon invocation.
*   **Action:**
    *   Temporarily modify `project.dataset.k_ausd_bp_ta_bpr_beschr` to:
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_beschr`(
          IN p_job_kennung STRING,
          IN p_stichtag STRING,
          IN p_dw_eintrags_nr INT64,
          IN p_wiederanlauf_wert INT64
        )
        BEGIN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from k_ausd_bp_ta_bpr_beschr';
        END;
        ```
    *   Call `project.dataset.ausd_bp_ta_bpr_beschr(NULL, NULL);`
    *   Revert `project.dataset.k_ausd_bp_ta_bpr_beschr` to its original (succeeding) mock state.
*   **Pass/Fail Criterion:**
    *   The `CALL` statement should raise a BigQuery error (due to `SIGNAL SQLSTATE '45000'`) with a message containing "Simulated error from k_ausd_bp_ta_bpr_beschr".
    *   One entry in `project.dataset.job_control` with `job_name = 'AUSD_BP_TA_BPR_BESCHR'` and `status = 'ERROR'`.
    *   `job_entry_nr` should be `1`.
    *   `created_at` and `finished_at` timestamps should be populated.
    *   One entry in `project.dataset.job_error_log` with `job_name = 'AUSD_BP_TA_BPR_BESCHR'`, `job_entry_nr` matching the `job_control` entry, and `error_message` containing `'Simulated error from k_ausd_bp_ta_bpr_beschr'`.

```python
def test_error_path_core_script_failure(bq_client):
    # Setup: Temporarily modify k_ausd_bp_ta_bpr_beschr to fail
    failing_k_sp_ddl = """
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_beschr`(
          IN p_job_kennung STRING,
          IN p_stichtag STRING,
          IN p_dw_eintrags_nr INT64,
          IN p_wiederanlauf_wert INT64
        )
        BEGIN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from k_ausd_bp_ta_bpr_beschr';
        END;
    """
    original_k_sp_ddl = """
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_beschr`(
          IN p_job_kennung STRING,
          IN p_stichtag STRING,
          IN p_dw_eintrags_nr INT64,
          IN p_wiederanlauf_wert INT64
        )
        BEGIN
          INSERT INTO `project.dataset.job_error_log` (job_name, job_entry_nr, error_message, created_at)
          VALUES (p_job_kennung, p_dw_eintrags_nr, CONCAT('k_ausd_bp_ta_bpr_beschr called successfully with stichtag=', p_stichtag, ' and wiederanlauf_wert=', CAST(p_wiederanlauf_wert AS STRING)), CURRENT_TIMESTAMP());
        END;
    """
    bq_client.query(failing_k_sp_ddl).result()

    try:
        # Action: Expecting a BigQuery error
        with pytest.raises(Exception) as excinfo:
            bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_beschr`(NULL, NULL);").result()
        
        # Assert that the error message contains the expected text
        assert "Simulated error from k_ausd_bp_ta_bpr_beschr" in str(excinfo.value), "Expected specific core script error message."

        # Assertions for job_control table
        job_control_query = """
            SELECT job_entry_nr, job_name, stichtag, status, created_at, finished_at
            FROM `project.dataset.job_control`
            WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
            ORDER BY created_at DESC
            LIMIT 1
        """
        job_control_rows = list(bq_client.query(job_control_query).result())
        assert len(job_control_rows) == 1, "Expected one entry in job_control table."
        job_entry = job_control_rows[0]

        assert job_entry.job_entry_nr == 1, "job_entry_nr should be 1."
        assert job_entry.job_name == 'AUSD_BP_TA_BPR_BESCHR', "Job name mismatch."
        assert job_entry.status == 'ERROR', "Job status should be 'ERROR'."
        assert job_entry.created_at is not None, "created_at should be populated."
        assert job_entry.finished_at is not None, "finished_at should be populated."
        assert job_entry.finished_at >= job_entry.created_at, "finished_at should be after created_at."

        # Assertions for job_error_log table
        error_log_query = """
            SELECT job_name, job_entry_nr, error_message
            FROM `project.dataset.job_error_log`
            WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
            ORDER BY created_at DESC
            LIMIT 1
        """
        error_log_rows = list(bq_client.query(error_log_query).result())
        assert len(error_log_rows) == 1, "Expected one error entry in job_error_log."
        error_entry = error_log_rows[0]

        assert error_entry.job_name == 'AUSD_BP_TA_BPR_BESCHR', "Job name mismatch in error log."
        assert error_entry.job_entry_nr == job_entry.job_entry_nr, "job_entry_nr mismatch in error log."
        assert "Simulated error from k_ausd_bp_ta_bpr_beschr" in error_entry.error_message, "Error message mismatch."

    finally:
        # Teardown: Revert k_ausd_bp_ta_bpr_beschr to its original state
        bq_client.query(original_k_sp_ddl).result()
```

---

## Test Case 5: Concurrency - `DW_EintragsNr` Generation

*   **Purpose:** Verify that `DW_EintragsNr` is generated uniquely and sequentially even if multiple calls to the wrapper SP happen in rapid succession. This tests the `MAX(job_entry_nr) + 1` logic within the BigQuery SP.
*   **Setup:**
    *   Ensure `job_control` and `job_error_log` tables are empty.
    *   `project.dataset.k_ausd_bp_ta_bpr_beschr` is configured to succeed.
*   **Action:**
    *   Execute the `project.dataset.ausd_bp_ta_bpr_beschr` SP three times in rapid succession with different `stichtag` values.
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_beschr`('01012023', 1);
    CALL `project.dataset.ausd_bp_ta_bpr_beschr`('02012023', 2);
    CALL `project.dataset.ausd_bp_ta_bpr_beschr`('03012023', 3);
    ```
*   **Pass/Fail Criterion:**
    *   Three distinct entries in `project.dataset.job_control` with `status = 'OK'`.
    *   The `job_entry_nr` values should be `1, 2, 3` (assuming an empty table initially) and unique.
    *   The `stichtag` values in `job_control` should match the input parameters.
    *   No entries in `project.dataset.job_error_log` with `error_number` populated.
    *   Three entries in `project.dataset.job_error_log` (from the mock `k_ausd_bp_ta_bpr_beschr` SP) indicating successful invocation, each with the correct `stichtag` and `wiederanlauf_wert`.

```python
def test_concurrency_job_entry_nr_generation(bq_client):
    # Action: Execute multiple calls in rapid succession
    bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_beschr`('01012023', 1);").result()
    bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_beschr`('02012023', 2);").result()
    bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_beschr`('03012023', 3);").result()

    # Assertions for job_control table
    job_control_query = """
        SELECT job_entry_nr, job_name, stichtag, status
        FROM `project.dataset.job_control`
        WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
        ORDER BY job_entry_nr ASC
    """
    job_control_rows = list(bq_client.query(job_control_query).result())
    assert len(job_control_rows) == 3, "Expected three entries in job_control table."

    # Check job_entry_nr uniqueness and sequence
    expected_job_entry_nrs = [1, 2, 3]
    actual_job_entry_nrs = [row.job_entry_nr for row in job_control_rows]
    assert actual_job_entry_nrs == expected_job_entry_nrs, "job_entry_nr sequence mismatch."

    # Check stichtag and status
    assert job_control_rows[0].stichtag == '01012023' and job_control_rows[0].status == 'OK'
    assert job_control_rows[1].stichtag == '02012023' and job_control_rows[1].status == 'OK'
    assert job_control_rows[2].stichtag == '03012023' and job_control_rows[2].status == 'OK'

    # Assertions for job_error_log (success logs from k_ausd_bp_ta_bpr_beschr)
    error_log_query = "SELECT COUNT(*) FROM `project.dataset.job_error_log` WHERE error_number IS NOT NULL"
    error_count = bq_client.query(error_log_query).result().scalar_iterator().next()
    assert error_count == 0, "No error entries expected in job_error_log."

    k_core_log_query = """
        SELECT error_message FROM `project.dataset.job_error_log`
        WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
        AND error_message LIKE '%k_ausd_bp_ta_bpr_beschr called successfully%'
        ORDER BY created_at ASC
    """
    k_core_log_rows = list(bq_client.query(k_core_log_query).result())
    assert len(k_core_log_rows) == 3, "Expected three success log entries from k_ausd_bp_ta_bpr_beschr."
    assert "stichtag=01012023 and wiederanlauf_wert=1" in k_core_log_rows[0].error_message
    assert "stichtag=02012023 and wiederanlauf_wert=2" in k_core_log_rows[1].error_message
    assert "stichtag=03012023 and wiederanlauf_wert=3" in k_core_log_rows[2].error_message
```

---

## Test Case 6: Orchestration Integration (Airflow DAG)

*   **Purpose:** Verify the Airflow DAG (`bert_ausd_bp_ta_bpr_beschr_dag`) correctly triggers the BigQuery Stored Procedure and passes parameters, including the default `ds_nodash` for `stichtag` and explicit parameters from DAG configuration. This covers the external system replacement (UC4 -> Airflow).
*   **Setup:**
    *   An Airflow environment (e.g., Cloud Composer) is running and configured with BigQuery connection.
    *   The `bert_ausd_bp_ta_bpr_beschr_dag.py` DAG is deployed and unpaused.
    *   Ensure `job_control` and `job_error_log` tables are empty before each run.
    *   `project.dataset.k_ausd_bp_ta_bpr_beschr` is configured to succeed.
*   **Action:**
    1.  Trigger the Airflow DAG `bert_ausd_bp_ta_bpr_beschr_dag` manually, without providing `stichtag` or `wiederanlaufwert` in the trigger config. Let the execution date be `2023-10-26`.
    2.  After the first run completes, trigger the DAG again, providing `stichtag='05052024'` and `wiederanlaufwert=999` in the DAG run configuration. Let the execution date be `2023-10-27`.
*   **Pass/Fail Criterion:**
    *   **For both runs:** The Airflow task `call_ausd_bp_ta_bpr_beschr_sp` completes successfully (marked green in Airflow UI).
    *   **First run (default params):**
        *   One entry in `project.dataset.job_control` with `job_name = 'AUSD_BP_TA_BPR_BESCHR'` and `status = 'OK'`.
        *   The `stichtag` in `job_control` matches `26102023` (Airflow's `ds_nodash` for `2023-10-26`).
        *   The `k_ausd_bp_ta_bpr_beschr` success log entry should show `stichtag=26102023` and `wiederanlauf_wert=0`.
    *   **Second run (explicit params):**
        *   A second entry in `project.dataset.job_control` with `job_name = 'AUSD_BP_TA_BPR_BESCHR'` and `status = 'OK'`.
        *   The `stichtag` in `job_control` matches `'05052024'`.
        *   The `k_ausd_bp_ta_bpr_beschr` success log entry should show `stichtag=05052024` and `wiederanlauf_wert=999`.
    *   No entries in `project.dataset.job_error_log` with `error_number` populated for either run.

```python
# This is a conceptual test, as actual Airflow DAG testing involves a running Airflow instance
# or specific Airflow testing utilities. The assertions below describe what to check in BigQuery
# after triggering the DAGs.

# --- First Airflow Run (Default Parameters) ---
# Action: Manually trigger DAG 'bert_ausd_bp_ta_bpr_beschr_dag' with execution_date=2023-10-26, no conf.

# Pass/Fail Criterion (after DAG completes successfully):
# 1. Check Airflow UI: Task 'call_ausd_bp_ta_bpr_beschr_sp' for DAG run 2023-10-26 is 'success'.
# 2. BigQuery Assertion (SQL):
#    SELECT job_entry_nr, job_name, stichtag, status
#    FROM `project.dataset.job_control`
#    WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
#    ORDER BY created_at DESC LIMIT 1;
#    -- Expected: job_entry_nr=1, job_name='AUSD_BP_TA_BPR_BESCHR', stichtag='26102023', status='OK'

#    SELECT error_message
#    FROM `project.dataset.job_error_log`
#    WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
#    AND error_message LIKE '%k_ausd_bp_ta_bpr_beschr called successfully with stichtag=26102023 and wiederanlauf_wert=0%';
#    -- Expected: 1 row matching the pattern.

#    SELECT COUNT(*) FROM `project.dataset.job_error_log` WHERE error_number IS NOT NULL;
#    -- Expected: 0

# --- Second Airflow Run (Explicit Parameters) ---
# Action: Manually trigger DAG 'bert_ausd_bp_ta_bpr_beschr_dag' with execution_date=2023-10-27,
#         and conf: {"stichtag": "05052024", "wiederanlaufwert": 999}

# Pass/Fail Criterion (after DAG completes successfully):
# 1. Check Airflow UI: Task 'call_ausd_bp_ta_bpr_beschr_sp' for DAG run 2023-10-27 is 'success'.
# 2. BigQuery Assertion (SQL):
#    SELECT job_entry_nr, job_name, stichtag, status
#    FROM `project.dataset.job_control`
#    WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
#    ORDER BY created_at DESC LIMIT 1;
#    -- Expected: job_entry_nr=2, job_name='AUSD_BP_TA_BPR_BESCHR', stichtag='05052024', status='OK'

#    SELECT error_message
#    FROM `project.dataset.job_error_log`
#    WHERE job_name = 'AUSD_BP_TA_BPR_BESCHR'
#    AND error_message LIKE '%k_ausd_bp_ta_bpr_beschr called successfully with stichtag=05052024 and wiederanlauf_wert=999%';
#    -- Expected: 1 row matching the pattern.

#    SELECT COUNT(*) FROM `project.dataset.job_error_log` WHERE error_number IS NOT NULL;
#    -- Expected: 0
```