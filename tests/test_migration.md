As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_v_ta_discount_rr.ksh` to BigQuery. The focus is on the wrapper script's orchestration, logging, parameter handling, and error management, with the core reconciliation logic (`k_ausd_v_ta_discount_rr.ksh`) being a separate migration concern (represented by a stub in this design).

The following test cases are designed to validate the migrated BigQuery stored procedure (`sp_vertragsdatenabgleich_ta_discount_rr`) against the specified requirements.

---

## Test Environment Setup

Before running the tests, ensure the following BigQuery objects are deployed:

1.  **Datasets:** `my_project.my_dataset` (replace `my_project` and `my_dataset` with your actual project and dataset IDs).
2.  **Tables:**
    *   `my_project.my_dataset.job_log`
    *   `my_project.my_dataset.job_control`
3.  **Stored Procedures:**
    *   `my_project.my_dataset.sp_k_ausd_v_ta_discount_rr` (the stub provided in the design)
    *   `my_project.my_dataset.sp_vertragsdatenabgleich_ta_discount_rr`

**BigQuery DDL for Setup:**

```sql
-- Create dataset if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS `my_project.my_dataset`;

-- Logging table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
  job_kennung STRING,
  job_entry_nr INT64,
  log_level STRING,
  message STRING,
  log_file_name STRING,
  created_at TIMESTAMP
);

-- Job control table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_control` (
  job_kennung STRING,
  job_entry_nr INT64,
  stichtag STRING,
  stichtag_format STRING,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- Core script stub (sp_k_ausd_v_ta_discount_rr)
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_k_ausd_v_ta_discount_rr`(
  IN p_job_kennung STRING,
  IN p_dw_eintragsnr INT64
)
BEGIN
  INSERT INTO `my_project.my_dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
  VALUES (p_job_kennung, p_dw_eintragsnr, 'I', 'Core reconciliation script (sp_k_ausd_v_ta_discount_rr) stub called. Logic needs to be implemented.', NULL, CURRENT_TIMESTAMP());
  SELECT 'Core script stub executed successfully for Job:', p_job_kennung, 'Entry Nr:', p_dw_eintragsnr;
END;

-- Wrapper script (sp_vertragsdatenabgleich_ta_discount_rr)
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_vertragsdatenabgleich_ta_discount_rr`(
  IN p_help BOOL,
  IN p_s STRING,
  IN p_l STRING
)
BEGIN
  DECLARE v_prog_name STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_DISCOUNT_RR';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_dw_eintragsnr INT64 DEFAULT 0;
  DECLARE v_logdatei STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  IF p_help THEN
    SELECT v_prog_name AS Programm, 'V1.0.0' AS Version, 'Aufruf: Parameter -h zeigt diese Seite an' AS Beschreibung;
    RETURN;
  END IF;

  IF p_s IS NULL OR p_l IS NULL THEN
    SET v_err_nr = 193;
    SET v_err_arg = IF(p_s IS NULL, 's', 'l');
  END IF;

  IF v_err_nr != 0 THEN
    INSERT INTO `my_project.my_dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
    VALUES (v_job_kennung, v_dw_eintragsnr, 'E', CONCAT('Parameterfehler: ', v_err_arg), v_logdatei, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Parameter error: ', v_err_arg);
  END IF;

  SET v_dw_eintragsnr = (SELECT IFNULL(MAX(job_entry_nr), 0) + 1 FROM `my_project.my_dataset.job_log` WHERE job_kennung = v_job_kennung);
  SET v_logdatei = CONCAT(v_job_kennung, '_', CAST(v_dw_eintragsnr AS STRING), '.log');
  INSERT INTO `my_project.my_dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
  VALUES (v_job_kennung, v_dw_eintragsnr, 'I', CONCAT('Job start: ', v_prog_name), v_logdatei, CURRENT_TIMESTAMP());
  INSERT INTO `my_project.my_dataset.job_control` (job_kennung, job_entry_nr, stichtag, stichtag_format, status, created_at, updated_at)
  VALUES (v_job_kennung, v_dw_eintragsnr, v_sysdate, 'DDMMYYYY', 'INIT', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

  BEGIN
    SELECT 'Job' AS section, v_dw_eintragsnr AS job_nr, v_job_kennung AS job_kennung, v_logdatei AS logdatei;

    CALL `my_project.my_dataset.sp_k_ausd_v_ta_discount_rr`(v_job_kennung, v_dw_eintragsnr);

    INSERT INTO `my_project.my_dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
    VALUES (v_job_kennung, v_dw_eintragsnr, 'I', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', v_logdatei, CURRENT_TIMESTAMP());
    UPDATE `my_project.my_dataset.job_control` SET status = 'OK', updated_at = CURRENT_TIMESTAMP() WHERE job_kennung = v_job_kennung AND job_entry_nr = v_dw_eintragsnr;
    SET v_status = 'OK';

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `my_project.my_dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
    VALUES (v_job_kennung, v_dw_eintragsnr, 'E', CONCAT('AppError: Abbruch - ', @@error.message), v_logdatei, CURRENT_TIMESTAMP());
    UPDATE `my_project.my_dataset.job_control` SET status = 'ERROR', updated_at = CURRENT_TIMESTAMP() WHERE job_kennung = v_job_kennung AND job_entry_nr = v_dw_eintragsnr;
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('AppError: Abbruch - ', @@error.message);
  END;
END;
```

---

## Python Test Helpers (Pytest)

```python
import pytest
from google.cloud import bigquery
import time
import datetime

# --- Configuration ---
PROJECT_ID = "my_project"  # Replace with your GCP Project ID
DATASET_ID = "my_dataset"  # Replace with your BigQuery Dataset ID
JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
JOB_CONTROL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_control"
WRAPPER_SP = f"{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich_ta_discount_rr"
CORE_SP_STUB = f"{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_discount_rr"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def setup_and_teardown_tables(bq_client):
    """Clears logging tables before and after each test."""
    # Setup: Clear tables
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{JOB_CONTROL_TABLE}`").result()
    yield
    # Teardown: Clear tables again
    bq_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{JOB_CONTROL_TABLE}`").result()

def execute_sp(bq_client, sp_name, params=None):
    """Helper to execute a BigQuery stored procedure."""
    param_str = ""
    if params:
        param_parts = []
        for k, v in params.items():
            if isinstance(v, str):
                param_parts.append(f"{k} => '{v}'")
            elif isinstance(v, bool):
                param_parts.append(f"{k} => {str(v).upper()}")
            elif isinstance(v, int):
                param_parts.append(f"{k} => {v}")
            else:
                param_parts.append(f"{k} => {v}") # Fallback for other types
        param_str = ", ".join(param_parts)
    query = f"CALL `{sp_name}`({param_str})"
    print(f"Executing: {query}")
    try:
        job = bq_client.query(query)
        # For procedures, job.result() will raise an exception if the SP signals an error.
        # It might also return results if the SP contains SELECT statements that are not part of an INSERT.
        rows = list(job.result()) # Consume results if any
        return True, None, rows
    except Exception as e:
        print(f"SP execution failed: {e}")
        return False, str(e), []

def get_table_data(bq_client, table_id):
    """Helper to fetch all data from a table."""
    query = f"SELECT * FROM `{table_id}` ORDER BY created_at ASC"
    rows = bq_client.query(query).result()
    return [dict(row) for row in rows]

def get_table_row_count(bq_client, table_id):
    """Helper to get row count of a table."""
    query = f"SELECT COUNT(1) FROM `{table_id}`"
    row = bq_client.query(query).result().to_dataframe().iloc[0, 0]
    return row

# Helper to temporarily replace a stored procedure for error simulation
def replace_sp_with_error_stub(bq_client, sp_name):
    error_sp_ddl = f"""
    CREATE OR REPLACE PROCEDURE `{sp_name}`(
      IN p_job_kennung STRING,
      IN p_dw_eintragsnr INT64
    )
    BEGIN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from core script.';
    END;
    """
    bq_client.query(error_sp_ddl).result()

def restore_original_sp_stub(bq_client, sp_name):
    original_sp_ddl = f"""
    CREATE OR REPLACE PROCEDURE `{sp_name}`(
      IN p_job_kennung STRING,
      IN p_dw_eintragsnr INT64
    )
    BEGIN
      INSERT INTO `{JOB_LOG_TABLE}` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
      VALUES (p_job_kennung, p_dw_eintragsnr, 'I', 'Core reconciliation script (sp_k_ausd_v_ta_discount_rr) stub called. Logic needs to be implemented.', NULL, CURRENT_TIMESTAMP());
      SELECT 'Core script stub executed successfully for Job:', p_job_kennung, 'Entry Nr:', p_dw_eintragsnr;
    END;
    """
    bq_client.query(original_sp_ddl).result()
```

---

## Test Cases

### Test Case 1: Help Message Display

*   **Purpose:** Verify that calling the stored procedure with `p_help = TRUE` (equivalent to `-h` in the legacy script) correctly displays the usage information and exits without performing any other job logic or logging.
*   **Category:** Output Parity, Transformation Correctness (parameter handling).
*   **Setup:** Ensure `job_log` and `job_control` tables are empty.
*   **Action:** Execute `CALL my_project.my_dataset.sp_vertragsdatenabgleich_ta_discount_rr(p_help => TRUE, p_s => NULL, p_l => NULL);`
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully (does not raise an error).
    *   The output contains the expected program name, version, and usage description.
    *   `my_project.my_dataset.job_log` table remains empty.
    *   `my_project.my_dataset.job_control` table remains empty.

```python
def test_help_message_display(bq_client):
    success, error_message, results = execute_sp(bq_client, WRAPPER_SP, {"p_help": True, "p_s": None, "p_l": None})

    assert success, f"SP execution failed: {error_message}"
    assert len(results) == 1
    output_row = results[0]
    assert output_row["Programm"] == "Vertragsdatenabgleich"
    assert output_row["Version"] == "V1.0.0"
    assert "Aufruf: Parameter -h zeigt diese Seite an" in output_row["Beschreibung"]

    # Verify no logging occurred
    assert get_table_row_count(bq_client, JOB_LOG_TABLE) == 0
    assert get_table_row_count(bq_client, JOB_CONTROL_TABLE) == 0
```

### Test Case 2: Successful Execution Path

*   **Purpose:** Verify that the stored procedure executes successfully when all required parameters are provided, correctly logs job start/end, updates job status to 'OK', and invokes the core script stub.
*   **Category:** Output Parity, Transformation Correctness, External-system replacements, Data Quality/Row Count.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty.
*   **Action:** Execute `CALL my_project.my_dataset.sp_vertragsdatenabgleich_ta_discount_rr(p_help => FALSE, p_s => 'some_s_value', p_l => 'some_l_value');`
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully.
    *   `my_project.my_dataset.job_log` contains 3 entries: 'Job start', 'Core script stub called', 'Abarbeitung wurde ohne erkennbare Fehler beendet'.
    *   `my_project.my_dataset.job_control` contains 1 entry with `status = 'OK'`.
    *   `job_entry_nr` is 1 for this run.
    *   `log_file_name` in `job_log` matches the expected pattern (`BERT_V_TA_DISCOUNT_RR_1.log`).
    *   `stichtag` in `job_control` matches `CURRENT_DATE()` in `DDMMYYYY` format.

```python
def test_successful_execution_path(bq_client):
    current_date_ddmmyyyy = datetime.datetime.now().strftime('%d%m%Y')
    
    success, error_message, results = execute_sp(bq_client, WRAPPER_SP, {"p_help": False, "p_s": "test_s", "p_l": "test_l"})

    assert success, f"SP execution failed: {error_message}"

    # Verify job_log entries
    job_log_data = get_table_data(bq_client, JOB_LOG_TABLE)
    assert len(job_log_data) == 3
    assert job_log_data[0]["log_level"] == "I"
    assert "Job start: Vertragsdatenabgleich" in job_log_data[0]["message"]
    assert job_log_data[0]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
    assert job_log_data[0]["job_entry_nr"] == 1
    assert job_log_data[0]["log_file_name"] == "BERT_V_TA_DISCOUNT_RR_1.log"

    assert job_log_data[1]["log_level"] == "I"
    assert "Core reconciliation script (sp_k_ausd_v_ta_discount_rr) stub called" in job_log_data[1]["message"]
    assert job_log_data[1]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
    assert job_log_data[1]["job_entry_nr"] == 1

    assert job_log_data[2]["log_level"] == "I"
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in job_log_data[2]["message"]
    assert job_log_data[2]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
    assert job_log_data[2]["job_entry_nr"] == 1

    # Verify job_control entry
    job_control_data = get_table_data(bq_client, JOB_CONTROL_TABLE)
    assert len(job_control_data) == 1
    assert job_control_data[0]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
    assert job_control_data[0]["job_entry_nr"] == 1
    assert job_control_data[0]["stichtag"] == current_date_ddmmyyyy
    assert job_control_data[0]["stichtag_format"] == "DDMMYYYY"
    assert job_control_data[0]["status"] == "OK"
    assert job_control_data[0]["created_at"] is not None
    assert job_control_data[0]["updated_at"] is not None
    assert job_control_data[0]["created_at"] <= job_control_data[0]["updated_at"]
```

### Test Case 3: Missing Required Parameter (`p_s` is NULL)

*   **Purpose:** Verify that the stored procedure correctly identifies a missing required parameter (`p_s`), logs an error, and signals an error state, preventing further execution.
*   **Category:** Transformation Correctness (parameter validation, error handling), External-system replacements, Data Quality/Row Count.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty.
*   **Action:** Execute `CALL my_project.my_dataset.sp_vertragsdatenabgleich_ta_discount_rr(p_help => FALSE, p_s => NULL, p_l => 'some_l_value');`
*   **Pass/Fail Criterion:**
    *   The procedure execution fails with an error message indicating a parameter error.
    *   `my_project.my_dataset.job_log` contains 1 entry with `log_level = 'E'` and message `Parameterfehler: s`.
    *   `my_project.my_dataset.job_control` table remains empty (as the error occurs before control table insertion).

```python
def test_missing_parameter_s(bq_client):
    success, error_message, _ = execute_sp(bq_client, WRAPPER_SP, {"p_help": False, "p_s": None, "p_l": "test_l"})

    assert not success
    assert "Parameter error: s" in error_message

    # Verify job_log entries
    job_log_data = get_table_data(bq_client, JOB_LOG_TABLE)
    assert len(job_log_data) == 1
    assert job_log_data[0]["log_level"] == "E"
    assert "Parameterfehler: s" in job_log_data[0]["message"]
    assert job_log_data[0]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
    assert job_log_data[0]["job_entry_nr"] == 0 # DW_EintragsNr is 0 before it's determined

    # Verify job_control entry (should be empty as error occurs before insertion)
    assert get_table_row_count(bq_client, JOB_CONTROL_TABLE) == 0
```

### Test Case 4: Missing Required Parameter (`p_l` is NULL)

*   **Purpose:** Verify that the stored procedure correctly identifies a missing required parameter (`p_l`), logs an error, and signals an error state, preventing further execution.
*   **Category:** Transformation Correctness (parameter validation, error handling), External-system replacements, Data Quality/Row Count.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty.
*   **Action:** Execute `CALL my_project.my_dataset.sp_vertragsdatenabgleich_ta_discount_rr(p_help => FALSE, p_s => 'some_s_value', p_l => NULL);`
*   **Pass/Fail Criterion:**
    *   The procedure execution fails with an error message indicating a parameter error.
    *   `my_project.my_dataset.job_log` contains 1 entry with `log_level = 'E'` and message `Parameterfehler: l`.
    *   `my_project.my_dataset.job_control` table remains empty.

```python
def test_missing_parameter_l(bq_client):
    success, error_message, _ = execute_sp(bq_client, WRAPPER_SP, {"p_help": False, "p_s": "test_s", "p_l": None})

    assert not success
    assert "Parameter error: l" in error_message

    # Verify job_log entries
    job_log_data = get_table_data(bq_client, JOB_LOG_TABLE)
    assert len(job_log_data) == 1
    assert job_log_data[0]["log_level"] == "E"
    assert "Parameterfehler: l" in job_log_data[0]["message"]
    assert job_log_data[0]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
    assert job_log_data[0]["job_entry_nr"] == 0

    # Verify job_control entry (should be empty)
    assert get_table_row_count(bq_client, JOB_CONTROL_TABLE) == 0
```

### Test Case 5: Core Script Failure Handling

*   **Purpose:** Verify that the wrapper stored procedure correctly handles an error originating from the invoked core script (`sp_k_ausd_v_ta_discount_rr`), logs the error, and updates the job status to 'ERROR' in `job_control`. This tests the `BEGIN...EXCEPTION WHEN ERROR THEN...END` block.
*   **Category:** Transformation Correctness (error handling, `trap` equivalent), External-system replacements, Data Quality/Row Count.
*   **Setup:**
    1.  Ensure `job_log` and `job_control` tables are empty.
    2.  Temporarily replace `my_project.my_dataset.sp_k_ausd_v_ta_discount_rr` with a version that always raises an error.
*   **Action:** Execute `CALL my_project.my_dataset.sp_vertragsdatenabgleich_ta_discount_rr(p_help => FALSE, p_s => 'test_s', p_l => 'test_l');`
*   **Pass/Fail Criterion:**
    *   The procedure execution fails with an error message indicating an "AppError: Abbruch".
    *   `my_project.my_dataset.job_log` contains entries for 'Job start', 'Core script stub called', and 'AppError: Abbruch'.
    *   `my_project.my_dataset.job_control` contains 1 entry with `status = 'ERROR'`.
    *   `job_entry_nr` is 1 for this run.
*   **Teardown:** Restore the original `my_project.my_dataset.sp_k_ausd_v_ta_discount_rr` stub.

```python
def test_core_script_failure_handling(bq_client):
    # Setup: Replace core SP with an error-throwing stub
    replace_sp_with_error_stub(bq_client, CORE_SP_STUB)

    try:
        success, error_message, _ = execute_sp(bq_client, WRAPPER_SP, {"p_help": False, "p_s": "test_s", "p_l": "test_l"})

        assert not success
        assert "AppError: Abbruch - Simulated error from core script." in error_message

        # Verify job_log entries
        job_log_data = get_table_data(bq_client, JOB_LOG_TABLE)
        assert len(job_log_data) == 3 # Job start, Core stub called, AppError
        assert job_log_data[0]["log_level"] == "I"
        assert "Job start: Vertragsdatenabgleich" in job_log_data[0]["message"]
        assert job_log_data[0]["job_entry_nr"] == 1

        assert job_log_data[1]["log_level"] == "I"
        assert "Core reconciliation script (sp_k_ausd_v_ta_discount_rr) stub called" in job_log_data[1]["message"]
        assert job_log_data[1]["job_entry_nr"] == 1

        assert job_log_data[2]["log_level"] == "E"
        assert "AppError: Abbruch - Simulated error from core script." in job_log_data[2]["message"]
        assert job_log_data[2]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
        assert job_log_data[2]["job_entry_nr"] == 1

        # Verify job_control entry
        job_control_data = get_table_data(bq_client, JOB_CONTROL_TABLE)
        assert len(job_control_data) == 1
        assert job_control_data[0]["job_kennung"] == "BERT_V_TA_DISCOUNT_RR"
        assert job_control_data[0]["job_entry_nr"] == 1
        assert job_control_data[0]["status"] == "ERROR"

    finally:
        # Teardown: Restore original core SP stub
        restore_original_sp_stub(bq_client, CORE_SP_STUB)
```

### Test Case 6: `job_entry_nr` Generation and Uniqueness

*   **Purpose:** Verify that `job_entry_nr` is correctly generated as a sequential, unique number for the given `job_kennung` across multiple successful runs.
*   **Category:** Transformation Correctness, Data Quality.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty.
*   **Action:**
    1.  Execute the stored procedure successfully.
    2.  Execute the stored procedure successfully again.
*   **Pass/Fail Criterion:**
    *   The first run's `job_entry_nr` in `job_log` and `job_control` is 1.
    *   The second run's `job_entry_nr` in `job_log` and `job_control` is 2.
    *   All entries for each run correctly reference their respective `job_entry_nr`.

```python
def test_job_entry_nr_generation(bq_client):
    # First successful run
    success1, error_message1, _ = execute_sp(bq_client, WRAPPER_SP, {"p_help": False, "p_s": "test_s1", "p_l": "test_l1"})
    assert success1, f"First SP execution failed: {error_message1}"

    # Second successful run
    success2, error_message2, _ = execute_sp(bq_client, WRAPPER_SP, {"p_help": False, "p_s": "test_s2", "p_l": "test_l2"})
    assert success2, f"Second SP execution failed: {error_message2}"

    # Verify job_log entries for both runs
    job_log_data = get_table_data(bq_client, JOB_LOG_TABLE)
    assert len(job_log_data) == 6 # 3 entries per run

    # Check first run entries
    run1_logs = [log for log in job_log_data if log["job_entry_nr"] == 1]
    assert len(run1_logs) == 3
    assert all(log["job_kennung"] == "BERT_V_TA_DISCOUNT_RR" for log in run1_logs)
    assert "Job start" in run1_logs[0]["message"]
    assert "Core reconciliation script" in run1_logs[1]["message"]
    assert "ohne erkennbare Fehler beendet" in run1_logs[2]["message"]

    # Check second run entries
    run2_logs = [log for log in job_log_data if log["job_entry_nr"] == 2]
    assert len(run2_logs) == 3
    assert all(log["job_kennung"] == "BERT_V_TA_DISCOUNT_RR" for log in run2_logs)
    assert "Job start" in run2_logs[0]["message"]
    assert "Core reconciliation script" in run2_logs[1]["message"]
    assert "ohne erkennbare Fehler beendet" in run2_logs[2]["message"]

    # Verify job_control entries for both runs
    job_control_data = get_table_data(bq_client, JOB_CONTROL_TABLE)
    assert len(job_control_data) == 2
    assert job_control_data[0]["job_entry_nr"] == 1
    assert job_control_data[0]["status"] == "OK"
    assert job_control_data[1]["job_entry_nr"] == 2
    assert job_control_data[1]["status"] == "OK"
```

### Test Case 7: `LogDatei` Name Generation

*   **Purpose:** Verify that the `v_logdatei` variable is correctly constructed using the `JobKennung` and `DW_EintragsNr` as per the legacy script's logic.
*   **Category:** Transformation Correctness.
*   **Setup:** Ensure `job_log` and `job_control` tables are empty.
*   **Action:** Execute the stored procedure successfully.
*   **Pass/Fail Criterion:**
    *   The `log_file_name` field in the `job_log` table for the 'Job start' entry matches the pattern `BERT_V_TA_DISCOUNT_RR_<job_entry_nr>.log`.

```python
def test_logdatei_name_generation(bq_client):
    success, error_message, _ = execute_sp(bq_client, WRAPPER_SP, {"p_help": False, "p_s": "test_s", "p_l": "test_l"})
    assert success, f"SP execution failed: {error_message}"

    job_log_data = get_table_data(bq_client, JOB_LOG_TABLE)
    assert len(job_log_data) >= 1 # At least the "Job start" entry

    # Find the "Job start" entry
    job_start_log = next((log for log in job_log_data if "Job start" in log["message"]), None)
    assert job_start_log is not None, "Could not find 'Job start' log entry."

    expected_log_file_name = f"BERT_V_TA_DISCOUNT_RR_{job_start_log['job_entry_nr']}.log"
    assert job_start_log["log_file_name"] == expected_log_file_name
```

### Test Case 8: Schema Validation of Logging Tables

*   **Purpose:** Ensure the DDL for `job_log` and `job_control` tables matches the expected structure and data types. This is a static check of the DDL.
*   **Category:** Schema Assertions.
*   **Setup:** Ensure the tables `my_project.my_dataset.job_log` and `my_project.my_dataset.job_control` exist.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for table and column details.
*   **Pass/Fail Criterion:**
    *   `job_log` table has columns: `job_kennung` (STRING), `job_entry_nr` (INT64), `log_level` (STRING), `message` (STRING), `log_file_name` (STRING), `created_at` (TIMESTAMP).
    *   `job_control` table has columns: `job_kennung` (STRING), `job_entry_nr` (INT64), `stichtag` (STRING), `stichtag_format` (STRING), `status` (STRING), `created_at` (TIMESTAMP), `updated_at` (TIMESTAMP).

```python
def test_logging_table_schemas(bq_client):
    # Test job_log schema
    job_log_schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log'
    ORDER BY ordinal_position
    """
    job_log_columns = {row["column_name"]: row["data_type"] for row in bq_client.query(job_log_schema_query).result()}

    expected_job_log_schema = {
        "job_kennung": "STRING",
        "job_entry_nr": "INT64",
        "log_level": "STRING",
        "message": "STRING",
        "log_file_name": "STRING",
        "created_at": "TIMESTAMP"
    }
    assert job_log_columns == expected_job_log_schema, f"job_log schema mismatch: {job_log_columns}"

    # Test job_control schema
    job_control_schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_control'
    ORDER BY ordinal_position
    """
    job_control_columns = {row["column_name"]: row["data_type"] for row in bq_client.query(job_control_schema_query).result()}

    expected_job_control_schema = {
        "job_kennung": "STRING",
        "job_entry_nr": "INT64",
        "stichtag": "STRING",
        "stichtag_format": "STRING",
        "status": "STRING",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP"
    }
    assert job_control_columns == expected_job_control_schema, f"job_control schema mismatch: {job_control_columns}"
```