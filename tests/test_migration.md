The following migration validation tests are designed to ensure the BigQuery implementation of `r_ausd_bp_ta_bpr_opt_text.ksh` is behaviourally equivalent to its legacy KornShell counterpart. The tests cover parameter handling, logging, core logic invocation, data transformations, and error handling.

The tests are structured using `pytest` and include BigQuery SQL for setup, actions, and assertions.

---

## Pytest Setup and Fixtures

This section outlines the `pytest` setup, including fixtures for BigQuery client interaction and environment setup/teardown. These fixtures ensure a clean and consistent testing environment for each test case.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz # For timezone-aware timestamps

# --- Configuration ---
# Replace with your actual GCP project and dataset IDs
PROJECT_ID = 'your-gcp-project-id'
DATASET_ID = 'your_dataset_id'

# --- BigQuery Client Fixture ---
@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=PROJECT_ID)

# --- BigQuery Environment Setup/Teardown Fixture ---
@pytest.fixture(scope="module", autouse=True)
def setup_bigquery_environment(bq_client):
    """
    Sets up necessary BigQuery tables and procedures before tests run,
    and cleans them up afterwards.
    """
    print(f"\nSetting up BigQuery environment in {PROJECT_ID}.{DATASET_ID}...")

    # 1. Create DDL for job_log_audit
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.job_log_audit` (
            entry_nr INT64,
            job_name STRING,
            script_name STRING,
            log_name STRING,
            status STRING,
            stichtag STRING,
            restart_value INT64,
            message STRING,
            created_at TIMESTAMP
        );
    """).result()
    print("Created/Ensured job_log_audit table.")

    # 2. Create DDL for contract_cache_source
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.contract_cache_source` (
            DWH_VERTRAG_ID INT64,
            col1 STRING,
            col2 STRING,
            gültig_von DATE,
            gültig_bis DATE,
            ladedatum DATE
        );
    """).result()
    print("Created/Ensured contract_cache_source table.")

    # 3. Create DDL for fos_table
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.fos_table` (
            DWH_VERTRAG_ID INT64,
            col1 STRING,
            col2 STRING
        );
    """).result()
    print("Created/Ensured fos_table.")

    # 4. Deploy k_ausd_bp_ta_bpr_opt_text (core logic) procedure
    # Note: This version includes a p_simulate_error parameter for testing error paths.
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_bp_ta_bpr_opt_text`(
          IN p_jobkennung STRING,
          IN p_stichtag STRING,
          IN p_eintragsnr INT64,
          IN p_wiederanlaufWert INT64,
          IN p_simulate_error BOOL DEFAULT FALSE
        )
        BEGIN
          IF p_simulate_error THEN
            RAISE USING MESSAGE = 'Simulated error in k_ausd_bp_ta_bpr_opt_text core logic.';
          END IF;

          IF p_wiederanlaufWert > 0 THEN
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.fos_table`
            WHERE DWH_VERTRAG_ID >= p_wiederanlaufWert;
          END IF;

          INSERT INTO `{PROJECT_ID}.{DATASET_ID}.fos_table` (DWH_VERTRAG_ID, col1, col2)
          SELECT
            DWH_VERTRAG_ID, col1, col2
          FROM `{PROJECT_ID}.{DATASET_ID}.contract_cache_source`
          WHERE
            gültig_von <= PARSE_DATE('%d%m%Y', p_stichtag)
            AND PARSE_DATE('%d%m%Y', p_stichtag) < gültig_bis
            AND ladedatum < PARSE_DATE('%d%m%Y', p_stichtag)
            AND (p_wiederanlaufWert = 0 OR DWH_VERTRAG_ID > p_wiederanlaufWert);

          INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log_audit`
            (entry_nr, job_name, script_name, status, message, created_at)
          VALUES
            (p_eintragsnr, p_jobkennung, 'k_ausd_bp_ta_bpr_opt_text', 'INFO', 'Core logic executed successfully', CURRENT_TIMESTAMP());
        END;
    """).result()
    print("Deployed k_ausd_bp_ta_bpr_opt_text procedure.")

    # 5. Deploy ausd_bp_ta_bpr_opt_text_wrapper procedure
    # Note: This version includes a p_simulate_error parameter to pass to the core logic.
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`(
          IN p_stichtag STRING,
          IN p_wiederanlaufWert INT64,
          IN p_simulate_error BOOL DEFAULT FALSE
        )
        BEGIN
          DECLARE v_sysdate STRING;
          DECLARE v_stichtag STRING;
          DECLARE v_wiederanlaufWert INT64;
          DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_bpr_opt_text';
          DECLARE v_eintragsnr INT64;
          DECLARE v_logdatei STRING;
          DECLARE v_errnr INT64 DEFAULT 0;
          DECLARE v_errarg STRING DEFAULT '';

          BEGIN
            SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);
            SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
            SET v_stichtag = IFNULL(p_stichtag, v_sysdate);

            ASSERT v_stichtag IS NOT NULL
              AS 'Stichtag must be provided or derivable';

            -- Initial log entry for job start
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log_audit`
              (job_name, status, stichtag, restart_value, created_at)
            VALUES
              (v_jobkennung, 'STARTED', v_stichtag, v_wiederanlaufWert, CURRENT_TIMESTAMP());

            -- Determine next entry number for logging
            SET v_eintragsnr = (
              SELECT IFNULL(MAX(entry_nr), 0) + 1
              FROM `{PROJECT_ID}.{DATASET_ID}.job_log_audit`
              WHERE job_name = v_jobkennung
            );

            SET v_logdatei = CONCAT('log_', v_jobkennung, '_', CAST(v_eintragsnr AS STRING));

            -- Log entry for job running with details
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log_audit`
              (entry_nr, job_name, script_name, log_name, stichtag, status, created_at)
            VALUES
              (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_bpr_opt_text_wrapper', v_logdatei, v_stichtag, 'RUNNING', CURRENT_TIMESTAMP());

            -- Call core business logic procedure
            CALL `{PROJECT_ID}.{DATASET_ID}.k_ausd_bp_ta_bpr_opt_text`(
              v_jobkennung,
              v_stichtag,
              v_eintragsnr,
              v_wiederanlaufWert,
              p_simulate_error -- Pass error simulation flag
            );

            -- Log success
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log_audit`
              (entry_nr, job_name, status, message, created_at)
            VALUES
              (v_eintragsnr, v_jobkennung, 'OK', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

          EXCEPTION WHEN ERROR THEN
            -- Log error
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log_audit`
              (entry_nr, job_name, status, message, created_at)
            VALUES
              (v_eintragsnr, v_jobkennung, 'ERROR', @@error.message, CURRENT_TIMESTAMP());
            RAISE USING MESSAGE = 'AppError: Abbruch'; -- Re-raise for external orchestration if needed
          END;
        END;
    """).result()
    print("Deployed ausd_bp_ta_bpr_opt_text_wrapper procedure.")

    yield # Run tests

    # Teardown: Clean up tables and procedures
    print(f"\nCleaning up BigQuery environment in {PROJECT_ID}.{DATASET_ID}...")
    bq_client.query(f"DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.job_log_audit`;").result()
    bq_client.query(f"DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.contract_cache_source`;").result()
    bq_client.query(f"DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.fos_table`;").result()
    bq_client.query(f"DROP PROCEDURE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.k_ausd_bp_ta_bpr_opt_text`;").result()
    bq_client.query(f"DROP PROCEDURE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`;").result()
    print("BigQuery environment cleaned up.")

@pytest.fixture(autouse=True)
def cleanup_tables_before_each_test(bq_client):
    """Cleans up data in tables before each test to ensure isolation."""
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log_audit`;").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.contract_cache_source`;").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.fos_table`;").result()
    yield
```

---

## Test Cases

### Test Case 1: Successful Execution - No Parameters (Default Stichtag, Default Wiederanlaufwert)

**Purpose:**
Verify that the wrapper procedure correctly handles default values for `p_stichtag` (current system date) and `p_wiederanlaufWert` (0) when no parameters are provided. It also checks for successful end-to-end execution and proper logging.

**Setup:**
1.  Ensure `job_log_audit`, `contract_cache_source`, and `fos_table` are empty.
2.  Populate `contract_cache_source` with sample data that should be processed by the core logic when `p_stichtag` is `CURRENT_DATE()` and `p_wiederanlaufWert` is `0`.

```sql
-- Populate contract_cache_source for this test
INSERT INTO `project.dataset.contract_cache_source` (DWH_VERTRAG_ID, col1, col2, gültig_von, gültig_bis, ladedatum) VALUES
(1, 'A', 'X', CURRENT_DATE() - INTERVAL 10 DAY, CURRENT_DATE() + INTERVAL 10 DAY, CURRENT_DATE() - INTERVAL 5 DAY),
(2, 'B', 'Y', CURRENT_DATE() - INTERVAL 5 DAY, CURRENT_DATE() + INTERVAL 5 DAY, CURRENT_DATE() - INTERVAL 2 DAY),
(3, 'C', 'Z', CURRENT_DATE() + INTERVAL 1 DAY, CURRENT_DATE() + INTERVAL 10 DAY, CURRENT_DATE() - INTERVAL 1 DAY); -- Should NOT be included (gültig_von > stichtag)
```

**Action:**
Call the wrapper procedure without any explicit parameters.

```python
def test_successful_execution_no_parameters(bq_client):
    # Action: Call the wrapper procedure with NULL for both parameters
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`(NULL, NULL);"
    bq_client.query(query).result()

    # Get current date in DDMMYYYY format for assertions
    current_date_ddmmyyyy = datetime.now(pytz.timezone('UTC')).strftime('%d%m%Y')

    # Assertions will follow in the pass/fail criterion
```

**Pass/Fail Criterion:**
1.  **Output Parity (Logging):** The `job_log_audit` table must contain exactly 4 entries for `job_name = 'ausd_bp_ta_bpr_opt_text'` with statuses 'STARTED', 'RUNNING', 'INFO', and 'OK' in chronological order.
2.  **Transformation Correctness (Parameter Defaulting):**
    *   The `stichtag` in all log entries must match `CURRENT_DATE()` in `DDMMYYYY` format.
    *   The `restart_value` in all log entries must be `0`.
3.  **Data Quality (Row Count):** The `fos_table` must contain 2 rows (DWH_VERTRAG_ID 1 and 2) as per the setup data and filtering logic.
4.  **Data Quality (Content):** The inserted rows in `fos_table` must match the expected data from `contract_cache_source`.

```sql
-- SQL Assertions for Pass/Fail Criterion
-- 1. Check log entries and order
SELECT
    status, stichtag, restart_value, message
FROM
    `project.dataset.job_log_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_opt_text'
ORDER BY
    created_at;
-- Expected output:
-- status   | stichtag   | restart_value | message
-- ---------|------------|---------------|--------------------------------------------------
-- STARTED  | DDMMYYYY   | 0             | NULL
-- RUNNING  | DDMMYYYY   | 0             | NULL
-- INFO     | DDMMYYYY   | 0             | Core logic executed successfully
-- OK       | DDMMYYYY   | 0             | Die Abarbeitung wurde ohne erkennbare Fehler beendet

-- 2. Check row count in fos_table
SELECT COUNT(*) FROM `project.dataset.fos_table`;
-- Expected output: 2

-- 3. Check content in fos_table
SELECT DWH_VERTRAG_ID, col1, col2 FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID;
-- Expected output:
-- DWH_VERTRAG_ID | col1 | col2
-- ---------------|------|------
-- 1              | A    | X
-- 2              | B    | Y
```

---

### Test Case 2: Successful Execution - With Explicit Stichtag, Default Wiederanlaufwert

**Purpose:**
Verify that the wrapper procedure correctly uses an explicitly provided `p_stichtag` and defaults `p_wiederanlaufWert` to `0`.

**Setup:**
1.  Ensure `job_log_audit`, `contract_cache_source`, and `fos_table` are empty.
2.  Populate `contract_cache_source` with sample data relevant to the explicit `p_stichtag`.

```sql
-- Populate contract_cache_source for this test
INSERT INTO `project.dataset.contract_cache_source` (DWH_VERTRAG_ID, col1, col2, gültig_von, gültig_bis, ladedatum) VALUES
(10, 'D', 'P', DATE '2023-01-01', DATE '2023-01-10', DATE '2023-01-05'), -- Should NOT be included (stichtag < gültig_bis fails)
(11, 'E', 'Q', DATE '2023-01-01', DATE '2023-01-15', DATE '2023-01-05'), -- Should be included
(12, 'F', 'R', DATE '2023-01-01', DATE '2023-01-20', DATE '2023-01-12'), -- Should NOT be included (ladedatum < stichtag fails)
(13, 'G', 'S', DATE '2023-01-10', DATE '2023-01-20', DATE '2023-01-12'); -- Should be included
```

**Action:**
Call the wrapper procedure with `p_stichtag = '13012023'` and `p_wiederanlaufWert = NULL`.

```python
def test_successful_execution_with_stichtag(bq_client):
    # Action: Call the wrapper procedure with an explicit stichtag
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`('13012023', NULL);"
    bq_client.query(query).result()

    # Assertions will follow
```

**Pass/Fail Criterion:**
1.  **Output Parity (Logging):** The `job_log_audit` table must contain exactly 4 entries for `job_name = 'ausd_bp_ta_bpr_opt_text'` with statuses 'STARTED', 'RUNNING', 'INFO', and 'OK'.
2.  **Transformation Correctness (Parameter Handling):**
    *   The `stichtag` in all log entries must be `'13012023'`.
    *   The `restart_value` in all log entries must be `0`.
3.  **Data Quality (Row Count):** The `fos_table` must contain 2 rows (DWH_VERTRAG_ID 11 and 13).
4.  **Data Quality (Content):** The inserted rows in `fos_table` must match the expected data based on the filtering logic with `p_stichtag = '13012023'`.

```sql
-- SQL Assertions for Pass/Fail Criterion
-- 1. Check log entries
SELECT
    status, stichtag, restart_value
FROM
    `project.dataset.job_log_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_opt_text'
ORDER BY
    created_at;
-- Expected output:
-- status   | stichtag   | restart_value
-- ---------|------------|---------------
-- STARTED  | 13012023   | 0
-- RUNNING  | 13012023   | 0
-- INFO     | 13012023   | 0
-- OK       | 13012023   | 0

-- 2. Check row count in fos_table
SELECT COUNT(*) FROM `project.dataset.fos_table`;
-- Expected output: 2

-- 3. Check content in fos_table
SELECT DWH_VERTRAG_ID, col1, col2 FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID;
-- Expected output:
-- DWH_VERTRAG_ID | col1 | col2
-- ---------------|------|------
-- 11             | E    | Q
-- 13             | G    | S
```

---

### Test Case 3: Successful Execution - With Stichtag and Wiederanlaufwert (Restart Logic)

**Purpose:**
Verify that the wrapper procedure correctly passes both `p_stichtag` and `p_wiederanlaufWert` to the core logic, and that the core logic's restart (`DELETE` and `INSERT` with `DWH_VERTRAG_ID` condition) works as expected.

**Setup:**
1.  Ensure `job_log_audit`, `contract_cache_source`, and `fos_table` are empty.
2.  **Initial Run Simulation:** Populate `fos_table` with data as if a previous run completed successfully.
3.  Populate `contract_cache_source` with data, some of which would be affected by the `p_wiederanlaufWert` condition.

```sql
-- Initial data in fos_table (simulating a previous run)
INSERT INTO `project.dataset.fos_table` (DWH_VERTRAG_ID, col1, col2) VALUES
(1, 'Old', 'Data'),
(50, 'Old', 'Data'),
(100, 'Old', 'Data'),
(150, 'Old', 'Data'),
(200, 'Old', 'Data');

-- Populate contract_cache_source for this test
INSERT INTO `project.dataset.contract_cache_source` (DWH_VERTRAG_ID, col1, col2, gültig_von, gültig_bis, ladedatum) VALUES
(1, 'A', 'X', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'), -- Should be inserted (DWH_VERTRAG_ID < 100)
(99, 'B', 'Y', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'), -- Should be inserted (DWH_VERTRAG_ID < 100)
(100, 'C', 'Z', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'), -- Should be deleted and re-inserted
(101, 'D', 'P', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'), -- Should be inserted (DWH_VERTRAG_ID > 100)
(250, 'E', 'Q', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'); -- Should be inserted (DWH_VERTRAG_ID > 100)
```

**Action:**
Call the wrapper procedure with `p_stichtag = '15012023'` and `p_wiederanlaufWert = 100`.

```python
def test_successful_execution_with_restart_value(bq_client):
    # Setup initial data in fos_table
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.fos_table` (DWH_VERTRAG_ID, col1, col2) VALUES
        (1, 'Old', 'Data'), (50, 'Old', 'Data'), (100, 'Old', 'Data'), (150, 'Old', 'Data'), (200, 'Old', 'Data');
    """).result()

    # Setup contract_cache_source
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.contract_cache_source` (DWH_VERTRAG_ID, col1, col2, gültig_von, gültig_bis, ladedatum) VALUES
        (1, 'A', 'X', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'),
        (99, 'B', 'Y', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'),
        (100, 'C', 'Z', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'),
        (101, 'D', 'P', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10'),
        (250, 'E', 'Q', DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10');
    """).result()

    # Action: Call the wrapper procedure with explicit stichtag and restart value
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`('15012023', 100);"
    bq_client.query(query).result()

    # Assertions will follow
```

**Pass/Fail Criterion:**
1.  **Output Parity (Logging):** The `job_log_audit` table must contain exactly 4 entries for `job_name = 'ausd_bp_ta_bpr_opt_text'` with statuses 'STARTED', 'RUNNING', 'INFO', and 'OK'.
2.  **Transformation Correctness (Parameter Handling):**
    *   The `stichtag` in all log entries must be `'15012023'`.
    *   The `restart_value` in all log entries must be `100`.
3.  **Data Quality (Row Count):** The `fos_table` must contain 5 rows. (Original 1, 50, plus new 1, 99, 100, 101, 250. Original 100, 150, 200 should be deleted and 100, 101, 250 re-inserted).
    *   Expected rows: (1, 'Old', 'Data'), (50, 'Old', 'Data'), (1, 'A', 'X'), (99, 'B', 'Y'), (100, 'C', 'Z'), (101, 'D', 'P'), (250, 'E', 'Q').
    *   Wait, the `DELETE` is `DWH_VERTRAG_ID >= p_wiederanlaufWert`. So original 100, 150, 200 are deleted.
    *   The `INSERT` is `(p_wiederanlaufWert = 0 OR DWH_VERTRAG_ID > p_wiederanlaufWert)`. So for `p_wiederanlaufWert = 100`, it inserts `DWH_VERTRAG_ID > 100`.
    *   This means `contract_cache_source` rows with DWH_VERTRAG_ID 1 and 99 will *not* be inserted in this run because `DWH_VERTRAG_ID > 100` is false.
    *   Let's re-evaluate the `INSERT` condition: `(p_wiederanlaufWert = 0 OR DWH_VERTRAG_ID > p_wiederanlaufWert)`.
    *   If `p_wiederanlaufWert = 100`, then `DWH_VERTRAG_ID > 100`.
    *   So, from `contract_cache_source`, only 101 and 250 would be inserted.
    *   `fos_table` after run: (1, 'Old', 'Data'), (50, 'Old', 'Data'), (101, 'D', 'P'), (250, 'E', 'Q'). Total 4 rows.
    *   This is a critical check for transformation correctness. The legacy script says: "wird dieser Wert gesetzt, so werden nur Vertraege zu DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle geschrieben (die Eintraege bzgl. Werten >= diesem Wert werden geloescht)". This implies the `INSERT` condition should be `DWH_VERTRAG_ID > p_wiederanlaufWert` and the `DELETE` condition `DWH_VERTRAG_ID >= p_wiederanlaufWert`. The current BigQuery code implements this.

4.  **Data Quality (Content):** The `fos_table` must contain the expected rows after the restart logic.

```sql
-- SQL Assertions for Pass/Fail Criterion
-- 1. Check log entries
SELECT
    status, stichtag, restart_value
FROM
    `project.dataset.job_log_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_opt_text'
ORDER BY
    created_at;
-- Expected output:
-- status   | stichtag   | restart_value
-- ---------|------------|---------------
-- STARTED  | 15012023   | 100
-- RUNNING  | 15012023   | 100
-- INFO     | 15012023   | 100
-- OK       | 15012023   | 100

-- 2. Check row count in fos_table
SELECT COUNT(*) FROM `project.dataset.fos_table`;
-- Expected output: 4

-- 3. Check content in fos_table
SELECT DWH_VERTRAG_ID, col1, col2 FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID;
-- Expected output:
-- DWH_VERTRAG_ID | col1 | col2
-- ---------------|------|------
-- 1              | Old  | Data
-- 50             | Old  | Data
-- 101            | D    | P
-- 250            | E    | Q
```

---

### Test Case 4: Error Handling - Core Logic Failure

**Purpose:**
Verify that the wrapper procedure's `EXCEPTION WHEN ERROR` block correctly catches errors from the core logic, logs the error, and re-raises it to the caller.

**Setup:**
1.  Ensure `job_log_audit`, `contract_cache_source`, and `fos_table` are empty.
2.  The `k_ausd_bp_ta_bpr_opt_text` procedure is designed to accept a `p_simulate_error` boolean parameter to trigger an error.

**Action:**
Call the wrapper procedure with `p_simulate_error = TRUE`.

```python
def test_error_handling_core_logic_failure(bq_client):
    # Action: Call the wrapper procedure, instructing core logic to simulate an error
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`(NULL, NULL, TRUE);"
    
    # Expect the call to raise an exception
    with pytest.raises(Exception) as excinfo:
        bq_client.query(query).result()
    
    # Assertions will follow
```

**Pass/Fail Criterion:**
1.  **Output Parity (Error Propagation):** The `pytest` call to the wrapper procedure must raise an exception, and its message should indicate the re-raised error (`AppError: Abbruch`).
2.  **External-system replacements (Logging):** The `job_log_audit` table must contain exactly 3 entries for `job_name = 'ausd_bp_ta_bpr_opt_text'` with statuses 'STARTED', 'RUNNING', and 'ERROR'.
3.  **Data Quality (Error Message):** The 'ERROR' entry in `job_log_audit` must contain a message reflecting the simulated error from the core logic (e.g., 'Simulated error in k_ausd_bp_ta_bpr_opt_text core logic.').
4.  **Data Quality (Row Count):** The `fos_table` must remain empty, as the transaction should be rolled back or no data inserted due to the error.

```sql
-- SQL Assertions for Pass/Fail Criterion
-- 1. Check log entries and order
SELECT
    status, message
FROM
    `project.dataset.job_log_audit`
WHERE
    job_name = 'ausd_bp_ta_bpr_opt_text'
ORDER BY
    created_at;
-- Expected output:
-- status   | message
-- ---------|-------------------------------------------------------------------
-- STARTED  | NULL
-- RUNNING  | NULL
-- ERROR    | Simulated error in k_ausd_bp_ta_bpr_opt_text core logic.

-- 2. Check row count in fos_table
SELECT COUNT(*) FROM `project.dataset.fos_table`;
-- Expected output: 0
```

---

### Test Case 5: Data Transformation - Filtering Logic Edge Cases

**Purpose:**
Verify the precise boundary conditions of the date-based filtering logic (`gültig_von`, `gültig_bis`, `ladedatum`) in the core procedure.

**Setup:**
1.  Ensure `job_log_audit`, `contract_cache_source`, and `fos_table` are empty.
2.  Populate `contract_cache_source` with data specifically designed to test the `WHERE` clause conditions relative to a `p_stichtag`.

```sql
-- Populate contract_cache_source for this test with p_stichtag = '10012023' (DATE '2023-01-10')
INSERT INTO `project.dataset.contract_cache_source` (DWH_VERTRAG_ID, col1, col2, gültig_von, gültig_bis, ladedatum) VALUES
-- Valid cases
(1, 'V1', 'A', DATE '2023-01-01', DATE '2023-01-11', DATE '2023-01-09'), -- gültig_von <= stichtag, stichtag < gültig_bis, ladedatum < stichtag
(2, 'V2', 'B', DATE '2023-01-10', DATE '2023-01-11', DATE '2023-01-09'), -- gültig_von = stichtag, stichtag < gültig_bis, ladedatum < stichtag

-- Invalid: gültig_von > stichtag
(3, 'I1', 'C', DATE '2023-01-11', DATE '2023-01-15', DATE '2023-01-09'),

-- Invalid: stichtag >= gültig_bis
(4, 'I2', 'D', DATE '2023-01-01', DATE '2023-01-10', DATE '2023-01-09'), -- stichtag = gültig_bis
(5, 'I3', 'E', DATE '2023-01-01', DATE '2023-01-09', DATE '2023-01-08'),

-- Invalid: ladedatum >= stichtag
(6, 'I4', 'F', DATE '2023-01-01', DATE '2023-01-11', DATE '2023-01-10'), -- ladedatum = stichtag
(7, 'I5', 'G', DATE '2023-01-01', DATE '2023-01-11', DATE '2023-01-11');
```

**Action:**
Call the wrapper procedure with `p_stichtag = '10012023'` and `p_wiederanlaufWert = 0`.

```python
def test_data_transformation_filtering_edge_cases(bq_client):
    # Setup contract_cache_source with edge case data
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.contract_cache_source` (DWH_VERTRAG_ID, col1, col2, gültig_von, gültig_bis, ladedatum) VALUES
        (1, 'V1', 'A', DATE '2023-01-01', DATE '2023-01-11', DATE '2023-01-09'),
        (2, 'V2', 'B', DATE '2023-01-10', DATE '2023-01-11', DATE '2023-01-09'),
        (3, 'I1', 'C', DATE '2023-01-11', DATE '2023-01-15', DATE '2023-01-09'),
        (4, 'I2', 'D', DATE '2023-01-01', DATE '2023-01-10', DATE '2023-01-09'),
        (5, 'I3', 'E', DATE '2023-01-01', DATE '2023-01-09', DATE '2023-01-08'),
        (6, 'I4', 'F', DATE '2023-01-01', DATE '2023-01-11', DATE '2023-01-10'),
        (7, 'I5', 'G', DATE '2023-01-01', DATE '2023-01-11', DATE '2023-01-11');
    """).result()

    # Action: Call the wrapper procedure with a specific stichtag
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`('10012023', 0);"
    bq_client.query(query).result()

    # Assertions will follow
```

**Pass/Fail Criterion:**
1.  **Data Quality (Row Count):** The `fos_table` must contain exactly 2 rows.
2.  **Data Quality (Content):** The inserted rows must correspond to `DWH_VERTRAG_ID` 1 and 2, demonstrating correct handling of the date boundaries.

```sql
-- SQL Assertions for Pass/Fail Criterion
-- 1. Check row count in fos_table
SELECT COUNT(*) FROM `project.dataset.fos_table`;
-- Expected output: 2

-- 2. Check content in fos_table
SELECT DWH_VERTRAG_ID, col1, col2 FROM `project.dataset.fos_table` ORDER BY DWH_VERTRAG_ID;
-- Expected output:
-- DWH_VERTRAG_ID | col1 | col2
-- ---------------|------|------
-- 1              | V1   | A
-- 2              | V2   | B
```

---

### Test Case 6: Audit Log Table Schema and Data Quality

**Purpose:**
Verify the schema, data types, and content integrity of the `job_log_audit` table across different job statuses. This ensures the logging mechanism is a faithful replacement for file-based logging.

**Setup:**
1.  Ensure `job_log_audit` is empty.
2.  Run a successful job and a failed job to generate various log entries.

**Action:**
1.  Call `ausd_bp_ta_bpr_opt_text_wrapper('01012023', 0)` (successful run).
2.  Call `ausd_bp_ta_bpr_opt_text_wrapper(NULL, NULL, TRUE)` (failed run).

```python
def test_audit_log_table_schema_and_data_quality(bq_client):
    # Action 1: Successful run
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`('01012023', 0);").result()

    # Action 2: Failed run
    with pytest.raises(Exception):
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper`(NULL, NULL, TRUE);").result()

    # Assertions will follow
```

**Pass/Fail Criterion:**
1.  **Schema Assertion:** The `job_log_audit` table schema must match the defined DDL (e.g., `entry_nr` is `INT64`, `job_name` is `STRING`, `created_at` is `TIMESTAMP`).
2.  **Data Quality (Row Count):** The `job_log_audit` table must contain 7 entries (4 for success, 3 for failure).
3.  **Data Quality (Content & Sequencing):**
    *   `job_name` must always be 'ausd_bp_ta_bpr_opt_text'.
    *   `entry_nr` must be sequential for each job run (e.g., 1, 2, 3, 4 for the first run, then 1, 2, 3 for the second run if `MAX(entry_nr) + 1` is scoped per job_name). *Correction*: The design implies `entry_nr` is globally incrementing for the job, not reset per run. `SELECT IFNULL(MAX(entry_nr), 0) + 1 FROM ... WHERE job_name = v_jobkennung` means it increments *per job_name*. So, the first run will have 1,2,3,4. The second run will have 5,6,7.
    *   `status` values must correctly reflect 'STARTED', 'RUNNING', 'INFO', 'OK', and 'ERROR'.
    *   `stichtag` and `restart_value` must reflect the parameters passed for each run.
    *   `message` field should be populated for 'INFO', 'OK', and 'ERROR' statuses.
    *   `created_at` values must be valid timestamps and chronologically ordered for each job run.

```sql
-- SQL Assertions for Pass/Fail Criterion
-- 1. Check schema (manual inspection or using BigQuery API/CLI)
-- Example: bq show --schema --format=prettyjson project:dataset.job_log_audit

-- 2. Check total row count
SELECT COUNT(*) FROM `project.dataset.job_log_audit`;
-- Expected output: 7

-- 3. Check content and sequencing
SELECT
    entry_nr, job_name, status, stichtag, restart_value, message, created_at
FROM
    `project.dataset.job_log_audit`
ORDER BY
    created_at;
-- Expected output (approximate, timestamps will vary):
-- entry_nr | job_name                 | status   | stichtag   | restart_value | message                                          | created_at
-- ---------|--------------------------|----------|------------|---------------|--------------------------------------------------|--------------------------
-- 1        | ausd_bp_ta_bpr_opt_text  | STARTED  | 01012023   | 0             | NULL                                             | 2023-10-27 10:00:01 UTC
-- 2        | ausd_bp_ta_bpr_opt_text  | RUNNING  | 01012023   | 0             | NULL                                             | 2023-10-27 10:00:02 UTC
-- 2        | ausd_bp_ta_bpr_opt_text  | INFO     | 01012023   | 0             | Core logic executed successfully                 | 2023-10-27 10:00:03 UTC
-- 2        | ausd_bp_ta_bpr_opt_text  | OK       | NULL       | NULL          | Die Abarbeitung wurde ohne erkennbare Fehler beendet | 2023-10-27 10:00:04 UTC
-- 3        | ausd_bp_ta_bpr_opt_text  | STARTED  | DDMMYYYY   | 0             | NULL                                             | 2023-10-27 10:00:05 UTC
-- 4        | ausd_bp_ta_bpr_opt_text  | RUNNING  | DDMMYYYY   | 0             | NULL                                             | 2023-10-27 10:00:06 UTC
-- 4        | ausd_bp_ta_bpr_opt_text  | ERROR    | NULL       | NULL          | Simulated error in k_ausd_bp_ta_bpr_opt_text core logic. | 2023-10-27 10:00:07 UTC
```