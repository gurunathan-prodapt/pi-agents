The migration of `r_ausd_v_ta_action_assoc.ksh` is primarily a re-platforming of an orchestration script to a BigQuery Stored Procedure. The core business logic resides in a dependent kernel script (`k_ausd_v_ta_action_assoc.ksh`), which is a placeholder in this migration. Therefore, the tests will focus heavily on the wrapper's control flow, parameter handling, logging, and error management, ensuring behavioral equivalence with the legacy KornShell script.

---

## Migration Validation Tests: `r_ausd_v_ta_action_assoc.ksh` to BigQuery Stored Procedure

**Target System:** Google BigQuery
**Migrated Component:** `your_project.your_dataset.vertragsdatenabgleich` (BigQuery Stored Procedure)
**Dependent Components:**
*   `your_project.your_dataset.job_log` (BigQuery Table)
*   `your_project.your_dataset.job_error_log` (BigQuery Table)
*   `your_project.your_dataset.k_ausd_v_ta_action_assoc` (BigQuery Stored Procedure - placeholder)
*   `orchestration/dag_vertragsdatenabgleich.py` (Airflow DAG)

---

### Test Case 1: Help Display Functionality

**Purpose:** To verify that the migrated BigQuery Stored Procedure correctly handles the help flag, displaying usage information and exiting gracefully, similar to the legacy script's `-h` option.

**Setup:**
1.  Ensure the `your_project.your_dataset.vertragsdatenabgleich` stored procedure is deployed.
2.  No specific data setup is required for logging tables as this should not trigger full execution.

**Action:**
Execute the BigQuery Stored Procedure with the `p_enable_help` parameter set to `TRUE`.

**Runnable Test Code (BigQuery SQL):**
```sql
-- Action: Call the stored procedure with help flag
CALL `your_project.your_dataset.vertragsdatenabgleich`(
  p_jobkennung => 'TEST_HELP',
  p_run_date => CURRENT_DATE(),
  p_enable_help => TRUE
);
```

**Pass/Fail Criterion:**
*   **Pass:** The query execution completes successfully (does not raise an error). The result set of the `CALL` statement contains a single row with columns `Programm`, `Version`, `Aufruf`, and `Beschreibung`, matching the expected help text from the design document. No entries should be made in `your_project.your_dataset.job_log` or `your_project.your_dataset.job_error_log` tables.
*   **Fail:** The query raises an error, or the output message is incorrect/missing, or log entries are created.

---

### Test Case 2: Successful Execution (Happy Path)

**Purpose:** To verify that the migrated BigQuery Stored Procedure executes successfully under normal conditions, correctly logs job status, and invokes the dependent kernel procedure. This covers output parity for successful runs and basic transformation correctness for logging.

**Setup:**
1.  Ensure `your_project.your_dataset.vertragsdatenabgleich` and the placeholder `your_project.your_dataset.k_ausd_v_ta_action_assoc` stored procedures are deployed.
2.  Ensure `your_project.your_dataset.job_log` and `your_project.your_dataset.job_error_log` tables exist and are empty or contain known baseline data.
3.  For this test, ensure the placeholder `k_ausd_v_ta_action_assoc` procedure is configured to *succeed* (as it is by default in the provided code).

**Action:**
Execute the BigQuery Stored Procedure with valid parameters and `p_enable_help` set to `FALSE`.

**Runnable Test Code (BigQuery SQL):**
```sql
-- Setup: Clear log tables for a clean test run (optional, but good practice)
TRUNCATE TABLE `your_project.your_dataset.job_log`;
TRUNCATE TABLE `your_project.your_dataset.job_error_log`;

-- Action: Call the stored procedure for a successful run
CALL `your_project.your_dataset.vertragsdatenabgleich`(
  p_jobkennung => 'TEST_HAPPY_PATH',
  p_run_date => CURRENT_DATE(),
  p_enable_help => FALSE
);

-- Verification: Check log entries
SELECT
  entry_no,
  job_kennung,
  program_name,
  status,
  stichtag
FROM `your_project.your_dataset.job_log`
ORDER BY created_ts ASC;

SELECT * FROM `your_project.your_dataset.job_error_log`;
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The `CALL` statement completes successfully.
    *   The `SELECT 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' AS message;` is returned.
    *   `your_project.your_dataset.job_log` contains exactly three entries for `job_kennung = 'TEST_HAPPY_PATH'`:
        1.  `status = 'STARTED'` (from wrapper)
        2.  `status = 'INVOKED'` (from kernel placeholder)
        3.  `status = 'OK'` (from wrapper)
    *   The `entry_no` for all three entries is the same and incremented from any previous runs.
    *   `stichtag` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    *   `your_project.your_dataset.job_error_log` contains no entries.
*   **Fail:** The `CALL` statement fails, incorrect log entries are found, or error logs are populated.

---

### Test Case 3: Parameter Validation - Missing `p_jobkennung`

**Purpose:** To verify that the migrated BigQuery Stored Procedure correctly handles missing or empty `p_jobkennung` parameters, logging the error and signaling an SQLSTATE, mimicking the legacy script's `ErrNr=193` behavior. This covers transformation correctness for parameter handling and error logging.

**Setup:**
1.  Ensure `your_project.your_dataset.vertragsdatenabgleich` is deployed.
2.  Ensure `your_project.your_dataset.job_log` and `your_project.your_dataset.job_error_log` tables exist.

**Action:**
Execute the BigQuery Stored Procedure with `p_jobkennung` set to `NULL` or an empty string.

**Runnable Test Code (BigQuery SQL):**
```sql
-- Setup: Clear log tables for a clean test run (optional)
TRUNCATE TABLE `your_project.your_dataset.job_log`;
TRUNCATE TABLE `your_project.your_dataset.job_error_log`;

-- Action 1: Call with NULL p_jobkennung (expected to fail)
BEGIN
  CALL `your_project.your_dataset.vertragsdatenabgleich`(
    p_jobkennung => NULL,
    p_run_date => CURRENT_DATE(),
    p_enable_help => FALSE
  );
EXCEPTION WHEN ERROR THEN
  SELECT @@error.message AS error_message;
END;

-- Verification 1: Check log entries after NULL parameter
SELECT
  entry_no,
  job_kennung,
  program_name,
  error_no,
  error_arg,
  error_message
FROM `your_project.your_dataset.job_error_log`
WHERE job_kennung = 'BERT_V_TA_ACTION_ASSOC'; -- Default if p_jobkennung is NULL/empty

SELECT * FROM `your_project.your_dataset.job_log`;

-- Setup: Clear log tables again for next action
TRUNCATE TABLE `your_project.your_dataset.job_log`;
TRUNCATE TABLE `your_project.your_dataset.job_error_log`;

-- Action 2: Call with empty string p_jobkennung (expected to fail)
BEGIN
  CALL `your_project.your_dataset.vertragsdatenabgleich`(
    p_jobkennung => '',
    p_run_date => CURRENT_DATE(),
    p_enable_help => FALSE
  );
EXCEPTION WHEN ERROR THEN
  SELECT @@error.message AS error_message;
END;

-- Verification 2: Check log entries after empty string parameter
SELECT
  entry_no,
  job_kennung,
  program_name,
  error_no,
  error_arg,
  error_message
FROM `your_project.your_dataset.job_error_log`
WHERE job_kennung = 'BERT_V_TA_ACTION_ASSOC';

SELECT * FROM `your_project.your_dataset.job_log`;
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   Both `CALL` statements raise an error with `SQLSTATE '45000'`.
    *   The `error_message` returned by the `EXCEPTION` block contains `Parameterfehler: 193 p_jobkennung`.
    *   `your_project.your_dataset.job_error_log` contains one entry for each failed call:
        *   `error_no = 193`
        *   `error_arg = 'p_jobkennung'`
        *   `job_kennung = 'BERT_V_TA_ACTION_ASSOC'` (due to `COALESCE` and `UPPER` in the SP)
    *   `your_project.your_dataset.job_log` contains one `status = 'STARTED'` entry and one `status = 'ERROR'` entry for each failed call, with the same `entry_no` as the error log. The `program_name` should be `Vertragsdatenabgleich`.
*   **Fail:** The `CALL` statement succeeds, or the error message/log entries are incorrect.

---

### Test Case 4: Kernel Script Failure Handling

**Purpose:** To verify that the wrapper BigQuery Stored Procedure correctly handles errors originating from the dependent kernel procedure, logging the failure and propagating an error, similar to the legacy script's `trap ERR` mechanism. This covers transformation correctness for error handling and external system replacement for the kernel script.

**Setup:**
1.  Ensure `your_project.your_dataset.vertragsdatenabgleich` is deployed.
2.  Ensure `your_project.your_dataset.job_log` and `your_project.your_dataset.job_error_log` tables exist.
3.  **Modify the placeholder `k_ausd_v_ta_action_assoc` to simulate a failure.**

**Action:**
1.  Modify `your_project.your_dataset.k_ausd_v_ta_action_assoc` to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel error';`.
2.  Execute `your_project.your_dataset.vertragsdatenabgleich` with valid parameters.
3.  Revert `your_project.your_dataset.k_ausd_v_ta_action_assoc` to its original successful state after the test.

**Runnable Test Code (BigQuery SQL):**
```sql
-- Setup: Clear log tables for a clean test run (optional)
TRUNCATE TABLE `your_project.your_dataset.job_log`;
TRUNCATE TABLE `your_project.your_dataset.job_error_log`;

-- Setup: Temporarily modify k_ausd_v_ta_action_assoc to fail
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_action_assoc`(
  IN p_jobkennung STRING,
  IN p_entry_no INT64
)
BEGIN
  INSERT INTO `your_project.your_dataset.job_log`
    (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
  VALUES
    (p_entry_no, p_jobkennung, 'k_ausd_v_ta_action_assoc', 'V1.0.0', 'N/A', 'INVOKED_AND_FAILED', FORMAT_DATE('%d%m%Y', CURRENT_DATE()), CURRENT_TIMESTAMP());

  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel error from k_ausd_v_ta_action_assoc';
END;

-- Action: Call the wrapper procedure (expected to fail)
BEGIN
  CALL `your_project.your_dataset.vertragsdatenabgleich`(
    p_jobkennung => 'TEST_KERNEL_FAIL',
    p_run_date => CURRENT_DATE(),
    p_enable_help => FALSE
  );
EXCEPTION WHEN ERROR THEN
  SELECT @@error.message AS error_message;
END;

-- Verification: Check log entries
SELECT
  entry_no,
  job_kennung,
  program_name,
  status,
  stichtag
FROM `your_project.your_dataset.job_log`
ORDER BY created_ts ASC;

SELECT
  entry_no,
  job_kennung,
  program_name,
  error_message
FROM `your_project.your_dataset.job_error_log`;

-- Teardown: Revert k_ausd_v_ta_action_assoc to its original successful state
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_action_assoc`(
  IN p_jobkennung STRING,
  IN p_entry_no INT64
)
BEGIN
  INSERT INTO `your_project.your_dataset.job_log`
    (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
  VALUES
    (p_entry_no, p_jobkennung, 'k_ausd_v_ta_action_assoc', 'V1.0.0', 'N/A', 'INVOKED', FORMAT_DATE('%d%m%Y', CURRENT_DATE()), CURRENT_TIMESTAMP());

  SELECT 'Core kernel script k_ausd_v_ta_action_assoc invoked.' AS message;
END;
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The `CALL` statement for `vertragsdatenabgleich` raises an error with `SQLSTATE '45000'`.
    *   The `error_message` returned by the `EXCEPTION` block contains `AppError: Abbruch - Simulated kernel error from k_ausd_v_ta_action_assoc`.
    *   `your_project.your_dataset.job_log` contains three entries for `job_kennung = 'TEST_KERNEL_FAIL'`:
        1.  `status = 'STARTED'` (from wrapper)
        2.  `status = 'INVOKED_AND_FAILED'` (from kernel placeholder, indicating it was called and failed)
        3.  `status = 'ERROR'` (from wrapper's exception handler)
    *   `your_project.your_dataset.job_error_log` contains one entry:
        *   `job_kennung = 'TEST_KERNEL_FAIL'`
        *   `program_name = 'Vertragsdatenabgleich'`
        *   `error_message` contains `Simulated kernel error from k_ausd_v_ta_action_assoc`.
*   **Fail:** The `CALL` statement succeeds, or the error message/log entries are incorrect.

---

### Test Case 5: Data Quality / Schema Assertions for Logging Tables

**Purpose:** To verify that the `job_log` and `job_error_log` tables are created with the correct schema, data types, and nullability constraints as defined in the DDL, ensuring data quality for the logging mechanism. This covers data quality and schema assertions.

**Setup:**
1.  Ensure the DDL scripts (`ddl/job_log_table.sql`, `ddl/job_error_log_table.sql`) have been executed to create the tables.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA` to inspect the table schemas.

**Runnable Test Code (BigQuery SQL):**
```sql
-- Verification: Check job_log table schema
SELECT
  column_name,
  data_type,
  is_nullable
FROM `your_project.your_dataset`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'job_log'
ORDER BY ordinal_position;

-- Verification: Check job_error_log table schema
SELECT
  column_name,
  data_type,
  is_nullable
FROM `your_project.your_dataset`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'job_error_log'
ORDER BY ordinal_position;
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   **`job_log` table:**
        *   `entry_no`: `INT64`, `NO`
        *   `job_kennung`: `STRING`, `NO`
        *   `program_name`: `STRING`, `YES`
        *   `program_version`: `STRING`, `YES`
        *   `log_name`: `STRING`, `YES`
        *   `status`: `STRING`, `NO`
        *   `stichtag`: `STRING`, `YES`
        *   `created_ts`: `TIMESTAMP`, `NO`
    *   **`job_error_log` table:**
        *   `entry_no`: `INT64`, `NO`
        *   `job_kennung`: `STRING`, `NO`
        *   `program_name`: `STRING`, `YES`
        *   `error_no`: `INT64`, `YES`
        *   `error_arg`: `STRING`, `YES`
        *   `error_message`: `STRING`, `YES`
        *   `created_ts`: `TIMESTAMP`, `NO`
*   **Fail:** Any column's data type or nullability constraint does not match the expected definition.

---

### Test Case 6: Airflow DAG Orchestration

**Purpose:** To verify that the provided Airflow DAG can successfully trigger the BigQuery Stored Procedure, passing parameters correctly, and that the overall orchestration flow works as expected. This covers external-system replacements (Airflow for shell execution).

**Setup:**
1.  An Airflow environment (e.g., Cloud Composer) is configured and running.
2.  The `dag_vertragsdatenabgleich.py` DAG is deployed to Airflow.
3.  A `google_cloud_default` connection is configured in Airflow with appropriate BigQuery permissions.
4.  `your_project.your_dataset.vertragsdatenabgleich` and `your_project.your_dataset.k_ausd_v_ta_action_assoc` (in its successful state) are deployed.
5.  `your_project.your_dataset.job_log` and `your_project.your_dataset.job_error_log` tables exist.

**Action:**
Manually trigger the `vertragsdatenabgleich_workflow` DAG in Airflow.

**Runnable Test Code (Airflow UI / CLI):**
```bash
# From Airflow CLI (assuming DAG is deployed)
airflow dags trigger vertragsdatenabgleich_workflow
```

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG run completes successfully (green status).
    *   The `call_vertragsdatenabgleich_sp` task within the DAG completes successfully.
    *   Inspecting BigQuery logs (`your_project.your_dataset.job_log`) shows entries for `job_kennung` starting with `AIRFLOW_TRIGGERED_JOB_` (e.g., `AIRFLOW_TRIGGERED_JOB_20231027`) and `status = 'OK'`, similar to Test Case 2.
    *   No entries are found in `your_project.your_dataset.job_error_log` for this run.
*   **Fail:** The Airflow DAG run fails, the BigQuery task fails, or the BigQuery log entries indicate an error or incorrect parameter passing.