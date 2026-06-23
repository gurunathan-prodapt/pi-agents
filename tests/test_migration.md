As a senior data-migration QA engineer, I've analyzed the migration design and the provided legacy and target code for `r_ausd_v_ta_p_discount_rr.ksh`. The core of this migration is replatforming a KornShell orchestrator to a BigQuery Stored Procedure managed by Airflow, with file-based logging replaced by a BigQuery logging table.

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests: `r_ausd_v_ta_p_discount_rr.ksh`

**Target Job:** `my_gcp_project.my_bq_dataset.Vertragsdatenabgleich` (BigQuery Stored Procedure) orchestrated by `dags/r_ausd_v_ta_p_discount_rr_dag.py` (Airflow DAG).

**Assumptions:**
*   The `my_gcp_project.my_bq_dataset` BigQuery dataset exists.
*   All `sql/ddl/job_log.sql` and `sql/procedures/*.sql` files have been deployed to BigQuery.
*   The `dags/r_ausd_v_ta_p_discount_rr_dag.py` DAG has been deployed to Airflow.
*   Access to a legacy environment to run the original `r_ausd_v_ta_p_discount_rr.ksh` script and capture its output (log files, exit codes).
*   Access to BigQuery to query the `job_log` table.
*   Access to Airflow UI/CLI to trigger DAGs and check task statuses.

---

### Test Case 1: Successful Execution (Happy Path)

**Purpose:** To verify that the migrated job executes successfully end-to-end with valid parameters, correctly orchestrates the core logic, and logs all expected events to the BigQuery `job_log` table, mirroring the legacy script's successful run.

**Setup:**
1.  Ensure the `job_log` table is empty or truncated before execution.
2.  Identify a valid `stichtag` (e.g., `01012023`) and `laufnummer` (e.g., `123`).
3.  **Legacy:** Prepare a clean environment for the legacy script.
4.  **Migrated:** Ensure the `k_ausd_v_ta_p_discount_rr` BigQuery Stored Procedure is deployed (even as a placeholder).

**Action:**
1.  **Legacy:** Execute the original KornShell script with valid parameters:
    ```bash
    # Example:
    ./r_ausd_v_ta_p_discount_rr.ksh -s 01012023 -l 123
    # Capture stdout/stderr and the content of the generated log file ($LogDatei)
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr` with the same parameters:
    ```python
    # Example Airflow CLI command (or trigger via UI):
    # airflow dags trigger r_ausd_v_ta_p_discount_rr -c '{"stichtag": "01012023", "laufnummer": "123"}'
    ```
    Wait for the DAG run to complete successfully.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG run completes successfully.
    *   The legacy script exits with code `0`.
    *   The `job_log` table contains a sequence of log entries that semantically match the legacy script's log file, including:
        *   Job start message (`DWMSG_ErzeugeEintrag`).
        *   Stichtag info (`DWMSG_SetzeStichtagInfo`).
        *   Job header information.
        *   Invocation of `k_ausd_v_ta_p_discount_rr`.
        *   Core procedure execution message (from `k_ausd_v_ta_p_discount_rr`).
        *   Successful completion message ("Die Abarbeitung wurde ohne erkennbare Fehler beendet").
        *   Status OK message (`DWMSG_SetzeStatusOK`).
    *   The `job_id` (DW_EintragsNr) is consistent across all entries for a single run.
    *   No error entries (`severity = 'E'`) are present in `job_log`.

**Runnable Test Code (BigQuery Assertion):**
```sql
-- After running the migrated job, get the latest job_id
DECLARE latest_job_id STRING;
SET latest_job_id = (SELECT job_id FROM `my_gcp_project.my_bq_dataset.job_log` ORDER BY created_at DESC LIMIT 1);

-- Assert the presence and order of key log messages
SELECT
  ARRAY_AGG(message ORDER BY created_at) AS actual_messages
FROM
  `my_gcp_project.my_bq_dataset.job_log`
WHERE
  job_id = latest_job_id
  AND job_name = 'BERT_V_TA_P_DISCOUNT_RR'
  AND severity = 'I'
HAVING
  -- Check for key messages in expected order (simplified for example)
  STRING_AGG(message, ' ||| ' ORDER BY created_at) LIKE '%Job started: Vertragsdatenabgleich%'
  AND STRING_AGG(message, ' ||| ' ORDER BY created_at) LIKE '%Stichtag (Reference Date) set: 01012023 (Format: DDMMYYYY)%'
  AND STRING_AGG(message, ' ||| ' ORDER BY created_at) LIKE '%JobHeader: Vertragsdatenabgleich Version: V1.0.0 Stichtag: 01012023 Laufnummer: 123%'
  AND STRING_AGG(message, ' ||| ' ORDER BY created_at) LIKE '%Core procedure k_ausd_v_ta_p_discount_rr executed (placeholder).%'
  AND STRING_AGG(message, ' ||| ' ORDER BY created_at) LIKE '%Die Abarbeitung wurde ohne erkennbare Fehler beendet%'
  AND STRING_AGG(message, ' ||| ' ORDER BY created_at) LIKE '%Job completed successfully.%';
```

---

### Test Case 2: Help Message Display (`-h` / `p_h=TRUE`)

**Purpose:** To verify that the migrated job correctly displays the usage/help message and exits without further processing when the help flag is provided, matching the legacy script's behavior.

**Setup:**
1.  Ensure the `job_log` table is empty or truncated before execution.

**Action:**
1.  **Legacy:** Execute the original KornShell script with the help flag:
    ```bash
    ./r_ausd_v_ta_p_discount_rr.ksh -h
    # Capture stdout/stderr
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr` with `p_h` set to `true`. This might require modifying the DAG's `parameters` for `p_h` to `true` or directly calling the BQ SP. For direct BQ SP call:
    ```sql
    CALL `my_gcp_project.my_bq_dataset.Vertragsdatenabgleich`(TRUE, NULL, NULL);
    -- Capture the result message
    ```

**Pass/Fail Criterion:**
*   **Pass:**
    *   **Legacy:** The script prints the usage text to stdout and exits with code `0`. No log file is created or modified.
    *   **Migrated:**
        *   When called directly, the BigQuery Stored Procedure returns a result set containing the `usage_text`.
        *   No entries are inserted into the `job_log` table.
        *   If triggered via Airflow with `p_h=TRUE` (assuming the DAG is modified to allow this, or a separate test DAG is used), the task completes successfully, and the usage message is visible in the BigQuery job output/logs, with no `job_log` entries.

**Runnable Test Code (BigQuery Assertion):**
```sql
-- To verify no log entries are created:
SELECT COUNT(*) FROM `my_gcp_project.my_bq_dataset.job_log`;
-- Expected result: 0

-- To verify usage text (manual inspection of BQ query result for direct call):
-- CALL `my_gcp_project.my_bq_dataset.Vertragsdatenabgleich`(TRUE, NULL, NULL);
-- Expected output: A result set with a column 'message' containing the usage string.
```

---

### Test Case 3: Missing Required Parameter (`-s` / `p_s=NULL`)

**Purpose:** To verify that the migrated job correctly identifies and handles a missing `stichtag` parameter, logs the error, and terminates, matching the legacy script's error behavior.

**Setup:**
1.  Ensure the `job_log` table is empty or truncated before execution.
2.  Identify a valid `laufnummer` (e.g., `123`).

**Action:**
1.  **Legacy:** Execute the original KornShell script without the `-s` parameter:
    ```bash
    ./r_ausd_v_ta_p_discount_rr.ksh -l 123
    # Capture stdout/stderr and the content of the generated log file ($LogDatei)
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr` with `stichtag` parameter missing or set to `NULL`.
    ```python
    # Example Airflow CLI command:
    # airflow dags trigger r_ausd_v_ta_p_discount_rr -c '{"laufnummer": "123"}'
    ```
    Wait for the DAG run to complete (it should fail).

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG task `call_vertragsdatenabgleich_procedure` fails.
    *   The legacy script exits with a non-zero error code (e.g., `193` or `1`).
    *   The `job_log` table contains an error entry (`severity = 'E'`) indicating a parameter error for `Stichtag`, with `error_code = 1` and `error_arg = 'Stichtag'`.
    *   The `usage_text` is implicitly or explicitly displayed in the Airflow task logs (from the `SELECT usage_text AS message;` statement before `RAISE`).
    *   The error message in `job_log` and Airflow task logs semantically matches the legacy script's error output.

**Runnable Test Code (BigQuery Assertion):**
```sql
-- After running the migrated job, get the latest job_id (it will be 'N/A' for parameter errors)
DECLARE latest_job_id STRING;
SET latest_job_id = (SELECT job_id FROM `my_gcp_project.my_bq_dataset.job_log` ORDER BY created_at DESC LIMIT 1);

-- Assert the error log entry
SELECT
  COUNT(*)
FROM
  `my_gcp_project.my_bq_dataset.job_log`
WHERE
  job_id = 'N/A' -- Parameter errors log 'N/A' for job_id initially
  AND job_name = 'BERT_V_TA_P_DISCOUNT_RR'
  AND severity = 'E'
  AND error_code = 1
  AND error_arg = 'Stichtag'
  AND message = 'Parameterfehler';
-- Expected result: 1
```

---

### Test Case 4: Invalid Parameter (`-l` / `p_l` not numeric)

**Purpose:** To verify that the migrated job correctly identifies and handles an invalid `laufnummer` parameter (non-numeric), logs the error, and terminates, matching the legacy script's error behavior.

**Setup:**
1.  Ensure the `job_log` table is empty or truncated before execution.
2.  Identify a valid `stichtag` (e.g., `01012023`).

**Action:**
1.  **Legacy:** Execute the original KornShell script with a non-numeric `-l` parameter:
    ```bash
    ./r_ausd_v_ta_p_discount_rr.ksh -s 01012023 -l ABC
    # Capture stdout/stderr and the content of the generated log file ($LogDatei)
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr` with `laufnummer` set to a non-numeric string.
    ```python
    # Example Airflow CLI command:
    # airflow dags trigger r_ausd_v_ta_p_discount_rr -c '{"stichtag": "01012023", "laufnummer": "ABC"}'
    ```
    Wait for the DAG run to complete (it should fail).

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG task `call_vertragsdatenabgleich_procedure` fails.
    *   The legacy script exits with a non-zero error code (e.g., `193` or `1`).
    *   The `job_log` table contains an error entry (`severity = 'E'`) indicating a parameter error for `Laufnummer`, with `error_code = 2` and `error_arg = 'Laufnummer'`.
    *   The `usage_text` is implicitly or explicitly displayed in the Airflow task logs.
    *   The error message in `job_log` and Airflow task logs semantically matches the legacy script's error output.

**Runnable Test Code (BigQuery Assertion):**
```sql
-- After running the migrated job, get the latest job_id (it will be 'N/A' for parameter errors)
DECLARE latest_job_id STRING;
SET latest_job_id = (SELECT job_id FROM `my_gcp_project.my_bq_dataset.job_log` ORDER BY created_at DESC LIMIT 1);

-- Assert the error log entry
SELECT
  COUNT(*)
FROM
  `my_gcp_project.my_bq_dataset.job_log`
WHERE
  job_id = 'N/A' -- Parameter errors log 'N/A' for job_id initially
  AND job_name = 'BERT_V_TA_P_DISCOUNT_RR'
  AND severity = 'E'
  AND error_code = 2
  AND error_arg = 'Laufnummer'
  AND message = 'Parameterfehler';
-- Expected result: 1
```

---

### Test Case 5: Core Script Failure (`k_ausd_v_ta_p_discount_rr` raises error)

**Purpose:** To verify that the migrated job's error handling mechanism correctly catches exceptions raised by the invoked core processing procedure (`k_ausd_v_ta_p_discount_rr`), logs the failure, and terminates gracefully, mirroring the legacy script's `trap ERR` behavior.

**Setup:**
1.  Ensure the `job_log` table is empty or truncated before execution.
2.  Identify a valid `stichtag` (e.g., `01012023`) and `laufnummer` (e.g., `123`).
3.  **Migrated:** Temporarily modify `my_gcp_project.my_bq_dataset.k_ausd_v_ta_p_discount_rr` to explicitly `RAISE` an error.
    ```sql
    -- Modified k_ausd_v_ta_p_discount_rr for testing:
    CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.k_ausd_v_ta_p_discount_rr`(
      IN p_job_kennung STRING,
      IN p_dw_eintrags_nr STRING
    )
    BEGIN
      -- Simulate an error in the core logic
      RAISE USING MESSAGE = 'Simulated error in k_ausd_v_ta_p_discount_rr';
    END;
    ```
4.  **Legacy:** Simulate an error in `k_ausd_v_ta_p_discount_rr.ksh` (e.g., `exit 1` at the beginning of the script).

**Action:**
1.  **Legacy:** Execute the original KornShell script with valid parameters (which will then trigger the failing core script):
    ```bash
    ./r_ausd_v_ta_p_discount_rr.ksh -s 01012023 -l 123
    # Capture stdout/stderr and the content of the generated log file ($LogDatei)
    ```
2.  **Migrated:** Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr` with valid parameters.
    ```python
    # airflow dags trigger r_ausd_v_ta_p_discount_rr -c '{"stichtag": "01012023", "laufnummer": "123"}'
    ```
    Wait for the DAG run to complete (it should fail).

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG task `call_vertragsdatenabgleich_procedure` fails.
    *   The legacy script exits with a non-zero error code (e.g., `1`).
    *   The `job_log` table contains:
        *   Initial job start and header messages.
        *   An error entry from `DWMSG_Fehlerbehandlung` (severity 'E', message like 'Job failed with error: ...').
        *   An error entry with message 'AppError: Abbruch - Job terminated due to an error.'.
    *   The error messages in `job_log` and Airflow task logs semantically match the legacy script's error output (e.g., "AppError: Abbruch").

**Runnable Test Code (BigQuery Assertion):**
```sql
-- After running the migrated job, get the latest job_id
DECLARE latest_job_id STRING;
SET latest_job_id = (SELECT job_id FROM `my_gcp_project.my_bq_dataset.job_log` ORDER BY created_at DESC LIMIT 1);

-- Assert the presence of error log entries
SELECT
  COUNT(*)
FROM
  `my_gcp_project.my_bq_dataset.job_log`
WHERE
  job_id = latest_job_id
  AND severity = 'E'
  AND (message LIKE 'Job failed with error: Simulated error in k_ausd_v_ta_p_discount_rr%'
       OR message = 'AppError: Abbruch - Job terminated due to an error.');
-- Expected result: 2 (one from DWMSG_Fehlerbehandlung, one from the explicit AppError log)
```

---

### Test Case 6: `job_log` Schema and Data Quality

**Purpose:** To verify that the `job_log` table schema is correctly defined and that data inserted into it adheres to the expected types and constraints. This ensures the reliability of the new logging mechanism.

**Setup:**
1.  Ensure the `job_log` table is created as per `sql/ddl/job_log.sql`.
2.  Run a successful execution of the migrated job (as in Test Case 1) to populate the `job_log` table.

**Action:**
1.  Query the BigQuery information schema for the `job_log` table.
2.  Query the `job_log` table to inspect data types and nullability.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The `job_log` table exists in `my_gcp_project.my_bq_dataset`.
    *   The schema matches the definition in `sql/ddl/job_log.sql` (column names, data types, nullability).
    *   All `NOT NULL` columns (`job_id`, `job_name`, `severity`, `message`, `created_at`) contain non-NULL values for all entries.
    *   `created_at` values are valid timestamps.
    *   `error_code` values are `INT64` or `NULL`.

**Runnable Test Code (BigQuery Assertion):**
```sql
-- Assert table existence and schema
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `my_gcp_project.my_bq_dataset`.INFORMATION_SCHEMA.COLUMNS
WHERE
  table_name = 'job_log'
ORDER BY
  ordinal_position;

-- Expected output (compare against DDL):
-- column_name | data_type | is_nullable
-- -------------|-----------|-------------
-- job_id      | STRING    | NO
-- job_name    | STRING    | NO
-- severity    | STRING    | NO
-- error_code  | INT64     | YES
-- error_arg   | STRING    | YES
-- message     | STRING    | NO
-- created_at  | TIMESTAMP | NO

-- Assert data quality for non-null constraints (after a successful run)
SELECT
  COUNT(*)
FROM
  `my_gcp_project.my_bq_dataset.job_log`
WHERE
  job_id IS NULL OR job_name IS NULL OR severity IS NULL OR message IS NULL OR created_at IS NULL;
-- Expected result: 0
```

---

### Test Case 7: `DWMSG_ErmittleNr` Uniqueness

**Purpose:** To verify that the `DW_EintragsNr` (job_id) generated by `DWMSG_ErmittleNr` is unique for each distinct execution of the `Vertragsdatenabgleich` procedure, ensuring proper job tracking.

**Setup:**
1.  Ensure the `job_log` table is empty or truncated before execution.

**Action:**
1.  Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr` twice in quick succession with different valid parameters (e.g., different `laufnummer`).
    ```python
    # airflow dags trigger r_ausd_v_ta_p_discount_rr -c '{"stichtag": "01012023", "laufnummer": "100"}'
    # airflow dags trigger r_ausd_v_ta_p_discount_rr -c '{"stichtag": "01012023", "laufnummer": "101"}'
    ```
    Wait for both DAG runs to complete successfully.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The `job_log` table contains entries for two distinct `job_id` values.
    *   Each `job_id` is unique and consistent across all log entries belonging to its respective run.

**Runnable Test Code (BigQuery Assertion):**
```sql
-- After running the job twice
SELECT
  job_id,
  COUNT(DISTINCT job_id) AS distinct_job_ids_per_run,
  COUNT(*) AS total_entries_for_job_id
FROM
  `my_gcp_project.my_bq_dataset.job_log`
GROUP BY
  job_id
HAVING
  COUNT(DISTINCT job_id) = 1 -- Ensure each group has only one distinct job_id (self-consistency)
ORDER BY
  job_id;
-- Expected result: Two rows, each with distinct job_id and distinct_job_ids_per_run = 1.
-- The total number of rows in job_log should be sum of total_entries_for_job_id.

SELECT COUNT(DISTINCT job_id) FROM `my_gcp_project.my_bq_dataset.job_log`;
-- Expected result: 2 (for two runs)
```

---

### Test Case 8: Airflow DAG Integration and Parameter Passing

**Purpose:** To verify that the Airflow DAG correctly invokes the BigQuery Stored Procedure and passes the `stichtag` and `laufnummer` parameters as expected, and that Airflow's parameter validation (if any) works.

**Setup:**
1.  Ensure the Airflow DAG `r_ausd_v_ta_p_discount_rr` is deployed.
2.  Ensure the `job_log` table is empty or truncated.

**Action:**
1.  Trigger the Airflow DAG `r_ausd_v_ta_p_discount_rr` via the Airflow UI or CLI, providing valid `stichtag` and `laufnummer` parameters.
    ```python
    # airflow dags trigger r_ausd_v_ta_p_discount_rr -c '{"stichtag": "01012024", "laufnummer": "456"}'
    ```
2.  Observe the Airflow task logs for the `call_vertragsdatenabgleich_procedure` task.
3.  Query the `job_log` table.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG run completes successfully.
    *   The Airflow task logs show the BigQuery job being executed with the correct procedure name and parameters.
    *   The `job_log` table contains entries for the run, and the `Stichtag` and `Laufnummer` values in the "JobHeader" message match the parameters passed from Airflow.
    *   If Airflow's parameter validation (e.g., `pattern: r"^\d{8}$"`) is tested with invalid input, the DAG run should fail at the parameter parsing stage before the BigQuery procedure is even called.

**Runnable Test Code (BigQuery Assertion):**
```sql
-- After running the Airflow DAG
DECLARE latest_job_id STRING;
SET latest_job_id = (SELECT job_id FROM `my_gcp_project.my_bq_dataset.job_log` ORDER BY created_at DESC LIMIT 1);

SELECT
  message
FROM
  `my_gcp_project.my_bq_dataset.job_log`
WHERE
  job_id = latest_job_id
  AND message LIKE 'JobHeader: %'
ORDER BY
  created_at DESC
LIMIT 1;
-- Expected output: A message like 'JobHeader: Vertragsdatenabgleich Version: V1.0.0 Stichtag: 01012024 Laufnummer: 456'
-- The '01012024' and '456' should match the parameters passed to the Airflow DAG.
```

---