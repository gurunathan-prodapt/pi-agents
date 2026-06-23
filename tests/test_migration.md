As a senior data-migration QA engineer, I've designed a suite of tests to validate the migration of `r_ausd_bp_ta_rn_vertrag.ksh` to Google Cloud Platform. These tests focus on ensuring behavioral equivalence, data integrity, and correct integration with the new GCP ecosystem.

The tests are structured to cover:
1.  **Output Parity**: Verifying that the logging and core script invocation match the legacy behavior.
2.  **Transformation Correctness**: Ensuring parameter parsing, defaulting, validation, and error handling logic are accurately translated.
3.  **External-System Replacements**: Confirming that BigQuery tables (`job_log`, `job_error_log`) correctly replace file-based logging and that the BigQuery Stored Procedure invocation replaces the shell script call.
4.  **Data-Quality / Row-Count / Schema Assertions**: Validating the structure and content of the new logging tables.

**Assumptions for Test Execution:**
*   A BigQuery client is available for executing SQL queries and stored procedures.
*   An Airflow environment (e.g., Cloud Composer) is set up, and the DAG is deployed.
*   The Airflow DAG can be triggered programmatically (e.g., via Airflow REST API or `airflow dags trigger` command in a test runner).
*   The placeholder `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure is deployed.
*   `gcp-project-id` and `bq_dataset_name` are placeholders for the actual project and dataset IDs in the test environment.

---

## Common Setup: BigQuery Table Initialization

Before running any tests that interact with the `job_log` or `job_error_log` tables, these tables should be cleared to ensure a clean state for each test run.

```sql
-- Clear job_log table
TRUNCATE TABLE `gcp-project-id.bq_dataset_name.job_log`;

-- Clear job_error_log table
TRUNCATE TABLE `gcp-project-id.bq_dataset_name.job_error_log`;
```

---

## Test Case 1: Successful Execution - All Parameters Provided

*   **Purpose**: Verify that the migrated job executes successfully when all expected parameters (`stichtag`, `wiederanlaufwert`) are explicitly provided, and that logging and core script invocation are correct.
*   **Setup**: Ensure `job_log` and `job_error_log` tables are empty.
*   **Action**: Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with specific `stichtag` and `wiederanlaufwert` parameters.
    *   `stichtag`: `01012024`
    *   `wiederanlaufwert`: `12345`
    ```python
    # Example pytest-airflow interaction
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import DagRunState
    from airflow.utils import timezone

    def test_successful_execution_all_params():
        # Clear BQ tables (assuming a fixture or setup function handles this)
        # bq_client.query("TRUNCATE TABLE `gcp-project-id.bq_dataset_name.job_log`").result()
        # bq_client.query("TRUNCATE TABLE `gcp-project-id.bq_dataset_name.job_error_log`").result()

        execution_date = timezone.utcnow()
        dag_run = DagRun(
            dag_id="ausd_bp_ta_rn_vertrag_dag",
            run_id=f"test_all_params_{execution_date.isoformat()}",
            execution_date=execution_date,
            state=DagRunState.RUNNING,
            conf={"stichtag": "01012024", "wiederanlaufwert": "12345"},
        )
        # In a real test, you'd use Airflow's API or CLI to trigger and wait for completion.
        # For demonstration, assume this triggers the DAG and it completes.
        # dag_run.create_dagrun()
        # wait_for_dag_run_completion(dag_run.run_id)

        # Simulate successful completion for assertion
        assert get_dag_run_status("ausd_bp_ta_rn_vertrag_dag", dag_run.run_id) == DagRunState.SUCCESS

        # Assertions will be made against BigQuery
    ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG completes successfully.
    2.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_log`.
    3.  The `job_log` record has:
        *   `jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'`
        *   `stichtag = DATE('2024-01-01')`
        *   `wiederanlaufwert = '12345'`
        *   `status = 'COMPLETED'`
    4.  No records exist in `gcp-project-id.bq_dataset_name.job_error_log`.
    5.  The `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure was called with `p_stichtag_ddmmyyyy = '01012024'` and `p_wiederanlaufWert = '12345'`. (This would typically be verified by inspecting BigQuery audit logs or by modifying the placeholder SP to log its inputs to a test table).

    ```sql
    -- Assertion 1: Check job_log entry
    SELECT
        eintragsnr,
        jobkennung,
        FORMAT_DATE('%d%m%Y', stichtag) AS stichtag_formatted,
        wiederanlaufwert,
        status
    FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND stichtag = DATE('2024-01-01')
      AND wiederanlaufwert = '12345'
      AND status = 'COMPLETED';
    -- Expected: 1 row with the specified values.

    -- Assertion 2: Check job_error_log is empty
    SELECT COUNT(*) FROM `gcp-project-id.bq_dataset_name.job_error_log`;
    -- Expected: 0 rows.
    ```

---

## Test Case 2: Successful Execution - Default Stichtag

*   **Purpose**: Verify that `p_stichtag` correctly defaults to `CURRENT_DATE()` when not provided, matching the legacy script's `v_sysdate` fallback.
*   **Setup**: Ensure `job_log` and `job_error_log` tables are empty.
*   **Action**: Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with `wiederanlaufwert` but *without* `stichtag`.
    *   `wiederanlaufwert`: `54321`
    ```python
    # Example pytest-airflow interaction
    def test_successful_execution_default_stichtag():
        # Trigger DAG without 'stichtag'
        # dag_run = trigger_dag("ausd_bp_ta_rn_vertrag_dag", conf={"wiederanlaufwert": "54321"})
        # wait_for_dag_run_completion(dag_run.run_id)
        assert get_dag_run_status("ausd_bp_ta_rn_vertrag_dag", "test_default_stichtag") == DagRunState.SUCCESS
    ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG completes successfully.
    2.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_log`.
    3.  The `job_log` record has:
        *   `jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'`
        *   `stichtag = CURRENT_DATE()` (the date of execution)
        *   `wiederanlaufwert = '54321'`
        *   `status = 'COMPLETED'`
    4.  No records exist in `gcp-project-id.bq_dataset_name.job_error_log`.
    5.  The `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure was called with `p_stichtag_ddmmyyyy` as today's date in `DDMMYYYY` format and `p_wiederanlaufWert = '54321'`.

    ```sql
    -- Assertion 1: Check job_log entry
    SELECT
        eintragsnr,
        jobkennung,
        FORMAT_DATE('%d%m%Y', stichtag) AS stichtag_formatted,
        wiederanlaufwert,
        status
    FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND stichtag = CURRENT_DATE()
      AND wiederanlaufwert = '54321'
      AND status = 'COMPLETED';
    -- Expected: 1 row with the specified values.
    ```

---

## Test Case 3: Successful Execution - Default Wiederanlaufwert

*   **Purpose**: Verify that `p_wiederanlaufWert` correctly defaults to `'0'` when not provided.
*   **Setup**: Ensure `job_log` and `job_error_log` tables are empty.
*   **Action**: Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with `stichtag` but *without* `wiederanlaufwert`.
    *   `stichtag`: `15032024`
    ```python
    # Example pytest-airflow interaction
    def test_successful_execution_default_wiederanlaufwert():
        # Trigger DAG without 'wiederanlaufwert'
        # dag_run = trigger_dag("ausd_bp_ta_rn_vertrag_dag", conf={"stichtag": "15032024"})
        # wait_for_dag_run_completion(dag_run.run_id)
        assert get_dag_run_status("ausd_bp_ta_rn_vertrag_dag", "test_default_wiederanlaufwert") == DagRunState.SUCCESS
    ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG completes successfully.
    2.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_log`.
    3.  The `job_log` record has:
        *   `jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'`
        *   `stichtag = DATE('2024-03-15')`
        *   `wiederanlaufwert = '0'`
        *   `status = 'COMPLETED'`
    4.  No records exist in `gcp-project-id.bq_dataset_name.job_error_log`.
    5.  The `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure was called with `p_stichtag_ddmmyyyy = '15032024'` and `p_wiederanlaufWert = '0'`.

    ```sql
    -- Assertion 1: Check job_log entry
    SELECT
        eintragsnr,
        jobkennung,
        FORMAT_DATE('%d%m%Y', stichtag) AS stichtag_formatted,
        wiederanlaufwert,
        status
    FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND stichtag = DATE('2024-03-15')
      AND wiederanlaufwert = '0'
      AND status = 'COMPLETED';
    -- Expected: 1 row with the specified values.
    ```

---

## Test Case 4: Invalid Stichtag Format

*   **Purpose**: Verify that the `ausd_bp_ta_rn_vertrag_wrapper` SP correctly validates the `stichtag` format (DDMMYYYY) and logs an error if it's incorrect.
*   **Setup**: Ensure `job_log` and `job_error_log` tables are empty.
*   **Action**: Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with an invalid `stichtag` format.
    *   `stichtag`: `2024-01-01` (YYYY-MM-DD format)
    *   `wiederanlaufwert`: `100`
    ```python
    # Example pytest-airflow interaction
    def test_invalid_stichtag_format():
        # Trigger DAG with invalid 'stichtag'
        # dag_run = trigger_dag("ausd_bp_ta_rn_vertrag_dag", conf={"stichtag": "2024-01-01", "wiederanlaufwert": "100"})
        # wait_for_dag_run_completion(dag_run.run_id)
        assert get_dag_run_status("ausd_bp_ta_rn_vertrag_dag", "test_invalid_stichtag_format") == DagRunState.FAILED
    ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG fails.
    2.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_log` with `status = 'FAILED'`.
    3.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_error_log`.
    4.  The `job_error_log` record's `error_message` contains a substring like `'Invalid p_stichtag format: expected DDMMYYYY, got '2024-01-01''`.
    5.  The `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure was *not* called.

    ```sql
    -- Assertion 1: Check job_log entry status
    SELECT status FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper';
    -- Expected: 1 row with status = 'FAILED'.

    -- Assertion 2: Check job_error_log entry
    SELECT error_message FROM `gcp-project-id.bq_dataset_name.job_error_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND error_message LIKE '%Invalid p_stichtag format: expected DDMMYYYY, got ''2024-01-01''%';
    -- Expected: 1 row with the specific error message.
    ```

---

## Test Case 5: Invalid Stichtag Value (Unparseable Date)

*   **Purpose**: Verify that the `ausd_bp_ta_rn_vertrag_wrapper` SP correctly handles unparseable `stichtag` values (e.g., non-existent dates) and logs an error.
*   **Setup**: Ensure `job_log` and `job_error_log` tables are empty.
*   **Action**: Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with an unparseable `stichtag` value.
    *   `stichtag`: `32012024` (32nd of January)
    *   `wiederanlaufwert`: `200`
    ```python
    # Example pytest-airflow interaction
    def test_invalid_stichtag_value():
        # Trigger DAG with unparseable 'stichtag'
        # dag_run = trigger_dag("ausd_bp_ta_rn_vertrag_dag", conf={"stichtag": "32012024", "wiederanlaufwert": "200"})
        # wait_for_dag_run_completion(dag_run.run_id)
        assert get_dag_run_status("ausd_bp_ta_rn_vertrag_dag", "test_invalid_stichtag_value") == DagRunState.FAILED
    ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG fails.
    2.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_log` with `status = 'FAILED'`.
    3.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_error_log`.
    4.  The `job_error_log` record's `error_message` contains a substring like `'Invalid p_stichtag value: unable to parse DDMMYYYY date '32012024''`.
    5.  The `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure was *not* called.

    ```sql
    -- Assertion 1: Check job_log entry status
    SELECT status FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper';
    -- Expected: 1 row with status = 'FAILED'.

    -- Assertion 2: Check job_error_log entry
    SELECT error_message FROM `gcp-project-id.bq_dataset_name.job_error_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND error_message LIKE '%Invalid p_stichtag value: unable to parse DDMMYYYY date ''32012024''%';
    -- Expected: 1 row with the specific error message.
    ```

---

## Test Case 6: Invalid Wiederanlaufwert (Non-numeric)

*   **Purpose**: Verify that the `ausd_bp_ta_rn_vertrag_wrapper` SP correctly validates `wiederanlaufwert` as numeric and logs an error if it's not.
*   **Setup**: Ensure `job_log` and `job_error_log` tables are empty.
*   **Action**: Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with a non-numeric `wiederanlaufwert`.
    *   `stichtag`: `01012024`
    *   `wiederanlaufwert`: `ABC`
    ```python
    # Example pytest-airflow interaction
    def test_invalid_wiederanlaufwert():
        # Trigger DAG with non-numeric 'wiederanlaufwert'
        # dag_run = trigger_dag("ausd_bp_ta_rn_vertrag_dag", conf={"stichtag": "01012024", "wiederanlaufwert": "ABC"})
        # wait_for_dag_run_completion(dag_run.run_id)
        assert get_dag_run_status("ausd_bp_ta_rn_vertrag_dag", "test_invalid_wiederanlaufwert") == DagRunState.FAILED
    ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG fails.
    2.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_log` with `status = 'FAILED'`.
    3.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_error_log`.
    4.  The `job_error_log` record's `error_message` contains a substring like `'Invalid p_wiederanlaufWert: expected numeric string, got 'ABC''`.
    5.  The `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure was *not* called.

    ```sql
    -- Assertion 1: Check job_log entry status
    SELECT status FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper';
    -- Expected: 1 row with status = 'FAILED'.

    -- Assertion 2: Check job_error_log entry
    SELECT error_message FROM `gcp-project-id.bq_dataset_name.job_error_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND error_message LIKE '%Invalid p_wiederanlaufWert: expected numeric string, got ''ABC''%';
    -- Expected: 1 row with the specific error message.
    ```

---

## Test Case 7: Core Script Failure Handling

*   **Purpose**: Verify that if the downstream `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure fails, the wrapper SP correctly catches the error, logs it, and marks the job as `FAILED`. This mimics the `trap` behavior in the legacy script.
*   **Setup**:
    1.  Ensure `job_log` and `job_error_log` tables are empty.
    2.  **Temporarily modify** the `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure to `RAISE` an error immediately upon invocation.
        ```sql
        -- Modified k_ausd_bp_ta_rn_vertrag for testing failure
        CREATE OR REPLACE PROCEDURE `gcp-project-id.bq_dataset_name.k_ausd_bp_ta_rn_vertrag`(
          p_jobkennung STRING,
          p_stichtag_ddmmyyyy STRING,
          p_eintragsnr INT64,
          p_wiederanlaufWert STRING
        )
        BEGIN
          RAISE USING MESSAGE = 'Simulated failure in k_ausd_bp_ta_rn_vertrag';
        END;
        ```
*   **Action**: Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with valid parameters.
    *   `stichtag`: `01012024`
    *   `wiederanlaufwert`: `123`
    ```python
    # Example pytest-airflow interaction
    def test_core_script_failure():
        # Trigger DAG with valid params, expecting core script to fail
        # dag_run = trigger_dag("ausd_bp_ta_rn_vertrag_dag", conf={"stichtag": "01012024", "wiederanlaufwert": "123"})
        # wait_for_dag_run_completion(dag_run.run_id)
        assert get_dag_run_status("ausd_bp_ta_rn_vertrag_dag", "test_core_script_failure") == DagRunState.FAILED
    ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG fails.
    2.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_log` with `status = 'FAILED'`.
    3.  Exactly one record exists in `gcp-project-id.bq_dataset_name.job_error_log`.
    4.  The `job_error_log` record's `error_message` contains `'Simulated failure in k_ausd_bp_ta_rn_vertrag'`.
    5.  The `job_error_log` record's `eintragsnr` matches the `eintragsnr` in `job_log`.
*   **Cleanup**: Revert `k_ausd_bp_ta_rn_vertrag` to its original placeholder definition.

    ```sql
    -- Assertion 1: Check job_log entry status
    SELECT status FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper';
    -- Expected: 1 row with status = 'FAILED'.

    -- Assertion 2: Check job_error_log entry
    SELECT error_message, eintragsnr FROM `gcp-project-id.bq_dataset_name.job_error_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND error_message LIKE '%Simulated failure in k_ausd_bp_ta_rn_vertrag%';
    -- Expected: 1 row with the specific error message and a valid eintragsnr.

    -- Assertion 3: Verify eintragsnr consistency
    SELECT T1.eintragsnr
    FROM `gcp-project-id.bq_dataset_name.job_log` AS T1
    JOIN `gcp-project-id.bq_dataset_name.job_error_log` AS T2
      ON T1.eintragsnr = T2.eintragsnr
    WHERE T1.jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
      AND T2.jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper';
    -- Expected: 1 row, confirming the eintragsnr matches between log and error log.
    ```

---

## Test Case 8: `job_log` Table Schema and Data Types

*   **Purpose**: Verify that the `job_log` table has the correct schema, column names, data types, and nullability constraints as defined in the migration design.
*   **Setup**: None (relies on BigQuery's `INFORMATION_SCHEMA`).
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `job_log` table.
*   **Pass/Fail Criterion**: The query results match the expected schema:

    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `gcp-project-id.bq_dataset_name.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log'
    ORDER BY ordinal_position;
    ```

    **Expected Output:**

    | column_name    | data_type | is_nullable |
    | :------------- | :-------- | :---------- |
    | eintragsnr     | INT64     | NO          |
    | jobkennung     | STRING    | NO          |
    | stichtag       | DATE      | YES         |
    | wiederanlaufwert | STRING    | YES         |
    | status         | STRING    | YES         |
    | created_at     | TIMESTAMP | NO          |
    | updated_at     | TIMESTAMP | YES         |

---

## Test Case 9: `job_error_log` Table Schema and Data Types

*   **Purpose**: Verify that the `job_error_log` table has the correct schema, column names, data types, and nullability constraints.
*   **Setup**: None.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `job_error_log` table.
*   **Pass/Fail Criterion**: The query results match the expected schema:

    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `gcp-project-id.bq_dataset_name.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_error_log'
    ORDER BY ordinal_position;
    ```

    **Expected Output:**

    | column_name     | data_type | is_nullable |
    | :-------------- | :-------- | :---------- |
    | jobkennung      | STRING    | NO          |
    | eintragsnr      | INT64     | YES         |
    | error_message   | STRING    | NO          |
    | error_stack     | STRING    | YES         |
    | error_statement | STRING    | YES         |
    | created_at      | TIMESTAMP | NO          |

---

## Test Case 10: `eintragsnr` Incrementation and Uniqueness

*   **Purpose**: Verify that the `eintragsnr` (entry number) is correctly incremented and remains unique across multiple job runs, simulating the `DWMSG_ErmittleNr` behavior.
*   **Setup**: Ensure `job_log` and `job_error_log` tables are empty.
*   **Action**:
    1.  Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` with valid parameters (e.g., `stichtag=01012024`, `wiederanlaufwert=1`).
    2.  Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` again with different valid parameters (e.g., `stichtag=02012024`, `wiederanlaufwert=2`).
    3.  Trigger the Airflow DAG `ausd_bp_ta_rn_vertrag_dag` a third time with different valid parameters (e.g., `stichtag=03012024`, `wiederanlaufwert=3`).
    (Ensure each run completes successfully).
*   **Pass/Fail Criterion**:
    1.  All three DAG runs complete successfully.
    2.  Exactly three records exist in `gcp-project-id.bq_dataset_name.job_log`.
    3.  The `eintragsnr` values in `job_log` are sequential and unique (e.g., 1, 2, 3).
    4.  No records exist in `gcp-project-id.bq_dataset_name.job_error_log`.

    ```sql
    -- Assertion 1: Check total row count in job_log
    SELECT COUNT(*) FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper';
    -- Expected: 3 rows.

    -- Assertion 2: Check eintragsnr sequence and uniqueness
    SELECT
        eintragsnr,
        FORMAT_DATE('%d%m%Y', stichtag) AS stichtag_formatted,
        wiederanlaufwert
    FROM `gcp-project-id.bq_dataset_name.job_log`
    WHERE jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'
    ORDER BY eintragsnr;
    -- Expected: 3 rows with eintragsnr 1, 2, 3 and corresponding stichtag/wiederanlaufwert.
    -- The eintragsnr should be strictly increasing.
    ```