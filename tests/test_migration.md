The migration of `r_ausd_v_ta_barrier.ksh` to a BigQuery Stored Procedure `project.dataset.Vertragsdatenabgleich` involves significant architectural changes, moving from shell scripting and file-based logging to BigQuery SQL and table-based logging. The following tests aim to validate the behavioral equivalence and correctness of this migration.

**Assumptions for Testing:**
*   A BigQuery project and dataset (`project.dataset`) exist.
*   The `job_control`, `job_log`, and `job_error_log` tables have been created as per the design document.
*   A mock BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_barrier` exists. This mock will be controlled to simulate success or failure for testing purposes.

**Mock `project.dataset.k_ausd_v_ta_barrier` for Testing:**

```sql
-- This mock procedure allows simulating success or failure of the core logic.
-- For testing, you would either redeploy this with p_simulate_error=TRUE
-- or modify the main procedure to pass this flag.
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_barrier`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr INT64,
  IN p_simulate_error BOOL DEFAULT FALSE
)
BEGIN
  INSERT INTO `project.dataset.job_log`
    (eintrags_nr, job_kennung, log_level, message, created_at)
  VALUES
    (p_eintrags_nr, p_job_kennung, 'DEBUG', CONCAT('k_ausd_v_ta_barrier called with JobKennung=', p_job_kennung, ', DW_EintragsNr=', CAST(p_eintrags_nr AS STRING), ', SimulateError=', CAST(p_simulate_error AS STRING)), CURRENT_TIMESTAMP());

  IF p_simulate_error THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in k_ausd_v_ta_barrier';
  END IF;
END;
```

---

## Test Case 1: Schema and Initial State Verification

*   **Purpose**: Verify that the required BigQuery tables (`job_control`, `job_log`, `job_error_log`) and the core logic mock procedure (`k_ausd_v_ta_barrier`) exist and have the correct schema as defined in the migration design.
*   **Setup**: Ensure the BigQuery project and dataset are accessible. The tables and mock procedure should be deployed.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA` for table and routine metadata.
*   **Pass/Fail Criterion**:
    *   All three tables (`job_control`, `job_log`, `job_error_log`) must exist in `project.dataset`.
    *   Their schemas must match the definitions provided in the design document (column names and data types).
    *   The stored procedure `project.dataset.k_ausd_v_ta_barrier` must exist.

*   **Runnable Test Code (SQL Assertions)**:

    ```sql
    -- Verify job_control table schema
    SELECT
        column_name, data_type
    FROM
        `project.dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'job_control'
    ORDER BY
        ordinal_position;
    /* Expected Output:
    column_name | data_type
    ------------|----------
    eintrags_nr | INT64
    job_kennung | STRING
    script_name | STRING
    log_datei   | STRING
    stichtag_info| STRING
    status      | STRING
    created_at  | TIMESTAMP
    finished_at | TIMESTAMP
    */

    -- Verify job_log table schema
    SELECT
        column_name, data_type
    FROM
        `project.dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'job_log'
    ORDER BY
        ordinal_position;
    /* Expected Output:
    column_name | data_type
    ------------|----------
    eintrags_nr | INT64
    job_kennung | STRING
    log_level   | STRING
    message     | STRING
    created_at  | TIMESTAMP
    */

    -- Verify job_error_log table schema
    SELECT
        column_name, data_type
    FROM
        `project.dataset`.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'job_error_log'
    ORDER BY
        ordinal_position;
    /* Expected Output:
    column_name | data_type
    ------------|----------
    job_kennung | STRING
    eintrags_nr | INT64
    err_nr      | INT64
    err_arg     | STRING
    created_at  | TIMESTAMP
    */

    -- Verify k_ausd_v_ta_barrier procedure existence
    SELECT
        routine_name, routine_type
    FROM
        `project.dataset`.INFORMATION_SCHEMA.ROUTINES
    WHERE
        routine_name = 'k_ausd_v_ta_barrier' AND routine_type = 'PROCEDURE';
    /* Expected Output:
    routine_name         | routine_type
    ---------------------|-------------
    k_ausd_v_ta_barrier | PROCEDURE
    */
    ```

---

## Test Case 2: Successful Execution - Happy Path

*   **Purpose**: Verify that the `Vertragsdatenabgleich` procedure executes successfully when no errors occur, correctly logs job status, and invokes the core logic procedure. This tests output parity (logging) and transformation correctness (orchestration flow).
*   **Setup**:
    1.  Ensure the mock `project.dataset.k_ausd_v_ta_barrier` is configured to succeed (e.g., `p_simulate_error` is `FALSE` or not passed).
    2.  Clear all rows from `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
*   **Action**: Call the main procedure with no parameters.
    ```sql
    CALL `project.dataset.Vertragsdatenabgleich`(NULL, NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_control` table**:
        *   Exactly one row exists.
        *   `job_kennung` is 'BERT_V_TA_BARRIER'.
        *   `status` is 'OK'.
        *   `eintrags_nr` is 1 (for the first run after clearing).
        *   `script_name` is 'Vertragsdatenabgleich'.
        *   `log_datei` matches the pattern `log_BERT_V_TA_BARRIER_1.log`.
        *   `stichtag_info` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
        *   `created_at` and `finished_at` are populated and `finished_at` is after `created_at`.
    2.  **`job_log` table**:
        *   At least two rows exist (one for `k_ausd_v_ta_barrier` invocation, one for the success message).
        *   One row with `log_level='DEBUG'` and message indicating `k_ausd_v_ta_barrier` was called with `JobKennung='BERT_V_TA_BARRIER'` and `DW_EintragsNr=1`.
        *   One row with `log_level='INFO'` and `message='Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
    3.  **`job_error_log` table**:
        *   Zero rows exist.

*   **Runnable Test Code (pytest with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    import datetime

    PROJECT_ID = "your-gcp-project-id"
    DATASET_ID = "your_dataset_id"
    JOB_CONTROL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_control"
    JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
    JOB_ERROR_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_error_log"
    MAIN_PROCEDURE = f"{PROJECT_ID}.{DATASET_ID}.Vertragsdatenabgleich"
    CORE_PROCEDURE = f"{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_barrier"

    @pytest.fixture(scope="module")
    def bigquery_client():
        return bigquery.Client(project=PROJECT_ID)

    @pytest.fixture(autouse=True)
    def setup_and_teardown_tables(bigquery_client):
        # Clear tables before each test
        bigquery_client.query(f"TRUNCATE TABLE `{JOB_CONTROL_TABLE}`").result()
        bigquery_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
        bigquery_client.query(f"TRUNCATE TABLE `{JOB_ERROR_LOG_TABLE}`").result()
        yield

    def execute_bq_procedure(client, procedure_name, params):
        param_str = ", ".join([f"'{p}'" if isinstance(p, str) else str(p) for p in params])
        query = f"CALL `{procedure_name}`({param_str});"
        try:
            job = client.query(query)
            job.result()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_table_data(client, table_id):
        query = f"SELECT * FROM `{table_id}` ORDER BY created_at ASC;"
        rows = client.query(query).result()
        return [dict(row) for row in rows]

    def test_successful_execution(bigquery_client):
        # Action
        success, error_message = execute_bq_procedure(bigquery_client, MAIN_PROCEDURE, [None, None, None])
        assert success, f"Procedure call failed: {error_message}"

        # Assertions
        job_control_data = get_table_data(bigquery_client, JOB_CONTROL_TABLE)
        job_log_data = get_table_data(bigquery_client, JOB_LOG_TABLE)
        job_error_log_data = get_table_data(bigquery_client, JOB_ERROR_LOG_TABLE)

        # 1. job_control table
        assert len(job_control_data) == 1
        control_entry = job_control_data[0]
        assert control_entry['job_kennung'] == 'BERT_V_TA_BARRIER'
        assert control_entry['status'] == 'OK'
        assert control_entry['eintrags_nr'] == 1
        assert control_entry['script_name'] == 'Vertragsdatenabgleich'
        assert control_entry['log_datei'] == 'log_BERT_V_TA_BARRIER_1.log'
        assert control_entry['stichtag_info'] == datetime.date.today().strftime('%d%m%Y')
        assert control_entry['created_at'] is not None
        assert control_entry['finished_at'] is not None
        assert control_entry['finished_at'] > control_entry['created_at']

        # 2. job_log table
        assert len(job_log_data) >= 2 # At least core script debug and success message
        core_log_found = False
        success_log_found = False
        for log_entry in job_log_data:
            if 'k_ausd_v_ta_barrier called' in log_entry['message'] and log_entry['log_level'] == 'DEBUG':
                core_log_found = True
                assert log_entry['eintrags_nr'] == 1
                assert log_entry['job_kennung'] == 'BERT_V_TA_BARRIER'
            if 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' in log_entry['message'] and log_entry['log_level'] == 'INFO':
                success_log_found = True
                assert log_entry['eintrags_nr'] == 1
                assert log_entry['job_kennung'] == 'BERT_V_TA_BARRIER'
        assert core_log_found, "Core script invocation log not found."
        assert success_log_found, "Success message log not found."

        # 3. job_error_log table
        assert len(job_error_log_data) == 0, "job_error_log should be empty on success."
    ```

---

## Test Case 3: Parameter Handling - Help Flag (`-h`)

*   **Purpose**: Verify that the procedure correctly handles the `-h` parameter by displaying the usage text and exiting immediately without performing any job processing or logging. This tests transformation correctness (parameter parsing) and output parity (usage message).
*   **Setup**: Clear all rows from `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
*   **Action**: Call the main procedure with `p_h = '-h'`.
    ```sql
    CALL `project.dataset.Vertragsdatenabgleich`('-h', NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure call should complete successfully (it uses `LEAVE` not `SIGNAL ERROR`).
    2.  The result set returned by the `CALL` statement should contain the `usage_text`.
    3.  **`job_control` table**: Zero rows exist.
    4.  **`job_log` table**: Zero rows exist.
    5.  **`job_error_log` table**: Zero rows exist.
    6.  The mock `k_ausd_v_ta_barrier` should *not* have been called.

*   **Runnable Test Code (pytest with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bigquery_client, setup_and_teardown_tables, get_table_data, etc. from previous test) ...

    def execute_bq_procedure_with_result(client, procedure_name, params):
        param_str = ", ".join([f"'{p}'" if isinstance(p, str) else str(p) for p in params])
        query = f"CALL `{procedure_name}`({param_str});"
        try:
            job = client.query(query)
            # For procedures that return a result set (like usage_text), we need to fetch it
            rows = list(job.result())
            return True, rows, None
        except Exception as e:
            return False, [], str(e)

    def test_parameter_help_flag(bigquery_client):
        # Action
        success, results, error_message = execute_bq_procedure_with_result(bigquery_client, MAIN_PROCEDURE, ['-h', None, None])
        assert success, f"Procedure call failed: {error_message}"

        # Assertions
        assert len(results) == 1
        assert 'usage' in results[0]
        assert 'Programm: Vertragsdatenabgleich' in results[0]['usage']
        assert 'Parameter:' in results[0]['usage']
        assert '-h     zeigt diese Seite an' in results[0]['usage']

        job_control_data = get_table_data(bigquery_client, JOB_CONTROL_TABLE)
        job_log_data = get_table_data(bigquery_client, JOB_LOG_TABLE)
        job_error_log_data = get_table_data(bigquery_client, JOB_ERROR_LOG_TABLE)

        assert len(job_control_data) == 0, "job_control should be empty for -h flag."
        assert len(job_log_data) == 0, "job_log should be empty for -h flag."
        assert len(job_error_log_data) == 0, "job_error_log should be empty for -h flag."
    ```

---

## Test Case 4: Parameter Handling - Unused/Invalid Parameters (Behavioral Discrepancy)

*   **Purpose**: Highlight and document the behavioral difference in parameter handling between the legacy KornShell script (using `getopts` for robust validation) and the migrated BigQuery procedure. The legacy script would report errors for unknown flags or missing arguments for expected parameters (`-s:`, `-l:`). The migrated procedure, as designed, does not implement this level of validation for `p_s` and `p_l`.
*   **Setup**: Clear all rows from `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log`. Ensure `k_ausd_v_ta_barrier` mock is configured to succeed.
*   **Action**:
    1.  Call the procedure with an unused parameter that would require an argument in the legacy script: `CALL project.dataset.Vertragsdatenabgleich(NULL, 'some_value_for_s', NULL);`
    2.  Call the procedure with an unknown flag: `CALL project.dataset.Vertragsdatenabgleich(NULL, NULL, 'invalid_flag');` (assuming `p_l` is used for this, or an additional parameter if the procedure allowed it).
*   **Pass/Fail Criterion**:
    *   **Legacy Behavior (Expected for Parity)**:
        *   `r_ausd_v_ta_barrier.ksh -s` would result in `ErrNr=193` (missing argument for -s).
        *   `r_ausd_v_ta_barrier.ksh -x` (unknown flag) would result in `ErrNr=192`.
        *   Both would log to `job_error_log` and `exit` with the error code.
    *   **Migrated Behavior (Observed)**:
        *   The BigQuery procedure will execute successfully for both actions above.
        *   It will ignore the values passed to `p_s` and `p_l` as they are not used in the procedure's logic.
        *   No error will be raised, and the job will complete as if no parameters were passed (i.e., `status='OK'`, `job_control` and `job_log` populated as in a happy path).
    *   **Pass/Fail**: This test *fails* if strict behavioral equivalence for parameter validation is required. It *passes* if this deviation is accepted and documented. The current design implies this deviation is accepted.

*   **Runnable Test Code (pytest with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bigquery_client, setup_and_teardown_tables, execute_bq_procedure, get_table_data, etc.) ...

    def test_parameter_unused_or_invalid_behavior(bigquery_client):
        # Action 1: Call with a value for p_s (which is unused in the BQ SP)
        success_s, error_message_s = execute_bq_procedure(bigquery_client, MAIN_PROCEDURE, [None, 'some_value_for_s', None])
        assert success_s, f"Procedure call with p_s failed unexpectedly: {error_message_s}"

        # Verify job completed successfully, indicating parameter was ignored
        job_control_data_s = get_table_data(bigquery_client, JOB_CONTROL_TABLE)
        assert len(job_control_data_s) == 1
        assert job_control_data_s[0]['status'] == 'OK'
        assert len(get_table_data(bigquery_client, JOB_ERROR_LOG_TABLE)) == 0

        # Clear for next action
        bigquery_client.query(f"TRUNCATE TABLE `{JOB_CONTROL_TABLE}`").result()
        bigquery_client.query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`").result()
        bigquery_client.query(f"TRUNCATE TABLE `{JOB_ERROR_LOG_TABLE}`").result()

        # Action 2: Call with a value for p_l (which is unused in the BQ SP)
        success_l, error_message_l = execute_bq_procedure(bigquery_client, MAIN_PROCEDURE, [None, None, 'some_value_for_l'])
        assert success_l, f"Procedure call with p_l failed unexpectedly: {error_message_l}"

        # Verify job completed successfully, indicating parameter was ignored
        job_control_data_l = get_table_data(bigquery_client, JOB_CONTROL_TABLE)
        assert len(job_control_data_l) == 1
        assert job_control_data_l[0]['status'] == 'OK'
        assert len(get_table_data(bigquery_client, JOB_ERROR_LOG_TABLE)) == 0

        # Documentation of Discrepancy:
        # The legacy ksh script would have failed with ErrNr=193 (missing argument)
        # if called as `r_ausd_v_ta_barrier.ksh -s` or `r_ausd_v_ta_barrier.ksh -l`.
        # It would also fail with ErrNr=192 for unknown flags like `r_ausd_v_ta_barrier.ksh -x`.
        # The BigQuery procedure, as implemented, does not perform this validation
        # for p_s or p_l, nor for unknown flags. This is a known behavioral difference.
    ```

---

## Test Case 5: Error in Core Logic (`k_ausd_v_ta_barrier`)

*   **Purpose**: Verify that the wrapper procedure correctly handles errors originating from the called core procedure (`k_ausd_v_ta_barrier`), updates the job status to 'ERROR', logs the error message, and signals a failure. This tests transformation correctness (error handling).
*   **Setup**:
    1.  **Crucially**: Modify the mock `project.dataset.k_ausd_v_ta_barrier` to simulate an error. This can be done by redeploying it with `p_simulate_error` set to `TRUE` or by temporarily altering its definition to `SIGNAL SQLSTATE '45000'` unconditionally.
    2.  Clear all rows from `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
*   **Action**: Call the main procedure with no parameters.
    ```sql
    CALL `project.dataset.Vertragsdatenabgleich`(NULL, NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure call should *fail* and `SIGNAL SQLSTATE '45000'` with `MESSAGE_TEXT = 'AppError: Abbruch'`.
    2.  **`job_control` table**:
        *   Exactly one row exists.
        *   `job_kennung` is 'BERT_V_TA_BARRIER'.
        *   `status` is 'ERROR'.
        *   `eintrags_nr` is 1.
        *   `finished_at` is populated.
    3.  **`job_log` table**:
        *   At least two rows exist (one for `k_ausd_v_ta_barrier` invocation, one for the wrapper's error message).
        *   One row with `log_level='DEBUG'` indicating `k_ausd_v_ta_barrier` was called (and likely indicating `SimulateError=TRUE`).
        *   One row with `log_level='ERROR'` and `message='AppError: Abbruch'`.
    4.  **`job_error_log` table**:
        *   Zero rows exist. (This is a **behavioral discrepancy**: The legacy `trap ERR` would likely have invoked `DWMSG_Fehlerbehandlung` which would log to the error log. The BigQuery procedure's `EXCEPTION WHEN ERROR THEN` block only logs to `job_log` and updates `job_control`, but does not insert into `job_error_log` for errors originating from the core script.)

*   **Runnable Test Code (pytest with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bigquery_client, setup_and_teardown_tables, get_table_data, etc.) ...

    # Helper to temporarily modify the mock core procedure for error simulation
    def deploy_mock_core_procedure(client, simulate_error=False):
        mock_sql = f"""
        CREATE OR REPLACE PROCEDURE `{CORE_PROCEDURE}`(
          IN p_job_kennung STRING,
          IN p_eintrags_nr INT64,
          IN p_simulate_error BOOL DEFAULT FALSE
        )
        BEGIN
          INSERT INTO `{JOB_LOG_TABLE}`
            (eintrags_nr, job_kennung, log_level, message, created_at)
          VALUES
            (p_eintrags_nr, p_job_kennung, 'DEBUG', CONCAT('k_ausd_v_ta_barrier called with JobKennung=', p_job_kennung, ', DW_EintragsNr=', CAST(p_eintrags_nr AS STRING), ', SimulateError=', CAST(p_simulate_error AS STRING)), CURRENT_TIMESTAMP());

          IF {simulate_error} THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in k_ausd_v_ta_barrier';
          END IF;
        END;
        """
        client.query(mock_sql).result()

    def test_error_in_core_logic(bigquery_client):
        # Setup: Deploy mock core procedure to simulate error
        deploy_mock_core_procedure(bigquery_client, simulate_error=True)

        # Action
        success, error_message = execute_bq_procedure(bigquery_client, MAIN_PROCEDURE, [None, None, None])
        assert not success, "Procedure call should have failed due to core logic error."
        assert 'AppError: Abbruch' in error_message

        # Assertions
        job_control_data = get_table_data(bigquery_client, JOB_CONTROL_TABLE)
        job_log_data = get_table_data(bigquery_client, JOB_LOG_TABLE)
        job_error_log_data = get_table_data(bigquery_client, JOB_ERROR_LOG_TABLE)

        # 1. job_control table
        assert len(job_control_data) == 1
        control_entry = job_control_data[0]
        assert control_entry['job_kennung'] == 'BERT_V_TA_BARRIER'
        assert control_entry['status'] == 'ERROR'
        assert control_entry['eintrags_nr'] == 1
        assert control_entry['finished_at'] is not None

        # 2. job_log table
        assert len(job_log_data) >= 2 # Core script debug and wrapper error message
        core_log_found = False
        error_log_found = False
        for log_entry in job_log_data:
            if 'k_ausd_v_ta_barrier called' in log_entry['message'] and log_entry['log_level'] == 'DEBUG':
                core_log_found = True
            if 'AppError: Abbruch' in log_entry['message'] and log_entry['log_level'] == 'ERROR':
                error_log_found = True
        assert core_log_found, "Core script invocation log not found."
        assert error_log_found, "Wrapper error message log not found."

        # 3. job_error_log table
        assert len(job_error_log_data) == 0, "job_error_log should be empty for core script errors in current BQ design."

        # Teardown: Restore mock core procedure to default (succeed)
        deploy_mock_core_procedure(bigquery_client, simulate_error=False)
    ```

---

## Test Case 6: `DW_EintragsNr` Generation and `LogDatei` Naming

*   **Purpose**: Verify that the `eintrags_nr` is correctly incremented for subsequent job runs and that the `log_datei` name is generated with the correct `eintrags_nr` and `JobKennung`. This tests transformation correctness (sequence generation and string manipulation).
*   **Setup**:
    1.  Ensure the mock `project.dataset.k_ausd_v_ta_barrier` is configured to succeed.
    2.  Clear all rows from `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
*   **Action**: Call the main procedure twice consecutively.
    ```sql
    CALL `project.dataset.Vertragsdatenabgleich`(NULL, NULL, NULL); -- First run
    CALL `project.dataset.Vertragsdatenabgleich`(NULL, NULL, NULL); -- Second run
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_control` table**:
        *   Exactly two rows exist.
        *   The first row has `eintrags_nr=1` and `log_datei='log_BERT_V_TA_BARRIER_1.log'`.
        *   The second row has `eintrags_nr=2` and `log_datei='log_BERT_V_TA_BARRIER_2.log'`.
        *   Both rows have `status='OK'`.
    2.  **`job_log` table**:
        *   Contains log entries corresponding to both runs, correctly associated with `eintrags_nr=1` and `eintrags_nr=2`.
    3.  **`job_error_log` table**: Zero rows exist.

*   **Runnable Test Code (pytest with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    import datetime

    # ... (bigquery_client, setup_and_teardown_tables, execute_bq_procedure, get_table_data, etc.) ...

    def test_eintragsnr_and_logdatei_generation(bigquery_client):
        # Setup: Ensure mock core procedure succeeds
        deploy_mock_core_procedure(bigquery_client, simulate_error=False)

        # Action 1: First run
        success1, error_message1 = execute_bq_procedure(bigquery_client, MAIN_PROCEDURE, [None, None, None])
        assert success1, f"First procedure call failed: {error_message1}"

        # Action 2: Second run
        success2, error_message2 = execute_bq_procedure(bigquery_client, MAIN_PROCEDURE, [None, None, None])
        assert success2, f"Second procedure call failed: {error_message2}"

        # Assertions
        job_control_data = get_table_data(bigquery_client, JOB_CONTROL_TABLE)
        job_log_data = get_table_data(bigquery_client, JOB_LOG_TABLE)
        job_error_log_data = get_table_data(bigquery_client, JOB_ERROR_LOG_TABLE)

        # 1. job_control table
        assert len(job_control_data) == 2

        # First entry
        entry1 = job_control_data[0]
        assert entry1['eintrags_nr'] == 1
        assert entry1['job_kennung'] == 'BERT_V_TA_BARRIER'
        assert entry1['status'] == 'OK'
        assert entry1['log_datei'] == 'log_BERT_V_TA_BARRIER_1.log'

        # Second entry
        entry2 = job_control_data[1]
        assert entry2['eintrags_nr'] == 2
        assert entry2['job_kennung'] == 'BERT_V_TA_BARRIER'
        assert entry2['status'] == 'OK'
        assert entry2['log_datei'] == 'log_BERT_V_TA_BARRIER_2.log'

        # 2. job_log table
        # Verify log entries are correctly associated with their eintrags_nr
        logs_for_1 = [log for log in job_log_data if log['eintrags_nr'] == 1]
        logs_for_2 = [log for log in job_log_data if log['eintrags_nr'] == 2]

        assert len(logs_for_1) >= 2 # Core script debug and success message
        assert any('k_ausd_v_ta_barrier called' in l['message'] for l in logs_for_1)
        assert any('Die Abarbeitung wurde ohne erkennbare Fehler beendet' in l['message'] for l in logs_for_1)

        assert len(logs_for_2) >= 2 # Core script debug and success message
        assert any('k_ausd_v_ta_barrier called' in l['message'] for l in logs_for_2)
        assert any('Die Abarbeitung wurde ohne erkennbare Fehler beendet' in l['message'] for l in logs_for_2)

        # 3. job_error_log table
        assert len(job_error_log_data) == 0, "job_error_log should be empty on successful runs."
    ```