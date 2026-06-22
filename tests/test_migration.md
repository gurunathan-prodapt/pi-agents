As a senior data-migration QA engineer, I have designed the following tests to validate the migration of the KornShell script `r_ausd_v_ta_vertrag_tmp.ksh` to BigQuery Stored Procedures. These tests focus on ensuring behavioral equivalence, correct transformation logic, proper external system replacements (specifically, the logging mechanism), and data quality assertions for the new audit log table.

The tests assume the BigQuery dataset is named `project.dataset` and that the necessary BigQuery Stored Procedures (`sp_vertragsdatenabgleich`, `sp_k_ausd_v_ta_vertrag_tmp`) and the `job_audit_log` table have been deployed.

---

## Test Case 1: Successful Execution with Default Parameters

*   **Purpose**: Verify that the migrated job runs successfully when invoked with default parameters, correctly orchestrates the core procedure, and logs all execution steps and the final status as 'SUCCESS' in the `job_audit_log` table. This covers output parity and transformation correctness for the happy path.

*   **Setup**:
    1.  Ensure the `project.dataset.job_audit_log` table exists and is empty.
    2.  Ensure the `project.dataset.sp_k_ausd_v_ta_vertrag_tmp` stored procedure is deployed in its default, successful placeholder state.
    3.  Ensure the `project.dataset.sp_vertragsdatenabgleich` stored procedure is deployed.

*   **Action**:
    Execute the wrapper stored procedure without providing any parameters, allowing it to use its defaults.

    ```sql
    CALL project.dataset.sp_vertragsdatenabgleich(NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully without raising any errors.
    2.  Querying the `job_audit_log` table for the most recent run (identified by `job_run_id`) should return exactly 4 entries.
    3.  The entries must reflect the correct sequence and status:
        *   One entry for `sp_vertragsdatenabgleich` start: `status='RUNNING'`, `message` indicating wrapper start.
        *   One entry for `sp_k_ausd_v_ta_vertrag_tmp` start: `status='RUNNING'`, `message` indicating core start.
        *   One entry for `sp_k_ausd_v_ta_vertrag_tmp` end: `status='SUCCESS'`, `message` indicating core completion.
        *   One entry for `sp_vertragsdatenabgleich` end: `status='SUCCESS'`, `message` indicating wrapper completion.
    4.  All entries for `job_name` must be 'BERT_V_TA_VERTRAG_TMP'.
    5.  All entries for `stichtag` must match `CURRENT_DATE()` at the time of execution.
    6.  `start_time` and `end_time` columns must be populated appropriately, and `error_message` must be `NULL` for all entries.
    7.  The `job_run_id` must be consistent across all 4 entries.

    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import date

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"

    def test_successful_execution_default_params():
        # Clean up previous runs
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()

        # Action: Call the stored procedure
        query_job = client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(NULL, NULL)")
        query_job.result() # Wait for the procedure to complete

        # Pass/Fail: Assertions
        results = client.query(f"""
            SELECT job_name, job_run_id, status, message, stichtag, error_message,
                   FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', start_time) as start_time_fmt,
                   FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', end_time) as end_time_fmt
            FROM `{project_id}.{dataset_id}.job_audit_log`
            ORDER BY log_timestamp
        """).result()

        entries = list(results)
        assert len(entries) == 4, f"Expected 4 log entries, got {len(entries)}"

        # Extract job_run_id for consistency check
        job_run_id = entries[0].job_run_id
        for entry in entries:
            assert entry.job_run_id == job_run_id, "job_run_id mismatch across entries"

        # Assert specific entry details
        assert entries[0].job_name == 'BERT_V_TA_VERTRAG_TMP'
        assert entries[0].status == 'RUNNING'
        assert 'Wrapper procedure sp_vertragsdatenabgleich started.' in entries[0].message
        assert entries[0].stichtag == date.today()
        assert entries[0].error_message is None
        assert entries[0].start_time_fmt is not None
        assert entries[0].end_time_fmt is None # Wrapper start should not have end_time

        assert entries[1].job_name == 'BERT_V_TA_VERTRAG_TMP'
        assert entries[1].status == 'RUNNING'
        assert 'Core procedure sp_k_ausd_v_ta_vertrag_tmp started' in entries[1].message
        assert entries[1].stichtag == date.today()
        assert entries[1].error_message is None
        assert entries[1].start_time_fmt is not None
        assert entries[1].end_time_fmt is None # Core start should not have end_time

        assert entries[2].job_name == 'BERT_V_TA_VERTRAG_TMP'
        assert entries[2].status == 'SUCCESS'
        assert 'Core procedure sp_k_ausd_v_ta_vertrag_tmp completed successfully' in entries[2].message
        assert entries[2].stichtag == date.today()
        assert entries[2].error_message is None
        assert entries[2].start_time_fmt is not None # Should be the start_time of the core procedure
        assert entries[2].end_time_fmt is not None # Core end should have end_time

        assert entries[3].job_name == 'BERT_V_TA_VERTRAG_TMP'
        assert entries[3].status == 'SUCCESS'
        assert 'Wrapper procedure sp_vertragsdatenabgleich completed successfully' in entries[3].message
        assert entries[3].stichtag == date.today()
        assert entries[3].error_message is None
        assert entries[3].start_time_fmt is not None # Should be the start_time of the wrapper procedure
        assert entries[3].end_time_fmt is not None # Wrapper end should have end_time
    ```

---

## Test Case 2: Successful Execution with Custom Parameters

*   **Purpose**: Verify that the migrated job correctly accepts and utilizes custom `job_name` and `stichtag` parameters, ensuring these values are accurately reflected in the `job_audit_log`. This validates parameter handling and output parity.

*   **Setup**:
    1.  Ensure the `project.dataset.job_audit_log` table exists and is empty.
    2.  Ensure `sp_k_ausd_v_ta_vertrag_tmp` is in its default, successful placeholder state.
    3.  Ensure `sp_vertragsdatenabgleich` is deployed.

*   **Action**:
    Execute the wrapper stored procedure with specific `job_name` and `stichtag` values.

    ```sql
    CALL project.dataset.sp_vertragsdatenabgleich('MY_CUSTOM_JOB_NAME', '2023-01-15');
    ```

*   **Pass/Fail Criterion**:
    1.  The `CALL` statement completes successfully without raising any errors.
    2.  Querying the `job_audit_log` table for the most recent run should return exactly 4 entries, all with `status='SUCCESS'` for the wrapper and core completion.
    3.  All entries for `job_name` must be 'MY_CUSTOM_JOB_NAME'.
    4.  All entries for `stichtag` must be `DATE('2023-01-15')`.
    5.  The `job_run_id` must be consistent across all 4 entries.
    6.  `error_message` must be `NULL` for all entries.

    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import date

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"

    def test_successful_execution_custom_params():
        # Clean up previous runs
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()

        custom_job_name = 'MY_CUSTOM_JOB_NAME'
        custom_stichtag = date(2023, 1, 15)

        # Action: Call the stored procedure with custom parameters
        query_job = client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`('{custom_job_name}', '{custom_stichtag.isoformat()}')")
        query_job.result()

        # Pass/Fail: Assertions
        results = client.query(f"""
            SELECT job_name, job_run_id, status, message, stichtag, error_message
            FROM `{project_id}.{dataset_id}.job_audit_log`
            ORDER BY log_timestamp
        """).result()

        entries = list(results)
        assert len(entries) == 4, f"Expected 4 log entries, got {len(entries)}"

        job_run_id = entries[0].job_run_id
        for entry in entries:
            assert entry.job_run_id == job_run_id, "job_run_id mismatch across entries"
            assert entry.job_name == custom_job_name, f"Expected job_name '{custom_job_name}', got '{entry.job_name}'"
            assert entry.stichtag == custom_stichtag, f"Expected stichtag '{custom_stichtag}', got '{entry.stichtag}'"
            assert entry.error_message is None, "error_message should be NULL for successful run"

        assert entries[0].status == 'RUNNING' and 'Wrapper procedure sp_vertragsdatenabgleich started.' in entries[0].message
        assert entries[1].status == 'RUNNING' and 'Core procedure sp_k_ausd_v_ta_vertrag_tmp started' in entries[1].message
        assert entries[2].status == 'SUCCESS' and 'Core procedure sp_k_ausd_v_ta_vertrag_tmp completed successfully' in entries[2].message
        assert entries[3].status == 'SUCCESS' and 'Wrapper procedure sp_vertragsdatenabgleich completed successfully' in entries[3].message
    ```

---

## Test Case 3: Core Script Failure Handling

*   **Purpose**: Verify that if the core processing script (`sp_k_ausd_v_ta_vertrag_tmp`) fails, the wrapper script (`sp_vertragsdatenabgleich`) correctly captures the error, logs the failure status for both the core and wrapper procedures, and propagates the error to the caller. This validates transformation correctness for error handling.

*   **Setup**:
    1.  Ensure the `project.dataset.job_audit_log` table exists and is empty.
    2.  **Modify `sp_k_ausd_v_ta_vertrag_tmp` to intentionally `RAISE` an error.** This simulates a failure in the core data processing logic.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`(
          IN p_job_name STRING,
          IN p_job_run_id STRING
        )
        BEGIN
          INSERT INTO `project.dataset.job_audit_log` (job_name, job_run_id, start_time, status, message)
          VALUES (p_job_name, p_job_run_id, CURRENT_TIMESTAMP(), 'RUNNING', 'Core procedure sp_k_ausd_v_ta_vertrag_tmp started (placeholder).');

          RAISE USING MESSAGE = 'Simulated core script failure!'; -- INTENTIONAL FAILURE POINT

        EXCEPTION WHEN ERROR THEN
          INSERT INTO `project.dataset.job_audit_log` (job_name, job_run_id, end_time, status, error_message)
          VALUES (p_job_name, p_job_run_id, CURRENT_TIMESTAMP(), 'FAILED', @@error.message);
          RAISE USING MESSAGE = 'Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: ' || @@error.message;
        END;
        ```
    3.  Ensure `sp_vertragsdatenabgleich` is deployed.

*   **Action**:
    Execute the wrapper stored procedure with default parameters.

    ```sql
    CALL project.dataset.sp_vertragsdatenabgleich(NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The `CALL` statement for `sp_vertragsdatenabgleich` **must fail** and return an error message. The error message should contain text similar to: "Job BERT_V_TA_VERTRAG_TMP (Run ID: ...) FAILED: Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: Simulated core script failure!".
    2.  Querying the `job_audit_log` table for the most recent run should return exactly 4 entries.
    3.  The entries must reflect the correct sequence and status:
        *   One entry for `sp_vertragsdatenabgleich` start: `status='RUNNING'`.
        *   One entry for `sp_k_ausd_v_ta_vertrag_tmp` start: `status='RUNNING'`.
        *   One entry for `sp_k_ausd_v_ta_vertrag_tmp` end: `status='FAILED'`, `error_message` containing "Simulated core script failure!".
        *   One entry for `sp_vertragsdatenabgleich` end: `status='FAILED'`, `error_message` containing "Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: Simulated core script failure!".
    4.  The `end_time` for the failed core and wrapper entries must be populated.
    5.  The `job_run_id` must be consistent across all 4 entries.

    ```python
    import pytest
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPIError

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"

    # Helper to deploy the failing core procedure
    def deploy_failing_core_procedure():
        failing_core_sql = f"""
        CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_k_ausd_v_ta_vertrag_tmp`(
          IN p_job_name STRING,
          IN p_job_run_id STRING
        )
        BEGIN
          INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, status, message)
          VALUES (p_job_name, p_job_run_id, CURRENT_TIMESTAMP(), 'RUNNING', 'Core procedure sp_k_ausd_v_ta_vertrag_tmp started (placeholder).');

          RAISE USING MESSAGE = 'Simulated core script failure!';

        EXCEPTION WHEN ERROR THEN
          INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, end_time, status, error_message)
          VALUES (p_job_name, p_job_run_id, CURRENT_TIMESTAMP(), 'FAILED', @@error.message);
          RAISE USING MESSAGE = 'Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: ' || @@error.message;
        END;
        """
        client.query(failing_core_sql).result()

    # Helper to deploy the successful core procedure (for cleanup/other tests)
    def deploy_successful_core_procedure():
        successful_core_sql = f"""
        CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_k_ausd_v_ta_vertrag_tmp`(
          IN p_job_name STRING,
          IN p_job_run_id STRING
        )
        BEGIN
          INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, status, message)
          VALUES (p_job_name, p_job_run_id, CURRENT_TIMESTAMP(), 'RUNNING', 'Core procedure sp_k_ausd_v_ta_vertrag_tmp started (placeholder).');

          INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, end_time, status, message)
          VALUES (p_job_name, p_job_run_id, CURRENT_TIMESTAMP(), 'SUCCESS', 'Core procedure sp_k_ausd_v_ta_vertrag_tmp completed successfully (placeholder).');

        EXCEPTION WHEN ERROR THEN
          INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, end_time, status, error_message)
          VALUES (p_job_name, p_job_run_id, CURRENT_TIMESTAMP(), 'FAILED', @@error.message);
          RAISE USING MESSAGE = 'Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: ' || @@error.message;
        END;
        """
        client.query(successful_core_sql).result()

    def test_core_script_failure_handling():
        # Clean up previous runs
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()

        # Setup: Deploy the failing core procedure
        deploy_failing_core_procedure()

        # Action: Call the wrapper stored procedure
        with pytest.raises(GoogleAPIError) as excinfo:
            client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(NULL, NULL)").result()

        # Pass/Fail: Assertions on error message
        assert "Job BERT_V_TA_VERTRAG_TMP (Run ID:" in str(excinfo.value)
        assert "FAILED: Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: Simulated core script failure!" in str(excinfo.value)

        # Assertions on log entries
        results = client.query(f"""
            SELECT job_name, job_run_id, status, message, error_message,
                   FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', start_time) as start_time_fmt,
                   FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', end_time) as end_time_fmt
            FROM `{project_id}.{dataset_id}.job_audit_log`
            ORDER BY log_timestamp
        """).result()

        entries = list(results)
        assert len(entries) == 4, f"Expected 4 log entries, got {len(entries)}"

        job_run_id = entries[0].job_run_id
        for entry in entries:
            assert entry.job_run_id == job_run_id, "job_run_id mismatch across entries"
            assert entry.job_name == 'BERT_V_TA_VERTRAG_TMP'

        assert entries[0].status == 'RUNNING' and 'Wrapper procedure sp_vertragsdatenabgleich started.' in entries[0].message
        assert entries[0].error_message is None

        assert entries[1].status == 'RUNNING' and 'Core procedure sp_k_ausd_v_ta_vertrag_tmp started' in entries[1].message
        assert entries[1].error_message is None

        assert entries[2].status == 'FAILED'
        assert 'Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: Simulated core script failure!' in entries[2].error_message
        assert entries[2].end_time_fmt is not None

        assert entries[3].status == 'FAILED'
        assert 'Wrapper procedure sp_vertragsdatenabgleich failed due to an error.' in entries[3].message
        assert 'Core procedure sp_k_ausd_v_ta_vertrag_tmp failed: Simulated core script failure!' in entries[3].error_message
        assert entries[3].end_time_fmt is not None

        # Cleanup: Re-deploy the successful core procedure for subsequent tests
        deploy_successful_core_procedure()
    ```

---

## Test Case 4: Wrapper Script Internal Failure Handling

*   **Purpose**: Verify that if the wrapper script (`sp_vertragsdatenabgleich`) itself encounters an internal error (e.g., a DML error or explicit `RAISE` before calling the core script), it correctly logs its own failure and propagates the error to the caller. This validates transformation correctness for robust error handling.

*   **Setup**:
    1.  Ensure the `project.dataset.job_audit_log` table exists and is empty.
    2.  **Modify `sp_vertragsdatenabgleich` to intentionally `RAISE` an error** after its initial log entry but before calling the core procedure.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich`(
          IN p_job_name_param STRING,
          IN p_stichtag_param DATE
        )
        BEGIN
          DECLARE v_job_name STRING DEFAULT COALESCE(p_job_name_param, 'BERT_V_TA_VERTRAG_TMP');
          DECLARE v_job_run_id STRING;
          DECLARE v_start_time TIMESTAMP;
          DECLARE v_end_time TIMESTAMP;
          DECLARE v_status STRING;
          DECLARE v_message STRING;
          DECLARE v_error_message STRING;
          DECLARE v_stichtag DATE DEFAULT COALESCE(p_stichtag_param, CURRENT_DATE());

          SET v_job_run_id = GENERATE_UUID();
          SET v_start_time = CURRENT_TIMESTAMP();
          SET v_status = 'RUNNING';
          SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich started.';

          INSERT INTO `project.dataset.job_audit_log` (job_name, job_run_id, start_time, status, message, stichtag)
          VALUES (v_job_name, v_job_run_id, v_start_time, v_status, v_message, v_stichtag);

          RAISE USING MESSAGE = 'Simulated wrapper script internal failure!'; -- INTENTIONAL FAILURE POINT

          BEGIN
            -- CALL `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`(...); -- This line will not be reached
            -- ... rest of the successful completion logic
          EXCEPTION WHEN ERROR THEN
            -- ... error handling
          END;
        END;
        ```
    3.  Ensure `sp_k_ausd_v_ta_vertrag_tmp` is in its default, successful placeholder state (though it won't be called).

*   **Action**:
    Execute the wrapper stored procedure with default parameters.

    ```sql
    CALL project.dataset.sp_vertragsdatenabgleich(NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The `CALL` statement for `sp_vertragsdatenabgleich` **must fail** and return an error message. The error message should contain text similar to: "Job BERT_V_TA_VERTRAG_TMP (Run ID: ...) FAILED: Simulated wrapper script internal failure!".
    2.  Querying the `job_audit_log` table for the most recent run should return exactly 2 entries.
    3.  The entries must reflect:
        *   One entry for `sp_vertragsdatenabgleich` start: `status='RUNNING'`.
        *   One entry for `sp_vertragsdatenabgleich` end: `status='FAILED'`, `message` indicating wrapper failure, and `error_message` containing "Simulated wrapper script internal failure!".
    4.  The `end_time` for the failed wrapper entry must be populated.
    5.  The `job_run_id` must be consistent across both entries.

    ```python
    import pytest
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPIError

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"

    # Helper to deploy the failing wrapper procedure
    def deploy_failing_wrapper_procedure():
        failing_wrapper_sql = f"""
        CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
          IN p_job_name_param STRING,
          IN p_stichtag_param DATE
        )
        BEGIN
          DECLARE v_job_name STRING DEFAULT COALESCE(p_job_name_param, 'BERT_V_TA_VERTRAG_TMP');
          DECLARE v_job_run_id STRING;
          DECLARE v_start_time TIMESTAMP;
          DECLARE v_end_time TIMESTAMP;
          DECLARE v_status STRING;
          DECLARE v_message STRING;
          DECLARE v_error_message STRING;
          DECLARE v_stichtag DATE DEFAULT COALESCE(p_stichtag_param, CURRENT_DATE());

          SET v_job_run_id = GENERATE_UUID();
          SET v_start_time = CURRENT_TIMESTAMP();
          SET v_status = 'RUNNING';
          SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich started.';

          INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, status, message, stichtag)
          VALUES (v_job_name, v_job_run_id, v_start_time, v_status, v_message, v_stichtag);

          RAISE USING MESSAGE = 'Simulated wrapper script internal failure!'; -- INTENTIONAL FAILURE POINT

          BEGIN
            -- This part will not be reached due to the RAISE above
            CALL `{project_id}.{dataset_id}.sp_k_ausd_v_ta_vertrag_tmp`(v_job_name, v_job_run_id);

            SET v_end_time = CURRENT_TIMESTAMP();
            SET v_status = 'SUCCESS';
            SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich completed successfully. Core script finished.';

            INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, end_time, status, message, stichtag)
            VALUES (v_job_name, v_job_run_id, v_start_time, v_end_time, v_status, v_message, v_stichtag);

          EXCEPTION WHEN ERROR THEN
            SET v_end_time = CURRENT_TIMESTAMP();
            SET v_status = 'FAILED';
            SET v_error_message = @@error.message;
            SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich failed due to an error.';

            INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, end_time, status, message, error_message, stichtag)
            VALUES (v_job_name, v_job_run_id, v_start_time, v_end_time, v_status, v_message, v_error_message, v_stichtag);

            RAISE USING MESSAGE = 'Job ' || v_job_name || ' (Run ID: ' || v_job_run_id || ') FAILED: ' || v_error_message;

          END;
        END;
        """
        client.query(failing_wrapper_sql).result()

    # Helper to deploy the original wrapper procedure (for cleanup/other tests)
    def deploy_original_wrapper_procedure():
        original_wrapper_sql = f"""
        CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
          IN p_job_name_param STRING,
          IN p_stichtag_param DATE
        )
        BEGIN
          DECLARE v_job_name STRING DEFAULT COALESCE(p_job_name_param, 'BERT_V_TA_VERTRAG_TMP');
          DECLARE v_job_run_id STRING;
          DECLARE v_start_time TIMESTAMP;
          DECLARE v_end_time TIMESTAMP;
          DECLARE v_status STRING;
          DECLARE v_message STRING;
          DECLARE v_error_message STRING;
          DECLARE v_stichtag DATE DEFAULT COALESCE(p_stichtag_param, CURRENT_DATE());

          SET v_job_run_id = GENERATE_UUID();
          SET v_start_time = CURRENT_TIMESTAMP();
          SET v_status = 'RUNNING';
          SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich started.';

          INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, status, message, stichtag)
          VALUES (v_job_name, v_job_run_id, v_start_time, v_status, v_message, v_stichtag);

          BEGIN
            CALL `{project_id}.{dataset_id}.sp_k_ausd_v_ta_vertrag_tmp`(v_job_name, v_job_run_id);

            SET v_end_time = CURRENT_TIMESTAMP();
            SET v_status = 'SUCCESS';
            SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich completed successfully. Core script finished.';

            INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, end_time, status, message, stichtag)
            VALUES (v_job_name, v_job_run_id, v_start_time, v_end_time, v_status, v_message, v_stichtag);

          EXCEPTION WHEN ERROR THEN
            SET v_end_time = CURRENT_TIMESTAMP();
            SET v_status = 'FAILED';
            SET v_error_message = @@error.message;
            SET v_message = 'Wrapper procedure sp_vertragsdatenabgleich failed due to an error.';

            INSERT INTO `{project_id}.{dataset_id}.job_audit_log` (job_name, job_run_id, start_time, end_time, status, message, error_message, stichtag)
            VALUES (v_job_name, v_job_run_id, v_start_time, v_end_time, v_status, v_message, v_error_message, v_stichtag);

            RAISE USING MESSAGE = 'Job ' || v_job_name || ' (Run ID: ' || v_job_run_id || ') FAILED: ' || v_error_message;

          END;
        END;
        """
        client.query(original_wrapper_sql).result()


    def test_wrapper_script_internal_failure_handling():
        # Clean up previous runs
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()

        # Setup: Deploy the failing wrapper procedure
        deploy_failing_wrapper_procedure()

        # Action: Call the wrapper stored procedure
        with pytest.raises(GoogleAPIError) as excinfo:
            client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(NULL, NULL)").result()

        # Pass/Fail: Assertions on error message
        assert "Job BERT_V_TA_VERTRAG_TMP (Run ID:" in str(excinfo.value)
        assert "FAILED: Simulated wrapper script internal failure!" in str(excinfo.value)

        # Assertions on log entries
        results = client.query(f"""
            SELECT job_name, job_run_id, status, message, error_message,
                   FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', start_time) as start_time_fmt,
                   FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', end_time) as end_time_fmt
            FROM `{project_id}.{dataset_id}.job_audit_log`
            ORDER BY log_timestamp
        """).result()

        entries = list(results)
        assert len(entries) == 2, f"Expected 2 log entries, got {len(entries)}"

        job_run_id = entries[0].job_run_id
        for entry in entries:
            assert entry.job_run_id == job_run_id, "job_run_id mismatch across entries"
            assert entry.job_name == 'BERT_V_TA_VERTRAG_TMP'

        assert entries[0].status == 'RUNNING' and 'Wrapper procedure sp_vertragsdatenabgleich started.' in entries[0].message
        assert entries[0].error_message is None

        assert entries[1].status == 'FAILED'
        assert 'Wrapper procedure sp_vertragsdatenabgleich failed due to an error.' in entries[1].message
        assert 'Simulated wrapper script internal failure!' in entries[1].error_message
        assert entries[1].end_time_fmt is not None

        # Cleanup: Re-deploy the original wrapper procedure
        deploy_original_wrapper_procedure()
    ```

---

## Test Case 5: `job_audit_log` Schema and Data Quality Assertions

*   **Purpose**: Verify that the `job_audit_log` table's schema, data types, `NOT NULL` constraints, partitioning, and clustering are correctly implemented as per the design. This covers data quality and schema assertions, and the replacement of filesystem logging.

*   **Setup**:
    1.  Ensure the `project.dataset.job_audit_log` table is created using the provided DDL.
    2.  Ensure `project.dataset.sp_vertragsdatenabgleich` and `project.dataset.sp_k_ausd_v_ta_vertrag_tmp` are deployed in their default, successful states.

*   **Action**:
    1.  Query BigQuery's `INFORMATION_SCHEMA` to inspect the table's schema.
    2.  Attempt `INSERT` statements that violate `NOT NULL` constraints.
    3.  Run a successful job and a failed job (using the modified procedures from previous tests) to populate data and observe partitioning/clustering behavior.

*   **Pass/Fail Criterion**:
    1.  **Schema Match**: The `INFORMATION_SCHEMA.COLUMNS` for `job_audit_log` must match the DDL:
        *   `job_name` (STRING, NOT NULL)
        *   `job_run_id` (STRING, NOT NULL)
        *   `start_time` (TIMESTAMP, NOT NULL)
        *   `end_time` (TIMESTAMP, NULLABLE)
        *   `status` (STRING, NOT NULL)
        *   `message` (STRING, NULLABLE)
        *   `error_message` (STRING, NULLABLE)
        *   `stichtag` (DATE, NULLABLE)
        *   `log_timestamp` (TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
    2.  **NOT NULL Enforcement**: Attempting to insert `NULL` into any `NOT NULL` column (e.g., `job_name`, `job_run_id`, `start_time`, `status`) must result in a BigQuery error.
    3.  **Partitioning**: The table must be partitioned by `DATE(start_time)`. This can be verified via `INFORMATION_SCHEMA.TABLE_OPTIONS`.
    4.  **Clustering**: The table must be clustered by `job_name, status`. This can be verified via `INFORMATION_SCHEMA.TABLE_OPTIONS` (looking at `clustering_columns`).

    ```python
    import pytest
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPIError

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    table_id = "job_audit_log"

    def test_job_audit_log_schema_and_data_quality():
        # 1. Verify Schema Match
        schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_id}'
            ORDER BY ordinal_position
        """
        schema_results = client.query(schema_query).result()
        actual_schema = {row.column_name: {'data_type': row.data_type, 'is_nullable': row.is_nullable} for row in schema_results}

        expected_schema = {
            'job_name': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'job_run_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'start_time': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
            'end_time': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
            'status': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'message': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'error_message': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'stichtag': {'data_type': 'DATE', 'is_nullable': 'YES'},
            'log_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
        }

        assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema}"

        # 2. Verify NOT NULL Enforcement
        not_null_columns = ['job_name', 'job_run_id', 'start_time', 'status']
        for col in not_null_columns:
            insert_sql = f"""
                INSERT INTO `{project_id}.{dataset_id}.{table_id}` (job_name, job_run_id, start_time, status)
                VALUES ('test_job', 'test_run_id', CURRENT_TIMESTAMP(), 'TEST_STATUS')
            """
            # Dynamically replace the column being tested with NULL
            if col == 'job_name':
                insert_sql = insert_sql.replace("'test_job'", "NULL")
            elif col == 'job_run_id':
                insert_sql = insert_sql.replace("'test_run_id'", "NULL")
            elif col == 'start_time':
                insert_sql = insert_sql.replace("CURRENT_TIMESTAMP()", "NULL")
            elif col == 'status':
                insert_sql = insert_sql.replace("'TEST_STATUS'", "NULL")

            with pytest.raises(GoogleAPIError) as excinfo:
                client.query(insert_sql).result()
            assert "Cannot insert NULL into a NOT NULL column" in str(excinfo.value) or \
                   "Required field is null" in str(excinfo.value)

        # 3. Verify Partitioning and Clustering
        table_details_query = f"""
            SELECT
                option_name, option_value
            FROM
                `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLE_OPTIONS`
            WHERE
                table_name = '{table_id}' AND option_name IN ('partitioning_expression', 'clustering_columns')
        """
        table_details_results = client.query(table_details_query).result()
        options = {row.option_name: row.option_value for row in table_details_results}

        assert 'partitioning_expression' in options
        assert options['partitioning_expression'] == 'DATE(start_time)'

        assert 'clustering_columns' in options
        # BigQuery stores clustering columns as a string array representation, e.g., "['job_name', 'status']"
        clustering_cols_str = options['clustering_columns']
        assert 'job_name' in clustering_cols_str
        assert 'status' in clustering_cols_str
        # A more robust check might parse the string and compare lists, but this covers basic existence.

        # Clean up any test data
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()
    ```

---

## Test Case 6: Date Handling (`stichtag`)

*   **Purpose**: Verify that the `stichtag` parameter, which replaces the legacy `v_sysdate=$(date +%d%m%Y)` logic, correctly defaults to the current date and accepts explicit date values, storing them accurately in the `job_audit_log`. This ensures correct date transformation and output parity.

*   **Setup**:
    1.  Ensure the `project.dataset.job_audit_log` table exists and is empty.
    2.  Ensure `sp_vertragsdatenabgleich` and `sp_k_ausd_v_ta_vertrag_tmp` are deployed in their default, successful states.

*   **Action**:
    1.  Execute `sp_vertragsdatenabgleich` with `NULL` for `p_stichtag_param` (to test default behavior).
    2.  Execute `sp_vertragsdatenabgleich` with a specific date for `p_stichtag_param`.

*   **Pass/Fail Criterion**:
    1.  Both `CALL` statements complete successfully.
    2.  For the first call (default `stichtag`): All `stichtag` entries in `job_audit_log` for that run must match `CURRENT_DATE()` at the time of execution.
    3.  For the second call (explicit `stichtag`): All `stichtag` entries in `job_audit_log` for that run must match the provided date (`DATE('2024-07-20')`).

    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import date, timedelta

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"

    def test_date_handling_stichtag():
        # Clean up previous runs
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()

        # Action 1: Call with default stichtag
        query_job_default = client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`('DEFAULT_DATE_JOB', NULL)")
        query_job_default.result()

        # Pass/Fail 1: Assert default stichtag
        results_default = client.query(f"""
            SELECT DISTINCT stichtag, job_run_id
            FROM `{project_id}.{dataset_id}.job_audit_log`
            WHERE job_name = 'DEFAULT_DATE_JOB'
        """).result()
        default_stichtag_entries = list(results_default)
        assert len(default_stichtag_entries) == 1, "Expected one distinct stichtag for default run"
        assert default_stichtag_entries[0].stichtag == date.today(), \
            f"Default stichtag mismatch: Expected {date.today()}, got {default_stichtag_entries[0].stichtag}"

        # Clean up for next action
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()

        # Action 2: Call with custom stichtag
        custom_stichtag = date(2024, 7, 20)
        query_job_custom = client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`('CUSTOM_DATE_JOB', '{custom_stichtag.isoformat()}')")
        query_job_custom.result()

        # Pass/Fail 2: Assert custom stichtag
        results_custom = client.query(f"""
            SELECT DISTINCT stichtag, job_run_id
            FROM `{project_id}.{dataset_id}.job_audit_log`
            WHERE job_name = 'CUSTOM_DATE_JOB'
        """).result()
        custom_stichtag_entries = list(results_custom)
        assert len(custom_stichtag_entries) == 1, "Expected one distinct stichtag for custom run"
        assert custom_stichtag_entries[0].stichtag == custom_stichtag, \
            f"Custom stichtag mismatch: Expected {custom_stichtag}, got {custom_stichtag_entries[0].stichtag}"

        # Clean up any remaining test data
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()
    ```