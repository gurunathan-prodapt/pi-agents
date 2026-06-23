The migration of `k_ausd_v_ta_cntrct_crs2.ksh` to BigQuery involves a significant shift from shell scripting and `SQL*Plus` orchestration to native BigQuery Stored Procedures and DML operations. The tests below focus on validating the behavioral equivalence of the migrated orchestration logic, including parameter handling, job lifecycle management, error reporting, and audit logging.

The provided BigQuery code snippets for `d_ausd_v_ta_cntrct_crs2_sp` and `r_ausd_vertrag_control` have been slightly adjusted to facilitate testing, specifically by using an `OUT` parameter for `records_processed_out` in `d_ausd_v_ta_cntrct_crs2_sp` and capturing it in `r_ausd_vertrag_control`. Additionally, a mechanism to simulate errors in `d_ausd_v_ta_cntrct_crs2_sp` has been added for testing error handling.

All tests assume a `test_project.test_dataset` for isolation.

---

## Test 1: Successful Job Execution

*   **Purpose**: Verify the full successful execution flow, including correct parameter handling, deactivation of older active jobs, registration and deactivation of the current job, invocation of the core data processing procedure, capture of processed record count, and accurate audit logging. This covers **Output parity**, **Transformation correctness**, and **Data-quality / row-count / schema assertions**.

*   **Setup**:
    1.  Ensure `test_project.test_dataset.job_table`, `test_project.test_dataset.job_run_audit`, and `test_project.test_dataset.job_error_log` are empty.
    2.  Pre-populate `test_project.test_dataset.job_table` with an "older active job" for the same `job_kennung` but a different `eintrags_nr`.
    3.  Ensure `d_ausd_v_ta_cntrct_crs2_sp` is configured to return a specific non-zero `records_processed_out` (e.g., 123) and does not raise an error.

*   **Action**:
    Call the migrated orchestration procedure:
    ```sql
    CALL `test_project.test_dataset.r_ausd_vertrag_control`('TEST_JOB_SUCCESS', 'RUN_001');
    ```

*   **Pass/Fail Criterion**:
    1.  **Job Table State**:
        *   The older job (`TEST_JOB_SUCCESS`, `OLD_RUN`) should have `active_flag = FALSE`.
        *   The current job (`TEST_JOB_SUCCESS`, `RUN_001`) should exist and have `active_flag = FALSE`.
        *   There should be no other active jobs for `TEST_JOB_SUCCESS`.
    2.  **Audit Log Entry**:
        *   `test_project.test_dataset.job_run_audit` should contain exactly one entry for `('TEST_JOB_SUCCESS', 'RUN_001')`.
        *   This entry's `status` should be 'SUCCESS'.
        *   `records_processed` should match the value returned by `d_ausd_v_ta_cntrct_crs2_sp` (e.g., 123).
        *   `start_timestamp` and `end_timestamp` should be populated.
    3.  **Error Log State**:
        *   `test_project.test_dataset.job_error_log` should be empty.

*   **Runnable Test Code (Pytest / SQL Assertions)**:
    ```python
    import pytest
    from google.cloud import bigquery
    import time

    client = bigquery.Client()
    PROJECT_ID = "test_project"
    DATASET_ID = "test_dataset"

    @pytest.fixture(autouse=True)
    def setup_teardown_tables():
        # Clean up tables before each test
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_audit`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        yield

    def test_successful_job_execution():
        job_kennung = 'TEST_JOB_SUCCESS'
        eintrags_nr = 'RUN_001'
        older_eintrags_nr = 'OLD_RUN'
        expected_records_processed = 123 # Matches the simulated value in d_ausd_v_ta_cntrct_crs2_sp

        # Setup: Pre-populate job_table with an older active job
        client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_table`
            (job_kennung, eintrags_nr, active_flag, created_at, updated_at)
            VALUES ('{job_kennung}', '{older_eintrags_nr}', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        """).result()

        # Action: Call the orchestrator
        client.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
        """).result()

        # Assertions
        # 1. Job Table State
        job_table_query = client.query(f"""
            SELECT job_kennung, eintrags_nr, active_flag
            FROM `{PROJECT_ID}.{DATASET_ID}.job_table`
            WHERE job_kennung = '{job_kennung}'
            ORDER BY eintrags_nr
        """).result()
        job_entries = list(job_table_query)

        assert len(job_entries) == 2
        assert (job_kennung, older_eintrags_nr, False) in [(e.job_kennung, e.eintrags_nr, e.active_flag) for e in job_entries]
        assert (job_kennung, eintrags_nr, False) in [(e.job_kennung, e.eintrags_nr, e.active_flag) for e in job_entries]

        # 2. Audit Log Entry
        audit_query = client.query(f"""
            SELECT job_kennung, eintrags_nr, tab_name, records_processed, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_audit`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        audit_entries = list(audit_query)

        assert len(audit_entries) == 1
        audit_entry = audit_entries[0]
        assert audit_entry.job_kennung == job_kennung
        assert audit_entry.eintrags_nr == eintrags_nr
        assert audit_entry.tab_name == 'ta_cntrct_crs2'
        assert audit_entry.records_processed == expected_records_processed
        assert audit_entry.status == 'SUCCESS'

        # 3. Error Log State
        error_log_query = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        assert list(error_log_query)[0][0] == 0
    ```

---

## Test 2: Missing Required Parameters

*   **Purpose**: Verify that the procedure correctly identifies and handles missing `p_job_kennung` or `p_eintrags_nr` parameters, logs the error, and exits gracefully without processing. This covers **Transformation correctness** and **Error handling**.

*   **Setup**:
    1.  Ensure `test_project.test_dataset.job_table`, `test_project.test_dataset.job_run_audit`, and `test_project.test_dataset.job_error_log` are empty.

*   **Action**:
    Attempt to call the migrated orchestration procedure with a `NULL` `p_job_kennung`:
    ```sql
    -- This call is expected to raise an error
    CALL `test_project.test_dataset.r_ausd_vertrag_control`(NULL, 'RUN_002');
    ```
    (A similar test would be performed for `NULL` `p_eintrags_nr`).

*   **Pass/Fail Criterion**:
    1.  **Procedure Behavior**: The `CALL` statement should raise an error (e.g., `BigQueryException` in Python).
    2.  **Error Log Entry**:
        *   `test_project.test_dataset.job_error_log` should contain exactly one entry.
        *   `err_nr` should be `1` (as defined in the procedure for parameter validation).
        *   `error_message` should contain "Required parameters p_job_kennung or p_eintrags_nr are not set."
        *   `err_arg` should be 'PARAMETER_MISSING'.
    3.  **Job Table State**:
        *   `test_project.test_dataset.job_table` should remain empty.
    4.  **Audit Log State**:
        *   `test_project.test_dataset.job_run_audit` should remain empty.

*   **Runnable Test Code (Pytest / SQL Assertions)**:
    ```python
    import pytest
    from google.cloud import bigquery
    from google.api_core.exceptions import BadRequest # For BigQuery errors

    client = bigquery.Client()
    PROJECT_ID = "test_project"
    DATASET_ID = "test_dataset"

    @pytest.fixture(autouse=True)
    def setup_teardown_tables():
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_audit`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        yield

    def test_missing_parameters():
        job_kennung = None # Simulate missing parameter
        eintrags_nr = 'RUN_002'

        # Action: Call the orchestrator, expecting an error
        with pytest.raises(BadRequest) as excinfo:
            client.query(f"""
                CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`({job_kennung if job_kennung else 'NULL'}, '{eintrags_nr}');
            """).result()

        assert "Required parameters p_job_kennung or p_eintrags_nr are not set." in str(excinfo.value)

        # Assertions
        # 1. Error Log Entry
        error_log_query = client.query(f"""
            SELECT err_nr, err_arg, error_message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        """).result()
        error_entries = list(error_log_query)

        assert len(error_entries) == 1
        error_entry = error_entries[0]
        assert error_entry.err_nr == 1
        assert error_entry.err_arg == 'PARAMETER_MISSING'
        assert "Required parameters p_job_kennung or p_eintrags_nr are not set." in error_entry.error_message

        # 2. Job Table State
        job_table_query = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
        assert list(job_table_query)[0][0] == 0

        # 3. Audit Log State
        audit_query = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_run_audit`").result()
        assert list(audit_query)[0][0] == 0
    ```

---

## Test 3: Internal Data Processing Error

*   **Purpose**: Verify that if the core data transformation procedure (`d_ausd_v_ta_cntrct_crs2_sp`) fails, the orchestrator correctly catches the error, logs it, marks the job as failed/inactive, and audits the failure. This covers **Transformation correctness** and **Error handling**.

*   **Setup**:
    1.  Ensure `test_project.test_dataset.job_table`, `test_project.test_dataset.job_run_audit`, and `test_project.test_dataset.job_error_log` are empty.
    2.  Modify `d_ausd_v_ta_cntrct_crs2_sp` to intentionally raise an error when a specific `job_kennung` is passed (e.g., 'ERROR_JOB_SP').

*   **Action**:
    Call the migrated orchestration procedure with the `job_kennung` that triggers the error in `d_ausd_v_ta_cntrct_crs2_sp`:
    ```sql
    -- This call is expected to raise an error
    CALL `test_project.test_dataset.r_ausd_vertrag_control`('ERROR_JOB_SP', 'RUN_003');
    ```

*   **Pass/Fail Criterion**:
    1.  **Procedure Behavior**: The `CALL` statement should raise an error.
    2.  **Error Log Entry**:
        *   `test_project.test_dataset.job_error_log` should contain exactly one entry.
        *   `err_nr` should be `-1` (or the specific error code from `d_ausd_v_ta_cntrct_crs2_sp`).
        *   `error_message` should contain the simulated error message from `d_ausd_v_ta_cntrct_crs2_sp`.
        *   `err_arg` should be 'UNEXPECTED_EXCEPTION'.
    3.  **Job Table State**:
        *   The job (`ERROR_JOB_SP`, `RUN_003`) should exist in `job_table` and have `active_flag = FALSE`.
    4.  **Audit Log Entry**:
        *   `test_project.test_dataset.job_run_audit` should contain exactly one entry for `('ERROR_JOB_SP', 'RUN_003')`.
        *   This entry's `status` should be 'FAILED'.
        *   `records_processed` should be `0`.

*   **Runnable Test Code (Pytest / SQL Assertions)**:
    ```python
    import pytest
    from google.cloud import bigquery
    from google.api_core.exceptions import BadRequest

    client = bigquery.Client()
    PROJECT_ID = "test_project"
    DATASET_ID = "test_dataset"

    @pytest.fixture(autouse=True)
    def setup_teardown_tables():
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_audit`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        yield

    def test_internal_data_processing_error():
        job_kennung = 'ERROR_JOB_SP' # This triggers the error in d_ausd_v_ta_cntrct_crs2_sp
        eintrags_nr = 'RUN_003'
        simulated_error_message = 'Simulated error in d_ausd_v_ta_cntrct_crs2_sp for ERROR_JOB_SP'

        # Action: Call the orchestrator, expecting an error
        with pytest.raises(BadRequest) as excinfo:
            client.query(f"""
                CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
            """).result()

        assert simulated_error_message in str(excinfo.value)

        # Assertions
        # 1. Error Log Entry
        error_log_query = client.query(f"""
            SELECT err_nr, err_arg, error_message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        """).result()
        error_entries = list(error_log_query)

        assert len(error_entries) == 1
        error_entry = error_entries[0]
        assert error_entry.err_nr == -1 # Generic error code for unexpected exceptions
        assert error_entry.err_arg == 'UNEXPECTED_EXCEPTION'
        assert simulated_error_message in error_entry.error_message

        # 2. Job Table State
        job_table_query = client.query(f"""
            SELECT job_kennung, eintrags_nr, active_flag
            FROM `{PROJECT_ID}.{DATASET_ID}.job_table`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        job_entries = list(job_table_query)

        assert len(job_entries) == 1
        assert job_entries[0].active_flag == False

        # 3. Audit Log Entry
        audit_query = client.query(f"""
            SELECT job_kennung, eintrags_nr, records_processed, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_audit`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        audit_entries = list(audit_query)

        assert len(audit_entries) == 1
        audit_entry = audit_entries[0]
        assert audit_entry.records_processed == 0
        assert audit_entry.status == 'FAILED'
    ```

---

## Test 4: Idempotency / Concurrent Run Handling

*   **Purpose**: Verify that running the job multiple times with the same `job_kennung` and `eintrags_nr` behaves correctly, specifically regarding job activation/deactivation and audit logging, ensuring no duplicate active entries or unexpected side effects. This covers **Transformation correctness** and **Data-quality / row-count / schema assertions**.

*   **Setup**:
    1.  Ensure `test_project.test_dataset.job_table`, `test_project.test_dataset.job_run_audit`, and `test_project.test_dataset.job_error_log` are empty.
    2.  Ensure `d_ausd_v_ta_cntrct_crs2_sp` is configured for successful execution.

*   **Action**:
    1.  Call `r_ausd_vertrag_control` with `job_kennung='IDEMPOTENT_JOB'`, `eintrags_nr='RUN_004'`.
    2.  Call `r_ausd_vertrag_control` again with `job_kennung='IDEMPOTENT_JOB'`, `eintrags_nr='RUN_004'`.

*   **Pass/Fail Criterion**:
    1.  **Job Table State**:
        *   `test_project.test_dataset.job_table` should contain exactly one entry for `('IDEMPOTENT_JOB', 'RUN_004')`.
        *   This entry should have `active_flag = FALSE`.
        *   The `updated_at` timestamp for this entry should reflect the time of the *second* run.
    2.  **Audit Log Entries**:
        *   `test_project.test_dataset.job_run_audit` should contain exactly two entries for `('IDEMPOTENT_JOB', 'RUN_004')`, both with `status = 'SUCCESS'`.
        *   Each entry should have the correct `records_processed` count.
    3.  **Error Log State**:
        *   `test_project.test_dataset.job_error_log` should be empty.

*   **Runnable Test Code (Pytest / SQL Assertions)**:
    ```python
    import pytest
    from google.cloud import bigquery
    import time

    client = bigquery.Client()
    PROJECT_ID = "test_project"
    DATASET_ID = "test_dataset"

    @pytest.fixture(autouse=True)
    def setup_teardown_tables():
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_audit`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        yield

    def test_idempotent_run_handling():
        job_kennung = 'IDEMPOTENT_JOB'
        eintrags_nr = 'RUN_004'
        expected_records_processed = 123

        # Action 1: First call
        client.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
        """).result()
        time.sleep(1) # Ensure updated_at changes for the second run

        # Action 2: Second call with same parameters
        client.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
        """).result()

        # Assertions
        # 1. Job Table State
        job_table_query = client.query(f"""
            SELECT job_kennung, eintrags_nr, active_flag, updated_at
            FROM `{PROJECT_ID}.{DATASET_ID}.job_table`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        job_entries = list(job_table_query)

        assert len(job_entries) == 1 # Only one entry for the job run
        job_entry = job_entries[0]
        assert job_entry.active_flag == False # Should be inactive after both runs

        # 2. Audit Log Entries
        audit_query = client.query(f"""
            SELECT job_kennung, eintrags_nr, records_processed, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_audit`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        audit_entries = list(audit_query)

        assert len(audit_entries) == 2 # Two audit entries, one for each run
        for entry in audit_entries:
            assert entry.status == 'SUCCESS'
            assert entry.records_processed == expected_records_processed

        # 3. Error Log State
        error_log_query = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        assert list(error_log_query)[0][0] == 0
    ```

---

## Test 5: Schema and Data Quality Assertions

*   **Purpose**: Verify that the schemas of the audit and log tables are as expected, and that basic data quality (e.g., non-null constraints, data types) is maintained for critical fields after a job run. This covers **Data-quality / row-count / schema assertions**.

*   **Setup**:
    1.  Ensure all tables are created according to the provided DDLs.
    2.  Run a successful job (e.g., `CALL test_project.test_dataset.r_ausd_vertrag_control('SCHEMA_TEST_JOB', 'RUN_005');`) to populate the tables with data.

*   **Action**:
    Query BigQuery's `INFORMATION_SCHEMA` and the populated tables.

*   **Pass/Fail Criterion**:
    1.  **`job_table` Schema and Constraints**:
        *   `job_kennung` and `eintrags_nr` columns exist and are `STRING NOT NULL`.
        *   `active_flag` is `BOOL NOT NULL`.
        *   `created_at` and `updated_at` are `TIMESTAMP NOT NULL`.
    2.  **`job_error_log` Schema and Constraints**:
        *   `job_kennung` is `STRING NOT NULL`.
        *   `created_at` is `TIMESTAMP`.
    3.  **`job_run_audit` Schema and Constraints**:
        *   `job_kennung` and `eintrags_nr` are `STRING NOT NULL`.
        *   `records_processed` is `INT64`.
        *   `status` is `STRING`.
    4.  **Data Quality (Post-Run)**:
        *   For the successful run, `job_table` entry for `('SCHEMA_TEST_JOB', 'RUN_005')` has `active_flag = FALSE`.
        *   `job_run_audit` entry has `status = 'SUCCESS'` and `records_processed > 0`.
        *   No `NULL` values in `job_kennung`, `eintrags_nr` for any entries in `job_table` or `job_run_audit`.

*   **Runnable Test Code (Pytest / SQL Assertions)**:
    ```python
    import pytest
    from google.cloud import bigquery

    client = bigquery.Client()
    PROJECT_ID = "test_project"
    DATASET_ID = "test_dataset"

    @pytest.fixture(autouse=True)
    def setup_teardown_tables():
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_audit`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        yield

    def test_schema_and_data_quality():
        job_kennung = 'SCHEMA_TEST_JOB'
        eintrags_nr = 'RUN_005'
        expected_records_processed = 123

        # Setup: Run a successful job to populate data
        client.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
        """).result()

        # Assertions for Schema and Constraints using INFORMATION_SCHEMA
        def assert_column_properties(table_id, column_name, expected_data_type, expected_is_nullable):
            query = f"""
                SELECT data_type, is_nullable
                FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_id}' AND column_name = '{column_name}'
            """
            result = client.query(query).result()
            row = list(result)[0]
            assert row.data_type == expected_data_type
            assert row.is_nullable == expected_is_nullable

        # 1. job_table Schema
        assert_column_properties('job_table', 'job_kennung', 'STRING', 'NO')
        assert_column_properties('job_table', 'eintrags_nr', 'STRING', 'NO')
        assert_column_properties('job_table', 'active_flag', 'BOOL', 'NO')
        assert_column_properties('job_table', 'created_at', 'TIMESTAMP', 'NO')
        assert_column_properties('job_table', 'updated_at', 'TIMESTAMP', 'NO')

        # 2. job_error_log Schema
        assert_column_properties('job_error_log', 'job_kennung', 'STRING', 'NO')
        assert_column_properties('job_error_log', 'created_at', 'TIMESTAMP', 'YES') # Default CURRENT_TIMESTAMP() makes it nullable

        # 3. job_run_audit Schema
        assert_column_properties('job_run_audit', 'job_kennung', 'STRING', 'NO')
        assert_column_properties('job_run_audit', 'eintrags_nr', 'STRING', 'NO')
        assert_column_properties('job_run_audit', 'records_processed', 'INT64', 'YES') # Can be NULL if not set
        assert_column_properties('job_run_audit', 'status', 'STRING', 'YES') # Can be NULL if not set
        assert_column_properties('job_run_audit', 'start_timestamp', 'TIMESTAMP', 'YES')
        assert_column_properties('job_run_audit', 'end_timestamp', 'TIMESTAMP', 'YES')

        # 4. Data Quality (Post-Run)
        # Verify job_table entry
        job_table_data_query = client.query(f"""
            SELECT active_flag FROM `{PROJECT_ID}.{DATASET_ID}.job_table`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        assert list(job_table_data_query)[0].active_flag == False

        # Verify job_run_audit entry
        audit_data_query = client.query(f"""
            SELECT records_processed, status FROM `{PROJECT_ID}.{DATASET_ID}.job_run_audit`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        audit_entry = list(audit_data_query)[0]
        assert audit_entry.records_processed == expected_records_processed
        assert audit_entry.status == 'SUCCESS'

        # Verify no NULLs in critical fields
        null_check_query = client.query(f"""
            SELECT
                (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_table` WHERE job_kennung IS NULL OR eintrags_nr IS NULL) AS job_table_nulls,
                (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_run_audit` WHERE job_kennung IS NULL OR eintrags_nr IS NULL) AS audit_table_nulls,
                (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE job_kennung IS NULL) AS error_log_nulls
        """).result()
        null_counts = list(null_check_query)[0]
        assert null_counts.job_table_nulls == 0
        assert null_counts.audit_table_nulls == 0
        assert null_counts.error_log_nulls == 0
    ```

---

## Test 6: `d_ausd_v_ta_cntrct_crs2_sp` Integration (Output Parity - Data)

*   **Purpose**: Verify that the core data transformation procedure (`d_ausd_v_ta_cntrct_crs2_sp`) is correctly invoked by the orchestrator and that the number of records it processes is accurately captured and logged in the `job_run_audit` table. This covers **Output parity** and **Transformation correctness**.

*   **Setup**:
    1.  Ensure `test_project.test_dataset.job_run_audit` is empty.
    2.  Modify `d_ausd_v_ta_cntrct_crs2_sp` to return a specific, non-zero number of processed records (e.g., 456) via its `OUT` parameter.

*   **Action**:
    Call `r_ausd_vertrag_control` with valid parameters:
    ```sql
    CALL `test_project.test_dataset.r_ausd_vertrag_control`('DATA_INTEGRATION_JOB', 'RUN_006');
    ```

*   **Pass/Fail Criterion**:
    1.  **Audit Log Entry**:
        *   `test_project.test_dataset.job_run_audit` should contain exactly one entry for `('DATA_INTEGRATION_JOB', 'RUN_006')`.
        *   This entry's `status` should be 'SUCCESS'.
        *   `records_processed` should exactly match the value returned by `d_ausd_v_ta_cntrct_crs2_sp` (e.g., 456).
    2.  **Error Log State**:
        *   `test_project.test_dataset.job_error_log` should be empty.

*   **Runnable Test Code (Pytest / SQL Assertions)**:
    ```python
    import pytest
    from google.cloud import bigquery

    client = bigquery.Client()
    PROJECT_ID = "test_project"
    DATASET_ID = "test_dataset"

    @pytest.fixture(autouse=True)
    def setup_teardown_tables():
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_audit`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        yield

    def test_d_ausd_v_ta_cntrct_crs2_sp_integration():
        job_kennung = 'DATA_INTEGRATION_JOB'
        eintrags_nr = 'RUN_006'
        # IMPORTANT: Manually update d_ausd_v_ta_cntrct_crs2_sp to return this value for this test
        expected_records_processed = 456

        # Action: Call the orchestrator
        client.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
        """).result()

        # Assertions
        # 1. Audit Log Entry
        audit_query = client.query(f"""
            SELECT records_processed, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_audit`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """).result()
        audit_entries = list(audit_query)

        assert len(audit_entries) == 1
        audit_entry = audit_entries[0]
        assert audit_entry.records_processed == expected_records_processed
        assert audit_entry.status == 'SUCCESS'

        # 2. Error Log State
        error_log_query = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
        assert list(error_log_query)[0][0] == 0
    ```