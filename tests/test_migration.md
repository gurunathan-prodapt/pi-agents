As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_v_ta_c_bfc.ksh` to BigQuery. These tests focus on ensuring behavioral equivalence, correct data transformations, proper logging, and robust error handling in the new BigQuery environment.

The tests are structured to cover the key aspects identified in the migration design: output parity, transformation correctness, external system replacements (logging), and data quality assertions.

**Assumptions for Testing:**
*   The BigQuery tables `your_project_id.your_dataset_id.job_audit_log` and `your_project_id.your_dataset_id.job_error_log` exist.
*   The BigQuery stored procedures `your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc` and `your_project_id.your_dataset_id.sp_bindefristcache_update` have been deployed.
*   A Python environment with `google-cloud-bigquery` installed is available for running the `pytest` examples.
*   `your_project_id` and `your_dataset_id` should be replaced with actual project and dataset names.

---

## Migration Validation Tests for `r_ausd_v_ta_c_bfc.ksh`

### Test Case 1: Successful Execution (Happy Path)

*   **Purpose:** Verify that the migrated BigQuery stored procedure executes successfully without any input parameters, correctly orchestrates the core logic, and logs the job's start and completion status. This tests output parity for successful runs and correct logging.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_audit_log` and `your_project_id.your_dataset_id.job_error_log` tables are empty.
    2.  Ensure `your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc` is in its default, non-error-raising state (i.e., it completes successfully).
*   **Action:**
    Execute the `sp_bindefristcache_update` stored procedure without any parameters.
    ```sql
    CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes without raising an error.
    2.  The `job_audit_log` table contains exactly two entries for the executed job:
        *   One with `status = 'STARTED'` and `message = 'Job execution started.'`.
        *   One with `status = 'COMPLETED'` and `message = 'Job execution completed successfully.'`.
    3.  Both entries share the same `job_kennung` and `entry_nr`.
    4.  The `job_error_log` table contains zero entries.
    5.  The `job_kennung` follows the format `YYYYMMDDHHMMSS_r_ausd_v_ta_c_bfc`.
    6.  The `entry_nr` is a positive integer.

*   **Runnable Test Code (pytest / SQL assertions):**
    ```python
    import pytest
    from google.cloud import bigquery
    import time
    import datetime

    PROJECT_ID = "your_project_id"
    DATASET_ID = "your_dataset_id"
    AUDIT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_audit_log"
    ERROR_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_error_log"
    SP_MAIN = f"{PROJECT_ID}.{DATASET_ID}.sp_bindefristcache_update"
    SP_CORE = f"{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_c_bfc"

    client = bigquery.Client()

    def _clear_logs():
        client.query(f"TRUNCATE TABLE `{AUDIT_TABLE}`").result()
        client.query(f"TRUNCATE TABLE `{ERROR_TABLE}`").result()

    def _set_core_sp_behavior(raise_error=False):
        if raise_error:
            core_sp_code = f"""
            CREATE OR REPLACE PROCEDURE `{SP_CORE}`(
                p_job_kennung STRING,
                p_entry_nr INT64
            )
            BEGIN
                RAISE BQ EXCEPTION 'Simulated error in core logic for testing.';
            END;
            """
        else:
            core_sp_code = f"""
            CREATE OR REPLACE PROCEDURE `{SP_CORE}`(
                p_job_kennung STRING,
                p_entry_nr INT64
            )
            BEGIN
                SELECT FORMAT("Core logic for JobKennung: %s, EntryNr: %d - placeholder executed.", p_job_kennung, p_entry_nr) AS message;
            END;
            """
        client.query(core_sp_code).result()

    @pytest.fixture(autouse=True)
    def setup_and_teardown():
        _clear_logs()
        _set_core_sp_behavior(raise_error=False) # Ensure core SP is not raising errors
        yield
        _clear_logs()

    def test_successful_execution():
        # Action
        client.query(f"CALL `{SP_MAIN}`()").result()

        # Assertions
        audit_rows = list(client.query(f"SELECT * FROM `{AUDIT_TABLE}` ORDER BY created_ts").result())
        error_rows = list(client.query(f"SELECT * FROM `{ERROR_TABLE}`").result())

        assert len(audit_rows) == 2, "Expected 2 audit log entries (STARTED, COMPLETED)"
        assert len(error_rows) == 0, "Expected no error log entries"

        started_entry = audit_rows[0]
        completed_entry = audit_rows[1]

        # Check STARTED entry
        assert started_entry.status == 'STARTED'
        assert started_entry.message == 'Job execution started.'
        assert started_entry.script_name == 'r_ausd_v_ta_c_bfc.ksh'
        assert started_entry.log_file == 'N/A'
        assert started_entry.stichtag == datetime.date.today()
        assert started_entry.end_ts is None # Should be NULL for STARTED

        # Check COMPLETED entry
        assert completed_entry.status == 'COMPLETED'
        assert completed_entry.message == 'Job execution completed successfully.'
        assert completed_entry.script_name == 'r_ausd_v_ta_c_bfc.ksh'
        assert completed_entry.log_file == 'N/A'
        assert completed_entry.stichtag == datetime.date.today()
        assert completed_entry.end_ts is not None # Should be populated for COMPLETED

        # Check parity between entries
        assert started_entry.job_kennung == completed_entry.job_kennung
        assert started_entry.entry_nr == completed_entry.entry_nr

        # Check job_kennung format (e.g., 20231027103000_r_ausd_v_ta_c_bfc)
        assert len(started_entry.job_kennung) == 28
        assert started_entry.job_kennung.endswith('_r_ausd_v_ta_c_bfc')
        assert started_entry.job_kennung[:14].isdigit() # Timestamp part

        # Check entry_nr
        assert started_entry.entry_nr > 0
    ```

### Test Case 2: Help Parameter (`-h`)

*   **Purpose:** Verify that passing the help parameter (`p_help => TRUE`) correctly displays the usage information and exits without performing any job logic or logging. This tests parameter handling and output parity for the help message.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_audit_log` and `your_project_id.your_dataset_id.job_error_log` tables are empty.
*   **Action:**
    Execute the `sp_bindefristcache_update` stored procedure with `p_help => TRUE`.
    ```sql
    CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`(p_help => TRUE);
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes without raising an error.
    2.  The result of the `CALL` statement (or the client's output) contains the expected usage message.
    3.  The `job_audit_log` table contains zero entries.
    4.  The `job_error_log` table contains zero entries.

*   **Runnable Test Code (pytest / SQL assertions):**
    ```python
    # ... (imports and client setup from Test Case 1) ...

    def test_help_parameter():
        # Action - BigQuery client.query().result() doesn't directly return SELECT output
        # for CALL statements. We need to query the INFORMATION_SCHEMA.JOBS for output.
        # However, for a simple SELECT statement within the SP, we can't easily capture it
        # directly from the CALL. The best way to test this is to ensure no logs are written.
        # If the SP were designed to return a table, it would be easier.
        # For now, we rely on the absence of log entries.

        # The design states: "SELECT ... AS usage_info;" which means it's an output.
        # BigQuery stored procedures don't return values directly to the caller in the same way
        # a function does. A SELECT statement inside a SP will produce a result set.
        # We can't directly assert the content of this result set from a simple CALL.
        # The most robust test is to ensure no side effects (logging) occur.

        # Execute the SP
        job = client.query(f"CALL `{SP_MAIN}`(p_help => TRUE)")
        job.result() # Wait for completion

        # Assertions
        audit_rows = list(client.query(f"SELECT * FROM `{AUDIT_TABLE}`").result())
        error_rows = list(client.query(f"SELECT * FROM `{ERROR_TABLE}`").result())

        assert len(audit_rows) == 0, "Expected no audit log entries when help is requested"
        assert len(error_rows) == 0, "Expected no error log entries when help is requested"

        # To truly test the output message, one would need to:
        # 1. Modify the SP to insert the help message into a temporary table.
        # 2. Query that temporary table after the CALL.
        # For this exercise, we'll assume the SP's internal SELECT works as intended
        # and focus on the side-effect (logging) absence.
    ```

### Test Case 3: Core Logic Failure

*   **Purpose:** Verify that if the core processing logic (`sp_ausd_v_ta_c_bfc`) fails, the orchestrator (`sp_bindefristcache_update`) correctly catches the error, logs it to both audit and error tables, and re-raises the error to indicate job failure. This tests transformation correctness for error handling and external system replacements (error logging).
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_audit_log` and `your_project_id.your_dataset_id.job_error_log` tables are empty.
    2.  Modify `your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc` to explicitly `RAISE BQ EXCEPTION` when called.
*   **Action:**
    Execute the `sp_bindefristcache_update` stored procedure without any parameters.
    ```sql
    CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement raises a BigQuery exception, indicating job failure.
    2.  The `job_audit_log` table contains exactly two entries for the executed job:
        *   One with `status = 'STARTED'`.
        *   One with `status = 'FAILED'` and `message = 'Job execution failed.'`.
    3.  Both entries share the same `job_kennung` and `entry_nr`.
    4.  The `job_error_log` table contains exactly one entry for the executed job, with details about the raised exception (`error_message`, `error_stack`, etc.).
    5.  The `job_kennung` and `entry_nr` in the error log match those in the audit log.

*   **Runnable Test Code (pytest / SQL assertions):**
    ```python
    # ... (imports and client setup from Test Case 1) ...

    @pytest.fixture(autouse=True)
    def setup_and_teardown_error_test():
        _clear_logs()
        _set_core_sp_behavior(raise_error=True) # Core SP will raise an error
        yield
        _clear_logs()
        _set_core_sp_behavior(raise_error=False) # Reset core SP for other tests

    def test_core_logic_failure():
        # Action - Expect an exception
        with pytest.raises(Exception) as excinfo:
            client.query(f"CALL `{SP_MAIN}`()").result()

        # Assert that the exception message indicates job failure
        assert "Job failed. Error: Simulated error in core logic for testing." in str(excinfo.value)

        # Assertions on logs
        audit_rows = list(client.query(f"SELECT * FROM `{AUDIT_TABLE}` ORDER BY created_ts").result())
        error_rows = list(client.query(f"SELECT * FROM `{ERROR_TABLE}`").result())

        assert len(audit_rows) == 2, "Expected 2 audit log entries (STARTED, FAILED)"
        assert len(error_rows) == 1, "Expected 1 error log entry"

        started_entry = audit_rows[0]
        failed_entry = audit_rows[1]
        error_entry = error_rows[0]

        # Check STARTED entry
        assert started_entry.status == 'STARTED'
        assert started_entry.message == 'Job execution started.'

        # Check FAILED entry
        assert failed_entry.status == 'FAILED'
        assert failed_entry.message == 'Job execution failed.'
        assert failed_entry.end_ts is not None

        # Check parity between audit entries
        assert started_entry.job_kennung == failed_entry.job_kennung
        assert started_entry.entry_nr == failed_entry.entry_nr

        # Check error log entry
        assert error_entry.job_kennung == failed_entry.job_kennung
        assert error_entry.err_nr == failed_entry.entry_nr # err_nr is mapped to entry_nr
        assert "Simulated error in core logic for testing." in error_entry.error_message
        assert error_entry.error_stack is not None
        assert error_entry.created_ts is not None
    ```

### Test Case 4: `JobKennung` and `EntryNr` Generation Uniqueness

*   **Purpose:** Verify that `JobKennung` is unique based on timestamp and `EntryNr` is sequentially incremented, even with rapid successive calls. This tests transformation correctness for metadata generation and data quality assertions.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_audit_log` and `your_project_id.your_dataset_id.job_error_log` tables are empty.
    2.  Ensure `your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc` is in its default, non-error-raising state.
*   **Action:**
    Execute the `sp_bindefristcache_update` stored procedure multiple times in quick succession (e.g., 3 times).
    ```sql
    CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
    CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
    CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
    ```
*   **Pass/Fail Criterion:**
    1.  All calls complete successfully.
    2.  The `job_audit_log` table contains 6 entries (3 'STARTED', 3 'COMPLETED').
    3.  Each pair of 'STARTED'/'COMPLETED' entries has a unique `job_kennung`.
    4.  The `entry_nr` values for each job are unique and sequentially incremented (e.g., 1, 2, 3 for the 'STARTED' entries).
    5.  The `job_kennung` format is consistent (`YYYYMMDDHHMMSS_r_ausd_v_ta_c_bfc`).

*   **Runnable Test Code (pytest / SQL assertions):**
    ```python
    # ... (imports and client setup from Test Case 1) ...

    def test_job_kennung_and_entry_nr_uniqueness():
        num_runs = 3

        # Action
        for _ in range(num_runs):
            client.query(f"CALL `{SP_MAIN}`()").result()
            time.sleep(1) # Ensure timestamps are distinct for JobKennung

        # Assertions
        audit_rows = list(client.query(f"SELECT * FROM `{AUDIT_TABLE}` ORDER BY entry_nr, created_ts").result())
        error_rows = list(client.query(f"SELECT * FROM `{ERROR_TABLE}`").result())

        assert len(audit_rows) == num_runs * 2, f"Expected {num_runs*2} audit log entries"
        assert len(error_rows) == 0, "Expected no error log entries"

        job_kennungs = set()
        entry_nrs = set()
        previous_entry_nr = 0

        for i in range(num_runs):
            started_entry = audit_rows[i*2]
            completed_entry = audit_rows[i*2 + 1]

            # Check job_kennung uniqueness
            assert started_entry.job_kennung not in job_kennungs
            job_kennungs.add(started_entry.job_kennung)

            # Check entry_nr sequence
            assert started_entry.entry_nr == previous_entry_nr + 1
            previous_entry_nr = started_entry.entry_nr
            entry_nrs.add(started_entry.entry_nr)

            # Check job_kennung format
            assert len(started_entry.job_kennung) == 28
            assert started_entry.job_kennung.endswith('_r_ausd_v_ta_c_bfc')
            assert started_entry.job_kennung[:14].isdigit()

            # Check parity between entries
            assert started_entry.job_kennung == completed_entry.job_kennung
            assert started_entry.entry_nr == completed_entry.entry_nr
            assert started_entry.status == 'STARTED'
            assert completed_entry.status == 'COMPLETED'

        assert len(job_kennungs) == num_runs, "Expected all job_kennungs to be unique"
        assert len(entry_nrs) == num_runs, "Expected all entry_nrs to be unique and sequential"
    ```

### Test Case 5: `stichtag` and `created_ts` Accuracy

*   **Purpose:** Verify that date and timestamp fields (`stichtag`, `created_ts`, `end_ts`) are correctly populated with accurate values reflecting the execution time. This tests transformation correctness for date handling.
*   **Setup:**
    1.  Ensure `your_project_id.your_dataset_id.job_audit_log` is empty.
    2.  Ensure `your_project_id.your_dataset_id.sp_ausd_v_ta_c_bfc` is in its default, non-error-raising state.
*   **Action:**
    Execute the `sp_bindefristcache_update` stored procedure.
    ```sql
    CALL `your_project_id.your_dataset_id.sp_bindefristcache_update`();
    ```
*   **Pass/Fail Criterion:**
    1.  The `stichtag` for both 'STARTED' and 'COMPLETED' entries in `job_audit_log` is the current date.
    2.  The `created_ts` for the 'STARTED' entry is very close to the actual start time of the procedure call.
    3.  The `end_ts` for the 'COMPLETED' entry is very close to the actual end time of the procedure call and is greater than `created_ts`.

*   **Runnable Test Code (pytest / SQL assertions):**
    ```python
    # ... (imports and client setup from Test Case 1) ...

    def test_timestamp_accuracy():
        start_test_time = datetime.datetime.now(datetime.timezone.utc)

        # Action
        client.query(f"CALL `{SP_MAIN}`()").result()

        end_test_time = datetime.datetime.now(datetime.timezone.utc)

        # Assertions
        audit_rows = list(client.query(f"SELECT * FROM `{AUDIT_TABLE}` ORDER BY created_ts").result())
        assert len(audit_rows) == 2

        started_entry = audit_rows[0]
        completed_entry = audit_rows[1]

        # Check stichtag
        assert started_entry.stichtag == datetime.date.today()
        assert completed_entry.stichtag == datetime.date.today()

        # Check created_ts for STARTED entry
        # Allow a small delta for execution time
        assert start_test_time <= started_entry.created_ts.replace(tzinfo=datetime.timezone.utc) <= end_test_time

        # Check end_ts for COMPLETED entry
        assert start_test_time <= completed_entry.end_ts.replace(tzinfo=datetime.timezone.utc) <= end_test_time
        assert completed_entry.end_ts > started_entry.created_ts
    ```