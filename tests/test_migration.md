As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery code for `r_ausd_v_ta_vvl_upgrade.ksh`. The core challenge is that the kernel script (`k_ausd_v_ta_vvl_upgrade.ksh`) is not provided, meaning the actual data transformation logic is a placeholder in the BigQuery migration. Therefore, these tests will focus heavily on the *orchestration* aspects: parameter handling, logging, status updates, and correct invocation/error handling of the kernel script.

I will use `pytest` for the test framework and `google.cloud.bigquery` client library for interaction. SQL assertions will be embedded within the test cases.

---

## Migration Validation Tests: `r_ausd_v_ta_vvl_upgrade.ksh` to BigQuery

**Assumptions:**
*   BigQuery project and dataset (`your_project.your_dataset`) are configured.
*   The `job_log` and `job_status` tables have been created using the provided DDL.
*   The placeholder `k_ausd_v_ta_vvl_upgrade` stored procedure exists in `your_project.your_dataset`. For tests requiring kernel failure, we will assume a mechanism to make `k_ausd_v_ta_vvl_upgrade` fail (e.g., by uncommenting `SELECT 1/0;` in its definition or by mocking its behavior).
*   The `r_ausd_v_ta_vvl_upgrade_bq` stored procedure has been deployed.

**Setup for all tests (Pre-requisites):**

1.  **BigQuery Client Setup (Python):**
    ```python
    import pytest
    from google.cloud import bigquery
    import datetime
    import time
    import os

    # --- Configuration ---
    PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "your_project")
    DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "your_dataset")
    BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

    JOB_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_log"
    JOB_STATUS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_status"
    WRAPPER_SP = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_v_ta_vvl_upgrade_bq"
    KERNEL_SP = f"{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_vvl_upgrade"

    # --- Helper Functions ---
    def execute_bq_query(query):
        query_job = BQ_CLIENT.query(query)
        return query_job.result()

    def call_wrapper_sp(stichtag_str=None, log_level=None, job_kennung=None):
        params = []
        if stichtag_str is not None:
            params.append(f"'{stichtag_str}'")
        else:
            params.append("NULL")

        if log_level is not None:
            params.append(f"'{log_level}'")
        else:
            params.append("NULL")

        if job_kennung is not None:
            params.append(f"'{job_kennung}'")
        else:
            params.append("NULL")

        call_sql = f"CALL {WRAPPER_SP}({', '.join(params)})"
        print(f"Executing: {call_sql}")
        try:
            execute_bq_query(call_sql)
            return True, None
        except Exception as e:
            print(f"Error during SP call: {e}")
            return False, str(e)

    def get_job_log_entries(job_number=None, job_identifier=None):
        where_clauses = []
        if job_number is not None:
            where_clauses.append(f"job_number = {job_number}")
        if job_identifier is not None:
            where_clauses.append(f"job_identifier = '{job_identifier}'")

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM {JOB_LOG_TABLE} {where_sql} ORDER BY log_timestamp ASC"
        return list(execute_bq_query(query))

    def get_job_status_entry(job_number, job_identifier):
        query = f"""
            SELECT job_number, job_identifier, status, stichtag, updated_timestamp
            FROM {JOB_STATUS_TABLE}
            WHERE job_number = {job_number} AND job_identifier = '{job_identifier}'
        """
        results = list(execute_bq_query(query))
        return results[0] if results else None

    def get_latest_job_number():
        query = f"SELECT COALESCE(MAX(job_number), 0) FROM {JOB_STATUS_TABLE}"
        result = list(execute_bq_query(query))
        return result[0][0] if result else 0

    # --- Fixtures ---
    @pytest.fixture(scope="module", autouse=True)
    def setup_bigquery_tables():
        # Ensure tables exist
        execute_bq_query(f"""
            CREATE TABLE IF NOT EXISTS `{JOB_LOG_TABLE}` (
                log_timestamp TIMESTAMP NOT NULL,
                job_number INT64,
                job_identifier STRING,
                severity STRING,
                message STRING
            );
        """)
        execute_bq_query(f"""
            CREATE TABLE IF NOT EXISTS `{JOB_STATUS_TABLE}` (
                job_number INT64 NOT NULL,
                job_identifier STRING NOT NULL,
                status STRING NOT NULL,
                stichtag DATE,
                updated_timestamp TIMESTAMP NOT NULL
            );
        """)
        # Ensure kernel SP exists (placeholder)
        execute_bq_query(f"""
            CREATE OR REPLACE PROCEDURE `{KERNEL_SP}`(
                IN p_job_kennung STRING,
                IN p_job_number INT64
            )
            BEGIN
                INSERT INTO `{JOB_LOG_TABLE}` (log_timestamp, job_number, job_identifier, severity, message)
                VALUES (CURRENT_TIMESTAMP(), p_job_number, p_job_kennung, 'WARNING', 'Placeholder: k_ausd_v_ta_vvl_upgrade executed. Add actual logic here.');
                -- SELECT 1 / 0; -- Uncomment for failure tests
            END;
        """)
        # Ensure wrapper SP exists
        with open("procedures/r_ausd_v_ta_vvl_upgrade_bq.sql", "r") as f:
            wrapper_sp_sql = f.read()
            # Replace placeholder project.dataset with actual values
            wrapper_sp_sql = wrapper_sp_sql.replace("`project.dataset.", f"`{PROJECT_ID}.{DATASET_ID}.")
            execute_bq_query(wrapper_sp_sql)

        # Clean up tables before tests
        execute_bq_query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`")
        execute_bq_query(f"TRUNCATE TABLE `{JOB_STATUS_TABLE}`")
        yield
        # Optional: Clean up tables after tests
        # execute_bq_query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`")
        # execute_bq_query(f"TRUNCATE TABLE `{JOB_STATUS_TABLE}`")

    @pytest.fixture(autouse=True)
    def clear_tables_per_test():
        # Clear tables before each test to ensure isolation
        execute_bq_query(f"TRUNCATE TABLE `{JOB_LOG_TABLE}`")
        execute_bq_query(f"TRUNCATE TABLE `{JOB_STATUS_TABLE}`")
        yield
    ```

---

### Test Case 1: Successful Execution with Default Parameters

*   **Purpose:** Verify the BigQuery wrapper script executes successfully with default parameters (no `p_stichtag_str`, no `p_log_level`, no `p_job_kennung`), logs appropriate messages, and updates job status to 'OK'. This covers the basic success path and default job identifier generation.
*   **Setup:**
    *   `job_log` and `job_status` tables are empty.
    *   The `k_ausd_v_ta_vvl_upgrade` stored procedure is configured to succeed (i.e., `SELECT 1/0;` is commented out).
*   **Action:** Call `r_ausd_v_ta_vvl_upgrade_bq` without any parameters.
    ```python
    # In a pytest function:
    success, error_msg = call_wrapper_sp()
    assert success, f"Wrapper SP call failed: {error_msg}"
    ```
*   **Pass/Fail Criterion:**
    1.  The `call_wrapper_sp` function returns `True` (indicating no BigQuery error).
    2.  Exactly one entry exists in `job_status` with `status = 'OK'`.
    3.  Multiple entries exist in `job_log` for the executed job, including:
        *   An 'INFO' message for job start.
        *   An 'INFO' message for calling the kernel script.
        *   A 'WARNING' message from the kernel script (placeholder).
        *   An 'INFO' message for kernel script success.
        *   An 'INFO' message for job completion.
    4.  The `job_identifier` in `job_status` and `job_log` matches the generated default pattern (`YYYYMMDDHHMMSS_r_ausd_v_ta_vvl_upgrade_bq`).
    5.  The `stichtag` in `job_status` is the current date.
    6.  The `job_number` is 1 (as it's the first run).

*   **Runnable Test Code (Pytest):**
    ```python
    def test_successful_execution_default_params():
        initial_max_job_number = get_latest_job_number()

        success, error_msg = call_wrapper_sp()
        assert success, f"Wrapper SP call failed: {error_msg}"

        # Get the job number and identifier from the first log entry
        log_entries = get_job_log_entries()
        assert len(log_entries) >= 5, "Expected at least 5 log entries (start, call kernel, kernel warning, kernel success, job complete)"

        job_number = log_entries[0].job_number
        job_identifier = log_entries[0].job_identifier
        current_date = datetime.date.today()

        # 1. Verify job_status
        status_entry = get_job_status_entry(job_number, job_identifier)
        assert status_entry is not None, "Job status entry not found"
        assert status_entry.status == 'OK', f"Expected status 'OK', got '{status_entry.status}'"
        assert status_entry.stichtag == current_date, f"Expected stichtag {current_date}, got {status_entry.stichtag}"
        assert status_entry.job_number == initial_max_job_number + 1, "Job number did not increment correctly"
        assert job_identifier.startswith(current_date.strftime('%Y%m%d')) and job_identifier.endswith('_r_ausd_v_ta_vvl_upgrade_bq'), \
            f"Job identifier '{job_identifier}' does not match default pattern"

        # 2. Verify job_log entries
        log_messages = [entry.message for entry in log_entries]
        assert any("Starting r_ausd_v_ta_vvl_upgrade_bq" in msg for msg in log_messages), "Job start message missing"
        assert any("Calling kernel script k_ausd_v_ta_vvl_upgrade" in msg for msg in log_messages), "Kernel call message missing"
        assert any("Placeholder: k_ausd_v_ta_vvl_upgrade executed" in msg for msg in log_messages), "Kernel placeholder message missing"
        assert any("Kernel script k_ausd_v_ta_vvl_upgrade completed successfully" in msg for msg in log_messages), "Kernel success message missing"
        assert any(f"Job {job_identifier} (Number: {job_number}) finished with status: OK" in msg for msg in log_messages), "Job completion message missing"

        # Verify severities
        assert log_entries[0].severity == 'INFO' # Start
        assert log_entries[1].severity == 'INFO' # Calling kernel
        assert log_entries[2].severity == 'WARNING' # Kernel placeholder
        assert log_entries[3].severity == 'INFO' # Kernel success
        assert log_entries[4].severity == 'INFO' # Job finished
    ```

---

### Test Case 2: Execution with Custom Parameters

*   **Purpose:** Verify the BigQuery wrapper script correctly processes and uses provided `p_stichtag_str` and `p_job_kennung` parameters.
*   **Setup:**
    *   `job_log` and `job_status` tables are empty.
    *   The `k_ausd_v_ta_vvl_upgrade` stored procedure is configured to succeed.
*   **Action:** Call `r_ausd_v_ta_vvl_upgrade_bq` with a specific `p_stichtag_str` and `p_job_kennung`.
    ```python
    # In a pytest function:
    test_stichtag = "2023-01-15"
    test_job_kennung = "MY_CUSTOM_JOB_ID"
    success, error_msg = call_wrapper_sp(stichtag_str=test_stichtag, job_kennung=test_job_kennung)
    assert success, f"Wrapper SP call failed: {error_msg}"
    ```
*   **Pass/Fail Criterion:**
    1.  The `call_wrapper_sp` function returns `True`.
    2.  Exactly one entry exists in `job_status` with `status = 'OK'`.
    3.  The `job_identifier` in `job_status` and `job_log` is `MY_CUSTOM_JOB_ID`.
    4.  The `stichtag` in `job_status` is `2023-01-15`.
    5.  Log messages correctly reflect the custom `job_identifier` and `stichtag`.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_execution_with_custom_parameters():
        initial_max_job_number = get_latest_job_number()
        test_stichtag_str = "2023-01-15"
        test_stichtag_date = datetime.date(2023, 1, 15)
        test_job_kennung = "MY_CUSTOM_JOB_ID"
        test_log_level = "DEBUG" # Should be accepted but not used by wrapper

        success, error_msg = call_wrapper_sp(
            stichtag_str=test_stichtag_str,
            log_level=test_log_level,
            job_kennung=test_job_kennung
        )
        assert success, f"Wrapper SP call failed: {error_msg}"

        log_entries = get_job_log_entries()
        assert len(log_entries) >= 5, "Expected at least 5 log entries"

        job_number = log_entries[0].job_number
        job_identifier = log_entries[0].job_identifier

        # 1. Verify job_status
        status_entry = get_job_status_entry(job_number, job_identifier)
        assert status_entry is not None, "Job status entry not found"
        assert status_entry.status == 'OK', f"Expected status 'OK', got '{status_entry.status}'"
        assert status_entry.stichtag == test_stichtag_date, f"Expected stichtag {test_stichtag_date}, got {status_entry.stichtag}"
        assert status_entry.job_number == initial_max_job_number + 1, "Job number did not increment correctly"
        assert status_entry.job_identifier == test_job_kennung, f"Expected job_identifier '{test_job_kennung}', got '{status_entry.job_identifier}'"

        # 2. Verify job_log entries
        log_messages = [entry.message for entry in log_entries]
        assert any(f"Starting r_ausd_v_ta_vvl_upgrade_bq (Version: 1.0) for Job: {test_job_kennung}, Number: {job_number}, Stichtag: {test_stichtag_str}" in msg for msg in log_messages), "Job start message missing or incorrect"
        assert any(f"Calling kernel script k_ausd_v_ta_vvl_upgrade with JobKennung={test_job_kennung}, JobNr={job_number}" in msg for msg in log_messages), "Kernel call message missing or incorrect"
        assert any("Placeholder: k_ausd_v_ta_vvl_upgrade executed" in msg for msg in log_messages), "Kernel placeholder message missing"
        assert any(f"Kernel script k_ausd_v_ta_vvl_upgrade completed successfully for Job: {test_job_kennung}, Number: {job_number}." in msg for msg in log_messages), "Kernel success message missing or incorrect"
        assert any(f"Job {test_job_kennung} (Number: {job_number}) finished with status: OK" in msg for msg in log_messages), "Job completion message missing or incorrect"
    ```

---

### Test Case 3: Kernel Script Failure Handling

*   **Purpose:** Verify the BigQuery wrapper script correctly handles errors originating from the invoked kernel script, logs the error, and updates the job status to 'ERROR'.
*   **Setup:**
    *   `job_log` and `job_status` tables are empty.
    *   Modify the `k_ausd_v_ta_vvl_upgrade` stored procedure to intentionally fail (e.g., uncomment `SELECT 1 / 0;`).
    ```sql
    -- Modify KERNEL_SP to fail
    CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_v_ta_vvl_upgrade`(
        IN p_job_kennung STRING,
        IN p_job_number INT64
    )
    BEGIN
        INSERT INTO `your_project.your_dataset.job_log` (log_timestamp, job_number, job_identifier, severity, message)
        VALUES (CURRENT_TIMESTAMP(), p_job_number, p_job_kennung, 'WARNING', 'Placeholder: k_ausd_v_ta_vvl_upgrade executed. Intentionally failing.');
        SELECT 1 / 0; -- Intentional error
    END;
    ```
*   **Action:** Call `r_ausd_v_ta_vvl_upgrade_bq` with default parameters.
    ```python
    # In a pytest function:
    success, error_msg = call_wrapper_sp()
    assert not success, "Wrapper SP call unexpectedly succeeded"
    ```
*   **Pass/Fail Criterion:**
    1.  The `call_wrapper_sp` function returns `False` (indicating a BigQuery error was caught and re-raised).
    2.  Exactly one entry exists in `job_status` with `status = 'ERROR'`.
    3.  `job_log` contains:
        *   An 'INFO' message for job start.
        *   An 'INFO' message for calling the kernel script.
        *   A 'WARNING' message from the kernel script (placeholder).
        *   An 'ERROR' message indicating kernel script failure, including the error details (e.g., "division by zero").
        *   An 'INFO' message for job completion, showing 'ERROR' status.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_kernel_script_failure_handling():
        initial_max_job_number = get_latest_job_number()
        # Temporarily modify kernel SP to fail
        execute_bq_query(f"""
            CREATE OR REPLACE PROCEDURE `{KERNEL_SP}`(
                IN p_job_kennung STRING,
                IN p_job_number INT64
            )
            BEGIN
                INSERT INTO `{JOB_LOG_TABLE}` (log_timestamp, job_number, job_identifier, severity, message)
                VALUES (CURRENT_TIMESTAMP(), p_job_number, p_job_kennung, 'WARNING', 'Placeholder: k_ausd_v_ta_vvl_upgrade executed. Intentionally failing.');
                SELECT 1 / 0; -- Intentional error
            END;
        """)

        success, error_msg = call_wrapper_sp()
        assert not success, "Wrapper SP call unexpectedly succeeded when kernel should fail"
        assert "division by zero" in error_msg or "Division by zero" in error_msg, f"Error message did not contain expected 'division by zero': {error_msg}"

        log_entries = get_job_log_entries()
        assert len(log_entries) >= 5, "Expected at least 5 log entries (start, call kernel, kernel warning, kernel error, job complete)"

        job_number = log_entries[0].job_number
        job_identifier = log_entries[0].job_identifier
        current_date = datetime.date.today()

        # 1. Verify job_status
        status_entry = get_job_status_entry(job_number, job_identifier)
        assert status_entry is not None, "Job status entry not found"
        assert status_entry.status == 'ERROR', f"Expected status 'ERROR', got '{status_entry.status}'"
        assert status_entry.stichtag == current_date, f"Expected stichtag {current_date}, got {status_entry.stichtag}"
        assert status_entry.job_number == initial_max_job_number + 1, "Job number did not increment correctly"

        # 2. Verify job_log entries
        log_messages = [entry.message for entry in log_entries]
        assert any("Starting r_ausd_v_ta_vvl_upgrade_bq" in msg for msg in log_messages), "Job start message missing"
        assert any("Calling kernel script k_ausd_v_ta_vvl_upgrade" in msg for msg in log_messages), "Kernel call message missing"
        assert any("Placeholder: k_ausd_v_ta_vvl_upgrade executed" in msg for msg in log_messages), "Kernel placeholder message missing"
        assert any("ERROR: Kernel script k_ausd_v_ta_vvl_upgrade failed" in msg for msg in log_messages), "Kernel failure message missing"
        assert any("division by zero" in msg for msg in log_messages), "Specific error detail 'division by zero' missing from log"
        assert any(f"Job {job_identifier} (Number: {job_number}) finished with status: ERROR" in msg for msg in log_messages), "Job completion message missing or incorrect"

        # Verify severities
        assert log_entries[0].severity == 'INFO' # Start
        assert log_entries[1].severity == 'INFO' # Calling kernel
        assert log_entries[2].severity == 'WARNING' # Kernel placeholder
        assert log_entries[3].severity == 'ERROR' # Kernel failure
        assert log_entries[4].severity == 'INFO' # Job finished (with ERROR status)

        # Restore kernel SP to success state for other tests
        execute_bq_query(f"""
            CREATE OR REPLACE PROCEDURE `{KERNEL_SP}`(
                IN p_job_kennung STRING,
                IN p_job_number INT64
            )
            BEGIN
                INSERT INTO `{JOB_LOG_TABLE}` (log_timestamp, job_number, job_identifier, severity, message)
                VALUES (CURRENT_TIMESTAMP(), p_job_number, p_job_kennung, 'WARNING', 'Placeholder: k_ausd_v_ta_vvl_upgrade executed. Add actual logic here.');
            END;
        """)
    ```

---

### Test Case 4: Job Number Increment and Uniqueness

*   **Purpose:** Verify that `job_number` is correctly generated as an incrementing sequence based on the `job_status` table, ensuring uniqueness for each job run.
*   **Setup:**
    *   `job_log` and `job_status` tables are empty.
    *   The `k_ausd_v_ta_vvl_upgrade` stored procedure is configured to succeed.
*   **Action:** Execute `r_ausd_v_ta_vvl_upgrade_bq` multiple times consecutively.
    ```python
    # In a pytest function:
    call_wrapper_sp(job_kennung="RUN_1")
    call_wrapper_sp(job_kennung="RUN_2")
    call_wrapper_sp(job_kennung="RUN_3")
    ```
*   **Pass/Fail Criterion:**
    1.  Three distinct entries exist in `job_status`.
    2.  Their `job_number` values are sequential (e.g., 1, 2, 3).
    3.  Each `job_status` entry has `status = 'OK'`.
    4.  Corresponding log entries exist for each job number.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_job_number_increment_and_uniqueness():
        initial_max_job_number = get_latest_job_number()

        # Run 1
        success1, _ = call_wrapper_sp(job_kennung="JOB_RUN_1")
        assert success1, "First job run failed"
        time.sleep(1) # Ensure timestamps are distinct for default job_identifier

        # Run 2
        success2, _ = call_wrapper_sp(job_kennung="JOB_RUN_2")
        assert success2, "Second job run failed"
        time.sleep(1)

        # Run 3
        success3, _ = call_wrapper_sp(job_kennung="JOB_RUN_3")
        assert success3, "Third job run failed"

        # Verify job_status entries
        status_entries_query = f"SELECT job_number, job_identifier, status FROM {JOB_STATUS_TABLE} ORDER BY job_number ASC"
        status_entries = list(execute_bq_query(status_entries_query))

        assert len(status_entries) == 3, f"Expected 3 job status entries, got {len(status_entries)}"

        # Check job numbers and status
        expected_job_numbers = [initial_max_job_number + 1, initial_max_job_number + 2, initial_max_job_number + 3]
        for i, entry in enumerate(status_entries):
            assert entry.job_number == expected_job_numbers[i], f"Expected job_number {expected_job_numbers[i]}, got {entry.job_number}"
            assert entry.status == 'OK', f"Expected status 'OK' for job {entry.job_number}, got '{entry.status}'"
            assert entry.job_identifier == f"JOB_RUN_{i+1}", f"Expected job_identifier 'JOB_RUN_{i+1}', got '{entry.job_identifier}'"

        # Verify log entries for each job
        for i in range(3):
            job_num = expected_job_numbers[i]
            job_id = f"JOB_RUN_{i+1}"
            log_entries = get_job_log_entries(job_number=job_num, job_identifier=job_id)
            assert len(log_entries) >= 5, f"Expected at least 5 log entries for job {job_num}"
            assert any(f"Job {job_id} (Number: {job_num}) finished with status: OK" in entry.message for entry in log_entries)
    ```

---

### Test Case 5: Invalid `p_stichtag_str` Handling

*   **Purpose:** Verify that providing an invalid date string for `p_stichtag_str` results in a BigQuery error, and the job status is correctly updated to 'ERROR'.
*   **Setup:**
    *   `job_log` and `job_status` tables are empty.
    *   The `k_ausd_v_ta_vvl_upgrade` stored procedure is configured to succeed (it shouldn't even be called in this case).
*   **Action:** Call `r_ausd_v_ta_vvl_upgrade_bq` with an invalid date string.
    ```python
    # In a pytest function:
    invalid_stichtag = "2023/13/01" # Invalid month
    success, error_msg = call_wrapper_sp(stichtag_str=invalid_stichtag)
    assert not success, "Wrapper SP call unexpectedly succeeded with invalid stichtag"
    ```
*   **Pass/Fail Criterion:**
    1.  The `call_wrapper_sp` function returns `False`.
    2.  The error message contains "Invalid date" or similar parsing error.
    3.  A `job_status` entry might or might not be created depending on when the error occurs. If created, its status should be 'ERROR'. (In BigQuery, `PARSE_DATE` errors before the `BEGIN...EXCEPTION` block can prevent `job_status` insertion). The current code inserts `job_status` *before* the `BEGIN` block, so it should be `RUNNING` initially, then `ERROR`.
    4.  `job_log` should contain an 'ERROR' message related to the date parsing.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_invalid_stichtag_string_handling():
        initial_max_job_number = get_latest_job_number()
        invalid_stichtag_str = "2023/13/01" # Invalid month format for YYYY-MM-DD

        success, error_msg = call_wrapper_sp(stichtag_str=invalid_stichtag_str, job_kennung="INVALID_DATE_JOB")
        assert not success, "Wrapper SP call unexpectedly succeeded with invalid stichtag"
        assert "Invalid date" in error_msg or "Failed to parse" in error_msg, f"Error message did not contain expected date parsing error: {error_msg}"

        # Check job_status - it should have been inserted as RUNNING and then updated to ERROR
        # The error occurs during PARSE_DATE, which is before the BEGIN...EXCEPTION block.
        # This means the initial INSERT into job_status happens, but the subsequent UPDATE to OK/ERROR
        # in the EXCEPTION block might not catch it if the error is too early.
        # Let's re-evaluate the BQ code: PARSE_DATE is before the BEGIN block.
        # So, the SP will fail *before* the BEGIN block, and the EXCEPTION block won't catch it.
        # This means no 'ERROR' status update will occur *within* the SP's EXCEPTION block.
        # The job_status will remain 'RUNNING' if the initial insert happened, or not exist if it failed earlier.
        # This is a design flaw in the BQ migration if the intent was to always log an ERROR status.

        # Re-reading the BQ code:
        # SET v_stichtag = PARSE_DATE('%Y-%m-%d', p_stichtag_str); -- This is outside BEGIN...EXCEPTION
        # INSERT INTO `project.dataset.job_status` ... 'RUNNING' -- This is outside BEGIN...EXCEPTION
        # So, if PARSE_DATE fails, the SP execution will terminate *before* the BEGIN block,
        # and the job_status will *not* be updated to ERROR by the SP itself.
        # The external caller (pytest) will catch the error.

        # Let's test for the current behavior, and note this as a potential improvement.
        # The job_status table should contain an entry with 'RUNNING' status if the initial insert happened.
        # However, the BigQuery `CALL` statement itself will fail and roll back any changes if an error occurs
        # *before* a transaction is explicitly started or before the EXCEPTION block is entered.
        # So, if PARSE_DATE fails, the entire SP call will fail and no rows should be inserted.

        # Let's verify no entries were inserted at all.
        log_entries = get_job_log_entries()
        status_entries_query = f"SELECT * FROM {JOB_STATUS_TABLE}"
        status_entries = list(execute_bq_query(status_entries_query))

        assert len(log_entries) == 0, "No log entries expected if PARSE_DATE fails before logging starts"
        assert len(status_entries) == 0, "No status entries expected if PARSE_DATE fails before status is set"

        # This highlights a difference: the legacy script would log an error via DWMSG_MeldeFehler
        # even for parameter parsing issues. The BQ script fails hard before logging.
        # This is a behavioral difference that should be documented or addressed.
        # For this test, we assert the current BQ behavior.
    ```

---

### Test Case 6: Schema Validation of `job_log` and `job_status`

*   **Purpose:** Verify that the `job_log` and `job_status` tables exist and have the correct schema as defined in the DDL. This is a data quality/schema assertion.
*   **Setup:**
    *   BigQuery client is configured.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for table and column details.
    ```python
    # In a pytest function:
    # (This is handled by the setup_bigquery_tables fixture, but can be explicitly asserted)
    ```
*   **Pass/Fail Criterion:**
    1.  `job_log` table exists.
    2.  `job_log` has columns: `log_timestamp` (TIMESTAMP), `job_number` (INT64), `job_identifier` (STRING), `severity` (STRING), `message` (STRING).
    3.  `job_status` table exists.
    4.  `job_status` has columns: `job_number` (INT64), `job_identifier` (STRING), `status` (STRING), `stichtag` (DATE), `updated_timestamp` (TIMESTAMP).
    5.  All columns have the expected data types and nullability (as per DDL).

*   **Runnable Test Code (Pytest):**
    ```python
    def test_job_log_schema_validation():
        table = BQ_CLIENT.get_table(JOB_LOG_TABLE)
        assert table is not None, f"Table {JOB_LOG_TABLE} does not exist"

        expected_schema = {
            "log_timestamp": ("TIMESTAMP", "REQUIRED"),
            "job_number": ("INT64", "NULLABLE"),
            "job_identifier": ("STRING", "NULLABLE"),
            "severity": ("STRING", "NULLABLE"),
            "message": ("STRING", "NULLABLE"),
        }
        actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}

        for col_name, (col_type, col_mode) in expected_schema.items():
            assert col_name in actual_schema, f"Column {col_name} missing from {JOB_LOG_TABLE}"
            assert actual_schema[col_name][0] == col_type, f"Column {col_name} type mismatch: Expected {col_type}, Got {actual_schema[col_name][0]}"
            assert actual_schema[col_name][1] == col_mode, f"Column {col_name} nullability mismatch: Expected {col_mode}, Got {actual_schema[col_name][1]}"

    def test_job_status_schema_validation():
        table = BQ_CLIENT.get_table(JOB_STATUS_TABLE)
        assert table is not None, f"Table {JOB_STATUS_TABLE} does not exist"

        expected_schema = {
            "job_number": ("INT64", "REQUIRED"),
            "job_identifier": ("STRING", "REQUIRED"),
            "status": ("STRING", "REQUIRED"),
            "stichtag": ("DATE", "NULLABLE"),
            "updated_timestamp": ("TIMESTAMP", "REQUIRED"),
        }
        actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}

        for col_name, (col_type, col_mode) in expected_schema.items():
            assert col_name in actual_schema, f"Column {col_name} missing from {JOB_STATUS_TABLE}"
            assert actual_schema[col_name][0] == col_type, f"Column {col_name} type mismatch: Expected {col_type}, Got {actual_schema[col_name][0]}"
            assert actual_schema[col_name][1] == col_mode, f"Column {col_name} nullability mismatch: Expected {col_mode}, Got {actual_schema[col_name][1]}"
    ```

---

### Test Case 7: `p_log_level` Parameter Handling (No Impact)

*   **Purpose:** Verify that the `p_log_level` parameter can be passed to the BigQuery stored procedure without causing errors, even though the wrapper itself does not explicitly use it in its current implementation. This addresses the "unhandled parameters" risk from the design.
*   **Setup:**
    *   `job_log` and `job_status` tables are empty.
    *   The `k_ausd_v_ta_vvl_upgrade` stored procedure is configured to succeed.
*   **Action:** Call `r_ausd_v_ta_vvl_upgrade_bq` with a custom `p_log_level` value.
    ```python
    # In a pytest function:
    test_log_level = "DEBUG"
    success, error_msg = call_wrapper_sp(log_level=test_log_level, job_kennung="LOG_LEVEL_TEST")
    assert success, f"Wrapper SP call failed with log_level: {error_msg}"
    ```
*   **Pass/Fail Criterion:**
    1.  The `call_wrapper_sp` function returns `True`.
    2.  The job completes successfully with `status = 'OK'` in `job_status`.
    3.  No errors related to `p_log_level` are observed in the BigQuery job logs or `job_log` table.

*   **Runnable Test Code (Pytest):**
    ```python
    def test_log_level_parameter_handling():
        initial_max_job_number = get_latest_job_number()
        test_log_level = "DEBUG"
        test_job_kennung = "LOG_LEVEL_TEST_JOB"

        success, error_msg = call_wrapper_sp(log_level=test_log_level, job_kennung=test_job_kennung)
        assert success, f"Wrapper SP call failed with log_level '{test_log_level}': {error_msg}"

        log_entries = get_job_log_entries()
        assert len(log_entries) >= 5, "Expected at least 5 log entries"

        job_number = log_entries[0].job_number
        job_identifier = log_entries[0].job_identifier

        # 1. Verify job_status
        status_entry = get_job_status_entry(job_number, job_identifier)
        assert status_entry is not None, "Job status entry not found"
        assert status_entry.status == 'OK', f"Expected status 'OK', got '{status_entry.status}'"
        assert status_entry.job_identifier == test_job_kennung, f"Expected job_identifier '{test_job_kennung}', got '{status_entry.job_identifier}'"

        # 2. Verify no errors related to log_level
        log_messages = [entry.message for entry in log_entries]
        assert not any("error" in msg.lower() and "log_level" in msg.lower() for msg in log_messages), "Error related to log_level found in logs"
    ```

---

**Summary of Behavioral Differences/Risks Identified During Testing:**

1.  **Default `JobKennung` / `job_identifier`:** The legacy script uses a fixed `BERT_V_TA_VVL_UPGRADE` by default. The BigQuery script generates a timestamp-based identifier if `p_job_kennung` is NULL. This is a behavioral change that should be explicitly confirmed with stakeholders.
2.  **Parameter Validation Error Handling:** The legacy script would use `DWMSG_MeldeFehler` for parameter parsing errors (e.g., missing arguments, unknown parameters). The BigQuery script, if `PARSE_DATE` fails *before* the `BEGIN...EXCEPTION` block, will terminate the entire stored procedure call, rolling back any changes and preventing the `job_status` table from being updated to 'ERROR' by the SP itself. This is a significant difference in error reporting and robustness. It might be desirable to move parameter parsing *inside* the `BEGIN...EXCEPTION` block or implement more robust pre-validation.
3.  **`p_log_level` Usage:** The `p_log_level` parameter is accepted but not used by the wrapper. While not an error, it's an unused parameter that might indicate incomplete migration of the `-l` functionality from the legacy script.

These tests provide a solid foundation for validating the migrated orchestration logic. Further tests would be required for the `k_ausd_v_ta_vvl_upgrade` stored procedure once its actual logic is migrated.