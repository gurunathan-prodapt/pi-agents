The migration of `r_ausd_bp_ta_bpr_optionen.ksh` to a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_bpr_optionen_wrapper`) primarily involves translating orchestration, parameter handling, logging, and error trapping logic. The actual data provisioning logic, residing in `k_ausd_bp_ta_bpr_optionen.ksh`, is assumed to be migrated to a separate callable BigQuery component.

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery wrapper procedure against the legacy KornShell script, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

### **General Test Setup & Prerequisites**

Before running the tests, ensure the following:

1.  **BigQuery Project and Dataset:** A dedicated BigQuery project and dataset (e.g., `project.dataset`) are set up for testing.
2.  **DDL Execution:** The DDL scripts for `job_control.sql` and `job_log.sql` have been executed in the test dataset.
3.  **Mock Core Procedure:** A mock BigQuery Stored Procedure for `project.dataset.k_ausd_bp_ta_bpr_optionen` is created. This mock will allow us to verify parameters passed to the core logic and simulate success/failure scenarios without needing the actual core logic.

    ```sql
    -- Mock table to capture parameters passed to k_ausd_bp_ta_bpr_optionen
    CREATE OR REPLACE TABLE `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls` (
        call_id INT64 OPTIONS(description="Unique ID for each call"),
        job_kennung STRING,
        stichtag STRING,
        dw_eintrags_nr INT64,
        wiederanlauf_wert INT64,
        call_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        simulate_error BOOL DEFAULT FALSE -- Control flag for mock behavior
    );

    -- Mock stored procedure for k_ausd_bp_ta_bpr_optionen
    CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_optionen`(
        IN p_job_kennung STRING,
        IN p_stichtag STRING,
        IN p_dw_eintrags_nr INT64,
        IN p_wiederanlauf_wert INT64
    )
    BEGIN
        DECLARE v_simulate_error BOOL;
        DECLARE v_call_id INT64;

        -- Get a unique call_id
        SELECT IFNULL(MAX(call_id), 0) + 1 INTO v_call_id FROM `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`;

        -- Simulate error if p_stichtag is 'ERROR'
        SET v_simulate_error = (p_stichtag = 'ERROR');

        INSERT INTO `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls` (
            call_id, job_kennung, stichtag, dw_eintrags_nr, wiederanlauf_wert, simulate_error
        ) VALUES (
            v_call_id, p_job_kennung, p_stichtag, p_dw_eintrags_nr, p_wiederanlauf_wert, v_simulate_error
        );

        IF v_simulate_error THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from k_ausd_bp_ta_bpr_optionen';
        END IF;
    END;
    ```

4.  **Wrapper Procedure Deployment:** The `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` stored procedure is deployed.
5.  **Test Runner:** A test runner (e.g., Python with `pytest` and `google-cloud-bigquery` client library) is configured to execute the tests. Each test should clear the `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables before execution to ensure isolation.

---

### **Test Case 1: Schema Validation for Job Control and Log Tables**

*   **Purpose:** Verify that the DDL for `job_control` and `job_log` tables creates tables with the expected schema, data types, and nullability constraints. This ensures data quality and compatibility with the stored procedure's `INSERT`/`UPDATE` statements.
*   **Setup:**
    1.  Ensure the `project.dataset` exists.
    2.  Run the DDL scripts for `job_control.sql` and `job_log.sql`.
*   **Action:** Query the BigQuery information schema for the created tables.
*   **Pass/Fail Criterion:** The queried schema matches the expected schema (column names, data types, nullability) as defined in the DDL.

    ```python
    # Pytest example (conceptual, requires BigQuery client library)
    import pytest
    from google.cloud import bigquery

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client()

    def test_job_control_schema(bq_client):
        table_id = "project.dataset.job_control"
        table = bq_client.get_table(table_id)

        expected_schema = {
            "job_nr": ("INT64", "REQUIRED"),
            "job_kennung": ("STRING", "REQUIRED"),
            "script_name": ("STRING", "REQUIRED"),
            "log_file": ("STRING", "NULLABLE"),
            "stichtag_info": ("STRING", "NULLABLE"),
            "status": ("STRING", "REQUIRED"),
            "created_at": ("TIMESTAMP", "REQUIRED"),
            "finished_at": ("TIMESTAMP", "NULLABLE"),
        }

        actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}
        assert actual_schema == expected_schema, f"Schema mismatch for {table_id}"

    def test_job_log_schema(bq_client):
        table_id = "project.dataset.job_log"
        table = bq_client.get_table(table_id)

        expected_schema = {
            "job_nr": ("INT64", "REQUIRED"),
            "job_kennung": ("STRING", "REQUIRED"),
            "log_level": ("STRING", "REQUIRED"),
            "message": ("STRING", "REQUIRED"),
            "created_at": ("TIMESTAMP", "REQUIRED"),
        }

        actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}
        assert actual_schema == expected_schema, f"Schema mismatch for {table_id}"
    ```

---

### **Test Case 2: Successful Execution with All Parameters Provided**

*   **Purpose:** Verify that the wrapper procedure executes successfully when all optional parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`) are explicitly provided. This ensures output parity and transformation correctness for parameter handling.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_optionen` procedure is configured to succeed.
*   **Action:** Call `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` with specific `p_stichtag_in` and `p_wiederanlaufWert_in` values.
*   **Pass/Fail Criterion:**
    1.  The `job_control` table contains one entry with `status = 'OK'`, `script_name = 'r_ausd_bp_ta_bpr_optionen.ksh'`, and `stichtag_info` matching the provided `p_stichtag_in`.
    2.  The `job_log` table contains at least two entries (start and success messages) for the `job_nr` from `job_control`.
    3.  The `mock_k_ausd_bp_ta_bpr_optionen_calls` table contains one entry where `stichtag` and `wiederanlauf_wert` match the provided input, and `job_kennung` and `dw_eintrags_nr` match the generated values.
    4.  The procedure completes without raising an error.

    ```python
    # Pytest example
    import pytest
    from google.cloud import bigquery
    import datetime

    @pytest.fixture(autouse=True)
    def setup_and_teardown_tables(bq_client):
        # Clear tables before each test
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result()
        yield

    def test_successful_execution_all_params(bq_client):
        test_stichtag = "01012023"
        test_wiederanlaufwert = 12345

        # Action: Call the wrapper procedure
        query = f"""
        CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
            p_stichtag_in => '{test_stichtag}',
            p_wiederanlaufWert_in => {test_wiederanlaufwert}
        );
        """
        bq_client.query(query).result()

        # Assertions
        job_control_rows = list(bq_client.query("SELECT * FROM `project.dataset.job_control`").result())
        assert len(job_control_rows) == 1
        job_control_entry = job_control_rows[0]
        assert job_control_entry.status == "OK"
        assert job_control_entry.script_name == "r_ausd_bp_ta_bpr_optionen.ksh"
        assert job_control_entry.stichtag_info == test_stichtag
        assert job_control_entry.finished_at is not None

        job_log_rows = list(bq_client.query(f"SELECT * FROM `project.dataset.job_log` WHERE job_nr = {job_control_entry.job_nr} ORDER BY created_at").result())
        assert len(job_log_rows) >= 2
        assert job_log_rows[0].log_level == "INFO"
        assert "Job started" in job_log_rows[0].message
        assert job_log_rows[-1].log_level == "INFO"
        assert "Job completed successfully" in job_log_rows[-1].message

        mock_calls_rows = list(bq_client.query("SELECT * FROM `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result())
        assert len(mock_calls_rows) == 1
        mock_call_entry = mock_calls_rows[0]
        assert mock_call_entry.stichtag == test_stichtag
        assert mock_call_entry.wiederanlauf_wert == test_wiederanlaufwert
        assert mock_call_entry.job_kennung == job_control_entry.job_kennung
        assert mock_call_entry.dw_eintrags_nr == job_control_entry.job_nr
    ```

---

### **Test Case 3: Successful Execution with Default Stichtag**

*   **Purpose:** Verify that when `p_stichtag_in` is not provided, the wrapper correctly defaults it to the current system date (DDMMYYYY format) and passes this default value to the core logic and logs it. This tests transformation correctness for date handling and defaulting.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_optionen` procedure is configured to succeed.
*   **Action:** Call `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` with `p_stichtag_in => NULL` and a specific `p_wiederanlaufWert_in`.
*   **Pass/Fail Criterion:**
    1.  The `job_control` table contains one entry with `status = 'OK'` and `stichtag_info` matching `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` at the time of execution.
    2.  The `job_log` table contains appropriate entries.
    3.  The `mock_k_ausd_bp_ta_bpr_optionen_calls` table contains one entry where `stichtag` matches the current system date (DDMMYYYY) and `wiederanlauf_wert` matches the provided input.

    ```python
    # Pytest example
    def test_successful_execution_default_stichtag(bq_client):
        test_wiederanlaufwert = 67890
        expected_stichtag = datetime.datetime.now().strftime("%d%m%Y")

        # Action: Call the wrapper procedure with NULL stichtag
        query = f"""
        CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
            p_stichtag_in => NULL,
            p_wiederanlaufWert_in => {test_wiederanlaufwert}
        );
        """
        bq_client.query(query).result()

        # Assertions
        job_control_rows = list(bq_client.query("SELECT * FROM `project.dataset.job_control`").result())
        assert len(job_control_rows) == 1
        job_control_entry = job_control_rows[0]
        assert job_control_entry.status == "OK"
        assert job_control_entry.stichtag_info == expected_stichtag

        mock_calls_rows = list(bq_client.query("SELECT * FROM `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result())
        assert len(mock_calls_rows) == 1
        mock_call_entry = mock_calls_rows[0]
        assert mock_call_entry.stichtag == expected_stichtag
        assert mock_call_entry.wiederanlauf_wert == test_wiederanlaufwert
    ```

---

### **Test Case 4: Successful Execution with Default Wiederanlaufwert**

*   **Purpose:** Verify that when `p_wiederanlaufWert_in` is not provided, the wrapper correctly defaults it to `0` and passes this default value to the core logic and logs it. This tests transformation correctness for numeric defaulting.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_optionen` procedure is configured to succeed.
*   **Action:** Call `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` with a specific `p_stichtag_in` and `p_wiederanlaufWert_in => NULL`.
*   **Pass/Fail Criterion:**
    1.  The `job_control` table contains one entry with `status = 'OK'`.
    2.  The `job_log` table contains appropriate entries.
    3.  The `mock_k_ausd_bp_ta_bpr_optionen_calls` table contains one entry where `stichtag` matches the provided input and `wiederanlauf_wert` is `0`.

    ```python
    # Pytest example
    def test_successful_execution_default_wiederanlaufwert(bq_client):
        test_stichtag = "15032024"
        expected_wiederanlaufwert = 0

        # Action: Call the wrapper procedure with NULL wiederanlaufWert
        query = f"""
        CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
            p_stichtag_in => '{test_stichtag}',
            p_wiederanlaufWert_in => NULL
        );
        """
        bq_client.query(query).result()

        # Assertions
        job_control_rows = list(bq_client.query("SELECT * FROM `project.dataset.job_control`").result())
        assert len(job_control_rows) == 1
        job_control_entry = job_control_rows[0]
        assert job_control_entry.status == "OK"
        assert job_control_entry.stichtag_info == test_stichtag

        mock_calls_rows = list(bq_client.query("SELECT * FROM `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result())
        assert len(mock_calls_rows) == 1
        mock_call_entry = mock_calls_rows[0]
        assert mock_call_entry.stichtag == test_stichtag
        assert mock_call_entry.wiederanlauf_wert == expected_wiederanlaufwert
    ```

---

### **Test Case 5: Core Script Failure Handling**

*   **Purpose:** Verify that if the invoked core script (`k_ausd_bp_ta_bpr_optionen`) fails, the wrapper correctly catches the error, updates the `job_control` status to 'FAILED', logs the error, and re-raises the error to the caller, mimicking the legacy script's error handling (`trap` and `exit`). This covers transformation correctness for error handling and external system replacement behavior.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
    2.  Configure the mock `k_ausd_bp_ta_bpr_optionen` procedure to simulate a failure (e.g., by passing a special `stichtag` value like 'ERROR').
*   **Action:** Call `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` with parameters that trigger the mock core script to fail.
*   **Pass/Fail Criterion:**
    1.  The call to the wrapper procedure raises an error (e.g., `google.api_core.exceptions.InternalServerError` for BigQuery stored procedure errors).
    2.  The `job_control` table contains one entry with `status = 'FAILED'` and `finished_at` is set.
    3.  The `job_log` table contains an 'ERROR' level entry with a message indicating the failure, linked to the `job_nr`.
    4.  The `mock_k_ausd_bp_ta_bpr_optionen_calls` table contains one entry with the parameters passed, and `simulate_error` is TRUE.

    ```python
    # Pytest example
    def test_core_script_failure_handling(bq_client):
        test_stichtag_for_error = "ERROR"
        test_wiederanlaufwert = 99999

        # Action: Call the wrapper procedure, expecting an error
        query = f"""
        CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
            p_stichtag_in => '{test_stichtag_for_error}',
            p_wiederanlaufWert_in => {test_wiederanlaufwert}
        );
        """
        with pytest.raises(Exception) as excinfo:
            bq_client.query(query).result()

        assert "Simulated error from k_ausd_bp_ta_bpr_optionen" in str(excinfo.value)

        # Assertions
        job_control_rows = list(bq_client.query("SELECT * FROM `project.dataset.job_control`").result())
        assert len(job_control_rows) == 1
        job_control_entry = job_control_rows[0]
        assert job_control_entry.status == "FAILED"
        assert job_control_entry.finished_at is not None

        job_log_rows = list(bq_client.query(f"SELECT * FROM `project.dataset.job_log` WHERE job_nr = {job_control_entry.job_nr} ORDER BY created_at").result())
        assert any("Job failed with error" in row.message and row.log_level == "ERROR" for row in job_log_rows)
        assert any("Simulated error from k_ausd_bp_ta_bpr_optionen" in row.message for row in job_log_rows)

        mock_calls_rows = list(bq_client.query("SELECT * FROM `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result())
        assert len(mock_calls_rows) == 1
        mock_call_entry = mock_calls_rows[0]
        assert mock_call_entry.stichtag == test_stichtag_for_error
        assert mock_call_entry.wiederanlauf_wert == test_wiederanlaufwert
        assert mock_call_entry.simulate_error is True
    ```

---

### **Test Case 6: `DW_EintragsNr` Generation (Sequential Increment)**

*   **Purpose:** Verify that `DW_EintragsNr` (job_nr) is correctly generated as a sequential increment based on the maximum existing `job_nr` in `job_control`. This tests transformation correctness for job tracking.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
    2.  Insert a dummy entry into `job_control` with a known `job_nr` (e.g., 100).
    3.  Ensure the mock `k_ausd_bp_ta_bpr_optionen` procedure is configured to succeed.
*   **Action:** Call `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` twice.
*   **Pass/Fail Criterion:**
    1.  The first call results in a `job_nr` of 101.
    2.  The second call results in a `job_nr` of 102.
    3.  All other assertions (status, logging) are as expected for successful runs.

    ```python
    # Pytest example
    def test_dw_eintragsnr_generation(bq_client):
        # Setup: Insert a base job_nr
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result()

        bq_client.query("""
        INSERT INTO `project.dataset.job_control` (job_nr, job_kennung, script_name, status, created_at)
        VALUES (100, 'base_job', 'base_script.ksh', 'OK', CURRENT_TIMESTAMP());
        """).result()

        # Action 1: First call
        bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(p_stichtag_in => '01012024', p_wiederanlaufWert_in => 1);").result()

        # Assertions for first call
        job_control_rows_1 = list(bq_client.query("SELECT job_nr FROM `project.dataset.job_control` WHERE job_kennung LIKE 'r_ausd_bp_ta_bpr_optionen%'").result())
        assert len(job_control_rows_1) == 1
        assert job_control_rows_1[0].job_nr == 101

        # Action 2: Second call
        bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(p_stichtag_in => '02012024', p_wiederanlaufWert_in => 2);").result()

        # Assertions for second call
        job_control_rows_2 = list(bq_client.query("SELECT job_nr FROM `project.dataset.job_control` WHERE job_kennung LIKE 'r_ausd_bp_ta_bpr_optionen%' ORDER BY job_nr DESC").result())
        assert len(job_control_rows_2) == 2
        assert job_control_rows_2[0].job_nr == 102
        assert job_control_rows_2[1].job_nr == 101

        total_job_control_rows = list(bq_client.query("SELECT COUNT(*) as count FROM `project.dataset.job_control`").result())[0].count
        assert total_job_control_rows == 3
    ```

---

### **Test Case 7: `JobKennung` Format and Uniqueness**

*   **Purpose:** Verify that `v_job_kennung` is generated correctly by combining the script name and a timestamp, ensuring it's unique per run. This tests transformation correctness for identifier generation.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_optionen` procedure is configured to succeed.
*   **Action:** Call `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` twice in quick succession.
*   **Pass/Fail Criterion:**
    1.  Each call results in a distinct `job_kennung` in `job_control`.
    2.  The `job_kennung` follows the pattern `r_ausd_bp_ta_bpr_optionen_<YYYYMMDDHHMMSS>`.
    3.  The timestamp part of `job_kennung` is close to the actual execution time.

    ```python
    # Pytest example
    import time

    def test_job_kennung_format_and_uniqueness(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result()

        # Action 1
        bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(p_stichtag_in => '01012024', p_wiederanlaufWert_in => 1);").result()
        time.sleep(1) # Ensure distinct timestamps for uniqueness check

        # Action 2
        bq_client.query("CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(p_stichtag_in => '02012024', p_wiederanlaufWert_in => 2);").result()

        # Assertions
        job_control_rows = list(bq_client.query("SELECT job_kennung, created_at FROM `project.dataset.job_control` WHERE script_name = 'r_ausd_bp_ta_bpr_optionen.ksh' ORDER BY created_at").result())
        assert len(job_control_rows) == 2

        job_kennung_1 = job_control_rows[0].job_kennung
        job_kennung_2 = job_control_rows[1].job_kennung

        assert job_kennung_1 != job_kennung_2

        assert job_kennung_1.startswith("r_ausd_bp_ta_bpr_optionen_")
        assert len(job_kennung_1) == len("r_ausd_bp_ta_bpr_optionen_") + 14

        timestamp_part_1 = job_kennung_1.split('_')[-1]
        created_at_dt_1 = job_control_rows[0].created_at.strftime('%Y%m%d%H%M%S')
        assert created_at_dt_1[:10] == timestamp_part_1[:10]
    ```

---

### **Test Case 8: Logging Content Verification**

*   **Purpose:** Verify that the log messages written to `job_log` table contain the expected information for start, success, and error scenarios, including parameters. This covers output parity and data quality.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
    2.  Run one successful execution and one failed execution (using the mock).
*   **Action:** Query the `job_log` table for the entries corresponding to these runs.
*   **Pass/Fail Criterion:**
    1.  Successful run's log entries contain "Job started. Stichtag: <value>, Wiederanlaufwert: <value>" and "Job completed successfully.".
    2.  Failed run's log entries contain "Job started..." and "Job failed with error: Simulated error from k_ausd_bp_ta_bpr_optionen".
    3.  Log levels (`INFO`, `ERROR`) are correctly assigned.

    ```python
    # Pytest example
    def test_logging_content_verification(bq_client):
        bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result()

        # Run a successful case
        test_stichtag_success = "05052025"
        test_wiederanlauf_success = 10
        bq_client.query(f"""
        CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
            p_stichtag_in => '{test_stichtag_success}',
            p_wiederanlaufWert_in => {test_wiederanlauf_success}
        );
        """).result()

        # Run a failed case
        test_stichtag_fail = "ERROR"
        test_wiederanlauf_fail = 20
        with pytest.raises(Exception):
            bq_client.query(f"""
            CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
                p_stichtag_in => '{test_stichtag_fail}',
                p_wiederanlaufWert_in => {test_wiederanlauf_fail}
            );
            """).result()

        # Assertions for successful run logs
        success_job_nr = list(bq_client.query("SELECT job_nr FROM `project.dataset.job_control` WHERE status = 'OK'").result())[0].job_nr
        success_logs = list(bq_client.query(f"SELECT log_level, message FROM `project.dataset.job_log` WHERE job_nr = {success_job_nr} ORDER BY created_at").result())
        assert any(f"Job started. Stichtag: {test_stichtag_success}, Wiederanlaufwert: {test_wiederanlauf_success}" in log.message and log.log_level == "INFO" for log in success_logs)
        assert any("Job completed successfully." in log.message and log.log_level == "INFO" for log in success_logs)

        # Assertions for failed run logs
        fail_job_nr = list(bq_client.query("SELECT job_nr FROM `project.dataset.job_control` WHERE status = 'FAILED'").result())[0].job_nr
        fail_logs = list(bq_client.query(f"SELECT log_level, message FROM `project.dataset.job_log` WHERE job_nr = {fail_job_nr} ORDER BY created_at").result())
        assert any(f"Job started. Stichtag: {test_stichtag_fail}, Wiederanlaufwert: {test_wiederanlauf_fail}" in log.message and log.log_level == "INFO" for log in fail_logs)
        assert any("Job failed with error: Simulated error from k_ausd_bp_ta_bpr_optionen" in log.message and log.log_level == "ERROR" for log in fail_logs)
    ```

---

### **Test Case 9: Type Handling for `p_wiederanlaufWert_in` (Invalid Input)**

*   **Purpose:** Verify that the BigQuery stored procedure correctly handles the `p_wiederanlaufWert_in` parameter, specifically its `INT64` type. This ensures type correctness and robust error handling for invalid input, which would cause the *call to the procedure itself* to fail, preventing any job tracking.
*   **Setup:**
    1.  Clear `job_control`, `job_log`, and `mock_k_ausd_bp_ta_bpr_optionen_calls` tables.
*   **Action:** Attempt to call `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` with a non-integer string for `p_wiederanlaufWert_in`.
*   **Pass/Fail Criterion:**
    1.  The call to the wrapper procedure raises a BigQuery error indicating a type conversion failure (e.g., "Bad int64 value" or "Cannot parse").
    2.  No entries are created in `job_control`, `job_log`, or `mock_k_ausd_bp_ta_bpr_optionen_calls` tables, as the error occurs before the procedure's transaction begins.

    ```python
    # Pytest example
    def test_type_handling_wiederanlaufwert_invalid(bq_client):
        test_stichtag = "01012023"
        invalid_wiederanlaufwert = "abc"

        # Action: Call the wrapper procedure with invalid type, expecting an error
        query = f"""
        CALL `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`(
            p_stichtag_in => '{test_stichtag}',
            p_wiederanlaufWert_in => '{invalid_wiederanlaufwert}'
        );
        """
        with pytest.raises(Exception) as excinfo:
            bq_client.query(query).result()

        assert "Bad int64 value" in str(excinfo.value) or "Cannot parse" in str(excinfo.value)

        # Assertions: No entries should be created in job_control or job_log
        job_control_rows = list(bq_client.query("SELECT COUNT(*) as count FROM `project.dataset.job_control`").result())[0].count
        assert job_control_rows == 0

        job_log_rows = list(bq_client.query("SELECT COUNT(*) as count FROM `project.dataset.job_log`").result())[0].count
        assert job_log_rows == 0

        mock_calls_rows = list(bq_client.query("SELECT COUNT(*) as count FROM `project.dataset.mock_k_ausd_bp_ta_bpr_optionen_calls`").result())[0].count
        assert mock_calls_rows == 0
    ```