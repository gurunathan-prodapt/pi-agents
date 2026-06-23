As a senior data-migration QA engineer, I've designed a suite of validation tests for the migrated BigQuery stored procedure `project.dataset.sp_vertragsdatenabgleich`. These tests aim to ensure behavioral equivalence with the legacy KornShell script `r_ausd_v_ta_inv_acc.ksh`, covering output parity, transformation correctness, external system replacements, and data quality.

The tests are structured with a clear purpose, setup, action, and concrete pass/fail criteria. Python with the `google-cloud-bigquery` client is used for runnable test code examples, along with SQL assertions where applicable.

---

## Test Case 1: Successful Execution - Happy Path

*   **Purpose:** Verify that the migrated stored procedure executes successfully with valid inputs, logs job start/end, and updates job status correctly. This covers output parity and basic transformation correctness.
*   **Setup:**
    1.  Ensure all BigQuery DDLs (`dw_job_entries`, `dw_error_log`, `dw_job_status`) and stored procedures (`sp_dwmsg_erzeuge_eintrag`, `sp_dwmsg_setze_status_ok`, `sp_dwmsg_meldefehler`, `sp_dwmsg_fehlerbehandlung`, `sp_k_ausd_v_ta_inv_acc`, `sp_vertragsdatenabgleich`) are deployed in the `project.dataset`.
    2.  Clear the `dw_job_entries`, `dw_error_log`, and `dw_job_status` tables to ensure a clean state for the test.

    ```python
    from google.cloud import bigquery

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"

    def setup_clean_tables():
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dw_job_entries`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dw_error_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dw_job_status`").result()

    setup_clean_tables()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` with valid parameters.
    2.  Query the logging tables to verify the outcome.

    ```python
    # Action: Call the main stored procedure
    reporting_date = "26102023"
    mode = "PROD"
    job_kennung = "R_AUSD_V_TA_INV_ACC" # Default value, but explicitly setting for clarity

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
        p_reporting_date_str => '{reporting_date}',
        p_mode => '{mode}',
        p_job_kennung_param => '{job_kennung}'
    );
    """
    client.query(call_query).result()

    # Action: Query logging tables
    query_job_entries = f"SELECT job_id, run_id, status, message, start_timestamp, end_timestamp FROM `{project_id}.{dataset_id}.dw_job_entries` ORDER BY start_timestamp DESC LIMIT 1"
    query_job_status = f"SELECT job_id, run_id, status, last_message, last_update_timestamp FROM `{project_id}.{dataset_id}.dw_job_status` ORDER BY last_update_timestamp DESC LIMIT 1"
    query_error_log = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.dw_error_log`"

    job_entry_result = list(client.query(query_job_entries).result())
    job_status_result = list(client.query(query_job_status).result())
    error_log_count = list(client.query(query_error_log).result())[0][0]
    ```
*   **Pass/Fail Criteria:**
    *   The `sp_vertragsdatenabgleich` call completes without raising an error.
    *   One entry exists in `project.dataset.dw_job_entries` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'SUCCESS'
        *   `start_timestamp` and `end_timestamp` are populated (i.e., not NULL).
        *   `message` contains 'Job successfully completed'.
    *   One entry exists in `project.dataset.dw_job_status` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'SUCCESS'
        *   `last_update_timestamp` is populated.
        *   `last_message` contains 'Job successfully completed'.
    *   Zero entries in `project.dataset.dw_error_log`.

    ```python
    assert len(job_entry_result) == 1
    assert job_entry_result[0]['job_id'] == job_kennung.upper()
    assert job_entry_result[0]['status'] == 'SUCCESS'
    assert job_entry_result[0]['start_timestamp'] is not None
    assert job_entry_result[0]['end_timestamp'] is not None
    assert 'Job successfully completed' in job_entry_result[0]['message']

    assert len(job_status_result) == 1
    assert job_status_result[0]['job_id'] == job_kennung.upper()
    assert job_status_result[0]['status'] == 'SUCCESS'
    assert job_status_result[0]['last_update_timestamp'] is not None
    assert 'Job successfully completed' in job_status_result[0]['last_message']

    assert error_log_count == 0
    print("Test Case 1 Passed: Successful execution verified.")
    ```

---

## Test Case 2: Parameter Validation - Missing Reporting Date

*   **Purpose:** Verify that the stored procedure correctly handles a missing `p_reporting_date_str` parameter, logs the error, and fails gracefully. This covers transformation correctness (parameter handling, conditional logic, error handling).
*   **Setup:**
    1.  Ensure all BigQuery DDLs and stored procedures are deployed.
    2.  Clear the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`).

    ```python
    setup_clean_tables()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` with `p_reporting_date_str` as `NULL`.
    2.  Query the logging tables to verify the outcome.

    ```python
    # Action: Call the main stored procedure with missing reporting date
    reporting_date = "NULL" # Simulating missing parameter
    mode = "PROD"
    job_kennung = "R_AUSD_V_TA_INV_ACC"

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
        p_reporting_date_str => {reporting_date},
        p_mode => '{mode}',
        p_job_kennung_param => '{job_kennung}'
    );
    """
    try:
        client.query(call_query).result()
        assert False, "Expected an error to be raised, but the procedure completed successfully."
    except Exception as e:
        print(f"Caught expected error: {e}")

    # Action: Query logging tables
    query_job_entries = f"SELECT job_id, run_id, status, message FROM `{project_id}.{dataset_id}.dw_job_entries` ORDER BY start_timestamp DESC LIMIT 1"
    query_job_status = f"SELECT job_id, run_id, status, last_message FROM `{project_id}.{dataset_id}.dw_job_status` ORDER BY last_update_timestamp DESC LIMIT 1"
    query_error_log = f"SELECT job_id, run_id, error_code, error_message, source_component FROM `{project_id}.{dataset_id}.dw_error_log` ORDER BY error_timestamp DESC LIMIT 1"

    job_entry_result = list(client.query(query_job_entries).result())
    job_status_result = list(client.query(query_job_status).result())
    error_log_result = list(client.query(query_error_log).result())
    ```
*   **Pass/Fail Criteria:**
    *   The `sp_vertragsdatenabgleich` call raises an error.
    *   One entry exists in `project.dataset.dw_job_entries` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `message` contains 'ERROR: Reporting date parameter is missing.'
    *   One entry exists in `project.dataset.dw_job_status` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `last_message` contains 'ERROR: Reporting date parameter is missing.'
    *   One entry exists in `project.dataset.dw_error_log` with:
        *   `error_code` = 'PARAM_ERROR'
        *   `error_message` contains 'ERROR: Reporting date parameter is missing.'
        *   `source_component` = 'sp_vertragsdatenabgleich'

    ```python
    assert len(job_entry_result) == 1
    assert job_entry_result[0]['job_id'] == job_kennung.upper()
    assert job_entry_result[0]['status'] == 'FAILED'
    assert 'ERROR: Reporting date parameter is missing.' in job_entry_result[0]['message']

    assert len(job_status_result) == 1
    assert job_status_result[0]['job_id'] == job_kennung.upper()
    assert job_status_result[0]['status'] == 'FAILED'
    assert 'ERROR: Reporting date parameter is missing.' in job_status_result[0]['last_message']

    assert len(error_log_result) == 1
    assert error_log_result[0]['error_code'] == 'PARAM_ERROR'
    assert 'ERROR: Reporting date parameter is missing.' in error_log_result[0]['error_message']
    assert error_log_result[0]['source_component'] == 'sp_vertragsdatenabgleich'
    print("Test Case 2 Passed: Missing reporting date handled correctly.")
    ```

---

## Test Case 3: Parameter Validation - Invalid Reporting Date Format

*   **Purpose:** Verify that the stored procedure correctly handles an invalid format for `p_reporting_date_str`, logs the error, and fails gracefully. This covers transformation correctness (type handling, conditional logic, error handling).
*   **Setup:**
    1.  Ensure all BigQuery DDLs and stored procedures are deployed.
    2.  Clear the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`).

    ```python
    setup_clean_tables()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` with `p_reporting_date_str='2023-10-26'` (an invalid format).
    2.  Query the logging tables to verify the outcome.

    ```python
    # Action: Call the main stored procedure with invalid reporting date format
    reporting_date = "2023-10-26" # Invalid format
    mode = "PROD"
    job_kennung = "R_AUSD_V_TA_INV_ACC"

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
        p_reporting_date_str => '{reporting_date}',
        p_mode => '{mode}',
        p_job_kennung_param => '{job_kennung}'
    );
    """
    try:
        client.query(call_query).result()
        assert False, "Expected an error to be raised, but the procedure completed successfully."
    except Exception as e:
        print(f"Caught expected error: {e}")

    # Action: Query logging tables
    query_job_entries = f"SELECT job_id, run_id, status, message FROM `{project_id}.{dataset_id}.dw_job_entries` ORDER BY start_timestamp DESC LIMIT 1"
    query_job_status = f"SELECT job_id, run_id, status, last_message FROM `{project_id}.{dataset_id}.dw_job_status` ORDER BY last_update_timestamp DESC LIMIT 1"
    query_error_log = f"SELECT job_id, run_id, error_code, error_message, source_component FROM `{project_id}.{dataset_id}.dw_error_log` ORDER BY error_timestamp DESC LIMIT 1"

    job_entry_result = list(client.query(query_job_entries).result())
    job_status_result = list(client.query(query_job_status).result())
    error_log_result = list(client.query(query_error_log).result())
    ```
*   **Pass/Fail Criteria:**
    *   The `sp_vertragsdatenabgleich` call raises an error.
    *   One entry exists in `project.dataset.dw_job_entries` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `message` contains 'ERROR: Invalid reporting date format.'
    *   One entry exists in `project.dataset.dw_job_status` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `last_message` contains 'ERROR: Invalid reporting date format.'
    *   One entry exists in `project.dataset.dw_error_log` with:
        *   `error_code` = 'PARAM_ERROR'
        *   `error_message` contains 'ERROR: Invalid reporting date format.'
        *   `source_component` = 'sp_vertragsdatenabgleich'

    ```python
    assert len(job_entry_result) == 1
    assert job_entry_result[0]['job_id'] == job_kennung.upper()
    assert job_entry_result[0]['status'] == 'FAILED'
    assert 'ERROR: Invalid reporting date format.' in job_entry_result[0]['message']

    assert len(job_status_result) == 1
    assert job_status_result[0]['job_id'] == job_kennung.upper()
    assert job_status_result[0]['status'] == 'FAILED'
    assert 'ERROR: Invalid reporting date format.' in job_status_result[0]['last_message']

    assert len(error_log_result) == 1
    assert error_log_result[0]['error_code'] == 'PARAM_ERROR'
    assert 'ERROR: Invalid reporting date format.' in error_log_result[0]['error_message']
    assert error_log_result[0]['source_component'] == 'sp_vertragsdatenabgleich'
    print("Test Case 3 Passed: Invalid reporting date format handled correctly.")
    ```

---

## Test Case 4: Parameter Validation - Invalid Mode

*   **Purpose:** Verify that the stored procedure correctly handles an invalid `p_mode` parameter, logs the error, and fails gracefully. This covers transformation correctness (conditional logic, error handling).
*   **Setup:**
    1.  Ensure all BigQuery DDLs and stored procedures are deployed.
    2.  Clear the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`).

    ```python
    setup_clean_tables()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` with `p_mode='DEV'` (an invalid value).
    2.  Query the logging tables to verify the outcome.

    ```python
    # Action: Call the main stored procedure with invalid mode
    reporting_date = "26102023"
    mode = "DEV" # Invalid mode
    job_kennung = "R_AUSD_V_TA_INV_ACC"

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
        p_reporting_date_str => '{reporting_date}',
        p_mode => '{mode}',
        p_job_kennung_param => '{job_kennung}'
    );
    """
    try:
        client.query(call_query).result()
        assert False, "Expected an error to be raised, but the procedure completed successfully."
    except Exception as e:
        print(f"Caught expected error: {e}")

    # Action: Query logging tables
    query_job_entries = f"SELECT job_id, run_id, status, message FROM `{project_id}.{dataset_id}.dw_job_entries` ORDER BY start_timestamp DESC LIMIT 1"
    query_job_status = f"SELECT job_id, run_id, status, last_message FROM `{project_id}.{dataset_id}.dw_job_status` ORDER BY last_update_timestamp DESC LIMIT 1"
    query_error_log = f"SELECT job_id, run_id, error_code, error_message, source_component FROM `{project_id}.{dataset_id}.dw_error_log` ORDER BY error_timestamp DESC LIMIT 1"

    job_entry_result = list(client.query(query_job_entries).result())
    job_status_result = list(client.query(query_job_status).result())
    error_log_result = list(client.query(query_error_log).result())
    ```
*   **Pass/Fail Criteria:**
    *   The `sp_vertragsdatenabgleich` call raises an error.
    *   One entry exists in `project.dataset.dw_job_entries` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `message` contains 'ERROR: Invalid or missing mode parameter.'
    *   One entry exists in `project.dataset.dw_job_status` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `last_message` contains 'ERROR: Invalid or missing mode parameter.'
    *   One entry exists in `project.dataset.dw_error_log` with:
        *   `error_code` = 'PARAM_ERROR'
        *   `error_message` contains 'ERROR: Invalid or missing mode parameter.'
        *   `source_component` = 'sp_vertragsdatenabgleich'

    ```python
    assert len(job_entry_result) == 1
    assert job_entry_result[0]['job_id'] == job_kennung.upper()
    assert job_entry_result[0]['status'] == 'FAILED'
    assert 'ERROR: Invalid or missing mode parameter.' in job_entry_result[0]['message']

    assert len(job_status_result) == 1
    assert job_status_result[0]['job_id'] == job_kennung.upper()
    assert job_status_result[0]['status'] == 'FAILED'
    assert 'ERROR: Invalid or missing mode parameter.' in job_status_result[0]['last_message']

    assert len(error_log_result) == 1
    assert error_log_result[0]['error_code'] == 'PARAM_ERROR'
    assert 'ERROR: Invalid or missing mode parameter.' in error_log_result[0]['error_message']
    assert error_log_result[0]['source_component'] == 'sp_vertragsdatenabgleich'
    print("Test Case 4 Passed: Invalid mode handled correctly.")
    ```

---

## Test Case 5: Core Logic Failure Handling

*   **Purpose:** Verify that if the invoked core stored procedure (`sp_k_ausd_v_ta_inv_acc`) fails, the main orchestration procedure correctly catches the error, logs it, and updates job status to FAILED. This covers external-system replacements (invocation of sub-SP) and error handling.
*   **Setup:**
    1.  Ensure all BigQuery DDLs and stored procedures are deployed.
    2.  Clear the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`).
    3.  **Temporarily modify `sp_k_ausd_v_ta_inv_acc` to raise an error:**

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        RAISE USING MESSAGE 'Simulated error in core reconciliation logic.';
    END;
    ```
    ```python
    setup_clean_tables()
    client.query(f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        RAISE USING MESSAGE 'Simulated error in core reconciliation logic.';
    END;
    """).result()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` with valid parameters.
    2.  Query the logging tables to verify the outcome.

    ```python
    # Action: Call the main stored procedure
    reporting_date = "26102023"
    mode = "PROD"
    job_kennung = "R_AUSD_V_TA_INV_ACC"

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
        p_reporting_date_str => '{reporting_date}',
        p_mode => '{mode}',
        p_job_kennung_param => '{job_kennung}'
    );
    """
    try:
        client.query(call_query).result()
        assert False, "Expected an error to be raised, but the procedure completed successfully."
    except Exception as e:
        print(f"Caught expected error: {e}")

    # Action: Query logging tables
    query_job_entries = f"SELECT job_id, run_id, status, message FROM `{project_id}.{dataset_id}.dw_job_entries` ORDER BY start_timestamp DESC LIMIT 1"
    query_job_status = f"SELECT job_id, run_id, status, last_message FROM `{project_id}.{dataset_id}.dw_job_status` ORDER BY last_update_timestamp DESC LIMIT 1"
    query_error_log = f"SELECT job_id, run_id, error_code, error_message, source_component FROM `{project_id}.{dataset_id}.dw_error_log` ORDER BY error_timestamp DESC LIMIT 1"

    job_entry_result = list(client.query(query_job_entries).result())
    job_status_result = list(client.query(query_job_status).result())
    error_log_result = list(client.query(query_error_log).result())
    ```
*   **Pass/Fail Criteria:**
    *   The `sp_vertragsdatenabgleich` call raises an error.
    *   One entry exists in `project.dataset.dw_job_entries` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `message` contains 'Simulated error in core reconciliation logic.'
    *   One entry exists in `project.dataset.dw_job_status` with:
        *   `job_id` = 'R_AUSD_V_TA_INV_ACC'
        *   `status` = 'FAILED'
        *   `last_message` contains 'Simulated error in core reconciliation logic.'
    *   One entry exists in `project.dataset.dw_error_log` with:
        *   `error_code` = 'EXECUTION_ERROR'
        *   `error_message` contains 'Simulated error in core reconciliation logic.'
        *   `source_component` = 'sp_k_ausd_v_ta_inv_acc_call'

    ```python
    assert len(job_entry_result) == 1
    assert job_entry_result[0]['job_id'] == job_kennung.upper()
    assert job_entry_result[0]['status'] == 'FAILED'
    assert 'Simulated error in core reconciliation logic.' in job_entry_result[0]['message']

    assert len(job_status_result) == 1
    assert job_status_result[0]['job_id'] == job_kennung.upper()
    assert job_status_result[0]['status'] == 'FAILED'
    assert 'Simulated error in core reconciliation logic.' in job_status_result[0]['last_message']

    assert len(error_log_result) == 1
    assert error_log_result[0]['error_code'] == 'EXECUTION_ERROR'
    assert 'Simulated error in core reconciliation logic.' in error_log_result[0]['error_message']
    assert error_log_result[0]['source_component'] == 'sp_k_ausd_v_ta_inv_acc_call'
    print("Test Case 5 Passed: Core logic failure handled correctly.")
    ```
*   **Cleanup:** Revert `sp_k_ausd_v_ta_inv_acc` to its original placeholder definition.

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        SELECT 'Core reconciliation logic will be implemented here for job ' || p_job_id || ' run ' || p_run_id || ' for date ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date) || ' in mode ' || p_mode;
    END;
    ```
    ```python
    client.query(f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        SELECT 'Core reconciliation logic will be implemented here for job ' || p_job_id || ' run ' || p_run_id || ' for date ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date) || ' in mode ' || p_mode;
    END;
    """).result()
    ```

---

## Test Case 6: JobKennung Uppercasing

*   **Purpose:** Verify that the `p_job_kennung_param` is correctly uppercased, mirroring the `typeset -u JobKennung` behavior of the original script. This covers transformation correctness (string manipulation).
*   **Setup:**
    1.  Ensure all BigQuery DDLs and stored procedures are deployed.
    2.  Clear the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`).

    ```python
    setup_clean_tables()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` with a lowercase `p_job_kennung_param`.
    2.  Query the logging tables to verify the `job_id`.

    ```python
    # Action: Call the main stored procedure with a lowercase job_kennung
    reporting_date = "26102023"
    mode = "PROD"
    job_kennung_input = "bert_v_ta_inv_acc_test"
    expected_job_id = job_kennung_input.upper()

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
        p_reporting_date_str => '{reporting_date}',
        p_mode => '{mode}',
        p_job_kennung_param => '{job_kennung_input}'
    );
    """
    client.query(call_query).result()

    # Action: Query logging tables
    query_job_entries = f"SELECT job_id FROM `{project_id}.{dataset_id}.dw_job_entries` ORDER BY start_timestamp DESC LIMIT 1"
    query_job_status = f"SELECT job_id FROM `{project_id}.{dataset_id}.dw_job_status` ORDER BY last_update_timestamp DESC LIMIT 1"

    job_entry_result = list(client.query(query_job_entries).result())
    job_status_result = list(client.query(query_job_status).result())
    ```
*   **Pass/Fail Criteria:**
    *   No error is raised.
    *   The `job_id` in `project.dataset.dw_job_entries` is 'BERT_V_TA_INV_ACC_TEST'.
    *   The `job_id` in `project.dataset.dw_job_status` is 'BERT_V_TA_INV_ACC_TEST'.

    ```python
    assert len(job_entry_result) == 1
    assert job_entry_result[0]['job_id'] == expected_job_id

    assert len(job_status_result) == 1
    assert job_status_result[0]['job_id'] == expected_job_id
    print("Test Case 6 Passed: JobKennung uppercasing verified.")
    ```

---

## Test Case 7: Logging Table Schema and Constraints

*   **Purpose:** Verify that the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`) have the correct schema, data types, and `NOT NULL` constraints as defined in the DDLs. This covers data quality and schema assertions.
*   **Setup:**
    1.  Ensure the DDLs for `dw_job_entries`, `dw_error_log`, `dw_job_status` have been executed.
*   **Action:**
    1.  Query BigQuery's `INFORMATION_SCHEMA` for table and column details.

    ```python
    # Action: Query INFORMATION_SCHEMA
    query_schema = f"""
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable
    FROM
        `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name IN ('dw_job_entries', 'dw_error_log', 'dw_job_status')
    ORDER BY
        table_name, ordinal_position;
    """
    schema_results = list(client.query(query_schema).result())
    ```
*   **Pass/Fail Criteria:**
    *   `project.dataset.dw_job_entries` exists and has columns: `job_id STRING NOT NULL`, `run_id STRING NOT NULL`, `job_name STRING`, `start_timestamp TIMESTAMP`, `end_timestamp TIMESTAMP`, `status STRING`, `message STRING`.
    *   `project.dataset.dw_error_log` exists and has columns: `job_id STRING NOT NULL`, `run_id STRING NOT NULL`, `error_timestamp TIMESTAMP NOT NULL`, `error_code STRING`, `error_message STRING`, `source_component STRING`, `stack_trace STRING`.
    *   `project.dataset.dw_job_status` exists and has columns: `job_id STRING NOT NULL`, `run_id STRING NOT NULL`, `status STRING`, `last_update_timestamp TIMESTAMP NOT NULL`, `last_message STRING`.
    *   The `PRIMARY KEY (job_id, run_id) NOT ENFORCED` for `dw_job_status` is correctly represented by the combination of `job_id` and `run_id` being `NOT NULL`.

    ```python
    expected_schema = {
        'dw_job_entries': {
            'job_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'run_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'job_name': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'start_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
            'end_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
            'status': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'message': {'data_type': 'STRING', 'is_nullable': 'YES'}
        },
        'dw_error_log': {
            'job_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'run_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'error_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
            'error_code': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'error_message': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'source_component': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'stack_trace': {'data_type': 'STRING', 'is_nullable': 'YES'}
        },
        'dw_job_status': {
            'job_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'run_id': {'data_type': 'STRING', 'is_nullable': 'NO'},
            'status': {'data_type': 'STRING', 'is_nullable': 'YES'},
            'last_update_timestamp': {'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
            'last_message': {'data_type': 'STRING', 'is_nullable': 'YES'}
        }
    }

    actual_schema = {}
    for row in schema_results:
        table = row['table_name']
        column = row['column_name']
        if table not in actual_schema:
            actual_schema[table] = {}
        actual_schema[table][column] = {
            'data_type': row['data_type'],
            'is_nullable': row['is_nullable']
        }

    for table, columns in expected_schema.items():
        assert table in actual_schema, f"Table {table} not found."
        for column, props in columns.items():
            assert column in actual_schema[table], f"Column {column} not found in table {table}."
            assert actual_schema[table][column]['data_type'] == props['data_type'], \
                f"Data type mismatch for {table}.{column}: Expected {props['data_type']}, Got {actual_schema[table][column]['data_type']}"
            assert actual_schema[table][column]['is_nullable'] == props['is_nullable'], \
                f"Nullability mismatch for {table}.{column}: Expected {props['is_nullable']}, Got {actual_schema[table][column]['is_nullable']}"

    print("Test Case 7 Passed: Logging table schema and constraints verified.")
    ```

---

## Test Case 8: DWMSG_ErmittleNr / GENERATE_UUID Equivalence

*   **Purpose:** Verify that the `run_id` generated by `GENERATE_UUID()` in BigQuery serves the same purpose of providing a unique identifier for a job run as `DW_EintragsNr` in the legacy system. This ensures behavioral equivalence for job tracking.
*   **Setup:**
    1.  Ensure all BigQuery DDLs and stored procedures are deployed.
    2.  Clear the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`).

    ```python
    setup_clean_tables()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` multiple times with the same parameters.
    2.  Query `dw_job_entries` for the `run_id`s.

    ```python
    # Action: Call the main stored procedure multiple times
    num_runs = 3
    reporting_date = "26102023"
    mode = "PROD"
    job_kennung = "R_AUSD_V_TA_INV_ACC"

    for _ in range(num_runs):
        call_query = f"""
        CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
            p_reporting_date_str => '{reporting_date}',
            p_mode => '{mode}',
            p_job_kennung_param => '{job_kennung}'
        );
        """
        client.query(call_query).result()

    # Action: Query dw_job_entries for run_ids
    query_run_ids = f"SELECT run_id FROM `{project_id}.{dataset_id}.dw_job_entries` WHERE job_id = '{job_kennung.upper()}'"
    run_id_results = list(client.query(query_run_ids).result())
    ```
*   **Pass/Fail Criteria:**
    *   The number of `run_id`s retrieved matches the number of times the procedure was called.
    *   All retrieved `run_id`s are unique.
    *   Each `run_id` is consistently used across `dw_job_entries` and `dw_job_status` for its respective job execution (this is implicitly covered by the `MERGE` and `UPDATE` statements in the logging procedures, which use `job_id` and `run_id` as keys).

    ```python
    actual_run_ids = [row['run_id'] for row in run_id_results]
    assert len(actual_run_ids) == num_runs
    assert len(set(actual_run_ids)) == num_runs # All run_ids must be unique
    print("Test Case 8 Passed: Unique run_id generation verified.")
    ```

---

## Test Case 9: Date Formatting Equivalence

*   **Purpose:** Verify that the date parsing `PARSE_DATE('%d%m%Y', p_reporting_date_str)` correctly interprets the input date string, mirroring the implicit date handling in the original script (which uses `date +%d%m%Y` to *generate* a date string, and `DWMSG_SetzeStichtagInfo` to process it). The key is that `DDMMYYYY` is correctly handled and passed to the core logic.
*   **Setup:**
    1.  Ensure all BigQuery DDLs and stored procedures are deployed.
    2.  Clear the logging tables (`dw_job_entries`, `dw_error_log`, `dw_job_status`).
    3.  **Temporarily modify `sp_k_ausd_v_ta_inv_acc` to log the `p_reporting_date` it receives:**

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        -- Log the received date for verification
        INSERT INTO `project.dataset.dw_job_entries` (job_id, run_id, job_name, start_timestamp, status, message)
        VALUES (p_job_id, p_run_id, 'Core Logic Debug', CURRENT_TIMESTAMP(), 'INFO', 'Received reporting date: ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date));
        -- Original placeholder logic
        SELECT 'Core reconciliation logic will be implemented here for job ' || p_job_id || ' run ' || p_run_id || ' for date ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date) || ' in mode ' || p_mode;
    END;
    ```
    ```python
    setup_clean_tables()
    client.query(f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        INSERT INTO `{project_id}.{dataset_id}.dw_job_entries` (job_id, run_id, job_name, start_timestamp, status, message)
        VALUES (p_job_id, p_run_id, 'Core Logic Debug', CURRENT_TIMESTAMP(), 'INFO', 'Received reporting date: ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date));
        SELECT 'Core reconciliation logic will be implemented here for job ' || p_job_id || ' run ' || p_run_id || ' for date ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date) || ' in mode ' || p_mode;
    END;
    """).result()
    ```
*   **Action:**
    1.  Call `project.dataset.sp_vertragsdatenabgleich` with a specific `p_reporting_date_str` in `DDMMYYYY` format.
    2.  Query `dw_job_entries` for the debug message from `sp_k_ausd_v_ta_inv_acc`.

    ```python
    # Action: Call the main stored procedure
    reporting_date_str = "01012024"
    expected_date_format = "2024-01-01"
    mode = "PROD"
    job_kennung = "R_AUSD_V_TA_INV_ACC"

    call_query = f"""
    CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(
        p_reporting_date_str => '{reporting_date_str}',
        p_mode => '{mode}',
        p_job_kennung_param => '{job_kennung}'
    );
    """
    client.query(call_query).result()

    # Action: Query dw_job_entries for the debug message
    query_debug_log = f"""
    SELECT message
    FROM `{project_id}.{dataset_id}.dw_job_entries`
    WHERE job_name = 'Core Logic Debug'
    ORDER BY start_timestamp DESC LIMIT 1;
    """
    debug_log_result = list(client.query(query_debug_log).result())
    ```
*   **Pass/Fail Criteria:**
    *   No error is raised.
    *   A `dw_job_entries` record from `sp_k_ausd_v_ta_inv_acc` exists with `message` containing 'Received reporting date: 2024-01-01'.

    ```python
    assert len(debug_log_result) == 1
    assert f'Received reporting date: {expected_date_format}' in debug_log_result[0]['message']
    print("Test Case 9 Passed: Date formatting equivalence verified.")
    ```
*   **Cleanup:** Revert `sp_k_ausd_v_ta_inv_acc` to its original placeholder definition.

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        SELECT 'Core reconciliation logic will be implemented here for job ' || p_job_id || ' run ' || p_run_id || ' for date ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date) || ' in mode ' || p_mode;
    END;
    ```
    ```python
    client.query(f"""
    CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_k_ausd_v_ta_inv_acc`(
        IN p_job_id STRING,
        IN p_run_id STRING,
        IN p_reporting_date DATE,
        IN p_mode STRING
    )
    BEGIN
        SELECT 'Core reconciliation logic will be implemented here for job ' || p_job_id || ' run ' || p_run_id || ' for date ' || FORMAT_DATE('%Y-%m-%d', p_reporting_date) || ' in mode ' || p_mode;
    END;
    """).result()
    ```