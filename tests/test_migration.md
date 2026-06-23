As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_v_ta_cntrct_templ.ksh` to `project.dataset.sp_vertragsdatenabgleich`. The core of this migration involves translating shell-based orchestration, parameter handling, logging, and error trapping into BigQuery Stored Procedure logic and dedicated logging tables.

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements (logging framework, core script invocation), and data quality/schema assertions for the new BigQuery components.

---

## Pre-requisites for all Tests

Before running any tests, ensure the following:
1.  The BigQuery project and dataset (`project.dataset`) exist.
2.  The DDLs for `job_registry`, `job_log`, and `job_error_log` have been executed to create the tables.
3.  The BigQuery Stored Procedure `project.dataset.sp_vertragsdatenabgleich` has been deployed.
4.  A dummy `project.dataset.sp_k_ausd_v_ta_cntrct_templ` procedure exists. This procedure will be modified or replaced by specific test cases to simulate success or failure of the core logic.

**Example Dummy `sp_k_ausd_v_ta_cntrct_templ` (for general success simulation):**

```sql
CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_cntrct_templ(
    p_job_nr INT64,
    p_job_kennung STRING,
    p_param_s STRING,
    p_param_l STRING
)
BEGIN
    -- Simulate successful execution of the core logic
    INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
    VALUES (p_job_nr, p_job_kennung, 'INFO', FORMAT('Core script (sp_k_ausd_v_ta_cntrct_templ) executed successfully with s="%s", l="%s"', IFNULL(p_param_s, 'NULL'), IFNULL(p_param_l, 'NULL')), CURRENT_TIMESTAMP());
END;
```

---

## Test Case 1: Schema Validation of Logging Tables

*   **Purpose**: To verify that the BigQuery logging tables (`job_registry`, `job_log`, `job_error_log`) are created with the correct schema, column names, data types, and descriptions as specified in the migration design. This ensures data quality and adherence to the defined audit framework.
*   **Setup**:
    *   Ensure the DDLs for `project.dataset.job_registry`, `project.dataset.job_log`, and `project.dataset.job_error_log` have been executed.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` view for each table to retrieve its metadata.
*   **Pass/Fail Criterion**:
    *   All three tables exist in `project.dataset`.
    *   Each table contains the exact set of columns with the specified data types and descriptions as defined in the DDLs provided in the migration design document.

*   **Runnable Test Code (Pytest / SQL Assertions)**:

    ```python
    import pytest
    from google.cloud import bigquery

    PROJECT_ID = "project"
    DATASET_ID = "dataset"

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client(project=PROJECT_ID)

    def test_job_registry_schema(bq_client):
        table_id = f"{PROJECT_ID}.{DATASET_ID}.job_registry"
        expected_schema = {
            "job_nr": {"data_type": "INT64", "description": "Unique job run number"},
            "job_kennung": {"data_type": "STRING", "description": "Identifier for the job type (e.g., BERT_V_TA_CNTRCT_TEMPL)"},
            "script_name": {"data_type": "STRING", "description": "Name of the script/procedure executed"},
            "status": {"data_type": "STRING", "description": "Current status of the job (e.g., RUNNING, SUCCESS, ERROR)"},
            "start_timestamp": {"data_type": "TIMESTAMP", "description": "Timestamp when the job started"},
            "end_timestamp": {"data_type": "TIMESTAMP", "description": "Timestamp when the job ended"},
            "last_update_timestamp": {"data_type": "TIMESTAMP", "description": "Last update timestamp for job status"},
        }
        table = bq_client.get_table(table_id)
        assert table.schema is not None, f"Table {table_id} does not exist or has no schema."
        
        actual_schema = {field.name: {"data_type": field.field_type, "description": field.description} for field in table.schema}
        
        assert len(actual_schema) == len(expected_schema), f"Schema mismatch for {table_id}: column count differs."
        for col_name, expected_props in expected_schema.items():
            assert col_name in actual_schema, f"Column '{col_name}' missing in {table_id}."
            assert actual_schema[col_name]["data_type"] == expected_props["data_type"], \
                f"Data type mismatch for {table_id}.{col_name}: Expected {expected_props['data_type']}, Got {actual_schema[col_name]['data_type']}."
            assert actual_schema[col_name]["description"] == expected_props["description"], \
                f"Description mismatch for {table_id}.{col_name}: Expected '{expected_props['description']}', Got '{actual_schema[col_name]['description']}'."

    def test_job_log_schema(bq_client):
        table_id = f"{PROJECT_ID}.{DATASET_ID}.job_log"
        expected_schema = {
            "job_nr": {"data_type": "INT64", "description": "Foreign key to job_registry.job_nr"},
            "job_kennung": {"data_type": "STRING", "description": "Identifier for the job type"},
            "log_level": {"data_type": "STRING", "description": "Level of the log message (e.g., INFO, WARNING, ERROR)"},
            "message": {"data_type": "STRING", "description": "Detailed log message"},
            "log_timestamp": {"data_type": "TIMESTAMP", "description": "Timestamp when the log entry was recorded"},
        }
        table = bq_client.get_table(table_id)
        assert table.schema is not None, f"Table {table_id} does not exist or has no schema."
        
        actual_schema = {field.name: {"data_type": field.field_type, "description": field.description} for field in table.schema}
        
        assert len(actual_schema) == len(expected_schema), f"Schema mismatch for {table_id}: column count differs."
        for col_name, expected_props in expected_schema.items():
            assert col_name in actual_schema, f"Column '{col_name}' missing in {table_id}."
            assert actual_schema[col_name]["data_type"] == expected_props["data_type"], \
                f"Data type mismatch for {table_id}.{col_name}: Expected {expected_props['data_type']}, Got {actual_schema[col_name]['data_type']}."
            assert actual_schema[col_name]["description"] == expected_props["description"], \
                f"Description mismatch for {table_id}.{col_name}: Expected '{expected_props['description']}', Got '{actual_schema[col_name]['description']}'."

    def test_job_error_log_schema(bq_client):
        table_id = f"{PROJECT_ID}.{DATASET_ID}.job_error_log"
        expected_schema = {
            "job_nr": {"data_type": "INT64", "description": "Foreign key to job_registry.job_nr"},
            "job_kennung": {"data_type": "STRING", "description": "Identifier for the job type"},
            "err_nr": {"data_type": "INT64", "description": "Error number or code"},
            "err_arg": {"data_type": "STRING", "description": "Additional error argument or context"},
            "message": {"data_type": "STRING", "description": "Detailed error message"},
            "error_timestamp": {"data_type": "TIMESTAMP", "description": "Timestamp when the error occurred"},
        }
        table = bq_client.get_table(table_id)
        assert table.schema is not None, f"Table {table_id} does not exist or has no schema."
        
        actual_schema = {field.name: {"data_type": field.field_type, "description": field.description} for field in table.schema}
        
        assert len(actual_schema) == len(expected_schema), f"Schema mismatch for {table_id}: column count differs."
        for col_name, expected_props in expected_schema.items():
            assert col_name in actual_schema, f"Column '{col_name}' missing in {table_id}."
            assert actual_schema[col_name]["data_type"] == expected_props["data_type"], \
                f"Data type mismatch for {table_id}.{col_name}: Expected {expected_props['data_type']}, Got {actual_schema[col_name]['data_type']}."
            assert actual_schema[col_name]["description"] == expected_props["description"], \
                f"Description mismatch for {table_id}.{col_name}: Expected '{expected_props['description']}', Got '{actual_schema[col_name]['description']}'."
    ```

---

## Test Case 2: Successful Job Execution (Happy Path)

*   **Purpose**: To verify that `sp_vertragsdatenabgleich` correctly orchestrates a successful job run, including proper job registration, logging of start and success messages, and updating the final status, when the core script (`sp_k_ausd_v_ta_cntrct_templ`) completes without errors. This covers output parity and transformation correctness for the main flow.
*   **Setup**:
    *   Clear all data from `project.dataset.job_registry`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
    *   Ensure the dummy `sp_k_ausd_v_ta_cntrct_templ` is configured to succeed (as shown in the pre-requisites).
*   **Action**:
    *   Execute the BigQuery Stored Procedure:
        ```sql
        CALL project.dataset.sp_vertragsdatenabgleich(FALSE, 'param_s_value', 'param_l_value');
        ```
*   **Pass/Fail Criterion**:
    *   **`job_registry`**: Contains exactly one record.
        *   `job_nr` is 1.
        *   `job_kennung` is 'BERT_V_TA_CNTRCT_TEMPL'.
        *   `script_name` is 'r_ausd_v_ta_cntrct_templ.ksh'.
        *   `status` is 'SUCCESS'.
        *   `start_timestamp` and `end_timestamp` are populated, with `end_timestamp` being after `start_timestamp`.
    *   **`job_log`**: Contains at least 3 'INFO' records for `job_nr = 1`:
        *   One for job start.
        *   One from the core script indicating successful execution (if the dummy logs it).
        *   One for job successful completion.
    *   **`job_error_log`**: Is empty.
    *   The `sp_k_ausd_v_ta_cntrct_templ` was invoked exactly once with `p_param_s = 'param_s_value'` and `p_param_l = 'param_l_value'`.

*   **Runnable Test Code (Pytest / SQL Assertions)**:

    ```python
    import pytest
    from google.cloud import bigquery
    import time

    PROJECT_ID = "project"
    DATASET_ID = "dataset"

    @pytest.fixture(scope="function")
    def setup_bq_for_test(bq_client):
        # Clear tables
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_registry").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_log").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_error_log").result()
        # Ensure dummy core script is set to succeed
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE {PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_cntrct_templ(
                p_job_nr INT64, p_job_kennung STRING, p_param_s STRING, p_param_l STRING
            )
            BEGIN
                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
                VALUES (p_job_nr, p_job_kennung, 'INFO', FORMAT('Core script executed successfully with s="%s", l="%s"', IFNULL(p_param_s, 'NULL'), IFNULL(p_param_l, 'NULL')), CURRENT_TIMESTAMP());
            END;
        """).result()
        yield
        # Clean up (optional, or handled by next test's setup)

    def test_successful_job_execution(bq_client, setup_bq_for_test):
        # Action: Call the main procedure
        bq_client.query(f"CALL {PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich(FALSE, 'test_s_val', 'test_l_val')").result()
        
        # Assertions for job_registry
        registry_rows = list(bq_client.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_registry").result())
        assert len(registry_rows) == 1
        registry_entry = registry_rows[0]
        assert registry_entry.job_nr == 1
        assert registry_entry.job_kennung == 'BERT_V_TA_CNTRCT_TEMPL'
        assert registry_entry.script_name == 'r_ausd_v_ta_cntrct_templ.ksh'
        assert registry_entry.status == 'SUCCESS'
        assert registry_entry.start_timestamp is not None
        assert registry_entry.end_timestamp is not None
        assert registry_entry.end_timestamp > registry_entry.start_timestamp

        # Assertions for job_log
        log_rows = list(bq_client.query(f"SELECT message, log_level FROM {PROJECT_ID}.{DATASET_ID}.job_log ORDER BY log_timestamp").result())
        assert len(log_rows) >= 3 # Start, core success, main success
        assert "Job 1 (BERT_V_TA_CNTRCT_TEMPL) started for script r_ausd_v_ta_cntrct_templ.ksh." in [r.message for r in log_rows]
        assert "Core script executed successfully with s=\"test_s_val\", l=\"test_l_val\"" in [r.message for r in log_rows]
        assert "Job 1 (BERT_V_TA_CNTRCT_TEMPL) completed successfully." in [r.message for r in log_rows]
        assert all(r.log_level == 'INFO' for r in log_rows)

        # Assertions for job_error_log
        error_rows = list(bq_client.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_error_log").result())
        assert len(error_rows) == 0
    ```

---

## Test Case 3: Help Message Display

*   **Purpose**: To verify that calling `sp_vertragsdatenabgleich` with `p_show_help = TRUE` correctly triggers the help message, logs it, updates the job status to `COMPLETED_WITH_HELP`, and *does not* invoke the core processing script. This tests parameter handling and control flow.
*   **Setup**:
    *   Clear all data from `project.dataset.job_registry`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
    *   Ensure the dummy `sp_k_ausd_v_ta_cntrct_templ` is configured to log its invocation, so we can verify it wasn't called.
        ```sql
        CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_cntrct_templ(
            p_job_nr INT64, p_job_kennung STRING, p_param_s STRING, p_param_l STRING
        )
        BEGIN
            INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
            VALUES (p_job_nr, p_job_kennung, 'WARNING', 'ERROR: Core script was unexpectedly called!', CURRENT_TIMESTAMP());
            RAISE USING MESSAGE = 'Core script should not have been called.';
        END;
        ```
*   **Action**:
    *   Execute the BigQuery Stored Procedure:
        ```sql
        CALL project.dataset.sp_vertragsdatenabgleich(TRUE, NULL, NULL);
        ```
*   **Pass/Fail Criterion**:
    *   **`job_registry`**: Contains exactly one record.
        *   `job_nr` is 1.
        *   `status` is 'COMPLETED_WITH_HELP'.
        *   `end_timestamp` is populated.
    *   **`job_log`**: Contains at least 2 'INFO' records for `job_nr = 1`:
        *   One for job start.
        *   One containing the help message text.
        *   One for job completion after showing help.
        *   Crucially, no 'WARNING' or 'ERROR' messages from the dummy core script.
    *   **`job_error_log`**: Is empty.
    *   The dummy `sp_k_ausd_v_ta_cntrct_templ` was *not* invoked.

*   **Runnable Test Code (Pytest / SQL Assertions)**:

    ```python
    import pytest
    from google.cloud import bigquery

    PROJECT_ID = "project"
    DATASET_ID = "dataset"

    @pytest.fixture(scope="function")
    def setup_bq_for_help_test(bq_client):
        # Clear tables
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_registry").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_log").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_error_log").result()
        # Ensure dummy core script is set to fail if called
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE {PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_cntrct_templ(
                p_job_nr INT64, p_job_kennung STRING, p_param_s STRING, p_param_l STRING
            )
            BEGIN
                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_log (job_nr, p_job_kennung, log_level, message, log_timestamp)
                VALUES (p_job_nr, p_job_kennung, 'WARNING', 'ERROR: Core script was unexpectedly called!', CURRENT_TIMESTAMP());
                RAISE USING MESSAGE = 'Core script should not have been called.';
            END;
        """).result()
        yield

    def test_help_message_display(bq_client, setup_bq_for_help_test):
        # Action: Call the main procedure with p_show_help = TRUE
        bq_client.query(f"CALL {PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich(TRUE, NULL, NULL)").result()
        
        # Assertions for job_registry
        registry_rows = list(bq_client.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_registry").result())
        assert len(registry_rows) == 1
        registry_entry = registry_rows[0]
        assert registry_entry.job_nr == 1
        assert registry_entry.status == 'COMPLETED_WITH_HELP'
        assert registry_entry.end_timestamp is not None

        # Assertions for job_log
        log_rows = list(bq_client.query(f"SELECT message, log_level FROM {PROJECT_ID}.{DATASET_ID}.job_log ORDER BY log_timestamp").result())
        assert len(log_rows) >= 3 # Start, help message, completed_with_help message
        assert "Help message: This procedure orchestrates the contract data reconciliation." in [r.message for r in log_rows]
        assert "Job 1 (BERT_V_TA_CNTRCT_TEMPL) completed after showing help." in [r.message for r in log_rows]
        assert not any("Core script was unexpectedly called!" in r.message for r in log_rows), "Core script should not have been called."
        assert all(r.log_level == 'INFO' for r in log_rows if "Core script was unexpectedly called!" not in r.message)

        # Assertions for job_error_log
        error_rows = list(bq_client.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_error_log").result())
        assert len(error_rows) == 0
    ```

---

## Test Case 4: Core Script Failure Handling

*   **Purpose**: To verify that `sp_vertragsdatenabgleich` correctly handles errors raised by the invoked core script (`sp_k_ausd_v_ta_cntrct_templ`), logging the error details, updating the job status to 'ERROR', and re-raising the error to the caller. This tests the `EXCEPTION WHEN ERROR` block and error logging.
*   **Setup**:
    *   Clear all data from `project.dataset.job_registry`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
    *   Configure the dummy `sp_k_ausd_v_ta_cntrct_templ` to explicitly raise an error.
        ```sql
        CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_cntrct_templ(
            p_job_nr INT64, p_job_kennung STRING, p_param_s STRING, p_param_l STRING
        )
        BEGIN
            RAISE USING MESSAGE = 'Simulated core script failure for testing purposes.';
        END;
        ```
*   **Action**:
    *   Attempt to execute the BigQuery Stored Procedure. This call is expected to fail.
        ```sql
        CALL project.dataset.sp_vertragsdatenabgleich(FALSE, NULL, NULL);
        ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement itself must fail and return an error message (due to the re-raised error).
    *   **`job_registry`**: Contains exactly one record.
        *   `job_nr` is 1.
        *   `status` is 'ERROR'.
        *   `end_timestamp` is populated.
    *   **`job_log`**: Contains at least 2 records for `job_nr = 1`:
        *   One 'INFO' for job start.
        *   One 'ERROR' message indicating the job failure.
    *   **`job_error_log`**: Contains exactly one record for `job_nr = 1`.
        *   `message` contains details about the simulated core script failure.
        *   `error_timestamp` is populated.
        *   `err_arg` or `err_nr` might be populated depending on how BigQuery captures the error details.

*   **Runnable Test Code (Pytest / SQL Assertions)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPICallError

    PROJECT_ID = "project"
    DATASET_ID = "dataset"

    @pytest.fixture(scope="function")
    def setup_bq_for_failure_test(bq_client):
        # Clear tables
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_registry").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_log").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_error_log").result()
        # Ensure dummy core script is set to fail
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE {PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_cntrct_templ(
                p_job_nr INT64, p_job_kennung STRING, p_param_s STRING, p_param_l STRING
            )
            BEGIN
                RAISE USING MESSAGE = 'Simulated core script failure for testing purposes.';
            END;
        """).result()
        yield

    def test_core_script_failure_handling(bq_client, setup_bq_for_failure_test):
        # Action: Call the main procedure, expecting it to fail
        with pytest.raises(GoogleAPICallError) as excinfo:
            bq_client.query(f"CALL {PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich(FALSE, NULL, NULL)").result()
        
        # Verify the error message contains the re-raised error
        assert "Job 1 failed. Details in job_error_log and job_log. Error: Simulated core script failure for testing purposes." in str(excinfo.value)

        # Assertions for job_registry
        registry_rows = list(bq_client.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_registry").result())
        assert len(registry_rows) == 1
        registry_entry = registry_rows[0]
        assert registry_entry.job_nr == 1
        assert registry_entry.status == 'ERROR'
        assert registry_entry.end_timestamp is not None

        # Assertions for job_log
        log_rows = list(bq_client.query(f"SELECT message, log_level FROM {PROJECT_ID}.{DATASET_ID}.job_log ORDER BY log_timestamp").result())
        assert len(log_rows) >= 2 # Start, main error
        assert "Job 1 (BERT_V_TA_CNTRCT_TEMPL) started for script r_ausd_v_ta_cntrct_templ.ksh." in [r.message for r in log_rows]
        assert any("Job 1 (BERT_V_TA_CNTRCT_TEMPL) failed with error: Simulated core script failure for testing purposes." in r.message and r.log_level == 'ERROR' for r in log_rows)

        # Assertions for job_error_log
        error_rows = list(bq_client.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_error_log").result())
        assert len(error_rows) == 1
        error_entry = error_rows[0]
        assert error_entry.job_nr == 1
        assert "Error in job 1: Simulated core script failure for testing purposes." in error_entry.message
        assert error_entry.error_timestamp is not None
    ```

---

## Test Case 5: Job Number Increment and Parameter Passing

*   **Purpose**: To verify that the `job_nr` is correctly incremented for each new job run and that the `p_param_s` and `p_param_l` parameters are correctly passed from the wrapper to the core script, including `NULL` values. This covers transformation correctness and external system replacement (parameter passing).
*   **Setup**:
    *   Clear all data from `project.dataset.job_registry`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
    *   Configure the dummy `sp_k_ausd_v_ta_cntrct_templ` to log the parameters it receives.
        ```sql
        CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_v_ta_cntrct_templ(
            p_job_nr INT64, p_job_kennung STRING, p_param_s STRING, p_param_l STRING
        )
        BEGIN
            INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
            VALUES (p_job_nr, p_job_kennung, 'DEBUG', FORMAT('Core script received s="%s", l="%s"', IFNULL(p_param_s, 'NULL'), IFNULL(p_param_l, 'NULL')), CURRENT_TIMESTAMP());
        END;
        ```
*   **Action**:
    1.  Call `project.dataset.sp_vertragsdatenabgleich(FALSE, 'value_s_1', 'value_l_1')`.
    2.  Call `project.dataset.sp_vertragsdatenabgleich(FALSE, 'value_s_2', NULL)`.
    3.  Call `project.dataset.sp_vertragsdatenabgleich(FALSE, NULL, 'value_l_3')`.
    4.  Call `project.dataset.sp_vertragsdatenabgleich(FALSE, NULL, NULL)`.
*   **Pass/Fail Criterion**:
    *   **`job_registry`**: Contains four records with `job_nr` values 1, 2, 3, and 4 respectively. All records have `status = 'SUCCESS'`.
    *   **`job_log`**: For each `job_nr`, there is a 'DEBUG' entry from `sp_k_ausd_v_ta_cntrct_templ` that accurately reflects the `p_param_s` and `p_param_l` values passed to `sp_vertragsdatenabgleich` (e.g., `s="value_s_1", l="value_l_1"`, `s="value_s_2", l="NULL"`, `s="NULL", l="value_l_3"`, `s="NULL", l="NULL"`).
    *   No errors are logged in `job_error_log`.

*   **Runnable Test Code (Pytest / SQL Assertions)**:

    ```python
    import pytest
    from google.cloud import bigquery

    PROJECT_ID = "project"
    DATASET_ID = "dataset"

    @pytest.fixture(scope="function")
    def setup_bq_for_param_test(bq_client):
        # Clear tables
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_registry").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_log").result()
        bq_client.query(f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.job_error_log").result()
        # Ensure dummy core script logs parameters
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE {PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_cntrct_templ(
                p_job_nr INT64, p_job_kennung STRING, p_param_s STRING, p_param_l STRING
            )
            BEGIN
                INSERT INTO {PROJECT_ID}.{DATASET_ID}.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
                VALUES (p_job_nr, p_job_kennung, 'DEBUG', FORMAT('Core script received s="%s", l="%s"', IFNULL(p_param_s, 'NULL'), IFNULL(p_param_l, 'NULL')), CURRENT_TIMESTAMP());
            END;
        """).result()
        yield

    def test_job_number_increment_and_param_passing(bq_client, setup_bq_for_param_test):
        test_cases = [
            ('value_s_1', 'value_l_1'),
            ('value_s_2', None),
            (None, 'value_l_3'),
            (None, None)
        ]
        
        for i, (s_param, l_param) in enumerate(test_cases):
            job_nr_expected = i + 1
            s_sql = f"'{s_param}'" if s_param is not None else "NULL"
            l_sql = f"'{l_param}'" if l_param is not None else "NULL"
            
            # Action: Call the main procedure
            bq_client.query(f"CALL {PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich(FALSE, {s_sql}, {l_sql})").result()
            
            # Assertions for job_registry for current job_nr
            registry_rows = list(bq_client.query(f"SELECT job_nr, status FROM {PROJECT_ID}.{DATASET_ID}.job_registry WHERE job_nr = {job_nr_expected}").result())
            assert len(registry_rows) == 1
            assert registry_rows[0].job_nr == job_nr_expected
            assert registry_rows[0].status == 'SUCCESS'

            # Assertions for job_log for parameter passing
            log_rows = list(bq_client.query(f"SELECT message, log_level FROM {PROJECT_ID}.{DATASET_ID}.job_log WHERE job_nr = {job_nr_expected} AND log_level = 'DEBUG'").result())
            assert len(log_rows) == 1
            expected_message = f'Core script received s="{s_param if s_param is not None else "NULL"}", l="{l_param if l_param is not None else "NULL"}"'
            assert log_rows[0].message == expected_message

        # Final check on total job_registry count
        total_registry_rows = list(bq_client.query(f"SELECT COUNT(*) as count FROM {PROJECT_ID}.{DATASET_ID}.job_registry").result())
        assert total_registry_rows[0].count == len(test_cases)

        # Assertions for job_error_log (should be empty)
        error_rows = list(bq_client.query(f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.job_error_log").result())
        assert len(error_rows) == 0
    ```

---