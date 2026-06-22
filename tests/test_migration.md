As a senior data-migration QA engineer, I've reviewed the migration design and the generated BigQuery and Airflow code for `vobs/dw_source/istools/seu/template/.dw_global`. The core challenge is translating a shell script's environment variable manipulation and conditional execution into a BigQuery Stored Procedure and an orchestration layer.

The tests below are designed to ensure behavioral equivalence, focusing on the specified areas: output parity, transformation correctness (string manipulation, conditional logic, NULL handling), external system replacements (Cognos flag), and data quality/schema.

For the purpose of these tests, we'll assume the following BigQuery environment:
*   **Project ID:** `test-gcp-project`
*   **Dataset Name:** `dw_global_test_dataset`
*   **Stored Procedure:** `test-gcp-project.dw_global_test_dataset.dw_global_init`

---

## Migration Validation Tests: `vobs/dw_source/istools/seu/template/.dw_global`

### 1. Test Case: Successful Environment Variable Initialization

*   **Purpose:** Verify that the BigQuery stored procedure correctly computes all derived environment variables and sets static NLS values when all required input parameters are provided and valid. This covers output parity and transformation correctness for path construction and static assignments.
*   **Setup:**
    *   All required input parameters for `dw_global_init` are provided with valid, non-empty string values.
    *   `p_existing_ld_library_path` and `p_existing_path` are provided with non-empty values.
    *   `p_cognos_setup_exists` is set to `FALSE`.
*   **Action:** Execute the `dw_global_init` stored procedure with the specified parameters.
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully without raising an error.
    *   The returned table contains exactly one row.
    *   The `computed_ld_library_path` column matches the expected value: `${p_oracle_home}/lib:${p_existing_ld_library_path}`.
    *   The `computed_path` column matches the expected value: `${p_existing_path}:${p_oracle_home}/bin:`.
    *   The `nls_lang` column is `GERMAN_GERMANY.WE8ISO8859P1`.
    *   The `nls_date_format` column is `DD-MON-YY`.
    *   The `nls_date_language` column is `AMERICAN`.
    *   The `cognos_note` column is `NULL`.

*   **Test Code (BigQuery SQL):**

    ```sql
    -- Setup: Create the dataset and procedure if not already done
    CREATE SCHEMA IF NOT EXISTS `test-gcp-project.dw_global_test_dataset` OPTIONS(location = 'us-central1');

    -- (Assume dw_global_init procedure is already deployed as per the generated code)

    -- Action: Call the stored procedure with valid inputs
    CALL `test-gcp-project.dw_global_test_dataset.dw_global_init`(
      p_dw_dir_root => '/app/dw/root_test',
      p_dw_dir_prot => '/app/dw/prot_test',
      p_dw_dir_cubes => '/app/dw/cubes_test',
      p_dw_dir_imp_d1 => '/app/dw/imp_d1_test',
      p_dw_dir_imp_xtra => '/app/dw/imp_xtra_test',
      p_dw_dir_imp_ctel => '/app/dw/imp_ctel_test',
      p_oracle_home => '/opt/oracle/product/19c_test',
      p_existing_ld_library_path => '/usr/local/lib_test:/opt/custom/lib',
      p_existing_path => '/usr/local/bin_test:/usr/bin_test',
      p_cognos_setup_exists => FALSE
    );

    -- Pass/Fail Criterion: Retrieve the result and assert
    SELECT
      dw_dir_root,
      dw_dir_prot,
      dw_dir_cubes,
      dw_dir_imp_d1,
      dw_dir_imp_xtra,
      dw_dir_imp_ctel,
      oracle_home,
      computed_ld_library_path,
      computed_path,
      nls_lang,
      nls_date_format,
      nls_date_language,
      cognos_note
    FROM TABLE(RESULT())
    WHERE
      dw_dir_root = '/app/dw/root_test' AND
      oracle_home = '/opt/oracle/product/19c_test' AND
      computed_ld_library_path = '/opt/oracle/product/19c_test/lib:/usr/local/lib_test:/opt/custom/lib' AND
      computed_path = '/usr/local/bin_test:/usr/bin_test:/opt/oracle/product/19c_test/bin:' AND
      nls_lang = 'GERMAN_GERMANY.WE8ISO8859P1' AND
      nls_date_format = 'DD-MON-YY' AND
      nls_date_language = 'AMERICAN' AND
      cognos_note IS NULL;
    ```

### 2. Test Case: Path Construction with Empty/NULL Existing Paths (Transformation Correctness - NULL Handling)

*   **Purpose:** Verify that `LD_LIBRARY_PATH` and `PATH` are correctly constructed when the existing `LD_LIBRARY_PATH` or `PATH` parameters are `NULL` or empty strings, ensuring proper NULL handling and concatenation logic.
*   **Setup:**
    *   All required parameters are valid.
    *   `p_existing_ld_library_path` is `NULL` or an empty string (`''`).
    *   `p_existing_path` is `NULL` or an empty string (`''`).
    *   `p_cognos_setup_exists` is `FALSE`.
*   **Action:** Execute the `dw_global_init` stored procedure with these parameters.
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully.
    *   The returned table contains exactly one row.
    *   `computed_ld_library_path` is `${p_oracle_home}/lib` (no leading colon).
    *   `computed_path` is `${p_oracle_home}/bin:` (no trailing colon from empty existing path).

*   **Test Code (BigQuery SQL):**

    ```sql
    -- Action: Call the stored procedure with NULL/empty existing paths
    CALL `test-gcp-project.dw_global_test_dataset.dw_global_init`(
      p_dw_dir_root => '/app/dw/root_null_path',
      p_dw_dir_prot => '/app/dw/prot_null_path',
      p_dw_dir_cubes => '/app/dw/cubes_null_path',
      p_dw_dir_imp_d1 => '/app/dw/imp_d1_null_path',
      p_dw_dir_imp_xtra => '/app/dw/imp_xtra_null_path',
      p_dw_dir_imp_ctel => '/app/dw/imp_ctel_null_path',
      p_oracle_home => '/opt/oracle/product/19c_null_path',
      p_existing_ld_library_path => NULL, -- Test with NULL
      p_existing_path => '',             -- Test with empty string
      p_cognos_setup_exists => FALSE
    );

    -- Pass/Fail Criterion: Retrieve the result and assert
    SELECT
      computed_ld_library_path,
      computed_path
    FROM TABLE(RESULT())
    WHERE
      computed_ld_library_path = '/opt/oracle/product/19c_null_path/lib' AND
      computed_path = '/opt/oracle/product/19c_null_path/bin:';
    ```

### 3. Test Case: Validation Failure - Single Missing Required Variable (Transformation Correctness - Error Handling)

*   **Purpose:** Verify that the stored procedure correctly identifies and raises an error when a single critical input parameter is missing or empty, matching the error message structure from the legacy script. This tests the `RAISE` mechanism.
*   **Setup:**
    *   All required parameters are valid, except for one (e.g., `p_dw_dir_root`), which is set to `NULL` or an empty string.
*   **Action:** Execute the `dw_global_init` stored procedure.
*   **Pass/Fail Criterion:**
    *   The procedure execution fails and raises an exception.
    *   The error message contains the specific text: "Fehler in .dw_global: Umgebungsvariable(n) nicht gesetzt: DW_DIR_ROOT Breche ab .." (or the name of the specific missing variable).

*   **Test Code (BigQuery SQL - Example for `p_dw_dir_root`):**

    ```sql
    -- Action: Call the stored procedure with a missing required input
    -- This query is expected to fail.
    SELECT * FROM EXTERNAL_QUERY(
      'test-gcp-project',
      'CALL `test-gcp-project.dw_global_test_dataset.dw_global_init`(
        p_dw_dir_root => NULL, -- Missing variable
        p_dw_dir_prot => \'/app/dw/prot_fail\',
        p_dw_dir_cubes => \'/app/dw/cubes_fail\',
        p_dw_dir_imp_d1 => \'/app/dw/imp_d1_fail\',
        p_dw_dir_imp_xtra => \'/app/dw/imp_xtra_fail\',
        p_dw_dir_imp_ctel => \'/app/dw/imp_ctel_fail\',
        p_oracle_home => \'/opt/oracle/product/19c_fail\',
        p_existing_ld_library_path => \'/usr/local/lib_fail\',
        p_existing_path => \'/usr/local/bin_fail\',
        p_cognos_setup_exists => FALSE
      )'
    );
    -- Expected output: An error message similar to:
    -- "BigQuery error: Failed to execute stored procedure: Fehler in .dw_global: Umgebungsvariable(n) nicht gesetzt: DW_DIR_ROOT Breche ab .."
    ```

### 4. Test Case: Validation Failure - Multiple Missing Required Variables (Transformation Correctness - Error Handling)

*   **Purpose:** Verify that the stored procedure correctly aggregates and lists multiple missing critical input parameters in its error message.
*   **Setup:**
    *   Multiple required parameters (e.g., `p_dw_dir_root`, `p_oracle_home`) are set to `NULL` or empty strings.
*   **Action:** Execute the `dw_global_init` stored procedure.
*   **Pass/Fail Criterion:**
    *   The procedure execution fails and raises an exception.
    *   The error message contains the specific text: "Fehler in .dw_global: Umgebungsvariable(n) nicht gesetzt: DW_DIR_ROOT ORACLE_HOME Breche ab .." (or the names of all specific missing variables).

*   **Test Code (BigQuery SQL - Example for `p_dw_dir_root` and `p_oracle_home`):**

    ```sql
    -- Action: Call the stored procedure with multiple missing required inputs
    -- This query is expected to fail.
    SELECT * FROM EXTERNAL_QUERY(
      'test-gcp-project',
      'CALL `test-gcp-project.dw_global_test_dataset.dw_global_init`(
        p_dw_dir_root => \'\', -- Missing variable (empty string)
        p_dw_dir_prot => \'/app/dw/prot_fail_multi\',
        p_dw_dir_cubes => \'/app/dw/cubes_fail_multi\',
        p_dw_dir_imp_d1 => \'/app/dw/imp_d1_fail_multi\',
        p_dw_dir_imp_xtra => \'/app/dw/imp_xtra_fail_multi\',
        p_dw_dir_imp_ctel => \'/app/dw/imp_ctel_fail_multi\',
        p_oracle_home => NULL, -- Missing variable (NULL)
        p_existing_ld_library_path => \'/usr/local/lib_fail_multi\',
        p_existing_path => \'/usr/local/bin_fail_multi\',
        p_cognos_setup_exists => FALSE
      )'
    );
    -- Expected output: An error message similar to:
    -- "BigQuery error: Failed to execute stored procedure: Fehler in .dw_global: Umgebungsvariable(n) nicht gesetzt: DW_DIR_ROOT ORACLE_HOME Breche ab .."
    ```

### 5. Test Case: Cognos Setup Flag - True (External System Replacement)

*   **Purpose:** Verify that when the `p_cognos_setup_exists` flag is true, the stored procedure correctly indicates the need for external orchestration via the `cognos_note` output. This validates the replacement of the shell `[ -f ... ]` check and `. /path/to/script.sh` sourcing.
*   **Setup:**
    *   All required parameters are valid.
    *   `p_cognos_setup_exists` is set to `TRUE`.
*   **Action:** Execute the `dw_global_init` stored procedure.
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully.
    *   The returned table contains exactly one row.
    *   The `cognos_note` column contains the string: "Cognos setup script exists; external orchestration must apply its effects."

*   **Test Code (BigQuery SQL):**

    ```sql
    -- Action: Call the stored procedure with cognos_setup_exists = TRUE
    CALL `test-gcp-project.dw_global_test_dataset.dw_global_init`(
      p_dw_dir_root => '/app/dw/root_cognos',
      p_dw_dir_prot => '/app/dw/prot_cognos',
      p_dw_dir_cubes => '/app/dw/cubes_cognos',
      p_dw_dir_imp_d1 => '/app/dw/imp_d1_cognos',
      p_dw_dir_imp_xtra => '/app/dw/imp_xtra_cognos',
      p_dw_dir_imp_ctel => '/app/dw/imp_ctel_cognos',
      p_oracle_home => '/opt/oracle/product/19c_cognos',
      p_existing_ld_library_path => '/usr/local/lib_cognos',
      p_existing_path => '/usr/local/bin_cognos',
      p_cognos_setup_exists => TRUE
    );

    -- Pass/Fail Criterion: Retrieve the result and assert
    SELECT
      cognos_note
    FROM TABLE(RESULT())
    WHERE
      cognos_note = 'Cognos setup script exists; external orchestration must apply its effects.';
    ```

### 6. Test Case: Cognos Setup Flag - False (External System Replacement)

*   **Purpose:** Verify that when the `p_cognos_setup_exists` flag is false, the `cognos_note` output is `NULL`, indicating no external Cognos action is required.
*   **Setup:**
    *   All required parameters are valid.
    *   `p_cognos_setup_exists` is set to `FALSE`.
*   **Action:** Execute the `dw_global_init` stored procedure.
*   **Pass/Fail Criterion:**
    *   The procedure executes successfully.
    *   The returned table contains exactly one row.
    *   The `cognos_note` column is `NULL`.

*   **Test Code (BigQuery SQL):**

    ```sql
    -- Action: Call the stored procedure with cognos_setup_exists = FALSE
    CALL `test-gcp-project.dw_global_test_dataset.dw_global_init`(
      p_dw_dir_root => '/app/dw/root_no_cognos',
      p_dw_dir_prot => '/app/dw/prot_no_cognos',
      p_dw_dir_cubes => '/app/dw/cubes_no_cognos',
      p_dw_dir_imp_d1 => '/app/dw/imp_d1_no_cognos',
      p_dw_dir_imp_xtra => '/app/dw/imp_xtra_no_cognos',
      p_dw_dir_imp_ctel => '/app/dw/imp_ctel_no_cognos',
      p_oracle_home => '/opt/oracle/product/19c_no_cognos',
      p_existing_ld_library_path => '/usr/local/lib_no_cognos',
      p_existing_path => '/usr/local/bin_no_cognos',
      p_cognos_setup_exists => FALSE
    );

    -- Pass/Fail Criterion: Retrieve the result and assert
    SELECT
      cognos_note
    FROM TABLE(RESULT())
    WHERE
      cognos_note IS NULL;
    ```

### 7. Test Case: Airflow Integration - Parameter Passing and Output Capture (Output Parity, External System Replacement)

*   **Purpose:** Verify that the Airflow DAG correctly fetches parameters from Airflow Variables, passes them to the BigQuery stored procedure, and successfully captures the procedure's output into XCom for downstream tasks.
*   **Setup:**
    *   Airflow Variables are configured with valid values for `gcp_project_id`, `bq_dataset_name`, and all `p_dw_dir_*`, `p_oracle_home`, `p_existing_ld_library_path`, `p_existing_path`, `p_cognos_setup_exists` parameters.
    *   The `dw_global_init` stored procedure is deployed in BigQuery.
*   **Action:** Trigger the `dw_global_orchestration` Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The `process_dw_global_output` task's XCom output (`dw_global_vars`) contains a dictionary with all expected computed and static values, matching the results from direct BigQuery calls (e.g., `computed_ld_library_path`, `computed_path`, `nls_lang`, `cognos_note`).

*   **Test Code (Python - Pytest-style assertion for Airflow XCom):**

    ```python
    import pytest
    from airflow.models import DagBag, Variable
    from airflow.utils.session import provide_session
    from airflow.utils.state import State
    from airflow.operators.python import PythonOperator
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
    from unittest.mock import patch, MagicMock
    import json

    # Assume the DAG file is in a 'dags' folder relative to the test
    DAG_PATH = "airflow/dags/dw_global_orchestration_dag.py"
    DAG_ID = "dw_global_orchestration"

    @pytest.fixture(scope="module")
    def dag():
        dag_bag = DagBag(dag_folder=".", include_examples=False)
        return dag_bag.get_dag(DAG_ID)

    @provide_session
    def test_dw_global_orchestration_success(session, dag):
        # Setup Airflow Variables
        Variable.set("gcp_project_id", "test-gcp-project")
        Variable.set("bq_dataset_name", "dw_global_test_dataset")
        Variable.set("dw_dir_root", "/airflow/dw/root")
        Variable.set("dw_dir_prot", "/airflow/dw/prot")
        Variable.set("dw_dir_cubes", "/airflow/dw/cubes")
        Variable.set("dw_dir_imp_d1", "/airflow/dw/imp_d1")
        Variable.set("dw_dir_imp_xtra", "/airflow/dw/imp_xtra")
        Variable.set("dw_dir_imp_ctel", "/airflow/dw/imp_ctel")
        Variable.set("oracle_home", "/airflow/oracle/home")
        Variable.set("existing_ld_library_path", "/airflow/lib")
        Variable.set("existing_path", "/airflow/bin")
        Variable.set("cognos_setup_exists", "true") # Test with Cognos true

        # Mock BigQueryHook and BigQueryInsertJobOperator to simulate BQ SP execution
        # and return a predefined result
        mock_bq_hook = MagicMock()
        mock_bq_hook.get_records.return_value = [[json.dumps({
            "dw_dir_root": "/airflow/dw/root",
            "dw_dir_prot": "/airflow/dw/prot",
            "dw_dir_cubes": "/airflow/dw/cubes",
            "dw_dir_imp_d1": "/airflow/dw/imp_d1",
            "dw_dir_imp_xtra": "/airflow/dw/imp_xtra",
            "dw_dir_imp_ctel": "/airflow/dw/imp_ctel",
            "oracle_home": "/airflow/oracle/home",
            "computed_ld_library_path": "/airflow/oracle/home/lib:/airflow/lib",
            "computed_path": "/airflow/bin:/airflow/oracle/home/bin:",
            "nls_lang": "GERMAN_GERMANY.WE8ISO8889P1",
            "nls_date_format": "DD-MON-YY",
            "nls_date_language": "AMERICAN",
            "cognos_note": "Cognos setup script exists; external orchestration must apply its effects."
        })]]

        with patch('airflow.providers.google.cloud.hooks.bigquery.BigQueryHook', return_value=mock_bq_hook):
            # Create a test DAG run
            dr = dag.create_dagrun(
                run_id="test_run_success",
                state=State.RUNNING,
                execution_date=dag.start_date,
                session=session,
            )

            # Manually run tasks (in a real test, you'd use a LocalExecutor or similar)
            # Task 1: fetch_config_parameters
            task_instance_fetch = dr.get_task_instance(task_id='fetch_config_parameters', session=session)
            task_instance_fetch.run(session=session)
            assert task_instance_fetch.xcom_pull(key='dw_global_sp_params') is not None

            # Task 2: call_dw_global_init (mocked BQ call)
            task_instance_call_bq = dr.get_task_instance(task_id='call_dw_global_init', session=session)
            # Simulate the destination_table attribute that the next task expects
            task_instance_call_bq.destination_table = {
                "tableId": "temp_dw_global_init_results_20230101_00_00_00"
            }
            task_instance_call_bq.run(session=session) # This will call the mocked BQ hook

            # Task 3: process_dw_global_output
            task_instance_process = dr.get_task_instance(task_id='process_dw_global_output', session=session)
            task_instance_process.run(session=session)

            # Assertions
            assert task_instance_process.current_state() == State.SUCCESS
            output_vars = task_instance_process.xcom_pull(key='dw_global_vars')
            assert output_vars is not None
            assert output_vars['dw_dir_root'] == "/airflow/dw/root"
            assert output_vars['computed_ld_library_path'] == "/airflow/oracle/home/lib:/airflow/lib"
            assert output_vars['computed_path'] == "/airflow/bin:/airflow/oracle/home/bin:"
            assert output_vars['nls_lang'] == "GERMAN_GERMANY.WE8ISO8889P1"
            assert output_vars['cognos_note'] == "Cognos setup script exists; external orchestration must apply its effects."

    ```

### 8. Test Case: Airflow Integration - Error Handling (Transformation Correctness - Error Handling)

*   **Purpose:** Verify that the Airflow DAG correctly handles and fails when the BigQuery stored procedure raises an error (e.g., due to missing required parameters).
*   **Setup:**
    *   Airflow Variables are configured such that one or more required parameters for `dw_global_init` are missing or empty (e.g., `dw_dir_root` is not set).
    *   The `dw_global_init` stored procedure is deployed in BigQuery.
*   **Action:** Trigger the `dw_global_orchestration` Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The `call_dw_global_init` task fails.
    *   The overall Airflow DAG run is marked as failed.
    *   The Airflow task logs for `call_dw_global_init` contain the BigQuery `RAISE` error message (e.g., "Fehler in .dw_global: Umgebungsvariable(n) nicht gesetzt: DW_DIR_ROOT Breche ab ..").

*   **Test Code (Python - Pytest-style assertion for Airflow DAG failure):**

    ```python
    import pytest
    from airflow.models import DagBag, Variable
    from airflow.utils.session import provide_session
    from airflow.utils.state import State
    from unittest.mock import patch, MagicMock
    from google.cloud.exceptions import GoogleCloudError # Or a more specific BQ exception

    DAG_PATH = "airflow/dags/dw_global_orchestration_dag.py"
    DAG_ID = "dw_global_orchestration"

    @pytest.fixture(scope="module")
    def dag():
        dag_bag = DagBag(dag_folder=".", include_examples=False)
        return dag_bag.get_dag(DAG_ID)

    @provide_session
    def test_dw_global_orchestration_failure(session, dag):
        # Setup Airflow Variables, with one missing
        Variable.set("gcp_project_id", "test-gcp-project")
        Variable.set("bq_dataset_name", "dw_global_test_dataset")
        # Variable.set("dw_dir_root", "/airflow/dw/root") # MISSING!
        Variable.set("dw_dir_prot", "/airflow/dw/prot")
        Variable.set("dw_dir_cubes", "/airflow/dw/cubes")
        Variable.set("dw_dir_imp_d1", "/airflow/dw/imp_d1")
        Variable.set("dw_dir_imp_xtra", "/airflow/dw/imp_xtra")
        Variable.set("dw_dir_imp_ctel", "/airflow/dw/imp_ctel")
        Variable.set("oracle_home", "/airflow/oracle/home")
        Variable.set("existing_ld_library_path", "/airflow/lib")
        Variable.set("existing_path", "/airflow/bin")
        Variable.set("cognos_setup_exists", "false")

        # Mock BigQueryInsertJobOperator to raise an exception, simulating BQ SP failure
        mock_bq_operator_execute = MagicMock(side_effect=GoogleCloudError(
            "400 Bad Request: Failed to execute stored procedure: Fehler in .dw_global: Umgebungsvariable(n) nicht gesetzt: DW_DIR_ROOT Breche ab .."
        ))

        with patch('airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator.execute', new=mock_bq_operator_execute):
            dr = dag.create_dagrun(
                run_id="test_run_failure",
                state=State.RUNNING,
                execution_date=dag.start_date,
                session=session,
            )

            # Task 1: fetch_config_parameters
            task_instance_fetch = dr.get_task_instance(task_id='fetch_config_parameters', session=session)
            task_instance_fetch.run(session=session)
            assert task_instance_fetch.current_state() == State.SUCCESS

            # Task 2: call_dw_global_init (this is where the mock will raise an error)
            task_instance_call_bq = dr.get_task_instance(task_id='call_dw_global_init', session=session)
            with pytest.raises(GoogleCloudError):
                task_instance_call_bq.run(session=session)

            # Verify the task state is failed
            task_instance_call_bq.refresh_from_db()
            assert task_instance_call_bq.current_state() == State.FAILED

            # The downstream task should not run
            task_instance_process = dr.get_task_instance(task_id='process_dw_global_output', session=session)
            task_instance_process.refresh_from_db()
            assert task_instance_process.current_state() == State.SKIPPED # or UPSTREAM_FAILED

    ```

### 9. Test Case: Output Schema and Row Count (Data Quality / Schema Assertions)

*   **Purpose:** Verify that the `dw_global_init` stored procedure consistently returns a single row with the expected schema (column names and data types).
*   **Setup:**
    *   All required input parameters are provided with valid, non-empty string values.
*   **Action:** Execute the `dw_global_init` stored procedure and query its result.
*   **Pass/Fail Criterion:**
    *   The returned table has exactly one row.
    *   The schema of the returned table matches the `RETURNS TABLE` definition in the stored procedure (column names and their respective data types).

*   **Test Code (BigQuery SQL):**

    ```sql
    -- Action: Call the stored procedure and then query its schema and row count
    CALL `test-gcp-project.dw_global_test_dataset.dw_global_init`(
      p_dw_dir_root => '/app/dw/root_schema',
      p_dw_dir_prot => '/app/dw/prot_schema',
      p_dw_dir_cubes => '/app/dw/cubes_schema',
      p_dw_dir_imp_d1 => '/app/dw/imp_d1_schema',
      p_dw_dir_imp_xtra => '/app/dw/imp_xtra_schema',
      p_dw_dir_imp_ctel => '/app/dw/imp_ctel_schema',
      p_oracle_home => '/opt/oracle/product/19c_schema',
      p_existing_ld_library_path => '/usr/local/lib_schema',
      p_existing_path => '/usr/local/bin_schema',
      p_cognos_setup_exists => FALSE
    );

    -- Pass/Fail Criterion 1: Check row count
    SELECT COUNT(*) FROM TABLE(RESULT()); -- Expected: 1

    -- Pass/Fail Criterion 2: Check schema (manual inspection or programmatic check)
    -- This requires querying BigQuery's INFORMATION_SCHEMA or a client library.
    -- Example using bq command line tool:
    -- bq query --use_legacy_sql=false "SELECT * FROM TABLE(RESULT()) LIMIT 0" --schema
    -- Expected Schema:
    -- [
    --   {"name": "dw_dir_root", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "dw_dir_prot", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "dw_dir_cubes", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "dw_dir_imp_d1", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "dw_dir_imp_xtra", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "dw_dir_imp_ctel", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "oracle_home", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "computed_ld_library_path", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "computed_path", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "nls_lang", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "nls_date_format", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "nls_date_language", "type": "STRING", "mode": "NULLABLE"},
    --   {"name": "cognos_note", "type": "STRING", "mode": "NULLABLE"}
    -- ]
    ```