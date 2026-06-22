The migration of `r_ausd_v_ta_p_vertrag.ksh` to an Airflow DAG `vertragsdatenabgleich_ta_p_vertrag.py` primarily involves translating orchestration logic, parameter handling, error trapping, and logging from KornShell to Python/Airflow. The core data synchronization logic, originally in `k_ausd_v_ta_p_vertrag.ksh`, is represented by a placeholder BigQuery SQL task.

These tests focus on validating the behavior of the orchestrator, given the unknown nature of the core synchronization script.

---

## Migration Validation Tests for `vertragsdatenabgleich_ta_p_vertrag.py`

### Test Case 1: Successful Orchestration and Core Logic Execution

**Purpose:**
To verify that the migrated Airflow DAG executes successfully end-to-end when provided with valid parameters, correctly initializes the environment, parses parameters, invokes the core synchronization logic, and logs a successful completion. This covers the primary success path and output parity for logging.

**Setup:**
1.  Ensure the Airflow DAG `vertragsdatenabgleich_ta_p_vertrag.py` is deployed to a Cloud Composer environment.
2.  Create a dummy `test_project.test_dataset.source_vertrag` table in BigQuery with at least one row, as the `execute_core_sync` task's placeholder SQL depends on it.
    ```sql
    CREATE TABLE IF NOT EXISTS `test_project.test_dataset.source_vertrag` (
        id INT64,
        contract_name STRING,
        start_date DATE
    );
    INSERT INTO `test_project.test_dataset.source_vertrag` (id, contract_name, start_date)
    VALUES (1, 'Contract A', '2023-01-01');
    ```
3.  Modify the `execute_core_sync` task in the DAG to use `test_project.test_dataset` for both source and target tables.
    ```python
    # ... inside execute_core_sync task definition ...
    sql="""
        CREATE OR REPLACE TABLE `test_project.test_dataset.ta_p_vertrag` AS
        WITH source_data AS (
            SELECT
                *
            FROM `test_project.test_dataset.source_vertrag`
        ),
        normalized_data AS (
            SELECT
                *,
                CURRENT_DATE() AS load_date
            FROM source_data
        ),
        final_data AS (
            SELECT
                *
            FROM normalized_data
        )
        SELECT
            *
        FROM final_data;
    """,
    # ...
    ```

**Action:**
1.  Trigger the Airflow DAG `vertragsdatenabgleich_ta_p_vertrag` manually via the Airflow UI or CLI.
2.  Provide the following DAG run configuration (JSON):
    ```json
    {
        "JobKennung": "BERT_V_TA_P_VERTRAG_TEST",
        "DW_EintragsNr": "12345"
    }
    ```
3.  Monitor the DAG run status in the Airflow UI.
4.  Check the logs for each task in Cloud Logging.
5.  Query the BigQuery table `test_project.test_dataset.ta_p_vertrag`.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG run completes successfully (status `success`).
    *   All tasks (`initialize_environment`, `parse_parameters`, `execute_core_sync`, `handle_success`) complete successfully.
    *   Cloud Logging for the `initialize_environment` task contains messages indicating environment setup and the provided `JobKennung` and `DW_EintragsNr`.
    *   Cloud Logging for the `parse_parameters` task contains messages confirming `JobKennung=BERT_V_TA_P_VERTRAG_TEST` and `DW_EintragsNr=12345` were parsed.
    *   Cloud Logging for the `handle_success` task contains a message similar to "Job BERT_V_TA_P_VERTRAG_TEST completed successfully!".
    *   The BigQuery table `test_project.test_dataset.ta_p_vertrag` is created/overwritten and contains data from `source_vertrag` with an added `load_date` column.
        ```sql
        SELECT COUNT(*) FROM `test_project.test_dataset.ta_p_vertrag`; -- Should return 1
        SELECT id, contract_name, start_date, load_date FROM `test_project.test_dataset.ta_p_vertrag`;
        -- Expected: (1, 'Contract A', '2023-01-01', CURRENT_DATE())
        ```
*   **Fail:** Any of the above conditions are not met.

---

### Test Case 2: Parameter Handling - Missing Required Parameters

**Purpose:**
To verify that the migrated DAG correctly identifies and handles missing required parameters, failing early and triggering the failure handling mechanism, similar to how the legacy script would exit with an error code if `getopts` failed. This validates parameter parsing and error handling.

**Setup:**
1.  Ensure the Airflow DAG `vertragsdatenabgleich_ta_p_vertrag.py` is deployed.
2.  Ensure the `handle_failure_task` is correctly configured as `on_failure_callback`.

**Action:**
1.  Trigger the Airflow DAG `vertragsdatenabgleich_ta_p_vertrag` manually.
2.  Provide an incomplete DAG run configuration (JSON), e.g., missing `DW_EintragsNr`:
    ```json
    {
        "JobKennung": "BERT_V_TA_P_VERTRAG_MISSING_PARAM"
    }
    ```
3.  Monitor the DAG run status in the Airflow UI.
4.  Check the logs for the `parse_parameters` task and the overall DAG run in Cloud Logging.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG run fails (status `failed`).
    *   The `parse_parameters` task fails.
    *   Cloud Logging for the `parse_parameters` task shows an error message indicating missing parameters (e.g., "ValueError: Missing required parameters: JobKennung and DW_EintragsNr").
    *   The `handle_failure_task` is invoked, and its logs indicate a job failure.
*   **Fail:**
    *   The DAG run completes successfully.
    *   The `parse_parameters` task does not fail as expected.
    *   The `handle_failure_task` is not invoked upon parameter parsing failure.

---

### Test Case 3: Error Handling - Core Synchronization Logic Failure

**Purpose:**
To verify that the migrated DAG's error handling (`on_failure_callback`) is correctly triggered when the core synchronization logic (represented by `execute_core_sync`) encounters an error, mimicking the `trap ERR` behavior of the legacy script. This validates the robustness of the error handling.

**Setup:**
1.  Ensure the Airflow DAG `vertragsdatenabgleich_ta_p_vertrag.py` is deployed.
2.  Modify the `execute_core_sync` task in the DAG to intentionally cause a BigQuery error. For example, try to query a non-existent table or use invalid SQL.
    ```python
    # ... inside execute_core_sync task definition ...
    sql="""
        SELECT * FROM `test_project.test_dataset.non_existent_table`; -- This will cause an error
    """,
    # ...
    ```
3.  Ensure the `handle_failure_task` is correctly configured as `on_failure_callback`.

**Action:**
1.  Trigger the Airflow DAG `vertragsdatenabgleich_ta_p_vertrag` manually.
2.  Provide valid DAG run configuration:
    ```json
    {
        "JobKennung": "BERT_V_TA_P_VERTRAG_ERROR_TEST",
        "DW_EintragsNr": "67890"
    }
    ```
3.  Monitor the DAG run status in the Airflow UI.
4.  Check the logs for the `execute_core_sync` task and the overall DAG run in Cloud Logging.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The Airflow DAG run fails (status `failed`).
    *   The `execute_core_sync` task fails with a BigQuery-related error.
    *   Cloud Logging for the `execute_core_sync` task shows the BigQuery error details.
    *   The `handle_failure_task` is invoked, and its logs indicate a job failure, potentially including the exception message from the failed task.
*   **Fail:**
    *   The DAG run completes successfully despite the BigQuery error.
    *   The `handle_failure_task` is not invoked.

---

### Test Case 4: Output Parity - Logging Content and Structure

**Purpose:**
To verify that the migrated DAG produces comparable logging information (start messages, parameter details, success/failure indicators) to the legacy script, even if the format differs due to Cloud Logging integration. This covers output parity for logging.

**Setup:**
1.  Ensure the Airflow DAG `vertragsdatenabgleich_ta_p_vertrag.py` is deployed.
2.  Have a successful run of the legacy `r_ausd_v_ta_p_vertrag.ksh` script and save its log file.
3.  Have a successful run of the migrated Airflow DAG (from Test Case 1).

**Action:**
1.  Compare the content of the legacy log file with the aggregated logs from the successful Airflow DAG run in Cloud Logging.
2.  Specifically look for:
    *   Job start indication.
    *   Job ID/Kennung and Entry Number (`DW_EintragsNr`).
    *   Log file name (or equivalent in Cloud Logging context).
    *   Indication of core script execution.
    *   Job success message.

**Pass/Fail Criterion:**
*   **Pass:**
    *   Cloud Logging for the Airflow DAG contains clear indications of the job's start, the parameters used (`JobKennung`, `DW_EintragsNr`), the execution of the core synchronization task, and a final success message.
    *   While the exact wording and format will differ (e.g., no explicit "Logdatei" in Airflow logs, but task IDs and DAG run IDs provide context), the *information conveyed* about the job's lifecycle and key identifiers is present and easily retrievable.
    *   Example log snippets from Cloud Logging should reflect:
        *   `initialize_environment`: "Initializing environment...", "Environment initialized for job: BERT_V_TA_P_VERTRAG_TEST", "DW_EintragsNr: 12345"
        *   `parse_parameters`: "Parsing parameters...", "Parameters parsed: JobKennung=BERT_V_TA_P_VERTRAG_TEST, DW_EintragsNr=12345"
        *   `execute_core_sync`: BigQuery query execution details.
        *   `handle_success`: "Job BERT_V_TA_P_VERTRAG_TEST completed successfully!"
*   **Fail:** Key informational messages about the job's execution, parameters, or status are missing or unclear in Cloud Logging.

---

### Test Case 5: Schema and Data Quality Assertion for `ta_p_vertrag`

**Purpose:**
To ensure that the target table `ta_p_vertrag` (created/updated by the `execute_core_sync` task) has the expected schema and that basic data quality (e.g., row count) is maintained, based on the placeholder SQL provided. This covers data quality and schema assertions, acknowledging the placeholder nature of the core logic.

**Setup:**
1.  Perform a successful run of the Airflow DAG (as in Test Case 1).
2.  Ensure `test_project.test_dataset.source_vertrag` contains known test data.

**Action:**
1.  After a successful DAG run, query the schema of `test_project.test_dataset.ta_p_vertrag` in BigQuery.
2.  Query the row count of `test_project.test_dataset.ta_p_vertrag`.
3.  Query the data content of `test_project.test_dataset.ta_p_vertrag`.

**Pass/Fail Criterion:**
*   **Pass:**
    *   The `test_project.test_dataset.ta_p_vertrag` table exists.
    *   The schema of `ta_p_vertrag` matches the expected schema based on the placeholder SQL: `id INT64`, `contract_name STRING`, `start_date DATE`, `load_date DATE`.
    *   The row count of `ta_p_vertrag` is equal to the row count of `source_vertrag` (e.g., 1 row from the setup).
    *   The `load_date` column contains the current date for all rows.
    *   The data from `source_vertrag` is correctly copied into `ta_p_vertrag`.

    ```python
    # Example Pytest assertion for schema and row count
    from google.cloud import bigquery
    import datetime

    def test_ta_p_vertrag_schema_and_data():
        client = bigquery.Client()
        table_id = "test_project.test_dataset.ta_p_vertrag"

        # Assert schema
        table = client.get_table(table_id)
        expected_schema = [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("contract_name", "STRING"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("load_date", "DATE"),
        ]
        assert len(table.schema) == len(expected_schema)
        for expected_field in expected_schema:
            assert any(f.name == expected_field.name and f.field_type == expected_field.field_type for f in table.schema)

        # Assert row count
        query_job = client.query(f"SELECT COUNT(*) FROM `{table_id}`")
        rows = list(query_job.result())
        assert rows[0][0] == 1 # Assuming 1 row in source_vertrag

        # Assert data content and load_date
        query_job = client.query(f"SELECT id, contract_name, start_date, load_date FROM `{table_id}`")
        rows = list(query_job.result())
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].contract_name == 'Contract A'
        assert rows[0].start_date == datetime.date(2023, 1, 1)
        assert rows[0].load_date == datetime.date.today()
    ```
*   **Fail:** Any of the above conditions are not met.

---