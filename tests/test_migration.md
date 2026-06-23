The migration of `r_ausd_adressen.ksh` to a BigQuery Stored Procedure `sp_temp_adressabzug_crs` involves transforming shell scripting logic into BigQuery SQL, including parameter handling, logging, error management, and orchestrating a call to a core procedure. The following tests validate the behavioral equivalence of the migrated solution.

---

## Prerequisites for all Tests

Before running any tests, ensure the following:
1.  **BigQuery Environment**: A Google Cloud Project and BigQuery dataset (`project.dataset`) are set up.
2.  **Audit Tables Deployed**: The DDL for `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log` has been executed.
3.  **Stored Procedures Deployed**:
    *   `project.dataset.sp_ausd_adressen` (the placeholder for the core logic) is deployed.
    *   `project.dataset.sp_temp_adressabzug_crs` (the migrated wrapper) is deployed.
4.  **Airflow Environment (for Airflow tests)**: An Airflow instance is running, and the `airflow/dags/dag_temp_adressabzug_crs.py` DAG is deployed and unpaused.
5.  **Python Test Setup**: For `pytest` examples, ensure `google-cloud-bigquery` is installed and authenticated to your GCP project. Replace `PROJECT_ID` and `DATASET_ID` placeholders with your actual values.

---

## Test Suite: Migration Validation for `sp_temp_adressabzug_crs`

### Test 1.1: Successful Execution - No Parameters (Defaults Applied)

*   **Purpose**: Verify that when no `p_stichtag` or `p_wiederanlaufWert` are provided, the stored procedure correctly applies the default values: `p_stichtag` to the current system date (DDMMYYYY) and `p_wiederanlaufWert` to `0`.
*   **Setup**: Ensure `sp_ausd_adressen` is in its default placeholder state (does not raise errors).
*   **Action**: Execute `sp_temp_adressabzug_crs` with empty/NULL string parameters.
    ```sql
    CALL `project.dataset.sp_temp_adressabzug_crs`(p_stichtag => '', p_wiederanlaufWert => '');
    -- Alternatively, for explicit NULLs:
    -- CALL `project.dataset.sp_temp_adressabzug_crs`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    *   The procedure executes successfully without error.
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'OK'.
        *   `stichtag` column is `CURRENT_DATE()` (as a DATE type).
        *   `wiederanlauf_wert` column is `0`.
    *   Query `project.dataset.job_log` for the latest entries:
        *   At least two 'INFO' level messages are present, indicating job start and successful completion.
        *   The job start message explicitly mentions `Stichtag: <current_date>` and `Wiederanlaufwert: 0`.
    *   `project.dataset.job_error_log` contains no new entries.

*   **Pytest Example (Conceptual)**:
    ```python
    import pytest
    from google.cloud import bigquery
    import datetime

    PROJECT_ID = "your-gcp-project-id"
    DATASET_ID = "your_dataset_id"

    @pytest.fixture(scope="module")
    def bigquery_client():
        return bigquery.Client(project=PROJECT_ID)

    def call_sp_temp_adressabzug_crs(client, p_stichtag: str, p_wiederanlaufWert: str):
        query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.sp_temp_adressabzug_crs`(
            p_stichtag => '{p_stichtag}',
            p_wiederanlaufWert => '{p_wiederanlaufWert}'
        );
        """
        try:
            job = client.query(query)
            job.result() # Wait for the job to complete
            return True, None
        except Exception as e:
            return False, str(e)

    def get_last_job_run_data(client):
        query = f"""
        SELECT jc.*, ARRAY_AGG(jl.message ORDER BY jl.log_timestamp) as log_messages
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control` jc
        JOIN `{PROJECT_ID}.{DATASET_ID}.job_log` jl ON jc.job_run_id = jl.job_run_id
        ORDER BY jc.start_time DESC LIMIT 1
        """
        rows = list(client.query(query).result())
        return rows[0] if rows else None

    def test_successful_execution_no_params(bigquery_client):
        success, error_msg = call_sp_temp_adressabzug_crs(bigquery_client, '', '')
        assert success, f"Stored procedure failed: {error_msg}"

        job_data = get_last_job_run_data(bigquery_client)
        assert job_data is not None
        assert job_data.status == 'OK'
        assert job_data.stichtag == datetime.date.today()
        assert job_data.wiederanlauf_wert == 0
        assert any(f"Job sp_temp_adressabzug_crs started with Stichtag: {datetime.date.today()}" in msg for msg in job_data.log_messages)
        assert any("Job sp_temp_adressabzug_crs completed successfully." in msg for msg in job_data.log_messages)

        error_logs = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE job_run_id = '{job_data.job_run_id}'").result()
        assert len(list(error_logs)) == 0
    ```

### Test 1.2: Successful Execution - All Valid Parameters Provided

*   **Purpose**: Verify that the stored procedure correctly parses and uses explicitly provided `p_stichtag` and `p_wiederanlaufWert`.
*   **Setup**: Ensure `sp_ausd_adressen` is in its default placeholder state.
*   **Action**: Execute `sp_temp_adressabzug_crs` with specific valid parameters.
    ```sql
    CALL `project.dataset.sp_temp_adressabzug_crs`(p_stichtag => '01012023', p_wiederanlaufWert => '12345');
    ```
*   **Pass/Fail Criterion**:
    *   The procedure executes successfully without error.
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'OK'.
        *   `stichtag` column is `DATE '2023-01-01'`.
        *   `wiederanlauf_wert` column is `12345`.
    *   Query `project.dataset.job_log` for the latest entries:
        *   The job start message explicitly mentions `Stichtag: 2023-01-01` and `Wiederanlaufwert: 12345`.
    *   `project.dataset.job_error_log` contains no new entries.

### Test 1.3: Successful Execution - Only `p_stichtag` Provided

*   **Purpose**: Verify `p_stichtag` is used and `p_wiederanlaufWert` defaults to `0`.
*   **Setup**: Ensure `sp_ausd_adressen` is in its default placeholder state.
*   **Action**: Execute `sp_temp_adressabzug_crs` with `p_stichtag` and an empty `p_wiederanlaufWert`.
    ```sql
    CALL `project.dataset.sp_temp_adressabzug_crs`(p_stichtag => '15032024', p_wiederanlaufWert => '');
    ```
*   **Pass/Fail Criterion**:
    *   The procedure executes successfully without error.
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'OK'.
        *   `stichtag` column is `DATE '2024-03-15'`.
        *   `wiederanlauf_wert` column is `0`.
    *   Query `project.dataset.job_log` for the latest entries:
        *   The job start message explicitly mentions `Stichtag: 2024-03-15` and `Wiederanlaufwert: 0`.

### Test 1.4: Successful Execution - Only `p_wiederanlaufWert` Provided

*   **Purpose**: Verify `p_wiederanlaufWert` is used and `p_stichtag` defaults to `CURRENT_DATE()`.
*   **Setup**: Ensure `sp_ausd_adressen` is in its default placeholder state.
*   **Action**: Execute `sp_temp_adressabzug_crs` with an empty `p_stichtag` and a specific `p_wiederanlaufWert`.
    ```sql
    CALL `project.dataset.sp_temp_adressabzug_crs`(p_stichtag => '', p_wiederanlaufWert => '54321');
    ```
*   **Pass/Fail Criterion**:
    *   The procedure executes successfully without error.
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'OK'.
        *   `stichtag` column is `CURRENT_DATE()`.
        *   `wiederanlauf_wert` column is `54321`.
    *   Query `project.dataset.job_log` for the latest entries:
        *   The job start message explicitly mentions `Stichtag: <current_date>` and `Wiederanlaufwert: 54321`.

---

### Test 2.1: Error Handling - Invalid `p_stichtag` Format

*   **Purpose**: Verify the procedure correctly handles an invalid `p_stichtag` format (e.g., not DDMMYYYY) by failing and logging the error.
*   **Setup**: Ensure `sp_ausd_adressen` is in its default placeholder state.
*   **Action**: Execute `sp_temp_adressabzug_crs` with an invalid `p_stichtag` format.
    ```sql
    CALL `project.dataset.sp_temp_adressabzug_crs`(p_stichtag => '2023-01-01', p_wiederanlaufWert => '');
    ```
*   **Pass/Fail Criterion**:
    *   The procedure call fails with a BigQuery error (e.g., "Failed to parse date string").
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'FAILED'.
        *   `error_message` column contains a message related to date parsing failure.
    *   Query `project.dataset.job_log` for the latest entries:
        *   At least one 'ERROR' level message is present, detailing the failure.
    *   Query `project.dataset.job_error_log` for the latest entry:
        *   A new entry exists with `job_name = 'sp_temp_adressabzug_crs'` and `error_message` matching the parsing error.

*   **Pytest Example (Conceptual)**:
    ```python
    # ... (bigquery_client fixture and helper functions from Test 1.1) ...

    def test_error_invalid_stichtag_format(bigquery_client):
        success, error_msg = call_sp_temp_adressabzug_crs(bigquery_client, '2023-01-01', '')
        assert not success, "Stored procedure should have failed with invalid date format"
        assert "Failed to parse date string" in error_msg # Specific BigQuery error message

        job_data = get_last_job_run_data(bigquery_client)
        assert job_data is not None
        assert job_data.status == 'FAILED'
        assert "Failed to parse date string" in job_data.error_message

        error_logs = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE job_run_id = '{job_data.job_run_id}'").result()
        assert len(list(error_logs)) == 1
        assert "Failed to parse date string" in list(error_logs)[0].error_message
    ```

### Test 2.2: Error Handling - Core Procedure (`sp_ausd_adressen`) Fails

*   **Purpose**: Verify that `sp_temp_adressabzug_crs` correctly catches errors originating from the `sp_ausd_adressen` (core logic) and updates audit tables accordingly.
*   **Setup**:
    1.  **Temporarily modify `sp_ausd_adressen` to simulate a failure**:
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_adressen`(
            p_stichtag DATE,
            p_wiederanlaufWert INT64
        )
        OPTIONS(
            description="[PLACEHOLDER] This procedure will contain the core logic migrated from k_ausd_adressen.ksh. It performs the actual address extraction and transformation."
        )
        BEGIN
            RAISE USING MESSAGE = 'Simulated error in core procedure for testing purposes.';
        END;
        ```
    2.  Deploy this modified `sp_ausd_adressen`.
*   **Action**: Execute `sp_temp_adressabzug_crs` with valid parameters.
    ```sql
    CALL `project.dataset.sp_temp_adressabzug_crs`(p_stichtag => '01012023', p_wiederanlaufWert => '100');
    ```
*   **Pass/Fail Criterion**:
    *   The procedure call fails with the simulated error message ("Simulated error in core procedure...").
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'FAILED'.
        *   `error_message` column contains "Simulated error in core procedure...".
    *   Query `project.dataset.job_log` for the latest entries:
        *   At least one 'ERROR' level message is present, detailing the simulated failure.
    *   Query `project.dataset.job_error_log` for the latest entry:
        *   A new entry exists with `job_name = 'sp_temp_adressabzug_crs'` and `error_message` matching the simulated error.
*   **Teardown**: Revert `sp_ausd_adressen` to its original placeholder definition.

### Test 2.3: Error Handling - `p_stichtag` Validation (Explicitly NULL after defaulting)

*   **Purpose**: Verify the explicit `IF v_stichtag_date IS NULL THEN SIGNAL` validation within `sp_temp_adressabzug_crs` functions correctly. This tests the specific validation logic, assuming a scenario where `v_stichtag_date` somehow becomes NULL.
*   **Setup**:
    1.  **Temporarily modify `sp_temp_adressabzug_crs` to force `v_stichtag_date` to NULL**:
        ```sql
        -- ... (beginning of sp_temp_adressabzug_crs) ...
        SET v_stichtag_date = PARSE_DATE('%d%m%Y', IFNULL(NULLIF(TRIM(p_stichtag), ''), FORMAT_DATE('%d%m%Y', CURRENT_DATE())));

        -- FORCING NULL FOR TESTING PURPOSES
        SET v_stichtag_date = NULL;

        -- Log job start and initial parameters
        -- ... (rest of sp_temp_adressabzug_crs) ...
        ```
    2.  Deploy this modified `sp_temp_adressabzug_crs`.
*   **Action**: Execute `sp_temp_adressabzug_crs` with any valid parameters (e.g., `p_stichtag => '01012023'`).
    ```sql
    CALL `project.dataset.sp_temp_adressabzug_crs`(p_stichtag => '01012023', p_wiederanlaufWert => '');
    ```
*   **Pass/Fail Criterion**:
    *   The procedure call fails with the `SIGNAL` message: "Parameter p_stichtag cannot be null after defaulting...".
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'FAILED'.
        *   `error_message` column contains "Parameter p_stichtag cannot be null...".
    *   Query `project.dataset.job_log` for the latest entries:
        *   At least one 'ERROR' level message is present, detailing the `SIGNAL` error.
    *   Query `project.dataset.job_error_log` for the latest entry:
        *   A new entry exists with `job_name = 'sp_temp_adressabzug_crs'` and `error_message` matching the `SIGNAL` message.
*   **Teardown**: Revert `sp_temp_adressabzug_crs` to its original definition.

---

### Test 3.1: External System Replacement - Airflow DAG Triggers Successfully with Default Parameters

*   **Purpose**: Verify the Airflow DAG can successfully invoke `sp_temp_adressabzug_crs` without explicit parameters, relying on the SP's internal defaulting logic.
*   **Setup**:
    *   Ensure the `dag_temp_adressabzug_crs.py` DAG is deployed and unpaused in Airflow.
    *   Ensure `sp_ausd_adressen` is in its default placeholder state.
    *   Ensure the `google_cloud_default` connection is configured in Airflow.
*   **Action**: Manually trigger the `temp_adressabzug_crs` DAG in the Airflow UI, leaving the `stichtag` and `wiederanlaufwert` parameters empty/default.
*   **Pass/Fail Criterion**:
    *   The Airflow DAG run completes successfully (green status).
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'OK'.
        *   `stichtag` column is `CURRENT_DATE()`.
        *   `wiederanlauf_wert` column is `0`.
    *   Query `project.dataset.job_log` for the latest entries:
        *   Contains 'INFO' messages reflecting the job start with defaulted parameters and successful completion.

### Test 3.2: External System Replacement - Airflow DAG Triggers Successfully with Explicit Parameters

*   **Purpose**: Verify the Airflow DAG correctly passes explicit `stichtag` and `wiederanlaufwert` parameters to `sp_temp_adressabzug_crs`.
*   **Setup**:
    *   Ensure the `dag_temp_adressabzug_crs.py` DAG is deployed and unpaused.
    *   Ensure `sp_ausd_adressen` is in its default placeholder state.
*   **Action**: Manually trigger the `temp_adressabzug_crs` DAG in the Airflow UI, providing `stichtag='01012023'` and `wiederanlaufwert='999'` in the trigger configuration.
*   **Pass/Fail Criterion**:
    *   The Airflow DAG run completes successfully.
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'OK'.
        *   `stichtag` column is `DATE '2023-01-01'`.
        *   `wiederanlauf_wert` column is `999`.
    *   Query `project.dataset.job_log` for the latest entries:
        *   Contains 'INFO' messages reflecting the job start with `Stichtag: 2023-01-01` and `Wiederanlaufwert: 999`.

### Test 3.3: External System Replacement - Airflow DAG Handles SP Failure

*   **Purpose**: Verify the Airflow DAG correctly reports a failure if `sp_temp_adressabzug_crs` (or its called sub-procedure) encounters an error.
*   **Setup**:
    1.  **Temporarily modify `sp_ausd_adressen` to simulate a failure** (same as Test 2.2 setup).
    2.  Deploy this modified `sp_ausd_adressen`.
    3.  Ensure the `dag_temp_adressabzug_crs.py` DAG is deployed and unpaused.
*   **Action**: Manually trigger the `temp_adressabzug_crs` DAG in the Airflow UI (parameters don't matter, as `sp_ausd_adressen` will fail).
*   **Pass/Fail Criterion**:
    *   The Airflow DAG run fails (red status).
    *   Query `project.dataset.job_control` for the latest entry:
        *   `status` column is 'FAILED'.
        *   `error_message` column contains the simulated error message from `sp_ausd_adressen`.
    *   Query `project.dataset.job_log` and `project.dataset.job_error_log` for the latest entries:
        *   Both tables contain entries reflecting the error.
*   **Teardown**: Revert `sp_ausd_adressen` to its original placeholder definition.

---

### Test 4.1: Data Quality / Schema Assertions - Audit Table Integrity

*   **Purpose**: Verify that the audit tables (`job_control`, `job_log`, `job_error_log`) exist, have the correct schema, and store data with appropriate types and non-null constraints.
*   **Setup**: Run any successful test case (e.g., Test 1.2) to populate the audit tables.
*   **Action**: Query the schema of the audit tables and inspect data types and content.
    ```sql
    SELECT column_name, data_type, is_nullable
    FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name IN ('job_control', 'job_log', 'job_error_log')
    ORDER BY table_name, ordinal_position;

    -- Example data inspection for job_control
    SELECT job_run_id, job_name, start_time, end_time, status, stichtag, wiederanlauf_wert, error_message
    FROM `project.dataset.job_control`
    WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) -- Adjust time window as needed
    ORDER BY start_time DESC
    LIMIT 1;
    ```
*   **Pass/Fail Criterion**:
    *   **`job_control` table**:
        *   `job_run_id`: `STRING`, `NOT NULL`. Contains a valid UUID format.
        *   `job_name`: `STRING`, `NOT NULL`. Contains 'sp_temp_adressabzug_crs'.
        *   `start_time`: `TIMESTAMP`, `NOT NULL`. Contains valid timestamps.
        *   `end_time`: `TIMESTAMP`, `NULLABLE`. Contains valid timestamps for completed jobs.
        *   `status`: `STRING`, `NOT NULL`. Contains 'RUNNING', 'OK', or 'FAILED'.
        *   `stichtag`: `DATE`, `NULLABLE`. Contains valid dates or NULL.
        *   `wiederanlauf_wert`: `INT64`, `NULLABLE`. Contains valid integers or NULL.
        *   `error_message`: `STRING`, `NULLABLE`. Populated for failed jobs.
    *   **`job_log` table**:
        *   `job_run_id`: `STRING`, `NOT NULL`. Matches `job_control.job_run_id`.
        *   `log_timestamp`: `TIMESTAMP`, `NOT NULL`. Contains valid timestamps.
        *   `log_level`: `STRING`, `NOT NULL`. Contains 'INFO', 'WARN', or 'ERROR'.
        *   `message`: `STRING`, `NOT NULL`. Contains descriptive log messages.
    *   **`job_error_log` table**:
        *   `job_run_id`: `STRING`, `NOT NULL`. Matches `job_control.job_run_id`.
        *   `error_timestamp`: `TIMESTAMP`, `NOT NULL`. Contains valid timestamps.
        *   `job_name`: `STRING`, `NOT NULL`. Contains 'sp_temp_adressabzug_crs'.
        *   `error_message`: `STRING`, `NOT NULL`. Contains detailed error messages.
        *   `stack_trace`: `STRING`, `NULLABLE`. Contains stack trace for errors.
    *   No unexpected columns or data types are present. All `NOT NULL` columns are populated in relevant scenarios.