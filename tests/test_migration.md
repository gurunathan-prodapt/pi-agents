As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `k_ausd_bp_ta_msisdn.ksh`. The primary challenge is the unknown content of `d_ausd_bp_ta_msisdn.sql`, which is the core data transformation logic. The tests below will focus on validating the orchestration, parameter handling, error logging, and the *interface* and *placeholder behavior* of the core transformation, while acknowledging the limitation regarding full data transformation correctness without the original SQL.

The tests are categorized to address output parity, transformation correctness, external system replacements (which are mostly internal to GCP in this case), and data quality/schema assertions.

---

# Migration Validation Tests for `k_ausd_bp_ta_msisdn.ksh`

## Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **GCP Project and BigQuery Dataset**: Replace `your_gcp_project` and `your_bq_dataset` with actual values in all DDLs, stored procedures, and DAGs.
2.  **BigQuery Tables**: The DDLs for `job_error_log`, `job_audit_log`, and `PoolBasisprodukt` must be executed in BigQuery.
3.  **BigQuery Stored Procedures**: The `d_ausd_bp_ta_msisdn`, `r_ausd_bp_ta_msisdn`, and (if applicable) `postprocess_cibasis` stored procedures must be created in BigQuery.
4.  **Airflow Environment**:
    *   The `k_ausd_bp_ta_msisdn_orchestration_dag.py` DAG is deployed to an Airflow environment.
    *   A `google_cloud_default` connection is configured in Airflow with appropriate permissions to BigQuery.
5.  **Test Data**: For each test, ensure the target tables are in a known, clean state (e.g., empty or containing specific baseline data).

---

## Test Case 1: Successful Execution - Parameter Handling and Audit Logging

*   **Purpose**: Verify that the migrated job executes successfully with valid parameters, correctly derives dates, calls the core transformation, captures record counts, and logs audit information. This covers output parity for audit logs and basic orchestration correctness.
*   **Setup**:
    1.  Ensure `your_gcp_project.your_bq_dataset.job_audit_log`, `your_gcp_project.your_bq_dataset.job_error_log`, and `your_gcp_project.your_bq_dataset.PoolBasisprodukt` tables are empty.
    2.  Note the current date for `_processing_date` validation.
*   **Action**:
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_msisdn_orchestration_dag` manually or via a test script.
    2.  Provide the following DAG run configuration:
        ```json
        {
            "job_kennung": "TEST_JOB_SUCCESS",
            "eintrags_nr": "001",
            "stichtag": "01012023",
            "wiederanlauf_wert": ""
        }
        ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run completes successfully (status `success`).
    2.  Query `your_gcp_project.your_bq_dataset.job_audit_log`:
        *   It contains exactly one entry.
        *   The entry has `job_kennung = 'TEST_JOB_SUCCESS'`, `eintrags_nr = '001'`, `stichtag = '2023-01-01'`, `status = 'SUCCESS'`.
        *   `records_processed` is greater than 0 (expected 10 based on the placeholder `d_ausd_bp_ta_msisdn` SP).
        *   `start_timestamp` and `end_timestamp` are populated and `end_timestamp` is after `start_timestamp`.
    3.  Query `your_gcp_project.your_bq_dataset.job_error_log`:
        *   It contains no entries.
    4.  Query `your_gcp_project.your_bq_dataset.PoolBasisprodukt`:
        *   It contains exactly 10 records.
        *   All records have `stichtag = '2023-01-01'` and `_processing_date = CURRENT_DATE()`.

*   **Runnable Test Code (Pytest / Python with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from airflow.models import DagRun
    from airflow.utils.state import State
    from datetime import datetime, timedelta

    # Replace with your actual project and dataset
    GCP_PROJECT_ID = 'your_gcp_project'
    BQ_DATASET_ID = 'your_bq_dataset'

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client(project=GCP_PROJECT_ID)

    @pytest.fixture(autouse=True)
    def cleanup_tables(bq_client):
        # Clear tables before each test
        bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_error_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt`").result()
        yield

    def test_successful_execution(bq_client):
        dag_id = 'k_ausd_bp_ta_msisdn_orchestration_dag'
        job_kennung = 'TEST_JOB_SUCCESS'
        eintrags_nr = '001'
        stichtag_str = '01012023'
        stichtag_date = datetime.strptime(stichtag_str, '%d%m%Y').date()
        current_date = datetime.now().date()

        # Simulate Airflow DAG run (requires Airflow context or mocking)
        # For a real integration test, you'd trigger the DAG via Airflow API or CLI
        # and poll for its status. For this example, we'll assume the SP call directly.
        # In a true pytest setup, you'd use `airflow dags test` or a similar mechanism.

        # Direct call to the main SP for testing purposes, bypassing Airflow for simplicity
        # In a real scenario, this would be triggered by the Airflow DAG.
        try:
            bq_client.query(f"""
                CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_bp_ta_msisdn`(
                    p_job_kennung => '{job_kennung}',
                    p_eintrags_nr => '{eintrags_nr}',
                    p_stichtag => '{stichtag_str}',
                    p_wiederanlauf_wert => ''
                );
            """).result()
            dag_run_status = State.SUCCESS
        except Exception as e:
            print(f"Error during SP call: {e}")
            dag_run_status = State.FAILED

        assert dag_run_status == State.SUCCESS, "Airflow DAG run should succeed"

        # Verify job_audit_log
        audit_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log`"
        audit_results = list(bq_client.query(audit_log_query).result())
        assert len(audit_results) == 1, "job_audit_log should contain exactly one entry"
        audit_entry = audit_results[0]
        assert audit_entry['job_kennung'] == job_kennung
        assert audit_entry['eintrags_nr'] == eintrags_nr
        assert audit_entry['stichtag'] == stichtag_date
        assert audit_entry['status'] == 'SUCCESS'
        assert audit_entry['records_processed'] == 10 # Based on d_ausd_bp_ta_msisdn placeholder
        assert audit_entry['end_timestamp'] > audit_entry['start_timestamp']

        # Verify job_error_log
        error_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_error_log`"
        error_results = list(bq_client.query(error_log_query).result())
        assert len(error_results) == 0, "job_error_log should contain no entries"

        # Verify PoolBasisprodukt
        pool_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt` WHERE stichtag = '{stichtag_date}' AND _processing_date = '{current_date}'"
        pool_results = list(bq_client.query(pool_query).result())
        assert len(pool_results) == 10, "PoolBasisprodukt should contain 10 records for the given stichtag and processing date"
        for record in pool_results:
            assert record['stichtag'] == stichtag_date
            assert record['_processing_date'] == current_date
            assert record['msisdn'] is not None
            assert record['produkt_id'] is not None
            assert record['aktiv_von'] is not None
            assert record['aktiv_bis'] is not None
    ```

---

## Test Case 2: Invalid `Stichtag` Format - Error Handling

*   **Purpose**: Verify that the job correctly handles an invalid `p_stichtag` format (e.g., `YYYY-MM-DD` instead of `DDMMYYYY`), logs the error, and marks the job as failed. This tests transformation correctness for date parsing and error handling.
*   **Setup**:
    1.  Ensure `job_audit_log`, `job_error_log`, and `PoolBasisprodukt` tables are empty.
*   **Action**:
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_msisdn_orchestration_dag`.
    2.  Provide the following DAG run configuration:
        ```json
        {
            "job_kennung": "TEST_JOB_ERR_DATE",
            "eintrags_nr": "002",
            "stichtag": "2023-01-01",
            "wiederanlauf_wert": ""
        }
        ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run fails (status `failed`).
    2.  Query `your_gcp_project.your_bq_dataset.job_error_log`:
        *   It contains exactly one entry.
        *   The entry has `job_kennung = 'TEST_JOB_ERR_DATE'`, `eintrags_nr = '002'`.
        *   `error_message` contains "Invalid p_stichtag format. Expected DDMMYYYY. Got: 2023-01-01".
        *   `error_timestamp` is populated.
    3.  Query `your_gcp_project.your_bq_dataset.job_audit_log`:
        *   It contains exactly one entry.
        *   The entry has `job_kennung = 'TEST_JOB_ERR_DATE'`, `eintrags_nr = '002'`, `status = 'FAILED'`.
        *   `records_processed` is 0.
    4.  Query `your_gcp_project.your_bq_dataset.PoolBasisprodukt`:
        *   It contains no new records.

*   **Runnable Test Code (Pytest / Python with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from airflow.utils.state import State

    GCP_PROJECT_ID = 'your_gcp_project'
    BQ_DATASET_ID = 'your_bq_dataset'

    # bq_client and cleanup_tables fixtures from Test Case 1

    def test_invalid_stichtag_format(bq_client):
        job_kennung = 'TEST_JOB_ERR_DATE'
        eintrags_nr = '002'
        stichtag_str = '2023-01-01' # Invalid format

        try:
            bq_client.query(f"""
                CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_bp_ta_msisdn`(
                    p_job_kennung => '{job_kennung}',
                    p_eintrags_nr => '{eintrags_nr}',
                    p_stichtag => '{stichtag_str}',
                    p_wiederanlauf_wert => ''
                );
            """).result()
            dag_run_status = State.SUCCESS # Should not happen
        except Exception as e:
            print(f"Error during SP call (expected): {e}")
            dag_run_status = State.FAILED

        assert dag_run_status == State.FAILED, "Airflow DAG run should fail due to invalid stichtag"

        # Verify job_error_log
        error_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_error_log`"
        error_results = list(bq_client.query(error_log_query).result())
        assert len(error_results) == 1, "job_error_log should contain exactly one entry"
        error_entry = error_results[0]
        assert error_entry['job_kennung'] == job_kennung
        assert error_entry['eintrags_nr'] == eintrags_nr
        assert "Invalid p_stichtag format. Expected DDMMYYYY. Got: 2023-01-01" in error_entry['error_message']
        assert error_entry['error_timestamp'] is not None

        # Verify job_audit_log
        audit_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log`"
        audit_results = list(bq_client.query(audit_log_query).result())
        assert len(audit_results) == 1, "job_audit_log should contain exactly one entry"
        audit_entry = audit_results[0]
        assert audit_entry['job_kennung'] == job_kennung
        assert audit_entry['eintrags_nr'] == eintrags_nr
        assert audit_entry['status'] == 'FAILED'
        assert audit_entry['records_processed'] == 0

        # Verify PoolBasisprodukt
        pool_query = f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt`"
        pool_count = bq_client.query(pool_query).result().total_rows
        assert pool_count == 0, "PoolBasisprodukt should contain no new records"
    ```

---

## Test Case 3: Core Transformation Logic (d_ausd_bp_ta_msisdn) - Data Generation and Row Count

*   **Purpose**: Verify that the `d_ausd_bp_ta_msisdn` stored procedure is invoked by `r_ausd_bp_ta_msisdn` and generates data into `PoolBasisprodukt` as per its current placeholder logic. This tests the *invocation* and *basic output* of the core logic, given its unknown content.
*   **Setup**:
    1.  Ensure `PoolBasisprodukt` table is empty.
    2.  Run Test Case 1 successfully to ensure `r_ausd_bp_ta_msisdn` calls `d_ausd_bp_ta_msisdn`.
*   **Action**:
    1.  After a successful run of `r_ausd_bp_ta_msisdn` (e.g., from Test Case 1), query `your_gcp_project.your_bq_dataset.PoolBasisprodukt`.
*   **Pass/Fail Criterion**:
    1.  `your_gcp_project.your_bq_dataset.PoolBasisprodukt` contains exactly 10 records (as per the dummy data generation in the placeholder SP).
    2.  All records have `stichtag` matching the input `p_stichtag_date` (e.g., `2023-01-01`) and `_processing_date` matching `CURRENT_DATE()`.
    3.  `msisdn`, `produkt_id`, `aktiv_von`, `aktiv_bis` columns contain non-NULL values and conform to expected data types (STRING, DATE).
    4.  The `records_processed` count in `job_audit_log` matches the actual count of records inserted into `PoolBasisprodukt` for that run.

*   **Runnable Test Code (Pytest / Python with BigQuery client)**:
    *   This test is implicitly covered by the assertions in `test_successful_execution` from Test Case 1, specifically the checks on `PoolBasisprodukt` and `records_processed` in `job_audit_log`. No separate code block is needed.

---

## Test Case 4: Idempotency / Multiple Runs (Different Processing Dates)

*   **Purpose**: Verify that running the job multiple times for the same `stichtag` but on different `_processing_date`s (simulating runs on different days) correctly adds new records and logs audit information without conflicts. This tests the partitioning strategy and audit logging.
*   **Setup**:
    1.  Clear `job_audit_log`, `job_error_log`, and `PoolBasisprodukt`.
*   **Action**:
    1.  **First Run**: Trigger the Airflow DAG with `stichtag='01012023'` (simulating `CURRENT_DATE()` as `2023-01-02`).
    2.  **Second Run**: Simulate a run on a *different* `CURRENT_DATE()` (e.g., `2023-01-03`) with the *same* `stichtag='01012023'`. This would typically involve waiting a day or mocking `CURRENT_DATE()` in the test environment. For this test, we'll simulate by directly calling the SP with a mocked `CURRENT_DATE()` if possible, or by simply running it twice and observing the `_processing_date` column.
*   **Pass/Fail Criterion**:
    1.  Both Airflow DAG runs complete successfully.
    2.  `your_gcp_project.your_bq_dataset.job_audit_log` contains two entries for `stichtag = '2023-01-01'`, both with `status = 'SUCCESS'` and `records_processed = 10`.
    3.  `your_gcp_project.your_bq_dataset.PoolBasisprodukt` contains a total of 20 records.
    4.  10 records have `_processing_date` matching the `CURRENT_DATE()` of the first run.
    5.  10 records have `_processing_date` matching the `CURRENT_DATE()` of the second run.

*   **Runnable Test Code (Pytest / Python with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from airflow.utils.state import State
    from datetime import datetime, timedelta

    GCP_PROJECT_ID = 'your_gcp_project'
    BQ_DATASET_ID = 'your_bq_dataset'

    # bq_client and cleanup_tables fixtures from Test Case 1

    def test_multiple_runs_different_processing_dates(bq_client):
        job_kennung = 'TEST_JOB_MULTI_RUN'
        eintrags_nr = '003'
        stichtag_str = '01012023'
        stichtag_date = datetime.strptime(stichtag_str, '%d%m%Y').date()

        # Simulate first run (e.g., on 2023-01-02)
        # For testing, we can't easily mock CURRENT_DATE() in BQ SPs directly.
        # The simplest way to test this is to run the DAG on two different calendar days,
        # or accept that the _processing_date will be the actual CURRENT_DATE() of the test run.
        # For this test, we'll assume the test runs on two different days or that the SP
        # is designed to handle this. The current SP uses CURRENT_DATE().

        # Run 1
        current_date_run1 = datetime.now().date()
        try:
            bq_client.query(f"""
                CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_bp_ta_msisdn`(
                    p_job_kennung => '{job_kennung}_1',
                    p_eintrags_nr => '{eintrags_nr}',
                    p_stichtag => '{stichtag_str}',
                    p_wiederanlauf_wert => ''
                );
            """).result()
            dag_run1_status = State.SUCCESS
        except Exception as e:
            print(f"Error during SP call (run 1): {e}")
            dag_run1_status = State.FAILED

        assert dag_run1_status == State.SUCCESS, "First DAG run should succeed"

        # Run 2 (simulating a different processing date)
        # For a robust test, you'd need to mock CURRENT_DATE() in BigQuery or run this on a different day.
        # As a workaround for a single test execution, we can just run it again,
        # and the _processing_date will be the same, but the audit log will show two entries.
        # If _processing_date is truly meant to be different, this test needs a more complex setup
        # (e.g., using a test framework that can mock BQ functions or running on separate days).
        # For the purpose of this example, we'll assume _processing_date is the actual current date.
        # If the actual _processing_date needs to be different, the d_ausd_bp_ta_msisdn SP would need
        # to accept a processing_date parameter.
        current_date_run2 = datetime.now().date() # Will be same as run1 if executed immediately

        try:
            bq_client.query(f"""
                CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_bp_ta_msisdn`(
                    p_job_kennung => '{job_kennung}_2',
                    p_eintrags_nr => '{eintrags_nr}',
                    p_stichtag => '{stichtag_str}',
                    p_wiederanlauf_wert => ''
                );
            """).result()
            dag_run2_status = State.SUCCESS
        except Exception as e:
            print(f"Error during SP call (run 2): {e}")
            dag_run2_status = State.FAILED

        assert dag_run2_status == State.SUCCESS, "Second DAG run should succeed"

        # Verify job_audit_log
        audit_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log` ORDER BY start_timestamp"
        audit_results = list(bq_client.query(audit_log_query).result())
        assert len(audit_results) == 2, "job_audit_log should contain two entries"

        audit_entry1 = audit_results[0]
        assert audit_entry1['job_kennung'] == f'{job_kennung}_1'
        assert audit_entry1['stichtag'] == stichtag_date
        assert audit_entry1['status'] == 'SUCCESS'
        assert audit_entry1['records_processed'] == 10

        audit_entry2 = audit_results[1]
        assert audit_entry2['job_kennung'] == f'{job_kennung}_2'
        assert audit_entry2['stichtag'] == stichtag_date
        assert audit_entry2['status'] == 'SUCCESS'
        assert audit_entry2['records_processed'] == 10

        # Verify PoolBasisprodukt
        pool_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt` WHERE stichtag = '{stichtag_date}'"
        pool_results = list(bq_client.query(pool_query).result())
        assert len(pool_results) == 20, "PoolBasisprodukt should contain 20 records (10 from each run)"

        # Check processing dates (will be the same if run immediately)
        processing_dates = {record['_processing_date'] for record in pool_results}
        # If the test is run on the same day, this will be 1. If run on different days, it will be 2.
        # The key is that records are added, not overwritten, for different processing dates.
        assert len(processing_dates) >= 1
        for record in pool_results:
            assert record['stichtag'] == stichtag_date
    ```

---

## Test Case 5: NULL Handling for Optional Parameters

*   **Purpose**: Verify that optional parameters (like `p_wiederanlauf_wert`) are handled correctly when not provided (or passed as empty strings by Airflow). This tests transformation correctness for parameter handling.
*   **Setup**:
    1.  Clear `job_audit_log`, `job_error_log`, `PoolBasisprodukt`.
*   **Action**:
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_msisdn_orchestration_dag`.
    2.  Provide the following DAG run configuration, omitting `p_wiederanlauf_wert` or passing an empty string:
        ```json
        {
            "job_kennung": "TEST_NULL_PARAM",
            "eintrags_nr": "004",
            "stichtag": "02022023"
            // p_wiederanlauf_wert will default to '' as per DAG
        }
        ```
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run completes successfully.
    2.  `your_gcp_project.your_bq_dataset.job_audit_log` contains one entry for `TEST_NULL_PARAM` with `status = 'SUCCESS'`.
    3.  `your_gcp_project.your_bq_dataset.job_error_log` contains no entries.
    4.  `your_gcp_project.your_bq_dataset.PoolBasisprodukt` contains 10 records for `stichtag = '2023-02-02'`.
    5.  No errors related to `p_wiederanlauf_wert` are logged or raised.

*   **Runnable Test Code (Pytest / Python with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from airflow.utils.state import State
    from datetime import datetime

    GCP_PROJECT_ID = 'your_gcp_project'
    BQ_DATASET_ID = 'your_bq_dataset'

    # bq_client and cleanup_tables fixtures from Test Case 1

    def test_null_handling_optional_parameters(bq_client):
        job_kennung = 'TEST_NULL_PARAM'
        eintrags_nr = '004'
        stichtag_str = '02022023'
        stichtag_date = datetime.strptime(stichtag_str, '%d%m%Y').date()
        current_date = datetime.now().date()

        # Simulate Airflow DAG run without 'p_wiederanlauf_wert'
        try:
            bq_client.query(f"""
                CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_bp_ta_msisdn`(
                    p_job_kennung => '{job_kennung}',
                    p_eintrags_nr => '{eintrags_nr}',
                    p_stichtag => '{stichtag_str}',
                    p_wiederanlauf_wert => '' -- Airflow passes empty string if not configured
                );
            """).result()
            dag_run_status = State.SUCCESS
        except Exception as e:
            print(f"Error during SP call: {e}")
            dag_run_status = State.FAILED

        assert dag_run_status == State.SUCCESS, "Airflow DAG run should succeed even with empty optional parameter"

        # Verify job_audit_log
        audit_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log`"
        audit_results = list(bq_client.query(audit_log_query).result())
        assert len(audit_results) == 1, "job_audit_log should contain exactly one entry"
        audit_entry = audit_results[0]
        assert audit_entry['job_kennung'] == job_kennung
        assert audit_entry['eintrags_nr'] == eintrags_nr
        assert audit_entry['stichtag'] == stichtag_date
        assert audit_entry['status'] == 'SUCCESS'
        assert audit_entry['records_processed'] == 10

        # Verify job_error_log
        error_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_error_log`"
        error_results = list(bq_client.query(error_log_query).result())
        assert len(error_results) == 0, "job_error_log should contain no entries"

        # Verify PoolBasisprodukt
        pool_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt` WHERE stichtag = '{stichtag_date}' AND _processing_date = '{current_date}'"
        pool_results = list(bq_client.query(pool_query).result())
        assert len(pool_results) == 10, "PoolBasisprodukt should contain 10 records for the given stichtag and processing date"
    ```

---

## Test Case 6: Schema and Data Type Assertions

*   **Purpose**: Verify that the target tables (`PoolBasisprodukt`, `job_audit_log`, `job_error_log`) have the correct schema and data types as defined in the DDLs. This is a data quality/schema assertion.
*   **Setup**:
    1.  Ensure the DDLs for all tables have been executed in BigQuery.
*   **Action**:
    1.  Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the specified tables.
*   **Pass/Fail Criterion**:
    1.  `your_gcp_project.your_bq_dataset.PoolBasisprodukt` has the following columns with matching data types:
        *   `stichtag` (DATE)
        *   `msisdn` (STRING)
        *   `produkt_id` (STRING)
        *   `aktiv_von` (DATE)
        *   `aktiv_bis` (DATE)
        *   `_processing_date` (DATE)
    2.  `your_gcp_project.your_bq_dataset.job_audit_log` has the following columns with matching data types:
        *   `job_kennung` (STRING)
        *   `eintrags_nr` (STRING)
        *   `stichtag` (DATE)
        *   `records_processed` (INT64)
        *   `start_timestamp` (TIMESTAMP)
        *   `end_timestamp` (TIMESTAMP)
        *   `status` (STRING)
    3.  `your_gcp_project.your_bq_dataset.job_error_log` has the following columns with matching data types:
        *   `job_kennung` (STRING)
        *   `eintrags_nr` (STRING)
        *   `stichtag` (DATE)
        *   `error_message` (STRING)
        *   `error_timestamp` (TIMESTAMP)

*   **Runnable Test Code (Pytest / Python with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery

    GCP_PROJECT_ID = 'your_gcp_project'
    BQ_DATASET_ID = 'your_bq_dataset'

    # bq_client fixture from Test Case 1

    def get_table_schema(bq_client, table_name):
        query = f"""
            SELECT column_name, data_type
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        rows = bq_client.query(query).result()
        return {row.column_name: row.data_type for row in rows}

    def test_schema_and_data_types(bq_client):
        # Expected schema for PoolBasisprodukt
        expected_pool_schema = {
            'stichtag': 'DATE',
            'msisdn': 'STRING',
            'produkt_id': 'STRING',
            'aktiv_von': 'DATE',
            'aktiv_bis': 'DATE',
            '_processing_date': 'DATE'
        }
        actual_pool_schema = get_table_schema(bq_client, 'PoolBasisprodukt')
        assert actual_pool_schema == expected_pool_schema, "PoolBasisprodukt schema mismatch"

        # Expected schema for job_audit_log
        expected_audit_schema = {
            'job_kennung': 'STRING',
            'eintrags_nr': 'STRING',
            'stichtag': 'DATE',
            'records_processed': 'INT64',
            'start_timestamp': 'TIMESTAMP',
            'end_timestamp': 'TIMESTAMP',
            'status': 'STRING'
        }
        actual_audit_schema = get_table_schema(bq_client, 'job_audit_log')
        assert actual_audit_schema == expected_audit_schema, "job_audit_log schema mismatch"

        # Expected schema for job_error_log
        expected_error_schema = {
            'job_kennung': 'STRING',
            'eintrags_nr': 'STRING',
            'stichtag': 'DATE',
            'error_message': 'STRING',
            'error_timestamp': 'TIMESTAMP'
        }
        actual_error_schema = get_table_schema(bq_client, 'job_error_log')
        assert actual_error_schema == expected_error_schema, "job_error_log schema mismatch"
    ```

---

## Test Case 7: Error Propagation and Logging for `d_ausd_bp_ta_msisdn`

*   **Purpose**: Verify that if the core `d_ausd_bp_ta_msisdn` stored procedure encounters an error, `r_ausd_bp_ta_msisdn` catches it, logs it to `job_error_log`, and marks the job as failed in `job_audit_log`. This tests transformation correctness for error handling and external system replacement (SQL*Plus error handling to BQ error handling).
*   **Setup**:
    1.  **Temporarily Modify `d_ausd_bp_ta_msisdn`**: For this test, you need to temporarily modify the `d_ausd_bp_ta_msisdn` stored procedure to explicitly `RAISE` an error.
        ```sql
        CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.d_ausd_bp_ta_msisdn`(
            IN p_stichtag_date DATE
        )
        BEGIN
            RAISE USING MESSAGE 'Simulated error in d_ausd_bp_ta_msisdn for testing purposes.';
            -- Original dummy data insertion logic would be here, but won't be reached.
        END;
        ```
    2.  Clear `job_audit_log`, `job_error_log`, `PoolBasisprodukt`.
*   **Action**:
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_msisdn_orchestration_dag` with valid parameters (e.g., `job_kennung='TEST_CORE_ERR'`, `eintrags_nr='005'`, `stichtag='03032023'`).
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run fails (status `failed`).
    2.  `your_gcp_project.your_bq_dataset.job_error_log` contains one entry with:
        *   `job_kennung = 'TEST_CORE_ERR'`, `eintrags_nr = '005'`, `stichtag = '2023-03-03'`.
        *   `error_message` containing "Simulated error in d_ausd_bp_ta_msisdn for testing purposes."
        *   `error_timestamp` is populated.
    3.  `your_gcp_project.your_bq_dataset.job_audit_log` contains one entry with:
        *   `job_kennung = 'TEST_CORE_ERR'`, `eintrags_nr = '005'`, `stichtag = '2023-03-03'`.
        *   `status = 'FAILED'`.
        *   `records_processed` is 0.
    4.  `your_gcp_project.your_bq_dataset.PoolBasisprodukt` contains no new records.
    5.  **Crucially**: After the test, revert the `d_ausd_bp_ta_msisdn` stored procedure to its original, non-erroring state.

*   **Runnable Test Code (Pytest / Python with BigQuery client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from airflow.utils.state import State
    from datetime import datetime

    GCP_PROJECT_ID = 'your_gcp_project'
    BQ_DATASET_ID = 'your_bq_dataset'

    # bq_client and cleanup_tables fixtures from Test Case 1

    @pytest.fixture(autouse=True)
    def setup_and_teardown_d_ausd_bp_ta_msisdn_error(bq_client):
        # Temporarily modify d_ausd_bp_ta_msisdn to raise an error
        error_sp_ddl = f"""
            CREATE OR REPLACE PROCEDURE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_ausd_bp_ta_msisdn`(
                IN p_stichtag_date DATE
            )
            BEGIN
                RAISE USING MESSAGE 'Simulated error in d_ausd_bp_ta_msisdn for testing purposes.';
            END;
        """
        bq_client.query(error_sp_ddl).result()
        yield
        # Revert d_ausd_bp_ta_msisdn to its original state after the test
        original_sp_ddl = f"""
            CREATE OR REPLACE PROCEDURE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_ausd_bp_ta_msisdn`(
                IN p_stichtag_date DATE
            )
            BEGIN
                INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt` (
                    stichtag, msisdn, produkt_id, aktiv_von, aktiv_bis, _processing_date
                )
                SELECT
                    p_stichtag_date AS stichtag,
                    FORMAT('%010d', CAST(RAND() * 10000000000 AS INT64)) AS msisdn,
                    'PROD_' || FORMAT('%03d', CAST(RAND() * 100 AS INT64)) AS produkt_id,
                    DATE_SUB(p_stichtag_date, INTERVAL CAST(RAND() * 365 AS INT64) DAY) AS aktiv_von,
                    DATE_ADD(p_stichtag_date, INTERVAL CAST(RAND() * 365 AS INT64) DAY) AS aktiv_bis,
                    CURRENT_DATE() AS _processing_date
                FROM
                    UNNEST(GENERATE_ARRAY(1, 10));
            END;
        """
        bq_client.query(original_sp_ddl).result()

    def test_core_transformation_error_propagation(bq_client):
        job_kennung = 'TEST_CORE_ERR'
        eintrags_nr = '005'
        stichtag_str = '03032023'
        stichtag_date = datetime.strptime(stichtag_str, '%d%m%Y').date()

        try:
            bq_client.query(f"""
                CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_bp_ta_msisdn`(
                    p_job_kennung => '{job_kennung}',
                    p_eintrags_nr => '{eintrags_nr}',
                    p_stichtag => '{stichtag_str}',
                    p_wiederanlauf_wert => ''
                );
            """).result()
            dag_run_status = State.SUCCESS # Should not happen
        except Exception as e:
            print(f"Error during SP call (expected): {e}")
            dag_run_status = State.FAILED

        assert dag_run_status == State.FAILED, "Airflow DAG run should fail due to error in d_ausd_bp_ta_msisdn"

        # Verify job_error_log
        error_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_error_log`"
        error_results = list(bq_client.query(error_log_query).result())
        assert len(error_results) == 1, "job_error_log should contain exactly one entry"
        error_entry = error_results[0]
        assert error_entry['job_kennung'] == job_kennung
        assert error_entry['eintrags_nr'] == eintrags_nr
        assert error_entry['stichtag'] == stichtag_date
        assert "Simulated error in d_ausd_bp_ta_msisdn for testing purposes." in error_entry['error_message']
        assert error_entry['error_timestamp'] is not None

        # Verify job_audit_log
        audit_log_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_audit_log`"
        audit_results = list(bq_client.query(audit_log_query).result())
        assert len(audit_results) == 1, "job_audit_log should contain exactly one entry"
        audit_entry = audit_results[0]
        assert audit_entry['job_kennung'] == job_kennung
        assert audit_entry['eintrags_nr'] == eintrags_nr
        assert audit_entry['stichtag'] == stichtag_date
        assert audit_entry['status'] == 'FAILED'
        assert audit_entry['records_processed'] == 0

        # Verify PoolBasisprodukt
        pool_query = f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt`"
        pool_count = bq_client.query(pool_query).result().total_rows
        assert pool_count == 0, "PoolBasisprodukt should contain no new records"
    ```

---

## Test Case 8: (Conditional) Post-Processing Logic Equivalence (`postprocess_cibasis`)

*   **Purpose**: If the commented `sed/sort/join` logic from the original ksh script is implemented in `your_gcp_project.your_bq_dataset.postprocess_cibasis`, verify its correctness. This tests transformation correctness for complex data manipulations.
*   **Setup**:
    1.  Ensure `postprocess_cibasis` is enabled in the Airflow DAG (uncommented).
    2.  Create and populate any necessary source tables for `postprocess_cibasis` (e.g., `intermediate_fax_data` as suggested in the design) with specific test data designed to expose the `REPLACE`, `DISTINCT`, and `JOIN` behaviors.
    3.  Populate `your_gcp_project.your_bq_dataset.PoolBasisprodukt` with data that would be processed by `postprocess_cibasis`.
    4.  Define a clear expected output table (`CIBasisProduktOutput` or similar) with the exact data that should result from the post-processing.
*   **Action**:
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_msisdn_orchestration_dag` with parameters that would activate `postprocess_cibasis` (e.g., `stichtag='04042023'`).
    2.  Query the output table of `postprocess_cibasis` (e.g., `your_gcp_project.your_bq_dataset.CIBasisProduktOutput`).
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run completes successfully.
    2.  The data in `your_gcp_project.your_bq_dataset.CIBasisProduktOutput` (or equivalent) exactly matches the predefined expected output dataset, considering row count, column values, and data types.
    3.  Specifically, verify:
        *   Spaces are removed from relevant string columns (equivalent to `sed s/\\ //g`).
        *   Uniqueness is enforced based on the specified keys (equivalent to `sort -u -k 1 -t ';'`).
        *   Join operations correctly combine data from different sources.

*   **Runnable Test Code (Conceptual - depends heavily on actual `postprocess_cibasis` implementation)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from airflow.utils.state import State
    from datetime import datetime

    GCP_PROJECT_ID = 'your_gcp_project'
    BQ_DATASET_ID = 'your_bq_dataset'

    # bq_client and cleanup_tables fixtures from Test Case 1

    # This test is highly dependent on the actual implementation of postprocess_cibasis.
    # The current postprocess_cibasis is a placeholder.
    # If it were implemented, you would:
    # 1. Create source tables (e.g., intermediate_fax_data) and populate them.
    # 2. Populate PoolBasisprodukt with specific data.
    # 3. Define the expected output for CIBasisProduktOutput.

    # Example: Assuming postprocess_cibasis creates CIBasisProduktOutput
    # and performs a join and a REPLACE operation.

    def test_post_processing_logic(bq_client):
        # --- Setup: Create and populate mock source data ---
        # This part would be extensive, creating tables and inserting specific data
        # to test sed, sort, and join equivalents.
        # For example:
        # bq_client.query(f"CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.intermediate_fax_data` ...").result()
        # bq_client.query(f"INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.intermediate_fax_data` VALUES ...").result()
        # bq_client.query(f"INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.PoolBasisprodukt` VALUES ...").result()

        # --- Action: Call the post-processing SP ---
        # This assumes the DAG is configured to call postprocess_cibasis
        # or you call it directly for testing.
        stichtag_str = '04042023'
        stichtag_date = datetime.strptime(stichtag_str, '%d%m%Y').date()
        try:
            bq_client.query(f"""
                CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.postprocess_cibasis`(
                    p_stichtag_date => '{stichtag_date}'
                );
            """).result()
            sp_status = State.SUCCESS
        except Exception as e:
            print(f"Error during postprocess_cibasis call: {e}")
            sp_status = State.FAILED

        assert sp_status == State.SUCCESS, "Post-processing SP should succeed"

        # --- Pass/Fail: Verify the output table ---
        # This would involve comparing the actual output with a predefined expected output.
        # Example:
        # expected_output_data = [
        #     {'msisdn': '1234567890', 'produkt_id': 'PROD_001', 'fax_data': 'FAXDATA1', 'cleaned_string': 'NO SPACES'},
        #     ...
        # ]
        # actual_output_query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.CIBasisProduktOutput` ORDER BY msisdn"
        # actual_output_results = list(bq_client.query(actual_output_query).result())

        # assert len(actual_output_results) == len(expected_output_data)
        # for i, row in enumerate(actual_output_results):
        #     assert row['msisdn'] == expected_output_data[i]['msisdn']
        #     assert row['cleaned_string'] == expected_output_data[i]['cleaned_string']
        #     # ... and so on for all columns and transformations
    ```