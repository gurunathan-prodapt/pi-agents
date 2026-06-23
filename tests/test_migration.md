As a senior data-migration QA engineer, I've designed a comprehensive suite of tests to validate the migration of `r_ausd_v_ta_discount_rr.ksh` to an Airflow DAG on Cloud Composer, along with its core logic to BigQuery SQL. The tests focus on ensuring behavioral equivalence, data integrity, and correct integration with GCP services.

---

## Migration Validation Tests for `r_ausd_v_ta_discount_rr.ksh`

**Overall Goal**: Prove that the Airflow DAG `r_ausd_v_ta_discount_rr_dag.py` and its associated BigQuery SQL `d_ausd_v_ta_discount_rr.sql` are behaviourally equivalent to the legacy `r_ausd_v_ta_discount_rr.ksh` script.

**Assumptions**:
*   The legacy `k_ausd_v_ta_discount_rr.ksh` script, when executed, ultimately performs a `TRUNCATE` and `INSERT` operation into a target table, and its logic is accurately represented by `d_ausd_v_ta_discount_rr.sql`.
*   We have access to a test environment for the legacy system to run `r_ausd_v_ta_discount_rr.ksh` and inspect its output (log files, database state).
*   We have a GCP test environment with Cloud Composer, BigQuery, and necessary service accounts configured.
*   Source tables (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`, `dwtk_meldungen`) exist in BigQuery and contain representative test data.
*   The `target_project.target_dataset.sof_ta_discount_rr` table exists in BigQuery.
*   `legacy_db.legacy_schema.ta_discount_rr` refers to the target table in the legacy system, accessible for comparison (e.g., via a BigQuery external table or a data dump).

---

### Test Case 1: Output Parity - Core Data Reconciliation (Happy Path)

*   **Purpose**: Verify that the migrated BigQuery SQL, when executed via the Airflow DAG with equivalent inputs, produces the exact same data in the target table as the legacy KornShell script. This is the most critical test for data migration.
*   **Setup**:
    1.  **Legacy**: Prepare a test environment. Ensure all source tables (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`) contain a representative dataset, including various date scenarios (insert_at, modified_at, valid_from, valid_to), NULLs, and edge cases.
    2.  **Legacy**: Ensure the `dwtk_meldungen` table in the legacy Oracle database (or equivalent) contains an entry for `BERT_DROP_TEMP_TABLE` with a `timecreated` value that will be used as the processing date. For example, `MAX(timecreated)` for `BERT_DROP_TEMP_TABLE` is `2023-10-26 10:00:00`.
    3.  **Legacy**: Clear the legacy target table `ta_discount_rr`.
    4.  **GCP**: Ensure the BigQuery source tables are populated with *identical* data to the legacy Oracle source tables.
    5.  **GCP**: Ensure the BigQuery `dwtk_meldungen` table contains an entry for `BERT_DROP_TEMP_TABLE` with `timecreated = '2023-10-26 10:00:00'` (or equivalent to match legacy).
    6.  **GCP**: Clear the BigQuery target table `target_project.target_dataset.sof_ta_discount_rr`.
*   **Action**:
    1.  **Legacy**: Execute `r_ausd_v_ta_discount_rr.ksh` in the legacy environment.
    2.  **GCP**: Trigger the `r_ausd_v_ta_discount_rr_dag` in Cloud Composer.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  Both jobs complete successfully without errors.
        2.  The row count in the legacy `ta_discount_rr` table is identical to the row count in `target_project.target_dataset.sof_ta_discount_rr`.
        3.  A deep data comparison (e.g., using `EXCEPT DISTINCT` in SQL) between the legacy `ta_discount_rr` table and `target_project.target_dataset.sof_ta_discount_rr` shows no differences.
    *   **Fail**: Any of the above conditions are not met.
*   **Test Code (BigQuery Assertion Example)**:
    ```sql
    -- After both jobs have run and populated their respective target tables:
    -- Assuming 'legacy_db.legacy_schema.ta_discount_rr' is a BigQuery external table
    -- or a snapshot of the legacy output for comparison.

    -- Check for rows in legacy not in new
    SELECT 'Legacy_Only' AS diff_type, * FROM `legacy_db.legacy_schema.ta_discount_rr`
    EXCEPT DISTINCT
    SELECT 'Legacy_Only' AS diff_type, * FROM `target_project.target_dataset.sof_ta_discount_rr`;

    -- Check for rows in new not in legacy
    SELECT 'New_Only' AS diff_type, * FROM `target_project.target_dataset.sof_ta_discount_rr`
    EXCEPT DISTINCT
    SELECT 'New_Only' AS diff_type, * FROM `legacy_db.legacy_schema.ta_discount_rr`;

    -- Pass if both queries return 0 rows.
    ```

### Test Case 2: Transformation Correctness - Date Filtering Logic

*   **Purpose**: Verify the complex date filtering logic (`insert_at`, `modified_at`, `valid_from`, `valid_to`) in `d_ausd_v_ta_discount_rr.sql` behaves identically to the legacy system, especially with `NULL` values and boundary conditions.
*   **Setup**:
    1.  **Legacy & GCP**: Populate source tables with specific test data covering:
        *   Rows where `insert_at` is exactly `v_datum_str`, before, and after.
        *   Rows where `modified_at` is `NULL`, before, and after `v_datum_str`.
        *   Rows where `valid_from` is exactly `v_datum_str` and before.
        *   Rows where `valid_to` is `NULL`, exactly `v_datum_str` (should be excluded), and after.
        *   Ensure a mix of these conditions to test their interactions.
    2.  **Legacy & GCP**: Set `MAX(timecreated)` for `BERT_DROP_TEMP_TABLE` in `dwtk_meldungen` to a specific date, e.g., `2023-01-15`, to serve as `v_datum_str`.
    3.  Clear both target tables.
*   **Action**:
    1.  **Legacy**: Execute `r_ausd_v_ta_discount_rr.ksh`.
    2.  **GCP**: Trigger `r_ausd_v_ta_discount_rr_dag`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The data in `target_project.target_dataset.sof_ta_discount_rr` is identical to the data in the legacy `ta_discount_rr` table, specifically verifying that rows matching the date conditions (and non-matching ones) are correctly included/excluded.
    *   **Fail**: Any discrepancy in the filtered data.
*   **Test Code (Conceptual BigQuery Assertion)**:
    ```sql
    -- Assuming 'legacy_output' and 'migrated_output' are temporary tables/views
    -- containing the results of each run for comparison.

    -- Test case: Row with insert_at = '2023-01-15', modified_at IS NULL, valid_from = '2023-01-01', valid_to IS NULL
    -- Expected: Included
    SELECT COUNT(*) FROM migrated_output WHERE cntrct_id = 'TEST_DATE_INCL_1'; -- Should be 1

    -- Test case: Row with insert_at = '2023-01-16', modified_at IS NULL, valid_from = '2023-01-01', valid_to IS NULL
    -- Expected: Excluded (insert_at > v_datum_str)
    SELECT COUNT(*) FROM migrated_output WHERE cntrct_id = 'TEST_DATE_EXCL_1'; -- Should be 0

    -- Test case: Row with insert_at = '2023-01-10', modified_at = '2023-01-10', valid_from = '2023-01-01', valid_to = '2023-01-15'
    -- Expected: Excluded (valid_to is not > v_datum_str)
    SELECT COUNT(*) FROM migrated_output WHERE cntrct_id = 'TEST_DATE_EXCL_2'; -- Should be 0

    -- Perform similar checks for all relevant date combinations against legacy output.
    ```

### Test Case 3: Transformation Correctness - Join and Static Filter Logic

*   **Purpose**: Verify all `JOIN` conditions and static `WHERE` clauses (`LANGUAGE = 1`, `is_production = 1`) are correctly implemented and produce the expected results.
*   **Setup**:
    1.  **Legacy & GCP**: Populate source tables with data to test:
        *   Rows that should join successfully across all tables.
        *   Rows that fail to join due to missing keys in one or more tables.
        *   Rows where `LANGUAGE` is not `1` in `cds_ta_care_description`.
        *   Rows where `is_production` is not `1` in `cds_ta_discount`.
        *   Rows with `CALC_RULE_VALUE` (rabatthoehe) and `CDS_DESCRIPTION` (rabatt, rabattierte_rech_pos) values to ensure correct column mapping.
    2.  Clear both target tables.
*   **Action**:
    1.  **Legacy**: Execute `r_ausd_v_ta_discount_rr.ksh`.
    2.  **GCP**: Trigger `r_ausd_v_ta_discount_rr_dag`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The data in `target_project.target_dataset.sof_ta_discount_rr` is identical to the data in the legacy `ta_discount_rr` table, specifically verifying correct inclusion/exclusion based on join conditions and static filters.
    *   **Fail**: Any discrepancy in the joined and filtered data.
*   **Test Code (Conceptual BigQuery Assertion)**:
    ```sql
    -- Assuming 'migrated_output' is a temporary table/view containing the results.

    -- Example: A row that should be excluded because d.is_production = 0
    SELECT COUNT(*) FROM migrated_output WHERE cntrct_id = 'NON_PROD_CONTRACT_ID'; -- Should be 0

    -- Example: A row that should be excluded because cd.LANGUAGE = 2
    SELECT COUNT(*) FROM migrated_output WHERE cntrct_id = 'WRONG_LANG_CONTRACT_ID'; -- Should be 0

    -- Example: A row that should be included with specific rabatt and rabatthoehe values
    SELECT rabatt, rabatthoehe, rabattierte_rech_pos
    FROM migrated_output
    WHERE cntrct_id = 'VALID_CONTRACT_ID_1';
    -- Assert these values match expected from source data based on join logic.
    ```

### Test Case 4: External System Replacement - `v_datum_str` Extraction

*   **Purpose**: Verify that the `extract_v_datum_task` and `set_v_datum_parameter_task` correctly determine the processing date (`v_datum_str`) from the BigQuery `dwtk_meldungen` table, mimicking the legacy behavior of deriving this date.
*   **Setup**:
    1.  **GCP**: Populate `source_project.source_dataset.dwtk_meldungen` with multiple entries for `BERT_DROP_TEMP_TABLE` with varying `timecreated` values. Ensure one entry has the maximum `timecreated` (e.g., `2023-10-26 10:00:00`).
    2.  **GCP**: For a separate test run, ensure `BERT_DROP_TEMP_TABLE` has no entries, or `timecreated` is NULL, to verify the `COALESCE` to `'19000101'`.
*   **Action**:
    1.  Trigger `r_ausd_v_ta_discount_rr_dag` for both scenarios (max date found, no date found).
    2.  Inspect Airflow task logs and XCom values for `set_v_datum_parameter_task`.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The `extract_v_datum_task` successfully queries `dwtk_meldungen`.
        2.  The `set_v_datum_parameter_task` correctly pulls the `MAX(timecreated)` (formatted as YYYYMMDD, e.g., `20231026`) for `BERT_DROP_TEMP_TABLE` from XCom and pushes it as `v_datum_str`.
        3.  In the "no date found" scenario, `v_datum_str` is correctly set to `'19000101'`.
        4.  The `execute_core_reconciliation_sql` task receives the correct `v_datum_str` parameter.
    *   **Fail**: Incorrect `v_datum_str` is determined or passed.
*   **Test Code (Pytest for Airflow Task)**:
    ```python
    import pytest
    from airflow.models import DagBag, TaskInstance
    from datetime import datetime
    from unittest.mock import patch, MagicMock

    # Assuming 'dags' directory is in your PYTHONPATH for pytest to find the DAG
    # and 'utils' directory for dw_utils.py

    @pytest.fixture
    def dag_bag():
        return DagBag(dag_folder='dags', include_examples=False)

    def test_extract_v_datum_task_success(dag_bag):
        dag = dag_bag.get_dag(dag_id='r_ausd_v_ta_discount_rr_dag')
        task = dag.get_task(task_id='extract_v_datum_from_bigquery')

        mock_bq_hook = MagicMock()
        # Simulate a successful query result: MAX(timecreated) as 'YYYYMMDD'
        mock_bq_hook.get_first.return_value = ['20231026']

        with patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook', return_value=mock_bq_hook):
            ti = TaskInstance(task=task, execution_date=datetime(2023, 1, 1))
            ti.run()

            # Verify XCom push from BigQueryExecuteQueryOperator
            assert ti.xcom_pull(task_ids='extract_v_datum_from_bigquery') == [['20231026']]

    def test_set_v_datum_parameter_task_success(dag_bag):
        dag = dag_bag.get_dag(dag_id='r_ausd_v_ta_discount_rr_dag')
        task = dag.get_task(task_id='set_v_datum_parameter')

        ti = TaskInstance(task=task, execution_date=datetime(2023, 1, 1))
        # Manually push XCom from the previous BigQuery task
        ti.xcom_push(key='return_value', value=[['20231026']], task_ids='extract_v_datum_from_bigquery')

        task.execute(context=ti.get_template_context())

        # Verify XCom pull and push for v_datum_str
        assert ti.xcom_pull(key='v_datum_str', task_ids='set_v_datum_parameter') == '20231026'

    def test_set_v_datum_parameter_task_default_value(dag_bag):
        dag = dag_bag.get_dag(dag_id='r_ausd_v_ta_discount_rr_dag')
        task = dag.get_task(task_id='set_v_datum_parameter')

        ti = TaskInstance(task=task, execution_date=datetime(2023, 1, 1))
        # Simulate no result from BigQuery (e.g., table empty or no matching job_kennung)
        ti.xcom_push(key='return_value', value=[], task_ids='extract_v_datum_from_bigquery')

        task.execute(context=ti.get_template_context())

        # Verify default value '19000101' is used
        assert ti.xcom_pull(key='v_datum_str', task_ids='set_v_datum_parameter') == '19000101'
    ```

### Test Case 5: Data Quality - Row Count and Schema Parity

*   **Purpose**: Verify that the migrated job consistently produces the same number of rows and adheres to the expected schema in the target table.
*   **Setup**:
    1.  **Legacy & GCP**: Populate source tables with a diverse dataset, including scenarios that should result in zero rows, some rows, and many rows in the target.
    2.  Clear both target tables.
*   **Action**:
    1.  **Legacy**: Execute `r_ausd_v_ta_discount_rr.ksh`.
    2.  **GCP**: Trigger `r_ausd_v_ta_discount_rr_dag`.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The row count in `target_project.target_dataset.sof_ta_discount_rr` matches the row count in the legacy `ta_discount_rr` table.
        2.  The schema (column names, data types, nullability) of `target_project.target_dataset.sof_ta_discount_rr` matches the expected schema and the legacy table's schema.
    *   **Fail**: Any discrepancy in row count or schema.
*   **Test Code (BigQuery Assertion Example)**:
    ```sql
    -- Row Count Check
    SELECT
        (SELECT COUNT(*) FROM `legacy_db.legacy_schema.ta_discount_rr`) AS legacy_count,
        (SELECT COUNT(*) FROM `target_project.target_dataset.sof_ta_discount_rr`) AS migrated_count,
        CASE
            WHEN (SELECT COUNT(*) FROM `legacy_db.legacy_schema.ta_discount_rr`) = (SELECT COUNT(*) FROM `target_project.target_dataset.sof_ta_discount_rr`) THEN 'PASS'
            ELSE 'FAIL'
        END AS row_count_status;

    -- Schema Check (can be automated by comparing INFORMATION_SCHEMA views)
    -- Example for a single column type check:
    SELECT
        column_name, data_type, is_nullable
    FROM
        `target_project.target_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_discount_rr'
    ORDER BY
        ordinal_position;
    -- Compare this output to the expected schema and the legacy table's schema.
    ```

### Test Case 6: Error Handling - Parameter Validation (`dw_utils.py`)

*   **Purpose**: Verify that the `dw_utils.py` functions (`pruefe_parameter_gesetzt`, `dwmsg_meldefehler`) correctly handle missing/empty parameters and raise `DWError`, causing the Airflow task to fail, mimicking the legacy script's exit behavior.
*   **Setup**:
    1.  **GCP**: Modify the `_initialize_job_parameters` function in the DAG temporarily to simulate missing parameters (e.g., set `job_kennung = None` or `job_kennung = ''`).
*   **Action**:
    1.  Trigger `r_ausd_v_ta_discount_rr_dag`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `initialize_job_parameters_task` fails, and the Airflow task log contains an error message indicating a missing/empty parameter, similar to `DWMSG_ERROR - Code: 193, Arg: 'Required parameter 'JobKennung' is missing or empty.'`. The DAG run should be marked as failed.
    *   **Fail**: The task completes successfully despite missing parameters, or the error message is incorrect.
*   **Test Code (Pytest for PythonOperator)**:
    ```python
    import pytest
    from airflow.models import DagBag, TaskInstance
    from datetime import datetime
    from unittest.mock import patch, MagicMock
    from utils.dw_utils import DWError # Assuming utils is on PYTHONPATH

    @pytest.fixture
    def dag_bag():
        return DagBag(dag_folder='dags', include_examples=False)

    def test_initialize_job_parameters_missing_job_kennung(dag_bag):
        dag = dag_bag.get_dag(dag_id='r_ausd_v_ta_discount_rr_dag')
        task = dag.get_task(task_id='initialize_job_parameters')

        ti = TaskInstance(task=task, execution_date=datetime(2023, 1, 1))

        # Temporarily simulate a missing dag_id (which becomes job_kennung)
        with patch.object(dag, 'dag_id', new=None):
            with pytest.raises(DWError, match="Required parameter 'JobKennung' is missing or empty."):
                task.execute(context=ti.get_template_context())

    def test_initialize_job_parameters_empty_eintrags_nr(dag_bag):
        dag = dag_bag.get_dag(dag_id='r_ausd_v_ta_discount_rr_dag')
        task = dag.get_task(task_id='initialize_job_parameters')

        ti = TaskInstance(task=task, execution_date=datetime(2023, 1, 1))

        # Simulate an empty run_id (though Airflow usually provides one)
        with patch.object(ti, 'run_id', new=''):
            with pytest.raises(DWError, match="Required parameter 'EintragsNr' is missing or empty."):
                task.execute(context=ti.get_template_context())
    ```

### Test Case 7: Logging and Monitoring Integration

*   **Purpose**: Verify that all logging from the Airflow DAG and its tasks (including `dw_utils.py` messages) is correctly captured by Cloud Logging and that Airflow's native monitoring (e.g., task status, retries) functions as expected.
*   **Setup**:
    1.  **GCP**: Ensure Cloud Composer environment is configured for Cloud Logging.
    2.  **GCP**: Trigger the DAG with both successful and intentionally failed tasks (e.g., by introducing a syntax error in the BigQuery SQL or a division by zero in a Python task).
*   **Action**:
    1.  Trigger `r_ausd_v_ta_discount_rr_dag` for a successful run.
    2.  Trigger `r_ausd_v_ta_discount_rr_dag` for a failed run (e.g., by temporarily making the BigQuery SQL invalid).
    3.  Navigate to Cloud Logging in the GCP console and filter logs by `resource.type="cloud_composer_environment"` and `logName="projects/<project-id>/logs/airflow-tasks"`.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  All `log.info`, `log.warning`, `log.error` messages from the DAG and `dw_utils.py` are visible in Cloud Logging, associated with the correct Airflow DAG run and task.
        2.  Successful DAG runs show all tasks as `success` in Airflow UI and corresponding `INFO` level logs in Cloud Logging.
        3.  Failed DAG runs show the failing task as `failed` in Airflow UI, and corresponding `ERROR` level messages are present in Cloud Logging, clearly indicating the cause of failure.
        4.  Airflow's retry mechanism (if configured) is observed to function correctly for transient failures, with logs reflecting retries.
    *   **Fail**: Logs are missing, incorrectly formatted, or not associated with the correct Airflow context; Airflow UI status does not reflect actual task outcomes.

---