As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_v_ta_p_discount_rr.ksh` to Google Cloud Composer (Airflow) and BigQuery. The tests aim to ensure behavioral equivalence, data integrity, and correct system integration.

The migration design indicates that the KornShell script is primarily an orchestration layer for a SQL script (`d_ausd_v_ta_p_discount_rr.sql`). The core data transformation logic resides in this SQL script, which is migrated to BigQuery SQL. The Airflow DAG replaces the KornShell script's orchestration, parameter handling, and error management.

**Key Assumptions for Testing:**
*   **Legacy Environment Access**: Access to the legacy Oracle database (or a representative data dump) and the ability to execute the original `k_ausd_v_ta_p_discount_rr.ksh` script.
*   **GCP Environment Access**: A configured GCP project with BigQuery and Cloud Composer (Airflow) instances.
*   **Source Data Synchronization**: The source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`) in the legacy Oracle environment and the migrated BigQuery environment contain identical data for testing purposes.
*   **`d_ausd_v_ta_p_discount_rr.sql` (Oracle)**: It is assumed that the Oracle SQL script performs an `INSERT INTO ta_p_discount_rr SELECT ...` and that the `starteSQLSkript` function (or the SQL itself) effectively truncates or clears `ta_p_discount_rr` before inserting new data, mirroring the `WRITE_TRUNCATE` behavior in BigQuery. If not, this would be a behavioral change requiring specific testing.
*   **Parameters in SQL**: The provided `d_ausd_v_ta_p_discount_rr.bq.sql` does not use the `p_JobKennung` or `p_EintragsNr` parameters in its `INSERT` statement. Therefore, the data transformation itself is not dependent on these parameters. They are primarily for orchestration, logging, and job management.
*   **Job Management Table**: A placeholder BigQuery table named `job_tracking_table` is assumed for testing the job management logic.

---

## Updated Airflow DAG for Testing

Before diving into the tests, here's an updated version of the `k_ausd_v_ta_p_discount_rr_dag.py` that includes more robust record count retrieval and a concrete (though simplified) implementation for job management table updates.

```python
# --- FILE: k_ausd_v_ta_p_discount_rr_dag.py (Updated for testing) ---
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator, BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.utils.timezone import datetime
import logging
from google.cloud import bigquery
import pandas as pd # For BigQueryHook fallback

# Set up logging
log = logging.getLogger(__name__)

# --- Configuration Variables ---
BIGQUERY_PROJECT_ID = 'your_bigquery_project'
BIGQUERY_DATASET_ID = 'your_bigquery_dataset'
TARGET_TABLE_ID = 'ta_p_discount_rr'
SOURCE_SQL_PATH = 'd_ausd_v_ta_p_discount_rr.bq.sql' # Path to the BigQuery SQL file
JOB_TRACKING_TABLE_ID = 'job_tracking_table' # For job management tests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,
    'start_date': days_ago(1),
}

def _validate_parameters(**kwargs):
    """
    Validates required parameters (job_kennung, eintrags_nr) from DAG run configuration.
    Corresponds to 'pruefeParameterGesetzt' and error handling in the KSH script.
    """
    ti = kwargs['ti']
    dag_run_conf = kwargs['dag_run'].conf

    job_kennung = dag_run_conf.get('job_kennung')
    eintrags_nr = dag_run_conf.get('eintrags_nr')

    if not job_kennung:
        raise AirflowException("Required parameter 'job_kennung' is missing. Please provide it in DAG run configuration.")
    if not eintrags_nr:
        raise AirflowException("Required parameter 'eintrags_nr' is missing. Please provide it in DAG run configuration.")

    log.info(f"Parameters validated: job_kennung={job_kennung}, eintrags_nr={eintrags_nr}")
    ti.xcom_push(key='job_kennung', value=job_kennung)
    ti.xcom_push(key='eintrags_nr', value=eintrags_nr)


def _log_record_count(**kwargs):
    """
    Retrieves and logs the number of records processed by the BigQuery task.
    Corresponds to 'eval "v_records=`cat $tmpFile`"' in the KSH script.
    """
    ti = kwargs['ti']
    job_id = ti.xcom_pull(task_ids='execute_data_processing', key='return_value') # BigQueryOperator pushes job_id

    num_records_processed = 0
    if job_id:
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        job = client.get_job(job_id)

        if job.job_type == 'QUERY' and job.statement_type in ['INSERT', 'UPDATE', 'DELETE', 'MERGE']:
            num_records_processed = job.num_affected_rows
            log.info(f"Number of records processed by BigQuery job {job_id}: {num_records_processed}")
        else:
            log.warning(f"BigQuery job {job_id} did not report num_affected_rows directly or was not a DML statement. Falling back to COUNT(*).")
            # Fallback: query the target table for row count
            target_table_ref = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TARGET_TABLE_ID}"
            count_query = f"SELECT COUNT(*) FROM `{target_table_ref}`"
            count_job = client.query(count_query)
            count_result = list(count_job.result())
            num_records_processed = count_result[0][0] if count_result else 0
            log.info(f"Total records in target table {target_table_ref} after processing: {num_records_processed}")
    else:
        log.warning("Could not retrieve BigQuery job ID from XCom. Cannot get record count. Falling back to COUNT(*).")
        # As a last resort, query the table directly if job_id is missing
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        target_table_ref = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TARGET_TABLE_ID}"
        count_query = f"SELECT COUNT(*) FROM `{target_table_ref}`"
        count_result_df = hook.get_pandas_df(sql=count_query, project_id=BIGQUERY_PROJECT_ID)
        num_records_processed = count_result_df.iloc[0, 0] if not count_result_df.empty else 0
        log.info(f"Total records in target table {target_table_ref} after processing (fallback): {num_records_processed}")

    ti.xcom_push(key='num_records_processed', value=num_records_processed)


def _update_job_management_tables(**kwargs):
    """
    Updates a placeholder job tracking table in BigQuery.
    Corresponds to "Eintrag in die Job-Tabelle" and "alte aktive Jobs werden einfach dekativiert".
    """
    ti = kwargs['ti']
    job_kennung = ti.xcom_pull(task_ids='validate_parameters', key='job_kennung')
    eintrags_nr = ti.xcom_pull(task_ids='validate_parameters', key='eintrags_nr')
    status = kwargs.get('status', 'UNKNOWN')
    message = kwargs.get('message', '')
    current_time = datetime.now().isoformat()

    # Simplified upsert logic for demonstration
    update_query = f"""
        MERGE INTO `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{JOB_TRACKING_TABLE_ID}` T
        USING (SELECT '{job_kennung}' AS job_id, '{eintrags_nr}' AS entry_number) S
        ON T.job_id = S.job_id AND T.entry_number = S.entry_number
        WHEN MATCHED THEN
            UPDATE SET status = '{status}', end_time = '{current_time}', message = '{message}'
        WHEN NOT MATCHED THEN
            INSERT (job_id, entry_number, status, start_time, end_time, message)
            VALUES ('{job_kennung}', '{eintrags_nr}', '{status}', '{current_time}', NULL, '{message}');
    """

    BigQueryExecuteQueryOperator(
        task_id=f'update_job_tracking_{status.lower().replace(" ", "_")}',
        sql=update_query,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    ).execute(context=kwargs)
    log.info(f"Job tracking table updated for JobKennung={job_kennung}, EintragsNr={eintrags_nr} with status={status}")


with DAG(
    dag_id='k_ausd_v_ta_p_discount_rr_dag',
    default_args=default_args,
    description='Airflow DAG for k_ausd_v_ta_p_discount_rr.ksh migration',
    schedule_interval=None,
    tags=['bigquery', 'data_ingestion', 'isbert'],
    catchup=False,
) as dag:
    start_task = PythonOperator(
        task_id='start_processing',
        python_callable=lambda: log.info("Starting data processing for ta_p_discount_rr."),
    )

    validate_parameters = PythonOperator(
        task_id='validate_parameters',
        python_callable=_validate_parameters,
        provide_context=True,
    )

    update_job_status_start = PythonOperator(
        task_id='update_job_status_start',
        python_callable=_update_job_management_tables,
        op_kwargs={'status': 'RUNNING'},
        provide_context=True,
    )

    execute_data_processing = BigQueryOperator(
        task_id='execute_data_processing',
        sql=SOURCE_SQL_PATH,
        destination_project_dataset_table=f'{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TARGET_TABLE_ID}',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
        params={
            'p_job_kennung': "{{ task_instance.xcom_pull(task_ids='validate_parameters', key='job_kennung') }}",
            'p_eintrags_nr': "{{ task_instance.xcom_pull(task_ids='validate_parameters', key='eintrags_nr') }}",
        },
    )

    log_record_count = PythonOperator(
        task_id='log_record_count',
        python_callable=_log_record_count,
        provide_context=True,
    )

    update_job_status_end = PythonOperator(
        task_id='update_job_status_end',
        python_callable=_update_job_management_tables,
        op_kwargs={'status': 'COMPLETED', 'message': 'Successfully processed data'},
        provide_context=True,
    )

    end_task = PythonOperator(
        task_id='end_processing',
        python_callable=lambda: log.info("Finished data processing for ta_p_discount_rr."),
    )

    start_task >> validate_parameters >> update_job_status_start >> execute_data_processing >> log_record_count >> update_job_status_end >> end_task

```

---

## Migration Validation Tests

### 1. Output Parity & Transformation Correctness

These tests focus on ensuring the data produced by the migrated job is identical to the legacy job, covering the core transformation logic.

#### Test Case 1.1: Full Data Parity (Golden Record Comparison)

*   **Purpose**: To verify that the entire dataset generated by the migrated Airflow DAG in BigQuery is byte-for-byte identical to the dataset generated by the legacy KornShell script in Oracle, given the same input data. This is the most comprehensive test for behavioral equivalence.
*   **Setup**:
    1.  **Legacy**: Populate the Oracle source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`) with a diverse set of test data, including standard cases, edge cases (e.g., missing join keys, NULLs in non-key columns), and a reasonable volume of data.
    2.  **Migrated**: Replicate the exact same test data into the corresponding BigQuery source tables (`your_bigquery_project.your_bigquery_dataset.sof_ta_discount_rr`, etc.).
    3.  **Legacy Run**: Execute the legacy `k_ausd_v_ta_p_discount_rr.ksh` script with a specific `p_JobKennung` and `p_EintragsNr`.
        ```bash
        # Example legacy execution
        ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh -j "TEST_JOB_1" -f "ENTRY_1"
        ```
    4.  **Extract Legacy Output**: Export the entire `ta_p_discount_rr` table from Oracle into a canonical format (e.g., CSV, JSON, or a sorted flat file) for comparison. Ensure consistent sorting and handling of NULLs.
    5.  **Migrated Run**: Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG with the same `job_kennung` and `eintrags_nr` as the legacy run.
        ```python
        # Example Airflow DAG trigger (using Airflow CLI)
        # Replace 'your_airflow_env' with your Airflow environment name
        # Ensure BIGQUERY_PROJECT_ID and BIGQUERY_DATASET_ID are correctly set in the DAG file
        airflow dags trigger k_ausd_v_ta_p_discount_rr_dag --conf '{"job_kennung": "TEST_JOB_1", "eintrags_nr": "ENTRY_1"}'
        ```
*   **Action**:
    1.  After both jobs complete, extract the `ta_p_discount_rr` table from BigQuery into the same canonical format as the legacy output.
    2.  Perform a deep comparison of the two extracted datasets.
*   **Pass/Fail Criterion**: The extracted BigQuery `ta_p_discount_rr` dataset must be identical to the extracted Oracle `ta_p_discount_rr` dataset. This includes row count, column values, and data types (where applicable, considering BigQuery's type system).

    ```python
    # Example Python/Pytest assertion for data parity (conceptual)
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming Oracle connection

    def fetch_oracle_data(query):
        # Connect to Oracle and fetch data
        conn = cx_Oracle.connect("user/password@host:port/service_name")
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def fetch_bigquery_data(query, project_id):
        client = bigquery.Client(project=project_id)
        df = client.query(query).to_dataframe()
        return df

    def test_data_parity_ta_p_discount_rr():
        # Configuration
        oracle_table = "TA_P_DISCOUNT_RR"
        bq_table = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TARGET_TABLE_ID}"

        # Fetch data from legacy Oracle
        oracle_df = fetch_oracle_data(f"SELECT * FROM {oracle_table} ORDER BY cntrct_id, discount_id")

        # Fetch data from migrated BigQuery
        bq_df = fetch_bigquery_data(f"SELECT * FROM `{bq_table}` ORDER BY cntrct_id, discount_id", BIGQUERY_PROJECT_ID)

        # Ensure column names and order are consistent for comparison
        # (May require renaming/reordering columns if they differ slightly)
        bq_df.columns = [col.upper() for col in bq_df.columns] # Oracle often uses uppercase

        # Compare dataframes
        pd.testing.assert_frame_equal(
            oracle_df.reset_index(drop=True),
            bq_df.reset_index(drop=True),
            check_dtype=True,
            check_exact=False, # Use False for float comparisons, adjust tolerance if needed
            rtol=1e-5, atol=1e-8 # Relative and absolute tolerance for numeric comparisons
        )
    ```

#### Test Case 1.2: Row Count Parity

*   **Purpose**: To specifically verify that the number of records inserted into `ta_p_discount_rr` by the migrated job is identical to the legacy job. This also validates the `_log_record_count` task.
*   **Setup**: Same as Test Case 1.1.
*   **Action**:
    1.  Retrieve the record count from the legacy Oracle `ta_p_discount_rr` table.
    2.  Retrieve the record count from the BigQuery `ta_p_discount_rr` table.
    3.  Check the logs/XCom of the Airflow DAG for the `num_records_processed` value from the `log_record_count` task.
*   **Pass/Fail Criterion**:
    1.  `COUNT(*)` from BigQuery `ta_p_discount_rr` must equal `COUNT(*)` from Oracle `ta_p_discount_rr`.
    2.  The `num_records_processed` value logged by the Airflow DAG must match this count.

    ```sql
    -- SQL assertion for row count parity
    SELECT COUNT(*) FROM your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr;
    -- Compare this with the count from the legacy Oracle table.
    ```

#### Test Case 1.3: Join Logic Correctness (Inner Join)

*   **Purpose**: To specifically test the correctness of the inner join conditions (`da.cntrct_id = c.cntrct_id`, `da.cntrct_obj_version = c.obj_version`, `da.cntrct_template_id = ct.cntrct_template_id`) when records are intentionally missing in one of the joined tables.
*   **Setup**:
    1.  **Test Data**: Create specific test data where:
        *   Some records in `sof_ta_discount_rr` have no matching `cntrct_id` in `sof_ta_cntrct_crs`.
        *   Some records in `sof_ta_discount_rr` have no matching `cntrct_obj_version` in `sof_ta_cntrct_crs`.
        *   Some records in `sof_ta_discount_rr` have no matching `cntrct_template_id` in `sof_ta_cntrct_templ`.
        *   Some records have matches in all tables.
    2.  **Legacy/Migrated**: Populate source tables in both environments with this test data.
    3.  **Execute**: Run both legacy and migrated jobs.
*   **Action**: Query the `ta_p_discount_rr` table in both Oracle and BigQuery.
*   **Pass/Fail Criterion**: Only records that satisfy *all* join conditions should be present in the target `ta_p_discount_rr` table in both environments. The number and content of these records must match exactly.

#### Test Case 1.4: `std_vertrag` Derivation

*   **Purpose**: To verify that the `std_vertrag` column is correctly populated from `ct.cds_description` based on the `cntrct_template_id` join.
*   **Setup**:
    1.  **Test Data**: Create test data in `sof_ta_discount_rr` and `sof_ta_cntrct_templ` with various `cntrct_template_id` values and corresponding `cds_description` values, including cases where `cds_description` might be NULL or empty.
    2.  **Legacy/Migrated**: Populate source tables in both environments with this test data.
    3.  **Execute**: Run both legacy and migrated jobs.
*   **Action**: Query the `ta_p_discount_rr` table in both Oracle and BigQuery, specifically checking the `std_vertrag` column.
*   **Pass/Fail Criterion**: The `std_vertrag` values in BigQuery `ta_p_discount_rr` must exactly match those in Oracle `ta_p_discount_rr` for all corresponding records.

#### Test Case 1.5: NULL Handling in Source Data

*   **Purpose**: To verify how NULL values in non-join source columns are handled and propagated to the target table.
*   **Setup**:
    1.  **Test Data**: Create test data in `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, and `sof_ta_cntrct_templ` where various non-key columns (e.g., `rabatt`, `rabatthoehe`, `contract_number`) contain NULL values.
    2.  **Legacy/Migrated**: Populate source tables in both environments with this test data.
    3.  **Execute**: Run both legacy and migrated jobs.
*   **Action**: Query the `ta_p_discount_rr` table in both Oracle and BigQuery, inspecting columns that had NULLs in the source.
*   **Pass/Fail Criterion**: NULL values in the target BigQuery `ta_p_discount_rr` table must appear in the same records and columns as in the legacy Oracle `ta_p_discount_rr` table.

### 2. External System Replacements

These tests verify that the migration correctly replaces legacy external dependencies with their GCP counterparts.

#### Test Case 2.1: BigQuery as Database Replacement

*   **Purpose**: To confirm that the migrated data processing exclusively uses BigQuery for all data operations and does not attempt to connect to or read from the legacy Oracle database.
*   **Setup**:
    1.  Ensure the Airflow DAG's BigQuery connection (`gcp_conn_id='google_cloud_default'`) is correctly configured.
    2.  (Optional but recommended) Temporarily disable network access from the Airflow environment to the legacy Oracle database.
*   **Action**: Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG.
*   **Pass/Fail Criterion**: The Airflow DAG must complete successfully, and BigQuery `ta_p_discount_rr` must be populated. There should be no errors related to Oracle database connections or `sqlplus` execution in the Airflow task logs.

#### Test Case 2.2: Temporary File Replacement

*   **Purpose**: To verify that the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_rr_$$.tmp`) used by the legacy script for record counts is no longer created or used, and its functionality is replaced by Airflow's XCom and BigQuery job statistics.
*   **Setup**:
    1.  **Legacy**: Run the legacy `k_ausd_v_ta_p_discount_rr.ksh` script and observe the creation of the temporary file.
    2.  **Migrated**: Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG.
*   **Action**:
    1.  After the legacy script runs, check the `$DW_DIR_UTL` directory for the temporary file.
    2.  After the Airflow DAG runs, inspect the Airflow worker's filesystem and the task logs for `log_record_count`.
*   **Pass/Fail Criterion**:
    1.  The temporary file `$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_rr_$$.tmp` (or similar) must *not* be created on the Airflow worker's filesystem during the DAG run.
    2.  The `log_record_count` task in Airflow must successfully retrieve and log the number of processed records, indicating it used BigQuery job statistics or a direct BigQuery query.

### 3. Data Quality / Row Count / Schema Assertions

These tests ensure the integrity and structure of the data in the target BigQuery table.

#### Test Case 3.1: Target Table Schema Validation

*   **Purpose**: To verify that the schema of the `ta_p_discount_rr` table in BigQuery matches the expected schema defined in `ta_p_discount_rr_schema.sql` and is consistent with the legacy Oracle table's structure (column names, data types, nullability).
*   **Setup**:
    1.  Ensure `ta_p_discount_rr_schema.sql` has been executed to create the BigQuery table.
    2.  Run the migrated Airflow DAG to populate the table.
*   **Action**:
    1.  Query BigQuery's `INFORMATION_SCHEMA` to retrieve the schema of `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr`.
    2.  Compare this schema against the expected schema (e.g., from `ta_p_discount_rr_schema.sql` and the legacy Oracle table's DDL).
*   **Pass/Fail Criterion**: The BigQuery table schema (column names, data types, and nullability) must match the defined schema and be compatible with the legacy Oracle table's structure.

    ```python
    # Example Python/Pytest assertion for schema validation
    from google.cloud import bigquery

    def test_bigquery_schema_ta_p_discount_rr():
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        table_ref = client.dataset(BIGQUERY_DATASET_ID).table(TARGET_TABLE_ID)
        table = client.get_table(table_ref)

        expected_schema = {
            "CNTRCT_ID": "STRING",
            "DISCOUNT_ID": "STRING",
            "DISC_VECTOR_TY": "STRING",
            "CNTRCT_OBJ_VERSION": "STRING",
            "CNTRCT_TEMPLATE_ID": "STRING",
            "DISC_INVOICE_ITEM_ID": "STRING",
            "RABATT": "NUMERIC", # Or FLOAT64, depending on actual data
            "RABATTHOEHE": "NUMERIC",
            "RABATTIERTE_RECH_POS": "NUMERIC",
            "CONTRACT_NUMBER": "STRING",
            "STD_VERTRAG": "STRING"
        }

        actual_schema = {field.name.upper(): field.field_type for field in table.schema}

        assert len(actual_schema) == len(expected_schema), "Number of columns mismatch"
        for col, expected_type in expected_schema.items():
            assert col in actual_schema, f"Missing column: {col}"
            assert actual_schema[col] == expected_type, f"Data type mismatch for {col}: Expected {expected_type}, Got {actual_schema[col]}"

        # Additional checks for nullability if needed
        # For example:
        # actual_nullability = {field.name.upper(): field.mode for field in table.schema}
        # assert actual_nullability["CNTRCT_ID"] == "REQUIRED" # or "NULLABLE"
    ```

#### Test Case 3.2: Idempotency of Execution

*   **Purpose**: To verify that running the Airflow DAG multiple times with the same inputs produces the same result in the target table, due to the `WRITE_TRUNCATE` disposition.
*   **Setup**:
    1.  Populate BigQuery source tables with a consistent set of test data.
    2.  Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG once.
*   **Action**:
    1.  After the first successful run, query and store the `ta_p_discount_rr` table content (e.g., into a temporary table or a file).
    2.  Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG a second time with the exact same parameters.
    3.  After the second successful run, query the `ta_p_discount_rr` table again.
*   **Pass/Fail Criterion**: The content of `ta_p_discount_rr` after the second run must be identical to its content after the first run (same rows, same values).

    ```sql
    -- SQL assertion for idempotency (after two runs)
    -- Step 1: After first run, save current state
    CREATE OR REPLACE TABLE `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr_snapshot` AS
    SELECT * FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr`;

    -- Step 2: Run DAG again.

    -- Step 3: Compare
    SELECT
        (SELECT COUNT(*) FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr`) =
        (SELECT COUNT(*) FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr_snapshot`)
        AND
        (SELECT COUNT(*) FROM (
            SELECT * FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr`
            EXCEPT DISTINCT
            SELECT * FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr_snapshot`
        )) = 0
        AND
        (SELECT COUNT(*) FROM (
            SELECT * FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr_snapshot`
            EXCEPT DISTINCT
            SELECT * FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr`
        )) = 0 AS is_idempotent;
    -- Pass if is_idempotent is TRUE.
    ```

### 4. Orchestration & Parameter Handling

These tests verify the Airflow DAG's control flow and parameter management.

#### Test Case 4.1: Successful DAG Execution with Valid Parameters

*   **Purpose**: To verify that the Airflow DAG runs successfully when provided with all required parameters, mimicking a successful legacy run. This also implicitly tests the `start_task` and `end_task`.
*   **Setup**: Ensure BigQuery source tables are populated.
*   **Action**: Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG with valid `job_kennung` and `eintrags_nr` parameters.
    ```bash
    airflow dags trigger k_ausd_v_ta_p_discount_rr_dag --conf '{"job_kennung": "VALID_JOB_ID", "eintrags_nr": "VALID_ENTRY_NR"}'
    ```
*   **Pass/Fail Criterion**: The Airflow DAG completes with a "success" status. The `ta_p_discount_rr` table in BigQuery is populated. The `job_tracking_table` shows entries for `RUNNING` and `COMPLETED` statuses for the given `job_kennung` and `eintrags_nr`.

    ```python
    # Example Pytest for DAG success (conceptual, requires Airflow testing framework)
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import State
    from airflow.utils.types import DagRunType

    def test_dag_successful_execution(dag_bag):
        dag = dag_bag.get_dag(dag_id='k_ausd_v_ta_p_discount_rr_dag')
        assert dag is not None

        # Simulate a DAG run
        dr = dag.create_dagrun(
            state=State.RUNNING,
            execution_date=datetime.now(),
            run_type=DagRunType.MANUAL,
            conf={"job_kennung": "TEST_SUCCESS", "eintrags_nr": "12345"}
        )

        # This part would involve actually running the tasks, typically in an Airflow test harness
        # For a real integration test, you'd trigger via CLI/API and poll for status.
        # Assuming tasks are run and state is updated:
        # dr.refresh_from_db()
        # assert dr.state == State.SUCCESS

        # Verify XComs (e.g., from log_record_count)
        # ti = dr.get_task_instance(task_id='log_record_count')
        # assert ti.xcom_pull(key='num_records_processed') > 0

        # Verify job tracking table (requires direct DB query or mock)
        # client = bigquery.Client()
        # query = f"SELECT status FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{JOB_TRACKING_TABLE_ID}` WHERE job_id = 'TEST_SUCCESS' AND entry_number = '12345' ORDER BY start_time DESC LIMIT 1"
        # result = list(client.query(query).result())
        # assert result[0][0] == 'COMPLETED'
    ```

#### Test Case 4.2: DAG Fails with Missing `job_kennung` Parameter

*   **Purpose**: To verify that the DAG correctly identifies and fails when the required `job_kennung` parameter is missing, matching the legacy script's error handling (`pruefeParameterGesetzt`).
*   **Setup**: None.
*   **Action**: Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG without providing the `job_kennung` in the configuration.
    ```bash
    airflow dags trigger k_ausd_v_ta_p_discount_rr_dag --conf '{"eintrags_nr": "VALID_ENTRY_NR"}'
    ```
*   **Pass/Fail Criterion**: The Airflow DAG fails at the `validate_parameters` task with an `AirflowException` (or similar error) indicating that `job_kennung` is missing. The `job_tracking_table` should reflect a `RUNNING` status (if `update_job_status_start` ran) and then a `FAILED` status (if error handling updates it).

#### Test Case 4.3: DAG Fails with Missing `eintrags_nr` Parameter

*   **Purpose**: To verify that the DAG correctly identifies and fails when the required `eintrags_nr` parameter is missing.
*   **Setup**: None.
*   **Action**: Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG without providing the `eintrags_nr` in the configuration.
    ```bash
    airflow dags trigger k_ausd_v_ta_p_discount_rr_dag --conf '{"job_kennung": "VALID_JOB_ID"}'
    ```
*   **Pass/Fail Criterion**: The Airflow DAG fails at the `validate_parameters` task with an `AirflowException` (or similar error) indicating that `eintrags_nr` is missing. The `job_tracking_table` should reflect a `RUNNING` status (if `update_job_status_start` ran) and then a `FAILED` status.

#### Test Case 4.4: DAG Ignores Extra Parameters

*   **Purpose**: To verify that the Airflow DAG handles extra, unexpected parameters gracefully (i.e., ignores them without error), similar to `getopts` behavior in the KornShell script.
*   **Setup**: None.
*   **Action**: Trigger the `k_ausd_v_ta_p_discount_rr_dag` Airflow DAG with valid required parameters and one or more additional, unrecognized parameters.
    ```bash
    airflow dags trigger k_ausd_v_ta_p_discount_rr_dag --conf '{"job_kennung": "VALID_JOB_ID", "eintrags_nr": "VALID_ENTRY_NR", "extra_param": "some_value"}'
    ```
*   **Pass/Fail Criterion**: The Airflow DAG completes successfully. The extra parameters should not cause any errors or unexpected behavior. The `ta_p_discount_rr` table should be populated correctly, and the `job_tracking_table` should show a `COMPLETED` status.

---

These tests provide a robust framework for validating the migration of `k_ausd_v_ta_p_discount_rr.ksh`. They cover the critical aspects of output parity, transformation logic, external system replacement, and data quality, ensuring the migrated solution is functionally equivalent and reliable.