As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_v_ta_cntrct_templ.ksh` to Google Cloud Platform. These tests aim to ensure behavioral equivalence, data integrity, and functional correctness across the new BigQuery and Airflow architecture.

The tests are structured to cover output parity, transformation logic, external system replacements, and data quality assertions, as specified in the requirements.

---

## Migration Validation Tests: `r_ausd_v_ta_cntrct_templ.ksh`

### Test Case 1: End-to-End Data Parity (Happy Path)

*   **Purpose**: Verify that the migrated Airflow DAG, when executed with a representative set of source data, produces an identical `sof_ta_cntrct_templ` table in BigQuery as the legacy KornShell job produces in Oracle. This is the primary test for output parity and overall transformation correctness.
*   **Setup**:
    1.  **Source Data Preparation (Oracle)**:
        *   Create a comprehensive set of test data for `isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, and `cds$ta_care_description` in the Oracle legacy environment. This data should include:
            *   Records in `dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` to establish a `v_datum` (e.g., `2023-01-15`).
            *   Records in `cds_ta_cntrct_template` and `cds_ta_care_description` that satisfy all filter conditions.
            *   Records that are filtered out by date conditions (`insert_at`, `modified_at`, `valid_from`, `valid_to`).
            *   Records filtered out by `is_production = 0` or `language != 1`.
            *   Records with `NULL` values in `modified_at` and `valid_to`.
            *   Records where `cds_description_id` does not have a match in the other table (to verify inner join behavior).
        *   Ensure the Oracle target table `SOF$TA_CNTRCT_TEMPL` is empty before running the legacy job.
    2.  **Data Ingestion to BigQuery**: Ingest the *exact same* source data from Oracle into the corresponding BigQuery tables: `project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_cntrct_template`, `project.dataset.cds_ta_care_description`.
    3.  **BigQuery Target Table**: Ensure `project.dataset.sof_ta_cntrct_templ` exists with the correct schema and is empty before running the migrated job.
    4.  **Airflow Configuration**: Ensure the Airflow DAG (`r_ausd_v_ta_cntrct_templ_dag`) is deployed and configured with correct BigQuery project/dataset IDs and connection.
*   **Action**:
    1.  Execute the legacy KornShell job: `r_ausd_v_ta_cntrct_templ.ksh`.
    2.  Execute the migrated Airflow DAG: `r_ausd_v_ta_cntrct_templ_dag`.
*   **Pass/Fail Criterion**:
    *   The row count of `SOF$TA_CNTRCT_TEMPL` in Oracle must be identical to the row count of `project.dataset.sof_ta_cntrct_templ` in BigQuery.
    *   A full data comparison (e.g., using a checksum or row-by-row comparison) between the Oracle `SOF$TA_CNTRCT_TEMPL` table and the BigQuery `project.dataset.sof_ta_cntrct_templ` table shows no differences.

```python
# Example pytest assertion for data parity
import pandas as pd
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for Oracle connection

def test_end_to_end_data_parity():
    # --- Setup: Fetch data from Oracle ---
    # Replace with your actual Oracle connection string and schema
    oracle_conn_str = "user/password@host:port/service_name"
    oracle_conn = cx_Oracle.connect(oracle_conn_str)
    oracle_query = """
        SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION
        FROM SOF$TA_CNTRCT_TEMPL
        ORDER BY CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID
    """
    oracle_df = pd.read_sql(oracle_query, oracle_conn)
    oracle_conn.close()

    # --- Setup: Fetch data from BigQuery ---
    # Replace with your actual GCP project and dataset IDs
    bq_client = bigquery.Client(project="your-gcp-project-id")
    bq_query = """
        SELECT cntrct_template_id, cds_description_id, cds_description
        FROM `project.dataset.sof_ta_cntrct_templ`
        ORDER BY cntrct_template_id, cds_description_id
    """
    bq_df = bq_client.query(bq_query).to_dataframe()

    # --- Pass/Fail Criterion ---
    # 1. Row Count Check
    assert len(oracle_df) == len(bq_df), \
        f"Row count mismatch: Oracle={len(oracle_df)}, BigQuery={len(bq_df)}"

    # 2. Data Content Check (using pandas for easy comparison)
    # Ensure column names and types are consistent for comparison
    bq_df.columns = [col.upper() for col in bq_df.columns] # Make column names uppercase for comparison
    
    # Adjust data types if necessary for exact comparison (e.g., Oracle NUMBER to BQ INT64)
    # Example: bq_df['CNTRCT_TEMPLATE_ID'] = bq_df['CNTRCT_TEMPLATE_ID'].astype(int)

    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=True, check_exact=True,
                                  obj=f"Data mismatch between Oracle and BigQuery for {len(oracle_df)} rows.")
```

### Test Case 2: `v_datum` Calculation Logic

*   **Purpose**: Verify that the `v_datum` (processing date) is calculated identically in BigQuery as in Oracle, including the default value. This is critical for the filtering logic.
*   **Setup**:
    1.  **Scenario A (Records exist)**: Populate `isbert_schema.dwtk_meldungen` in Oracle with multiple records where `job_kennung = 'BERT_DROP_TEMP_TABLE'`, ensuring `timecreated` values are present (e.g., `SYSDATE - 5`, `SYSDATE - 2`, `SYSDATE`). Ingest this data to `project.dataset.dwtk_meldungen` in BigQuery.
    2.  **Scenario B (No records)**: Populate `isbert_schema.dwtk_meldungen` in Oracle with *no* records where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. Ingest this data to `project.dataset.dwtk_meldungen` in BigQuery.
*   **Action**:
    1.  **Legacy**: Manually execute the `v_datum` calculation part of `d_ausd_v_ta_cntrct_templ.sql` in Oracle SQL*Plus (or extract `MAX(m.timecreated)` directly).
    2.  **Migrated**: Execute the `extract_v_datum_task` in the Airflow DAG.
*   **Pass/Fail Criterion**:
    *   **Scenario A**: The `v_datum` value derived from Oracle (e.g., `YYYYMMDD` format) must match the `v_datum` value (e.g., `YYYY-MM-DD` format) pushed to XCom by the Airflow task, after appropriate format conversion.
    *   **Scenario B**: The `v_datum` value from Oracle (default `19000101`) must match the `v_datum` value from Airflow (default `1900-01-01`).

```python
# Example pytest assertion for v_datum calculation
from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import State
from datetime import datetime

# Helper to retrieve XCom value from a completed task
def get_xcom_value(dag_id, task_id, key='return_value'):
    with provide_session() as session:
        # Get the latest successful DAG run
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == State.SUCCESS,
        ).order_by(DagRun.execution_date.desc()).first()
        if dag_run:
            ti = dag_run.get_task_instance(task_id, session=session)
            if ti:
                return ti.xcom_pull(task_ids=task_id, key=key)
    return None

def test_v_datum_calculation_scenario_a(oracle_conn):
    # Setup: Populate Oracle dwtk_meldungen with specific timecreated
    # (This would be done via a test fixture or direct SQL before this test runs)
    # Example:
    # oracle_cursor = oracle_conn.cursor()
    # oracle_cursor.execute("DELETE FROM isbert_schema.dwtk_meldungen WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'")
    # oracle_cursor.execute("INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', TO_DATE('2023-01-10', 'YYYY-MM-DD'))")
    # oracle_cursor.execute("INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', TO_DATE('2023-01-15', 'YYYY-MM-DD'))")
    # oracle_conn.commit()
    # Ingest this data to BigQuery.

    # Action: Get expected v_datum from Oracle
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute("SELECT TO_CHAR(MAX(m.timecreated), 'YYYY-MM-DD') FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'")
    expected_v_datum_oracle = oracle_cursor.fetchone()[0]
    oracle_cursor.close()

    # Action: Trigger Airflow DAG and get v_datum from XCom
    # (Assume DAG is triggered and completes successfully for this scenario)
    actual_v_datum_bq = get_xcom_value(dag_id="r_ausd_v_ta_cntrct_templ_dag", task_id="extract_v_datum")

    # Pass/Fail Criterion
    assert actual_v_datum_bq == expected_v_datum_oracle, \
        f"v_datum mismatch (Scenario A): Oracle='{expected_v_datum_oracle}', BigQuery='{actual_v_datum_bq}'"

def test_v_datum_calculation_scenario_b(oracle_conn):
    # Setup: Ensure Oracle dwtk_meldungen has NO 'BERT_DROP_TEMP_TABLE' records
    # (This would be done via a test fixture or direct SQL before this test runs)
    # Example:
    # oracle_cursor = oracle_conn.cursor()
    # oracle_cursor.execute("DELETE FROM isbert_schema.dwtk_meldungen WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'")
    # oracle_conn.commit()
    # Ingest this data to BigQuery.

    # Action: Get expected v_datum from Oracle (should be default)
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute("SELECT COALESCE(TO_CHAR(MAX(m.timecreated), 'YYYY-MM-DD'), '1900-01-01') FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'")
    expected_v_datum_oracle = oracle_cursor.fetchone()[0]
    oracle_cursor.close()

    # Action: Trigger Airflow DAG and get v_datum from XCom
    # (Assume DAG is triggered and completes successfully for this scenario)
    actual_v_datum_bq = get_xcom_value(dag_id="r_ausd_v_ta_cntrct_templ_dag", task_id="extract_v_datum")

    # Pass/Fail Criterion
    assert actual_v_datum_bq == expected_v_datum_oracle, \
        f"v_datum mismatch (Scenario B): Oracle='{expected_v_datum_oracle}', BigQuery='{actual_v_datum_bq}'"
```

### Test Case 3: Filtering Logic - Date Conditions (Transformation Correctness)

*   **Purpose**: Verify that the date-based filtering (`insert_at`, `modified_at`, `valid_from`, `valid_to`) works correctly, including `NULL` handling for `modified_at` and `valid_to`.
*   **Setup**:
    1.  **Source Data**: Populate `cds$ta_cntrct_template` and `cds$ta_care_description` in Oracle (and mirrored in BigQuery) with specific records designed to test each date filter condition:
        *   `insert_at` exactly equal to `v_datum`.
        *   `insert_at` less than `v_datum`.
        *   `insert_at` greater than `v_datum` (should be excluded).
        *   `modified_at` is `NULL`.
        *   `modified_at` greater than `v_datum`.
        *   `modified_at` less than or equal to `v_datum` (should be excluded).
        *   `valid_from` exactly equal to `v_datum`.
        *   `valid_from` less than `v_datum`.
        *   `valid_from` greater than `v_datum` (should be excluded).
        *   `valid_to` is `NULL`.
        *   `valid_to` greater than `v_datum`.
        *   `valid_to` less than or equal to `v_datum` (should be excluded).
    2.  **`v_datum`**: Ensure `dwtk_meldungen` is set up such that `v_datum` is a specific, known date (e.g., `2023-01-15`) for consistent testing.
*   **Action**:
    1.  Execute the legacy KornShell job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion**:
    *   The set of `(CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID)` from Oracle's `SOF$TA_CNTRCT_TEMPL` must exactly match the set from BigQuery's `project.dataset.sof_ta_cntrct_templ`.
    *   Specifically, records designed to be included by the date filters must be present, and records designed to be excluded must be absent.

```sql
-- Example SQL assertions for specific record presence/absence (run against BigQuery after DAG execution)
-- Replace project.dataset with actual values.

-- Check for a record expected to be INCLUDED (e.g., insert_at < v_datum, modified_at IS NULL, valid_from < v_datum, valid_to IS NULL)
SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_templ`
WHERE cntrct_template_id = 'TEMPLATE_ID_INCLUDED_1' AND cds_description_id = 'DESC_ID_INCLUDED_1';
-- Expected result: 1

-- Check for a record expected to be EXCLUDED (e.g., insert_at > v_datum)
SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_templ`
WHERE cntrct_template_id = 'TEMPLATE_ID_EXCLUDED_1' AND cds_description_id = 'DESC_ID_EXCLUDED_1';
-- Expected result: 0

-- Check for a record expected to be EXCLUDED (e.g., modified_at <= v_datum)
SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_templ`
WHERE cntrct_template_id = 'TEMPLATE_ID_EXCLUDED_2' AND cds_description_id = 'DESC_ID_EXCLUDED_2';
-- Expected result: 0
```

### Test Case 4: Filtering Logic - `is_production` and `language` (Transformation Correctness)

*   **Purpose**: Verify that the `ct.is_production = 1` and `cd.language = 1` filters are applied correctly.
*   **Setup**:
    1.  **Source Data**: Populate `cds$ta_cntrct_template` and `cds$ta_care_description` in Oracle (and mirrored in BigQuery) with records where:
        *   `is_production = 1` (should be included).
        *   `is_production = 0` (should be excluded).
        *   `language = 1` (should be included).
        *   `language = 2` (or any other value, should be excluded).
    2.  Ensure other date conditions are met for these records so that only `is_production` and `language` filters are the deciding factors.
*   **Action**:
    1.  Execute the legacy KornShell job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion**:
    *   Records with `is_production = 1` and `language = 1` must be present in the target tables.
    *   Records with `is_production = 0` or `language != 1` must be absent from the target tables.
    *   The set of `(CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID)` from Oracle's `SOF$TA_CNTRCT_TEMPL` must exactly match the set from BigQuery's `project.dataset.sof_ta_cntrct_templ`.

### Test Case 5: Join Correctness and Handling of No-Match (Transformation Correctness)

*   **Purpose**: Verify that the `JOIN` condition `ct.cds_description_id = cd.cds_description_id` works as an `INNER JOIN` (as implied by the SQL) and correctly excludes records where no match is found.
*   **Setup**:
    1.  **Source Data**: Populate `cds$ta_cntrct_template` and `cds$ta_care_description` in Oracle (and mirrored in BigQuery) with:
        *   Records where `cds_description_id` exists in both tables (should be included if other filters pass).
        *   Records in `cds$ta_cntrct_template` where `cds_description_id` does *not* exist in `cds$ta_care_description` (should be excluded).
        *   Records in `cds$ta_care_description` where `cds_description_id` does *not* exist in `cds$ta_cntrct_template` (these won't affect the output of an inner join, but good to note).
    2.  Ensure all other filter conditions are met for the records intended to be included/excluded by the join.
*   **Action**:
    1.  Execute the legacy KornShell job.
    2.  Execute the migrated Airflow DAG.
*   **Pass/Fail Criterion**:
    *   Records with a matching `cds_description_id` in both source tables (and passing other filters) must be present.
    *   Records from `cds$ta_cntrct_template` that do not have a corresponding `cds_description_id` in `cds$ta_care_description` must be absent from the target tables.
    *   The set of `(CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID)` from Oracle's `SOF$TA_CNTRCT_TEMPL` must exactly match the set from BigQuery's `project.dataset.sof_ta_cntrct_templ`.

### Test Case 6: Schema and Data Type Parity (Data Quality / Schema Assertions)

*   **Purpose**: Verify that the target BigQuery table `project.dataset.sof_ta_cntrct_templ` has the correct schema (column names, data types, nullability) matching the Oracle `SOF$TA_CNTRCT_TEMPL` table. This ensures data integrity and compatibility.
*   **Setup**:
    1.  Ensure the Oracle `SOF$TA_CNTRCT_TEMPL` table exists with its defined schema.
    2.  Ensure the BigQuery `project.dataset.sof_ta_cntrct_templ` table is created (e.g., by the build plan step 2) with the intended schema.
*   **Action**:
    1.  Query the schema of `SOF$TA_CNTRCT_TEMPL` in Oracle.
    2.  Query the schema of `project.dataset.sof_ta_cntrct_templ` in BigQuery.
*   **Pass/Fail Criterion**:
    *   Column names must match (case-insensitivity might be a factor, but ideally, they should be consistent).
    *   Data types must be functionally equivalent (e.g., Oracle `NUMBER` to BigQuery `INT64` or `NUMERIC`, Oracle `VARCHAR2` to BigQuery `STRING`, Oracle `DATE` to BigQuery `DATE` or `TIMESTAMP`).
    *   Nullability constraints should be consistent.

```python
# Example pytest assertion for schema parity
from google.cloud import bigquery
import cx_Oracle

def test_schema_parity():
    # --- Setup: Get Oracle schema ---
    oracle_conn_str = "user/password@host:port/service_name"
    oracle_conn = cx_Oracle.connect(oracle_conn_str)
    oracle_cursor = oracle_conn.cursor()
    # Adjust OWNER and TABLE_NAME as per your Oracle environment
    oracle_cursor.execute("SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM ALL_TAB_COLUMNS WHERE OWNER = 'ISBERT_SCHEMA' AND TABLE_NAME = 'SOF$TA_CNTRCT_TEMPL' ORDER BY COLUMN_ID")
    oracle_schema = {row[0].upper(): {'data_type': row[1], 'nullable': row[2]} for row in oracle_cursor.fetchall()}
    oracle_cursor.close()
    oracle_conn.close()

    # --- Setup: Get BigQuery schema ---
    bq_client = bigquery.Client(project="your-gcp-project-id") # Replace with your GCP project ID
    table_ref = bq_client.dataset("dataset").table("sof_ta_cntrct_templ") # Replace with your dataset and table name
    bq_table = bq_client.get_table(table_ref)
    bq_schema = {}
    for field in bq_table.schema:
        bq_schema[field.name.upper()] = {
            'data_type': field.field_type,
            'nullable': 'Y' if field.mode == 'NULLABLE' else 'N'
        }

    # --- Pass/Fail Criterion ---
    assert len(oracle_schema) == len(bq_schema), \
        f"Column count mismatch: Oracle={len(oracle_schema)}, BigQuery={len(bq_schema)}"

    for col_name, oracle_col_info in oracle_schema.items():
        assert col_name in bq_schema, f"Column '{col_name}' missing in BigQuery schema."
        bq_col_info = bq_schema[col_name]

        # Define expected type mappings based on your migration strategy
        # This mapping should be comprehensive for all relevant Oracle types
        type_mapping = {
            'NUMBER': ['INT64', 'NUMERIC', 'BIGNUMERIC'], # Oracle NUMBER can map to various BQ numeric types
            'VARCHAR2': ['STRING'],
            'DATE': ['DATE', 'TIMESTAMP'], # Oracle DATE can map to BQ DATE or TIMESTAMP
            # Add other mappings as needed, e.g., 'CHAR': ['STRING'], 'CLOB': ['STRING']
        }
        
        oracle_type = oracle_col_info['data_type'].upper()
        bq_type = bq_col_info['data_type'].upper()

        assert any(bq_type in allowed_types for allowed_types in type_mapping.get(oracle_type, [])), \
            f"Data type mismatch for column '{col_name}': Oracle='{oracle_type}', BigQuery='{bq_type}'"
        
        # Nullability check (Oracle 'Y' for nullable, BQ 'NULLABLE' for nullable)
        assert oracle_col_info['nullable'] == bq_col_info['nullable'], \
            f"Nullability mismatch for column '{col_name}': Oracle='{oracle_col_info['nullable']}', BigQuery='{bq_col_info['nullable']}'"
```

### Test Case 7: Idempotency and Truncate Behavior (Data Quality / Row Count)

*   **Purpose**: Verify that running the migrated job multiple times produces the same result, ensuring the `TRUNCATE` operation works as expected and prevents duplicate data.
*   **Setup**:
    1.  **Source Data**: Populate source tables in BigQuery with a representative dataset.
    2.  **BigQuery Target Table**: Ensure `project.dataset.sof_ta_cntrct_templ` is empty initially.
*   **Action**:
    1.  Execute the Airflow DAG (`r_ausd_v_ta_cntrct_templ_dag`).
    2.  After successful completion, execute the Airflow DAG *again*.
*   **Pass/Fail Criterion**:
    *   The row count of `project.dataset.sof_ta_cntrct_templ` after the first run must be identical to the row count after the second run.
    *   The data content of `project.dataset.sof_ta_cntrct_templ` after the first run must be identical to the data content after the second run. This confirms no duplicates were inserted and the truncate worked.

```python
# Example pytest assertion for idempotency
import pandas as pd
from google.cloud import bigquery
# Assuming you have a utility to trigger Airflow DAGs programmatically for testing
# from airflow_test_utils import trigger_dag_and_wait

def test_idempotency(bq_client):
    # Assume source data is already set up in BQ for the test run
    target_table_id = "`project.dataset.sof_ta_cntrct_templ`" # Replace with actual table ID

    # Action: Trigger DAG for the first run
    # trigger_dag_and_wait("r_ausd_v_ta_cntrct_templ_dag") # Placeholder for Airflow DAG trigger

    # Fetch data after first run
    query_results = bq_client.query(f"SELECT cntrct_template_id, cds_description_id, cds_description FROM {target_table_id} ORDER BY cntrct_template_id, cds_description_id")
    df_first_run = query_results.to_dataframe()

    # Action: Trigger DAG again for the second run
    # trigger_dag_and_wait("r_ausd_v_ta_cntrct_templ_dag") # Placeholder for Airflow DAG trigger

    # Fetch data after second run
    query_results = bq_client.query(f"SELECT cntrct_template_id, cds_description_id, cds_description FROM {target_table_id} ORDER BY cntrct_template_id, cds_description_id")
    df_second_run = query_results.to_dataframe()

    # Pass/Fail Criterion
    assert len(df_first_run) == len(df_second_run), \
        f"Row count mismatch after second run: First run={len(df_first_run)}, Second run={len(df_second_run)}"
    
    pd.testing.assert_frame_equal(df_first_run, df_second_run, check_dtype=True, check_exact=True,
                                  obj=f"Data mismatch after second run, indicating non-idempotent behavior.")
```

### Test Case 8: Error Handling and Logging (External-System Replacements)

*   **Purpose**: Verify that the Airflow DAG handles errors gracefully and logs them appropriately, replacing the legacy `f_alis_msgerr.ksh` and `DWMSG_` functions. This ensures operational robustness.
*   **Setup**:
    1.  **Scenario A (SQL Error)**: Configure the BigQuery source tables or the DAG's SQL to intentionally cause an error (e.g., temporarily rename a source table, introduce an invalid column name in the `INSERT` statement, or cause a data type mismatch that leads to an error during insertion).
    2.  **Scenario B (Missing Dependency/Permission)**: Simulate a missing BigQuery connection, incorrect service account permissions, or a non-existent dataset.
*   **Action**:
    1.  Execute the Airflow DAG for Scenario A.
    2.  Execute the Airflow DAG for Scenario B.
*   **Pass/Fail Criterion**:
    *   **Scenario A**: The Airflow task executing the SQL must fail, and the DAG run must be marked as `failed`. Relevant error messages (e.g., BigQuery job failure details, SQL syntax errors) should be clearly visible in the Airflow task logs. No data should be inserted into the target table if the `INSERT` task fails.
    *   **Scenario B**: The Airflow task attempting BigQuery interaction must fail, and the DAG run must be marked as `failed`. Error messages related to connection issues, permissions, or resource not found should be present in the logs.

```python
# This test case is more about observing Airflow behavior and logs,
# and typically involves programmatic triggering and status/log retrieval via Airflow API.

# Example conceptual check (not directly runnable pytest, but outlines the logic)
# Assuming an 'airflow_client' fixture or utility that can interact with Airflow API.

def test_airflow_error_handling_sql_error(airflow_client, bq_client):
    # Setup: Intentionally break the SQL in the 'insert_transformed_data' task
    # This might involve deploying a modified DAG or using Airflow variables to inject bad SQL.
    # For example, temporarily rename `cds_ta_cntrct_template` in BQ to cause a "table not found" error.
    # bq_client.rename_table("project.dataset.cds_ta_cntrct_template", "project.dataset.temp_renamed_table")

    # Action: Trigger DAG run
    dag_run_id = airflow_client.trigger_dag("r_ausd_v_ta_cntrct_templ_dag", conf={"test_scenario": "sql_error"})
    airflow_client.wait_for_dag_run_completion(dag_run_id, timeout=300) # Wait for DAG to complete/fail

    # Pass/Fail Criterion: Check DAG run status
    dag_run_status = airflow_client.get_dag_run_status(dag_run_id)
    assert dag_run_status == "failed", f"DAG run expected to fail, but was {dag_run_status}"

    # Optionally, check logs for specific error messages
    task_logs = airflow_client.get_task_logs(dag_run_id, "insert_transformed_data")
    assert "BigQuery job failed" in task_logs or "Not found: Table" in task_logs, \
        "Expected BigQuery error message not found in logs."
    
    # Verify target table is empty or unchanged if the insert failed
    target_table_id = "`project.dataset.sof_ta_cntrct_templ`"
    row_count_query = f"SELECT COUNT(*) FROM {target_table_id}"
    row_count = bq_client.query(row_count_query).to_dataframe().iloc[0,0]
    assert row_count == 0, "Target table should be empty after failed insert due to SQL error."

    # Teardown: Revert changes (e.g., rename table back)
    # bq_client.rename_table("project.dataset.temp_renamed_table", "project.dataset.cds_ta_cntrct_template")

def test_airflow_error_handling_permissions(airflow_client, bq_client):
    # Setup: Configure Airflow connection or service account with insufficient permissions
    # (e.g., remove BigQuery Data Editor role for the service account used by Airflow)

    # Action: Trigger DAG run
    dag_run_id = airflow_client.trigger_dag("r_ausd_v_ta_cntrct_templ_dag", conf={"test_scenario": "permissions_error"})
    airflow_client.wait_for_dag_run_completion(dag_run_id, timeout=300)

    # Pass/Fail Criterion: Check DAG run status
    dag_run_status = airflow_client.get_dag_run_status(dag_run_id)
    assert dag_run_status == "failed", f"DAG run expected to fail, but was {dag_run_status}"

    # Check logs for permission-related error messages
    task_logs = airflow_client.get_task_logs(dag_run_id, "insert_transformed_data")
    assert "Access Denied" in task_logs or "Permission denied" in task_logs, \
        "Expected permission error message not found in logs."
```