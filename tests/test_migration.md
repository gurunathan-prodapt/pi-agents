As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_v_ta_discount.ksh` to an Airflow DAG. The critical observation is that the core data transformation logic, originally in `k_ausd_v_ta_discount.ksh`, is currently a placeholder in the migrated BigQueryOperator. This means full data parity and transformation correctness tests cannot be executed until `k_ausd_v_ta_discount.ksh` is fully analyzed and translated.

Therefore, the tests below are structured to:
1.  **Validate the wrapper's behavior:** Focus on orchestration, parameter handling, and error reporting as implemented in the Airflow DAG.
2.  **Provide conceptual tests for the core logic:** Outline how data transformation, schema, and data quality will be validated once the `k_ausd_v_ta_discount.ksh` logic is translated into BigQuery SQL. These conceptual tests are crucial for the "Phase 5: Testing & Validation" of the build plan.

---

## Migration Validation Tests for `r_ausd_v_ta_discount_migration_dag`

### Test Case 1: Successful Execution and Parameter Propagation

*   **Purpose:** To verify that the migrated Airflow DAG can execute successfully end-to-end when provided with valid parameters, and that these parameters are correctly propagated and utilized by the tasks within the DAG. This covers the basic orchestration flow and parameter handling of the legacy wrapper.
*   **Setup:**
    1.  Ensure the Airflow DAG `r_ausd_v_ta_discount_migration_dag` is deployed to a Cloud Composer environment.
    2.  Ensure the BigQuery dataset and a placeholder `ta_discount` table exist (even if empty) for the `process_ta_discount_data` task to target.
*   **Action:**
    1.  Trigger the Airflow DAG manually via the Airflow UI or `gcloud composer` command.
    2.  Provide specific, non-default values for the DAG parameters:
        *   `job_kennung`: `TEST_JOB_ID_123`
        *   `dw_eintrags_nr`: `98765`
    3.  Monitor the DAG run in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   **Pass:** The DAG run completes successfully (all tasks turn green).
    *   **Pass:** Review the logs for the `start_task` and `end_task`. The logs must clearly show the provided `job_kennung` and `dw_eintrags_nr` values.
    *   **Pass:** Review the logs for the `process_ta_discount_data` task. If the placeholder SQL is executed, its logs should also reflect the correct parameter values being passed to the BigQuery query (e.g., if the `MERGE` statement's `job_kennung_param` and `dw_eintrags_nr_param` columns are populated).

```python
# Example of how to trigger the DAG via Airflow API (conceptual for automated testing)
# This would typically be part of a Pytest suite interacting with Airflow.
import requests
import json

AIRFLOW_BASE_URL = "http://<your-airflow-webserver-url>/api/v1"
DAG_ID = "r_ausd_v_ta_discount_migration_dag"
HEADERS = {
    "Content-Type": "application/json",
    # Add authentication headers if required (e.g., Bearer token)
}

def trigger_dag_with_params(job_kennung, dw_eintrags_nr):
    payload = {
        "conf": {
            "job_kennung": job_kennung,
            "dw_eintrags_nr": dw_eintrags_nr
        }
    }
    response = requests.post(
        f"{AIRFLOW_BASE_URL}/dags/{DAG_ID}/dagRuns",
        headers=HEADERS,
        data=json.dumps(payload)
    )
    response.raise_for_status()
    print(f"DAG run triggered: {response.json()}")
    return response.json()

# In a pytest function:
def test_successful_execution_and_parameter_propagation():
    job_id = "TEST_JOB_ID_123"
    entry_nr = "98765"
    dag_run_info = trigger_dag_with_params(job_id, entry_nr)
    # Poll Airflow API for DAG run status and logs
    # Assert DAG run status is 'success'
    # Assert logs contain 'JobKennung: TEST_JOB_ID_123' and 'DW_EintragsNr: 98765'
    # (This part requires Airflow API interaction for log retrieval and parsing)
    assert True # Placeholder for actual assertions
```

### Test Case 2: Core Logic Failure Handling

*   **Purpose:** To verify that the Airflow DAG correctly handles failures within the core processing task (the BigQueryOperator), mirroring the `trap ERR` mechanism of the legacy KornShell script. The DAG should report a failure, and relevant error information should be available in logs.
*   **Setup:**
    1.  Deploy the Airflow DAG.
    2.  **Temporarily modify the `process_ta_discount_data` task's SQL** to intentionally cause an error (e.g., `SELECT 1/0;` or `SELECT * FROM non_existent_table;`).
*   **Action:**
    1.  Trigger the Airflow DAG manually.
    2.  Monitor the DAG run in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `process_ta_discount_data` task fails (turns red) in the Airflow UI.
    *   **Pass:** The overall DAG run status is marked as `failed`.
    *   **Pass:** Review the logs for the `process_ta_discount_data` task. The logs must contain detailed error messages from BigQuery indicating the cause of the failure.
    *   **Pass:** The `end_task` (which logs completion) should *not* execute, or if it does, it should reflect the upstream failure (Airflow's default behavior is to skip downstream tasks on upstream failure).

```python
# Conceptual modification to the DAG for testing failure
# This would be a temporary change for a specific test run.
# In dags/r_ausd_v_ta_discount_migration_dag.py:
# ...
    process_ta_discount_data = BigQueryOperator(
        task_id="process_ta_discount_data",
        sql="""
            -- Intentionally cause an error for testing purposes
            SELECT 1 / 0;
            -- Or: SELECT * FROM `your-gcp-project.your_bigquery_dataset.non_existent_table`;
        """,
        use_legacy_sql=False,
    )
# ...

# In a pytest function:
def test_core_logic_failure_handling():
    # Trigger DAG with the modified SQL
    dag_run_info = trigger_dag_with_params("FAIL_JOB", "111")
    # Poll Airflow API for DAG run status
    # Assert DAG run status is 'failed'
    # Assert 'process_ta_discount_data' task status is 'failed'
    # Assert logs for 'process_ta_discount_data' contain expected error message (e.g., "division by zero")
    assert True # Placeholder for actual assertions
```

### Test Case 3: Legacy Parameter Handling Equivalence (Invalid Parameters)

*   **Purpose:** To verify how the Airflow DAG handles invalid or malformed parameters, comparing it to the `getopts` error handling (`ErrNr=192` for unknown, `ErrNr=193` for missing argument) in the legacy script. While Airflow's `Param` mechanism is different, the goal is to ensure robust handling.
*   **Setup:**
    1.  Deploy the Airflow DAG.
    2.  The current DAG uses `Param` with `default` values, so missing parameters will use defaults. This test focuses on *malformed* input if Airflow allows it to pass initial validation.
*   **Action:**
    1.  Attempt to trigger the Airflow DAG with a parameter value that is syntactically valid but semantically incorrect for downstream tasks (e.g., `dw_eintrags_nr` as a non-numeric string if the core logic expects a number).
    2.  Attempt to trigger the Airflow DAG with an *unknown* parameter (e.g., `{"unknown_param": "value"}`).
*   **Pass/Fail Criterion:**
    *   **Pass (Malformed Value):** If a malformed parameter value (e.g., `dw_eintrags_nr: "abc"`) is passed, the DAG should either:
        *   Fail at the `process_ta_discount_data` task if the BigQuery SQL attempts to use it in a type-sensitive operation (e.g., `CAST('abc' AS INT)`). The task logs should clearly indicate a type conversion error.
        *   Or, if the parameter is only used as a string, the DAG should complete successfully, reflecting the flexibility of string parameters.
    *   **Pass (Unknown Parameter):** Airflow's `Param` system should ignore unknown parameters passed in the `conf` dictionary, and the DAG should execute using its defined parameters and defaults. No error should occur due to the unknown parameter itself.
    *   **Fail:** The DAG fails due to an unexpected error related to parameter parsing or type handling that is not caught by the BigQuery task.

```python
# Example of triggering with malformed parameter (conceptual)
def test_malformed_dw_eintrags_nr():
    # Assuming BigQuery SQL tries to cast dw_eintrags_nr to an integer
    dag_run_info = trigger_dag_with_params("MALFORMED_JOB", "NOT_A_NUMBER")
    # Assert DAG run status is 'failed'
    # Assert 'process_ta_discount_data' task status is 'failed'
    # Assert logs contain BigQuery type conversion error
    assert True # Placeholder for actual assertions

# Example of triggering with unknown parameter (conceptual)
def test_unknown_parameter_handling():
    payload = {
        "conf": {
            "job_kennung": "KNOWN_JOB",
            "dw_eintrags_nr": "123",
            "unknown_param": "some_value" # This should be ignored
        }
    }
    response = requests.post(
        f"{AIRFLOW_BASE_URL}/dags/{DAG_ID}/dagRuns",
        headers=HEADERS,
        data=json.dumps(payload)
    )
    response.raise_for_status()
    # Assert DAG run status is 'success'
    # Assert logs do NOT show errors related to 'unknown_param'
    assert True # Placeholder for actual assertions
```

### Test Case 4: Logging Content Parity (Wrapper Level)

*   **Purpose:** To verify that the Airflow DAG's logging captures similar high-level job information and completion messages as the legacy script's `DWMSG_` functions and `print` statements, ensuring operational visibility.
*   **Setup:**
    1.  Deploy the Airflow DAG.
    2.  Ensure Cloud Logging is configured for the Cloud Composer environment.
*   **Action:**
    1.  Trigger a successful Airflow DAG run with default or specific parameters.
    2.  Access Cloud Logging for the specific DAG run.
*   **Pass/Fail Criterion:**
    *   **Pass:** Cloud Logging for the `start_task` contains messages equivalent to:
        *   `Starting r_ausd_v_ta_discount migration job...`
        *   `JobKennung: <value of job_kennung>`
        *   `DW_EintragsNr: <value of dw_eintrags_nr>`
    *   **Pass:** Cloud Logging for the `end_task` contains messages equivalent to:
        *   `Job r_ausd_v_ta_discount completed successfully.`
        *   `Parameters used: JobKennung=<value>, DW_EintragsNr=<value>`
    *   **Pass:** The overall log structure and content provide sufficient information to understand the job's execution flow and status, comparable to the `LogDatei` generated by the legacy script.

```python
# No specific runnable code here, as this is about observing Cloud Logging.
# However, a conceptual pytest could involve:
import google.cloud.logging_v2.client

def test_logging_content_parity():
    # Trigger DAG
    dag_run_info = trigger_dag_with_params("LOG_TEST_JOB", "456")
    # Wait for DAG to complete
    # Use google.cloud.logging_v2.client to query logs for the specific DAG run
    # Assert log entries match expected patterns for start and end tasks.
    client = google.cloud.logging_v2.client.LoggingClient()
    # Example filter (needs refinement for specific DAG run ID)
    filter_str = f'resource.type="cloud_composer_environment" AND logName="projects/<project_id>/logs/airflow-tasks" AND textPayload:"LOG_TEST_JOB"'
    entries = client.list_entries(filter_=filter_str)
    found_start_log = False
    found_end_log = False
    for entry in entries:
        if "Starting r_ausd_v_ta_discount migration job..." in entry.text_payload:
            found_start_log = True
        if "Job r_ausd_v_ta_discount completed successfully." in entry.text_payload:
            found_end_log = True
    assert found_start_log
    assert found_end_log
```

### Test Case 5: Data Transformation Correctness (Conceptual - Post `k_ausd_v_ta_discount.ksh` analysis)

*   **Purpose:** To ensure that the translated BigQuery SQL logic, derived from `k_ausd_v_ta_discount.ksh`, produces exactly the same data transformations and final state in the `ta_discount` table as the legacy script. This is the core of "output parity" and "transformation correctness".
*   **Setup:**
    1.  **Crucially, `k_ausd_v_ta_discount.ksh` must be fully analyzed and its logic translated into BigQuery SQL.** This SQL will replace the placeholder in the `process_ta_discount_data` task.
    2.  Establish a controlled test environment for both legacy (e.g., Oracle) and BigQuery.
    3.  Prepare identical input data sets (source tables, configuration, etc.) for both environments. This might involve creating a snapshot of legacy source tables and loading them into BigQuery.
    4.  Ensure the `ta_discount` table in both environments is in an identical baseline state before processing.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_discount.ksh` (or the `r_ausd_v_ta_discount.ksh` wrapper) with the controlled input data.
    2.  Execute the migrated Airflow DAG (with the translated BigQuery SQL) using the *same* controlled input data.
    3.  After both runs complete, extract the final state of the `ta_discount` table from both the legacy system and BigQuery.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `ta_discount` table in BigQuery is byte-for-byte identical (or functionally equivalent, considering data type mappings like `NUMBER` to `NUMERIC`) to the `ta_discount` table in the legacy system after processing. This includes all columns, rows, and values.
    *   **Pass:** Any intermediate tables or outputs generated by the core logic are also identical between systems.

```sql
-- Example SQL for comparing tables in BigQuery (assuming legacy data is also in BQ for comparison)
-- This would be run AFTER the DAG has executed.
SELECT
  COUNT(*) AS diff_count
FROM (
  SELECT * FROM `your-gcp-project.your_bigquery_dataset.ta_discount_migrated`
  EXCEPT DISTINCT
  SELECT * FROM `your-gcp-project.your_bigquery_dataset.ta_discount_legacy_snapshot_after_run`
)
UNION ALL
SELECT
  COUNT(*) AS diff_count
FROM (
  SELECT * FROM `your-gcp-project.your_bigquery_dataset.ta_discount_legacy_snapshot_after_run`
  EXCEPT DISTINCT
  SELECT * FROM `your-gcp-project.your_bigquery_dataset.ta_discount_migrated`
);

-- Pass criterion: diff_count for both queries must be 0.
```

### Test Case 6: Schema and Data Type Parity (Conceptual)

*   **Purpose:** To verify that the `ta_discount` table in BigQuery has the same schema (column names, data types, nullability, primary keys/unique constraints if applicable) as the legacy table, ensuring data integrity and compatibility.
*   **Setup:**
    1.  Access to the schema definition of the legacy `ta_discount` table (e.g., `DESCRIBE ta_discount;` in Oracle).
    2.  The `ta_discount` table must be created in BigQuery as part of "Phase 2: BigQuery Schema & Data Migration".
*   **Action:**
    1.  Retrieve the schema of the legacy `ta_discount` table.
    2.  Retrieve the schema of the BigQuery `ta_discount` table.
    3.  Perform a programmatic or manual comparison of the two schemas.
*   **Pass/Fail Criterion:**
    *   **Pass:** All column names match.
    *   **Pass:** Data types are either identical or have appropriate BigQuery equivalents (e.g., `VARCHAR2(N)` to `STRING`, `NUMBER` to `NUMERIC` or `BIGNUMERIC`, `DATE` to `DATE` or `TIMESTAMP`).
    *   **Pass:** Nullability constraints are preserved.
    *   **Pass:** Any primary key or unique constraints (if logically enforced by the application or through BigQuery clustering/partitioning) are correctly reflected.

```python
# Example Python (pytest) for schema comparison (conceptual)
import pytest
from google.cloud import bigquery
# Assuming you have a way to get legacy schema (e.g., from a metadata store or direct DB query)
from legacy_db_connector import get_legacy_schema

def test_ta_discount_schema_parity():
    legacy_schema = get_legacy_schema("ta_discount") # Returns a list of dicts: [{'name': 'COL1', 'type': 'VARCHAR2', 'nullable': True}, ...]

    client = bigquery.Client()
    bq_table_id = "your-gcp-project.your_bigquery_dataset.ta_discount"
    bq_table = client.get_table(bq_table_id)
    bq_schema = bq_table.schema # Returns a list of bigquery.SchemaField objects

    assert len(legacy_schema) == len(bq_schema), "Number of columns mismatch"

    for legacy_col in legacy_schema:
        bq_col = next((c for c in bq_schema if c.name.upper() == legacy_col['name'].upper()), None)
        assert bq_col is not None, f"Column {legacy_col['name']} not found in BigQuery"

        # Map legacy types to expected BigQuery types
        expected_bq_type = {
            "VARCHAR2": "STRING",
            "NUMBER": "NUMERIC", # Or BIGNUMERIC, INT64 depending on precision
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            # Add more mappings as needed
        }.get(legacy_col['type'].upper(), "UNKNOWN")

        assert bq_col.field_type == expected_bq_type, \
            f"Type mismatch for column {legacy_col['name']}: Legacy {legacy_col['type']}, BQ {bq_col.field_type}"

        # BigQuery's mode 'NULLABLE' is equivalent to legacy nullable=True
        # BigQuery's mode 'REQUIRED' is equivalent to legacy nullable=False
        expected_bq_mode = "NULLABLE" if legacy_col['nullable'] else "REQUIRED"
        assert bq_col.mode == expected_bq_mode, \
            f"Nullability mismatch for column {legacy_col['name']}: Legacy {legacy_col['nullable']}, BQ {bq_col.mode}"

    print("Schema comparison successful!")
```

### Test Case 7: Row Count and Data Volume Parity (Conceptual)

*   **Purpose:** To verify that the number of rows and overall data volume in the `ta_discount` table remains consistent after migration and processing, ensuring no data loss or unexpected duplication.
*   **Setup:**
    1.  Baseline row counts and data volumes from the legacy `ta_discount` table.
    2.  The `ta_discount` table in BigQuery is populated with migrated historical data.
    3.  The Airflow DAG (with translated `k_ausd_v_ta_discount.ksh` logic) has been executed.
*   **Action:**
    1.  Query the row count of the legacy `ta_discount` table.
    2.  Query the row count of the BigQuery `ta_discount` table.
    3.  (Optional but recommended) Calculate the total data size (e.g., sum of column lengths for strings, byte size for numbers) for both tables.
*   **Pass/Fail Criterion:**
    *   **Pass:** The row count of the BigQuery `ta_discount` table matches the row count of the legacy `ta_discount` table after a full processing cycle.
    *   **Pass:** (Optional) The total data volume is within an acceptable tolerance, accounting for potential differences in data type storage sizes between systems.
    *   **Fail:** Significant discrepancies in row counts or data volume are observed without a clear, documented reason (e.g., expected filtering or aggregation).

```sql
-- Example SQL for row count comparison in BigQuery
-- This would be run AFTER the DAG has executed.

-- Query 1: Row count of the migrated table
SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.ta_discount`;

-- Query 2: Row count of the legacy snapshot (if available in BQ)
SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.ta_discount_legacy_snapshot_after_run`;

-- Pass criterion: Results of Query 1 and Query 2 must be identical.

-- Example SQL for data volume (conceptual, BigQuery specific)
SELECT
  SUM(LENGTH(CAST(col1 AS STRING))) + SUM(LENGTH(CAST(col2 AS STRING))) + ... -- for string columns
  + SUM(SAFE_CAST(col_numeric AS BIGNUMERIC)) -- for numeric columns
FROM `your-gcp-project.your_bigquery_dataset.ta_discount`;

-- This would be compared against a similar calculation for the legacy system.
```