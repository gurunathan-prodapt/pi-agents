As a senior data-migration QA engineer, I've developed a comprehensive suite of validation tests for the `DW.DWH_APT_EXPORT_MONATLICH_JP` migration. These tests are designed to ensure the migrated GCP solution is behaviourally equivalent to the legacy UC4 job, covering output parity, transformation correctness, external system interactions, and data quality.

**Assumptions for Testing:**

1.  **Legacy System Access:** We have access to the legacy UC4 environment, its output files (CSV), and the Oracle database for comparison and baseline data extraction.
2.  **Reverse-Engineered Logic:** The critical `r_exis_v2` executable logic, including the SQL queries and transformations from `h_exis_apt_nna_daten.var` and `h_exis_apt_nna_voice.var`, has been fully reverse-engineered and accurately implemented in the PySpark applications (`nna_data_exporter.py`, `nna_voice_exporter.py`). The placeholder SQL in the provided PySpark code has been replaced with the actual, complex queries.
3.  **Test Environment:** A dedicated GCP test environment is available, including Cloud Composer (Airflow), Dataproc, Cloud Storage, and a test Oracle database (either a replica of production or a representative dataset).
4.  **Prerequisite DAGs:** For `dw_dwh_run_apt_export_monatlich_jp_evt` prerequisite checks, we assume `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` are either migrated to Airflow (allowing `ExternalTaskSensor` or `DagRun` queries) or their status can be reliably mocked/queried from an external system. For these tests, we will mock their success/failure.
5.  **`MONAT_ID` Interpretation:** The `MONAT_ID` parameter (passed as `ds_nodash` from Airflow) is assumed to be a `YYYYMMDD` string that directly corresponds to a filtering column in the Oracle source tables (e.g., `WHERE DATA_MONTH_ID = '{monat_id}'`). This aligns with the generated PySpark code.

---

## Migration Validation Tests for DW.DWH_APT_EXPORT_MONATLICH_JP

### 1. Orchestration and Scheduling Tests

#### Test Case 1.1: Event DAG - Successful Trigger and Prerequisite Check

*   **Purpose:** Verify `dw_dwh_run_apt_export_monatlich_jp_evt` DAG correctly simulates successful prerequisite checks and triggers the main export DAG (`dw_dwh_apt_export_monatlich_jp`), passing the `MONAT_ID` parameter.
*   **Setup:**
    1.  Deploy `dw_dwh_run_apt_export_monatlich_jp_evt.py` and `dw_dwh_apt_export_monatlich_jp.py` to the Airflow test environment.
    2.  Ensure `dw_dwh_apt_export_monatlich_jp` DAG is unpaused.
    3.  Mock the `check_prerequisites_function` to always return `True` (simulating successful prerequisite DAGs).
    4.  Define a specific `MONAT_ID` for the test, e.g., `20231001`.
*   **Action:**
    1.  Manually trigger `dw_dwh_run_apt_export_monatlich_jp_evt` DAG for a specific execution date (e.g., `2023-10-01`).
    2.  Observe the Airflow UI for the execution flow.
*   **Pass/Fail Criterion:**
    *   The `guard_concurrency` task completes successfully.
    *   The `check_prerequisites` task completes successfully.
    *   The `trigger_main_export_dag` task completes successfully.
    *   A new DAG run for `dw_dwh_apt_export_monatlich_jp` is created and triggered.
    *   The triggered `dw_dwh_apt_export_monatlich_jp` DAG run's configuration (`dag_run.conf`) contains `{'monat_id': '20231001'}` (or the `ds_nodash` of the triggering DAG if `conf` is not explicitly set).

```python
# Example pytest for Airflow DAG structure and triggering
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.timezone import datetime
from unittest.mock import patch

@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_event_dag_successful_trigger(dagbag):
    event_dag_id = "dw_dwh_run_apt_export_monatlich_jp_evt"
    main_dag_id = "dw_dwh_apt_export_monatlich_jp"
    event_dag = dagbag.get_dag(event_dag_id)
    main_dag = dagbag.get_dag(main_dag_id)

    assert event_dag is not None
    assert main_dag is not None

    execution_date = datetime(2023, 10, 1)
    expected_monat_id = "20231001"

    with patch("dags.dw_dwh_run_apt_export_monatlich_jp_evt.check_prerequisites_function", return_value=True):
        # Simulate triggering the event DAG
        event_dag_run = event_dag.create_dagrun(
            state=DagRunState.QUEUED,
            execution_date=execution_date,
            start_date=execution_date,
            run_id=f"test_run_{execution_date.isoformat()}"
        )
        
        # Manually run tasks (in a real test, this would be via Airflow scheduler/executor)
        # For unit testing, we can simulate task execution
        ti_start = event_dag_run.get_task_instance(task_id="start")
        ti_start.run(session=event_dag_run.get_session())
        
        ti_guard = event_dag_run.get_task_instance(task_id="guard_concurrency")
        ti_guard.run(session=event_dag_run.get_session())

        ti_prereq = event_dag_run.get_task_instance(task_id="check_prerequisites")
        ti_prereq.run(session=event_dag_run.get_session())

        ti_trigger = event_dag_run.get_task_instance(task_id="trigger_main_export_dag")
        ti_trigger.run(session=event_dag_run.get_session())

        ti_cleanup = event_dag_run.get_task_instance(task_id="cleanup_event_state")
        ti_cleanup.run(session=event_dag_run.get_session())

        ti_end = event_dag_run.get_task_instance(task_id="end")
        ti_end.run(session=event_dag_run.get_session())

        # Assert event DAG tasks completed successfully
        assert event_dag_run.get_task_instance(task_id="start").current_state() == DagRunState.SUCCESS
        assert event_dag_run.get_task_instance(task_id="guard_concurrency").current_state() == DagRunState.SUCCESS
        assert event_dag_run.get_task_instance(task_id="check_prerequisites").current_state() == DagRunState.SUCCESS
        assert event_dag_run.get_task_instance(task_id="trigger_main_export_dag").current_state() == DagRunState.SUCCESS
        assert event_dag_run.get_task_instance(task_id="cleanup_event_state").current_state() == DagRunState.SUCCESS
        assert event_dag_run.get_task_instance(task_id="end").current_state() == DagRunState.SUCCESS

        # Assert main DAG was triggered
        triggered_dag_runs = DagRun.find(dag_id=main_dag_id, external_trigger=True)
        assert len(triggered_dag_runs) >= 1 # Could be more if other tests triggered
        
        # Find the specific run triggered by our test
        triggered_run = next((dr for dr in triggered_dag_runs if dr.conf.get('monat_id') == expected_monat_id), None)
        assert triggered_run is not None
        assert triggered_run.conf.get('monat_id') == expected_monat_id
        assert triggered_run.state == DagRunState.QUEUED # Or RUNNING/SUCCESS depending on executor
```

#### Test Case 1.2: Event DAG - Prerequisite Check Failure

*   **Purpose:** Verify `dw_dwh_run_apt_export_monatlich_jp_evt` DAG correctly handles prerequisite failures by not triggering the main export DAG.
*   **Setup:**
    1.  Deploy `dw_dwh_run_apt_export_monatlich_jp_evt.py` to the Airflow test environment.
    2.  Mock the `check_prerequisites_function` to raise an exception or return `False` (simulating failed prerequisite DAGs).
*   **Action:**
    1.  Manually trigger `dw_dwh_run_apt_export_monatlich_jp_evt` DAG.
    2.  Observe the Airflow UI.
*   **Pass/Fail Criterion:**
    *   The `check_prerequisites` task fails.
    *   The `trigger_main_export_dag` task is skipped or does not run.
    *   No new DAG run for `dw_dwh_apt_export_monatlich_jp` is created.

#### Test Case 1.3: Event DAG - Concurrency Handling (Else=Skip)

*   **Purpose:** Verify `dw_dwh_run_apt_export_monatlich_jp_evt` DAG correctly implements `SYNCREF Else=Skip` by skipping a new run if another is already active.
*   **Setup:**
    1.  Deploy `dw_dwh_run_apt_export_monatlich_jp_evt.py` to the Airflow test environment.
    2.  Ensure `max_active_runs=1` is set for the DAG.
*   **Action:**
    1.  Manually trigger `dw_dwh_run_apt_export_monatlich_jp_evt` DAG (Run A).
    2.  Immediately after Run A starts, manually trigger it again (Run B).
    3.  Observe the Airflow UI for both runs.
*   **Pass/Fail Criterion:**
    *   Run A proceeds as normal.
    *   Run B's `guard_concurrency` task fails with an `AirflowSkipException`, causing the entire Run B to be marked as skipped.

#### Test Case 1.4: Main Export DAG - Concurrency Handling (Else=Wait)

*   **Purpose:** Verify `dw_dwh_apt_export_monatlich_jp` DAG correctly implements `SYNCREF Else=Wait` by preventing concurrent runs.
*   **Setup:**
    1.  Deploy `dw_dwh_apt_export_monatlich_jp.py` to the Airflow test environment.
    2.  Ensure `max_active_runs=1` is set for the DAG.
*   **Action:**
    1.  Manually trigger `dw_dwh_apt_export_monatlich_jp` DAG (Run A).
    2.  Immediately after Run A starts, manually trigger it again (Run B).
    3.  Observe the Airflow UI for both runs.
*   **Pass/Fail Criterion:**
    *   Run A proceeds as normal.
    *   Run B remains in a `queued` or `scheduled` state until Run A completes, then it starts. It does not fail or skip due to concurrency.

### 2. Data Extraction and Transformation Tests

#### Test Case 2.1: PySpark Data Exporter - Oracle Connectivity and Query Execution

*   **Purpose:** Verify `nna_data_exporter.py` and `nna_voice_exporter.py` can successfully connect to the Oracle database and execute their respective SQL queries.
*   **Setup:**
    1.  Ensure the Dataproc cluster has the necessary Oracle JDBC driver installed or provided via `jar_file_uris`.
    2.  Configure `JDBC_URL`, `JDBC_USER`, `JDBC_PASSWORD` in `nna_data_exporter.py` and `nna_voice_exporter.py` with valid test Oracle credentials (preferably from Secret Manager).
    3.  Populate the test Oracle database with a small, representative dataset for a specific `MONAT_ID`.
    4.  Ensure the PySpark applications' SQL queries are fully implemented based on reverse-engineered legacy logic.
*   **Action:**
    1.  Manually trigger `dw_dwh_apt_export_monatlich_jp` DAG, passing a `MONAT_ID` that corresponds to the test data.
    2.  Monitor Dataproc job logs for connection errors or SQL execution failures.
*   **Pass/Fail Criterion:**
    *   Both `export_nna_data` and `export_nna_voice` tasks complete successfully.
    *   Dataproc job logs show successful connection to Oracle and execution of the SQL queries without errors.
    *   Output files are generated in GCS (even if empty, for this test).

#### Test Case 2.2: PySpark Data Exporter - `MONAT_ID` Filtering Correctness

*   **Purpose:** Verify that the `MONAT_ID` parameter is correctly used in the PySpark jobs' SQL queries to filter data, matching the legacy job's behavior.
*   **Setup:**
    1.  Test Oracle database with data for multiple `MONAT_ID` values (e.g., `20230901`, `20231001`, `20231101`).
    2.  Known expected row counts for each `MONAT_ID` from the legacy system or direct Oracle queries.
*   **Action:**
    1.  Run the `dw_dwh_apt_export_monatlich_jp` DAG for `MONAT_ID = 20231001`.
    2.  Run the legacy job for the same `MONAT_ID`.
    3.  Query the Oracle source database directly using the PySpark job's SQL query with `MONAT_ID = 20231001`.
*   **Pass/Fail Criterion:**
    *   The row count of the output CSV from the migrated job for `MONAT_ID = 20231001` matches the row count from the legacy job for the same `MONAT_ID`.
    *   The row count also matches the direct query against the Oracle source using the PySpark job's SQL.

```sql
-- Example SQL to verify MONAT_ID filtering for nna_data_exporter
-- This query should be the exact one used in nna_data_exporter.py,
-- with '{monat_id}' replaced by the test value (e.g., '20231001').
SELECT COUNT(*)
FROM your_oracle_schema.EXIS_APT_NNA_DATA_SOURCE_TABLE
WHERE DATA_MONTH_ID = '20231001';

-- Repeat for nna_voice_exporter.py
SELECT COUNT(*)
FROM your_oracle_schema.EXIS_APT_NNA_VOICE_SOURCE_TABLE
WHERE VOICE_MONTH_ID = '20231001';
```

#### Test Case 2.3: PySpark Data Exporter - Transformation Logic Parity

*   **Purpose:** Verify that any in-PySpark transformations (e.g., column selection, renaming, data type conversions, specific business logic from `r_exis_v2`) produce results identical to the legacy system.
*   **Setup:**
    1.  A small, controlled dataset in the test Oracle database that covers various data types, edge cases for transformations (e.g., division by zero, string manipulations, date formats).
    2.  Detailed documentation or reverse-engineered logic of `r_exis_v2` transformations.
    3.  Legacy output files for this specific dataset.
*   **Action:**
    1.  Run the migrated job with the controlled dataset.
    2.  Run the legacy job with the same controlled dataset.
    3.  Compare the output CSV files using a data comparison tool or script.
*   **Pass/Fail Criterion:**
    *   The transformed data in the output CSVs from the migrated job is byte-for-byte identical to the legacy output, or identical after accounting for expected differences like timestamp formats or file ordering (if not explicitly sorted).
    *   Specifically check:
        *   Column names and casing.
        *   Data types (e.g., numbers, dates, strings).
        *   Calculated fields.
        *   Filtering logic.

#### Test Case 2.4: PySpark Data Exporter - NULL Handling Parity

*   **Purpose:** Verify that NULL values in source data are handled consistently (e.g., represented as empty strings, specific default values, or actual NULLs) in the output CSVs compared to the legacy system.
*   **Setup:**
    1.  Test Oracle database with rows containing NULLs in various columns that are part of the export.
    2.  Legacy output files for this specific dataset.
*   **Action:**
    1.  Run the migrated job with the NULL-containing dataset.
    2.  Run the legacy job with the same dataset.
    3.  Compare the output CSV files, focusing on columns with NULL values.
*   **Pass/Fail Criterion:**
    *   The representation of NULL values in the migrated job's output CSV (e.g., `,,` for empty string, `,"NULL",` for explicit string "NULL") matches the legacy output exactly.

#### Test Case 2.5: PySpark Data Exporter - Edge Case: Empty Source Data

*   **Purpose:** Verify the PySpark jobs handle scenarios where the Oracle query returns no data, resulting in an empty but valid output file.
*   **Setup:**
    1.  Test Oracle database where the query for a specific `MONAT_ID` (e.g., `20230101`) is guaranteed to return zero rows.
*   **Action:**
    1.  Run the `dw_dwh_apt_export_monatlich_jp` DAG for `MONAT_ID = 20230101`.
    2.  Check the GCS output bucket.
*   **Pass/Fail Criterion:**
    *   Both `export_nna_data` and `export_nna_voice` tasks complete successfully.
    *   Compressed CSV files are generated in GCS for both exporters.
    *   When decompressed, these CSV files contain only the header row and no data rows.

### 3. Output Parity and Data Quality Tests

#### Test Case 3.1: Output File Content Parity (nna_data_exporter)

*   **Purpose:** Compare the content of the exported CSV file from `nna_data_exporter.py` with a corresponding legacy output file for a given `MONAT_ID`. This is the ultimate output parity test.
*   **Setup:**
    1.  Run the legacy `DW.DWH_EXIS_SD_APT_NNA_DATA` job for a specific `MONAT_ID` (e.g., `20231001`) and save its compressed CSV output.
    2.  Ensure the test Oracle database contains the exact same data as the legacy source for that `MONAT_ID`.
    3.  Run the migrated `dw_dwh_apt_export_monatlich_jp` DAG for `MONAT_ID = 20231001`.
    4.  Download the generated compressed CSV from GCS.
*   **Action:**
    1.  Decompress both the legacy and migrated CSV files.
    2.  Use a robust data comparison script (e.g., Python with `pandas`) to compare the two CSVs. Consider sorting both dataframes by a primary key (or all columns if no PK) before comparison to account for potential row order differences.
*   **Pass/Fail Criterion:**
    *   The data content (excluding header and potential row order) of the migrated CSV is identical to the legacy CSV.
    *   Row counts match.
    *   Column values match, accounting for any floating-point precision or date format variations if explicitly allowed by design.

```python
# Example Python script for CSV comparison (using pandas)
import pandas as pd
import gzip
import os

def compare_csv_files(legacy_file_path, migrated_file_path, sort_columns=None):
    # Decompress if gzipped
    def read_compressed_csv(file_path):
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt') as f:
                return pd.read_csv(f)
        else:
            return pd.read_csv(file_path)

    df_legacy = read_compressed_csv(legacy_file_path)
    df_migrated = read_compressed_csv(migrated_file_path)

    # Basic checks
    if df_legacy.shape != df_migrated.shape:
        print(f"Shape mismatch: Legacy {df_legacy.shape}, Migrated {df_migrated.shape}")
        return False
    
    if not df_legacy.columns.equals(df_migrated.columns):
        print("Column mismatch:")
        print(f"Legacy columns: {df_legacy.columns.tolist()}")
        print(f"Migrated columns: {df_migrated.columns.tolist()}")
        return False

    # Sort for robust comparison if row order is not guaranteed
    if sort_columns:
        df_legacy = df_legacy.sort_values(by=sort_columns).reset_index(drop=True)
        df_migrated = df_migrated.sort_values(by=sort_columns).reset_index(drop=True)
    else: # If no specific sort columns, sort by all columns
        df_legacy = df_legacy.sort_values(by=df_legacy.columns.tolist()).reset_index(drop=True)
        df_migrated = df_migrated.sort_values(by=df_migrated.columns.tolist()).reset_index(drop=True)


    # Compare dataframes
    comparison_result = df_legacy.equals(df_migrated)
    if not comparison_result:
        print("Data content mismatch found.")
        # Optional: print differences for debugging
        diff = df_legacy.compare(df_migrated)
        if not diff.empty:
            print("Differences:")
            print(diff)
    
    return comparison_result

# Usage example:
# legacy_output_path = "path/to/legacy/DWHM_APT_NNA_Daten_20231001000000.csv.gz"
# migrated_output_path = "path/to/migrated/DWHM_APT_NNA_Daten_20231001123456.csv.gz"
# assert compare_csv_files(legacy_output_path, migrated_output_path, sort_columns=['column_one', 'column_two'])
```

#### Test Case 3.2: Output File Content Parity (nna_voice_exporter)

*   **Purpose:** Same as Test Case 3.1, but for `nna_voice_exporter.py` output.
*   **Setup:**
    1.  Run the legacy `DW.DWH_EXIS_SD_APT_NNA_VOIC` job for a specific `MONAT_ID` (e.g., `20231001`) and save its compressed CSV output.
    2.  Ensure the test Oracle database contains the exact same data as the legacy source for that `MONAT_ID`.
    3.  Run the migrated `dw_dwh_apt_export_monatlich_jp` DAG for `MONAT_ID = 20231001`.
    4.  Download the generated compressed CSV from GCS.
*   **Action:**
    1.  Decompress both the legacy and migrated CSV files.
    2.  Use the `compare_csv_files` script (or similar) to compare the two CSVs.
*   **Pass/Fail Criterion:**
    *   The data content of the migrated CSV is identical to the legacy CSV.

#### Test Case 3.3: Output File Naming, Compression, and Location

*   **Purpose:** Verify the output file names, compression (`.gz`), and GCS path structure match the specified convention.
*   **Setup:**
    1.  Run the `dw_dwh_apt_export_monatlich_jp` DAG for a specific `MONAT_ID`.
    2.  Note the expected GCS output bucket and path prefix.
*   **Action:**
    1.  Inspect the GCS bucket (`gs://your-gcs-export-bucket/exports/`) after the job completes.
*   **Pass/Fail Criterion:**
    *   For `nna_data_exporter`: A file matching `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz` exists in the correct GCS path.
    *   For `nna_voice_exporter`: A file matching `DWHM_APT_NNA_VOIC_<yyyymmddhhmmss>.csv.gz` exists in the correct GCS path.
    *   Both files are compressed (indicated by `.gz` extension) and can be successfully decompressed.

#### Test Case 3.4: Row Count and Schema Verification

*   **Purpose:** Verify the number of rows and the schema (column names, order, inferred types) in the exported CSV files match the legacy output.
*   **Setup:**
    1.  Obtain the row counts and schema (column names, order, data types) from the legacy output files for a specific `MONAT_ID`.
    2.  Run the migrated job for the same `MONAT_ID`.
    3.  Download and decompress the migrated output CSVs.
*   **Action:**
    1.  Read the migrated CSVs into pandas DataFrames.
    2.  Compare `df.shape[0]` (row count) and `df.columns.tolist()` (column names and order) against the legacy baseline.
    3.  Inspect `df.dtypes` to ensure inferred data types are consistent with expectations (e.g., numeric columns are not read as objects).
*   **Pass/Fail Criterion:**
    *   The row count of each migrated output file matches its corresponding legacy output file.
    *   The column names and their order in the migrated output files match the legacy output files.
    *   Inferred data types are consistent with the legacy output and expected data types.

### 4. Error Handling and Resilience Tests

#### Test Case 4.1: PySpark Job - Oracle Connection Failure

*   **Purpose:** Verify the PySpark job fails gracefully and the Airflow task retries/fails as expected if it cannot establish a connection to the Oracle database.
*   **Setup:**
    1.  Intentionally misconfigure the `JDBC_URL`, `JDBC_USER`, or `JDBC_PASSWORD` in `nna_data_exporter.py` (e.g., invalid host, wrong credentials).
    2.  Set `retries` to a value > 0 in the Airflow DAG `default_args`.
*   **Action:**
    1.  Trigger the `dw_dwh_apt_export_monatlich_jp` DAG.
    2.  Observe the Airflow UI and Dataproc job logs.
*   **Pass/Fail Criterion:**
    *   The `export_nna_data` task fails after exhausting its retries.
    *   Dataproc job logs clearly indicate an Oracle connection error (e.g., `ORA-XXXXX`, `Connection refused`).
    *   The overall `dw_dwh_apt_export_monatlich_jp` DAG run is marked as failed.

#### Test Case 4.2: PySpark Job - GCS Write Permissions Failure

*   **Purpose:** Verify the PySpark job fails if it lacks permissions to write to the target GCS bucket.
*   **Setup:**
    1.  Configure the Dataproc service account (or the service account used by the Airflow worker submitting the Dataproc job) to explicitly *deny* write access to the target `GCS_OUTPUT_BUCKET`.
*   **Action:**
    1.  Trigger the `dw_dwh_apt_export_monatlich_jp` DAG.
    2.  Observe the Airflow UI and Dataproc job logs.
*   **Pass/Fail Criterion:**
    *   The `export_nna_data` or `export_nna_voice` task fails.
    *   Dataproc job logs indicate a permissions error (e.g., `AccessDeniedException`, `403 Forbidden`).
    *   No output files are created in the GCS bucket.
    *   The overall `dw_dwh_apt_export_monatlich_jp` DAG run is marked as failed.

---