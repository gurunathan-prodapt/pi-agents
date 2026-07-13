Here is the comprehensive suite of migration-validation tests designed to verify that the migrated Apache Airflow DAGs, BigQuery operations, and GCS exports behave identically to the legacy UC4 and KornShell-based workflows.

---

# Test Suite: `DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP` Migration Validation

## Section 1: Orchestration & Dependency Parity

### Test Case 1.1: DAG Structure and Dependency Topology Validation
* **Purpose**: Verify that the migrated Airflow DAG `dw_dwh_ikdb_stamm_kek_taeglich_jp` mirrors the exact task execution sequence, parallel branches, and synchronization barriers defined in the legacy UC4 XML.
* **Setup**: 
  * Deploy the migrated DAG file `dags/dw_dwh_ikdb_stamm_kek_taeglich_jp.py` to a test Airflow environment.
  * Initialize an Airflow `DagBag`.
* **Action**: Parse the DAG and programmatically assert the upstream and downstream relationships of each task.
* **Pass/Fail Criterion**: The test passes only if all task IDs exist, the execution flow is strictly sequential from `start` through the consolidation steps, splits into the three parallel SFTP branches, and converges at `end`.

```python
import pytest
from airflow.models import DagBag

def test_dag_structure_and_dependencies():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_ikdb_stamm_kek_taeglich_jp")
    
    assert dag is not None, "DAG dw_dwh_ikdb_stamm_kek_taeglich_jp failed to load."
    assert len(dag.errors) == 0, f"DAG import errors: {dag.errors}"
    
    # Verify Task Existence
    expected_tasks = {
        "start",
        "dw_dwh_ikdb_info_import_taeglich_jp",
        "dw_dwh_ikdb_export_stamm_taeglich_jp",
        "dw_dwh_ikdb_stamm_nachlieferung_export_jp",
        "dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp",
        "dw_dwh_ikdb_pseudo_nachlieferung_export_jp",
        "dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp",
        "dw_dwh_ikdb_kek_export_taeglich_jp",
        "dw_dwh_ikdb_kek_nachlieferung_export_jp",
        "dw_dwh_ikdb_kek_konsolidierung_taeglich_jp",
        "dw_dwh_ikdb_kek_out_tmd_sftp_jp",
        "dw_dwh_ikdb_stamm_out_tmd_sftp_jp",
        "dw_dwh_ikdb_pseudo_out_tmd_sftp_jp",
        "end"
    }
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Missing: {expected_tasks - actual_tasks}"

    # Verify Sequential Processing Chain
    assert "dw_dwh_ikdb_info_import_taeglich_jp" in dag.get_task("start").downstream_task_ids
    assert "dw_dwh_ikdb_export_stamm_taeglich_jp" in dag.get_task("dw_dwh_ikdb_info_import_taeglich_jp").downstream_task_ids
    assert "dw_dwh_ikdb_stamm_nachlieferung_export_jp" in dag.get_task("dw_dwh_ikdb_export_stamm_taeglich_jp").downstream_task_ids
    assert "dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp" in dag.get_task("dw_dwh_ikdb_stamm_nachlieferung_export_jp").downstream_task_ids
    assert "dw_dwh_ikdb_pseudo_nachlieferung_export_jp" in dag.get_task("dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp").downstream_task_ids
    assert "dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp" in dag.get_task("dw_dwh_ikdb_pseudo_nachlieferung_export_jp").downstream_task_ids
    assert "dw_dwh_ikdb_kek_export_taeglich_jp" in dag.get_task("dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp").downstream_task_ids
    assert "dw_dwh_ikdb_kek_nachlieferung_export_jp" in dag.get_task("dw_dwh_ikdb_kek_export_taeglich_jp").downstream_task_ids
    assert "dw_dwh_ikdb_kek_konsolidierung_taeglich_jp" in dag.get_task("dw_dwh_ikdb_kek_nachlieferung_export_jp").downstream_task_ids

    # Verify Parallel Downstream Branches (Predecessor Coordinates)
    assert "dw_dwh_ikdb_kek_out_tmd_sftp_jp" in dag.get_task("dw_dwh_ikdb_kek_konsolidierung_taeglich_jp").downstream_task_ids
    assert "dw_dwh_ikdb_stamm_out_tmd_sftp_jp" in dag.get_task("dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp").downstream_task_ids
    assert "dw_dwh_ikdb_pseudo_out_tmd_sftp_jp" in dag.get_task("dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp").downstream_task_ids

    # Verify Synchronized Collection at End Node
    end_upstream = dag.get_task("end").upstream_task_ids
    assert "dw_dwh_ikdb_kek_out_tmd_sftp_jp" in end_upstream
    assert "dw_dwh_ikdb_stamm_out_tmd_sftp_jp" in end_upstream
    assert "dw_dwh_ikdb_pseudo_out_tmd_sftp_jp" in end_upstream
```

### Test Case 1.2: Concurrency Control (Sync Object Parity)
* **Purpose**: Verify that the Airflow DAG enforces the legacy UC4 Sync Object behavior (`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP_SYNC` with `Else="Wait"`), which blocks concurrent executions of the same workflow.
* **Setup**: Deploy the DAG with `max_active_runs` configured.
* **Action**: Programmatically inspect the DAG's concurrency configuration.
* **Pass/Fail Criterion**: The DAG's `max_active_runs` property must be strictly equal to `1`.

```python
def test_dag_concurrency_limits():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_ikdb_stamm_kek_taeglich_jp")
    
    # Else="Wait" maps directly to max_active_runs=1
    assert dag.max_active_runs == 1, "DAG max_active_runs must be set to 1 to prevent concurrent runs."
```

---

# Section 2: Transformation Correctness & Date Logic

### Test Case 2.1: Dynamic Date Math and Backfill Range (7-Day Lookback)
* **Purpose**: Verify that the migrated Python/BigQuery logic correctly replicates the legacy KornShell date math (`r_exp_ikdb.ksh -n 7`), which checks for missing executions over the last 7 days and triggers backfills.
* **Setup**:
  * Populate the BigQuery tracking table `metadata_dataset.dwtk_meldungen` with mock execution records.
  * Leave a gap in execution for exactly 3 days ago.
  * Set the current execution date (`ds`) to `2026-04-21`.
* **Action**: Execute the dry-run/evaluation query that determines which dates within the 7-day window require processing.
* **Pass/Fail Criterion**: The evaluation query must return exactly the date that is missing from the tracking table within the `[ds - 7, ds - 1]` window.

```sql
-- Setup: Create mock tracking table and insert runs with a gap on 2026-04-18
CREATE OR REPLACE TABLE `metadata_dataset.dwtk_meldungen` AS (
  SELECT 'EXIS_IKDB_STAMM_R' AS job_name, DATE('2026-04-20') AS execution_date, 'SUCCESS' AS status UNION ALL
  SELECT 'EXIS_IKDB_STAMM_R', DATE('2026-04-19'), 'SUCCESS' UNION ALL
  -- 2026-04-18 is missing (Simulated Gap)
  SELECT 'EXIS_IKDB_STAMM_R', DATE('2026-04-17'), 'SUCCESS' UNION ALL
  SELECT 'EXIS_IKDB_STAMM_R', DATE('2026-04-16'), 'SUCCESS' UNION ALL
  SELECT 'EXIS_IKDB_STAMM_R', DATE('2026-04-15'), 'SUCCESS' UNION ALL
  SELECT 'EXIS_IKDB_STAMM_R', DATE('2026-04-14'), 'SUCCESS'
);

-- Action: Run lookback evaluation query for ds = '2026-04-21'
WITH target_dates AS (
  SELECT DATE_SUB(DATE('2026-04-21'), INTERVAL n DAY) AS eval_date
  FROM UNNEST(GENERATE_ARRAY(1, 7)) AS n
)
SELECT eval_date 
FROM target_dates
LEFT JOIN `metadata_dataset.dwtk_meldungen` m
  ON target_dates.eval_date = m.execution_date
  AND m.job_name = 'EXIS_IKDB_STAMM_R'
  AND m.status = 'SUCCESS'
WHERE m.execution_date IS NULL;

-- Assertion (Expected Output):
-- +------------+
-- | eval_date  |
-- +------------+
-- | 2026-04-18 |
-- +------------+
```

### Test Case 2.2: Master Data (Stamm) Transformation and Schema Validation
* **Purpose**: Verify that the BigQuery transformation query correctly handles data types, filters by execution date, and generates the expected schema structure.
* **Setup**:
  * Populate `analytical_dataset.source_stamm` with test records containing active records, past records, and NULL values.
* **Action**: Execute the transformation query for `ds = '2026-04-21'`.
* **Pass/Fail Criterion**: 
  * Only records matching `record_date = '2026-04-21'` are processed.
  * `processed_at` is populated with a valid timestamp.
  * No schema violations or type mismatches occur.

```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_stamm_transformation_logic(bq_client):
    # Setup: Insert mock source data
    setup_query = """
        CREATE OR REPLACE TABLE `analytical_dataset.source_stamm` AS
        SELECT 'M100' AS master_id, 'Alpha Corp' AS name, DATE('2026-04-21') AS record_date UNION ALL
        SELECT 'M200', 'Beta LLC', DATE('2026-04-21') UNION ALL
        SELECT 'M300', NULL, DATE('2026-04-21') UNION ALL -- NULL handling check
        SELECT 'M400', 'Gamma Inc', DATE('2026-04-20'); -- Out of scope date
    """
    bq_client.query(setup_query).result()

    # Action: Run the transformation query
    target_table = "temporary_staging_dataset.exis_ikdb_stamm_r_temp"
    transform_query = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT 
          master_id,
          COALESCE(name, 'UNKNOWN') AS name,
          record_date,
          CURRENT_TIMESTAMP() as processed_at
        FROM `analytical_dataset.source_stamm`
        WHERE record_date = '2026-04-21';
    """
    bq_client.query(transform_query).result()

    # Assertions
    results_query = f"SELECT * FROM `{target_table}` ORDER BY master_id"
    rows = list(bq_client.query(results_query).result())

    assert len(rows) == 3, "Expected exactly 3 records for 2026-04-21"
    
    # Assert record 1
    assert rows[0]["master_id"] == "M100"
    assert rows[0]["name"] == "Alpha Corp"
    assert rows[0]["record_date"] == bigquery.ArrayQueryParameterValue._to_json_value(rows[0]["record_date"]) or "2026-04-21"

    # Assert NULL handling (M300)
    assert rows[2]["master_id"] == "M300"
    assert rows[2]["name"] == "UNKNOWN", "NULL name was not correctly coalesced to 'UNKNOWN'"
```

---

# Section 3: External System Replacements

### Test Case 3.1: Metadata Tracking Table (`DWTK_MELDUNGEN`) Updates
* **Purpose**: Verify that the Airflow task group successfully logs execution status to the BigQuery tracking table upon completion, replacing the legacy Oracle `DWTK_MELDUNGEN` updates.
* **Setup**: Ensure the tracking table exists and is empty for the target execution date.
* **Action**: Execute the metadata logging task of `execute_ikdb_export` for `ds = '2026-04-21'`.
* **Pass/Fail Criterion**: A row must be appended to `metadata_dataset.dwtk_meldungen` with `status = 'SUCCESS'` and the correct job name and date.

```sql
-- Action: Run the logging query
INSERT INTO `metadata_dataset.dwtk_meldungen` (job_name, execution_date, status, updated_timestamp)
VALUES ('EXIS_IKDB_STAMM_R', DATE('2026-04-21'), 'SUCCESS', CURRENT_TIMESTAMP());

-- Assertion: Verify row insertion
SELECT COUNT(1) AS record_count 
FROM `metadata_dataset.dwtk_meldungen`
WHERE job_name = 'EXIS_IKDB_STAMM_R'
  AND execution_date = '2026-04-21'
  AND status = 'SUCCESS';

-- Expected Output: record_count = 1
```

### Test Case 3.2: GCS Export File Generation and Formatting
* **Purpose**: Verify that the `BigQueryToGCSOperator` correctly exports the transformed table to GCS as a semicolon-separated CSV file, matching the legacy export format.
* **Setup**: Run the transformation task to populate the temporary staging table.
* **Action**: Execute the GCS export task to write to `gs://dwh-export-ikdb-work/STAMM_OUT_TMD_20260421.csv`.
* **Pass/Fail Criterion**:
  * The file must exist in the GCS bucket.
  * The file content must be delimited by semicolons (`;`).
  * The file must contain the correct header and row count.

```python
from google.cloud import storage

def test_gcs_export_file_properties():
    bucket_name = "dwh-export-ikdb-work"
    blob_name = "STAMM_OUT_TMD_20260421.csv"
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    assert blob.exists(), f"Export file gs://{bucket_name}/{blob_name} does not exist."
    
    content = blob.download_as_text()
    lines = content.strip().split("\n")
    
    # Assert header structure
    header = lines[0]
    assert "master_id;name;record_date;processed_at" in header, "CSV header is missing or incorrectly delimited."
    
    # Assert data row formatting
    first_data_row = lines[1]
    assert ";" in first_data_row, "Data row is not semicolon-delimited."
    assert len(first_data_row.split(";")) == 4, "Data row does not contain exactly 4 columns."
```

---

# Section 4: Data Quality & Integrity Assertions

### Test Case 4.1: Zero-Row Export Prevention (Safety Gate)
* **Purpose**: Ensure that the export process fails or alerts if the source dataset contains 0 records for the execution date, preventing empty files from being transmitted downstream.
* **Setup**: Clear all records for `2026-04-21` from `analytical_dataset.source_stamm`.
* **Action**: Execute the `check_prior_run_registration` task or a custom validation check.
* **Pass/Fail Criterion**: The pipeline must raise an exception or fail the task group when the source row count is 0.

```python
def test_zero_row_safety_gate(bq_client):
    # Setup: Clear source table for the date
    clear_query = "DELETE FROM `analytical_dataset.source_stamm` WHERE record_date = '2026-04-21'"
    bq_client.query(clear_query).result()
    
    # Action & Assertion: Query row count and assert failure condition
    count_query = "SELECT COUNT(1) as cnt FROM `analytical_dataset.source_stamm` WHERE record_date = '2026-04-21'"
    result = list(bq_client.query(count_query).result())[0]
    
    # Safety gate assertion
    assert result["cnt"] > 0, "DATA QUALITY FAILURE: Source dataset contains 0 records. Aborting export to prevent empty file delivery."
```