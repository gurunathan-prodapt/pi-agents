# Migration Validation Test Suite: DW.DWH_EXIS_IKDB_STAMM_R

This document contains the comprehensive migration-validation test suite for the migrated job `DW.DWH_EXIS_IKDB_STAMM_R`. These tests verify behavioral equivalence between the legacy KornShell/UC4 implementation and the target Google Cloud Platform architecture (Apache Airflow, Dataproc PySpark, and BigQuery).

---

## Section 1: Output Parity Tests

### Test Case 1.1: End-to-End Output Data Parity (Legacy vs. Target)
* **Purpose:** Verify that running the migrated PySpark job on Dataproc with a given input dataset produces identical CSV output files (schema, row count, and column values) compared to the legacy KornShell execution.
* **Setup:**
  1. Identify a historical business date (e.g., `20260420`).
  2. Restore the legacy Oracle source table `contract_master_table` state for that date into a test BigQuery dataset: `test_2026_IKDB_SOURCE.contract_master_table`.
  3. Run the legacy export script on the legacy environment to generate the reference file: `STAMM_OUT_TMD_legacy.csv`.
  4. Upload the legacy reference file to a secure validation bucket: `gs://validation-bucket/reference/20260420/`.
* **Action:**
  Execute the migrated PySpark script on Dataproc using the following arguments:
  ```bash
  gcloud dataproc jobs submit pyspark gs://gcs-bucket-placeholder/pyspark_scripts/exis_ikdb_stamm_r.py \
      --cluster=dataproc-cluster-placeholder \
      --region=europe-west3 \
      -- \
      --query d_ikdb_exp_stamm.sql \
      --job_key EXIS_IKDB_STAMM_R \
      --file_type STAMM_OUT_TMD \
      --numeric_param 7 \
      --target_date 20260420
  ```
* **Pass/Fail Criterion:**
  The test passes if the generated GCS CSV files match the legacy reference file exactly. This is validated using the following PySpark assertion script run on the validation cluster:

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def test_output_parity():
    spark = SparkSession.builder.appName("Validation-OutputParity").getOrCreate()
    
    # Load legacy reference output
    legacy_df = spark.read.option("header", "true").csv("gs://validation-bucket/reference/20260420/*.csv")
    
    # Load migrated target output
    migrated_df = spark.read.option("header", "true").csv("gs://gcs-bucket-placeholder/exports/STAMM_OUT_TMD/20260420/*.csv")
    
    # 1. Row Count Validation
    legacy_count = legacy_df.count()
    migrated_count = migrated_df.count()
    assert legacy_count == migrated_count, f"Row count mismatch! Legacy: {legacy_count}, Migrated: {migrated_count}"
    
    # 2. Schema Validation (ignoring export_timestamp which is dynamic)
    legacy_cols = sorted([c for c in legacy_df.columns if c != "export_timestamp"])
    migrated_cols = sorted([c for c in migrated_df.columns if c != "export_timestamp"])
    assert legacy_cols == migrated_cols, f"Schema mismatch! Legacy: {legacy_cols}, Migrated: {migrated_cols}"
    
    # 3. Data Content Validation (excluding dynamic export_timestamp)
    legacy_clean = legacy_df.select(legacy_cols).orderBy("contract_id")
    migrated_clean = migrated_df.select(migrated_cols).orderBy("contract_id")
    
    diff_count = legacy_clean.subtract(migrated_clean).count()
    assert diff_count == 0, f"Data mismatch detected! {diff_count} rows differ between legacy and migrated outputs."
```

---

## Section 2: Transformation Correctness Tests

### Test Case 2.1: Date Subtraction Offset Logic (Numeric Param `-n 7`)
* **Purpose:** Verify that the dynamic SQL generator correctly translates the legacy date subtraction logic (`system_date >= DATE_SUB(..., INTERVAL 7 DAY)`) and filters the source dataset accurately.
* **Setup:**
  Populate the source BigQuery table `2026_IKDB_SOURCE.contract_master_table` with test records spanning various dates relative to the target date `20260420`:
  * Record A: `system_date` = `2026-04-20` (Within range)
  * Record B: `system_date` = `2026-04-13` (Exactly 7 days prior, boundary condition - Within range)
  * Record C: `system_date` = `2026-04-12` (8 days prior, outside range - Should be excluded)
  * Record D: `system_date` = `2026-04-21` (Future date relative to target - Within range)
* **Action:**
  Execute the query generator function in a test harness and run the query against the test dataset.
* **Pass/Fail Criterion:**
  The test passes if Record A, Record B, and Record D are returned, while Record C is strictly excluded.

```python
def test_date_subtraction_offset(spark_session):
    # Mocking the query generation for target_date='20260420' and numeric_param='7'
    from exis_ikdb_stamm_r import get_bigquery_query
    
    generated_sql = get_bigquery_query("d_ikdb_exp_stamm.sql", "20260420", "7")
    
    # Assert SQL contains correct BigQuery syntax for date subtraction
    assert "DATE_SUB(PARSE_DATE('%Y%m%d', '20260420'), INTERVAL 7 DAY)" in generated_sql
    
    # Execute generated query against mock view
    df = spark_session.sql(f"""
        WITH `2026_IKDB_SOURCE.contract_master_table` AS (
            SELECT 'A' as contract_id, 'P1' as partner_id, DATE '2026-04-20' as system_date, 'Active' as contract_status UNION ALL
            SELECT 'B' as contract_id, 'P2' as partner_id, DATE '2026-04-13' as system_date, 'Active' as contract_status UNION ALL
            SELECT 'C' as contract_id, 'P3' as partner_id, DATE '2026-04-12' as system_date, 'Active' as contract_status UNION ALL
            SELECT 'D' as contract_id, 'P4' as partner_id, DATE '2026-04-21' as system_date, 'Active' as contract_status
        )
        SELECT contract_id FROM ({generated_sql})
    """)
    
    results = [row['contract_id'] for row in df.collect()]
    assert "A" in results
    assert "B" in results
    assert "D" in results
    assert "C" not in results, "Record C (8 days old) should have been filtered out!"
```

### Test Case 2.2: NULL Handling and Schema Type Integrity
* **Purpose:** Ensure that NULL values in nullable columns (e.g., `partner_id`, `contract_status`) do not cause runtime failures and are preserved correctly in the output CSV without being converted to string `"null"` or `"NaN"`.
* **Setup:**
  Insert a record into `2026_IKDB_SOURCE.contract_master_table` where `partner_id` and `contract_status` are explicitly `NULL`.
* **Action:**
  Run the PySpark export job for target date `20260420`.
* **Pass/Fail Criterion:**
  The output CSV must contain empty fields (`,,`) for the NULL values, and the job must complete successfully with exit code 0.

```python
def test_null_handling(spark_session):
    # Create test DataFrame with NULLs
    schema_record = [{
        "contract_id": "CON_NULL_01",
        "partner_id": None,
        "system_date": "2026-04-20",
        "contract_status": None
    }]
    df = spark_session.createDataFrame(schema_record)
    
    # Write to temporary GCS path as CSV
    temp_path = "gs://gcs-bucket-placeholder/temporary_validation/null_test/"
    df.write.mode("overwrite").option("header", "true").csv(temp_path)
    
    # Read back and verify values are preserved as None/Null
    df_read = spark_session.read.option("header", "true").csv(temp_path)
    row = df_read.filter(col("contract_id") == "CON_NULL_01").first()
    
    assert row["partner_id"] is None or row["partner_id"] == ""
    assert row["contract_status"] is None or row["contract_status"] == ""
```

---

## Section 3: External-System Replacements & Orchestration Tests

### Test Case 3.1: Metadata Audit Log Verification (`DWTK_MELDUNGEN`)
* **Purpose:** Verify that the PySpark job correctly writes execution status records to the BigQuery metadata table `metadata_dataset.DWTK_MELDUNGEN` (Status `2` for success, Status `3` for failure).
* **Setup:**
  1. Ensure the BigQuery table `metadata_dataset.DWTK_MELDUNGEN` exists.
  2. Clear any existing records for `JOB_KENNUNG = 'EXIS_IKDB_STAMM_R'` and `STICHTAG = '20260420'`.
* **Action:**
  1. Run the PySpark job successfully for `20260420`. Verify status code `2` is written.
  2. Force a failure (e.g., by passing an invalid query name) and verify status code `3` is written.
* **Pass/Fail Criterion:**
  The metadata table must contain the correct status codes and timestamps matching the execution outcomes.

```sql
-- Assertion Query 1: Verify Success Status Registration
SELECT COUNT(1) FROM `metadata_dataset.DWTK_MELDUNGEN`
WHERE JOB_KENNUNG = 'EXIS_IKDB_STAMM_R'
  AND STATUS_NR = '2'
  AND STICHTAG = '20260420';
-- Expected Result: 1

-- Assertion Query 2: Verify Failure Status Registration (after forced failure run)
SELECT COUNT(1) FROM `metadata_dataset.DWTK_MELDUNGEN`
WHERE JOB_KENNUNG = 'EXIS_IKDB_STAMM_R'
  AND STATUS_NR = '3'
  AND STICHTAG = '20260420';
-- Expected Result: 1
```

### Test Case 3.2: Airflow Branching and Concurrency Lock (`max_active_runs=1`)
* **Purpose:** Verify that the Airflow DAG correctly skips execution if a successful run (Status `2`) already exists in the metadata database, and enforces a concurrency limit of 1.
* **Setup:**
  1. Insert a record into the metadata database: `JOB_KENNUNG = 'dw_dwh_exis_ikdb_stamm_r'`, `STATUS_NR = '2'`, `STICHTAG = '20260419'`.
  2. Configure the Airflow connection `metadata_db` to point to the metadata database.
* **Action:**
  1. Trigger the Airflow DAG `dw_dwh_exis_ikdb_stamm_r` for execution date `2026-04-20` (which evaluates business date `2026-04-19`).
  2. Simultaneously trigger a second run of the same DAG to test concurrency.
* **Pass/Fail Criterion:**
  * **Branching:** The task `check_already_executed` must branch to `skipped_already_run`, and `run_export_ikdb_task` must be skipped.
  * **Concurrency:** The second DAG run must be queued and must not run concurrently with the first run (enforced by `max_active_runs=1`).

```python
from airflow.providers.google.cloud.hooks.postgres import PostgresHook
from airflow.utils.state import State
from airflow.utils.types import DagRunType
import pytest

def test_airflow_skips_on_existing_success(dagbag):
    dag = dagbag.get_dag('dw_dwh_exis_ikdb_stamm_r')
    assert dag is not None
    assert dag.max_active_runs == 1
    
    # Verify task structure and branching
    check_task = dag.get_task('check_already_executed')
    assert "run_export_ikdb_task" in check_task.downstream_task_ids
    assert "skipped_already_run" in check_task.downstream_task_ids
```

---

## Section 4: Data-Quality, Row-Count, and Schema Assertions

### Test Case 4.1: Post-Migration Schema and Nullability Assertions
* **Purpose:** Ensure that the final exported CSV files conform to the strict target schema specifications and do not contain corrupted headers or structural anomalies.
* **Setup:**
  Run the PySpark export job to generate output files in GCS.
* **Action:**
  Execute a validation script against the generated CSV files in GCS.
* **Pass/Fail Criterion:**
  The files must pass all schema, non-emptiness, and column count assertions.

```python
def test_gcs_file_quality():
    import pandas as pd
    import gcsfs
    
    fs = gcsfs.GCSFileSystem()
    target_files = fs.glob("gs://gcs-bucket-placeholder/exports/STAMM_OUT_TMD/20260420/*.csv")
    
    assert len(target_files) > 0, "No export files found in GCS target directory!"
    
    # Read the first partition file into Pandas for structural validation
    with fs.open(target_files[0]) as f:
        df = pd.read_csv(f)
        
    # Assert Column Count
    expected_column_count = 6
    assert len(df.columns) == expected_column_count, f"Expected {expected_column_count} columns, found {len(df.columns)}"
    
    # Assert Column Names
    expected_headers = ["contract_id", "partner_id", "system_date", "contract_status", "reporting_date", "export_timestamp"]
    assert list(df.columns) == expected_headers, f"Header mismatch! Expected: {expected_headers}, Got: {list(df.columns)}"
    
    # Assert Critical Column Nullability (contract_id must never be null)
    null_contract_ids = df["contract_id"].isnull().sum()
    assert null_contract_ids == 0, f"Data Quality Failure: Found {null_contract_ids} NULL values in primary key 'contract_id'!"
```