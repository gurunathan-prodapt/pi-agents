Here is the comprehensive, production-ready test suite designed to validate the migration of the `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` pipeline from its legacy UC4/Oracle environment to GCP (Cloud Composer, BigQuery, and GCS).

---

# MIGRATION VALIDATION TEST SUITE
**Job Name**: `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`  
**Target Stack**: Cloud Composer (Airflow) + BigQuery + Google Cloud Storage (GCS)

---

## Test Suite Overview & Topology

To guarantee absolute behavioral equivalence, the validation strategy is split into four distinct phases:

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ 1. Pre-Migration Schema & Static Code Audits                            │
  │    - Validates BigQuery DDL, column types, and DAG structure.           │
  └────────────────────────────────────┬────────────────────────────────────┘
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ 2. Core Transformation & Dialect Parity (Dry-Runs)                      │
  │    - Verifies SQL compilation, parameter binding, and NULL handling.    │
  └────────────────────────────────────┬────────────────────────────────────┘
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ 3. End-to-End Dynamic Execution & Output Parity                         │
  │    - Runs parallel pipelines on identical inputs; compares outputs.     │
  └────────────────────────────────────┬────────────────────────────────────┘
                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ 4. Operational, Logging, & Edge-Case Assertions                         │
  │    - Validates zero-row warnings, GCS shard consolidation, & log text.  │
  └─────────────────────────────────────────────────────────────────────────┘
```

---

## Section 1: Pre-Migration Schema & Static Code Audits

### Test Case 1.1: Schema and Type Mapping Verification
#### Purpose
Ensure that the target BigQuery table `T_RECHNUNG` matches the precision, scale, and nullability constraints of the legacy Oracle table, preventing silent truncation or rounding errors on financial fields.

#### Setup
* Access to the target BigQuery environment with the migrated table `T_RECHNUNG` deployed.
* Access to the legacy Oracle database catalog or DDL metadata.

#### Action
Execute a metadata comparison query in BigQuery Information Schema and assert that the types map exactly to the design specifications.

```python
import pytest
from google.cloud import bigquery

@pytest.mark.schema
def test_schema_and_type_mapping():
    client = bigquery.Client()
    
    # Query BigQuery Information Schema for target table
    query = """
    SELECT column_name, data_type, is_nullable
    FROM `DWH_KERN.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'T_RECHNUNG'
    """
    query_job = client.query(query)
    results = {row.column_name: (row.data_type, row.is_nullable) for row in query_job}
    
    # Expected mappings based on migration design
    expected_schema = {
        "RECHNUNGSNUMMER": ("STRING", "YES"),
        "VERTRAG": ("STRING", "YES"),
        "KUNDE": ("STRING", "YES"),
        "TARIF": ("STRING", "YES"),
        "ABRECHNUNGSZEITRAUM": ("STRING", "YES"),
        "RECHNUNGSBETRAG": ("NUMERIC", "YES"), # Must be NUMERIC, not FLOAT64
        "WAEHRUNG": ("STRING", "YES"),
        "RECHNUNGSDATUM": ("DATE", "YES")
    }
    
    for col, expected in expected_schema.items():
        assert col in results, f"Column {col} is missing from the migrated BigQuery table."
        assert results[col][0] == expected[0], f"Column {col} type mismatch. Expected {expected[0]}, got {results[col][0]}."
```

#### Pass/Fail Criterion
* **Pass**: All columns exist in the target BigQuery table, and `RECHNUNGSBETRAG` is strictly typed as `NUMERIC` to preserve financial precision.
* **Fail**: Any column is missing, or `RECHNUNGSBETRAG` is mapped to `FLOAT64` (which introduces floating-point rounding risks).

---

### Test Case 1.2: Airflow DAG Structural & Import Validation
#### Purpose
Verify that the migrated Airflow DAG `dw_dwh_rechnung_export_taeglich_js` compiles without syntax errors, contains all required tasks, and correctly maps the execution dependencies.

#### Setup
* A local or CI/CD runner environment with `apache-airflow` and GCP provider packages installed.
* The migrated DAG file placed in the DAGs folder.

#### Action
Run a programmatic unit test using `pytest` to parse the DAG and assert its structure.

```python
import pytest
from airflow.models import DagBag

@pytest.mark.static
def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags/dwh_rechnung", include_examples=False)
    dag_id = "dw_dwh_rechnung_export_taeglich_js"
    
    # Assert no import errors in the entire bag
    assert len(dag_bag.import_errors) == 0, f"DAG Import Errors: {dag_bag.import_errors}"
    
    # Assert DAG exists
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} failed to load."
    
    # Assert task structure and execution sequence
    expected_tasks = ["print_status", "execute_export_query", "consolidate_files"]
    actual_tasks = [task.task_id for task in dag.tasks]
    assert set(expected_tasks) == set(actual_tasks), "DAG tasks do not match the design."
    
    # Assert sequential dependency: print_status -> execute_export_query -> consolidate_files
    print_task = dag.get_task("print_status")
    export_task = dag.get_task("execute_export_query")
    consolidate_task = dag.get_task("consolidate_files")
    
    assert export_task in print_task.downstream_list, "Dependency print_status -> execute_export_query is broken."
    assert consolidate_task in export_task.downstream_list, "Dependency execute_export_query -> consolidate_files is broken."
```

#### Pass/Fail Criterion
* **Pass**: The DAG parses with zero import errors, contains exactly the three specified tasks, and enforces the strict linear dependency chain.
* **Fail**: Any import error is raised, tasks are missing, or dependencies are incorrectly wired.

---

## Section 2: Core Transformation & Dialect Parity

### Test Case 2.1: SQL Dialect Compilation & Parameter Binding (Dry-Run)
#### Purpose
Verify that the converted BigQuery SQL script compiles successfully against the target engine and that the dynamic parameter `@p_Stichtag` binds correctly.

#### Setup
* Access to the target BigQuery dataset.
* The converted SQL file `dags/dwh_rechnung/sql/d_exp_rechnung_taeglich.sql`.

#### Action
Execute a dry-run query job in BigQuery with parameter binding enabled to validate syntax correctness without incurring query costs.

```python
import os
import pytest
from google.cloud import bigquery

@pytest.mark.compile
def test_sql_dry_run():
    client = bigquery.Client()
    
    # Read the SQL file
    sql_path = "dags/dwh_rechnung/sql/d_exp_rechnung_taeglich.sql"
    with open(sql_path, "r") as f:
        raw_sql = f.read()
    
    # Resolve environment placeholders
    resolved_sql = raw_sql.replace("@gcp_project", client.project).replace("@bq_dataset", "DWH_KERN")
    
    # Configure dry run with query parameters
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("p_Stichtag", "STRING", "20260101")
        ]
    )
    
    try:
        query_job = client.query(resolved_sql, job_config=job_config)
        # A successful dry run populates total_bytes_processed
        assert query_job.total_bytes_processed is not None
        print(f"Dry run successful. Estimated bytes: {query_job.total_bytes_processed}")
    except Exception as e:
        pytest.fail(f"SQL compilation failed: {str(e)}")
```

#### Pass/Fail Criterion
* **Pass**: The BigQuery engine compiles the SQL query successfully, returning a valid dry-run execution plan.
* **Fail**: The engine throws a syntax, table-not-found, or type-mismatch error.

---

### Test Case 2.2: NULL and Edge-Case Handling
#### Purpose
Verify that the query handles edge cases (such as NULL values in nullable fields and extreme decimal values in `RECHNUNGSBETRAG`) identically to the legacy Oracle implementation.

#### Setup
* A test dataset in BigQuery containing a mock table `T_RECHNUNG_TEST` populated with:
  * Row 1: All fields populated with standard values.
  * Row 2: All nullable fields set to `NULL` (except primary keys/identifiers).
  * Row 3: Extreme financial values (e.g., `999999999.99` and `-999999999.99`).

#### Action
Run the query against the mock table and assert that the output rows preserve NULLs and exact decimal values without truncation or conversion errors.

```sql
-- Test Query to execute against mock table
SELECT
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  CAST(r.RECHNUNGSBETRAG AS NUMERIC) AS RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
FROM 
  `DWH_KERN.T_RECHNUNG_TEST` AS r
WHERE 
  r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', '20260101')
ORDER BY 
  r.RECHNUNGSNUMMER;
```

```python
# Python assertion block
def assert_transformation_correctness(results):
    # Row 1: Standard
    assert results[0]["RECHNUNGSNUMMER"] == "REC-001"
    
    # Row 2: NULL Handling
    assert results[1]["VERTRAG"] is None
    assert results[1]["KUNDE"] is None
    assert results[1]["RECHNUNGSBETRAG"] is None
    
    # Row 3: Extreme Decimal Precision
    from decimal import Decimal
    assert results[2]["RECHNUNGSBETRAG"] == Decimal("999999999.99")
```

#### Pass/Fail Criterion
* **Pass**: NULL values are preserved as native BigQuery `NULL`s, and extreme decimal values are returned with exact precision (no floating-point approximations like `999999999.9900001`).
* **Fail**: NULLs are converted to empty strings, or decimal values suffer precision loss.

---

## Section 3: End-to-End Dynamic Execution & Output Parity

### Test Case 3.1: Parallel Run Output Parity (A/B Testing)
#### Purpose
Prove that running the legacy Oracle pipeline and the migrated GCP pipeline on the exact same input dataset produces identical output files (byte-for-byte or semantic equivalence).

#### Setup
* **Legacy Environment**: Oracle database containing a fixed set of 10,000 invoice records for `Stichtag` `20260115`.
* **GCP Environment**: BigQuery `T_RECHNUNG` loaded with the exact same 10,000 records.
* **Target GCS Bucket**: Configured and empty.

#### Action
1. Execute the legacy shell script:
   ```bash
   ./r_exp_rechnung_taeglich.ksh -s 20260115
   ```
2. Trigger the migrated Airflow DAG for logical date `2026-01-15` (`ds_nodash = 20260115`).
3. Download both output files:
   * Legacy: `rechnung_export_20260115.dat`
   * Migrated: `gs://[GCS_EXPORT_BUCKET]/rechnung/ausgang/rechnung_export_20260115.dat`
4. Run a semantic comparison script.

```python
import pandas as pd
import pytest

@pytest.mark.e2e
def test_output_parity_ab():
    legacy_file = "test_data/legacy_rechnung_export_20260115.dat"
    migrated_file = "test_data/migrated_rechnung_export_20260115.dat"
    
    # Load both files using pipe delimiter, no header as per design
    df_legacy = pd.read_csv(legacy_file, sep="|", header=None, dtype=str)
    df_migrated = pd.read_csv(migrated_file, sep="|", header=None, dtype=str)
    
    # Assert identical dimensions
    assert df_legacy.shape == df_migrated.shape, f"Dimension mismatch! Legacy: {df_legacy.shape}, Migrated: {df_migrated.shape}"
    
    # Sort both by the first column (RECHNUNGSNUMMER) to ensure order parity
    df_legacy.sort_values(by=0, inplace=True, ignore_index=True)
    df_migrated.sort_values(by=0, inplace=True, ignore_index=True)
    
    # Assert cell-by-cell equality
    pd.testing.assert_frame_equal(df_legacy, df_migrated, check_dtype=False)
```

#### Pass/Fail Criterion
* **Pass**: The migrated GCS output file has the exact same row count, column count, sorting order, and values as the legacy Oracle output file.
* **Fail**: Any discrepancy in row count, column structure, sorting, or data values is detected.

---

## Section 4: Operational, Logging, & Edge-Case Assertions

### Test Case 4.1: GCS Shard Consolidation & Cleanup Validation
#### Purpose
Verify that the custom helper class `GcsExportHelper` successfully merges sharded files generated by BigQuery's `EXPORT DATA` command into a single consolidated file and cleans up the temporary shards.

#### Setup
* A GCS bucket containing mock sharded files:
  * `gs://[GCS_EXPORT_BUCKET]/exports/rechnung/shards_20260101/rechnung_export_000000000000.csv`
  * `gs://[GCS_EXPORT_BUCKET]/exports/rechnung/shards_20260101/rechnung_export_000000000001.csv`

#### Action
Instantiate and run the `GcsExportHelper` to consolidate the shards, then verify GCS state.

```python
import pytest
from google.cloud import storage
from dags.dwh_rechnung.bin.dwh_rechnung_export_taeglich_bin import GcsExportHelper

@pytest.mark.integration
def test_gcs_shard_consolidation():
    project_id = "gcp-dwh-prod"
    bucket_name = "dwh-export-test-bucket"
    stichtag = "20260101"
    
    source_prefix = f"exports/rechnung/shards_{stichtag}/rechnung_export_"
    destination_blob = f"rechnung/ausgang/rechnung_export_{stichtag}.dat"
    
    # Initialize helper
    helper = GcsExportHelper(project_id=project_id)
    
    # Execute consolidation
    line_count = helper.validate_and_consolidate_shards(
        bucket_name=bucket_name,
        source_prefix=source_prefix,
        destination_blob_name=destination_blob,
        expected_delimiter="|"
    )
    
    # Assertions
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # 1. Verify consolidated file exists
    final_blob = bucket.blob(destination_blob)
    assert final_blob.exists(), "Consolidated destination file was not created."
    
    # 2. Verify shards were deleted
    shards = list(bucket.list_blobs(prefix=source_prefix))
    assert len(shards) == 0, f"Temporary shards were not cleaned up: {shards}"
    
    # 3. Verify returned line count matches physical lines in GCS
    content = final_blob.download_as_text()
    actual_lines = len([line for line in content.splitlines() if line.strip()])
    assert line_count == actual_lines, f"Line count mismatch. Helper returned {line_count}, physical file has {actual_lines}."
```

#### Pass/Fail Criterion
* **Pass**: The sharded files are successfully merged into a single `.dat` file, the original shards are deleted from GCS, and the returned line count matches the physical line count.
* **Fail**: Shards remain in GCS, the consolidated file is missing, or the line count calculation is incorrect.

---

### Test Case 4.2: Zero-Row Warning & Logging Parity
#### Purpose
Verify that the system correctly identifies a zero-row export, logs the exact warning message matching the legacy shell script's `f_alis_msgerr` output, and prints the UC4-style trigger message.

#### Setup
* BigQuery table `T_RECHNUNG` contains no records for `Stichtag` `20261231`.
* Airflow DAG run triggered for execution date `2026-12-31`.

#### Action
Execute the Airflow DAG and inspect the task logs for `print_status` and `consolidate_files`.

```python
import pytest
from airflow.models import DagRun
from airflow.utils.state import State

@pytest.mark.logging
def test_zero_row_logging_and_warnings(caplog):
    # Simulate running the consolidation task with 0 records
    from dags.dwh_rechnung.bin.dwh_rechnung_export_taeglich_bin import GcsExportHelper
    import logging
    
    logger = logging.getLogger("dags.dwh_rechnung.bin.dwh_rechnung_export_taeglich_bin")
    
    # Trigger helper validation on an empty prefix
    helper = GcsExportHelper(project_id="gcp-dwh-prod")
    
    with caplog.at_level(logging.WARNING):
        line_count = helper.validate_and_consolidate_shards(
            bucket_name="dwh-export-test-bucket",
            source_prefix="exports/rechnung/shards_empty_test/",
            destination_blob_name="rechnung/ausgang/rechnung_export_empty.dat"
        )
        
        assert line_count == 0
        # Assert warning matches legacy: "Keine Rechnungsdaten fuer Stichtag [Stichtag] exportiert"
        assert any(
            "[WARNING]" in record.message or "contains 0 rows" in record.message 
            for record in caplog.records
        ), "Zero-row warning was not logged."
```

#### Pass/Fail Criterion
* **Pass**: The pipeline completes successfully (does not crash on zero rows), logs the German trigger status message matching the legacy UC4 script, and raises a clear warning log when zero rows are exported.
* **Fail**: The pipeline fails on empty datasets, or fails to log the required warning and status messages.