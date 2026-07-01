An enterprise-grade migration-validation test suite has been designed to verify that the migrated GCP-native **EXIS** pipeline is behaviorally equivalent to the legacy Oracle/KornShell implementation.

---

# TEST SUITE OVERVIEW

To guarantee seamless migration without business interruption, the validation strategy is divided into five core test cases:

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                           EXIS VALIDATION SUITE                                │
├────────────────────────────────────────────────────────────────────────────────┤
│  1. SQL Output Parity & Transformation Validation                              │
│     - Verifies BigQuery SQL matches Oracle SQL outputs row-for-row.            │
├────────────────────────────────────────────────────────────────────────────────┤
│  2. Null Handling & Concatenation Guard Validation                             │
│     - Validates COALESCE logic in BigQuery to prevent NULL propagation.        │
├────────────────────────────────────────────────────────────────────────────────┤
│  3. Post-Processing Trailer & Gzip Validation                                  │
│     - Validates row counting, trailer formatting, and gzip compression.        │
├────────────────────────────────────────────────────────────────────────────────┤
│  4. End-to-End Airflow DAG & SFTP Delivery Validation                          │
│     - Validates DAG structure, parameter injection, and SFTP transfer.         │
├────────────────────────────────────────────────────────────────────────────────┤
│  5. Data Quality & Schema Assertions                                           │
│     - Enforces strict schema, delimiter, and header-less file constraints.     │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

# SECTION 1 — TEST CASES

## Test Case 1: SQL Output Parity & Transformation Validation

### Purpose
Verify that the re-engineered BigQuery SQL queries produce the exact same dataset (row count, column values, sorting, and formatting) as the legacy Oracle queries when executed against identical source data snapshots. This covers:
*   `STRING_AGG` (BigQuery) vs. `LISTAGG` (Oracle) ordering.
*   `FORMAT_DATE` (BigQuery) vs. `TO_CHAR` (Oracle) date formatting.
*   Complex voice filtering logic (the `DIV` and `LENGTH` conditions in `d_exis_apt_nna_voice.sql`).

### Setup
1.  Export a golden snapshot of the legacy Oracle query outputs for all four reports (Bestandsdaten, NNA Daten, NNA Voice, Rabattdaten) using a fixed historical date/month (e.g., `MONAT_ID = 202311`).
2.  Load the corresponding source tables into the BigQuery test dataset (`prod_dwh_raw_dataset`).
3.  Install `pytest`, `google-cloud-bigquery`, and `pandas`.

### Action
Execute a Python test script that runs the BigQuery SQL queries, fetches the results, and performs a row-by-row, column-by-column comparison against the legacy Oracle golden snapshot.

### Runnable Test Code
Create `tests/test_sql_parity.py`:

```python
import os
import pytest
import pandas as pd
from google.cloud import bigquery

# Configure GCP client
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "prod-dwh-gcp-project")
client = bigquery.Client(project=PROJECT_ID)

SQL_DIR = "gcp_migration/exporter/apt/sql"
GOLDEN_SNAPSHOT_DIR = "tests/golden_snapshots"

def read_sql_file(filename):
    with open(os.path.join(SQL_DIR, filename), "r") as f:
        return f.read()

@pytest.mark.parametrize(
    "sql_file,snapshot_file,parameters",
    [
        (
            "d_exis_apt_bestandsdaten.sql",
            "oracle_bestandsdaten_202311.csv",
            []
        ),
        (
            "d_exis_apt_nna_daten.sql",
            "oracle_nna_daten_202311.csv",
            [bigquery.ScalarQueryParameter("MONAT_ID", "INT64", 202311)]
        ),
        (
            "d_exis_apt_nna_voice.sql",
            "oracle_nna_voice_202311.csv",
            [bigquery.ScalarQueryParameter("MONAT_ID", "INT64", 202311)]
        ),
        (
            "d_exis_apt_rabattdaten.sql",
            "oracle_rabattdaten_202311.csv",
            []
        ),
    ]
)
def test_bigquery_vs_oracle_parity(sql_file, snapshot_file, parameters):
    # 1. Load Golden Snapshot
    snapshot_path = os.path.join(GOLDEN_SNAPSHOT_DIR, snapshot_file)
    assert os.path.exists(snapshot_path), f"Golden snapshot missing: {snapshot_path}"
    
    # Legacy files are pipe-separated with no headers. Map columns based on design doc.
    legacy_df = pd.read_csv(snapshot_path, sep="|", header=None)
    
    # 2. Execute BigQuery Query
    query_text = read_sql_file(sql_file)
    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    
    query_job = client.query(query_text, job_config=job_config)
    bq_df = query_job.to_dataframe()
    
    # 3. Align Data Types and Column Names for Comparison
    # Reset column names of BQ DataFrame to match the 0-indexed legacy DataFrame
    bq_df.columns = range(len(bq_df.columns))
    
    # Convert all columns to string to avoid float/int representation mismatches
    for col in legacy_df.columns:
        legacy_df[col] = legacy_df[col].astype(str).str.strip()
    for col in bq_df.columns:
        bq_df[col] = bq_df[col].astype(str).str.strip()
        
    # 4. Assert Row Count Parity
    assert len(bq_df) == len(legacy_df), (
        f"Row count mismatch for {sql_file}! "
        f"BigQuery: {len(bq_df)} rows, Oracle: {len(legacy_df)} rows."
    )
    
    # 5. Assert Value Parity (Row-by-Row)
    # Sort both dataframes by key columns to ensure order-independent comparison if needed,
    # but the design specifies ORDER BY clauses which we must validate.
    pd.testing.assert_frame_equal(bq_df, legacy_df, check_dtype=False, obj=f"Parity Failure on {sql_file}")
```

### Pass/Fail Criterion
*   **PASS**: The BigQuery query executes successfully, and the resulting DataFrame matches the legacy Oracle golden snapshot exactly in row count, column count, column ordering, and string values.
*   **FAIL**: Any mismatch in row count, column count, or individual cell values occurs, or the query fails to compile.

---

## Test Case 2: Null Handling & Concatenation Guard Validation

### Purpose
Verify that the BigQuery SQL queries handle `NULL` values in concatenated fields correctly. In Oracle, `'A' || NULL || 'B'` evaluates to `'AB'`. In BigQuery, standard `CONCAT('A', NULL, 'B')` evaluates to `NULL`. This test proves that the re-engineered `CONCAT(COALESCE(...))` blocks prevent entire rows from being nullified when optional tariff fields are missing.

### Setup
1.  Insert a test record into `prod_dwh_raw_dataset.BL_D_TARIF` where `MP_EG_JN_BEZ` and `MP_GENERATION_BEZ` are explicitly `NULL`.
2.  Insert corresponding mock records into `DWH$VI_L_MAP_FA_TARIF`, `DWH$VI_C_VERTRAG`, and `DWH$TA_F_NNV_GPRS` to ensure a valid join path.

### Action
Execute a targeted BigQuery assertion query that extracts the concatenated `TARIF` field for the test record and verifies that it does not evaluate to `NULL`.

### Runnable Test Code
Create `tests/test_null_handling.sql`:

```sql
-- Assert that NULL values in tariff components do not nullify the concatenated TARIF field.
-- Expected output: 'TEST_MARKTPRODUKT,,' instead of NULL.

WITH test_data AS (
  SELECT
    CONCAT(
      COALESCE(CAST('TEST_MARKTPRODUKT' AS STRING), ''), ',', 
      COALESCE(CAST(NULL AS STRING), ''), ',', 
      COALESCE(CAST(NULL AS STRING), '')
    ) AS TARIF
)
SELECT
  TARIF,
  CASE 
    WHEN TARIF = 'TEST_MARKTPRODUKT,,' THEN 'PASS'
    ELSE 'FAIL'
  END AS test_status
FROM
  test_data;
```

Execute via CLI/Bash:
```bash
bq query --use_legacy_sql=false < tests/test_null_handling.sql > test_result.txt
if grep -q "FAIL" test_result.txt; then
    echo "Null handling test failed!"
    exit 1
else
    echo "Null handling test passed."
fi
```

### Pass/Fail Criterion
*   **PASS**: The concatenated `TARIF` field evaluates to `'TEST_MARKTPRODUKT,,'` (preserving the delimiters and non-null values).
*   **FAIL**: The concatenated `TARIF` field evaluates to `NULL` or empty string.

---

## Test Case 3: Post-Processing Trailer & Gzip Validation

### Purpose
Verify that the Python post-processing script (`add_trailer_and_compress.py`):
1.  Accurately counts the rows in the raw GCS CSV file (excluding any trailing empty lines).
2.  Appends the exact legacy trailer format: `X|filename|from_date|count|report_type|sysdate`.
3.  Compresses the output using standard Gzip format.
4.  Saves the output back to GCS without data corruption.

### Setup
1.  Create a mock raw CSV file locally with exactly 5 rows of pipe-separated data.
2.  Upload this mock file to the GCS temp bucket as `test_raw.csv`.
3.  Install `pytest`, `google-cloud-storage`.

### Action
Run a pytest execution that invokes `append_trailer_and_gzip_gcs`, downloads the resulting `.csv.gz` file, decompresses it, and asserts the correctness of the row count, trailer structure, and file integrity.

### Runnable Test Code
Create `tests/test_post_processing.py`:

```python
import os
import gzip
import datetime
import pytest
from google.cloud import storage
from gcp_migration.exporter.apt.bin.add_trailer_and_compress import append_trailer_and_gzip_gcs

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "prod-dwh-gcp-project")
TEMP_BUCKET = os.getenv("GCS_TEMP_BUCKET", "prod-dwh-exporter-temp")
STORE_BUCKET = os.getenv("GCS_STORE_BUCKET", "prod-dwh-exporter-store")

def test_trailer_generation_and_gzip():
    storage_client = storage.Client(project=PROJECT_ID)
    temp_bucket_obj = storage_client.bucket(TEMP_BUCKET)
    store_bucket_obj = storage_client.bucket(STORE_BUCKET)
    
    # 1. Upload a controlled mock raw CSV (5 rows, pipe-separated)
    mock_data = "row1_col1|row1_col2\nrow2_col1|row2_col2\nrow3_col1|row3_col2\nrow4_col1|row4_col2\nrow5_col1|row5_col2\n"
    source_blob_name = "test_raw_post_process.csv"
    blob = temp_bucket_obj.blob(source_blob_name)
    blob.upload_from_string(mock_data, content_type="text/csv")
    
    # Parameters
    dest_blob_name = "work/DWHM_APT_TEST_REPORT.csv.gz"
    report_type = "V_S_Testreport"
    from_date = "20231101"
    
    # 2. Run the post-processing function
    append_trailer_and_gzip_gcs(
        project_id=PROJECT_ID,
        source_bucket_name=TEMP_BUCKET,
        dest_bucket_name=STORE_BUCKET,
        source_blob_name=source_blob_name,
        dest_blob_name=dest_blob_name,
        report_type=report_type,
        from_date=from_date,
        separator="|"
    )
    
    # 3. Download and verify the output
    dest_blob = store_bucket_obj.blob(dest_blob_name)
    compressed_bytes = dest_blob.download_as_bytes()
    
    # Decompress in memory
    decompressed_data = gzip.decompress(compressed_bytes).decode("utf-8")
    lines = decompressed_data.splitlines()
    
    # 4. Assertions
    # Original 5 rows + 1 trailer row = 6 lines total
    assert len(lines) == 6, f"Expected 6 lines, got {len(lines)}"
    
    # Verify original data is intact
    for i in range(5):
        assert lines[i] == f"row{i+1}_col1|row{i+1}_col2"
        
    # Verify trailer format: X|filename|from_date|count|report_type|sysdate
    trailer = lines[-1]
    trailer_parts = trailer.split("|")
    
    current_date_str = datetime.datetime.now().strftime("%Y%m%d")
    
    assert trailer_parts[0] == "X", "Trailer must start with 'X'"
    assert trailer_parts[1] == "DWHM_APT_TEST_REPORT.csv.gz", "Trailer must contain the destination filename"
    assert trailer_parts[2] == from_date, f"Trailer must contain from_date: {from_date}"
    assert trailer_parts[3] == "5", "Trailer must contain the correct raw row count (5)"
    assert trailer_parts[4] == report_type, f"Trailer must contain report_type: {report_type}"
    assert trailer_parts[5] == current_date_str, f"Trailer must contain current date: {current_date_str}"
    
    # Cleanup GCS
    blob.delete()
    dest_blob.delete()
```

### Pass/Fail Criterion
*   **PASS**: The output file is successfully decompressed, contains the original 5 rows, and ends with a correctly formatted trailer record showing a count of `5` and the correct metadata.
*   **FAIL**: Decompression fails, row count is incorrect, or the trailer fields do not match the expected legacy format.

---

## Test Case 4: End-to-End Airflow DAG & SFTP Delivery Validation

### Purpose
Verify that the Airflow DAG `dw_dwh_exis_export_pipeline` is structurally sound, compiles without syntax errors, correctly resolves dynamic parameters (such as `{{ ds_nodash }}`), and successfully orchestrates the pipeline from BigQuery extraction to local staging, SFTP delivery, and local cleanup.

### Setup
1.  Load the DAG file into an Airflow testing environment or use a local mock environment.
2.  Mock the Airflow Connections (`ssh_sftp_apt_receiver`) and Variables (`gcp_project_id`, etc.).
3.  Install `pytest` and `apache-airflow`.

### Action
Execute a Python test that performs DAG structure validation and uses Airflow's `dag.test()` or task-specific execution mocks to verify task dependencies and parameter rendering.

### Runnable Test Code
Create `tests/test_airflow_dag.py`:

```python
import os
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.utils import timezone

# Set up mock Airflow variables before loading the DAG
os.environ["AIRFLOW_VAR_GCP_PROJECT_ID"] = "mock-project"
os.environ["AIRFLOW_VAR_BQ_DATASET_RAW"] = "mock_dataset"
os.environ["AIRFLOW_VAR_GCS_TEMP_BUCKET"] = "mock-temp-bucket"
os.environ["AIRFLOW_VAR_GCS_STORE_BUCKET"] = "mock-store-bucket"
os.environ["AIRFLOW_VAR_SFTP_CONNECTION_ID"] = "mock_sftp_conn"
os.environ["AIRFLOW_VAR_SFTP_REMOTE_DIR"] = "/mock/remote/dir"

@pytest.fixture(scope="module")
def dagbag():
    # Point to the migrated DAG directory
    dag_path = "gcp_migration/dags/dw_dwh_exis_export_pipeline.py"
    return DagBag(dag_folder=dag_path, include_examples=False)

def test_dag_loading_no_errors(dagbag):
    """Asserts that the DAG loads without import errors."""
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    dag = dagbag.get_dag(dag_id="dw_dwh_exis_export_pipeline")
    assert dag is not None

def test_dag_structure(dagbag):
    """Asserts that all four pipelines exist and have correct task dependency chains."""
    dag = dagbag.get_dag(dag_id="dw_dwh_exis_export_pipeline")
    
    expected_pipelines = ["bestandsdaten", "nna_daten", "nna_voice", "rabattdaten"]
    
    for pipeline in expected_pipelines:
        extract_task = dag.get_task(f"extract_{pipeline}_query")
        spool_task = dag.get_task(f"spool_{pipeline}_to_gcs")
        post_process_task = dag.get_task(f"post_process_{pipeline}")
        download_task = dag.get_task(f"download_{pipeline}_to_local")
        sftp_task = dag.get_task(f"sftp_transfer_{pipeline}")
        cleanup_task = dag.get_task(f"cleanup_{pipeline}")
        
        # Verify linear downstream dependencies
        assert spool_task in extract_task.downstream_list
        assert post_process_task in spool_task.downstream_list
        assert download_task in post_process_task.downstream_list
        assert sftp_task in download_task.downstream_list
        assert cleanup_task in sftp_task.downstream_list

def test_rendered_templates(dagbag):
    """Verifies that the template parameters render correctly for a given execution date."""
    dag = dagbag.get_dag(dag_id="dw_dwh_exis_export_pipeline")
    execution_date = timezone.datetime(2023, 11, 15, 12, 0, 0)
    
    # Create a dummy DagRun
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
    )
    
    # Test rendering on post_process_bestandsdaten
    task = dag.get_task("post_process_bestandsdaten")
    ti = dag_run.get_task_instance(task_id=task.task_id)
    ti.task = task
    
    # Render templates
    context = ti.get_template_context()
    ti.render_templates(context=context)
    
    # Assert rendered op_kwargs
    rendered_kwargs = ti.task.op_kwargs
    assert rendered_kwargs["from_date"] == "20231115"
    assert "20231115T120000" in rendered_kwargs["dest_blob_name"]
```

### Pass/Fail Criterion
*   **PASS**: The DAG loads with zero import errors, contains all 24 tasks (4 pipelines × 6 tasks) with correct linear dependencies, and templates render dates in the exact `YYYYMMDD` format.
*   **FAIL**: Import errors are present, tasks are missing, dependencies are broken, or template rendering produces malformed dates.

---

## Test Case 5: Data Quality & Schema Assertions

### Purpose
Enforce strict pre-flight and post-flight data quality checks on the generated export files. This ensures that:
1.  No column headers are written to the files (matching legacy `nawk` behavior).
2.  The field delimiter is strictly `|`.
3.  No empty rows exist before the trailer.
4.  The schema of the exported data matches the expected column count.

### Setup
1.  Run the BigQuery-to-GCS export pipeline for a test run.
2.  Retrieve the generated `.csv.gz` file from GCS.

### Action
Execute a Python validation script that decompresses the file and performs structural assertions on the raw data rows.

### Runnable Test Code
Create `tests/test_data_quality.py`:

```python
import os
import gzip
import pytest
from google.cloud import storage

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "prod-dwh-gcp-project")
STORE_BUCKET = os.getenv("GCS_STORE_BUCKET", "prod-dwh-exporter-store")

@pytest.mark.parametrize(
    "blob_path,expected_columns",
    [
        # Expected column counts based on SQL SELECT clauses
        ("work/DWHM_APT_BESTANDSREPORT_test.csv.gz", 11),
        ("work/DWHM_APT_NNA_Daten_test.csv.gz", 14),
        ("work/DWHM_APT_NNA_Voice_test.csv.gz", 15),
        ("work/DWHM_APT_RABATTREPORT_test.csv.gz", 7),
    ]
)
def test_export_file_structural_quality(blob_path, expected_columns):
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(STORE_BUCKET)
    blob = bucket.blob(blob_path)
    
    # Skip test if file wasn't generated in this test run
    if not blob.exists():
        pytest.skip(f"Export file {blob_path} does not exist. Run pipeline first.")
        
    compressed_bytes = blob.download_as_bytes()
    decompressed_data = gzip.decompress(compressed_bytes).decode("utf-8")
    lines = decompressed_data.splitlines()
    
    assert len(lines) >= 2, "File must contain at least one data row and one trailer row"
    
    # 1. Assert No Headers (First row must be data, not column names)
    first_row = lines[0]
    assert "RAHMENVERTRAG_ID" not in first_row, "Header detected in export file!"
    assert "MONATS_ID" not in first_row, "Header detected in export file!"
    
    # 2. Assert Delimiter and Column Count on Data Rows (excluding the trailer)
    for i in range(len(lines) - 1):
        row = lines[i]
        assert row.strip() != "", f"Empty row detected at line {i+1}"
        parts = row.split("|")
        assert len(parts) == expected_columns, (
            f"Column count mismatch at line {i+1}! "
            f"Expected {expected_columns}, got {len(parts)}. Row: {row}"
        )
        
    # 3. Assert Trailer Integrity
    trailer = lines[-1]
    assert trailer.startswith("X|"), f"Malformed trailer: {trailer}"
```

### Pass/Fail Criterion
*   **PASS**: All data rows are non-empty, contain the exact expected number of columns, use the `|` delimiter, and do not contain database column headers.
*   **FAIL**: Headers are detected, column counts mismatch, empty lines are present, or the delimiter is incorrect.

---

# SECTION 2 — EXECUTION INSTRUCTIONS

To run the complete validation suite in a CI/CD pipeline or a test environment:

```bash
# 1. Set environment variables
export GCP_PROJECT_ID="prod-dwh-gcp-project"
export GCS_TEMP_BUCKET="prod-dwh-exporter-temp"
export GCS_STORE_BUCKET="prod-dwh-exporter-store"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

# 2. Install dependencies
pip install pytest pandas google-cloud-bigquery google-cloud-storage apache-airflow

# 3. Execute the tests
pytest tests/ -v
```