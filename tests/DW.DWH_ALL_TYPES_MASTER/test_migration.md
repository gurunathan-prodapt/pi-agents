# Migration Validation Test Suite: `DW.DWH_ALL_TYPES_MASTER`

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy UC4/Oracle/AWK/KSH pipeline and the migrated Apache Airflow/BigQuery/Python pipeline for **`DW.DWH_ALL_TYPES_MASTER`**.

---

## Test Case 1: AWK-to-Python Transformation Parity (`k_all_types_transform.py`)

### Purpose
Verify that the migrated Python script `k_all_types_transform.py` behaves identically to the legacy AWK script `k_all_types_transform.awk`. It must validate that every record contains exactly 12 fields, prepend `"D;"` to valid records, and terminate immediately with exit code `2` and the exact error message on any malformed record.

### Setup
1. Create a temporary directory containing three test files:
   - `valid_input.csv`: Contains rows with exactly 12 semicolon-separated fields.
   - `invalid_short.csv`: Contains at least one row with fewer than 12 fields.
   - `invalid_long.csv`: Contains at least one row with more than 12 fields.
2. Ensure the migrated script `isall/aufbereitung/awk/k_all_types_transform.py` is executable.

### Action
Run a automated test suite using `pytest` to execute the Python script against the test files and assert standard output, standard error, and exit codes.

```python
import subprocess
import sys
import os
import pytest

# Path to the migrated script
MIGRATED_SCRIPT = "isall/aufbereitung/awk/k_all_types_transform.py"

@pytest.fixture
def setup_test_files(tmp_path):
    valid_file = tmp_path / "valid_input.csv"
    invalid_short_file = tmp_path / "invalid_short.csv"
    invalid_long_file = tmp_path / "invalid_long.csv"
    
    # 12 fields (11 semicolons)
    valid_file.write_text(
        "val1;val2;val3;val4;val5;val6;val7;val8;val9;val10;val11;val12\n"
        "a;b;c;d;e;f;g;h;i;j;k;l\n"
    )
    
    # 11 fields (10 semicolons) - Invalid
    invalid_short_file.write_text(
        "val1;val2;val3;val4;val5;val6;val7;val8;val9;val10;val11\n"
    )
    
    # 13 fields (12 semicolons) - Invalid
    invalid_long_file.write_text(
        "val1;val2;val3;val4;val5;val6;val7;val8;val9;val10;val11;val12;val13\n"
    )
    
    return {
        "valid": str(valid_file),
        "short": str(invalid_short_file),
        "long": str(invalid_long_file)
    }

def test_valid_file_processing(setup_test_files):
    # Action: Run script with valid file
    result = subprocess.run(
        [sys.executable, MIGRATED_SCRIPT, setup_test_files["valid"]],
        capture_output=True,
        text=True
    )
    
    # Pass/Fail Criteria
    assert result.returncode == 0
    assert result.stdout.startswith("D;val1;val2;")
    assert len(result.stdout.splitlines()) == 2
    for line in result.stdout.splitlines():
        assert line.startswith("D;")

def test_invalid_short_file_processing(setup_test_files):
    # Action: Run script with short file
    result = subprocess.run(
        [sys.executable, MIGRATED_SCRIPT, setup_test_files["short"]],
        capture_output=True,
        text=True
    )
    
    # Pass/Fail Criteria
    assert result.returncode == 2
    # Note: AWK script prints "Error: Incorrect nos of Fields " with a trailing space to stdout
    assert "Error: Incorrect nos of Fields " in result.stdout

def test_invalid_long_file_processing(setup_test_files):
    # Action: Run script with long file
    result = subprocess.run(
        [sys.executable, MIGRATED_SCRIPT, setup_test_files["long"]],
        capture_output=True,
        text=True
    )
    
    # Pass/Fail Criteria
    assert result.returncode == 2
    assert "Error: Incorrect nos of Fields " in result.stdout
```

### Pass/Fail Criterion
- **Pass**: The script returns exit code `0` and prepends `"D;"` to all lines for valid 12-field inputs. It returns exit code `2` and outputs `"Error: Incorrect nos of Fields \n"` to `stdout` for any line not containing exactly 12 fields.
- **Fail**: Any non-zero exit code on valid data, exit code other than `2` on invalid data, or mismatch in the output format/error message.

---

## Test Case 2: BigQuery SQL Transformation Correctness (`d_all_types.sql`)

### Purpose
Verify that the migrated BigQuery SQL script `d_all_types.sql` correctly truncates the target table `sof$ta_all_types`, filters raw records from `cds$ta_all_types_raw` where `status = 'READY'`, and inserts them with the current system timestamp.

### Setup
1. Create a test dataset in BigQuery (e.g., `dwh_validation_test`).
2. Create the source table `cds$ta_all_types_raw` and target table `sof$ta_all_types` matching the production schema.
3. Populate `cds$ta_all_types_raw` with mock records:
   - 3 records with `status = 'READY'`
   - 2 records with `status = 'PENDING'`
   - 1 record with `status = 'FAILED'`
4. Populate `sof$ta_all_types` with 5 dummy records (to test truncation).

### Action
Execute the migrated SQL script using the BigQuery Python client, passing the test project and dataset as query parameters.

```python
import time
from google.cloud import bigquery

def test_sql_transformation(gcp_project, bq_dataset):
    client = bigquery.Client(project=gcp_project)
    
    # 1. Setup: Populate raw source table
    raw_table_id = f"{gcp_project}.{bq_dataset}.cds$ta_all_types_raw"
    target_table_id = f"{gcp_project}.{bq_dataset}.sof$ta_all_types"
    
    # Clear tables first
    client.query(f"TRUNCATE TABLE `{raw_table_id}`").result()
    client.query(f"TRUNCATE TABLE `{target_table_id}`").result()
    
    # Insert mock source data
    setup_raw_sql = f"""
    INSERT INTO `{raw_table_id}` (all_types_id, source_system, status) VALUES
    (101, 'SYS_A', 'READY'),
    (102, 'SYS_B', 'READY'),
    (103, 'SYS_C', 'PENDING'),
    (104, 'SYS_D', 'READY'),
    (105, 'SYS_E', 'FAILED')
    """
    client.query(setup_raw_sql).result()
    
    # Insert dummy target data to verify truncation
    client.query(f"INSERT INTO `{target_table_id}` (all_types_id, source_system, processed_at) VALUES (999, 'OLD', CURRENT_DATETIME())").result()

    # 2. Action: Read and execute the migrated SQL script
    with open("isall/aufbereitung/sql/d_all_types.sql", "r") as f:
        sql_script = f.read()
        
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gcp_project", "STRING", gcp_project),
            bigquery.ScalarQueryParameter("bq_dataset", "STRING", bq_dataset),
        ]
    )
    
    query_job = client.query(sql_script, job_config=job_config)
    query_job.result()  # Wait for execution
    
    # 3. Assertions
    # Verify target table contents
    target_rows = list(client.query(f"SELECT * FROM `{target_table_id}` ORDER BY all_types_id").result())
    
    # Pass/Fail Criteria
    assert len(target_rows) == 3, f"Expected 3 rows, got {len(target_rows)}"
    
    # Verify truncation (id 999 should be gone)
    ids = [row.all_types_id for row in target_rows]
    assert 999 not in ids, "Target table was not truncated before insertion"
    
    # Verify correct records were loaded
    assert ids == [101, 102, 104]
    
    # Verify processed_at timestamp is recent (within last 2 minutes)
    for row in target_rows:
        time_diff = datetime.utcnow() - row.processed_at.to_pydatetime()
        assert time_diff.total_seconds() < 120, "processed_at timestamp is not current"
```

### Pass/Fail Criterion
- **Pass**: The target table `sof$ta_all_types` is completely cleared of pre-existing data, and contains exactly the 3 records from the raw table that had `status = 'READY'`. The `processed_at` column contains the current system timestamp.
- **Fail**: Pre-existing target data remains, non-READY records are loaded, READY records are missing, or the execution fails with a BigQuery scripting exception.

---

## Test Case 3: Master Orchestrator Integration & Logging (`r_all_types_master.py`)

### Purpose
Verify that the master Python wrapper script `r_all_types_master.py` correctly orchestrates the execution of the BigQuery SQL script and the AWK-translated Python script in sequence, writes logs matching the legacy format, and propagates exit codes correctly.

### Setup
1. Set up environment variables:
   - `ALL_DIR_ROOT`: Path to a temporary workspace directory.
   - `GCP_PROJECT`: Target GCP Project ID.
   - `BQ_DATASET`: Target BigQuery Dataset ID.
2. Create the directory structure under `ALL_DIR_ROOT`:
   - `aufbereitung/sql/d_all_types.sql` (Migrated SQL script)
   - `aufbereitung/awk/k_all_types_transform.py` (Migrated AWK script)
   - `data/all_types_export.csv` (Input CSV file with 12-field rows)
   - `protokoll/` (Log directory)

### Action
Execute `r_all_types_master.py` via subprocess and validate the generated log file and output files.

```python
import os
import subprocess
import sys
from datetime import datetime
import pytest

def test_master_orchestrator_happy_path(tmp_path, monkeypatch):
    # Setup workspace
    all_dir_root = tmp_path / "isall"
    sql_dir = all_dir_root / "aufbereitung" / "sql"
    awk_dir = all_dir_root / "aufbereitung" / "awk"
    data_dir = all_dir_root / "data"
    
    os.makedirs(sql_dir, exist_ok=True)
    os.makedirs(awk_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    
    # Copy migrated scripts to workspace
    with open("isall/aufbereitung/sql/d_all_types.sql", "r") as src, open(sql_dir / "d_all_types.sql", "w") as dst:
        dst.write(src.read())
    with open("isall/aufbereitung/awk/k_all_types_transform.py", "r") as src, open(awk_dir / "k_all_types_transform.py", "w") as dst:
        dst.write(src.read())
        
    # Create valid input CSV
    (data_dir / "all_types_export.csv").write_text(
        "1;2;3;4;5;6;7;8;9;10;11;12\n"
        "a;b;c;d;e;f;g;h;i;j;k;l\n"
    )
    
    # Set environment variables
    env = os.environ.copy()
    env["ALL_DIR_ROOT"] = str(all_dir_root)
    env["GCP_PROJECT"] = "mock-project"  # Use mock or actual project
    env["BQ_DATASET"] = "mock_dataset"
    
    # Action: Run master script
    result = subprocess.run(
        [sys.executable, "isall/aufbereitung/bin/r_all_types_master.py"],
        env=env,
        capture_output=True,
        text=True
    )
    
    # Pass/Fail Criteria
    assert result.returncode == 0, f"Master script failed: {result.stderr}"
    
    # Verify output file exists and is processed
    output_file = data_dir / "all_types_export.out"
    assert output_file.exists()
    assert output_file.read_text().startswith("D;1;2;")
    
    # Verify log file creation and content
    v_sysdate = datetime.now().strftime("%d%m%Y")
    log_file = all_dir_root / "protokoll" / f"all_types_master_{v_sysdate}.log"
    assert log_file.exists()
    
    log_content = log_file.read_text()
    assert "----Starte SQL-Refresh----" in log_content
    assert "----Starte AWK-Nachbearbeitung----" in log_content
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in log_content
```

### Pass/Fail Criterion
- **Pass**: The master script exits with code `0`. The output file `all_types_export.out` is successfully created with `"D;"` prefixes. The log file is created in the correct directory with the exact German logging literals preserved.
- **Fail**: The script exits with a non-zero code, the log file is missing or lacks the required headers/footers, or the AWK step fails to generate the output file.

---

## Test Case 4: Airflow DAG Orchestration & Dependency Validation

### Purpose
Verify that the Airflow DAG `dw_dwh_all_types_master` is syntactically correct, loads without errors, maps variables correctly, and enforces the exact sequential execution order: `submit_pyspark_graph` followed by `post_processing_master`.

### Setup
An Airflow execution environment or a local Python environment with `apache-airflow` installed.

### Action
Run programmatic DAG validation tests using the Airflow DAGBag API.

```python
from airflow.models import DagBag, Variable
from airflow.utils.dag_cycle_tester import check_cycle
import pytest

@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    # Mock Airflow variables required during DAG parsing
    variables = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3",
        "DATAPROC_CLUSTER": "test-dataproc-cluster",
        "GCS_BUCKET": "test-gcs-bucket",
        "BQ_DATASET": "test_bq_dataset",
        "ALL_DIR_ROOT": "/isall"
    }
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_loading_and_structure():
    # Action: Load DAG
    dagbag = DagBag(dag_folder="DWH_ALL_TYPES_JOB", include_examples=False)
    dag_id = "dw_dwh_all_types_master"
    
    dag = dagbag.get_dag(dag_id)
    
    # Pass/Fail Criteria
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    assert dag is not None, f"Failed to load DAG {dag_id}"
    
    # Verify no cycles
    check_cycle(dag)
    
    # Verify Task IDs and Types
    assert len(dag.tasks) == 2, "DAG must contain exactly 2 tasks"
    
    task_pyspark = dag.get_task("jobs_unix_dw_dwh_all_types_master")
    task_post_proc = dag.get_task("post_processing_master")
    
    assert task_pyspark.__class__.__name__ == "DataprocSubmitJobOperator"
    assert task_post_proc.__class__.__name__ == "BashOperator"
    
    # Verify Sequential Execution Order (submit_pyspark_graph >> post_processing_master)
    assert task_post_proc in task_pyspark.downstream_list
    assert task_pyspark in task_post_proc.upstream_list
    
    # Verify Environment Variable Propagation to BashOperator
    env_vars = task_post_proc.env
    assert env_vars["DWH_JOB_KENNUNG"] == "ALL_TYPES_MASTER"
    assert env_vars["GCP_PROJECT"] == "test-gcp-project"
    assert env_vars["BQ_DATASET"] == "test_bq_dataset"
```

### Pass/Fail Criterion
- **Pass**: The DAG loads with zero import errors, contains no cycles, has exactly the two specified tasks, and strictly enforces the sequential dependency `submit_pyspark_graph >> post_processing_master`.
- **Fail**: Any DAG import errors are raised, tasks are missing, dependencies are incorrect, or required environment variables are not mapped to the BashOperator.