# Migration Validation Test Suite: DW.DWH_ALL_TYPES_MASTER

This document defines the migration-validation test suite for the job `DW.DWH_ALL_TYPES_MASTER`. These tests are designed to prove behavioral equivalence between the legacy UC4/Oracle/AWK/KSH stack and the migrated Apache Airflow/Google Cloud Dataproc/BigQuery/Python stack.

---

## Test Case 1: Airflow DAG Structure and Variable Validation

### Purpose
Verify that the migrated Airflow DAG `dw_dwh_all_types_master` is structurally sound, contains the correct tasks, preserves the legacy execution order, and correctly maps all environment variables and parameters.

### Setup
1. Ensure the Airflow DAG file `dw_dwh_all_types_master.py` is placed in the Airflow DAGs directory.
2. Configure the following Airflow Variables in the test environment:
   * `GCP_PROJECT` = `test-gcp-project`
   * `GCP_REGION` = `us-central1`
   * `GCP_CLUSTER_NAME` = `test-dataproc-cluster`
   * `GCS_BUCKET` = `test-gcs-bucket`
   * `BQ_DATASET` = `test_dataset`

### Action
Run a pytest suite to parse the DAG, validate its structure, check task dependencies, and verify that environment variables are correctly injected into the tasks.

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables():
    # Mock Airflow Variables
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCP_REGION", "us-central1")
    Variable.set("GCP_CLUSTER_NAME", "test-dataproc-cluster")
    Variable.set("GCS_BUCKET", "test-gcs-bucket")
    Variable.set("BQ_DATASET", "test_dataset")
    yield
    # Cleanup
    for var in ["GCP_PROJECT", "GCP_REGION", "GCP_CLUSTER_NAME", "GCS_BUCKET", "BQ_DATASET"]:
        Variable.delete(var)

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_all_types_master")
    assert dag_bag.import_errors == {}
    assert dag is not None

def test_dag_structure_and_dependencies():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_all_types_master")
    
    # Verify task existence
    assert dag.has_task("all_types_graph")
    assert dag.has_task("task_r_all_types_master")
    
    # Verify execution order: all_types_graph >> task_r_all_types_master
    all_types_graph_task = dag.get_task("all_types_graph")
    task_r_all_types_master = dag.get_task("task_r_all_types_master")
    
    assert task_r_all_types_master in all_types_graph_task.downstream_list
    assert all_types_graph_task in task_r_all_types_master.upstream_list

def test_task_parameter_injection():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_all_types_master")
    
    # Verify BashOperator environment variables
    bash_task = dag.get_task("task_r_all_types_master")
    env = bash_task.env
    
    assert env["DWH_JOB_KENNUNG"] == "ALL_TYPES_MASTER"
    assert env["ALL_DIR_ROOT"] == "/home/airflow/gcs/dags/isall"
    assert env["GCP_PROJECT"] == "test-gcp-project"
    assert env["BQ_DATASET"] == "test_dataset"
```

### Pass/Fail Criterion
* **Pass**: The DAG parses with zero import errors, contains exactly the two expected tasks in the correct dependency order, and successfully resolves all environment variables.
* **Fail**: Any import errors are raised, tasks are missing or misconfigured, or environment variables do not match the expected values.

---

## Test Case 2: AWK-to-Python Transformation Parity (Valid Records)

### Purpose
Prove that the migrated Python script `k_all_types_transform.py` produces output identical to the legacy `k_all_types_transform.awk` script when processing valid 12-field records.

### Setup
1. Prepare a test input file `valid_input.csv` containing records with exactly 12 fields separated by semicolons. Include edge cases such as empty fields and trailing semicolons.
2. Ensure the migrated script `isall/aufbereitung/awk/k_all_types_transform.py` is executable.

### Action
Execute the migrated Python script using `valid_input.csv` as input, and compare the output against the expected legacy output format (each line prefixed with `D;`).

```python
import subprocess
import tempfile
import os

def test_awk_transformation_valid_records():
    # 12-field records (including empty fields and trailing semicolons)
    input_data = (
        "1;2;3;4;5;6;7;8;9;10;11;12\n"
        "val1;val2;;val4;val5;val6;val7;val8;val9;val10;val11;val12\n"
        "a;b;c;d;e;f;g;h;i;j;k;\n"  # 12 fields (last field is empty string after trailing semicolon)
    )
    
    expected_output = (
        "D;1;2;3;4;5;6;7;8;9;10;11;12\n"
        "D;val1;val2;;val4;val5;val6;val7;val8;val9;val10;val11;val12\n"
        "D;a;b;c;d;e;f;g;h;i;j;k;\n"
    )
    
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as infile, \
         tempfile.NamedTemporaryFile(mode="r", delete=False) as outfile:
        
        infile.write(input_data)
        infile.close()
        outfile.close()
        
        script_path = "isall/aufbereitung/awk/k_all_types_transform.py"
        
        # Run the migrated Python script
        result = subprocess.run(
            ["python3", script_path, infile.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Cleanup temporary files
        os.remove(infile.name)
        os.remove(outfile.name)
        
        # Assertions
        assert result.returncode == 0, f"Script failed with stderr: {result.stderr}"
        assert result.stdout == expected_output, "Output mismatch between legacy AWK logic and Python"
```

### Pass/Fail Criterion
* **Pass**: The Python script exits with code `0` and produces output that matches the expected legacy format character-for-character.
* **Fail**: The script exits with a non-zero code, raises an exception, or produces output that does not match the expected format.

---

## Test Case 3: AWK-to-Python Transformation Parity (Invalid Records & Error Handling)

### Purpose
Prove that the migrated Python script `k_all_types_transform.py` correctly handles malformed records (field count $\neq 12$) by printing the exact legacy error message to standard output and terminating with exit code `2`.

### Setup
1. Prepare an input file `invalid_input.csv` containing at least one record with fewer than 12 fields and one with more than 12 fields.

### Action
Execute the migrated Python script with the malformed input and verify the exit code and output stream.

```python
import subprocess
import tempfile
import os

def test_awk_transformation_invalid_records():
    # Row 1 has 12 fields (valid), Row 2 has 11 fields (invalid)
    input_data = (
        "1;2;3;4;5;6;7;8;9;10;11;12\n"
        "1;2;3;4;5;6;7;8;9;10;11\n"
    )
    
    expected_stdout = (
        "D;1;2;3;4;5;6;7;8;9;10;11;12\n"
        "Error: Incorrect nos of Fields \n"
    )
    
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as infile:
        infile.write(input_data)
        infile.close()
        
        script_path = "isall/aufbereitung/awk/k_all_types_transform.py"
        
        # Run the migrated Python script
        result = subprocess.run(
            ["python3", script_path, infile.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        os.remove(infile.name)
        
        # Assertions
        assert result.returncode == 2, f"Expected exit code 2, got {result.returncode}"
        assert result.stdout == expected_stdout, "Error output mismatch"
```

### Pass/Fail Criterion
* **Pass**: The script terminates immediately upon reaching the invalid record, exits with code `2`, and prints `"Error: Incorrect nos of Fields "` to standard output (matching legacy AWK behavior).
* **Fail**: The script exits with a code other than `2`, processes the invalid record without failing, or prints a different error message.

---

## Test Case 4: BigQuery SQL Refresh Logic (`d_all_types.sql`)

### Purpose
Verify that the migrated BigQuery SQL script `d_all_types.sql` correctly truncates the target table `sof$ta_all_types` and populates it with records from `cds$ta_all_types_raw` where `status = 'READY'`.

### Setup
1. Ensure the target BigQuery dataset exists.
2. Create and populate the source table `cds$ta_all_types_raw` with test data containing various statuses (`READY`, `PENDING`, `ERROR`).
3. Populate the target table `sof$ta_all_types` with dummy historical records to verify truncation.

### Action
Execute the BigQuery SQL script `d_all_types.sql` using the BigQuery Python client, passing the project and dataset parameters, and assert the final state of the target table.

```python
import pytest
from google.cloud import bigquery
import os

@pytest.fixture
def bq_client():
    project = os.environ.get("GCP_PROJECT", "test-gcp-project")
    return bigquery.Client(project=project)

def test_bigquery_sql_refresh(bq_client):
    project = bq_client.project
    dataset = os.environ.get("BQ_DATASET", "test_dataset")
    
    target_table_id = f"{project}.{dataset}.sof$ta_all_types"
    source_table_id = f"{project}.{dataset}.cds$ta_all_types_raw"
    
    # 1. Setup: Recreate and populate tables
    bq_client.query(f"CREATE OR REPLACE TABLE `{target_table_id}` (all_types_id INT64, source_system STRING, processed_at DATETIME)").result()
    bq_client.query(f"CREATE OR REPLACE TABLE `{source_table_id}` (all_types_id INT64, source_system STRING, status STRING)").result()
    
    # Insert historical data to target (to verify truncation)
    bq_client.query(f"INSERT INTO `{target_table_id}` VALUES (999, 'OLD_SYS', CURRENT_DATETIME())").result()
    
    # Insert source data
    source_data_query = f"""
        INSERT INTO `{source_table_id}` (all_types_id, source_system, status) VALUES
        (1, 'SYS_A', 'READY'),
        (2, 'SYS_B', 'PENDING'),
        (3, 'SYS_C', 'READY'),
        (4, 'SYS_D', 'ERROR')
    """
    bq_client.query(source_data_query).result()
    
    # 2. Action: Run the migrated SQL script
    sql_script_path = "isall/aufbereitung/sql/d_all_types.sql"
    with open(sql_script_path, "r") as f:
        sql_text = f.read()
        
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gcp_project", "STRING", project),
            bigquery.ScalarQueryParameter("bq_dataset", "STRING", dataset)
        ]
    )
    
    query_job = bq_client.query(sql_text, job_config=job_config)
    query_job.result()  # Wait for execution
    
    # 3. Assertions
    # Verify target table contents
    results_query = f"SELECT all_types_id, source_system, processed_at FROM `{target_table_id}` ORDER BY all_types_id"
    rows = list(bq_client.query(results_query).result())
    
    # Verify old record (999) was truncated
    assert len(rows) == 2, f"Expected 2 rows after refresh, found {len(rows)}"
    
    # Verify only 'READY' records were inserted
    assert rows[0]["all_types_id"] == 1
    assert rows[0]["source_system"] == "SYS_A"
    assert isinstance(rows[0]["processed_at"], datetime.datetime)
    
    assert rows[1]["all_types_id"] == 3
    assert rows[1]["source_system"] == "SYS_C"
```

### Pass/Fail Criterion
* **Pass**: The target table `sof$ta_all_types` is successfully truncated (the historical record `999` is removed), and only the records with `status = 'READY'` (IDs `1` and `3`) are loaded with a valid `processed_at` timestamp.
* **Fail**: The historical record remains (truncation failed), incorrect records are loaded, or the query execution fails.

---

## Test Case 5: Master Script Integration (`r_all_types_master.py`)

### Purpose
Verify that the master orchestrator script `r_all_types_master.py` successfully coordinates the BigQuery SQL execution and the AWK-replacement Python execution, producing the correct log file and output file.

### Setup
1. Set up the local environment variables:
   * `ALL_DIR_ROOT` = `/tmp/test_isall`
   * `DW_ORAUSER` = `dummy_user`
   * `GCP_PROJECT` = `test-gcp-project`
   * `BQ_DATASET` = `test_dataset`
2. Create the directory structure under `/tmp/test_isall`:
   * `/tmp/test_isall/aufbereitung/sql/`
   * `/tmp/test_isall/aufbereitung/awk/`
   * `/tmp/test_isall/data/`
3. Copy the migrated SQL script `d_all_types.sql` and the Python script `k_all_types_transform.py` to their respective directories under `/tmp/test_isall`.
4. Create a valid 12-field CSV file at `/tmp/test_isall/data/all_types_export.csv`.
5. Mock the BigQuery client call in `r_all_types_master.py` to prevent actual network calls during integration testing, or run against a sandbox GCP project.

### Action
Execute `r_all_types_master.py` and verify the creation of the log file, the output file, and the console output.

```python
import os
import sys
import shutil
import subprocess
import datetime
import pytest
from unittest.mock import patch

@pytest.fixture
def setup_test_environment():
    test_root = "/tmp/test_isall"
    os.makedirs(os.path.join(test_root, "aufbereitung", "sql"), exist_ok=True)
    os.makedirs(os.path.join(test_root, "aufbereitung", "awk"), exist_ok=True)
    os.makedirs(os.path.join(test_root, "data"), exist_ok=True)
    
    # Copy scripts to test root
    shutil.copy("isall/aufbereitung/sql/d_all_types.sql", os.path.join(test_root, "aufbereitung", "sql", "d_all_types.sql"))
    shutil.copy("isall/aufbereitung/awk/k_all_types_transform.py", os.path.join(test_root, "aufbereitung", "awk", "k_all_types_transform.py"))
    
    # Create mock input CSV
    with open(os.path.join(test_root, "data", "all_types_export.csv"), "w") as f:
        f.write("1;2;3;4;5;6;7;8;9;10;11;12\n")
        
    # Set environment variables
    os.environ["ALL_DIR_ROOT"] = test_root
    os.environ["DW_ORAUSER"] = "dummy_user"
    os.environ["GCP_PROJECT"] = "test-gcp-project"
    os.environ["BQ_DATASET"] = "test_dataset"
    
    yield test_root
    
    # Cleanup
    shutil.rmtree(test_root)

@patch("google.cloud.bigquery.Client")
def test_master_script_execution(mock_bq_client, setup_test_environment):
    test_root = setup_test_environment
    
    # Mock BigQuery Client and Query Job
    mock_client_instance = mock_bq_client.return_value
    mock_query_job = mock_client_instance.query.return_value
    mock_query_job.result.return_value = True
    
    # Run the master script
    script_path = "isall/aufbereitung/bin/r_all_types_master.py"
    result = subprocess.run(
        ["python3", script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Assertions
    assert result.returncode == 0, f"Master script failed: {result.stderr}"
    
    # Verify console output contains legacy German print literals
    assert "----Starte SQL-Refresh----" in result.stdout
    assert "----Starte AWK-Nachbearbeitung----" in result.stdout
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in result.stdout
    
    # Verify log file creation and content
    v_sysdate = datetime.datetime.now().strftime("%d%m%Y")
    log_file_path = os.path.join(test_root, "protokoll", f"all_types_master_{v_sysdate}.log")
    assert os.path.exists(log_file_path)
    
    with open(log_file_path, "r") as log_f:
        log_content = log_f.read()
        assert "JobKennung: 'ALL_TYPES_MASTER'" in log_content
        assert "----Starte SQL-Refresh----" in log_content
        assert "----Starte AWK-Nachbearbeitung----" in log_content
        assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in log_content
        
    # Verify output file creation and content
    output_file_path = os.path.join(test_root, "data", "all_types_export.out")
    assert os.path.exists(output_file_path)
    with open(output_file_path, "r") as out_f:
        out_content = out_f.read()
        assert out_content == "D;1;2;3;4;5;6;7;8;9;10;11;12\n"
```

### Pass/Fail Criterion
* **Pass**: The master script runs successfully (exit code `0`), calls BigQuery, executes the AWK-replacement Python script, generates the expected output file `all_types_export.out` with prefixed records, and writes a complete execution log containing all legacy German print statements.
* **Fail**: The script exits with a non-zero code, fails to generate the log or output files, or misses the required log statements.