Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow/PySpark code is behaviorally equivalent to the legacy UC4/Ab Initio job `DW.DWH_ABPZ_KKM_AIL_AGENT`.

---

# Test Suite Overview: `DW.DWH_ABPZ_KKM_AIL_AGENT`

The validation strategy is divided into four distinct phases to guarantee parity across orchestration, data transformation, edge-case handling, and operational logging:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       MIGRATION VALIDATION FLOW                         │
├─────────────────────────────────────────────────────────────────────────┤
│  1. ORCHESTRATION & LOGGING PARITY                                      │
│     - Verify Airflow DAG structure, variables, and log output strings.  │
├─────────────────────────────────────────────────────────────────────────┤
│  2. DATA TRANSFORMATION & QUERY PARITY                                  │
│     - Validate PySpark SQL execution against legacy Oracle View rules.  │
├─────────────────────────────────────────────────────────────────────────┤
│  3. OUTPUT FORMAT & NULL HANDLING PARITY                                │
│     - Assert semicolon-delimited flat-file structure and NULL defaults. │
├─────────────────────────────────────────────────────────────────────────┤
│  4. END-TO-END INTEGRATION & IDEMPOTENCY                                │
│     - Execute dry-runs, verify GCS targets, and test failure callbacks. │
└─────────────────────────────────────────────────────────────────────────┘
```

---

# Section 1: Orchestration & Variable Resolution Parity

## Test Case 1.1: Airflow DAG Structure and Variable Resolution
### Purpose
Verify that the migrated Airflow DAG (`dw_dwh_abpz_kkm_ail_agent_dag`) correctly resolves global and job-specific variables, maps task dependencies exactly as specified in the UC4 design, and passes the correct arguments to the Dataproc operator.

### Setup
* A Python environment with `apache-airflow` and `pytest` installed.
* Mock Airflow Variables registered in the test context:
  * `GCP_PROJECT` = `test-gcp-project`
  * `DATAPROC_REGION` = `europe-west3`
  * `DATAPROC_CLUSTER` = `test-dataproc-cluster`
  * `GCS_BUCKET` = `test-dwh-bucket`
  * `dwh_home` = `/opt/dwh`
  * `kkm_rueckblick_ladedatum` = `2026-07-01`

### Action
Run a unit test using `pytest` to parse the DAG file, assert the task dependency graph, and validate the arguments generated for the `DataprocSubmitJobOperator` task.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def setup_airflow_variables(monkeypatch):
    vars_dict = {
        "GCP_PROJECT": "test-gcp-project",
        "DATAPROC_REGION": "europe-west3",
        "DATAPROC_CLUSTER": "test-dataproc-cluster",
        "GCS_BUCKET": "test-dwh-bucket",
        "dwh_home": "/opt/dwh",
        "kkm_rueckblick_ladedatum": "2026-07-01"
    }
    def mock_get(key, default_var=None):
        return vars_dict.get(key, default_var)
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_loads_with_correct_dependencies_and_args():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_dwh_abpz_kkm_ail_agent_dag")
    
    assert dag is not None
    assert len(dag.tasks) == 3
    
    # Assert dependency map: start -> execute -> end
    start_task = dag.get_task("start_pipeline")
    execute_task = dag.get_task("dw_dwh_abpz_kkm_ail_agent")
    end_task = dag.get_task("end_pipeline")
    
    assert execute_task in start_task.downstream_list
    assert end_task in execute_task.downstream_list
    
    # Assert Dataproc Operator Arguments
    job_conf = execute_task.job
    pyspark_args = job_conf["pyspark_job"]["args"]
    
    assert "--job_kennung" in pyspark_args
    assert "ABPZ_KKM_AIL_AGENT" in pyspark_args
    assert "--output_file" in pyspark_args
    assert "AgentADSLookup.txt" in pyspark_args
    assert "--rueckblick_ladedatum" in pyspark_args
    assert "2026-07-01" in pyspark_args
```

### Pass/Fail Criterion
* **Pass:** The DAG parses without errors, contains exactly the tasks `start_pipeline`, `dw_dwh_abpz_kkm_ail_agent`, and `end_pipeline` in sequential order, and resolves the PySpark arguments matching the mocked Airflow variables.
* **Fail:** Any parsing error occurs, dependencies are broken, or arguments do not match the variables.

---

## Test Case 1.2: Success and Failure Log Output Parity
### Purpose
Verify that the Airflow success and failure callbacks output the exact literal log strings required by the legacy DWH framework specifications.

### Setup
* A mocked Airflow context dictionary containing task instance metadata.
* A standard Python `logging` capture handler to inspect output logs.

### Action
Trigger the `on_task_failure_callback` and `on_dag_success_callback` manually within a test case and assert the captured log outputs.

```python
# test_logging_callbacks.py
import logging
from dw_dwh_abpz_kkm_ail_agent_dag import on_task_failure_callback, on_dag_success_callback

class MockTaskInstance:
    def __init__(self):
        self.task_id = "dw_dwh_abpz_kkm_ail_agent"

def test_callbacks_output_verbatim_legacy_strings(caplog):
    # Test Success Callback
    with caplog.at_level(logging.INFO):
        on_dag_success_callback(context={})
        assert "Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet." in caplog.text

    caplog.clear()

    # Test Failure Callback
    mock_context = {
        "task_instance": MockTaskInstance(),
        "execution_date": "2026-07-15T03:00:00",
        "exception": "Dataproc cluster connection timeout"
    }
    with caplog.at_level(logging.ERROR):
        on_task_failure_callback(context=mock_context)
        assert "Jobkennung ABPZ_KKM_AIL_AGENT eingetragen für dw_dwh_abpz_kkm_ail_agent" in caplog.text
        assert "Dataproc cluster connection timeout" in caplog.text
```

### Pass/Fail Criterion
* **Pass:** The success callback outputs the exact German success string. The failure callback outputs the exact legacy registration string containing the task ID.
* **Fail:** The log strings deviate by even one character from the specified legacy output patterns.

---

# Section 2: Data Transformation & Query Parity

## Test Case 2.1: PySpark SQL Query Logic and Filter Parity
### Purpose
Prove that the PySpark SQL query executed in `agent_ads_lookup.py` is behaviorally equivalent to the legacy Oracle view extraction logic, specifically verifying the date filtering logic (`UPDATE_TIMESTAMP` and `LAST_MODIFIED_DATE`).

### Setup
* A local `SparkSession` initialized via `pytest-spark`.
* A mock source table `DWH_VI_S_SDM_AGENT_ADS` populated with test records spanning different update dates.

### Action
Execute the query logic using `process_lookup_extraction` and assert that only records matching the date filter criteria are returned.

```python
# test_query_logic.py
import pytest
from pyspark.sql import SparkSession
from agent_ads_lookup import process_lookup_extraction

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("test_query_logic") \
        .master("local[*]") \
        .getOrCreate()

def test_process_lookup_extraction_filters(spark):
    # Setup mock source table
    schema = "AGENT_ID string, AGENT_NAME string, AGENT_STATUS string, ADS_DOMAIN string, ADS_USER_ID string, EMAIL string, UPDATE_TIMESTAMP string, LAST_MODIFIED_DATE string"
    data = [
        # Case 1: Matches rueckblick_date filter
        ("A01", "Agent One", "A", "DOM1", "usr01", "a1@co.com", "2026-07-02 12:00:00", "2026-06-01"),
        # Case 2: Matches exec_date filter
        ("A02", "Agent Two", "A", "DOM1", "usr02", "a2@co.com", "2026-05-01 12:00:00", "2026-07-15"),
        # Case 3: Matches both filters
        ("A03", "Agent Three", "I", "DOM2", "usr03", "a3@co.com", "2026-07-10 12:00:00", "2026-07-15"),
        # Case 4: Out of bounds (should be filtered out)
        ("A04", "Agent Four", "A", "DOM1", "usr04", "a4@co.com", "2026-06-29 11:59:59", "2026-07-14")
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    df.createOrReplaceTempView("DWH_VI_S_SDM_AGENT_ADS")
    
    # Execute extraction with parameters:
    # rueckblick_date = '2026-07-01'
    # exec_date = '2026-07-15'
    result_df = process_lookup_extraction(spark, rueckblick_date="2026-07-01", exec_date="2026-07-15")
    results = result_df.collect()
    
    # Assertions
    assert len(results) == 3
    retrieved_ids = [row["AGENT_ID"] for row in results]
    assert "A01" in retrieved_ids
    assert "A02" in retrieved_ids
    assert "A03" in retrieved_ids
    assert "A04" not in retrieved_ids
```

### Pass/Fail Criterion
* **Pass:** The query returns exactly 3 records (`A01`, `A02`, `A03`) and correctly filters out `A04` based on the boundary conditions.
* **Fail:** The query returns an incorrect number of rows or fails to apply the `OR` logic between the two date filters.

---

# Section 3: Output Format & NULL Handling Parity

## Test Case 3.1: Semicolon-Delimited Flat-File Generation & NULL Handling
### Purpose
Verify that the output flat-file is generated as a single semicolon-delimited file, and that any `NULL` values in the source DataFrame are correctly converted to empty strings (`""`) without literal `"null"` strings appearing in the output.

### Setup
* A local `SparkSession`.
* A test DataFrame containing various `NULL` values across different data types (string, integer, timestamp).
* A local temporary directory to simulate GCS output.

### Action
Run `write_flat_file_lookup` on the test DataFrame, read the generated text file, and assert its structural integrity.

```python
# test_output_format.py
import os
import shutil
import pytest
from pyspark.sql import SparkSession
from agent_ads_lookup import write_flat_file_lookup

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("test_output_format") \
        .master("local[*]") \
        .getOrCreate()

def test_flat_file_null_handling_and_delimiter(spark, tmp_path):
    schema = "AGENT_ID int, AGENT_NAME string, AGENT_STATUS string, ADS_DOMAIN string, ADS_USER_ID string, EMAIL string, UPDATE_TIMESTAMP string"
    data = [
        # Row 1: Fully populated
        (100, "John Doe", "Active", "CORP", "jdoe", "jdoe@corp.com", "2026-07-15 08:00:00"),
        # Row 2: Mixed NULL values
        (200, None, "Inactive", None, "jsmith", None, None)
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    
    # Write to local temp path simulating GCS bucket structure
    target_dir = str(tmp_path / "lookups" / "agent")
    os.makedirs(target_dir, exist_ok=True)
    
    # Mock write_flat_file_lookup behavior locally
    write_flat_file_lookup(df, target_bucket=str(tmp_path), output_file_name="AgentADSLookup.txt")
    
    # Locate the output part file in the temp directory
    temp_export_dir = tmp_path / "lookups" / "agent" / "temp_export_AgentADSLookup.txt"
    part_files = [f for f in os.listdir(temp_export_dir) if f.startswith("part-")]
    assert len(part_files) == 1, "Output must be coalesced into a single part file"
    
    output_file_path = temp_export_dir / part_files[0]
    with open(output_file_path, "r") as f:
        lines = f.read().splitlines()
        
    assert len(lines) == 2
    # Assert Row 1 format
    assert lines[0] == "100;John Doe;Active;CORP;jdoe;jdoe@corp.com;2026-07-15 08:00:00"
    # Assert Row 2 format (NULLs replaced with empty strings)
    assert lines[1] == "200;;Inactive;;jsmith;;"
```

### Pass/Fail Criterion
* **Pass:** The output contains exactly one part file, fields are delimited strictly by semicolons, and all `NULL` values are represented as empty strings (consecutive semicolons `;;`).
* **Fail:** Multiple part files are generated, fields are delimited by commas or tabs, or literal `"null"` strings are written to the file.

---

# Section 4: End-to-End Integration & Idempotency

## Test Case 4.1: End-to-End Dry Run and GCS Target Verification
### Purpose
Verify that the PySpark script executes successfully from end-to-end using a mock environment, writes the final output to the designated GCS path, and is fully idempotent (subsequent runs overwrite previous outputs without duplicate files).

### Setup
* A running local Spark environment.
* A mock GCS bucket directory structure on the local filesystem.

### Action
1. Execute the `main()` function of `agent_ads_lookup.py` using command-line arguments.
2. Verify the creation of the output file.
3. Re-run the execution with the same parameters and verify that the output is cleanly overwritten.

```bash
# Execute the PySpark job locally using python command line arguments
python3 pyspark_scripts/agent_ads_lookup.py \
  --job_kennung "ABPZ_KKM_AIL_AGENT" \
  --output_file "AgentADSLookup.txt" \
  --config_file "gs://test-bucket/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg" \
  --rueckblick_ladedatum "2026-07-01" \
  --exec_date "2026-07-15" \
  --target_bucket "test-bucket" \
  --dwh_home "/opt/dwh"
```

### Pass/Fail Criterion
* **Pass:** The script exits with code `0`. The output file `AgentADSLookup.txt` is created at `gs://test-bucket/lookups/agent/temp_export_AgentADSLookup.txt/part-00000...`. Running the script a second time succeeds and overwrites the target directory without throwing file-exists exceptions.
* **Fail:** The script exits with a non-zero code, fails to write the output, or throws an overwrite exception on the second run.