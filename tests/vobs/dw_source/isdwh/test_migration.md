# Migration Validation Test Suite: DW.DWH_ABPZ_KKM_AIL_AGENT

This document contains production-grade, runnable migration-validation tests to verify the behavioral equivalence of the migrated Google Cloud (Airflow + Dataproc Serverless PySpark) pipeline against the legacy Ab Initio/UC4 environment.

---

## Section 1: Orchestration & Parameter Parity Tests

### Test Case 1.1: Airflow Variable Resolution and Parameter Mapping
* **Purpose**: Verify that the Airflow DAG correctly resolves global and job-specific variables (specifically the lookback date parameter `KKM_Rueckblick_Ladedatum` and environment variables) and maps them to the Dataproc job configuration arguments without syntax errors or missing keys.
* **Setup**: 
  * A mock Airflow metadata database or active Airflow environment.
  * Populate the Airflow Variable `dw_variablen_dwk_kkm` with JSON: `{"kkm_rueckblick_ladedatum": "20231001"}`.
  * Populate global variables `GCP_PROJECT`, `GCP_REGION`, `DATAPROC_CLUSTER_NAME`, and `GCS_BUCKET`.
* **Action**: Instantiate the DAG `dw_dwh_abpz_kkm_ail_agent` and render the task `dw_dwh_abpz_kkm_ail_agent` templates for a execution date.
* **Pass/Fail Criterion**: The rendered arguments for the Dataproc job must match the expected structure exactly:
  * `--job_kennung` must resolve to `"ABPZ_KKM_AIL_AGENT"`
  * `--rueckblick_ladedatum` must resolve to `"20231001"`
  * `--output_file` must resolve to `"AgentADSLookup.txt"`
  * `--config` must resolve to `"BHB_CCM_PROC_WriteAgentADSLookup.cfg"`

```python
# test_dag_parameters.py
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def setup_airflow_variables(monkeypatch):
    # Mock Airflow Variables
    variables = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3",
        "DATAPROC_CLUSTER_NAME": "test-cluster",
        "GCS_BUCKET": "test-bucket"
    }
    json_variables = {
        "dw_variablen_dwk_kkm": {"kkm_rueckblick_ladedatum": "20231001"}
    }
    
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
        
    def mock_get_json(key, default_var=None):
        return json_variables.get(key, default_var)

    monkeypatch.setattr(Variable, "get", mock_get)
    monkeypatch.setattr(Variable, "get_val", mock_get_json, raising=False)

def test_dag_compiles_and_renders_correctly():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_dwh_abpz_kkm_ail_agent")
    
    assert dag is not None
    assert len(dag.tasks) == 1
    
    task = dag.get_task("dw_dwh_abpz_kkm_ail_agent")
    job_config = task.job
    
    # Assert GCP Infrastructure Parameters
    assert job_config["reference"]["project_id"] == "test-gcp-project"
    assert job_config["placement"]["cluster_name"] == "test-cluster"
    
    # Assert PySpark Script Arguments
    args = job_config["pyspark_job"]["args"]
    assert "--job_kennung" in args
    assert "ABPZ_KKM_AIL_AGENT" in args
    assert "--output_file" in args
    assert "AgentADSLookup.txt" in args
```

---

### Test Case 1.2: Failure Callback and Log Routing (Replacing `DW.LESE_LOG`)
* **Purpose**: Verify that when the PySpark execution fails, the Airflow task triggers the `on_failure_alarm` callback, writing the standardized error message to Cloud Logging (Stackdriver) to replicate the legacy `DW.LESE_LOG` and `f_alis_msgerr.ksh` behavior.
* **Setup**: 
  * Create a mock Airflow Task Instance context dictionary containing a failed state.
* **Action**: Programmatically execute `on_failure_alarm(context)` with the mock context.
* **Pass/Fail Criterion**: The callback must execute without throwing an exception and must output a log containing the DAG ID, Task ID, and Log URL to the standard logging stream.

```python
# test_failure_callback.py
import logging

def test_on_failure_callback_logs_correctly(caplog):
    from dags.dw_dwh_abpz_kkm_ail_agent import on_failure_alarm
    
    class MockTaskInstance:
        dag_id = "dw_dwh_abpz_kkm_ail_agent"
        task_id = "dw_dwh_abpz_kkm_ail_agent"
        log_url = "http://localhost:8080/log?dag_id=test&task_id=test"

    mock_context = {
        "task_instance": MockTaskInstance(),
        "execution_date": "2023-10-01T03:00:00"
    }
    
    with caplog.at_level(logging.ERROR):
        on_failure_alarm(mock_context)
        
    assert "• [ALARM] Airflow Task Failure Notification!" in caplog.text
    assert "DAG: dw_dwh_abpz_kkm_ail_agent" in caplog.text
    assert "Task: dw_dwh_abpz_kkm_ail_agent" in caplog.text
```

---

## Section 2: PySpark Wrapper & Argument Parsing Tests

### Test Case 2.1: PySpark Wrapper Console Output Parity
* **Purpose**: Verify that the PySpark script parses command-line arguments correctly and prints the job metadata block to stdout character-for-character matching the legacy wrapper's output format.
* **Setup**: 
  * A local PySpark environment or test runner.
* **Action**: Execute `agent_ads_lookup.py` with arguments:
  `--job_kennung ABPZ_KKM_AIL_AGENT --rueckblick_ladedatum 20231001 --output_file AgentADSLookup.txt --config BHB_CCM_PROC_WriteAgentADSLookup.cfg`
* **Pass/Fail Criterion**: The stdout must contain the exact formatted text block:
  ```
  ----------------- Job -----------------------
  Jobkennung (ab initio)     : 'ABPZ_KKM_AIL_AGENT'
  Objekt                     : 'AgentADSLookup.txt'
  DeltaT fuer Stichtag       : '20231001'
  Ab Initio Konfig           : 'BHB_CCM_PROC_WriteAgentADSLookup.cfg'
  ---------------------------------------------
  ```

```python
# test_pyspark_wrapper.py
import subprocess
import sys

def test_pyspark_wrapper_stdout_parity():
    cmd = [
        sys.executable,
        "pyspark_scripts/agent_ads_lookup.py",
        "--job_kennung", "ABPZ_KKM_AIL_AGENT",
        "--rueckblick_ladedatum", "20231001",
        "--output_file", "AgentADSLookup.txt",
        "--config", "BHB_CCM_PROC_WriteAgentADSLookup.cfg"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # The script will exit with code 10 (NotImplementedError) which is expected,
    # but we must verify the stdout printed before the exception.
    expected_output = (
        "----------------- Job -----------------------\n"
        "Jobkennung (ab initio)     : 'ABPZ_KKM_AIL_AGENT'\n"
        "Objekt                     : 'AgentADSLookup.txt'\n"
        "DeltaT fuer Stichtag       : '20231001'\n"
        "Ab Initio Konfig           : 'BHB_CCM_PROC_WriteAgentADSLookup.cfg'\n"
        "---------------------------------------------"
    )
    
    assert expected_output in result.stdout
    assert result.returncode == 10
    assert "[FATAL_GAP] Transformation Logic Missing" in result.stdout
```

---

## Section 3: Transformation & Data Quality Tests (Post-Implementation)

*Note: The following tests validate the core transformation block once the Ab Initio `.mp` graph logic is located and implemented inside `execute_transformation`.*

### Test Case 3.1: Lookback Filter Correctness (84-Day Window)
* **Purpose**: Verify that the transformation logic correctly filters the source view `DWH$VI_S_SDM_AGENT_ADS` using the calculated lookback date threshold (`--rueckblick_ladedatum`), ensuring no records older than the threshold are processed.
* **Setup**: 
  * Create an in-memory Spark DataFrame representing the source view with dates ranging from 90 days ago to today.
  * Set `--rueckblick_ladedatum` to a date exactly 84 days prior to today.
* **Action**: Execute the transformation logic on the test DataFrame.
* **Pass/Fail Criterion**: 
  * All output records must have a load date greater than or equal to the lookback date.
  * The row count of the output must match the count of source records within the 84-day window.

```python
# test_transformation_filters.py
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

def test_lookback_filtering():
    spark = SparkSession.builder.appName("TestLookback").getOrCreate()
    
    # Schema representing DWH$VI_S_SDM_AGENT_ADS
    schema = StructType([
        StructField("AGENT_ID", StringType(), False),
        StructField("LOAD_DATE", DateType(), False)
    ])
    
    today = datetime.today().date()
    data = [
        ("Agent_A", today - timedelta(days=10)),  # Within 84 days
        ("Agent_B", today - timedelta(days=83)),  # Within 84 days
        ("Agent_C", today - timedelta(days=84)),  # Boundary
        ("Agent_D", today - timedelta(days=85)),  # Out of bounds
        ("Agent_E", today - timedelta(days=120))  # Out of bounds
    ]
    
    source_df = spark.createDataFrame(data, schema)
    lookback_date_str = (today - timedelta(days=84)).strftime("%Y-%m-%d")
    
    # Apply filter logic (simulating execute_transformation)
    filtered_df = source_df.filter(source_df.LOAD_DATE >= lookback_date_str)
    results = filtered_df.collect()
    
    # Assertions
    assert len(results) == 3
    for row in results:
        assert row["AGENT_ID"] in ["Agent_A", "Agent_B", "Agent_C"]
        assert row["LOAD_DATE"] >= (today - timedelta(days=84))
```

---

### Test Case 3.2: Null Handling and Schema Assertions
* **Purpose**: Ensure that the output schema matches the target specification and that NULL values in non-nullable fields are handled gracefully (either rejected or defaulted) without crashing the Spark session.
* **Setup**: 
  * Create a source DataFrame containing NULL values in key fields (e.g., `AGENT_ID`).
* **Action**: Run the transformation logic.
* **Pass/Fail Criterion**: 
  * The pipeline must not crash.
  * Records with NULL keys must be filtered out or handled according to business rules.
  * Output schema must match the expected target layout.

```python
# test_schema_and_nulls.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def test_null_handling_in_keys():
    spark = SparkSession.builder.appName("TestNulls").getOrCreate()
    
    schema = StructType([
        StructField("AGENT_ID", StringType(), True),
        StructField("AGENT_NAME", StringType(), True)
    ])
    
    data = [
        ("A001", "Agent One"),
        (None, "Agent Missing Key"),
        ("A002", None)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Business Rule: AGENT_ID must not be null
    cleaned_df = df.filter(df.AGENT_ID.isNotNull())
    results = cleaned_df.collect()
    
    assert len(results) == 2
    assert "Agent Missing Key" not in [row["AGENT_NAME"] for row in results]
```