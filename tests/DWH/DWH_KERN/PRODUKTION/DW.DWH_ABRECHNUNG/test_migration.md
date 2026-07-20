Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Airflow DAG `dw_dwh_abrechnung_reformat_jp` is behaviorally equivalent to the legacy UC4 Job Plan `DW.DWH_ABRECHNUNG_REFORMAT_JP`.

---

# Test Suite: UC4 to Airflow Migration Validation

## Test Case 1: DAG Structural Integrity & Metadata Assertions

### Purpose
To verify that the migrated Airflow DAG is parsed without errors, matches the structural layout of the legacy UC4 Job Plan, and preserves all metadata—including the exact German description literal and concurrency constraints.

### Setup
* A Python environment with Apache Airflow installed.
* The migrated DAG file `DW_DWH_ABRECHNUNG_REFORMAT_JP.py` placed in the Airflow `dags/` directory or accessible via the Python path.

### Action
Run a pytest suite that loads the DAG using Airflow’s `DagBag` and asserts its properties, task IDs, dependencies, and metadata.

### Code Implementation
```python
import pytest
from airflow.models import DagBag

DAG_ID = "dw_dwh_abrechnung_reformat_jp"

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="DWH/DWH_KERN/PRODUKTION/DW.DWH_ABRECHNUNG", include_examples=False)

def test_dag_loading_and_metadata(dagbag):
    """Verify that the DAG loads without import errors and contains correct metadata."""
    assert DAG_ID in dagbag.dags, f"DAG {DAG_ID} not found in DagBag"
    dag = dagbag.get_dag(DAG_ID)
    
    # Verify description matches the legacy UC4 documentation literal exactly
    expected_desc = (
        "Jobplan zum taeglichen Reformatieren der Abrechnungsdaten fuer den Downstream-Feed. "
        "Ruft ein Legacy-Perl-Script auf, das noch aus der Erstmigration stammt."
    )
    assert dag.description == expected_desc
    
    # Verify scheduling and concurrency controls
    assert dag.schedule_interval == "0 2 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

def test_dag_structure_and_dependencies(dagbag):
    """Verify that the DAG contains all expected tasks and correct dependency mapping."""
    dag = dagbag.get_dag(DAG_ID)
    
    expected_tasks = {
        "sensor_kunde_abgl_woechentlich",
        "sensor_rechnung_export_taeglich",
        "sensor_tarifhist_scd_monatlich",
        "sensor_umsatz_konsolidierung_monatlich",
        "start",
        "trigger_dw_dwh_abrechnung_reformat_js",
        "end"
    }
    
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # Verify upstream sensor dependencies point to 'start'
    sensors = [
        "sensor_kunde_abgl_woechentlich",
        "sensor_rechnung_export_taeglich",
        "sensor_tarifhist_scd_monatlich",
        "sensor_umsatz_konsolidierung_monatlich"
    ]
    start_task = dag.get_task("start")
    for sensor_id in sensors:
        sensor_task = dag.get_task(sensor_id)
        assert start_task in sensor_task.downstream_list
        
    # Verify sequential chain: start -> trigger -> end
    trigger_task = dag.get_task("trigger_dw_dwh_abrechnung_reformat_js")
    end_task = dag.get_task("end")
    
    assert trigger_task in start_task.downstream_list
    assert end_task in trigger_task.downstream_list
```

### Pass/Fail Criterion
* **Pass**: The DAG loads with zero import errors, matches the exact schedule and description, contains all 7 tasks, and enforces the linear dependency chain `[sensors] >> start >> trigger >> end`.
* **Fail**: Any import error is raised, or any metadata/dependency assertion fails.

---

## Test Case 2: External Task Sensor Configuration Validation

### Purpose
To verify that the `ExternalTaskSensor` tasks are configured correctly to monitor the exact upstream DAGs and tasks that represent the legacy UC4 dependencies.

### Setup
* The same pytest environment as Test Case 1.

### Action
Inspect the properties of each `ExternalTaskSensor` in the DAG to ensure they target the correct external DAGs, target the `'end'` task, use the `'reschedule'` mode to save resources, and have appropriate timeouts.

### Code Implementation
```python
def test_external_task_sensors_config(dagbag):
    """Verify configuration of all upstream ExternalTaskSensors."""
    dag = dagbag.get_dag(DAG_ID)
    
    expected_sensor_configs = {
        "sensor_kunde_abgl_woechentlich": {
            "external_dag_id": "dw_dwh_kunde_abgl_woechentlich_js",
            "external_task_id": "end"
        },
        "sensor_rechnung_export_taeglich": {
            "external_dag_id": "dw_dwh_rechnung_export_taeglich_js",
            "external_task_id": "end"
        },
        "sensor_tarifhist_scd_monatlich": {
            "external_dag_id": "dw_dwh_tarifhist_scd_monatlich_js",
            "external_task_id": "end"
        },
        "sensor_umsatz_konsolidierung_monatlich": {
            "external_dag_id": "dw_dwh_umsatz_konsolidierung_monatlich_js",
            "external_task_id": "end"
        }
    }
    
    for task_id, config in expected_sensor_configs.items():
        sensor = dag.get_task(task_id)
        
        assert sensor.external_dag_id == config["external_dag_id"]
        assert sensor.external_task_id == config["external_task_id"]
        assert sensor.allowed_states == ["success"]
        assert sensor.mode == "reschedule"
        assert sensor.poke_interval == 300
        assert sensor.timeout == 3600
```

### Pass/Fail Criterion
* **Pass**: All four sensors are configured with the correct target DAG IDs, target the `'end'` task, and use the `'reschedule'` mode with a 1-hour timeout.
* **Fail**: Any sensor is misconfigured, uses `'poke'` mode (which wastes worker slots), or has incorrect target IDs.

---

## Test Case 3: Downstream Trigger Operator Configuration

### Purpose
To verify that the `TriggerDagRunOperator` is configured to execute the child DAG `dw_dwh_abrechnung_reformat_js` (which contains the actual migrated Perl script logic) and wait for its completion.

### Setup
* The same pytest environment as Test Case 1.

### Action
Inspect the properties of the `trigger_dw_dwh_abrechnung_reformat_js` task.

### Code Implementation
```python
def test_trigger_operator_config(dagbag):
    """Verify that the TriggerDagRunOperator is configured correctly."""
    dag = dagbag.get_dag(DAG_ID)
    trigger_task = dag.get_task("trigger_dw_dwh_abrechnung_reformat_js")
    
    assert trigger_task.trigger_dag_id == "dw_dwh_abrechnung_reformat_js"
    assert trigger_task.wait_for_completion is True
    assert trigger_task.reset_dag_run is True
    assert trigger_task.poke_interval == 60
```

### Pass/Fail Criterion
* **Pass**: The trigger operator targets `dw_dwh_abrechnung_reformat_js`, has `wait_for_completion=True` (ensuring the orchestrator does not finish prematurely), and has `reset_dag_run=True` (allowing clean retries).
* **Fail**: The target DAG ID is incorrect, or `wait_for_completion` is set to `False`.

---

## Test Case 4: End-to-End Execution & State Parity (Integration Test)

### Purpose
To verify the execution flow of the orchestrator DAG in a simulated environment, ensuring that successful upstream sensor signals trigger the child DAG and propagate success to the end anchor.

### Setup
* An Airflow environment configured for testing (e.g., using `dag.test()`).
* Mocked execution of the external tasks and the triggered child DAG to prevent actual external system calls during the integration test.

### Action
Execute the DAG using Airflow's internal testing framework, mocking the sensors and the child DAG run to return success, and verify that all tasks transition to the `SUCCESS` state.

### Code Implementation
```python
from unittest.mock import patch
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

@patch("airflow.sensors.external_task.ExternalTaskSensor.poke", return_value=True)
@patch("airflow.operators.trigger_dagrun.TriggerDagRunOperator.execute")
def test_dag_execution_flow(mock_trigger, mock_poke, dagbag):
    """Simulate a successful run of the orchestrator DAG."""
    dag = dagbag.get_dag(DAG_ID)
    
    # Create a test DagRun
    execution_date = datetime(2026, 1, 1, 2, 0, 0)
    dag_run = dag.test(execution_date=execution_date)
    
    # Verify overall DAG run state
    assert dag_run.state == DagRunState.SUCCESS
    
    # Verify individual task states
    for ti in dag_run.get_task_instances():
        assert ti.state == TaskInstanceState.SUCCESS, f"Task {ti.task_id} failed with state {ti.state}"
```

### Pass/Fail Criterion
* **Pass**: The DAG run completes with a state of `SUCCESS`, and every task instance (including sensors, start, trigger, and end) successfully transitions to `SUCCESS`.
* **Fail**: The DAG run fails, times out, or any individual task fails to execute.

---

## Test Case 5: End-to-End Data Parity & Schema Validation

### Purpose
To prove that the output data generated by the migrated child pipeline (`dw_dwh_abrechnung_reformat_js`) is identical in schema, row count, and content to the output generated by the legacy Perl script (`reformat_abrechnung.pl`).

### Setup
1. **Legacy Baseline**: Run the legacy Perl script on a controlled test dataset in the legacy environment. Export the resulting reformatted billing data to a temporary GCS location: `gs://migration-validation-bucket/gold/legacy_reformatted_billing/`.
2. **Migrated Target**: Run the migrated PySpark/BigQuery job (triggered by the child DAG) on the exact same input dataset. Write the output to a target validation table: `your_project.your_dataset.target_reformatted_billing`.

### Action
Execute a PySpark validation script that compares the legacy baseline GCS files with the migrated BigQuery target table. The script performs:
1. Schema structure comparison.
2. Row count validation.
3. A full outer join on primary keys to detect column-level value mismatches.

### Code Implementation
```python
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_parity_validation():
    spark = SparkSession.builder \
        .appName("DWH_Abrechnung_Reformat_Parity_Validation") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.0") \
        .getOrCreate()

    legacy_path = "gs://migration-validation-bucket/gold/legacy_reformatted_billing/"
    target_table = "your_project.your_dataset.target_reformatted_billing"
    
    # 1. Load Datasets
    df_legacy = spark.read.option("header", "true").csv(legacy_path)
    df_target = spark.read.format("bigquery").option("table", target_table).load()
    
    # Define primary keys for comparison (e.g., billing_id / Abrechnungs-ID)
    primary_keys = ["billing_id"]
    
    # 2. Schema Validation
    legacy_cols = sorted(df_legacy.columns)
    target_cols = sorted(df_target.columns)
    
    if legacy_cols != target_cols:
        print(f"SCHEMA MISMATCH!\nLegacy columns: {legacy_cols}\nTarget columns: {target_cols}")
        sys.exit(1)
        
    # 3. Row Count Validation
    count_legacy = df_legacy.count()
    count_target = df_target.count()
    
    if count_legacy != count_target:
        print(f"ROW COUNT MISMATCH!\nLegacy count: {count_legacy}\nTarget count: {count_target}")
        sys.exit(1)
        
    # 4. Data Parity Validation (Full Outer Join)
    # Join datasets on primary keys to find value discrepancies
    joined_df = df_legacy.alias("l").join(
        df_target.alias("t"),
        on=primary_keys,
        how="full_outer"
    )
    
    # Identify non-key columns to compare
    compare_cols = [c for c in df_legacy.columns if c not in primary_keys]
    mismatch_filter = None
    
    for col_name in compare_cols:
        condition = (col(f"l.{col_name}") != col(f"t.{col_name}")) | \
                    (col(f"l.{col_name}").isNull() & col(f"t.{col_name}").isNotNull()) | \
                    (col(f"l.{col_name}").isNotNull() & col(f"t.{col_name}").isNull())
        
        if mismatch_filter is None:
            mismatch_filter = condition
        else:
            mismatch_filter = mismatch_filter | condition
            
    mismatches = joined_df.filter(mismatch_filter)
    mismatch_count = mismatches.count()
    
    if mismatch_count > 0:
        print(f"DATA VALUE MISMATCH! Found {mismatch_count} mismatched rows.")
        mismatches.select(*primary_keys, *[col(f"l.{c}").alias(f"legacy_{c}") for c in compare_cols[:3]], 
                          *[col(f"t.{c}").alias(f"target_{c}") for c in compare_cols[:3]]).show(10)
        sys.exit(1)
        
    print("SUCCESS: Schema, Row Count, and Data Values are 100% equivalent!")
    sys.exit(0)

if __name__ == "__main__":
    run_parity_validation()
```

### Pass/Fail Criterion
* **Pass**: The validation script exits with code `0`, proving that schemas match, row counts are identical, and there are zero column-level value mismatches between the legacy and target outputs.
* **Fail**: The validation script exits with code `1` due to a schema mismatch, row count discrepancy, or data value variance.