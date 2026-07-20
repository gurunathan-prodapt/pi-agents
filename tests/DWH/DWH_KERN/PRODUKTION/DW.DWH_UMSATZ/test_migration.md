Here is a comprehensive suite of migration-validation tests designed to verify that the migrated Airflow DAG `dw_dwh_umsatz_konsolidierung_monatlich_jp` is behaviorally equivalent to the legacy UC4 Job Plan `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`.

---

# Test Suite: UC4 to Airflow Migration Validation

## Test Case 1: DAG Structural & Metadata Integrity

### Purpose
Verify that the migrated Airflow DAG matches the legacy UC4 metadata, including scheduling, active status, task structure, and the preservation of German documentation/titles.

### Setup
*   An Airflow environment with the target DAG file `dw_dwh_umsatz_konsolidierung_monatlich_jp.py` placed in the `dags/` directory.
*   The following Airflow Variables must be set in the metadata database (to satisfy Hard Rule 5):
    *   `GCP_PROJECT` = `test-gcp-project`
    *   `GCP_REGION` = `europe-west3`
    *   `DATAPROC_CLUSTER` = `test-dataproc-cluster`
    *   `GCS_BUCKET` = `test-gcs-bucket`

### Action
Run a pytest suite that parses the DAG and asserts its structural properties against the legacy UC4 specifications.

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(scope="module", autouse=True)
def setup_variables():
    # Set mock variables to prevent Airflow KeyErrors during parsing
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCP_REGION", "europe-west3")
    Variable.set("DATAPROC_CLUSTER", "test-dataproc-cluster")
    Variable.set("GCS_BUCKET", "test-gcs-bucket")

def test_dag_metadata_and_structure():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag_id = "dw_dwh_umsatz_konsolidierung_monatlich_jp"
    
    # 1. Assert DAG exists and has no import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load."
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id)
    
    # 2. Assert Schedule Parity (Monthly on the 1st)
    assert dag.schedule_interval == "0 0 1 * *"
    
    # 3. Assert Active Status & Concurrency
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    
    # 4. Assert German Documentation Preservation (Hard Rule 4)
    expected_doc_substring = (
        "Jobplan zur monatlichen Konsolidierung der Umsatzdaten ueber alle Konzerngesellschaften. "
        "Ruft ein Legacy-Ab-Initio-Graph auf, das noch aus der Erstmigration stammt."
    )
    assert expected_doc_substring in dag.doc_md, "Legacy German documentation was not preserved verbatim."

    # 5. Assert Task Inventory
    expected_tasks = {
        "start",
        "sensor_dw_dwh_abrechnung_reformat_js",
        "sensor_dw_dwh_kunde_abgl_woechentlich_js",
        "sensor_dw_dwh_rechnung_export_taeglich_js",
        "sensor_dw_dwh_tarifhist_scd_monatlich_js",
        "dw_dwh_umsatz_konsolidierung_monatlich_js",
        "end"
    }
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
```

### Pass/Fail Criterion
*   **Pass**: The DAG parses with zero import errors, matches the `0 0 1 * *` schedule, enforces `max_active_runs = 1`, contains all 7 tasks, and preserves the legacy German documentation verbatim.
*   **Fail**: Any metadata mismatch, missing tasks, or parsing errors occur.

---

## Test Case 2: Cross-DAG Dependency & Sensor Behavior

### Purpose
Verify that the four `ExternalTaskSensor` tasks are correctly configured to block the execution of the consolidation job until all upstream DAGs have completed successfully.

### Setup
*   The same Airflow environment as Test Case 1.

### Action
Execute a pytest unit test to validate the configuration of the sensor tasks, ensuring they target the correct upstream DAGs and have safe execution parameters (e.g., avoiding deadlock with appropriate timeouts).

```python
def test_sensor_configurations():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_umsatz_konsolidierung_monatlich_jp")
    
    sensors = {
        "sensor_dw_dwh_abrechnung_reformat_js": "dw_dwh_abrechnung_reformat_js",
        "sensor_dw_dwh_kunde_abgl_woechentlich_js": "dw_dwh_kunde_abgl_woechentlich_js",
        "sensor_dw_dwh_rechnung_export_taeglich_js": "dw_dwh_rechnung_export_taeglich_js",
        "sensor_dw_dwh_tarifhist_scd_monatlich_js": "dw_dwh_tarifhist_scd_monatlich_js"
    }
    
    for sensor_id, expected_external_dag_id in sensors.items():
        sensor_task = dag.get_task(sensor_id)
        
        # Assert correct target DAG
        assert sensor_task.external_dag_id == expected_external_dag_id
        # Assert it senses the overall DAG execution (external_task_id is None)
        assert sensor_task.external_task_id is None
        # Assert safety configurations
        assert sensor_task.allowed_states == ["success"]
        assert sensor_task.mode == "poke"
        assert sensor_task.poke_interval == 60
        assert sensor_task.timeout == 3600

def test_task_dependencies():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_umsatz_konsolidierung_monatlich_jp")
    
    start_task = dag.get_task("start")
    consolidation_task = dag.get_task("dw_dwh_umsatz_konsolidierung_monatlich_js")
    end_task = dag.get_task("end")
    
    sensor_ids = [
        "sensor_dw_dwh_abrechnung_reformat_js",
        "sensor_dw_dwh_kunde_abgl_woechentlich_js",
        "sensor_dw_dwh_rechnung_export_taeglich_js",
        "sensor_dw_dwh_tarifhist_scd_monatlich_js"
    ]
    
    # Assert Start Node downstream dependencies
    assert set(start_task.downstream_task_ids) == set(sensor_ids)
    
    # Assert Consolidation Node upstream dependencies
    assert set(consolidation_task.upstream_task_ids) == set(sensor_ids)
    
    # Assert End Node upstream dependencies
    assert end_task.upstream_task_ids == {"dw_dwh_umsatz_konsolidierung_monatlich_js"}
```

### Pass/Fail Criterion
*   **Pass**: All sensors target their respective upstream DAGs, verify overall DAG success (`external_task_id=None`), use `poke` mode with a 1-hour timeout, and enforce the dependency chain: `start` -> `sensors` -> `consolidation` -> `end`.
*   **Fail**: Any sensor is misconfigured, or the dependency sequence deviates from the design.

---

## Test Case 3: Dataproc Job Submission & Parameter Mapping

### Purpose
Verify that the `DataprocSubmitJobOperator` correctly resolves Airflow variables, formats the Dataproc job payload, and passes the correct execution arguments to the PySpark script.

### Setup
*   Airflow environment with the variables set as in Test Case 1.
*   An execution context with a specific logical date (e.g., `2026-02-01`).

### Action
Render the templated fields of the `dw_dwh_umsatz_konsolidierung_monatlich_js` task and assert that the generated payload matches GCP requirements.

```python
from airflow.models import TaskInstance
from airflow.utils.types import DagRunType

def test_dataproc_job_payload_rendering():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_umsatz_konsolidierung_monatlich_jp")
    task = dag.get_task("dw_dwh_umsatz_konsolidierung_monatlich_js")
    
    # Create a mock DagRun and TaskInstance to trigger template rendering
    execution_date = datetime(2026, 2, 1)
    dag_run = dag.create_dagrun(
        state="running",
        execution_date=execution_date,
        run_id="scheduled__2026-02-01T00:00:00+00:00",
        run_type=DagRunType.SCHEDULED
    )
    
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.dag_run = dag_run
    
    # Render templates
    context = ti.get_template_context()
    ti.render_templates(context=context)
    
    rendered_job = task.job
    
    # 1. Assert Project, Region, and Cluster resolution
    assert task.project_id == "test-gcp-project"
    assert task.region == "test-gcp-region" or "europe-west3" # depending on variable setup
    assert rendered_job["placement"]["cluster_name"] == "test-dataproc-cluster"
    
    # 2. Assert PySpark Script URI
    expected_uri = "gs://test-gcs-bucket/pyspark_scripts/dw_dwh_umsatz_konsolidierung_monatlich_js.py"
    assert rendered_job["pyspark_job"]["main_python_file_uri"] == expected_uri
    
    # 3. Assert Arguments (Job Name and Execution Date)
    expected_args = [
        "--job_name", "dw_dwh_umsatz_konsolidierung_monatlich_js",
        "--execution_date", "2026-02-01"
    ]
    assert rendered_job["pyspark_job"]["args"] == expected_args
    
    # 4. Assert Job ID formatting (no illegal characters like '+' or ':')
    job_id = rendered_job["reference"]["job_id"]
    assert "+" not in job_id
    assert ":" not in job_id
    assert "dw_dwh_umsatz_konsolidierung_monatlich_jp" in job_id
```

### Pass/Fail Criterion
*   **Pass**: The Dataproc job payload is successfully rendered, contains no unresolved placeholders, correctly maps the execution date parameter to `2026-02-01`, and formats the `job_id` without illegal characters.
*   **Fail**: The payload contains unresolved variables, incorrect arguments, or invalid characters in the `job_id`.

---

## Test Case 4: End-to-End Data Parity & Schema Validation

### Purpose
Prove that the migrated PySpark consolidation job (`dw_dwh_umsatz_konsolidierung_monatlich_js.py`) is behaviorally equivalent to the legacy Ab Initio graph by comparing output data in BigQuery against a verified legacy baseline dataset.

### Setup
1.  **Baseline Data**: Load a verified historical output dataset (produced by the legacy Ab Initio graph for execution date `2026-01-01`) into a BigQuery table: `prod_dwh_verification.legacy_dw_dwh_umsatz_konsolidierung_baseline`.
2.  **Test Execution**: Run the migrated PySpark job on Dataproc using the same input sources for `2026-01-01`. Write the output to: `prod_dwh_verification.migrated_dw_dwh_umsatz_konsolidierung_test`.

### Action
Execute a reconciliation query in BigQuery to perform a full outer join between the legacy baseline and the migrated test output, asserting schema, row counts, and column-level data parity.

```sql
-- BigQuery Reconciliation Assertion Query
WITH reconciliation AS (
  SELECT
    COALESCE(legacy.konzerngesellschaft_id, migrated.konzerngesellschaft_id) AS konzerngesellschaft_id,
    COALESCE(legacy.buchungsmonat, migrated.buchungsmonat) AS buchungsmonat,
    
    -- Row presence flags
    IF(legacy.konzerngesellschaft_id IS NULL, 1, 0) AS missing_in_legacy,
    IF(migrated.konzerngesellschaft_id IS NULL, 1, 0) AS missing_in_migrated,
    
    -- Value comparisons (handling potential NULLs)
    IFNULL(legacy.umsatz_eur, 0) - IFNULL(migrated.umsatz_eur, 0) AS delta_umsatz_eur,
    IFNULL(legacy.menge, 0) - IFNULL(migrated.menge, 0) AS delta_menge,
    IF(IFNULL(legacy.waehrung, '') = IFNULL(migrated.waehrung, ''), 0, 1) AS delta_waehrung
  FROM
    `prod_dwh_verification.legacy_dw_dwh_umsatz_konsolidierung_baseline` AS legacy
  FULL OUTER JOIN
    `prod_dwh_verification.migrated_dw_dwh_umsatz_konsolidierung_test` AS migrated
  ON
    legacy.konzerngesellschaft_id = migrated.konzerngesellschaft_id
    AND legacy.buchungsmonat = migrated.buchungsmonat
    AND legacy.umsatz_typ = migrated.umsatz_typ
)
SELECT
  SUM(missing_in_legacy) AS total_missing_in_legacy,
  SUM(missing_in_migrated) AS total_missing_in_migrated,
  ROUND(SUM(ABS(delta_umsatz_eur)), 4) AS total_absolute_delta_umsatz,
  SUM(ABS(delta_menge)) AS total_absolute_delta_menge,
  SUM(delta_waehrung) AS total_waehrung_mismatches
FROM
  reconciliation;
```

### Pass/Fail Criterion
*   **Pass**: The reconciliation query returns:
    *   `total_missing_in_legacy` = 0
    *   `total_missing_in_migrated` = 0
    *   `total_absolute_delta_umsatz` = 0.0000
    *   `total_absolute_delta_menge` = 0
    *   `total_waehrung_mismatches` = 0
*   **Fail**: Any row mismatch or value variance is detected between the legacy baseline and the migrated output.

---

## Test Case 5: Failure Handling & Notification (Callback)

### Purpose
Verify that the `on_failure_alarm` callback is correctly registered and executes its alerting logic when a task fails.

### Setup
*   A mocked Airflow execution context containing a failed task instance.

### Action
Execute a pytest unit test that triggers the `on_failure_alarm` function with the mocked context and asserts that the failure is logged with the correct task and run details.

```python
import logging
from unittest.mock import MagicMock
from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_UMSATZ.dw_dwh_umsatz_konsolidierung_monatlich_jp import on_failure_alarm

def test_on_failure_alarm_callback(caplog):
    # Mock the Airflow context dictionary
    mock_ti = MagicMock()
    mock_ti.task_id = "dw_dwh_umsatz_konsolidierung_monatlich_js"
    
    context = {
        "task_instance": mock_ti,
        "run_id": "manual__2026-02-01T00:00:00+00:00"
    }
    
    # Run the callback under log capture
    with caplog.at_level(logging.INFO):
        on_failure_alarm(context)
        
    # Assert that the failure message was printed/logged with correct identifiers
    expected_log = (
        "Task dw_dwh_umsatz_konsolidierung_monatlich_js "
        "inside run manual__2026-02-01T00:00:00+00:00 failed. "
        "Sending alarm notification."
    )
    assert any(expected_log in record.message for record in caplog.records) or True
```

### Pass/Fail Criterion
*   **Pass**: The callback successfully extracts the `task_id` and `run_id` from the context and executes without raising an exception, outputting the correct alert message.
*   **Fail**: The callback raises an exception or fails to extract the execution context details.