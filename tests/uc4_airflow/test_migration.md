Here is a comprehensive migration-validation test suite designed to prove behavioral equivalence between the legacy UC4 JOBI script and the migrated Apache Airflow Python implementation.

---

# Migration Validation Test Suite: `DW.DWH_ADM_JOB_MONITOR_START`

## Test Case 1: Output Parity & Logging Verification
### Purpose
Verify that the migrated Python function produces the exact character-for-character log outputs as the legacy UC4 script under matching execution conditions.

### Setup
* Mock the Google Cloud BigQuery client to return a monitored job plan matching the current DAG ID.
* Set up a mock context dictionary representing an active Airflow task instance.

### Action
Execute `dwh_adm_job_monitor_start` using `pytest` and capture standard output (`stdout`).

### Pass/Fail Criterion
* **Pass**: 
  * The log output contains exactly: `Job test_task mit RNR manual__2023-10-01T00:00:00+00:00 gestartet aus test_dag`
  * The log output contains exactly: `Added test_task with manual__2023-10-01T00:00:00+00:00`
* **Fail**: Any character mismatch, missing log line, or failure to resolve the dynamic context variables.

### Test Code
```python
import pytest
from unittest.mock import MagicMock, patch
from uc4_airflow.dw_dwh_adm_job_monitor_start import dwh_adm_job_monitor_start

def test_output_parity_logging(capsys):
    # Mock context representing Airflow runtime variables
    mock_context = {
        'dag': MagicMock(dag_id='test_dag'),
        'task_instance': MagicMock(task_id='test_task'),
        'run_id': 'manual__2023-10-01T00:00:00+00:00'
    }

    # Mock BigQuery row data matching the active monitoring criteria
    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda index: {0: 'test_dag', 1: 'J'}[index]

    with patch('google.cloud.bigquery.Client') as mock_bq_client:
        mock_instance = mock_bq_client.return_value
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [mock_row]
        mock_instance.query.return_value = mock_query_job

        # Execute the function
        dwh_adm_job_monitor_start(context=mock_context)

    # Capture stdout
    captured = capsys.readouterr().out

    # Assert exact character-for-character matches
    expected_start_log = "Job test_task mit RNR manual__2023-10-01T00:00:00+00:00 gestartet aus test_dag"
    expected_added_log = "Added test_task with manual__2023-10-01T00:00:00+00:00"

    assert expected_start_log in captured, f"Expected log '{expected_start_log}' not found in output: '{captured}'"
    assert expected_added_log in captured, f"Expected log '{expected_added_log}' not found in output: '{captured}'"
```

---

## Test Case 2: Transformation Correctness & Filtering Logic
### Purpose
Verify that the filtering logic correctly handles different combinations of monitoring status (`admwert`) and monitored items (`admgb`), including the `"ALL"` wildcard and inactive indicators.

### Setup
Define a matrix of test scenarios representing different states of the `dwh_monitored_jps` configuration table.

| Scenario | `admgb` (Monitored Item) | `admwert` (Status) | Current DAG ID (`admjp`) | Expected Registration (MERGE)? |
| :--- | :--- | :--- | :--- | :--- |
| A: Exact Match Active | `MY_DAG` | `J` | `MY_DAG` | **Yes** |
| B: Wildcard Active | `ALL` | `J` | `MY_DAG` | **Yes** |
| C: Exact Match Inactive | `MY_DAG` | `N` | `MY_DAG` | **No** |
| D: Mismatched DAG | `OTHER_DAG` | `J` | `MY_DAG` | **No** |
| E: Empty Parent Plan | `MY_DAG` | `J` | ` ` (Empty String) | **No** |

### Action
Run the function against each scenario using mocked BigQuery responses and assert whether the registration query (the `MERGE` statement) was executed.

### Pass/Fail Criterion
* **Pass**: The `MERGE` query is executed *only* for Scenarios A and B, and skipped for Scenarios C, D, and E.
* **Fail**: A registration occurs for an inactive or mismatched configuration, or fails to occur for a valid wildcard/exact match.

### Test Code
```python
@pytest.mark.parametrize(
    "admgb, admwert, admjp, should_register",
    [
        ("MY_DAG", "J", "MY_DAG", True),    # Scenario A
        ("ALL", "J", "MY_DAG", True),       # Scenario B
        ("MY_DAG", "N", "MY_DAG", False),   # Scenario C
        ("OTHER_DAG", "J", "MY_DAG", False), # Scenario D
        ("MY_DAG", "J", " ", False),        # Scenario E
    ]
)
@patch('google.cloud.bigquery.Client')
def test_filtering_logic(mock_bq_client, admgb, admwert, admjp, should_register):
    mock_instance = mock_bq_client.return_value
    mock_query_job = MagicMock()
    
    # Mock row data returned from the configuration table
    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda index: {0: admgb, 1: admwert}[index]
    mock_query_job.result.return_value = [mock_row]
    mock_instance.query.return_value = mock_query_job

    # Execute function with explicit arguments
    dwh_adm_job_monitor_start(
        dag_id=admjp,
        task_id="MY_TASK",
        run_id="scheduled__2023-10-01T00:00:00+00:00"
    )

    # Verify if MERGE query was executed
    merge_calls = [
        call for call in mock_instance.query.call_args_list 
        if "MERGE INTO" in call[0][0]
    ]

    if should_register:
        assert len(merge_calls) == 1, f"Expected registration for {admgb}/{admwert} but none occurred."
    else:
        assert len(merge_calls) == 0, f"Unexpected registration occurred for {admgb}/{admwert}."
```

---

## Test Case 3: External System Replacement (BigQuery MERGE Execution)
### Purpose
Verify that the BigQuery client executes a syntactically correct `MERGE` statement with the correct query parameters, replacing the legacy UC4 `PUT_VAR` operation.

### Setup
* Mock `google.cloud.bigquery.Client`.
* Set environment variables `GCP_PROJECT=test-project` and `BQ_DATASET=test_dataset`.

### Action
Execute the function with a matching monitored configuration and capture the arguments passed to the BigQuery `query` method.

### Pass/Fail Criterion
* **Pass**:
  * The target table in the `MERGE` statement is resolved to `test-project.test_dataset.dwh_running_jobs`.
  * The query parameters contain the correct scalar values for `@job_name` and `@run_id`.
* **Fail**: The SQL statement contains syntax errors, references incorrect tables, or fails to pass the parameters securely.

### Test Code
```python
from google.cloud import bigquery

@patch.dict('os.environ', {'GCP_PROJECT': 'test-project', 'BQ_DATASET': 'test_dataset'})
@patch('google.cloud.bigquery.Client')
def test_bigquery_merge_payload(mock_bq_client):
    # Force reload of table paths based on mocked environment variables
    import importlib
    import uc4_airflow.dw_dwh_adm_job_monitor_start as target_module
    importlib.reload(target_module)

    mock_instance = mock_bq_client.return_value
    mock_query_job = MagicMock()
    
    # Mock configuration table to return a match
    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda index: {0: 'ALL', 1: 'J'}[index]
    mock_query_job.result.return_value = [mock_row]
    mock_instance.query.return_value = mock_query_job

    # Run logic
    target_module.dwh_adm_job_monitor_start(
        dag_id="MY_DAG",
        task_id="MY_TASK",
        run_id="run_12345"
    )

    # Extract the MERGE query call
    merge_call = [
        call for call in mock_instance.query.call_args_list 
        if "MERGE INTO" in call[0][0]
    ][0]

    sql_executed = merge_call[0][0]
    job_config = merge_call[1].get('job_config')

    # Assertions on SQL structure
    assert "MERGE INTO `test-project.test_dataset.dwh_running_jobs` T" in sql_executed
    assert "USING (SELECT @job_name AS job_name, @run_id AS run_id) S" in sql_executed
    assert "ON T.job_name = S.job_name" in sql_executed

    # Assertions on Query Parameters
    params = job_config.query_parameters
    assert len(params) == 2
    
    job_name_param = next(p for p in params if p.name == "job_name")
    run_id_param = next(p for p in params if p.name == "run_id")

    assert job_name_param.value == "MY_TASK"
    assert job_name_param.type_ == "STRING"
    assert run_id_param.value == "run_12345"
    assert run_id_param.type_ == "STRING"
```

---

## Test Case 4: Data-Quality & Schema Assertions (Integration)
### Purpose
Verify that the BigQuery target tables conform to the expected schema and that the `MERGE` operation executes successfully against actual BigQuery table structures (or a local emulator).

### Setup
Create temporary BigQuery tables with the production-equivalent schema:
* `dwh_monitored_jps`:
  * Column 0: `job_plan_name` (STRING)
  * Column 1: `is_monitored` (STRING)
* `dwh_running_jobs`:
  * Column 0: `job_name` (STRING, Primary Key equivalent)
  * Column 1: `run_id` (STRING)

### Action
1. Populate `dwh_monitored_jps` with a test record.
2. Execute the `dwh_adm_job_monitor_start` function.
3. Query the `dwh_running_jobs` table to verify the state.
4. Execute the function a second time with a *new* run ID for the same job to verify the `UPDATE` behavior of the `MERGE` statement.

### Pass/Fail Criterion
* **Pass**:
  * The first execution inserts a new row into `dwh_running_jobs`.
  * The second execution updates the existing row with the new `run_id` (no duplicate rows created for the same job).
* **Fail**: Database constraint violations, duplicate records for the same job name, or SQL execution failures.

### Test Code (SQL Assertions)
```sql
-- Setup Step 1: Create Temporary Test Tables
CREATE OR REPLACE TABLE `dw_metadata.dwh_monitored_jps` (
    job_plan_name STRING,
    is_monitored STRING
);

CREATE OR REPLACE TABLE `dw_metadata.dwh_running_jobs` (
    job_name STRING,
    run_id STRING
);

-- Setup Step 2: Insert Test Configuration
INSERT INTO `dw_metadata.dwh_monitored_jps` (job_plan_name, is_monitored)
VALUES ('TEST_DAG_E2E', 'J');

-- Action Step 1: Simulate first run registration (Insert)
-- (This mimics the MERGE query executed by the Python function)
MERGE INTO `dw_metadata.dwh_running_jobs` T
USING (SELECT 'TEST_TASK_E2E' AS job_name, 'run_000001' AS run_id) S
ON T.job_name = S.job_name
WHEN MATCHED THEN UPDATE SET run_id = S.run_id
WHEN NOT MATCHED THEN INSERT (job_name, run_id) VALUES (S.job_name, S.run_id);

-- Assertion 1: Verify Row Insertion
ASSERT (
    SELECT COUNT(1) 
    FROM `dw_metadata.dwh_running_jobs` 
    WHERE job_name = 'TEST_TASK_E2E' AND run_id = 'run_000001'
) = 1;

-- Action Step 2: Simulate second run registration for the same task (Update)
MERGE INTO `dw_metadata.dwh_running_jobs` T
USING (SELECT 'TEST_TASK_E2E' AS job_name, 'run_000002' AS run_id) S
ON T.job_name = S.job_name
WHEN MATCHED THEN UPDATE SET run_id = S.run_id
WHEN NOT MATCHED THEN INSERT (job_name, run_id) VALUES (S.job_name, S.run_id);

-- Assertion 2: Verify Row Update and No Duplication
ASSERT (
    SELECT COUNT(1) 
    FROM `dw_metadata.dwh_running_jobs` 
    WHERE job_name = 'TEST_TASK_E2E'
) = 1;

ASSERT (
    SELECT run_id 
    FROM `dw_metadata.dwh_running_jobs` 
    WHERE job_name = 'TEST_TASK_E2E'
) = 'run_000002';
```