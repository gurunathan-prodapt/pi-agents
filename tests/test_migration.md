# Migration Validation Test Suite: `k_ausd_bp_ta_bpr_apn.ksh`

This document defines the migration-validation test suite to verify that the migrated BigQuery stored procedures (`r_ausd_bp_ta_bpr_apn` and `d_ausd_bp_ta_bpr_apn`) and the Airflow orchestration DAG behave identically to the legacy KornShell script.

---

## Test Strategy Overview

The validation strategy focuses on verifying:
1. **Parameter Validation & Exception Handling**: Ensuring that missing or malformed inputs trigger the exact error messages and abort behaviors seen in the legacy script.
2. **Date Computation Parity**: Verifying that the replacement of `gestern.ksh` with BigQuery's native date functions yields correct date boundaries.
3. **State & Metadata Persistence**: Ensuring that the legacy filesystem-based record count (`.tmp` file) and the commented-out FOS job registration are correctly unified into the BigQuery `job_control_table`.
4. **End-to-End Execution**: Verifying that the wrapper procedure correctly orchestrates the inner business logic procedure and registers the execution status.

---

## Test Case 1: Parameter Validation and Exception Handling

### Purpose
Verify that the stored procedure `r_ausd_bp_ta_bpr_apn` rejects invalid inputs with the correct error messages, mimicking the legacy `getopts` and `pruefeParameterGesetzt` validation logic.

### Setup
Ensure the target dataset exists. No data needs to be pre-populated in the target tables for this test, as the execution should fail before reaching the database write phase.

### Action
Execute the stored procedure with various invalid parameter combinations using the following SQL test script:

```sql
-- Test 1.1: Missing JobKennung
BEGIN
  CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
    NULL, '10001', '31122024', '0'
  );
  SELECT 'FAIL: Expected exception for missing Jobkennung' AS test_result;
EXCEPTION WHEN ERROR THEN
  IF @@error.message LIKE '%Jobkennung fehlt%' THEN
    SELECT 'PASS: Missing Jobkennung handled correctly' AS test_result;
  ELSE
    SELECT CONCAT('FAIL: Unexpected error message: ', @@error.message) AS test_result;
  END IF;
END;

-- Test 1.2: Missing EintragsNr
BEGIN
  CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
    'JOB_APN_01', '', '31122024', '0'
  );
  SELECT 'FAIL: Expected exception for missing EintragsNr' AS test_result;
EXCEPTION WHEN ERROR THEN
  IF @@error.message LIKE '%EintragsNr fehlt%' THEN
    SELECT 'PASS: Missing EintragsNr handled correctly' AS test_result;
  ELSE
    SELECT CONCAT('FAIL: Unexpected error message: ', @@error.message) AS test_result;
  END IF;
END;

-- Test 1.3: Invalid Date Format (DDMMYYYY expected)
BEGIN
  CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
    'JOB_APN_01', '10001', '2024-12-31', '0'
  );
  SELECT 'FAIL: Expected exception for invalid date format' AS test_result;
EXCEPTION WHEN ERROR THEN
  IF @@error.message LIKE '%Ungültiges Datum%' THEN
    SELECT 'PASS: Invalid date format handled correctly' AS test_result;
  ELSE
    SELECT CONCAT('FAIL: Unexpected error message: ', @@error.message) AS test_result;
  END IF;
END;
```

### Pass/Fail Criterion
* **Pass**: All three test blocks catch the expected exceptions and output `PASS`.
* **Fail**: Any block completes without throwing an error, or throws an error message that does not match the expected validation failure text.

---

## Test Case 2: Date Computation and Context Parity

### Purpose
Verify that the date computation logic (replacing `gestern.ksh`) correctly calculates `p_datum_heute` (today) and `p_datum_gestern` (yesterday) and passes them to the inner business logic procedure.

### Setup
1. Truncate the target table: `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt`.
2. Create a temporary test harness to capture the parameters passed to the inner procedure.

### Action
Run a test execution of the wrapper procedure using a Python `pytest` script that validates the written date values in the target table.

```python
import os
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_date_computation_parity(bq_client):
    project_id = os.environ.get("GCP_PROJECT_ID", "prod-data-platform")
    dataset = os.environ.get("GCP_DATASET", "isbert_schema")
    
    # Clean target table
    bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset}.poolbasisprodukt`").result()
    
    # Execute wrapper procedure
    stichtag = "15082024" # 15th Aug 2024
    query = f"""
    CALL `{project_id}.{dataset}.r_ausd_bp_ta_bpr_apn`(
      'TEST_DATE_JOB', '99999', '{stichtag}', '0'
    )
    """
    bq_client.query(query).result()
    
    # Retrieve the written records to verify date context passed to inner SP
    verify_query = f"""
    SELECT datum_heute, datum_gestern 
    FROM `{project_id}.{dataset}.poolbasisprodukt`
    WHERE job_kennung = 'TEST_DATE_JOB' AND eintrags_nr = '99999'
    LIMIT 1
    """
    results = list(bq_client.query(verify_query).result())
    
    assert len(results) == 1, "No record written by the inner procedure."
    
    row = results[0]
    expected_heute = datetime.utcnow().date()
    expected_gestern = expected_heute - timedelta(days=1)
    
    assert row.datum_heute == expected_heute, f"Expected heute: {expected_heute}, got: {row.datum_heute}"
    assert row.datum_gestern == expected_gestern, f"Expected gestern: {expected_gestern}, got: {row.datum_gestern}"
```

### Pass/Fail Criterion
* **Pass**: The target table contains the records with `datum_heute` equal to the current system date and `datum_gestern` equal to the current system date minus one day.
* **Fail**: The dates are null, incorrect, or the procedure fails to execute.

---

## Test Case 3: End-to-End Execution and Metadata Logging (Happy Path)

### Purpose
Verify that a successful run of the wrapper procedure:
1. Executes the inner business logic (`d_ausd_bp_ta_bpr_apn`).
2. Correctly counts the processed records (replacing the legacy `$tmpFile` mechanism).
3. Inserts a matching control record into `job_control_table` with correct metadata.

### Setup
1. Truncate both `poolbasisprodukt` and `job_control_table`.
2. Seed the inner procedure's source logic if necessary (the mock inner procedure writes directly to `poolbasisprodukt`).

### Action
Execute the wrapper procedure and run validation queries.

```sql
-- Step 1: Clean tables
TRUNCATE TABLE `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt`;
TRUNCATE TABLE `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table`;

-- Step 2: Execute wrapper
CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
  'JOB_E2E_01',
  '20002',
  '25122024',
  '0'
);

-- Step 3: Assertions on Target Output Table
ASSERT (
  SELECT COUNT(*) 
  FROM `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt`
  WHERE job_kennung = 'JOB_E2E_01' AND stichtag = '2024-12-25'
) = 1 
AS 'ERROR: Target table poolbasisprodukt was not populated correctly';

-- Step 4: Assertions on Job Control Table
ASSERT (
  SELECT COUNT(*) 
  FROM `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table`
  WHERE job_kennung = 'JOB_E2E_01'
    AND eintrags_nr = '20002'
    AND stichtag = '2024-12-25'
    AND tab_name = 'PoolBasisprodukt'
    AND record_count = 1
    AND status_code = 'A'
    AND process_type = 'I'
    AND active_flag = 'N'
) = 1 
AS 'ERROR: Job control table metadata mismatch or missing entry';
```

### Pass/Fail Criterion
* **Pass**: Both `ASSERT` statements execute successfully without throwing errors, proving that the record count was captured and the metadata was correctly persisted.
* **Fail**: Any `ASSERT` statement fails, indicating a mismatch in row counts, status codes, or missing records.

---

## Test Case 4: Restart Value Defaulting and Boundary Handling

### Purpose
Verify that the parameter `p_wiederanlaufWert` (restart value) defaults to `'0'` when passed as `NULL` or empty string, and is preserved when a non-empty value is provided.

### Setup
Truncate `poolbasisprodukt` and `job_control_table`.

### Action
Execute the procedure twice: once with an empty string/NULL for the restart value, and once with an explicit value.

```sql
-- Clean up
TRUNCATE TABLE `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt`;
TRUNCATE TABLE `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table`;

-- Run 1: Test defaulting (NULL value)
CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
  'JOB_RESTART_NULL', '30001', '01012024', NULL
);

-- Run 2: Test explicit value ('5')
CALL `${GCP_PROJECT_ID}.${GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
  'JOB_RESTART_VAL', '30002', '01012024', '5'
);

-- Assertions
ASSERT (
  SELECT restart_value 
  FROM `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table`
  WHERE job_kennung = 'JOB_RESTART_NULL'
) = '0'
AS 'ERROR: Restart value did not default to 0 for NULL input';

ASSERT (
  SELECT restart_value 
  FROM `${GCP_PROJECT_ID}.${GCP_DATASET}.job_control_table`
  WHERE job_kennung = 'JOB_RESTART_VAL'
) = '5'
AS 'ERROR: Explicit restart value of 5 was not preserved';
```

### Pass/Fail Criterion
* **Pass**: The first run registers a restart value of `'0'` and the second run registers `'5'` in the control table.
* **Fail**: The default value is not applied, or the explicit value is overwritten.

---

## Test Case 5: Airflow DAG Parameter Mapping and Orchestration

### Purpose
Verify that the Airflow DAG `dag_r_ausd_bp_ta_bpr_apn` correctly parses and maps runtime configuration parameters to the BigQuery stored procedure call.

### Setup
A Python testing environment with `apache-airflow` installed.

### Action
Execute a unit test to render the Airflow DAG templates and verify the generated SQL query parameters.

```python
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from datetime import datetime

def test_dag_parameter_rendering():
    dagbag = DagBag(dag_folder="gcp_target/dags", include_examples=False)
    dag = dagbag.get_dag("dag_r_ausd_bp_ta_bpr_apn")
    
    assert dag is not None, "Failed to load DAG"
    
    # Create a mock DAG run with specific configuration parameters
    conf = {
        "p_JobKennung": "AIRFLOW_TEST_JOB",
        "p_EintragsNr": "77777",
        "p_Stichtag": "12122024",
        "p_wiederanlaufWert": "3"
    }
    
    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_id="test_run_1",
        run_type=DagRunType.MANUAL,
        execution_date=datetime(2024, 1, 1),
        state=DagRunState.RUNNING,
        conf=conf
    )
    
    # Get the BigQuery task
    task = dag.get_task("run_r_ausd_bp_ta_bpr_apn")
    
    # Create template context
    context = dag_run.get_template_context()
    context["task"] = task
    
    # Render templates
    rendered_query = task.render_template(task.configuration["query"]["query"], context)
    rendered_params = task.render_template(task.configuration["query"]["queryParameters"], context)
    
    # Assertions on rendered parameters
    param_dict = {p["name"]: p["parameterValue"]["value"] for p in rendered_params}
    
    assert param_dict["p_JobKennung"] == "AIRFLOW_TEST_JOB"
    assert param_dict["p_EintragsNr"] == "77777"
    assert param_dict["p_Stichtag"] == "12122024"
    assert param_dict["p_wiederanlaufWert"] == "3"
```

### Pass/Fail Criterion
* **Pass**: The Airflow DAG parses successfully without syntax errors, and the rendered parameters match the input configuration dictionary exactly.
* **Fail**: The DAG fails to load, or the parameters are not correctly mapped to the BigQuery operator's query parameters.