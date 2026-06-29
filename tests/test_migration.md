# Migration Validation Test Suite: `k_aurd_rechstan.ksh`

This document defines the migration-validation test suite to verify that the migrated BigQuery stored procedures and Apache Airflow DAG behave identically to the legacy KornShell script `k_aurd_rechstan.ksh`.

---

## Test Suite Overview

The test suite is divided into four main validation areas:
1. **Parameter Validation & Error Logging** (Edge cases, missing arguments, invalid formats)
2. **Date Derivation & Defaulting Logic** (Restart values, yesterday/today calculations)
3. **Job Bookkeeping & Record Count Parity** (State tracking, target table counts)
4. **End-to-End Orchestration Validation** (Airflow DAG parameter mapping)

### Environment Configuration
All tests assume the following environment variables or configurations are set:
* **GCP Project ID:** `gcp-isbert-prod`
* **Dataset:** `isbert_aufbereitung`
* **Target Table:** `RKopfStan`
* **Metadata Tables:** `job_table`, `job_error_log`

---

## Section 1: Parameter Validation & Error Logging

### Test Case 1.1: Missing Required Parameters
#### Purpose
Verify that the stored procedure rejects execution and logs an error when any of the mandatory parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`) are missing or empty, matching the legacy `pruefeParameterGesetzt` behavior.

#### Setup
Ensure the `job_error_log` table is cleared of test entries for the run identifier.
```sql
DELETE FROM `gcp-isbert-prod.isbert_aufbereitung.job_error_log` 
WHERE entry_nr = 'TEST_ERR_01';
```

#### Action
Execute the main control procedure with a missing `p_JobKennung` (passed as `NULL`):
```sql
CALL `gcp-isbert-prod.isbert_aufbereitung.r_aurd_rechstan_control`(
  NULL,          -- p_JobKennung (Missing)
  'TEST_ERR_01', -- p_EintragsNr
  '31122023',    -- p_Stichtag
  '0'            -- p_wiederanlaufWert
);
```

#### Pass/Fail Criterion
* **Pass:** 
  1. The procedure returns a result set containing the message: `'FEHLER: 0 E 1 Jobkennung fehlt'`.
  2. A row is written to `gcp-isbert-prod.isbert_aufbereitung.job_error_log` matching the assertion query below.
* **Fail:** The procedure executes without returning an error message, or fails to log the error in the metadata table.

```sql
-- ASSERTION QUERY
SELECT 
  job_name, 
  entry_nr, 
  error_code, 
  error_message 
FROM `gcp-isbert-prod.isbert_aufbereitung.job_error_log`
WHERE entry_nr = 'TEST_ERR_01'
  AND job_name = 'r_aurd_rechstan'
  AND error_code = 1
  AND error_message = 'Jobkennung fehlt';
```

---

### Test Case 1.2: Invalid Date Format Validation
#### Purpose
Verify that the date validation helper `sp_validate_ddmmyyyy` correctly identifies non-`DDMMYYYY` formats and raises an exception, matching the legacy `DWDate_Datum_Check` behavior.

#### Setup
Clear the error log for the test entry.
```sql
DELETE FROM `gcp-isbert-prod.isbert_aufbereitung.job_error_log` 
WHERE entry_nr = 'TEST_ERR_02';
```

#### Action
Execute the control procedure with an invalid date format (`2023-12-31` instead of `31122023`):
```sql
DECLARE date_error BOOLEAN DEFAULT FALSE;

BEGIN
  CALL `gcp-isbert-prod.isbert_aufbereitung.r_aurd_rechstan_control`(
    'JOB_VAL_02',
    'TEST_ERR_02',
    '2023-12-31', -- Invalid Format (YYYY-MM-DD)
    '0'
  );
EXCEPTION WHEN ERROR THEN
  SET date_error = TRUE;
END;

SELECT date_error AS exception_raised;
```

#### Pass/Fail Criterion
* **Pass:** 
  1. `exception_raised` is `TRUE` (the procedure raised a GoogleSQL exception).
  2. A row is written to `job_error_log` with `error_code = 193` and `error_message = 'Ungueltiges Datum im Format DDMMYYYY'`.
* **Fail:** The procedure completes successfully or logs an incorrect error code.

```sql
-- ASSERTION QUERY
SELECT 
  job_name, 
  entry_nr, 
  error_code, 
  error_message 
FROM `gcp-isbert-prod.isbert_aufbereitung.job_error_log`
WHERE entry_nr = 'TEST_ERR_02'
  AND error_code = 193
  AND error_message = 'Ungueltiges Datum im Format DDMMYYYY';
```

---

## Section 2: Date Derivation & Defaulting Logic

### Test Case 2.1: Restart Value Defaulting
#### Purpose
Verify that if `p_wiederanlaufWert` is passed as `NULL` or empty, it defaults to `'0'`, matching the legacy shell logic `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0`.

#### Setup
None required (isolated unit test of the helper procedure).

#### Action
Execute the helper procedure `sp_default_restart_value` with a `NULL` input:
```sql
DECLARE v_out_restart STRING;

CALL `gcp-isbert-prod.isbert_aufbereitung.sp_default_restart_value`(
  NULL, 
  v_out_restart
);

SELECT v_out_restart AS resolved_restart_value;
```

#### Pass/Fail Criterion
* **Pass:** `resolved_restart_value` is returned as `'0'`.
* **Fail:** `resolved_restart_value` is returned as `NULL` or any value other than `'0'`.

---

### Test Case 2.2: System Date Derivation Parity
#### Purpose
Verify that the stored procedure's internal date variables (`v_datum_heute` and `v_datum_gestern`) match the legacy `gestern.ksh` output (Today and Yesterday).

#### Setup
None.

#### Action
Run a validation query comparing the BigQuery date functions against the expected system date logic:
```sql
SELECT 
  CURRENT_DATE() AS expected_heute,
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS expected_gestern;
```

#### Pass/Fail Criterion
* **Pass:** The derived dates match the current system date and the previous calendar day exactly.
* **Fail:** Any timezone mismatch or calculation error causes the dates to drift.

---

## Section 3: Job Bookkeeping & Record Count Parity

### Test Case 3.1: Job Table Entry and Record Count Verification
#### Purpose
Verify that upon successful execution, the procedure counts the records in the target table `RKopfStan` for the given `Stichtag` and writes a correct tracking entry to `job_table`, matching the legacy `FOSJobErzeugeEintrag` behavior.

#### Setup
1. Clean up existing test records in `RKopfStan` and `job_table`.
2. Insert mock records into `RKopfStan` representing the processed data for the business date `2023-12-31` (Stichtag: `31122023`).

```sql
-- Clean up
DELETE FROM `gcp-isbert-prod.isbert_aufbereitung.RKopfStan` WHERE stichtag_from = '2023-12-31';
DELETE FROM `gcp-isbert-prod.isbert_aufbereitung.job_table` WHERE eintrags_nr = 'ENTRY_QA_99';

-- Insert 3 mock records
INSERT INTO `gcp-isbert-prod.isbert_aufbereitung.RKopfStan` (stichtag_from, record_id)
VALUES 
  ('2023-12-31', 'REC_01'),
  ('2023-12-31', 'REC_02'),
  ('2023-12-31', 'REC_03');
```

#### Action
Execute the main control procedure for the target Stichtag:
```sql
CALL `gcp-isbert-prod.isbert_aufbereitung.r_aurd_rechstan_control`(
  'JOB_QA_99',
  'ENTRY_QA_99',
  '31122023',
  '0'
);
```

#### Pass/Fail Criterion
* **Pass:** A new entry is created in `job_table` with:
  * `table_name = 'RKopfStan'`
  * `status_code = 'A'`
  * `active_flag = 'I'`
  * `stichtag_from = '2023-12-31'`
  * `stichtag_to = '2023-12-31'`
  * `record_count = 3` (matching the number of mock records inserted)
  * `job_kennung = 'JOB_QA_99'`
  * `eintrags_nr = 'ENTRY_QA_99'`
* **Fail:** No entry is created, or the `record_count` does not match the target table count.

```sql
-- ASSERTION QUERY
SELECT 
  table_name,
  status_code,
  active_flag,
  stichtag_from,
  stichtag_to,
  record_count,
  job_kennung,
  eintrags_nr
FROM `gcp-isbert-prod.isbert_aufbereitung.job_table`
WHERE eintrags_nr = 'ENTRY_QA_99';
```

---

## Section 4: End-to-End Orchestration Validation

### Test Case 4.1: Airflow DAG Parameter Mapping & Compilation
#### Purpose
Verify that the Airflow DAG `k_aurd_rechstan_dag` parses correctly, maps configuration parameters from `dag_run.conf` to the BigQuery operator, and templates the execution arguments correctly.

#### Setup
Install `pytest` and `apache-airflow` in the test execution environment.

#### Action
Run the following Python unit test to validate DAG structure and parameter templating:

```python
# test_k_aurd_rechstan_dag.py
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.context import Context

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dagbag):
    """Verify that the DAG loads without import errors."""
    dag = dagbag.get_dag(dag_id="k_aurd_rechstan_dag")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

def test_dag_parameter_templating(dagbag):
    """Verify that the BigQuery task templates parameters correctly from conf."""
    dag = dagbag.get_dag(dag_id="k_aurd_rechstan_dag")
    task = dag.get_task("run_control_procedure")
    
    # Mock a DAG run with custom configuration parameters
    conf = {
        "job_kennung": "JOB_TEST_AIRFLOW",
        "eintrags_nr": "999999",
        "stichtag": "15082024",
        "restart_value": "1"
    }
    
    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_id="test_run_1",
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        conf=conf
    )
    
    # Create execution context
    context = Context(
        dag=dag,
        task=task,
        dag_run=dag_run,
        params=dag.params
    )
    
    # Render templates
    rendered_query = task.render_template(task.configuration["query"]["query"], context)
    rendered_params = task.render_template(task.configuration["query"]["queryParameters"], context)
    
    # Assertions
    assert "r_aurd_rechstan_control" in rendered_query
    
    # Verify parameter values are correctly mapped into the query parameters block
    param_dict = {p["name"]: p["parameterValue"]["value"] for p in rendered_params}
    assert param_dict["job_kennung"] == "JOB_TEST_AIRFLOW"
    assert param_dict["eintrags_nr"] == "999999"
    assert param_dict["stichtag"] == "15082024"
    assert param_dict["restart_value"] == "1"
```

#### Pass/Fail Criterion
* **Pass:** The test suite passes with zero import errors and all parameter assertions successfully match the mocked `dag_run.conf` values.
* **Fail:** The DAG fails to load, or parameters are not correctly resolved during templating.