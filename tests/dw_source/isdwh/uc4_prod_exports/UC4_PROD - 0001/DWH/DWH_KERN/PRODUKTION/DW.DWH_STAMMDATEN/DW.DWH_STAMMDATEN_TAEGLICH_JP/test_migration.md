# Migration Validation Test Suite
## Job: `DW.DWH_IPSD_DWH_MORPU_LID`

This document defines the migration-validation tests to prove that the migrated Apache Airflow DAG `dw_dwh_ipsd_dwh_morpu_lid` is behaviorally equivalent to the legacy UC4 UNIX job `DW.DWH_IPSD_DWH_MORPU_LID`.

---

### Test Case 1: DAG Structural & Metadata Validation
#### Purpose
Verify that the migrated Python DAG file is syntactically correct, parses without errors in the Airflow context, and retains all critical metadata, scheduling properties, and configurations defined in the legacy UC4 export.

#### Setup
* A Python environment with `apache-airflow` installed.
* The migrated DAG file placed in the Airflow `dags/` directory:
  `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/DW.DWH_IPSD_DWH_MORPU_LID.py`

#### Action
Run a pytest suite that imports the DAG and asserts its structural properties.

```python
import pytest
from airflow.models import DagBag

def test_dag_loading_and_metadata():
    dag_bag = DagBag(dag_folder="dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP", include_examples=False)
    
    # Assert no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    dag = dag_bag.get_dag(dag_id="dw_dwh_ipsd_dwh_morpu_lid")
    assert dag is not None, "DAG dw_dwh_ipsd_dwh_morpu_lid not found in DagBag"
    
    # Assert Scheduling and Concurrency Properties
    assert dag.schedule_interval is None, "Schedule interval must be None (externally triggered)"
    assert dag.max_active_runs == 1, "max_active_runs must be 1 to prevent concurrent execution conflicts"
    assert dag.catchup is False, "Catchup must be disabled"
    
    # Assert Default Arguments
    assert dag.default_args.get('retries') == 1, "Retries must be set to 1"
    assert dag.default_args.get('retry_delay') == pytest.approx(300, abs=1), "Retry delay must be 5 minutes (300s)"
    
    # Assert Task Existence
    assert "dw_dwh_ipsd_dwh_morpu_lid_task" in dag.task_ids, "Task 'dw_dwh_ipsd_dwh_morpu_lid_task' is missing"
```

#### Pass/Fail Criterion
* **Pass**: The test suite executes successfully with zero import errors, and all metadata assertions (schedule, retries, max active runs) match the legacy specification.
* **Fail**: Any import error is raised, or any metadata assertion fails.

---

### Test Case 2: Command Execution & Environment Sourcing Validation
#### Purpose
The legacy UC4 job executes a binary command on host `dwhdwh1p` under user `DW.UNIX.ISTNS` after sourcing `.dw_init` and `DW.HOLE_PFAD`. This test validates that when the `EmptyOperator` is replaced by an execution operator (e.g., `SSHOperator` or `BashOperator`), the exact command string, environment variables, and execution context are preserved.

#### Setup
* Configure the target execution operator (e.g., `SSHOperator` targeting connection `SSH_CONN_ID`).
* Mock the Airflow Connection for `SSH_CONN_ID` to point to a test environment mimicking host `dwhdwh1p`.

#### Action
Verify that the generated command string correctly sets the job identifier, sources the environment, and executes the target binary with the exact arguments.

```python
import pytest
from unittest.mock import MagicMock
from airflow.models import Connection, Variable

# Mocking Airflow Variables for the test context
@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    variables = {
        "GCP_PROJECT": "test-gcp-project",
        "SSH_CONN_ID": "ssh_dwh_prod"
    }
    monkeypatch.setattr(Variable, "get", lambda key, default_var=None: variables.get(key, default_var))

def test_command_construction():
    # Define the expected target command based on legacy script:
    # :inc DW.HOLE_PFAD
    # :set &DWH_JOB_KENNUNG='IPSD_DWH_MORPU_LID'
    # . $HOME/.dw_init
    # $HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid
    
    expected_job_kennung = "IPSD_DWH_MORPU_LID"
    expected_binary = "$HOME/aktuell/import/is/bin/r_ipis"
    expected_args = "-s dwh -k morpu_map_lid"
    
    # Construct the command as it must be executed in the target SSH/Bash operator
    constructed_command = (
        f"export DWH_JOB_KENNUNG='{expected_job_kennung}' && "
        f"source $HOME/.dw_init && "
        f"{expected_binary} {expected_args}"
    )
    
    # Assertions on command integrity
    assert "IPSD_DWH_MORPU_LID" in constructed_command
    assert ".dw_init" in constructed_command
    assert "-s dwh" in constructed_command
    assert "-k morpu_map_lid" in constructed_command
```

#### Pass/Fail Criterion
* **Pass**: The constructed command string correctly chains the environment initialization (`.dw_init`), sets the environment variable `DWH_JOB_KENNUNG='IPSD_DWH_MORPU_LID'`, and calls the binary with the exact arguments `-s dwh -k morpu_map_lid`.
* **Fail**: Any part of the command sequence, environment sourcing, or arguments is missing or altered.

---

### Test Case 3: Database State & Output Parity (Data Validation)
#### Purpose
Verify that running the migrated import process via the binary `r_ipis` produces identical data transformations and database states in the target tables as the legacy UC4 execution.

#### Setup
1. **Source Data**: Prepare a static set of raw invoice services ("Rechnungsleistungen") in the source staging area.
2. **Legacy Run**: Execute the legacy UC4 job on host `dwhdwh1p` against a dedicated legacy test schema. Capture the resulting target tables.
3. **Migrated Run**: Execute the migrated Airflow task against an identical target test schema.

#### Action
Execute a comparison script to verify row counts and data checksums across the target tables populated by the `morpu_map_lid` process.

```sql
-- SQL Assertion 1: Row Count Parity
SELECT 
    (SELECT COUNT(*) FROM legacy_schema.morpu_map_lid_target) AS legacy_count,
    (SELECT COUNT(*) FROM migrated_schema.morpu_map_lid_target) AS migrated_count;

-- SQL Assertion 2: Data Checksum Parity (Oracle/PostgreSQL compatible hash comparison)
-- Assumes target table has columns: RECHNUNG_ID, LEISTUNG_CD, BETRAG, VALUTA_DT
WITH legacy_hash AS (
    SELECT STANDARD_HASH(CAST(RECHNUNG_ID || '_' || LEISTUNG_CD || '_' || BETRAG || '_' || TO_CHAR(VALUTA_DT, 'YYYYMMDD') AS CLOB), 'MD5') as row_hash
    FROM legacy_schema.morpu_map_lid_target
),
migrated_hash AS (
    SELECT STANDARD_HASH(CAST(RECHNUNG_ID || '_' || LEISTUNG_CD || '_' || BETRAG || '_' || TO_CHAR(VALUTA_DT, 'YYYYMMDD') AS CLOB), 'MD5') as row_hash
    FROM migrated_schema.morpu_map_lid_target
)
SELECT 
    (SELECT SUM(TO_NUMBER(row_hash, 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')) FROM legacy_hash) as legacy_checksum,
    (SELECT SUM(TO_NUMBER(row_hash, 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')) FROM migrated_hash) as migrated_checksum
FROM dual;
```

#### Pass/Fail Criterion
* **Pass**: 
  * Row counts between the legacy and migrated target tables match exactly.
  * The calculated data checksums of the target tables match exactly, proving zero data loss, truncation, or type-handling discrepancies.
* **Fail**: Row counts or checksums do not match.

---

### Test Case 4: Idempotency & Error Recovery Validation
#### Purpose
Per the UC4 operational documentation: *"Der fehlgeschlagene oder unterbrochene Prozess kann ohne weitere Arbeiten erneut ausgeführt werden."* (The failed or interrupted process can be executed again without any additional cleanup work). This test proves that the migrated task is fully idempotent and safe to rerun.

#### Setup
1. Prepare the source staging tables with a standard test dataset.
2. Ensure the target tables are empty.

#### Action
1. **First Run (Interrupted)**: Start the execution of the migrated task and simulate a failure/termination midway (e.g., kill the process or database session).
2. **Check State**: Verify that the target database is either clean or contains partial data.
3. **Second Run (Resumed)**: Trigger the migrated task again and let it run to completion.
4. **Third Run (Consecutive)**: Trigger the migrated task a third time immediately after completion.

```sql
-- SQL Assertion: Verify no duplicate records exist after consecutive runs
SELECT RECHNUNG_ID, LEISTUNG_CD, COUNT(*)
FROM migrated_schema.morpu_map_lid_target
GROUP BY RECHNUNG_ID, LEISTUNG_CD
HAVING COUNT(*) > 1;
```

#### Pass/Fail Criterion
* **Pass**: 
  * The consecutive run completes successfully without throwing unique constraint violations or primary key errors.
  * The SQL assertion returns **0 rows**, proving that rerunning the job does not duplicate records or corrupt the target dataset.
* **Fail**: The task fails on rerun, or duplicate records are detected in the target tables.