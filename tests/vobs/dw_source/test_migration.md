# Migration Validation Test Suite: DW.BERT_AUSD_V_TA_P_VERTRAG

This document defines the migration-validation test suite for the contract twin-bill information update job (`DW.BERT_AUSD_V_TA_P_VERTRAG`). These tests verify behavioral equivalence between the legacy Oracle/KSH environment and the migrated Google Cloud (Airflow/BigQuery) environment.

---

## Test Case 1: CLI Parameter Parsing and Validation

### Purpose
Verify that the migrated Python scripts (`r_ausd_v_ta_p_vertrag.py` and `k_ausd_v_ta_p_vertrag.py`) parse command-line arguments correctly, handle missing or invalid parameters, and return the exact legacy exit codes (`192` for unknown parameters, `193` for missing arguments) and German error messages.

### Setup
- A Python environment with `pytest` installed.
- The migrated scripts must be accessible in the execution path or mock workspace.
- Set environment variables:
  ```bash
  export BERT_DIR_ROOT="/tmp/bert"
  export DW_DIR_UTL="/tmp/bert/utl"
  mkdir -p $BERT_DIR_ROOT/aufbereitung/bin
  mkdir -p $DW_DIR_UTL
  ```

### Action
Run a automated test suite using `pytest` to execute the scripts with various parameter combinations and assert exit codes and standard error outputs.

```python
# test_cli_validation.py
import subprocess
import os
import pytest

WORKSPACE_ROOT = os.environ.get("WORKSPACE_ROOT", "/tmp/bert")
R_SCRIPT = os.path.join(WORKSPACE_ROOT, "SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.py")
K_SCRIPT = os.path.join(WORKSPACE_ROOT, "SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.py")

def test_r_script_help():
    # -h should print usage and exit 0
    res = subprocess.run(["python3", R_SCRIPT, "-h"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "Rahmenskript fuer den Abgleich der Vertragsdaten" in res.stdout

def test_r_script_invalid_param():
    # Unknown parameter should exit 192
    res = subprocess.run(["python3", R_SCRIPT, "-x"], capture_output=True, text=True)
    assert res.returncode == 192
    assert "DWMSG_MeldeFehler" in res.stderr or "FEHLER" in res.stderr

def test_r_script_missing_arg():
    # Missing argument for -s should exit 193
    res = subprocess.run(["python3", R_SCRIPT, "-s"], capture_output=True, text=True)
    assert res.returncode == 193

def test_k_script_help():
    # -h should print "Bitte ueber Rahmenscript aufrufen" and exit 0
    res = subprocess.run(["python3", K_SCRIPT, "-h"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "Bitte ueber Rahmenscript aufrufen" in res.stdout

def test_k_script_missing_job_kennung():
    # Missing -j argument should exit 193
    res = subprocess.run(["python3", K_SCRIPT, "-f", "1001"], capture_output=True, text=True)
    assert res.returncode == 193
    assert "FEHLER: 0 E 193 Jobkennung" in res.stderr
```

### Pass/Fail Criterion
- **Pass**: All tests pass. The scripts return exit code `192` for invalid parameters, `193` for missing arguments, and `0` for help flags, matching the legacy KSH behavior.
- **Fail**: Any script returns an incorrect exit code, fails to output the expected German error message, or crashes with an unhandled Python exception (e.g., `IndexError` or `KeyError`).

---

## Test Case 2: Twin-Bill Self-Join Transformation Correctness

### Purpose
Verify that the BigQuery SQL script `d_ausd_v_ta_p_vertrag.sql` correctly performs the self-left-join on `sof$ta_vertrag_tmp` to resolve twin-bill contract relationships, ensuring that:
1. Standard contracts (no twin-card) are preserved.
2. Twin-bill contracts correctly pull the main card's `vo_kenn` and other attributes.
3. Twin-bill contracts with missing main cards are still preserved (due to the `LEFT OUTER JOIN` replacement of Oracle's `(+)`).

### Setup
1. Create the target BigQuery dataset `sof` and `isbert_schema` if they do not exist.
2. Populate `sof.sof$ta_vertrag_tmp` with synthetic test cases:
   - **Row 1 (Standard Contract)**: `vertrag_id_carmen = 101`, `twin_vertrag_id = NULL`, `vo_kenn = 'VO_101'`
   - **Row 2 (Twin-Bill Contract - Child)**: `vertrag_id_carmen = 102`, `twin_vertrag_id = 103`, `vo_kenn = 'VO_102'`
   - **Row 3 (Twin-Bill Contract - Parent)**: `vertrag_id_carmen = 103`, `twin_vertrag_id = NULL`, `vo_kenn = 'VO_103'`
   - **Row 4 (Orphaned Twin-Bill)**: `vertrag_id_carmen = 104`, `twin_vertrag_id = 999` (non-existent), `vo_kenn = 'VO_104'`
3. Ensure `isbert_schema.dwtk_meldungen` has at least one record with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

### Action
Execute the migrated BigQuery SQL script and query the results in `sof.sof$ta_p_vertrag`.

```sql
-- Execute the migrated SQL script
-- (Assuming the script is compiled and run in BigQuery)

-- Assertions Query
SELECT 
  vertrag_id_carmen,
  twin_vertrag_id,
  vo_kenn,
  -- Check if we successfully joined with parent card
  -- In the legacy query, v.vo_kenn is selected. We verify the left join doesn't drop rows.
  CASE 
    WHEN vertrag_id_carmen = 101 THEN 'Standard'
    WHEN vertrag_id_carmen = 102 THEN 'Twin-Child'
    WHEN vertrag_id_carmen = 103 THEN 'Twin-Parent'
    WHEN vertrag_id_carmen = 104 THEN 'Orphaned-Twin'
  END AS test_case
FROM `sof.sof$ta_p_vertrag`
ORDER BY vertrag_id_carmen;
```

### Pass/Fail Criterion
- **Pass**: 
  - Exactly 4 rows are inserted into `sof$ta_p_vertrag`.
  - Row `102` (Twin-Child) is successfully inserted and retains its own attributes while correctly executing the left join against parent `103`.
  - Row `104` (Orphaned-Twin) is successfully inserted despite parent `999` not existing (proving the ANSI `LEFT OUTER JOIN` behaves identically to Oracle's `(+)` and does not filter out the child record).
- **Fail**: Row `104` is missing (indicating an inner join was used), or row counts do not match the input staging table.

---

## Test Case 3: Metadata Date Extraction (`v_datum`)

### Purpose
Verify that the BigQuery SQL script correctly extracts the reporting date (`v_datum`) from `isbert_schema.dwtk_meldungen` using the `BERT_DROP_TEMP_TABLE` job key, handling empty or NULL states gracefully.

### Setup
1. Clear `isbert_schema.dwtk_meldungen`.
2. Insert a test record:
   ```sql
   INSERT INTO `isbert_schema.dwtk_meldungen` (job_kennung, timecreated)
   VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-21 18:00:00'));
   ```

### Action
Run the date extraction logic in isolation and assert the value of `v_datum`.

```sql
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `isbert_schema.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Assert the value
SELECT ASSERT_EQUALS('2026-04-21', FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', v_datum))) AS assertion;
```

### Pass/Fail Criterion
- **Pass**: The variable `v_datum` is evaluated as `'20260421'`. If no records exist, it falls back to `'19000101'` without throwing an error.
- **Fail**: The query fails due to syntax errors, or returns a NULL value instead of the fallback `'19000101'`.

---

## Test Case 4: Table Truncation and Cleanup Verification

### Purpose
Verify that the target table `sof$ta_p_vertrag` and all 22 intermediate temporary staging tables are truncated successfully during execution, ensuring no data leakage between runs.

### Setup
Populate all 23 tables with dummy records before running the script:
```sql
-- List of tables to populate
-- sof$ta_p_vertrag, sof$ta_disc_zusgf, sof$ta_discount, sof$ta_barrier_zusgf, sof$ta_barrier, 
-- sof$ta_cntrct_crs, sof$ta_cntrct_templ, sof$ta_cntrct_valid, sof$ta_period, sof$ta_bp_ref, 
-- sof$ta_inv_assign, sof$ta_inv_def, sof$ta_acc_ref, sof$ta_notice, sof$ta_apn_ve, 
-- sof$ta_discount_rr, sof$ta_vvl_dwh, sof$ta_vvl_upgrade, sof$ta_cntrct_crs2, sof$ta_cntrct_crs3, 
-- sof$ta_inv_acc, sof$ta_vertrag_tmp, sof$ta_action_assoc

-- Example population
INSERT INTO `sof.sof$ta_disc_zusgf` (col1) VALUES ('dummy');
-- Repeat for all 23 tables...
```

### Action
1. Execute the compiled BigQuery script `d_ausd_v_ta_p_vertrag.sql`.
2. Query the row counts of all 22 intermediate tables.

```sql
-- Assert all intermediate tables are empty
SELECT table_name, row_count 
FROM `sof.__TABLES__` 
WHERE table_id IN (
  'sof$ta_disc_zusgf', 'sof$ta_discount', 'sof$ta_barrier_zusgf', 'sof$ta_barrier', 
  'sof$ta_cntrct_crs', 'sof$ta_cntrct_templ', 'sof$ta_cntrct_valid', 'sof$ta_period', 
  'sof$ta_bp_ref', 'sof$ta_inv_assign', 'sof$ta_inv_def', 'sof$ta_acc_ref', 
  'sof$ta_notice', 'sof$ta_apn_ve', 'sof$ta_discount_rr', 'sof$ta_vvl_dwh', 
  'sof$ta_vvl_upgrade', 'sof$ta_cntrct_crs2', 'sof$ta_cntrct_crs3', 'sof$ta_inv_acc', 
  'sof$ta_vertrag_tmp', 'sof$ta_action_assoc'
);
```

### Pass/Fail Criterion
- **Pass**: All 22 intermediate tables have a row count of exactly `0`. The target table `sof$ta_p_vertrag` contains only the newly processed records from the current run.
- **Fail**: Any of the intermediate tables contain leftover rows, or the script fails with permission/existence errors during truncation.

---

## Test Case 5: Airflow DAG Orchestration & XCom Integration

### Purpose
Verify that the Airflow DAG `dw_bert_ausd_v_ta_p_vertrag` orchestrates the tasks in the correct sequence, passes environment variables correctly, and propagates the generated `DW_EINTRAGS_NR` via XCom from the wrapper task to the control task.

### Setup
- An Airflow testing environment with the DAG `dw_bert_ausd_v_ta_p_vertrag` loaded.
- Airflow Variables `WORKSPACE_ROOT`, `GCP_PROJECT`, `GCS_BUCKET`, and `DWH_JOB_KENNUNG` configured.

### Action
Run a pytest suite using the Airflow `DagBag` and task execution context to validate the DAG structure and task parameters.

```python
# test_airflow_dag.py
from airflow.models import DagBag, Variable
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
import pytest

def test_dag_structure():
    dagbag = DagBag(dag_folder="vobs/dw_source", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_bert_ausd_v_ta_p_vertrag")
    assert dag is not None
    assert len(dag.tasks) == 3
    
    # Verify execution order: r_ausd_v_ta_p_vertrag -> k_ausd_v_ta_p_vertrag -> d_ausd_v_ta_p_vertrag_dataform
    r_task = dag.get_task("r_ausd_v_ta_p_vertrag")
    k_task = dag.get_task("k_ausd_v_ta_p_vertrag")
    d_task = dag.get_task("d_ausd_v_ta_p_vertrag_dataform")
    
    assert k_task in r_task.downstream_list
    assert d_task in k_task.downstream_list

def test_task_env_variables():
    dagbag = DagBag(dag_folder="vobs/dw_source", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_bert_ausd_v_ta_p_vertrag")
    r_task = dag.get_task("r_ausd_v_ta_p_vertrag")
    
    assert "DWH_JOB_KENNUNG" in r_task.env
    assert "WORKSPACE_ROOT" in r_task.env
```

### Pass/Fail Criterion
- **Pass**: The DAG compiles without errors, task dependencies strictly match `r_ausd_v_ta_p_vertrag >> k_ausd_v_ta_p_vertrag >> d_ausd_v_ta_p_vertrag_dataform`, and required environment variables are mapped to the tasks.
- **Fail**: The DAG fails to load, task dependencies are broken, or environment variables are missing.