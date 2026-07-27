Here is a comprehensive migration-validation test suite designed for the `DW.BERT_AUSD_V_TA_PERIOD` migration. 

This test suite is designed for a QA Engineer to validate the migrated Apache Airflow DAG, the Python control/wrapper scripts, and the BigQuery SQL script against the legacy Oracle/KSH behavior.

---

# Test Suite: Migration Validation for `DW.BERT_AUSD_V_TA_PERIOD`

## Test Case 1: End-to-End DAG Execution & Orchestration
### Purpose
Verify that the migrated Airflow DAG `dw_bert_ausd_v_ta_period` successfully orchestrates the entire execution chain (Wrapper -> Control -> BigQuery SQL) and completes with a `SUCCESS` status.

### Setup
1. Ensure the Airflow DAG `dw_bert_ausd_v_ta_period` is deployed to the Cloud Composer environment.
2. Ensure the following Airflow Variables are configured:
   * `GCP_PROJECT`: Target GCP Project ID.
   * `GCP_REGION`: Target GCP Region.
   * `BERT_DIR_ROOT`: Path to the migrated Python scripts.
   * `DW_DIR_UTL`: Path for temporary execution metrics.
3. Seed the source tables in BigQuery with basic test data (see Test Case 3 for data structure).

### Action
1. Trigger the DAG `dw_bert_ausd_v_ta_period` manually via the Airflow UI or CLI:
   ```bash
   airflow dags trigger dw_bert_ausd_v_ta_period
   ```
2. Monitor the DAG run until completion.

### Pass/Fail Criterion
* **Pass**: The DAG run state is `success`. The task `bert_ausd_v_ta_period` completes with exit code `0`. The execution log contains the verbatim legacy success message: `"Die Abarbeitung wurde ohne erkennbare Fehler beendet"`.
* **Fail**: Any task in the DAG fails, or the DAG execution times out, or the success message is missing from the logs.

---

## Test Case 2: CLI Parameter Validation & Error Handling
### Purpose
Verify that the Python wrapper (`r_ausd_v_ta_period.py`) and control script (`k_ausd_v_ta_period.py`) correctly validate command-line arguments and return legacy-compatible exit codes (`192` for unknown parameters, `193` for missing parameters).

### Setup
1. Set the required environment variables in your test shell:
   ```bash
   export BERT_DIR_ROOT="/tmp/bert"
   export DW_DIR_UTL="/tmp/bert/utl"
   mkdir -p $BERT_DIR_ROOT/aufbereitung/bin
   mkdir -p $DW_DIR_UTL
   ```
2. Ensure `r_ausd_v_ta_period.py` and `k_ausd_v_ta_period.py` are placed in `$BERT_DIR_ROOT/aufbereitung/bin/`.

### Action
Run the following test cases using a Python test runner (e.g., `pytest`):

```python
import subprocess
import os
import pytest

BERT_DIR_ROOT = "/tmp/bert"
K_SCRIPT = f"{BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.py"
R_SCRIPT = f"{BERT_DIR_ROOT}/aufbereitung/bin/r_ausd_v_ta_period.py"

@pytest.fixture(scope="module", autouse=True)
def setup_env():
    os.environ["BERT_DIR_ROOT"] = BERT_DIR_ROOT
    os.environ["DW_DIR_UTL"] = f"{BERT_DIR_ROOT}/utl"
    os.makedirs(os.path.dirname(K_SCRIPT), exist_ok=True)
    os.makedirs(os.environ["DW_DIR_UTL"], exist_ok=True)

def test_k_script_missing_params():
    # Running k_script without required parameters -j and -f
    result = subprocess.run([sys.executable, K_SCRIPT], capture_output=True, text=True)
    assert result.returncode == 193
    assert "FEHLER: 0 E 193" in result.stdout
    assert "Bitte ueber Rahmenscript aufrufen" in result.stdout

def test_k_script_unknown_param():
    # Running k_script with an invalid parameter -z
    result = subprocess.run([sys.executable, K_SCRIPT, "-z", "test"], capture_output=True, text=True)
    assert result.returncode == 192
    assert "Bitte ueber Rahmenscript aufrufen" in result.stdout

def test_r_script_unknown_param():
    # Running r_script with an invalid parameter -z
    result = subprocess.run([sys.executable, R_SCRIPT, "-z"], capture_output=True, text=True)
    assert result.returncode == 192
    assert "DWMSG_MeldeFehler" in result.stderr
```

### Pass/Fail Criterion
* **Pass**: 
  * Missing parameters return exit code `193` and print `"Bitte ueber Rahmenscript aufrufen"`.
  * Unknown parameters return exit code `192` and print `"Bitte ueber Rahmenscript aufrufen"`.
* **Fail**: The scripts return exit code `0` or standard Python `argparse` stack traces instead of the legacy-compatible exit codes and German error messages.

---

## Test Case 3: SQL Transformation and Join Parity
### Purpose
Verify that the BigQuery SQL script correctly joins `cds$ta_period`, `CDS$TA_TIME_MEAS_CV`, and `cds$ta_description` and applies the date-filtering logic using `v_datum` to produce identical output to the legacy Oracle job.

### Setup
1. Create the test tables in your BigQuery test dataset (e.g., `test_dataset`):
   * `dwtk_meldungen`
   * `cds$ta_period`
   * `CDS$TA_TIME_MEAS_CV`
   * `cds$ta_description`
   * `sof$ta_period` (Target Table)

2. Seed the tables with the following test data:
   ```sql
   -- Set the cutoff date to 2026-04-20
   INSERT INTO `test_dataset.dwtk_meldungen` (job_kennung, timecreated) 
   VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-20 12:00:00 UTC'));

   -- Seed Descriptions
   INSERT INTO `test_dataset.cds$ta_description` (DESCRIPTION_ID, description) 
   VALUES (100, 'Months'), (200, 'Years'), (300, 'Days');

   -- Seed Time Measurement CV
   INSERT INTO `test_dataset.CDS$TA_TIME_MEAS_CV` (time_meas_cv, DESCRIPTION_ID) 
   VALUES ('M', 100), ('Y', 200), ('D', 300);

   -- Seed Periods (Testing Date Boundaries relative to cutoff: 2026-04-20)
   INSERT INTO `test_dataset.cds$ta_period` (period_id, number_time_measurement, time_meas_cv, insert_at, modified_at)
   VALUES 
     -- 1. Valid: Inserted before cutoff, modified is NULL
     (1, 12, 'M', DATE('2026-04-15'), NULL),
     -- 2. Invalid: Inserted after cutoff
     (2, 2, 'Y', DATE('2026-04-21'), NULL),
     -- 3. Invalid: Inserted before cutoff, but modified before/at cutoff
     (3, 30, 'D', DATE('2026-04-15'), DATE('2026-04-20')),
     -- 4. Valid: Inserted before cutoff, modified after cutoff
     (4, 6, 'M', DATE('2026-04-15'), DATE('2026-04-22'));
   ```

### Action
1. Execute the migrated BigQuery SQL script (with project and dataset variables replaced with `test_dataset`).
2. Query the target table `sof$ta_period`.

### Pass/Fail Criterion
* **Pass**: The target table `sof$ta_period` contains exactly **2 rows** corresponding to `period_id` 1 and 4 with the correct joined descriptions:
  | period_id | number_time_measurement | time_meas_cv | einheit | bfc_age |
  | :--- | :--- | :--- | :--- | :--- |
  | 1 | 12 | M | Months | 2026-04-15 |
  | 4 | 6 | M | Months | 2026-04-15 |
* **Fail**: The row count is not equal to 2, or the incorrect `period_id` records are loaded, or the `einheit` column does not correctly map to the description.

---

## Test Case 4: Cutoff Date Fallback (Null Handling)
### Purpose
Verify that if no metadata record exists in `dwtk_meldungen` for the job `BERT_DROP_TEMP_TABLE`, the query safely defaults the cutoff date (`v_datum`) to `'19000101'` and executes without throwing errors.

### Setup
1. Truncate the metadata table:
   ```sql
   TRUNCATE TABLE `test_dataset.dwtk_meldungen`;
   ```
2. Seed `cds$ta_period` with one record before 1900 and one after:
   ```sql
   INSERT INTO `test_dataset.cds$ta_period` (period_id, number_time_measurement, time_meas_cv, insert_at, modified_at)
   VALUES 
     (5, 1, 'Y', DATE('1899-12-31'), NULL),
     (6, 5, 'Y', DATE('2025-01-01'), NULL);
   ```

### Action
1. Execute the migrated BigQuery SQL script.
2. Query the target table `sof$ta_period`.

### Pass/Fail Criterion
* **Pass**: The script executes successfully. The target table `sof$ta_period` contains only the record with `period_id = 5` (since its `insert_at` is `<= '1900-01-01'`).
* **Fail**: The script fails with a `NULL` pointer/value error, or `period_id = 6` is incorrectly inserted.

---

## Test Case 5: Target Table Idempotency (Truncate Verification)
### Purpose
Verify that the target table `sof$ta_period` is completely truncated before insertion, preventing duplicate records across multiple runs.

### Setup
1. Insert dummy "stale" records directly into the target table:
   ```sql
   INSERT INTO `test_dataset.sof$ta_period` (period_id, number_time_measurement, time_meas_cv, einheit, bfc_age)
   VALUES (999, 99, 'X', 'Stale Record', DATE('2000-01-01'));
   ```

### Action
1. Execute the migrated BigQuery SQL script.
2. Query the target table for the stale record:
   ```sql
   SELECT COUNT(1) as cnt FROM `test_dataset.sof$ta_period` WHERE period_id = 999;
   ```

### Pass/Fail Criterion
* **Pass**: The query returns `cnt = 0`. The stale record was successfully purged by the `TRUNCATE TABLE` step before the insert phase.
* **Fail**: The query returns `cnt = 1`, indicating that the truncate step failed or was bypassed.