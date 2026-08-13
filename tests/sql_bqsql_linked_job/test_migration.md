# Migration Validation Test Suite: DW.BERT_AUSD_V_TA_PERIOD

This document defines the migration-validation test suite for the job `DW.BERT_AUSD_V_TA_PERIOD`. The test suite is designed to prove behavioral equivalence between the legacy Oracle/UNIX environment and the migrated Apache Airflow/Google Cloud BigQuery environment.

---

## Test Suite Overview

The validation strategy is divided into four distinct test cases:
1. **End-to-End Output Parity**: Verifies that identical source data in Oracle and BigQuery yields identical target data in `sof$ta_period`.
2. **Synchronous Error Propagation**: Proves that the unified execution strategy works correctly—specifically, that SQL failures block execution and bubble up to fail the Airflow task.
3. **Parameter Validation & Exit Codes**: Validates that the Python command-line interface replicates the exact legacy exit codes (`192` and `193`) for parameter mismatches.
4. **Transformation Logic & Edge Cases**: Validates the date-filtering boundaries (`v_datum` logic) and `NULL` handling for `modified_at`.

---

## Test Case 1: End-to-End Output Parity

### Purpose
To prove that the migrated BigQuery SQL script and Python execution wrapper produce the exact same dataset as the legacy Oracle SQL script when given the same input data.

### Setup
1. **Oracle (Legacy) Environment**:
   * Populate `isbert_schema.dwtk_meldungen` with a control record for `BERT_DROP_TEMP_TABLE` (e.g., `timecreated = TO_DATE('2026-08-15 10:00:00', 'YYYY-MM-DD HH24:MI:SS')`).
   * Populate remote Carmen tables via DB Link with 3 test records:
     * Record A: Active period (`insert_at <= 2026-08-15`, `modified_at IS NULL`).
     * Record B: Historically modified period (`insert_at <= 2026-08-15`, `modified_at = 2026-08-14` -> should be excluded).
     * Record C: Future active period (`insert_at = 2026-08-16` -> should be excluded).
   * Run the legacy script `r_ausd_v_ta_period.ksh`.

2. **BigQuery (Target) Environment**:
   * Populate `isbert_schema.dwtk_meldungen` with the identical control record.
   * Populate `carmen_replicated.cds$ta_period`, `carmen_replicated.cds$ta_time_meas_cv`, and `carmen_replicated.cds$ta_description` with the identical 3 test records.

### Action
Execute the migrated Python wrapper script in the target environment:
```bash
export BERT_DIR_ROOT="/opt/airflow"
export GCP_PROJECT="test-gcp-project"
export BQ_DATASET="isbert_schema"
python3 /opt/airflow/scripts/sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py
```

### Pass/Fail Criterion
* **Pass**: 
  * The target table `isbert_schema.sof$ta_period` in BigQuery contains the exact same row count and column values as the Oracle `sof$ta_period` table.
  * The schema types match the compatibility mapping (e.g., Oracle `NUMBER` -> BigQuery `INT64`, Oracle `VARCHAR2` -> BigQuery `STRING`).
* **Fail**: Any mismatch in row count, column values, or data types.

### Test Code (Pytest)
```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture
def bq_client():
    return bigquery.Client(project=os.environ.get("GCP_PROJECT", "test-gcp-project"))

def test_end_to_end_parity(bq_client):
    # 1. Fetch target data from BigQuery
    bq_dataset = os.environ.get("BQ_DATASET", "isbert_schema")
    query = f"""
        SELECT period_id, number_time_measurement, time_meas_cv, einheit, bfc_age 
        FROM `{bq_dataset}.sof$ta_period`
        ORDER BY period_id
    """
    bq_rows = list(bq_client.query(query).result())
    
    # 2. Define expected records based on setup
    # Record A should be the only one inserted
    assert len(bq_rows) == 1, f"Expected exactly 1 row, found {len(bq_rows)}"
    
    record = bq_rows[0]
    assert record["period_id"] == 1001
    assert record["number_time_measurement"] == 12
    assert record["time_meas_cv"] == "MONTH"
    assert record["einheit"] == "Months duration"
    # Verify bfc_age (insert_at) matches the setup date
    assert record["bfc_age"].strftime("%Y-%m-%d") == "2026-08-10"
```

---

## Test Case 2: Synchronous Error Propagation

### Purpose
To verify that the unified execution strategy functions correctly: if the BigQuery SQL execution fails, the core script `k_ausd_v_ta_period.py` must fail synchronously, and the outer wrapper `r_ausd_v_ta_period.py` must catch the failure, log the error, and exit with a non-zero status code (preventing false successes in Airflow).

### Setup
1. Temporarily rename the target table `isbert_schema.sof$ta_period` to `isbert_schema.sof$ta_period_backup` to force a "Table not found" error during the `TRUNCATE` step of `d_ausd_v_ta_period.sql`.

### Action
Run the outer wrapper script:
```bash
export BERT_DIR_ROOT="/opt/airflow"
export GCP_PROJECT="test-gcp-project"
export BQ_DATASET="isbert_schema"
python3 /opt/airflow/scripts/sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py
```

### Pass/Fail Criterion
* **Pass**:
  * The execution exits with a non-zero exit code (e.g., `1`).
  * The log file (e.g., `/tmp/BERT_V_TA_PERIOD_MOCK_ENTRY_01.log`) contains the error message `AppError: Abbruch` and the BigQuery exception details.
  * The success message `"Die Abarbeitung wurde ohne erkennbare Fehler beendet"` is **not** written to the log or stdout.
* **Fail**: The script exits with code `0` or writes the success message despite the SQL failure.

### Test Code (Pytest)
```python
import subprocess
import os

def test_synchronous_error_propagation():
    # Setup environment to point to a non-existent dataset to trigger a BQ error
    env = os.environ.copy()
    env["BERT_DIR_ROOT"] = "/opt/airflow"
    env["BQ_DATASET"] = "non_existent_dataset_to_force_failure"
    env["LogDatei"] = "/tmp/test_error_propagation.log"
    
    # Clean up old log if exists
    if os.path.exists(env["LogDatei"]):
        os.remove(env["LogDatei"])

    # Run the wrapper script
    result = subprocess.run(
        ["python3", "/opt/airflow/scripts/sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py"],
        env=env,
        capture_output=True,
        text=True
    )
    
    # Assertions
    assert result.returncode != 0, "Wrapper script returned exit code 0 despite SQL failure!"
    assert "AppError: Abbruch" in result.stderr or "AppError: Abbruch" in result.stdout
    
    # Verify log file contents
    assert os.path.exists(env["LogDatei"]), "Log file was not created!"
    with open(env["LogDatei"], "r") as f:
        log_content = f.read()
        assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" not in log_content, \
            "Success message found in log file despite execution failure!"
        assert "ERROR: Database query execution failed" in log_content
```

---

## Test Case 3: Parameter Validation & Exit Codes

### Purpose
To verify that the migrated Python core script `k_ausd_v_ta_period.py` enforces parameter constraints and returns the exact legacy exit codes (`192` for unknown parameters, `193` for missing required parameters).

### Setup
None required.

### Action
Execute `k_ausd_v_ta_period.py` with three scenarios:
1. Missing `-j` (Jobkennung) parameter.
2. Missing `-f` (EintragsNr) parameter.
3. Passing an unrecognized parameter (e.g., `-x`).

### Pass/Fail Criterion
* **Pass**:
  * Scenario 1 exits with code `193` and prints `FEHLER: 0 E 193 Jobkennung`.
  * Scenario 2 exits with code `193` and prints `FEHLER: 0 E 193 EintragsNr`.
  * Scenario 3 exits with code `192` and prints `FEHLER: 0 E 192 x`.
* **Fail**: Any scenario returns an incorrect exit code or incorrect error message.

### Test Code (Pytest)
```python
import subprocess
import os

def test_parameter_validation_missing_job_kennung():
    env = os.environ.copy()
    env["BERT_DIR_ROOT"] = "/opt/airflow"
    
    # Run without -j
    result = subprocess.run(
        ["python3", "/opt/airflow/scripts/sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py", "-f", "12345"],
        env=env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 193
    assert "FEHLER: 0 E 193 Jobkennung" in result.stdout

def test_parameter_validation_missing_eintrags_nr():
    env = os.environ.copy()
    env["BERT_DIR_ROOT"] = "/opt/airflow"
    
    # Run without -f
    result = subprocess.run(
        ["python3", "/opt/airflow/scripts/sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py", "-j", "TEST_JOB"],
        env=env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 193
    assert "FEHLER: 0 E 193 EintragsNr" in result.stdout

def test_parameter_validation_unknown_flag():
    env = os.environ.copy()
    env["BERT_DIR_ROOT"] = "/opt/airflow"
    
    # Run with invalid flag -x
    result = subprocess.run(
        ["python3", "/opt/airflow/scripts/sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py", "-j", "TEST_JOB", "-f", "12345", "-x"],
        env=env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 192
    assert "FEHLER: 0 E 192 x" in result.stdout
```

---

## Test Case 4: Date Filtering & NULL Handling Edge Cases

### Purpose
To verify that the BigQuery SQL transformation logic correctly handles date boundaries based on `v_datum` and correctly processes `NULL` values in the `modified_at` column.

### Setup
1. Set `v_datum` in `isbert_schema.dwtk_meldungen` to `'20260815'` (by inserting a record with `timecreated = '2026-08-15 00:00:00'`).
2. Insert the following test cases into the replicated source table `carmen_replicated.cds$ta_period`:
   * **Row 1 (Valid - Active)**: `insert_at = '2026-08-14'`, `modified_at = NULL` -> **Should be included**.
   * **Row 2 (Valid - Modified in Future)**: `insert_at = '2026-08-14'`, `modified_at = '2026-08-16'` -> **Should be included**.
   * **Row 3 (Invalid - Modified in Past)**: `insert_at = '2026-08-14'`, `modified_at = '2026-08-14'` -> **Should be excluded**.
   * **Row 4 (Invalid - Future Insert)**: `insert_at = '2026-08-16'`, `modified_at = NULL` -> **Should be excluded**.

### Action
Execute the SQL script `d_ausd_v_ta_period.sql` using the BigQuery client.

### Pass/Fail Criterion
* **Pass**:
  * Only **Row 1** and **Row 2** are inserted into `isbert_schema.sof$ta_period`.
  * **Row 3** and **Row 4** are filtered out.
* **Fail**: Any incorrect rows are inserted, or valid rows are missed.

### Test Code (SQL Assertion)
```sql
-- Assert that the correct rows were filtered and inserted
-- Run this query in BigQuery to validate the results of the test run:

WITH expected_results AS (
  SELECT 1001 AS period_id, 'Row 1 (Valid - Active)' AS label UNION ALL
  SELECT 1002 AS period_id, 'Row 2 (Valid - Modified in Future)' AS label
),
actual_results AS (
  SELECT period_id 
  FROM `isbert_schema.sof$ta_period`
)
SELECT 
  (SELECT COUNT(1) FROM actual_results) AS actual_count,
  (SELECT COUNT(1) FROM expected_results) AS expected_count,
  COUNT(a.period_id) AS matching_count,
  CASE 
    WHEN (SELECT COUNT(1) FROM actual_results) = 2 
     AND COUNT(a.period_id) = 2 THEN 'PASS'
    ELSE 'FAIL'
  END AS test_status
FROM expected_results e
LEFT JOIN actual_results a ON e.period_id = a.period_id;
```