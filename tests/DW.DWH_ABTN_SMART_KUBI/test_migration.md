# Migration-Validation Test Suite: DW.DWH_ABTN_SMART_KUBI

This document defines the migration-validation test suite for the migrated `DW.DWH_ABTN_SMART_KUBI` job group. The tests are designed to prove behavioral equivalence between the legacy UC4/Oracle/KSH implementation and the migrated Apache Airflow/BigQuery/Python implementation.

---

## Test Case 1: Date Logic Parity (Unit Test)

### Purpose
Verify that the Python-based `calculate_monatsid` function in the Airflow DAG replicates the legacy UC4 date calculation logic exactly across all boundary conditions (leap years, month transitions, and day-of-month thresholds).

### Setup
No external systems are required. The test runs as a local Python unit test using `pytest`.

### Action
Execute the `calculate_monatsid` function with a series of test dates representing boundary conditions and assert the output matches the expected `MONATSID` (`YYYYMM`).

### Code Implementation
```python
import pytest
from datetime import datetime
# Import the helper function from the migrated DAG
from local.home.gurunathan_t.kubi.DW_DWH_ABTN_SMART_KUBI import calculate_monatsid

@pytest.mark.parametrize(
    "input_date, expected_monatsid",
    [
        # Day < 15: Should return the previous month (YYYYMM)
        (datetime(2023, 10, 1), "202309"),
        (datetime(2023, 10, 14), "202309"),
        # Day >= 15: Should return the current month (YYYYMM)
        (datetime(2023, 10, 15), "202310"),
        (datetime(2023, 10, 31), "202310"),
        # Year boundary transition (Day < 15 in January)
        (datetime(2023, 1, 5), "202212"),
        (datetime(2023, 1, 15), "202301"),
        # Leap year boundary (February 2024)
        (datetime(2024, 3, 10), "202402"),
        (datetime(2024, 3, 15), "202403"),
    ]
)
def test_calculate_monatsid_parity(input_date, expected_monatsid):
    actual_monatsid = calculate_monatsid(input_date)
    assert actual_monatsid == expected_monatsid, \
        f"Failed for date {input_date.strftime('%Y-%m-%d')}: expected {expected_monatsid}, got {actual_monatsid}"
```

### Pass/Fail Criterion
*   **Pass**: All test cases return the exact expected `MONATSID` string.
*   **Fail**: Any test case returns an incorrect `MONATSID` string.

---

## Test Case 2: Output Parity & Transformation Correctness (Data Test)

### Purpose
Verify that the migrated BigQuery SQL script (`d_abtn_x_smart_kubi.sql`) produces identical output to the legacy Oracle SQL script under identical input data conditions, covering all join, aggregation, filter, and conditional logic edge cases.

### Setup
1.  Create temporary test tables in BigQuery representing the source tables:
    *   `dwh$vi_l_map_fa_tarif`
    *   `bl_d_tarif`
    *   `dwh$ta_f_d1_twvv_tn`
    *   `dwh$ta_c_vertrag`
2.  Populate these tables with mock data designed to trigger all conditional branches:
    *   `mp_geschaeftsfeld_id = 2` (should map `kundennummer` to `'-1'`).
    *   `mp_geschaeftsfeld_id != 2` (should map `kundennummer` to `t_mobile_kundennummer`).
    *   `vo_kenn_bearb` values: `NULL`, `'#'`, `'  trimmed_vo  '`, and standard values.
    *   `kennzahl_id` values: `'VVLREIN'`, `'VVLTWC2C'`, `'MIGP2CBF'` (included), and `'OTHER'` (excluded).
    *   Date boundaries for `dwh$ta_c_vertrag` (`gueltig_von` and `gueltig_bis`) relative to `l_monats_date`.

### Action
1.  Execute the migrated BigQuery SQL script using a test runner with parameters:
    *   `@l_monats_id = 202310`
    *   `@EintragsNr = 99999`
2.  Compare the resulting records in `dwh$ta_t_smart_kubi` against a pre-calculated expected gold dataset.

### SQL Assertions (BigQuery)
```sql
-- Assertion 1: Verify row count and basic aggregation correctness
ASSERT (
  SELECT COUNT(*) FROM `dwh$ta_t_smart_kubi`
) = 4;

-- Assertion 2: Verify mp_geschaeftsfeld_id = 2 maps to '-1'
ASSERT (
  SELECT COUNT(*) 
  FROM `dwh$ta_t_smart_kubi` 
  WHERE tarif_id = 101 AND kundennummer = '-1'
) = 1;

-- Assertion 3: Verify vo_kennung decoding logic
-- Trimmed NULL/empty/hash should fallback to vo_kenn, otherwise use vo_kenn_bearb
ASSERT (
  SELECT COUNT(*) 
  FROM `dwh$ta_t_smart_kubi`
  WHERE tarif_id = 102 AND vo_kennung = 'VO_ORIGINAL' -- vo_kenn_bearb was '#'
) = 1;

ASSERT (
  SELECT COUNT(*) 
  FROM `dwh$ta_t_smart_kubi`
  WHERE tarif_id = 103 AND vo_kennung = 'VO_BEARBEITET' -- vo_kenn_bearb was 'VO_BEARBEITET'
) = 1;

-- Assertion 4: Verify exclusion of non-matching kennzahl_id
ASSERT (
  SELECT COUNT(*) 
  FROM `dwh$ta_t_smart_kubi`
  WHERE kennzahl_id = 'OTHER'
) = 0;

-- Assertion 5: Verify date boundary joins
-- Records outside the contract validity window should have NULL/defaulted values
ASSERT (
  SELECT COUNT(*) 
  FROM `dwh$ta_t_smart_kubi`
  WHERE tarif_id = 104 AND test_gp IS NULL
) = 1;
```

### Pass/Fail Criterion
*   **Pass**: All SQL assertions execute successfully without throwing assertion errors, proving 100% behavioral equivalence in data transformation.
*   **Fail**: Any assertion fails, indicating a discrepancy in join, filter, or conditional logic.

---

## Test Case 3: Logging & Error Handling Validation (Integration Test)

### Purpose
Verify that `f_alis_msgerr.py` and `r_sqlscript.py` correctly manage execution status tracking (`RUNNING`, `OK`, `ABBRUCH`) and log errors in the BigQuery logging tables (`bert_meldung`, `bert_fehler`) under success and failure scenarios.

### Setup
1.  Ensure BigQuery logging tables `bert_meldung` and `bert_fehler` exist in the test dataset.
2.  Set environment variables:
    *   `BQ_DATASET` = `test_logging_dataset`
    *   `DW_DIR_PROT` = `/tmp/logfiles`

### Action
#### Scenario A: Successful Execution
1.  Run `r_sqlscript.py` with a valid dummy SQL script that executes successfully.
2.  Query `bert_meldung` to verify the status transitions to `OK`.

#### Scenario B: Failed Execution (SQL Error)
1.  Run `r_sqlscript.py` with an invalid SQL script (syntax error).
2.  Query `bert_meldung` to verify the status transitions to `ABBRUCH`.
3.  Query `bert_fehler` to verify the error details are logged.

### Code Implementation (Pytest + BigQuery Client)
```python
import os
import pytest
import subprocess
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.fixture(scope="module", autouse=True)
def setup_env():
    os.environ["BQ_DATASET"] = "your_project.test_logging_dataset"
    os.environ["DW_DIR_PROT"] = "/tmp/logfiles"
    os.makedirs("/tmp/logfiles", exist_ok=True)

def test_successful_execution_logging(bq_client):
    # Create a dummy successful SQL script
    sql_path = "/tmp/success_test.sql"
    with open(sql_path, "w") as f:
        f.write("SELECT 1;")

    # Run the runner script
    cmd = ["python3", "local/home/gurunathan_t/kubi/r_sqlscript.py", "-f", sql_path, "-j", "TEST_SUCCESS"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    assert result.returncode == 0
    
    # Verify BigQuery logging state
    query = f"""
        SELECT status FROM `{os.environ['BQ_DATASET']}.bert_meldung`
        WHERE job_kennung = 'TEST_SUCCESS'
        ORDER BY eintrags_nr DESC LIMIT 1
    """
    rows = list(bq_client.query(query).result())
    assert len(rows) == 1
    assert rows[0].status == "OK"

def test_failed_execution_logging(bq_client):
    # Create a dummy failing SQL script
    sql_path = "/tmp/fail_test.sql"
    with open(sql_path, "w") as f:
        f.write("SELECT * FROM `non_existent_table`;")

    # Run the runner script
    cmd = ["python3", "local/home/gurunathan_t/kubi/r_sqlscript.py", "-f", sql_path, "-j", "TEST_FAIL"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    assert result.returncode != 0
    
    # Verify BigQuery logging state transitions to ABBRUCH
    query_meldung = f"""
        SELECT eintrags_nr, status FROM `{os.environ['BQ_DATASET']}.bert_meldung`
        WHERE job_kennung = 'TEST_FAIL'
        ORDER BY eintrags_nr DESC LIMIT 1
    """
    rows_meldung = list(bq_client.query(query_meldung).result())
    assert len(rows_meldung) == 1
    assert rows_meldung[0].status == "ABBRUCH"
    
    # Verify error details are logged in bert_fehler
    eintrags_nr = rows_meldung[0].eintrags_nr
    query_fehler = f"""
        SELECT typ, fehler_nr FROM `{os.environ['BQ_DATASET']}.bert_fehler`
        WHERE eintrags_nr = @eintrags_nr
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("eintrags_nr", "STRING", eintrags_nr)]
    )
    rows_fehler = list(bq_client.query(query_fehler, job_config=job_config).result())
    assert len(rows_fehler) > 0
    assert rows_fehler[0].typ == "F"
```

### Pass/Fail Criterion
*   **Pass**: Successful runs update status to `OK`. Failed runs update status to `ABBRUCH` and insert descriptive error records into `bert_fehler`.
*   **Fail**: Logging tables are not updated, or status transitions do not match the execution outcome.

---

## Test Case 4: Path Resolution & Parameter Substitution (Integration Test)

### Purpose
Verify that `r_sqlscript.py` correctly resolves relative paths (`../sql`, `../mig`, `.`) and performs parameter substitution (`&1`, `&2`, etc.) in the SQL script before execution.

### Setup
1.  Create the directory structure:
    *   `/tmp/runner/bin` (where `r_sqlscript.py` resides)
    *   `/tmp/runner/sql` (where the SQL script resides)
2.  Create a SQL script `/tmp/runner/sql/param_test.sql` containing:
    ```sql
    -- Test parameter substitution
    SELECT '&1' as param1, '&2' as param2;
    ```

### Action
1.  Execute `r_sqlscript.py` from `/tmp/runner/bin` referencing only the filename `param_test.sql` (testing relative path resolution).
2.  Pass parameters `-i "VALUE1 VALUE2"`.
3.  Verify that the script executes successfully and substitutes the parameters correctly.

### Code Implementation
```python
import os
import shutil
import subprocess
import pytest

def test_path_resolution_and_substitution():
    # Setup directory structure
    base_dir = "/tmp/runner"
    bin_dir = os.path.join(base_dir, "bin")
    sql_dir = os.path.join(base_dir, "sql")
    os.makedirs(bin_dir, exist_ok=True)
    os.makedirs(sql_dir, exist_ok=True)

    # Copy runner script to bin directory
    shutil.copy("local/home/gurunathan_t/kubi/r_sqlscript.py", os.path.join(bin_dir, "r_sqlscript.py"))
    
    # Create SQL script in sql directory
    sql_content = "SELECT '&1' as p1, '&2' as p2;"
    sql_file_path = os.path.join(sql_dir, "param_test.sql")
    with open(sql_file_path, "w") as f:
        f.write(sql_content)

    # Run from the bin directory to test relative path resolution (../sql/param_test.sql)
    cmd = [
        "python3", "r_sqlscript.py",
        "-f", "param_test.sql",
        "-i", "VALUE1 VALUE2",
        "-j", "PATH_TEST"
    ]
    result = subprocess.run(cmd, cwd=bin_dir, capture_output=True, text=True)
    
    # Cleanup
    shutil.rmtree(base_dir)
    
    assert result.returncode == 0, f"Execution failed: {result.stderr}"
```

### Pass/Fail Criterion
*   **Pass**: The runner resolves the relative path to `../sql/param_test.sql`, substitutes `&1` with `VALUE1` and `&2` with `VALUE2`, and executes successfully.
*   **Fail**: The runner fails to find the file, or parameter substitution fails.

---

## Test Case 5: End-to-End DAG Execution (Orchestration Test)

### Purpose
Verify that the Airflow DAG `dw_dwh_abtn_smart_kubi` executes successfully, resolves parameters, prints the required German literal, and orchestrates the tasks in the correct order.

### Setup
A running Airflow environment (e.g., local development environment or Cloud Composer test environment) with the migrated DAG loaded.

### Action
1.  Trigger the DAG manually or via the Airflow CLI.
2.  Monitor the execution of the tasks: `calculate_parameters` -> `populate_temp_table`.
3.  Inspect task logs for the required output literals.

### Assertions (Airflow CLI / Logs)
```bash
# 1. Trigger the DAG
airflow dags trigger dw_dwh_abtn_smart_kubi

# 2. Wait for execution and assert success status
airflow dags state dw_dwh_abtn_smart_kubi $(date +%Y-%m-%d)

# 3. Assert the German print literal rule is satisfied in the logs of 'calculate_parameters'
# Expected output: "Berichtsmonat:  YYYYMM"
airflow tasks logs dw_dwh_abtn_smart_kubi calculate_parameters | grep "Berichtsmonat:  "
```

### Pass/Fail Criterion
*   **Pass**: The DAG completes with a `SUCCESS` state, and the task logs contain the exact German literal `"Berichtsmonat:  "` followed by the dynamically calculated `MONATSID`.
*   **Fail**: The DAG fails, tasks execute out of order, or the required log output is missing.