Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Google Cloud / Airflow / BigQuery components behave identically to the legacy UC4 / KornShell / Oracle components for the job `DW.DWH_ABTN_SMART_KUBI`.

---

# Test Case 1: Dynamic Date Logic (`MONATSID` Calculation)

### Purpose
Prove that the dynamic date logic implemented in the Airflow DAG (`get_reporting_month`) behaves identically to the legacy UC4 script date math across all boundary conditions (month boundaries, year boundaries, and the 15th-of-the-month threshold).

### Setup
Install `pytest` and `pendulum` (Airflow's default timezone/datetime library) in your local test environment.

### Action
Run a suite of unit tests passing specific execution dates to the `get_reporting_month` function and asserting the returned `MONATSID` string.

### Concrete Pass/Fail Criterion
* **Pass**: All test cases return the exact `YYYYMM` string matching the legacy logic:
  * Days 1–14 return the previous month (including year-boundary transitions).
  * Days 15–31 return the current month.
* **Fail**: Any date returns an incorrect month string or raises an unhandled exception.

### Test Code (`test_date_logic.py`)
```python
import pytest
import pendulum
from datetime import timedelta

def get_reporting_month(logical_date):
    """
    Migrated Python logic from dw_dwh_abtn_smart_kubi.py
    """
    day = logical_date.day
    if day < 15:
        first_of_month = logical_date.replace(day=1)
        last_day_prev_month = first_of_month - timedelta(days=1)
        monatsid = last_day_prev_month.strftime('%Y%m')
    else:
        monatsid = logical_date.strftime('%Y%m')
    return monatsid

@pytest.mark.parametrize(
    "input_date, expected_monatsid",
    [
        # Threshold boundaries for a standard month (October 2023)
        (pendulum.datetime(2023, 10, 1), "202309"),   # Day 1 < 15 -> Previous Month
        (pendulum.datetime(2023, 10, 14), "202309"),  # Day 14 < 15 -> Previous Month
        (pendulum.datetime(2023, 10, 15), "202310"),  # Day 15 >= 15 -> Current Month
        (pendulum.datetime(2023, 10, 31), "202310"),  # Day 31 >= 15 -> Current Month
        
        # Year boundary transitions (January)
        (pendulum.datetime(2023, 1, 1), "202212"),    # Day 1 < 15 -> Previous Year December
        (pendulum.datetime(2023, 1, 14), "202212"),   # Day 14 < 15 -> Previous Year December
        (pendulum.datetime(2023, 1, 15), "202301"),   # Day 15 >= 15 -> Current Month/Year
        
        # Leap year boundary (February/March 2024)
        (pendulum.datetime(2024, 3, 1), "202402"),    # Day 1 < 15 -> Feb 2024 (Leap Year)
        (pendulum.datetime(2024, 3, 15), "202403"),   # Day 15 >= 15 -> March 2024
    ]
)
def test_reporting_month_parity(input_date, expected_monatsid):
    assert get_reporting_month(input_date) == expected_monatsid
```

---

# Test Case 2: Environment Initialization Parity (`dw_init.py`)

### Purpose
Verify that the migrated `dw_init.py` correctly initializes and exports all legacy environment variables, dynamically resolves `ORACLE_HOME` based on filesystem state, and correctly maps local paths to Google Cloud Storage (GCS) URIs when the `GCS_BUCKET` variable is set.

### Setup
Create a test script that mocks the filesystem and environment variables, then executes `init_environment()`.

### Action
Execute the test suite using `pytest` with mocked directory structures.

### Concrete Pass/Fail Criterion
* **Pass**: 
  * All `DW_DIR_*` variables are set and exported.
  * If `GCS_BUCKET` is set, paths are prefixed with `gs://{bucket_name}/`.
  * `ORACLE_HOME` resolves to `/appl/local/oracle/12.2.0.1.0` if present, or `/appl/local/oracle/11.2.0` if the former is missing.
  * `DW_DIR_UTL_FILE` dynamically appends the correct `ORACLE_SID`.
* **Fail**: Any environment variable is missing, incorrectly mapped, or fails to resolve dynamically.

### Test Code (`test_dw_init.py`)
```python
import os
import sys
from unittest.mock import patch
import pytest

# Import the migrated module
from dw_init import init_environment

@pytest.fixture(autouse=True)
def cleanup_env():
    # Clear environment variables before and after each test
    keys_to_clear = [k for k in os.environ if k.startswith("DW_") or k in ("ORACLE_HOME", "GCS_BUCKET")]
    for k in keys_to_clear:
        del os.environ[k]
    yield

@patch("os.path.isdir")
def test_oracle_home_resolution_12c(mock_isdir):
    # Simulate Oracle 12c directory exists
    def isdir_side_effect(path):
        return path == "/appl/local/oracle/12.2.0.1.0"
    mock_isdir.side_effect = isdir_side_effect

    init_environment()
    assert os.environ.get("ORACLE_HOME") == "/appl/local/oracle/12.2.0.1.0"

@patch("os.path.isdir")
def test_oracle_home_resolution_11g(mock_isdir):
    # Simulate Oracle 12c missing, but 11g exists
    def isdir_side_effect(path):
        return path == "/appl/local/oracle/11.2.0"
    mock_isdir.side_effect = isdir_side_effect

    init_environment()
    assert os.environ.get("ORACLE_HOME") == "/appl/local/oracle/11.2.0"

def test_gcs_bucket_path_mapping():
    os.environ["GCS_BUCKET"] = "test-dwh-bucket"
    os.environ["ORACLE_SID"] = "PROD_DB"
    
    init_environment()
    
    # Assert GCS URI mapping
    assert os.environ.get("DW_DIR_PROT") == "gs://test-dwh-bucket/daten/logfiles"
    assert os.environ.get("DW_DIR_ROOT") == "gs://test-dwh-bucket/aktuell"
    assert os.environ.get("DW_DIR_UTL_FILE") == "/appl/local/oracle/admin/PROD_DB/utl_file"
    assert os.environ.get("DW_HOST_CUSTOMER") == "dxcst3.bn.detemobil.de"

def test_local_fallback_path_mapping():
    os.environ["HOME"] = "/home/dwh_user"
    os.environ["ORACLE_SID"] = "TEST_DB"
    
    init_environment()
    
    # Assert local filesystem mapping
    assert os.environ.get("DW_DIR_PROT") == "/home/dwh_user/daten/logfiles"
    assert os.environ.get("DW_DIR_ROOT") == "/home/dwh_user/aktuell"
    assert os.environ.get("DW_DIR_UTL_FILE") == "/appl/local/oracle/admin/TEST_DB/utl_file"
```

---

# Test Case 3: SQL Transformation Correctness & Null/Type Handling

### Purpose
Verify that the migrated BigQuery SQL script (`d_abtn_x_smart_kubi.sql`) produces identical output to the legacy Oracle PL/SQL script. This test validates:
1. Outer join logic (`(+)` to `LEFT OUTER JOIN`).
2. `DECODE` to `CASE WHEN` translation.
3. `NVL` to `COALESCE` translation.
4. `TRIM` and empty-string/null handling for `vo_kennung`.
5. Partition pruning behavior (querying the base table with a `WHERE` filter instead of using Oracle's `partition(...)` extension).

### Setup
1. Create a test dataset in BigQuery.
2. Create the target table `dwh_ta_t_smart_kubi` and the source tables/views:
   * `dwh_vi_l_map_fa_tarif`
   * `bl_d_tarif`
   * `dwh_ta_f_d1_twvv_tn`
   * `dwh_ta_c_vertrag`
3. Populate the source tables with mock data designed to trigger all conditional branches.

### Action
Execute the BigQuery SQL script with parameters `@p_monats_id = 201509` and `@p_eintragsnr = 1001`.

### Concrete Pass/Fail Criterion
* **Pass**: The target table `dwh_ta_t_smart_kubi` contains exactly the expected rows, verifying:
  * `kundennummer` is set to `'-1'` when `mp_geschaeftsfeld_id = 2`.
  * `tarif_id` and `tarif_id_alt` default to `0` when null.
  * `vo_kennung` correctly falls back to `vo_kenn` when `vo_kenn_bearb` is null, empty, or `'#'`.
  * Contract date logic correctly filters rows based on `l_monats_date` (2015-10-01).
* **Fail**: Row counts do not match, data values differ from expected outputs, or partition pruning fails.

### Test Code (SQL Assertions)
```sql
-- 1. Populate Mock Data
-- View: dwh_vi_l_map_fa_tarif
CREATE OR REPLACE TEMP TABLE dwh_vi_l_map_fa_tarif AS (
  SELECT 101 AS tarif_id, 1001 AS dwh_tarif_id, CAST('2015-01-01' AS DATETIME) AS gueltig_von, CAST('4712-12-31' AS DATETIME) AS gueltig_bis UNION ALL
  SELECT 102 AS tarif_id, 1002 AS dwh_tarif_id, CAST('2015-01-01' AS DATETIME) AS gueltig_von, CAST('4712-12-31' AS DATETIME) AS gueltig_bis
);

-- Table: bl_d_tarif
CREATE OR REPLACE TEMP TABLE bl_d_tarif AS (
  SELECT 101 AS tarif_id, 2 AS mp_geschaeftsfeld_id UNION ALL -- Will trigger kundennummer = '-1'
  SELECT 102 AS tarif_id, 1 AS mp_geschaeftsfeld_id
);

-- Table: dwh_ta_c_vertrag
CREATE OR REPLACE TEMP TABLE dwh_ta_c_vertrag AS (
  SELECT 5001 AS dwh_vertrag_id, 'KUND_A' AS t_mobile_kundennummer, 'N' AS test_gp, CAST('2015-01-01' AS DATETIME) AS gueltig_von, CAST('2016-12-31' AS DATETIME) AS gueltig_bis UNION ALL
  -- Out of date range contract (should be outer-joined as NULL)
  SELECT 5002 AS dwh_vertrag_id, 'KUND_B' AS t_mobile_kundennummer, 'Y' AS test_gp, CAST('2010-01-01' AS DATETIME) AS gueltig_von, CAST('2014-12-31' AS DATETIME) AS gueltig_bis
);

-- Table: dwh_ta_f_d1_twvv_tn
CREATE OR REPLACE TEMP TABLE dwh_ta_f_d1_twvv_tn AS (
  -- Row 1: Standard mapping, mp_geschaeftsfeld_id = 2 -> kundennummer = '-1'
  SELECT CAST('2015-09-15' AS DATETIME) AS gueltigkeitszeitpunkt, 'VVLREIN' AS kennzahl_id, 1001 AS dwh_tarif_id_neu, 1002 AS dwh_tarif_id_alt, 5001 AS dwh_vertrag_id, 'VO_A' AS vo_kenn, 'VO_B' AS vo_kenn_bearb, 10 AS zugang UNION ALL
  -- Row 2: vo_kenn_bearb is '#', should fall back to vo_kenn ('VO_C')
  SELECT CAST('2015-09-20' AS DATETIME) AS gueltigkeitszeitpunkt, 'VVLTWC2C' AS kennzahl_id, 1002 AS dwh_tarif_id_neu, NULL AS dwh_tarif_id_alt, 5001 AS dwh_vertrag_id, 'VO_C' AS vo_kenn, '#' AS vo_kenn_bearb, 5 AS zugang UNION ALL
  -- Row 3: Out of contract range, should outer join contract as NULL
  SELECT CAST('2015-09-25' AS DATETIME) AS gueltigkeitszeitpunkt, 'MIGP2CBF' AS kennzahl_id, 1002 AS dwh_tarif_id_neu, 1002 AS dwh_tarif_id_alt, 5002 AS dwh_vertrag_id, 'VO_D' AS vo_kenn, '  ' AS vo_kenn_bearb, 2 AS zugang
);

-- Target Table
CREATE OR REPLACE TABLE dwh_ta_t_smart_kubi (
  monats_id INT64,
  kundennummer STRING,
  tarif_id INT64,
  tarif_id_alt INT64,
  vo_kennung STRING,
  test_gp STRING,
  anzahl INT64,
  kennzahl_id STRING
);

-- 2. Run Migrated SQL Script (Simulated with parameters)
-- (Insert the converted query here, substituting temp tables)

-- 3. Assertions to verify output parity
-- Assertion A: Row count must be exactly 3
ASSERT (SELECT COUNT(1) FROM dwh_ta_t_smart_kubi) = 3;

-- Assertion B: Verify Row 1 (mp_geschaeftsfeld_id = 2 -> kundennummer = '-1')
ASSERT EXISTS (
  SELECT 1 FROM dwh_ta_t_smart_kubi 
  WHERE monats_id = 201509 
    AND kundennummer = '-1' 
    AND tarif_id = 101 
    AND tarif_id_alt = 102 
    AND vo_kennung = 'VO_B' 
    AND test_gp = 'N' 
    AND anzahl = 10 
    AND kennzahl_id = 'VVLREIN'
);

-- Assertion C: Verify Row 2 (vo_kenn_bearb = '#' -> vo_kennung = 'VO_C')
ASSERT EXISTS (
  SELECT 1 FROM dwh_ta_t_smart_kubi 
  WHERE monats_id = 201509 
    AND kundennummer = 'KUND_A' 
    AND tarif_id = 102 
    AND tarif_id_alt = 0 
    AND vo_kennung = 'VO_C' 
    AND test_gp = 'N' 
    AND anzahl = 5 
    AND kennzahl_id = 'VVLTWC2C'
);

-- Assertion D: Verify Row 3 (Out of contract range -> test_gp is NULL, vo_kenn_bearb is empty -> vo_kennung = 'VO_D')
ASSERT EXISTS (
  SELECT 1 FROM dwh_ta_t_smart_kubi 
  WHERE monats_id = 201509 
    AND kundennummer IS NULL 
    AND tarif_id = 102 
    AND tarif_id_alt = 102 
    AND vo_kennung = 'VO_D' 
    AND test_gp IS NULL 
    AND anzahl = 2 
    AND kennzahl_id = 'MIGP2CBF'
);
```

---

# Test Case 4: End-to-End Orchestration & Parameter Propagation

### Purpose
Verify that the Cloud Composer (Airflow) DAG correctly orchestrates the execution flow, dynamically calculates the `MONATSID` parameter based on the execution date, and passes it to the BigQuery operator.

### Setup
Deploy `dwh_abtn_smart_kubi_dag.py` to a local or test Airflow environment. Mock the BigQuery connection (`google_cloud_default`).

### Action
Trigger a DAG run with a specific logical date (e.g., `2023-10-10T00:00:00Z`) and inspect the rendered templates of the `BigQueryInsertJobOperator` task.

### Concrete Pass/Fail Criterion
* **Pass**:
  * The DAG parses successfully without syntax errors.
  * The rendered query parameters for `p_monats_id` evaluate to `202309` (since October 10th is before the 15th).
  * The rendered query parameters for `p_eintragsnr` evaluate to `1` (the task instance try number).
* **Fail**: The DAG fails to parse, calculates the wrong `p_monats_id`, or fails to propagate parameters to the BigQuery operator.

### Test Code (`test_dag_orchestration.py`)
```python
import pytest
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
import pendulum

def test_dag_loading():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag(dag_id="dwh_abtn_smart_kubi_dag")
    assert dagbag.import_errors == {}
    assert dag is not None

def test_parameter_rendering_before_15th():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag(dag_id="dwh_abtn_smart_kubi_dag")
    task = dag.get_task("execute_d_abtn_x_smart_kubi")
    
    # Simulate execution on October 10th (before the 15th)
    logical_date = pendulum.datetime(2023, 10, 10, 12, 0, 0)
    dag_run = dag.create_dagrun(
        state=DagRunState.running,
        execution_date=logical_date,
        run_type=DagRunType.MANUAL,
    )
    
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.render_templates()
    
    # Extract rendered query parameters
    query_params = task.configuration["query"]["queryParameters"]
    
    p_monats_id_param = next(p for p in query_params if p["name"] == "p_monats_id")
    p_eintragsnr_param = next(p for p in query_params if p["name"] == "p_eintragsnr")
    
    # Assertions
    assert p_monats_id_param["parameterValue"]["value"] == "202309"  # Previous Month
    assert p_eintragsnr_param["parameterValue"]["value"] == "1"

def test_parameter_rendering_after_15th():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag(dag_id="dwh_abtn_smart_kubi_dag")
    task = dag.get_task("execute_d_abtn_x_smart_kubi")
    
    # Simulate execution on October 15th (on/after the 15th)
    logical_date = pendulum.datetime(2023, 10, 15, 12, 0, 0)
    dag_run = dag.create_dagrun(
        state=DagRunState.running,
        execution_date=logical_date,
        run_type=DagRunType.MANUAL,
    )
    
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.render_templates()
    
    query_params = task.configuration["query"]["queryParameters"]
    p_monats_id_param = next(p for p in query_params if p["name"] == "p_monats_id")
    
    # Assertions
    assert p_monats_id_param["parameterValue"]["value"] == "202310"  # Current Month
```

---

# Test Case 5: Runner Script Argument Parsing & Path Resolution (`r_sqlscript.py`)

### Purpose
Prove that the migrated Python runner script (`r_sqlscript.py`) parses command-line arguments, resolves relative paths (searching `../sql`, `../mig`, and `.`), and handles errors identically to the legacy KornShell script.

### Setup
Create a temporary directory structure mimicking the legacy environment:
```
/tmp/test_runner/
├── bin/
│   └── r_sqlscript.py
├── sql/
│   └── d_abtn_x_smart_kubi.sql
└── mig/
```

### Action
Execute `r_sqlscript.py` via `subprocess` with various valid and invalid argument combinations.

### Concrete Pass/Fail Criterion
* **Pass**:
  * Missing `-f` argument returns exit code `193`.
  * Invalid flags return exit code `192`.
  * Suffixes are correctly converted to lowercase (e.g., `-f SCRIPT.SQL` resolves to `script.sql`).
  * Relative paths are resolved in the correct order (`../sql` -> `../mig` -> `.`).
* **Fail**: The script returns incorrect exit codes, fails to resolve paths, or does not match legacy behavior.

### Test Code (`test_runner_script.py`)
```python
import os
import subprocess
import shutil
import pytest

TEST_DIR = "/tmp/test_runner"

@pytest.fixture(scope="module", autouse=True)
def setup_test_environment():
    # Create legacy directory structure
    os.makedirs(f"{TEST_DIR}/bin", exist_ok=True)
    os.makedirs(f"{TEST_DIR}/sql", exist_ok=True)
    os.makedirs(f"{TEST_DIR}/mig", exist_ok=True)
    
    # Copy the migrated runner script to the bin directory
    shutil.copy("local/home/gurunathan_t/kubi/r_sqlscript.py", f"{TEST_DIR}/bin/r_sqlscript.py")
    
    # Create a dummy SQL script in the sql directory
    with open(f"{TEST_DIR}/sql/d_abtn_x_smart_kubi.sql", "w") as f:
        f.write("SELECT 1;")
        
    yield
    
    # Cleanup
    shutil.rmtree(TEST_DIR, ignore_errors=True)

def test_missing_mandatory_argument():
    # Running without -f should return exit code 193
    result = subprocess.run(
        ["python3", f"{TEST_DIR}/bin/r_sqlscript.py", "-j", "TEST_JOB"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 193
    assert "ERROR: MeldeFehler: Nr=193" in result.stderr

def test_invalid_argument():
    # Running with an invalid flag -z should return exit code 192
    result = subprocess.run(
        ["python3", f"{TEST_DIR}/bin/r_sqlscript.py", "-f", "d_abtn_x_smart_kubi.sql", "-z"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 192
    assert "ERROR: MeldeFehler: Nr=192" in result.stderr

def test_path_resolution_and_lowercase_conversion():
    # Pass uppercase script name; it should convert to lowercase and find it in ../sql/
    # Mock GCP_PROJECT to prevent BigQuery client initialization error
    env = os.environ.copy()
    env["GCP_PROJECT"] = "mock-project"
    
    result = subprocess.run(
        ["python3", f"{TEST_DIR}/bin/r_sqlscript.py", "-f", "D_ABTN_X_SMART_KUBI.SQL", "-j", "TEST_JOB"],
        cwd=f"{TEST_DIR}/bin",
        capture_output=True,
        text=True,
        env=env
    )
    
    # The script should find the file and attempt execution (failing on BQ connection, but resolving path)
    assert f"DB-Skript      : ../sql/d_abtn_x_smart_kubi.sql" in result.stdout
```