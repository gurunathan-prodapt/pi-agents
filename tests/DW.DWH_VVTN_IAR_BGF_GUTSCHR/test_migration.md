Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow DAG and its underlying execution scripts are behaviorally equivalent to the legacy UC4 system.

---

# Test Suite: DW.DWH_VVTN_IAR_BGF_GUTSCHR Migration Validation

## Test Case 1: End-to-End Happy Path (Output Parity & Transformation Correctness)

### Purpose
Verify that when valid input files (`.chk` and `.CSV.gz`) are present in the work directory, the processing script executes successfully, transforms the data rows correctly (prepends `D;`), appends the correct footer format (prepends `X;`), converts line endings to Unix format, and moves the source files to the store directory.

### Setup
1. Identify or create the work directory matching `$DW_DIR_IMP_TCOM_SEC/iar/work/`.
2. Identify or create the store directory matching `$DW_DIR_IMP_TCOM_SEC/iar/store/`.
3. Create a mock `.chk` file: `DWHK_DWHM_IAR_GUTSCHR_20231031_01.chk` containing:
   ```text
   10;15000.50
   ```
4. Create a mock data file with exactly 25 fields per row, compress it to `.CSV.gz`, and place it alongside the `.chk` file:
   ```bash
   # Create 2 rows of 25 semicolon-separated fields
   echo "f1;f2;f3;f4;f5;f6;f7;f8;f9;f10;f11;f12;f13;f14;f15;f16;f17;f18;f19;f20;f21;f22;f23;f24;f25" > DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV
   echo "a1;a2;a3;a4;a5;a6;a7;a8;a9;a10;a11;a12;a13;a14;a15;a16;a17;a18;a19;a20;a21;a22;a23;a24;a25" >> DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV
   gzip DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV
   ```

### Action
Execute the processing script directly or trigger the Airflow task with the test execution date:
```bash
export DW_DIR_IMP_TCOM_SEC="/tmp/test_iar"
mkdir -p $DW_DIR_IMP_TCOM_SEC/iar/work/ $DW_DIR_IMP_TCOM_SEC/iar/store/

# Copy setup files to work directory
cp DWHK_DWHM_IAR_GUTSCHR_20231031_01.chk $DW_DIR_IMP_TCOM_SEC/iar/work/
cp DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV.gz $DW_DIR_IMP_TCOM_SEC/iar/work/

# Run the script
$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
```

### Pass/Fail Criterion
* **Pass**: 
  * The script exits with status `0`.
  * The output file `DWHK_DWHM_IAR_GUTSCHR_20231031_01_out.CSV` is created in the work directory.
  * The output file contains exactly 3 lines (2 data rows + 1 footer row).
  * Data rows are prefixed with `D;` (e.g., `D;f1;f2;...`).
  * The footer row matches exactly: `X;Datei DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV;10;15000.50;File for BGF IAR Gutschrift;10`.
  * The output file has Unix line endings (`\n` only, no `\r\n`).
  * The source files `DWHK_DWHM_IAR_GUTSCHR_20231031_01.chk` and `DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV.gz` are moved to the store directory.
* **Fail**: Any of the above conditions are not met, or the script exits with a non-zero code.

---

## Test Case 2: Transformation Correctness & Field Count Validation (Edge Case)

### Purpose
Verify that the AWK transformation script (`k_vvtn_iar_bgf_gutschrift.awk`) strictly enforces the 25-field schema constraint and fails gracefully if a row contains an incorrect number of fields.

### Setup
1. Create a mock `.chk` file: `DWHK_DWHM_IAR_GUTSCHR_20231031_02.chk` containing:
   ```text
   5;500.00
   ```
2. Create a malformed data file where one row has 24 fields instead of 25, compress it, and place it in the work directory:
   ```bash
   # Row 1: Valid (25 fields)
   echo "f1;f2;f3;f4;f5;f6;f7;f8;f9;f10;f11;f12;f13;f14;f15;f16;f17;f18;f19;f20;f21;f22;f23;f24;f25" > DWHK_DWHM_IAR_GUTSCHR_20231031_02.CSV
   # Row 2: Invalid (24 fields)
   echo "e1;e2;e3;e4;e5;e6;e7;e8;e9;e10;e11;e12;e13;e14;e15;e16;e17;e18;e19;e20;e21;e22;e23;e24" >> DWHK_DWHM_IAR_GUTSCHR_20231031_02.CSV
   gzip DWHK_DWHM_IAR_GUTSCHR_20231031_02.CSV
   ```

### Action
Execute the processing script:
```bash
cp DWHK_DWHM_IAR_GUTSCHR_20231031_02.chk $DW_DIR_IMP_TCOM_SEC/iar/work/
cp DWHK_DWHM_IAR_GUTSCHR_20231031_02.CSV.gz $DW_DIR_IMP_TCOM_SEC/iar/work/

$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
```

### Pass/Fail Criterion
* **Pass**:
  * The script exits with status `2`.
  * The console output/log contains the error message: `Error: Incorrect nos of Fields`.
  * The temporary output file `DWHK_DWHM_IAR_GUTSCHR_20231031_02_out.CSV` is deleted (cleaned up) from the work directory.
  * The source files remain in the work directory (not moved to store).
* **Fail**: The script exits with `0`, does not log the field count error, or leaves a partially processed output file behind.

---

## Test Case 3: Missing or Empty Control File Validation (Edge Case)

### Purpose
Verify that the script fails immediately and safely if a `.chk` file is empty or if the corresponding `.CSV.gz` data file is missing.

### Setup
1. Create an empty `.chk` file: `DWHK_DWHM_IAR_GUTSCHR_20231031_03.chk` (0 bytes).
2. Create a non-empty `.chk` file `DWHK_DWHM_IAR_GUTSCHR_20231031_04.chk` but do **not** create its corresponding `.CSV.gz` file.

### Action
Execute the processing script:
```bash
# Scenario A: Empty .chk file
touch $DW_DIR_IMP_TCOM_SEC/iar/work/DWHK_DWHM_IAR_GUTSCHR_20231031_03.chk
$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
echo "Scenario A Exit Code: $?"

# Clean up Scenario A, setup Scenario B
rm -f $DW_DIR_IMP_TCOM_SEC/iar/work/DWHK_DWHM_IAR_GUTSCHR_20231031_03.chk
echo "10;100.00" > $DW_DIR_IMP_TCOM_SEC/iar/work/DWHK_DWHM_IAR_GUTSCHR_20231031_04.chk
# Ensure no CSV.gz exists for 04
rm -f $DW_DIR_IMP_TCOM_SEC/iar/work/DWHK_DWHM_IAR_GUTSCHR_20231031_04.CSV.gz

$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
echo "Scenario B Exit Code: $?"
```

### Pass/Fail Criterion
* **Pass**:
  * Scenario A exits with status `1` and logs: `Error: Empty .chk file found - ... !`.
  * Scenario B exits with status `1` and logs: `Error: Input file not found - ... !`.
* **Fail**: The script exits with `0` or fails with an unhandled shell error (e.g., file-not-found crash without custom error logging).

---

## Test Case 4: Airflow Jinja Macro & Environment Variable Validation

### Purpose
Verify that the migrated Airflow DAG correctly computes the `Month_ID` variable (representing the previous calendar month in `YYYYMM` format) and exports the correct `DWH_JOB_KENNUNG` environment variable.

### Setup
A Python test environment with `pytest` and `apache-airflow` installed.

### Action
Run the following `pytest` suite to validate the DAG structure, parameter rendering, and environment variable exports:

```python
import pytest
from datetime import datetime
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="DWH_IAR_BGF_GUTSCHRIFT_JOB", include_examples=False)

def test_dag_loaded(dagbag):
    """Verify the DAG is loaded without import errors."""
    dag = dagbag.get_dag(dag_id="dw_dwh_vvtn_iar_bgf_gutschr")
    assert dag is not None
    assert len(dag.tasks) == 1

def test_jinja_month_id_calculation(dagbag):
    """Verify that Month_ID is correctly calculated as the previous month (YYYYMM)."""
    dag = dagbag.get_dag(dag_id="dw_dwh_vvtn_iar_bgf_gutschr")
    task = dag.get_task("dw_dwh_vvtn_iar_bgf_gutschr_task")
    
    # Test execution date: Mid-January 2023 -> Expected Month_ID: 202212
    execution_date = datetime(2023, 1, 15)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
    )
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    context = ti.get_template_context()
    
    # Render the bash command template
    rendered_command = task.render_template(task.bash_command, context)
    
    assert 'export DWH_JOB_KENNUNG="VVTN_IAR_BGF_GUTSCHR"' in rendered_command
    assert 'export Month_ID="202212"' in rendered_command
    assert 'Lastmonth is $Month_ID' in rendered_command

def test_jinja_month_id_leap_year(dagbag):
    """Verify Month_ID calculation across leap years (e.g., March 2024 -> February 2024)."""
    dag = dagbag.get_dag(dag_id="dw_dwh_vvtn_iar_bgf_gutschr")
    task = dag.get_task("dw_dwh_vvtn_iar_bgf_gutschr_task")
    
    execution_date = datetime(2024, 3, 10)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
    )
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    context = ti.get_template_context()
    
    rendered_command = task.render_template(task.bash_command, context)
    assert 'export Month_ID="202402"' in rendered_command
```

### Pass/Fail Criterion
* **Pass**: All `pytest` assertions pass successfully.
* **Fail**: Any assertion fails, indicating incorrect date calculation or missing environment variables in the task command.