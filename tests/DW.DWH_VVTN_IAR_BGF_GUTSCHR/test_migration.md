# Migration Validation Test Suite: DW.DWH_VVTN_IAR_BGF_GUTSCHR

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy UC4/Unix environment and the migrated Apache Airflow / Python 3 environment for the job `DW.DWH_VVTN_IAR_BGF_GUTSCHR`.

---

## Test Case 1: Core Transformation & Schema Validation (`k_vvtn_iar_bgf_gutschrift.py`)

### Purpose
Verify that the migrated Python script `k_vvtn_iar_bgf_gutschrift.py` behaves identically to the legacy AWK script `k_vvtn_iar_bgf_gutschrift.awk`. It must validate that every row contains exactly 25 fields, prepend `"D;"` to valid rows, and terminate immediately with exit code `2` and the exact error message `"Error: Incorrect nos of Fields "` upon encountering any malformed row.

### Setup
*   A Python 3 environment with `pytest` installed.
*   The migrated script `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.py` accessible in the execution path.

### Action
Run a suite of automated unit tests passing various inputs (valid, invalid field counts, empty lines, boundary cases) to the Python script via standard input and verifying the exit codes and standard output.

### Pass/Fail Criterion
*   **Pass:** 
    *   Inputs with exactly 25 semicolon-separated fields are prepended with `"D;"` and exit with code `0`.
    *   Inputs with $\neq 25$ fields output exactly `"Error: Incorrect nos of Fields \n"` to `stdout` and exit with code `2`.
    *   Empty lines or lines with trailing carriage returns (`\r\n`) are handled gracefully without crashing.
*   **Fail:** Any deviation in exit codes, output formatting, or failure to terminate immediately on the first invalid row.

### Test Code (`test_gutschrift_validation.py`)
```python
import subprocess
import pytest
import sys

PYTHON_EXEC = sys.executable
SCRIPT_PATH = "isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.py"

def run_script(input_data: str) -> tuple[int, str, str]:
    """Helper to run the Python script with stdin and capture outputs."""
    process = subprocess.Popen(
        [PYTHON_EXEC, SCRIPT_PATH],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    stdout, stderr = process.communicate(input=input_data)
    return process.returncode, stdout, stderr

def test_valid_25_fields():
    # Construct a valid line with exactly 25 fields (24 semicolons)
    valid_line = ";".join([f"val{i}" for i in range(1, 26)])
    returncode, stdout, stderr = run_script(valid_line + "\n")
    
    assert returncode == 0
    assert stdout == f"D;{valid_line}\n"
    assert stderr == ""

def test_invalid_less_fields():
    # 24 fields (23 semicolons)
    invalid_line = ";".join([f"val{i}" for i in range(1, 25)])
    returncode, stdout, stderr = run_script(invalid_line + "\n")
    
    assert returncode == 2
    assert stdout == "Error: Incorrect nos of Fields \n"

def test_invalid_more_fields():
    # 26 fields (25 semicolons)
    invalid_line = ";".join([f"val{i}" for i in range(1, 27)])
    returncode, stdout, stderr = run_script(invalid_line + "\n")
    
    assert returncode == 2
    assert stdout == "Error: Incorrect nos of Fields \n"

def test_multi_line_fail_fast():
    # First line valid, second line invalid
    valid_line = ";".join([f"val{i}" for i in range(1, 26)])
    invalid_line = "col1;col2;col3"
    input_data = f"{valid_line}\n{invalid_line}\n"
    
    returncode, stdout, stderr = run_script(input_data)
    
    assert returncode == 2
    # Should output the first valid line, then the error message, and stop
    expected_output = f"D;{valid_line}\nError: Incorrect nos of Fields \n"
    assert stdout == expected_output

def test_carriage_return_handling():
    # Verify Windows-style line endings (\r\n) are stripped correctly
    valid_line = ";".join([f"val{i}" for i in range(1, 26)])
    returncode, stdout, stderr = run_script(valid_line + "\r\n")
    
    assert returncode == 0
    assert stdout == f"D;{valid_line}\n"
```

---

## Test Case 2: Footer Generation & Metadata Injection (`k_vvtn_iar_bgf_gutsch_foot.py`)

### Purpose
Verify that the migrated Python script `k_vvtn_iar_bgf_gutsch_foot.py` behaves identically to the legacy AWK script `k_vvtn_iar_bgf_gutsch_foot.awk`. It must accept the filename parameter `FLNM` (via `-v FLNM=...` or `--flnm`), parse the `.chk` file fields, and output the structured footer record.

### Setup
*   A Python 3 environment with `pytest` installed.
*   The migrated script `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.py` accessible in the execution path.

### Action
Execute the Python script passing a mock `.chk` file stream and the `FLNM` parameter, asserting that the output matches the exact legacy format:
`"X;Datei {FLNM};{field1};{field2};File for BGF IAR Gutschrift;{field1}"`

### Pass/Fail Criterion
*   **Pass:** 
    *   The output matches the legacy format character-for-character.
    *   The script handles missing or out-of-bounds fields in the `.chk` file gracefully by defaulting to empty strings (matching AWK's behavior).
    *   Both `-v FLNM=filename` and `--flnm filename` syntaxes are supported.
*   **Fail:** Any mismatch in the output string structure, unhandled exceptions on short lines, or failure to inject the filename.

### Test Code (`test_footer_generation.py`)
```python
import subprocess
import pytest
import sys

PYTHON_EXEC = sys.executable
SCRIPT_PATH = "isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.py"

def run_footer_script(input_data: str, flnm: str, use_legacy_flag: bool = True) -> tuple[int, str, str]:
    args = [PYTHON_EXEC, SCRIPT_PATH]
    if use_legacy_flag:
        args.extend(["-v", f"FLNM={flnm}"])
    else:
        args.extend(["--flnm", flnm])
        
    process = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    stdout, stderr = process.communicate(input=input_data)
    return process.returncode, stdout, stderr

def test_standard_footer_generation():
    chk_line = "20231027_120000;1500.50"
    flnm = "DWHK_DWHM_IAR_GUTSCHR_20231027_12.CSV"
    
    # Test with legacy -v flag
    rc, stdout, stderr = run_footer_script(chk_line, flnm, use_legacy_flag=True)
    assert rc == 0
    expected = f"X;Datei {flnm};20231027_120000;1500.50;File for BGF IAR Gutschrift;20231027_120000\n"
    assert stdout == expected
    
    # Test with modern --flnm flag
    rc, stdout, stderr = run_footer_script(chk_line, flnm, use_legacy_flag=False)
    assert rc == 0
    assert stdout == expected

def test_footer_missing_fields():
    # Only 1 field in .chk file instead of 2
    chk_line = "20231027_120000"
    flnm = "DWHK_DWHM_IAR_GUTSCHR_20231027_12.CSV"
    
    rc, stdout, stderr = run_footer_script(chk_line, flnm)
    assert rc == 0
    # Field 2 ($2) should default to empty string
    expected = f"X;Datei {flnm};20231027_120000;;File for BGF IAR Gutschrift;20231027_120000\n"
    assert stdout == expected

def test_footer_empty_input():
    rc, stdout, stderr = run_footer_script("", "test.CSV")
    assert rc == 0
    assert stdout == ""
```

---

## Test Case 3: End-to-End Integration & Wrapper Script Equivalence

### Purpose
Verify that the wrapper script `r_vvtn_iar_bgf_gutschrift` (or its migrated equivalent) correctly orchestrates the entire preprocessing pipeline:
1.  Detects `.chk` and `.CSV.gz` pairs in the work directory.
2.  Validates file existence and non-emptiness.
3.  Decompresses and streams data through `k_vvtn_iar_bgf_gutschrift.py`.
4.  Appends the footer using `k_vvtn_iar_bgf_gutsch_foot.py`.
5.  Performs `dos2unix` line-ending normalization.
6.  Moves processed source files to the store directory on success, or cleans up and fails on error.

### Setup
*   A local test directory structure mimicking the production paths:
    *   `work/` (Input directory)
    *   `store/` (Archive directory)
*   The wrapper script `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` configured to use the migrated Python scripts instead of legacy AWK.

### Action
1.  **Happy Path Run:** Place a valid `.chk` and a valid gzipped `.CSV` (containing 25-field rows) into `work/`. Execute the wrapper script.
2.  **Validation Failure Run:** Place a valid `.chk` and an invalid gzipped `.CSV` (containing a row with 24 fields) into `work/`. Execute the wrapper script.
3.  **Missing File Run:** Place a `.chk` file without its corresponding `.CSV.gz` file. Execute the wrapper script.

### Pass/Fail Criterion
*   **Pass:**
    *   *Happy Path:* Output file `*_out.CSV` is created with all data rows prefixed with `"D;"`, the footer row appended with `"X;"`, line endings normalized to Unix (`\n`), and original files moved to `store/`. Exit code is `0`.
    *   *Validation Failure:* Wrapper exits with code `2`, prints the validation error, deletes the partial output file, and leaves source files in `work/`.
    *   *Missing File:* Wrapper exits with code `1` and prints `"Error: Input file not found..."`.
*   **Fail:** Any leftover temporary files on failure, incorrect file routing, or incorrect exit codes.

### Test Code (`test_wrapper_integration.py`)
```python
import os
import shutil
import gzip
import subprocess
import pytest

# Setup mock environment paths
BASE_DIR = "/tmp/test_iar"
WORK_DIR = os.path.join(BASE_DIR, "work")
STORE_DIR = os.path.join(BASE_DIR, "store")
WRAPPER_SCRIPT = "isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift"

@pytest.fixture(autouse=True)
def setup_teardown_env():
    # Create directories
    os.makedirs(WORK_DIR, exist_ok=True)
    os.makedirs(STORE_DIR, exist_ok=True)
    
    # Set environment variables required by the wrapper script
    os.environ["DW_DIR_IMP_TCOM_SEC"] = BASE_DIR
    os.environ["DW_DIR_ROOT"] = "isdwh"
    
    yield
    
    # Cleanup
    shutil.rmtree(BASE_DIR, ignore_errors=True)

def create_mock_input(prefix: str, data_rows: list[str], chk_content: str):
    chk_path = os.path.join(WORK_DIR, f"{prefix}.chk")
    csv_gz_path = os.path.join(WORK_DIR, f"{prefix}.CSV.gz")
    
    with open(chk_path, "w") as f:
        f.write(chk_content)
        
    with gzip.open(csv_gz_path, "wt") as f:
        f.write("\n".join(data_rows) + "\n")
        
    return chk_path, csv_gz_path

def test_wrapper_happy_path():
    prefix = "DWHK_DWHM_IAR_GUTSCHR_20231027_12"
    valid_row = ";".join([f"col{i}" for i in range(1, 26)])
    chk_data = "20231027_120000;100.00"
    
    create_mock_input(prefix, [valid_row, valid_row], chk_data)
    
    # Run wrapper script
    result = subprocess.run([WRAPPER_SCRIPT], capture_output=True, text=True)
    
    assert result.returncode == 0
    
    # Verify output file
    out_csv_path = os.path.join(WORK_DIR, f"{prefix}_out.CSV")
    assert os.path.exists(out_csv_path)
    
    with open(out_csv_path, "r") as f:
        lines = f.read().splitlines()
        
    assert len(lines) == 3  # 2 data rows + 1 footer
    assert lines[0] == f"D;{valid_row}"
    assert lines[1] == f"D;{valid_row}"
    assert lines[2] == f"X;Datei {prefix}.CSV;20231027_120000;100.00;File for BGF IAR Gutschrift;20231027_120000"
    
    # Verify source files moved to store
    assert not os.path.exists(os.path.join(WORK_DIR, f"{prefix}.chk"))
    assert not os.path.exists(os.path.join(WORK_DIR, f"{prefix}.CSV.gz"))
    assert os.path.exists(os.path.join(STORE_DIR, f"{prefix}.chk"))
    assert os.path.exists(os.path.join(STORE_DIR, f"{prefix}.CSV.gz"))

def test_wrapper_validation_failure():
    prefix = "DWHK_DWHM_IAR_GUTSCHR_20231027_13"
    valid_row = ";".join([f"col{i}" for i in range(1, 26)])
    invalid_row = "col1;col2;col3"  # Only 3 fields
    chk_data = "20231027_130000;200.00"
    
    create_mock_input(prefix, [valid_row, invalid_row], chk_data)
    
    result = subprocess.run([WRAPPER_SCRIPT], capture_output=True, text=True)
    
    assert result.returncode == 2
    assert "Error: Incorrect nos of Fields" in result.stdout
    
    # Output file must be cleaned up
    out_csv_path = os.path.join(WORK_DIR, f"{prefix}_out.CSV")
    assert not os.path.exists(out_csv_path)
    
    # Source files must remain in work directory (not moved to store)
    assert os.path.exists(os.path.join(WORK_DIR, f"{prefix}.chk"))
    assert os.path.exists(os.path.join(WORK_DIR, f"{prefix}.CSV.gz"))
```

---

## Test Case 4: Airflow DAG Orchestration & Parameter Passing

### Purpose
Verify that the migrated Airflow DAG `dw_dwh_vvtn_iar_bgf_gutschr` is syntactically correct, resolves the dynamic `Month_ID` parameter using native Airflow macros, and maintains the correct task execution order.

### Setup
*   An Airflow execution environment (or local unit test context using `airflow.models.DagBag`).
*   The migrated DAG file `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py` placed in the DAGs folder.

### Action
1.  Load the DAG using `DagBag` to check for import errors.
2.  Assert the task dependency structure: `print_lastmonth_task >> dw_dwh_vvtn_iar_bgf_gutschr_task`.
3.  Render the Jinja templates for `print_lastmonth_task` to verify that the `Month_ID` resolves to the previous month in `YYYYMM` format relative to the execution date.

### Pass/Fail Criterion
*   **Pass:**
    *   The DAG loads with zero import errors.
    *   The task dependency matches the legacy sequence.
    *   The `Month_ID` template renders correctly (e.g., for execution date `2023-11-15`, it renders as `202310`).
*   **Fail:** Any import errors, incorrect task dependencies, or failure of the macro to resolve to the correct previous month string.

### Test Code (`test_dag_metadata.py`)
```python
import pytest
from airflow.models import DagBag
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from pendulum import datetime

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="DWH_IAR_BGF_GUTSCHRIFT_JOB", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vvtn_iar_bgf_gutschr")
    
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2

def test_dag_structure():
    dag_bag = DagBag(dag_folder="DWH_IAR_BGF_GUTSCHRIFT_JOB", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vvtn_iar_bgf_gutschr")
    
    print_task = dag.get_task("print_lastmonth_task")
    main_task = dag.get_task("dw_dwh_vvtn_iar_bgf_gutschr_task")
    
    # Verify print_lastmonth_task is upstream of dw_dwh_vvtn_iar_bgf_gutschr_task
    assert main_task in print_task.downstream_list
    assert print_task in main_task.upstream_list

def test_month_id_macro_rendering():
    dag_bag = DagBag(dag_folder="DWH_IAR_BGF_GUTSCHRIFT_JOB", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vvtn_iar_bgf_gutschr")
    
    # Create a mock execution run for November 15, 2023
    execution_date = datetime(2023, 11, 15)
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        run_type=DagRunType.MANUAL,
    )
    
    ti = dagrun.get_task_instance(task_id="print_lastmonth_task")
    ti.task = dag.get_task("print_lastmonth_task")
    
    # Render templates
    ti.render_templates()
    
    # Sourced from: {{ (data_interval_start.add(months=-1)).strftime('%Y%m') }}
    # For execution_date 2023-11-15, data_interval_start defaults to 2023-11-15 in manual runs.
    # Subtracting 1 month yields October 2023 -> "202310"
    rendered_month_id = ti.task.op_kwargs["month_id"]
    assert rendered_month_id == "202310"
```