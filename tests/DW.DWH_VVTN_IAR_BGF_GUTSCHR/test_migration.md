Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow DAG `dw_dwh_vvtn_iar_bgf_gutschr` is behaviorally equivalent to the legacy UC4 job `DW.DWH_VVTN_IAR_BGF_GUTSCHR`.

---

# Migration Validation Test Suite: DW.DWH_VVTN_IAR_BGF_GUTSCHR

## Test Case 1: End-to-End Output Parity (Happy Path)
### Purpose
Verify that given a valid set of input files (`.chk` and `.CSV.gz`), the migrated transformation script produces an output CSV file that is identical in content, structure, and formatting (including line endings) to the legacy output.

### Setup
1. Identify or create a test directory mimicking the legacy environment:
   * `WORK_PFAD`: `/tmp/iar/work/`
   * `STORE_PFAD`: `/tmp/iar/store/`
2. Create a mock `.chk` file `DWHK_DWHM_IAR_GUTSCHR_20231031_01.chk` containing:
   ```text
   100;12500.50
   ```
3. Create a mock data file with exactly 25 semicolon-separated fields, compress it to Gzip, and name it `DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV.gz`.
   * Example row: `val1;val2;val3;val4;val5;val6;val7;val8;val9;val10;val11;val12;val13;val14;val15;val16;val17;val18;val19;val20;val21;val22;val23;val24;val25`
4. Deploy the AWK scripts (`k_vvtn_iar_bgf_gutschrift.awk` and `k_vvtn_iar_bgf_gutsch_foot.awk`) to the test environment.

### Action
Run the preprocessing script using the test environment paths:
```bash
export DW_DIR_IMP_TCOM_SEC="/tmp"
export DW_DIR_ROOT="/tmp" # containing vorverarbeitung/tn/awk/...
# Execute the shell script
$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
```

### Pass/Fail Criterion
* **Pass**: 
  * The output file `DWHK_DWHM_IAR_GUTSCHR_20231031_01_out.CSV` is created.
  * The data row is prefixed with `D;`.
  * The footer row is appended, prefixed with `X;`, and matches the format: `X;Datei DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV;100;12500.50;File for BGF IAR Gutschrift;100`.
  * The output file has Unix line endings (`\n` only, verified via `file` or `xxd` due to `dos2unix` execution).
  * The original `.chk` and `.CSV.gz` files are moved to `STORE_PFAD`.
* **Fail**: Any deviation in output format, missing footer, incorrect line endings, or failure to move source files to the store directory.

---

## Test Case 2: Transformation Correctness & Schema Validation (AWK Unit Tests)
### Purpose
Isolate and validate the behavior of the two AWK scripts (`k_vvtn_iar_bgf_gutschrift.awk` and `k_vvtn_iar_bgf_gutsch_foot.awk`) to ensure field-count validation and footer formatting are executed correctly.

### Setup
A Python environment with `pytest` installed. The AWK scripts must be accessible.

### Action
Run the following `pytest` suite:

```python
import subprocess
import pytest
import tempfile
import os

AWK_DATA_SCRIPT = "isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk"
AWK_FOOT_SCRIPT = "isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk"

def test_awk_data_transformation_success():
    # Input with exactly 25 fields
    valid_row = ";".join([f"col{i}" for i in range(1, 26)])
    
    process = subprocess.Popen(
        ["nawk", "-f", AWK_DATA_SCRIPT],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    stdout, stderr = process.communicate(input=valid_row)
    
    assert process.returncode == 0
    assert stdout.startswith("D;")
    assert stdout.strip() == f"D;{valid_row}"

def test_awk_data_transformation_invalid_field_count():
    # Input with 24 fields (invalid)
    invalid_row = ";".join([f"col{i}" for i in range(1, 25)])
    
    process = subprocess.Popen(
        ["nawk", "-f", AWK_DATA_SCRIPT],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    stdout, stderr = process.communicate(input=invalid_row)
    
    # Script must exit with code 2 on field count mismatch
    assert process.returncode == 2
    assert "Error: Incorrect nos of Fields" in stdout

def test_awk_footer_transformation():
    chk_content = "150;99999.99"
    filename = "DWHK_DWHM_IAR_GUTSCHR_20231031_01.CSV"
    
    process = subprocess.Popen(
        ["nawk", "-v", f"FLNM={filename}", "-f", AWK_FOOT_SCRIPT],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    stdout, stderr = process.communicate(input=chk_content)
    
    assert process.returncode == 0
    expected_footer = f"X;Datei {filename};150;99999.99;File for BGF IAR Gutschrift;150\n"
    assert stdout == expected_footer
```

### Pass/Fail Criterion
* **Pass**: All `pytest` assertions pass, proving that the AWK scripts enforce the 25-field schema constraint and format the footer correctly.
* **Fail**: Any test fails (e.g., incorrect exit code on bad schema, or malformed footer output).

---

## Test Case 3: Error Handling & Robustness (Missing/Empty Inputs)
### Purpose
Verify that the shell script fails gracefully and returns the correct exit codes when encountering empty `.chk` files or missing `.CSV.gz` files.

### Setup
1. Create an empty `.chk` file: `/tmp/iar/work/DWHK_DWHM_IAR_GUTSCHR_20231031_99.chk` (0 bytes).
2. Ensure no corresponding `.CSV.gz` exists.

### Action
Run the script and capture the exit code and standard output:
```bash
export DW_DIR_IMP_TCOM_SEC="/tmp"
$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift 2>&1
echo "EXIT_CODE=$?"
```

### Pass/Fail Criterion
* **Pass**:
  * The script outputs: `Error: Empty .chk file found - /tmp/iar/work/DWHK_DWHM_IAR_GUTSCHR_20231031_99.chk !`
  * The script exits with status `1`.
* **Fail**: The script exits with `0` or does not log the specific empty file error.

---

## Test Case 4: Date Logic & Jinja Parameter Parity
### Purpose
Verify that the Airflow Jinja expression for `Month_ID` matches the legacy UC4 `&LASTMONTH_YYYYMM` logic across critical date boundaries (e.g., January run, leap years).

### Setup
An Airflow environment or a Python script utilizing Jinja rendering with the Airflow context.

### Action
Execute a Python script to test the Jinja template rendering for various logical execution dates:

```python
from jinja2 import Template
from dateutil.relativedelta import relativedelta
import pendulum

def render_month_id(logical_date_str: str) -> str:
    # Airflow's data_interval_end is equivalent to the execution_date in legacy terms
    data_interval_end = pendulum.parse(logical_date_str).in_timezone('Europe/Berlin')
    
    # Emulate the Airflow Jinja expression:
    # {{ (data_interval_end.in_timezone('Europe/Berlin') - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}
    target_date = data_interval_end - relativedelta(months=1)
    return target_date.strftime('%Y%m')

def test_date_boundaries():
    # Standard month transition
    assert render_month_id("2023-10-31T00:00:00+01:00") == "202309"
    
    # Year boundary transition (January execution must yield December of previous year)
    assert render_month_id("2024-01-15T00:00:00+01:00") == "202312"
    
    # Leap year transition
    assert render_month_id("2024-03-01T00:00:00+01:00") == "202402"

if __name__ == "__main__":
    test_date_boundaries()
    print("All date boundary assertions passed successfully!")
```

### Pass/Fail Criterion
* **Pass**: The rendered `Month_ID` matches the expected legacy `YYYYMM` format for the previous month across all boundary conditions.
* **Fail**: Any assertion fails (e.g., January execution yields month `00` or incorrect year).

---

## Test Case 5: Airflow Task Execution & Literal Logging Verification
### Purpose
Verify that the Airflow DAG executes the task successfully, sets the correct environment variables, and prints the exact literal log message required by the design document.

### Setup
1. Access to the Airflow CLI or a local development Composer environment.
2. Ensure the target script `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` is mocked or available.

### Action
Trigger the Airflow task for a specific logical date and inspect the task logs:
```bash
airflow tasks test dw_dwh_vvtn_iar_bgf_gutschr dwh_vvtn_iar_bgf_gutschr 2023-11-15
```

### Pass/Fail Criterion
* **Pass**:
  * The task execution completes with `State: SUCCESS`.
  * The task log contains the literal output: `Lastmonth is 202310` (since execution is in November 2023).
  * The environment variable `DWH_JOB_KENNUNG` is exported as `VVTN_IAR_BGF_GUTSCHR`.
* **Fail**: The task fails, the log message is missing, or the calculated `Month_ID` is incorrect.