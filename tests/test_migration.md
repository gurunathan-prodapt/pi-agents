As a senior data-migration QA engineer, I've analyzed the migration design for `h_alis_date.ksh` to BigQuery. The core challenge lies in ensuring behavioral equivalence, especially for functions interacting with `sqlplus` and `SYSDATE`, and for the shell-based date arithmetic.

For deterministic testing, I will assume the following:
*   **Fixed Test Date**: For functions relying on `SYSDATE` (legacy) or `CURRENT_DATE()` (BigQuery), a fixed `TEST_BASE_DATE = '2023-10-26'` will be used.
    *   **Legacy**: The Oracle SQL scripts (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`) will be temporarily modified to use `TO_DATE('2023-10-26', 'YYYY-MM-DD')` instead of `SYSDATE` during testing.
    *   **BigQuery**: The stored procedures `DWDate_Vormonat` and `DWDate_Gib_Zeitraum` will be called with an explicit `p_base_date` parameter set to `DATE '2023-10-26'` for testing. This implies a testable wrapper or a temporary modification to the procedures to accept this parameter.
*   **Legacy Script Execution**: A Python helper script (`run_legacy_ksh.py`) will be used to execute the KornShell functions and capture their output/return codes. This script will set necessary environment variables (`DW_ORAUSER`, `DW_DIR_ROOT`) and handle temporary file cleanup.
*   **BigQuery Execution**: Pytest with the `google-cloud-bigquery` client library will be used to call BigQuery procedures/UDFs and assert results.

---

## Migration Validation Tests for `h_alis_date.ksh` to BigQuery

### General Setup for All Tests

**Purpose**: To establish a consistent environment for executing both legacy KornShell functions and migrated BigQuery procedures/UDFs, ensuring deterministic results for comparison.

**Setup**:
1.  **Legacy Environment**:
    *   A Linux/Unix environment with `ksh` and `sqlplus` installed and configured to connect to an Oracle database.
    *   Oracle user `DW_ORAUSER` (e.g., `user/password@db`) is configured.
    *   `DW_DIR_ROOT` environment variable is set to the base directory containing the `allgemein/is/util/sql` path.
    *   The original `h_alis_date.ksh` script is available.
    *   The Oracle SQL scripts `d_alis_vormonat.sql` and `d_alis_datum_zeitraum.sql` are available at `$DW_DIR_ROOT/allgemein/is/util/sql/`.
    *   **Crucially for testing**: For functions dependent on `SYSDATE`, the Oracle SQL scripts will be temporarily modified to use `TO_DATE('2023-10-26', 'YYYY-MM-DD')` instead of `SYSDATE`.
2.  **BigQuery Environment**:
    *   A Google Cloud Project with BigQuery API enabled.
    *   The `dataset` (e.g., `your_project.your_dataset`) is created.
    *   All UDFs and Stored Procedures from `bq_date_utils.sql` are deployed to this dataset.
    *   **Crucially for testing**: For procedures dependent on `CURRENT_DATE()`, they will be called with an explicit `p_base_date` parameter (e.g., `DATE '2023-10-26'`) to ensure deterministic results. This requires a testable wrapper or temporary modification of the procedures.
3.  **Python Test Harness**: A Python environment with `pytest` and `google-cloud-bigquery` installed.

**Action (Conceptual `run_legacy_ksh.py` helper function):**
```python
import subprocess
import os
import tempfile

def run_legacy_ksh_function(function_name, *args, oracle_sysdate_mock=None):
    """
    Executes a function from h_alis_date.ksh and captures its output and return code.
    Temporarily modifies Oracle SQL scripts to use oracle_sysdate_mock if provided.
    """
    script_path = "vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh"
    sql_dir = os.path.join(os.environ.get("DW_DIR_ROOT", "/tmp"), "allgemein/is/util/sql")

    original_sql_files = {}
    if oracle_sysdate_mock:
        # Temporarily modify Oracle SQL files for deterministic SYSDATE
        for sql_file_name in ["d_alis_vormonat.sql", "d_alis_datum_zeitraum.sql"]:
            full_path = os.path.join(sql_dir, sql_file_name)
            if os.path.exists(full_path):
                with open(full_path, 'r') as f:
                    original_sql_files[full_path] = f.read()
                
                modified_content = original_sql_files[full_path].replace(
                    "SYSDATE", f"TO_DATE('{oracle_sysdate_mock}', 'YYYY-MM-DD')"
                )
                with open(full_path, 'w') as f:
                    f.write(modified_content)
            else:
                print(f"Warning: SQL file not found: {full_path}")

    try:
        # Construct the command to execute the ksh function
        # For functions that return values via echo, capture stdout.
        # For functions that set variables via eval, need to wrap in a way to echo those variables.
        # For functions that return status codes, capture return code.

        # Example for DWDate_Vormonat (which uses eval):
        if function_name == "DWDate_Vormonat":
            temp_var_name = "LEGACY_RESULT_VAR"
            command = [
                "ksh", "-c",
                f". {script_path}; {function_name} {temp_var_name} {args[0]}; echo ${temp_var_name}"
            ]
        # Example for DWDate_Gib_Zeitraum (which uses eval):
        elif function_name == "DWDate_Gib_Zeitraum":
            temp_start_var = "LEGACY_START_VAR"
            temp_end_var = "LEGACY_END_VAR"
            command = [
                "ksh", "-c",
                f". {script_path}; {function_name} {args[0]} {args[1]} {args[2]} {temp_start_var} {temp_end_var}; echo ${temp_start_var} ${temp_end_var}"
            ]
        # Example for LetzterTagDesMonats (which uses return code):
        elif function_name == "LetzterTagDesMonats":
            command = [
                "ksh", "-c",
                f". {script_path}; {function_name} {args[0]}"
            ]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            return process.returncode, process.stdout.strip()
        # Example for TageimMonat (which uses echo):
        elif function_name == "TageimMonat":
            command = [
                "ksh", "-c",
                f". {script_path}; {function_name} {args[0]} {args[1]}"
            ]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            return process.returncode, process.stdout.strip()
        # Example for AddiereDatum (which uses echo):
        elif function_name == "AddiereDatum":
            command = [
                "ksh", "-c",
                f". {script_path}; {function_name} {args[0]} {args[1]}"
            ]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            return process.returncode, process.stdout.strip()
        # Example for DWDate_Datum_Check / DWDate_Datum_LE (which use return code):
        elif function_name in ["DWDate_Datum_Check", "DWDate_Datum_LE"]:
            command = [
                "ksh", "-c",
                f". {script_path}; {function_name} {' '.join(args)}"
            ]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            return process.returncode, process.stderr.strip() # Capture stderr for error messages
        else:
            raise ValueError(f"Unsupported legacy function: {function_name}")

        process = subprocess.run(command, capture_output=True, text=True, check=False)
        return process.returncode, process.stdout.strip()

    finally:
        # Restore original Oracle SQL files
        for full_path, original_content in original_sql_files.items():
            with open(full_path, 'w') as f:
                f.write(original_content)

# BigQuery client setup (conceptual)
from google.cloud import bigquery
bq_client = bigquery.Client(project="your-gcp-project-id")

def call_bq_procedure(procedure_name, *args):
    # This is a simplified representation. Actual implementation would involve
    # constructing a CALL statement and parsing results.
    # For OUT parameters, a temporary table or SELECT statement might be needed.
    # For error handling, try-except blocks for BigQuery exceptions.
    pass
```

---

### Test Case 1: `DWDate_Vormonat` - Output Parity & Transformation Correctness

*   **Purpose**: Verify that the BigQuery `DWDate_Vormonat` stored procedure returns the same last day of the previous month as the legacy KornShell function, for various date formats. This also validates the replacement of `sqlplus` and temporary file parsing.
*   **Setup**:
    *   `TEST_BASE_DATE = '2023-10-26'` (October 26, 2023).
    *   Legacy `d_alis_vormonat.sql` is modified to use `TO_DATE('2023-10-26', 'YYYY-MM-DD')`.
*   **Action**:
    1.  Call legacy `DWDate_Vormonat` with different date formats.
    2.  Call BigQuery `dataset.DWDate_Vormonat` with the same date formats and `p_base_date = DATE '2023-10-26'`.
*   **Expected Output**:
    *   For `TEST_BASE_DATE = '2023-10-26'`, the previous month is September 2023. The last day is `2023-09-30`.
    *   Format `YYYYMMDD`: `20230930`
    *   Format `DD.MM.YYYY`: `30.09.2023`
    *   Format `MM/DD/YY`: `09/30/23`
*   **Pass/Fail Criterion**: The output string from the BigQuery procedure must exactly match the output string from the legacy KornShell function for each format.

```python
import pytest
from google.cloud import bigquery

# Assume bq_client and run_legacy_ksh_function are defined as in General Setup

@pytest.mark.parametrize("date_format, expected_output", [
    ("YYYYMMDD", "20230930"),
    ("DD.MM.YYYY", "30.09.2023"),
    ("MM/DD/YY", "09/30/23"),
])
def test_dwdate_vormonat_output_parity(date_format, expected_output):
    test_base_date = "2023-10-26"

    # 1. Legacy Execution
    # The run_legacy_ksh_function needs to handle the 'eval' for DWDate_Vormonat
    # and the oracle_sysdate_mock for d_alis_vormonat.sql
    legacy_return_code, legacy_output = run_legacy_ksh_function(
        "DWDate_Vormonat", date_format, oracle_sysdate_mock=test_base_date
    )
    assert legacy_return_code == 0, f"Legacy DWDate_Vormonat failed with code {legacy_return_code}"
    assert legacy_output == expected_output, \
        f"Legacy output mismatch for format {date_format}: Expected '{expected_output}', Got '{legacy_output}'"

    # 2. BigQuery Execution
    # Assuming a testable procedure `dataset.DWDate_Vormonat_Testable` exists
    # or a way to mock CURRENT_DATE() for the original procedure.
    query = f"""
    DECLARE v_result STRING;
    CALL dataset.DWDate_Vormonat_Testable('{date_format}', DATE '{test_base_date}', v_result);
    SELECT v_result;
    """
    job = bq_client.query(query)
    bq_result = [row[0] for row in job.result()][0]

    # 3. Assert Parity
    assert bq_result == legacy_output, \
        f"BigQuery output mismatch for format {date_format}: Expected '{legacy_output}', Got '{bq_result}'"
```

### Test Case 2: `DWDate_Datum_Check` - Transformation Correctness (Error Handling)

*   **Purpose**: Verify that the BigQuery `DWDate_Datum_Check` procedure correctly validates dates and raises errors for invalid inputs, mirroring the `sqlplus` exit code behavior. This validates type handling and error propagation.
*   **Setup**: None specific beyond general setup.
*   **Action**:
    1.  Call legacy `DWDate_Datum_Check` with valid and invalid date strings/formats.
    2.  Call BigQuery `dataset.DWDate_Datum_Check` with the same inputs.
*   **Expected Output**:
    *   Valid date (`20231026`, `YYYYMMDD`): Legacy returns `0`, BigQuery executes successfully.
    *   Invalid format (`2023-10-26`, `YYYYMMDD`): Legacy returns `1`, BigQuery raises an error.
    *   Non-existent date (`20230230`, `YYYYMMDD`): Legacy returns `1`, BigQuery raises an error.
*   **Pass/Fail Criterion**:
    *   For valid inputs, both should succeed.
    *   For invalid inputs, both should indicate failure (legacy: non-zero return code; BigQuery: raise an exception). The error message content should be reasonably similar or indicate the same root cause.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

# Assume bq_client and run_legacy_ksh_function are defined as in General Setup

@pytest.mark.parametrize("date_val, date_format, is_valid", [
    ("20231026", "YYYYMMDD", True),
    ("26.10.2023", "DD.MM.YYYY", True),
    ("2023-10-26", "YYYYMMDD", False),  # Format mismatch
    ("20230230", "YYYYMMDD", False),    # Non-existent date
    ("20231301", "YYYYMMDD", False),    # Invalid month
])
def test_dwdate_datum_check_error_handling(date_val, date_format, is_valid):
    # 1. Legacy Execution
    legacy_return_code, legacy_stderr = run_legacy_ksh_function(
        "DWDate_Datum_Check", date_val, date_format
    )

    # 2. BigQuery Execution
    query = f"CALL dataset.DWDate_Datum_Check('{date_val}', '{date_format}');"
    bq_exception = None
    try:
        bq_client.query(query).result()
    except BadRequest as e:
        bq_exception = e

    # 3. Assert Parity
    if is_valid:
        assert legacy_return_code == 0, f"Legacy failed for valid input: {date_val}, {date_format}. Stderr: {legacy_stderr}"
        assert bq_exception is None, f"BigQuery raised error for valid input: {date_val}, {date_format}. Error: {bq_exception}"
    else:
        assert legacy_return_code != 0, f"Legacy succeeded for invalid input: {date_val}, {date_format}"
        assert bq_exception is not None, f"BigQuery succeeded for invalid input: {date_val}, {date_format}"
        # Further check on error message content (optional but good practice)
        assert "Invalid date" in str(bq_exception) or "Failed to parse" in str(bq_exception), \
            f"BigQuery error message not as expected for {date_val}, {date_format}: {bq_exception}"
```

### Test Case 3: `DWDate_Datum_LE` - Transformation Correctness (Comparison & Error Raising)

*   **Purpose**: Verify that the BigQuery `DWDate_Datum_LE` procedure correctly compares two dates and raises an error if the first date is greater than the second, matching the legacy PL/SQL behavior.
*   **Setup**: None specific beyond general setup. Dates are in `YYYYMMDD` format as per legacy.
*   **Action**:
    1.  Call legacy `DWDate_Datum_LE` with `datum1 <= datum2` and `datum1 > datum2` scenarios.
    2.  Call BigQuery `dataset.DWDate_Datum_LE` with the same inputs.
*   **Expected Output**:
    *   `datum1 <= datum2`: Legacy returns `0`, BigQuery executes successfully.
    *   `datum1 > datum2`: Legacy returns `1` (due to `raise_application_error`), BigQuery raises an error with a specific message.
*   **Pass/Fail Criterion**:
    *   For `datum1 <= datum2`, both should succeed.
    *   For `datum1 > datum2`, both should indicate failure. The BigQuery error message should contain the expected `CONCAT` string.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

# Assume bq_client and run_legacy_ksh_function are defined as in General Setup

@pytest.mark.parametrize("datum1, datum2, expected_to_fail", [
    ("20231026", "20231026", False), # Equal
    ("20231025", "20231026", False), # datum1 < datum2
    ("20231027", "20231026", True),  # datum1 > datum2
    ("20240101", "20231231", True),  # Crossing year boundary
])
def test_dwdate_datum_le_comparison(datum1, datum2, expected_to_fail):
    # 1. Legacy Execution
    legacy_return_code, legacy_stderr = run_legacy_ksh_function(
        "DWDate_Datum_LE", datum1, datum2
    )

    # 2. BigQuery Execution
    query = f"CALL dataset.DWDate_Datum_LE('{datum1}', '{datum2}');"
    bq_exception = None
    try:
        bq_client.query(query).result()
    except BadRequest as e:
        bq_exception = e

    # 3. Assert Parity
    if expected_to_fail:
        assert legacy_return_code != 0, f"Legacy succeeded unexpectedly for {datum1} > {datum2}. Stderr: {legacy_stderr}"
        assert bq_exception is not None, f"BigQuery succeeded unexpectedly for {datum1} > {datum2}"
        expected_bq_error_msg = f"Datum {datum1} ist groesser als {datum2}"
        assert expected_bq_error_msg in str(bq_exception), \
            f"BigQuery error message mismatch. Expected '{expected_bq_error_msg}', Got: {bq_exception}"
    else:
        assert legacy_return_code == 0, f"Legacy failed unexpectedly for {datum1} <= {datum2}. Stderr: {legacy_stderr}"
        assert bq_exception is None, f"BigQuery raised error unexpectedly for {datum1} <= {datum2}. Error: {bq_exception}"
```

### Test Case 4: `DWDate_Gib_Zeitraum` - Output Parity & Transformation Correctness (Complex Date Range Logic)

*   **Purpose**: Verify that the BigQuery `DWDate_Gib_Zeitraum` procedure correctly calculates date ranges for various offsets, steps, and formats, matching the legacy KornShell function's behavior. This is a critical test for complex date arithmetic, `LEAST`/`GREATEST` logic, and replacement of `sqlplus`/`grep`/`cut`/`eval`.
*   **Setup**:
    *   `TEST_BASE_DATE = '2023-10-26'` (October 26, 2023).
    *   Legacy `d_alis_datum_zeitraum.sql` is modified to use `TO_DATE('2023-10-26', 'YYYY-MM-DD')`.
    *   **Note on `LEAST`/`GREATEST`**: The BigQuery implementation uses `LEAST(base_start, calc_start)` and `GREATEST(base_end, calc_end)`. This implies it returns the *encompassing range* of the current period and the offset period. The legacy `d_alis_datum_zeitraum.sql` is assumed to produce `calc_start` and `calc_end` (the start/end of the offset period), and the shell script then applies the `LEAST`/`GREATEST` logic implicitly or explicitly to match the BigQuery target. For testing, we will define the `expected_start`/`expected_end` based on this encompassing logic.
*   **Action**:
    1.  Call legacy `DWDate_Gib_Zeitraum` with various `offset_value`, `stufe`, `format`.
    2.  Call BigQuery `dataset.DWDate_Gib_Zeitraum` with the same inputs and `p_base_date = DATE '2023-10-26'`.
*   **Expected Output (based on `TEST_BASE_DATE = '2023-10-26'` and encompassing range logic)**:
    *   `offset=0, stufe='D', format='YYYYMMDD'`: `start=20231026`, `end=20231026`
    *   `offset=5, stufe='D', format='YYYYMMDD'`: `start=20231026`, `end=20231031` (base_start=20231026, calc_end=20231031)
    *   `offset=-5, stufe='D', format='YYYYMMDD'`: `start=20231021`, `end=20231026` (calc_start=20231021, base_end=20231026)
    *   `offset=0, stufe='M', format='YYYYMMDD'`: `start=20231001`, `end=20231031`
    *   `offset=1, stufe='M', format='YYYYMMDD'`: `start=20231001`, `end=20231130` (base_start=20231001, calc_end=20231130)
    *   `offset=-1, stufe='M', format='YYYYMMDD'`: `start=20230901`, `end=20231031` (calc_start=20230901, base_end=20231031)
    *   `offset=0, stufe='Y', format='YYYYMMDD'`: `start=20230101`, `end=20231231`
    *   `offset=1, stufe='Y', format='YYYYMMDD'`: `start=20230101`, `end=20241231` (base_start=20230101, calc_end=20241231)
    *   `offset=-1, stufe='Y', format='YYYYMMDD'`: `start=20220101`, `end=20231231` (calc_start=20220101, base_end=20231231)
*   **Pass/Fail Criterion**: The `start_date` and `end_date` returned by the BigQuery procedure must exactly match those returned by the legacy KornShell function for each test case.

```python
import pytest
from google.cloud import bigquery

# Assume bq_client and run_legacy_ksh_function are defined as in General Setup

@pytest.mark.parametrize("offset, stufe, date_format, expected_start, expected_end", [
    # Day (D)
    (0, "D", "YYYYMMDD", "20231026", "20231026"),
    (5, "D", "YYYYMMDD", "20231026", "20231031"), # base_start=20231026, calc_end=20231031
    (-5, "D", "YYYYMMDD", "20231021", "20231026"), # calc_start=20231021, base_end=20231026
    (10, "D", "DD.MM.YYYY", "26.10.2023", "05.11.2023"), # Crossing month
    (-30, "D", "DD.MM.YYYY", "26.09.2023", "26.10.2023"), # Crossing month

    # Month (M)
    (0, "M", "YYYYMMDD", "20231001", "20231031"),
    (1, "M", "YYYYMMDD", "20231001", "20231130"), # base_start=20231001, calc_end=20231130
    (-1, "M", "YYYYMMDD", "20230901", "20231031"), # calc_start=20230901, base_end=20231031
    (3, "M", "DD.MM.YYYY", "01.10.2023", "31.01.2024"), # Crossing year
    (-3, "M", "DD.MM.YYYY", "01.07.2023", "31.10.2023"), # Crossing year

    # Year (Y)
    (0, "Y", "YYYYMMDD", "20230101", "20231231"),
    (1, "Y", "YYYYMMDD", "20230101", "20241231"), # base_start=20230101, calc_end=20241231
    (-1, "Y", "YYYYMMDD", "20220101", "20231231"), # calc_start=20220101, base_end=20231231
    (2, "Y", "DD.MM.YYYY", "01.01.2023", "31.12.2025"),
    (-2, "Y", "DD.MM.YYYY", "01.01.2021", "31.12.2023"),
])
def test_dwdate_gib_zeitraum_output_parity(offset, stufe, date_format, expected_start, expected_end):
    test_base_date = "2023-10-26"

    # 1. Legacy Execution
    # The run_legacy_ksh_function needs to handle the 'eval' for DWDate_Gib_Zeitraum
    # and the oracle_sysdate_mock for d_alis_datum_zeitraum.sql
    legacy_return_code, legacy_output_str = run_legacy_ksh_function(
        "DWDate_Gib_Zeitraum", str(offset), stufe, date_format, oracle_sysdate_mock=test_base_date
    )
    assert legacy_return_code == 0, f"Legacy DWDate_Gib_Zeitraum failed with code {legacy_return_code}"
    legacy_start, legacy_end = legacy_output_str.split() # Assuming space-separated output

    assert legacy_start == expected_start, \
        f"Legacy start date mismatch for ({offset}, {stufe}, {date_format}): Expected '{expected_start}', Got '{legacy_start}'"
    assert legacy_end == expected_end, \
        f"Legacy end date mismatch for ({offset}, {stufe}, {date_format}): Expected '{expected_end}', Got '{legacy_end}'"

    # 2. BigQuery Execution
    # Assuming a testable procedure `dataset.DWDate_Gib_Zeitraum_Testable` exists
    query = f"""
    DECLARE start_date STRING;
    DECLARE end_date STRING;
    CALL dataset.DWDate_Gib_Zeitraum_Testable(
        {offset}, '{stufe}', '{date_format}', DATE '{test_base_date}', start_date, end_date
    );
    SELECT start_date, end_date;
    """
    job = bq_client.query(query)
    bq_result = [row for row in job.result()][0]
    bq_start = bq_result.start_date
    bq_end = bq_result.end_date

    # 3. Assert Parity
    assert bq_start == legacy_start, \
        f"BigQuery start date mismatch for ({offset}, {stufe}, {date_format}): Expected '{legacy_start}', Got '{bq_start}'"
    assert bq_end == legacy_end, \
        f"BigQuery end date mismatch for ({offset}, {stufe}, {date_format}): Expected '{legacy_end}', Got '{bq_end}'"
```

### Test Case 5: `LetzterTagDesMonat` - Output Parity & Edge Cases (Leap Years)

*   **Purpose**: Verify that the BigQuery `LetzterTagDesMonat` UDF correctly identifies if a given date is the last day of its month, including correct handling of leap years, matching the shell script's logic.
*   **Setup**: None specific beyond general setup.
*   **Action**:
    1.  Call legacy `LetzterTagDesMonat` with various dates (last day, not last day, leap year Feb 29, non-leap year Feb 28/29).
    2.  Call BigQuery `dataset.LetzterTagDesMonat` with the same dates.
*   **Expected Output**:
    *   `20231031`: Legacy returns `0` (true), BigQuery returns `TRUE`.
    *   `20231030`: Legacy returns `1` (false), BigQuery returns `FALSE`.
    *   `20240229` (leap year): Legacy returns `0` (true), BigQuery returns `TRUE`.
    *   `20230228` (non-leap year): Legacy returns `0` (true), BigQuery returns `TRUE`.
    *   `20230229` (non-leap year): Legacy returns `1` (false), BigQuery returns `FALSE` (or error if parsing fails, but BigQuery's `PARSE_DATE` is robust).
*   **Pass/Fail Criterion**: The boolean result from BigQuery must match the logical outcome (0/1) from the legacy function.

```python
import pytest
from google.cloud import bigquery

# Assume bq_client and run_legacy_ksh_function are defined as in General Setup

@pytest.mark.parametrize("date_val, is_last_day", [
    ("20231031", True),  # Last day
    ("20231030", False), # Not last day
    ("20240229", True),  # Leap year, last day
    ("20230228", True),  # Non-leap year, last day
    ("20230229", False), # Non-existent date in non-leap year (legacy should return 1, BQ should be false or error)
    ("20000229", True),  # Leap year (divisible by 400)
    ("19000228", True),  # Not a leap year (divisible by 100 but not 400)
    ("20000228", False), # Not last day in leap year
])
def test_letztertagdesmonat_output_parity(date_val, is_last_day):
    # 1. Legacy Execution
    legacy_return_code, _ = run_legacy_ksh_function("LetzterTagDesMonat", date_val)
    legacy_result = (legacy_return_code == 0) # 0 means true, 1 means false

    # 2. BigQuery Execution
    query = f"SELECT dataset.LetzterTagDesMonat('{date_val}');"
    job = bq_client.query(query)
    bq_result = [row[0] for row in job.result()][0]

    # 3. Assert Parity
    assert bq_result == legacy_result, \
        f"Mismatch for date '{date_val}': Legacy '{legacy_result}', BigQuery '{bq_result}'"
```

### Test Case 6: `TageimMonat` - Output Parity & Edge Cases (Leap Years)

*   **Purpose**: Verify that the BigQuery `TageimMonat` UDF returns the correct number of days for a given month and year, including correct handling of leap years, matching the shell script's logic.
*   **Setup**: None specific beyond general setup.
*   **Action**:
    1.  Call legacy `TageimMonat` with various year/month combinations.
    2.  Call BigQuery `dataset.TageimMonat` with the same inputs.
*   **Expected Output**:
    *   `2023, 10`: `31`
    *   `2023, 2`: `28`
    *   `2024, 2`: `29`
    *   `2000, 2`: `29`
    *   `1900, 2`: `28`
*   **Pass/Fail Criterion**: The integer result from BigQuery must exactly match the integer output from the legacy function.

```python
import pytest
from google.cloud import bigquery

# Assume bq_client and run_legacy_ksh_function are defined as in General Setup

@pytest.mark.parametrize("year, month, expected_days", [
    (2023, 1, 31),
    (2023, 2, 28),
    (2023, 3, 31),
    (2023, 4, 30),
    (2023, 5, 31),
    (2023, 6, 30),
    (2023, 7, 31),
    (2023, 8, 31),
    (2023, 9, 30),
    (2023, 10, 31),
    (2023, 11, 30),
    (2023, 12, 31),
    (2024, 2, 29),  # Leap year
    (2000, 2, 29),  # Century leap year
    (1900, 2, 28),  # Century non-leap year
])
def test_tageimmonat_output_parity(year, month, expected_days):
    # 1. Legacy Execution
    legacy_return_code, legacy_output = run_legacy_ksh_function("TageimMonat", str(year), str(month))
    assert legacy_return_code == 0, f"Legacy TageimMonat failed with code {legacy_return_code}"
    legacy_result = int(legacy_output)

    # 2. BigQuery Execution
    query = f"SELECT dataset.TageimMonat({year}, {month});"
    job = bq_client.query(query)
    bq_result = [row[0] for row in job.result()][0]

    # 3. Assert Parity
    assert bq_result == legacy_result, \
        f"Mismatch for ({year}, {month}): Legacy '{legacy_result}', BigQuery '{bq_result}'"
```

### Test Case 7: `AddiereDatum` - Output Parity & Edge Cases (Month/Year Rollovers, Leap Years)

*   **Purpose**: Verify that the BigQuery `AddiereDatum` UDF correctly adds a specified number of days to a date, handling month and year rollovers and leap years, matching the shell script's manual arithmetic.
*   **Setup**: None specific beyond general setup.
*   **Action**:
    1.  Call legacy `AddiereDatum` with various start dates and day offsets (positive, negative, crossing month/year boundaries, involving leap years).
    2.  Call BigQuery `dataset.AddiereDatum` with the same inputs.
*   **Expected Output**: Calculate the expected date manually or using a reliable date library.
    *   `20231026`, `5`: `20231031`
    *   `20231026`, `-5`: `20231021`
    *   `20231026`, `10`: `20231105` (crossing month)
    *   `20231231`, `1`: `20240101` (crossing year)
    *   `20240228`, `1`: `20240229` (leap year)
    *   `20240228`, `2`: `20240301` (leap year)
    *   `20230228`, `1`: `20230301` (non-leap year)
    *   `20230101`, `-365`: `20220101`
*   **Pass/Fail Criterion**: The resulting date string from BigQuery must exactly match the output string from the legacy function.

```python
import pytest
from google.cloud import bigquery

# Assume bq_client and run_legacy_ksh_function are defined as in General Setup

@pytest.mark.parametrize("start_date, days_to_add, expected_result", [
    ("20231026", 0, "20231026"),
    ("20231026", 5, "20231031"),
    ("20231026", -5, "20231021"),
    ("20231026", 10, "20231105"), # Crossing month
    ("20231231", 1, "20240101"), # Crossing year
    ("20240228", 1, "20240229"), # Leap year
    ("20240228", 2, "20240301"), # Leap year, crossing month
    ("20230228", 1, "20230301"), # Non-leap year
    ("20230101", -365, "20220101"), # Negative days, crossing year
    ("20200301", -1, "20200229"), # Backwards over leap day
    ("20200301", -2, "20200228"),
    ("20230301", -1, "20230228"), # Backwards over non-leap day
])
def test_addieredatum_output_parity(start_date, days_to_add, expected_result):
    # 1. Legacy Execution
    legacy_return_code, legacy_output = run_legacy_ksh_function("AddiereDatum", start_date, str(days_to_add))
    assert legacy_return_code == 0, f"Legacy AddiereDatum failed with code {legacy_return_code}"
    
    # 2. BigQuery Execution
    query = f"SELECT dataset.AddiereDatum('{start_date}', {days_to_add});"
    job = bq_client.query(query)
    bq_result = [row[0] for row in job.result()][0]

    # 3. Assert Parity
    assert bq_result == legacy_output, \
        f"Mismatch for ({start_date}, {days_to_add}): Legacy '{legacy_output}', BigQuery '{bq_result}'"
```