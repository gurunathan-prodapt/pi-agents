The migration of `h_alis_date.ksh` to BigQuery stored procedures involves a significant architectural shift, replacing shell scripting and Oracle `sqlplus` interactions with native BigQuery SQL. The tests below aim to ensure behavioral equivalence, covering output parity, transformation correctness, and the successful replacement of external dependencies.

**Assumptions for Testing:**
*   **Fixed `CURRENT_DATE()`:** For consistency across legacy and BigQuery tests, all tests involving `CURRENT_DATE()` (or Oracle's `SYSDATE`) will assume a fixed date of **`2023-10-26`**.
*   **Legacy Environment Simulation:** Since a live Oracle database is not available, the `sqlplus` calls within the legacy KornShell script will be simulated. This involves creating mock shell functions or Python wrappers that mimic the expected output and return codes of the Oracle queries and `sqlplus` commands.
*   **BigQuery Project/Dataset:** The BigQuery procedures are assumed to be deployed in `your_project.date_utilities`.
*   **Python Test Framework:** Pytest is used for runnable test code examples, interacting with the BigQuery API and simulating shell execution.

---

## Test Setup (General)

**1. BigQuery Procedures Deployment:**
Ensure all BigQuery stored procedures (`DWDate_Vormonat`, `DWDate_Datum_Check`, `DWDate_Datum_LE`, `DWDate_Gib_Zeitraum`, `LetzterTagDesMonat`, `TageimMonat`, `AddiereDatum`) are deployed to `your_project.date_utilities`.

**2. Legacy Script Preparation:**
Create a test harness for the legacy `h_alis_date.ksh` script. This involves:
*   Setting `DW_DIR_ROOT` to a directory containing mock SQL files or a mock `sqlplus` executable.
*   Setting `DW_ORAUSER` (though its value won't matter for mocked `sqlplus`).
*   Creating a mock `sqlplus` executable that intercepts calls and returns predefined outputs/exit codes based on the specific SQL being executed.

**Example Python setup for mocking `sqlplus` and running legacy script:**

```python
import subprocess
import os
import tempfile
from datetime import date, timedelta
from google.cloud import bigquery

# --- Configuration ---
BQ_PROJECT = "your_project"
BQ_DATASET = "date_utilities"
LEGACY_SCRIPT_PATH = "vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh"
TEST_CURRENT_DATE = date(2023, 10, 26) # Fixed date for consistent testing

# --- BigQuery Client ---
bq_client = bigquery.Client(project=BQ_PROJECT)

# --- Mocking Legacy Environment ---
def setup_legacy_env():
    """Sets up a temporary directory with mock sqlplus and DW_DIR_ROOT."""
    mock_dir = tempfile.mkdtemp()
    mock_bin_dir = os.path.join(mock_dir, "mock_bin")
    mock_sql_dir = os.path.join(mock_dir, "mock_sql")
    os.makedirs(mock_bin_dir)
    os.makedirs(mock_sql_dir)

    # Create mock sqlplus executable
    sqlplus_mock_script = f"""#!/bin/bash
    # Mock sqlplus for h_alis_date.ksh tests
    # Fixed current date for mocks
    TEST_CURRENT_DATE="{TEST_CURRENT_DATE.isoformat()}"

    # Function to convert Oracle format to shell date format
    convert_format() {{
        local oracle_format=$1
        case "$oracle_format" in
            "YYYYMMDD") echo "%Y%m%d" ;;
            "DD.MM.YYYY") echo "%d.%m.%Y" ;;
            *) echo "%Y%m%d" ;; # Default
        esac
    }}

    if [[ "$@" == *d_alis_vormonat.sql* ]]; then
        # DWDate_Vormonat mock
        local tmp_file=$2
        local format=$3
        local shell_format=$(convert_format "$format")
        local current_date_ts=$(date -d "$TEST_CURRENT_DATE" +%s)
        local first_day_current_month_ts=$(date -d "$(date -d "$TEST_CURRENT_DATE" +%Y-%m-01)" +%s)
        local last_day_prev_month_ts=$((first_day_current_month_ts - 86400)) # Subtract 1 day
        local prev_month_date=$(date -d "@$last_day_prev_month_ts" +"%Y-%m-%d")
        echo "$(date -d "$prev_month_date" +"$shell_format")" > "$tmp_file"
        exit 0
    elif [[ "$@" == *d_alis_datum_zeitraum.sql* ]]; then
        # DWDate_Gib_Zeitraum mock
        local tmp_file=$2
        local offset=$3
        local stufe=$4
        local format=$5
        local start_date=""
        local end_date=""
        local shell_format=$(convert_format "$format")

        case "$stufe" in
            "D")
                start_date=$(date -d "$TEST_CURRENT_DATE" +"%Y-%m-%d")
                end_date=$(date -d "$TEST_CURRENT_DATE $offset days" +"%Y-%m-%d")
                ;;
            "M")
                # Legacy logic: "Anfang immer Monatserste und das Ende immer der Ultimo des entsprechenden Monats"
                # for the *offset* month.
                local target_date=$(date -d "$TEST_CURRENT_DATE $offset months" +"%Y-%m-%d")
                start_date=$(date -d "$(date -d "$target_date" +%Y-%m-01)" +"%Y-%m-%d")
                end_date=$(date -d "$(date -d "$target_date" +%Y-%m-01) +1 month -1 day" +"%Y-%m-%d")
                ;;
            "Y")
                # Legacy logic: "Anfang immer Neujahr und das Ende immer Sylvester des entsprechenden Jahres"
                local target_year=$(date -d "$TEST_CURRENT_DATE $offset years" +%Y)
                start_date="$target_year-01-01"
                end_date="$target_year-12-31"
                ;;
            *)
                echo "!! Interner Fehler bei der Rueckgabe von Datumswerten" >&2
                echo "   Funktion: DWDate_Gib_Zeitraum" >&2
                echo "   Invalid Stufe: $stufe" >&2
                exit 1
                ;;
        esac
        local formatted_start=$(date -d "$start_date" +"$shell_format")
        local formatted_end=$(date -d "$end_date" +"$shell_format")
        echo "DWH_Ergebnis;$formatted_start;$formatted_end" > "$tmp_file"
        exit 0
    elif [[ "$1" == "-s" ]]; then # Inline SQL
        read -d '' SQL_INPUT
        if [[ "$SQL_INPUT" == *TO_DATE*from\ dual* ]]; then
            # DWDate_Datum_Check mock
            local wert=$(echo "$SQL_INPUT" | grep -o "to_date('[^']\+','[^']\+')" | sed -E "s/to_date\('([^']+)','.+'\)/\1/")
            local format=$(echo "$SQL_INPUT" | grep -o "to_date('[^']\+','[^']\+')" | sed -E "s/to_date\('.+','([^']+)'\)/\1/")
            local shell_format=$(convert_format "$format")
            if date -d "$wert" +"$shell_format" &>/dev/null; then
                exit 0 # Valid
            else
                exit 1 # Invalid
            fi
        elif [[ "$SQL_INPUT" == *raise_application_error* ]]; then
            # DWDate_Datum_LE mock
            local datum1=$(echo "$SQL_INPUT" | grep -o "datum1:=TO_DATE('[^']\+','YYYYMMDD')" | sed -E "s/datum1:=TO_DATE\('([^']+)','.+'\)/\1/")
            local datum2=$(echo "$SQL_INPUT" | grep -o "datum2:=TO_DATE('[^']\+','YYYYMMDD')" | sed -E "s/datum2:=TO_DATE\('([^']+)','.+'\)/\1/")
            if [[ "$datum1" -le "$datum2" ]]; then
                exit 0 # P1 <= P2
            else
                exit 1 # P1 > P2 (simulates raise_application_error)
            fi
        else
            echo "Unknown inline SQL" >&2
            exit 1
        fi
    else
        echo "Unknown sqlplus call: $@" >&2
        exit 1
    fi
    """
    with open(os.path.join(mock_bin_dir, "sqlplus"), "w") as f:
        f.write(sqlplus_mock_script)
    os.chmod(os.path.join(mock_bin_dir, "sqlplus"), 0o755)

    # Set environment variables for the legacy script
    os.environ["DW_DIR_ROOT"] = mock_sql_dir # Not strictly used by mock sqlplus, but good practice
    os.environ["DW_ORAUSER"] = "mock_user/mock_pass"
    os.environ["PATH"] = f"{mock_bin_dir}:{os.environ['PATH']}" # Prepend mock sqlplus

    return mock_dir

def teardown_legacy_env(mock_dir):
    """Cleans up the temporary mock directory."""
    import shutil
    shutil.rmtree(mock_dir)
    del os.environ["DW_DIR_ROOT"]
    del os.environ["DW_ORAUSER"]
    # Restore PATH (more complex, usually done by context manager or fixture)

def run_legacy_function(func_name, *args):
    """Executes a function from the legacy ksh script and captures output/return code."""
    cmd = [
        "ksh",
        "-c",
        f"source {LEGACY_SCRIPT_PATH}; {func_name} {' '.join(map(str, args))}; echo $?"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    output_lines = result.stdout.strip().split('\n')
    return_code = int(output_lines[-1])
    actual_output = '\n'.join(output_lines[:-1]).strip()
    return actual_output, return_code

def call_bq_procedure(procedure_name, params, output_params):
    """Calls a BigQuery stored procedure and returns output parameters."""
    param_declarations = []
    param_assignments = []
    out_param_selects = []

    for p_name, p_value in params.items():
        if isinstance(p_value, str):
            param_declarations.append(f"DECLARE {p_name} STRING DEFAULT '{p_value}';")
        elif isinstance(p_value, int):
            param_declarations.append(f"DECLARE {p_name} INT64 DEFAULT {p_value};")
        elif isinstance(p_value, bool):
            param_declarations.append(f"DECLARE {p_name} BOOL DEFAULT {str(p_value).upper()};")
        # Add other types as needed

    for op_name, op_type in output_params.items():
        param_declarations.append(f"DECLARE {op_name} {op_type};")
        out_param_selects.append(op_name)

    param_list = ", ".join([f"{p_name}" for p_name in params] + [f"{op_name}" for op_name in output_params])
    
    # Use EXECUTE IMMEDIATE to set CURRENT_DATE for consistent testing
    query = f"""
    EXECUTE IMMEDIATE '''
        SET @@current_date = "{TEST_CURRENT_DATE.isoformat()}";
        {'; '.join(param_declarations)};
        CALL `{BQ_PROJECT}.{BQ_DATASET}.{procedure_name}`({param_list});
        SELECT {', '.join(out_param_selects)};
    ''';
    """
    
    job = bq_client.query(query)
    results = job.result()
    
    if job.error_result:
        raise Exception(f"BigQuery job failed: {job.error_result}")

    for row in results:
        return {op_name: row[op_name] for op_name in output_params}
    return {} # Should not happen if output_params is not empty
```

---

## 1. `DWDate_Vormonat`

*   **Purpose:** Verify that the BigQuery `DWDate_Vormonat` procedure correctly calculates the last day of the previous month and formats it, matching the legacy script's behavior.
*   **Legacy Logic (Simulated):** The mock `sqlplus` for `d_alis_vormonat.sql` will return the last day of the month preceding `TEST_CURRENT_DATE`, formatted as requested.
*   **BQ Logic:** `FORMAT_DATE(p_format, DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY))`

### Test Case 1.1: Standard Previous Month Calculation

*   **Purpose:** Test with a common date format for the previous month.
*   **Setup:** `TEST_CURRENT_DATE = 2023-10-26`.
*   **Action:**
    *   Call legacy `DWDate_Vormonat` with `VarName="RESULT"`, `Format="YYYYMMDD"`.
    *   Call BigQuery `DWDate_Vormonat` with `p_format='YYYYMMDD'`.
*   **Expected Output:** `20230930` (last day of September 2023).
*   **Pass/Fail Criterion:** The output from the legacy script and the BigQuery procedure must be identical.

```python
import pytest

def test_dwdate_vormonat_standard():
    mock_dir = setup_legacy_env()
    try:
        # Legacy execution
        var_name = "LEGACY_RESULT"
        format_str = "YYYYMMDD"
        legacy_output, legacy_rc = run_legacy_function("DWDate_Vormonat", var_name, format_str)
        assert legacy_rc == 0, f"Legacy script failed with RC {legacy_rc}"
        # The legacy script uses eval, so we need to parse the variable assignment
        # For simplicity in this example, assume the script echoes the result directly for testing
        # In a real scenario, you'd parse `eval "$VarName=..."` from the script's context
        # For this mock, the mock_sqlplus_vormonat writes directly to a temp file,
        # and the ksh script `cat`s it. So `legacy_output` will be the content of the temp file.
        expected_legacy_result = "20230930" # Based on TEST_CURRENT_DATE = 2023-10-26
        assert legacy_output == expected_legacy_result, f"Legacy output mismatch: {legacy_output}"

        # BigQuery execution
        bq_output = call_bq_procedure("DWDate_Vormonat", {"p_format": format_str}, {"p_result": "STRING"})
        assert bq_output["p_result"] == expected_legacy_result, \
            f"BQ output mismatch: Expected {expected_legacy_result}, Got {bq_output['p_result']}"

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 1.2: Previous Month with Different Format

*   **Purpose:** Test with a different date format.
*   **Setup:** `TEST_CURRENT_DATE = 2023-10-26`.
*   **Action:**
    *   Call legacy `DWDate_Vormonat` with `VarName="RESULT"`, `Format="DD.MM.YYYY"`.
    *   Call BigQuery `DWDate_Vormonat` with `p_format='DD.MM.YYYY'`.
*   **Expected Output:** `30.09.2023`.
*   **Pass/Fail Criterion:** The output from the legacy script and the BigQuery procedure must be identical.

```python
def test_dwdate_vormonat_different_format():
    mock_dir = setup_legacy_env()
    try:
        var_name = "LEGACY_RESULT"
        format_str = "DD.MM.YYYY"
        legacy_output, legacy_rc = run_legacy_function("DWDate_Vormonat", var_name, format_str)
        assert legacy_rc == 0
        expected_legacy_result = "30.09.2023"
        assert legacy_output == expected_legacy_result

        bq_output = call_bq_procedure("DWDate_Vormonat", {"p_format": format_str}, {"p_result": "STRING"})
        assert bq_output["p_result"] == expected_legacy_result

    finally:
        teardown_legacy_env(mock_dir)
```

---

## 2. `DWDate_Datum_Check`

*   **Purpose:** Verify that the BigQuery `DWDate_Datum_Check` procedure correctly validates date strings against a format, matching the legacy `sqlplus TO_DATE` behavior.
*   **Legacy Logic (Simulated):** The mock `sqlplus` for inline `TO_DATE` will return 0 for valid dates/formats and 1 for invalid ones.
*   **BQ Logic:** `SAFE.PARSE_DATE(p_format, p_wert)` and check for `NULL`.

### Test Case 2.1: Valid Date and Format

*   **Purpose:** Check a valid date string with its correct format.
*   **Action:**
    *   Call legacy `DWDate_Datum_Check` with `wert="20231026"`, `format="YYYYMMDD"`.
    *   Call BigQuery `DWDate_Datum_Check` with `p_wert='20231026'`, `p_format='YYYYMMDD'`.
*   **Expected Output:** Legacy return code `0`, BQ `p_is_valid=TRUE`.
*   **Pass/Fail Criterion:** Both systems indicate the date is valid.

```python
def test_dwdate_datum_check_valid():
    mock_dir = setup_legacy_env()
    try:
        wert = "20231026"
        format_str = "YYYYMMDD"
        legacy_output, legacy_rc = run_legacy_function("DWDate_Datum_Check", wert, format_str)
        assert legacy_rc == 0, f"Legacy script failed with RC {legacy_rc}"

        bq_output = call_bq_procedure("DWDate_Datum_Check", {"p_wert": wert, "p_format": format_str}, {"p_is_valid": "BOOL"})
        assert bq_output["p_is_valid"] is True, f"BQ output mismatch: Expected TRUE, Got {bq_output['p_is_valid']}"

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 2.2: Invalid Date String

*   **Purpose:** Check an invalid date string (e.g., non-existent month).
*   **Action:**
    *   Call legacy `DWDate_Datum_Check` with `wert="20231301"`, `format="YYYYMMDD"`.
    *   Call BigQuery `DWDate_Datum_Check` with `p_wert='20231301'`, `p_format='YYYYMMDD'`.
*   **Expected Output:** Legacy return code `1`, BQ `p_is_valid=FALSE`.
*   **Pass/Fail Criterion:** Both systems indicate the date is invalid.

```python
def test_dwdate_datum_check_invalid_date():
    mock_dir = setup_legacy_env()
    try:
        wert = "20231301" # Invalid month
        format_str = "YYYYMMDD"
        legacy_output, legacy_rc = run_legacy_function("DWDate_Datum_Check", wert, format_str)
        assert legacy_rc == 1, f"Legacy script passed unexpectedly with RC {legacy_rc}"

        bq_output = call_bq_procedure("DWDate_Datum_Check", {"p_wert": wert, "p_format": format_str}, {"p_is_valid": "BOOL"})
        assert bq_output["p_is_valid"] is False, f"BQ output mismatch: Expected FALSE, Got {bq_output['p_is_valid']}"

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 2.3: Valid Date, Incorrect Format

*   **Purpose:** Check a valid date string with an incorrect format.
*   **Action:**
    *   Call legacy `DWDate_Datum_Check` with `wert="2023-10-26"`, `format="YYYYMMDD"`.
    *   Call BigQuery `DWDate_Datum_Check` with `p_wert='2023-10-26'`, `p_format='YYYYMMDD'`.
*   **Expected Output:** Legacy return code `1`, BQ `p_is_valid=FALSE`.
*   **Pass/Fail Criterion:** Both systems indicate the date is invalid due to format mismatch.

```python
def test_dwdate_datum_check_incorrect_format():
    mock_dir = setup_legacy_env()
    try:
        wert = "2023-10-26" # Has hyphens
        format_str = "YYYYMMDD" # Expects no hyphens
        legacy_output, legacy_rc = run_legacy_function("DWDate_Datum_Check", wert, format_str)
        assert legacy_rc == 1

        bq_output = call_bq_procedure("DWDate_Datum_Check", {"p_wert": wert, "p_format": format_str}, {"p_is_valid": "BOOL"})
        assert bq_output["p_is_valid"] is False

    finally:
        teardown_legacy_env(mock_dir)
```

---

## 3. `DWDate_Datum_LE`

*   **Purpose:** Verify that the BigQuery `DWDate_Datum_LE` procedure correctly compares two dates (`P1 <= P2`), matching the legacy `sqlplus` PL/SQL block behavior.
*   **Legacy Logic (Simulated):** The mock `sqlplus` for inline PL/SQL will return 0 if `datum1 <= datum2` and 1 if `datum1 > datum2`.
*   **BQ Logic:** `PARSE_DATE('%Y%m%d', p_datum1)` and direct comparison.

### Test Case 3.1: Date1 Less Than Date2

*   **Purpose:** Test `P1 < P2`.
*   **Action:**
    *   Call legacy `DWDate_Datum_LE` with `datum1="20231025"`, `datum2="20231026"`.
    *   Call BigQuery `DWDate_Datum_LE` with `p_datum1='20231025'`, `p_datum2='20231026'`.
*   **Expected Output:** Legacy return code `0`, BQ `p_is_le=TRUE`.
*   **Pass/Fail Criterion:** Both systems indicate `P1 <= P2` is true.

```python
def test_dwdate_datum_le_less_than():
    mock_dir = setup_legacy_env()
    try:
        datum1 = "20231025"
        datum2 = "20231026"
        legacy_output, legacy_rc = run_legacy_function("DWDate_Datum_LE", datum1, datum2)
        assert legacy_rc == 0

        bq_output = call_bq_procedure("DWDate_Datum_LE", {"p_datum1": datum1, "p_datum2": datum2}, {"p_is_le": "BOOL"})
        assert bq_output["p_is_le"] is True

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 3.2: Date1 Equal To Date2

*   **Purpose:** Test `P1 = P2`.
*   **Action:**
    *   Call legacy `DWDate_Datum_LE` with `datum1="20231026"`, `datum2="20231026"`.
    *   Call BigQuery `DWDate_Datum_LE` with `p_datum1='20231026'`, `p_datum2='20231026'`.
*   **Expected Output:** Legacy return code `0`, BQ `p_is_le=TRUE`.
*   **Pass/Fail Criterion:** Both systems indicate `P1 <= P2` is true.

```python
def test_dwdate_datum_le_equal():
    mock_dir = setup_legacy_env()
    try:
        datum1 = "20231026"
        datum2 = "20231026"
        legacy_output, legacy_rc = run_legacy_function("DWDate_Datum_LE", datum1, datum2)
        assert legacy_rc == 0

        bq_output = call_bq_procedure("DWDate_Datum_LE", {"p_datum1": datum1, "p_datum2": datum2}, {"p_is_le": "BOOL"})
        assert bq_output["p_is_le"] is True

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 3.3: Date1 Greater Than Date2

*   **Purpose:** Test `P1 > P2`.
*   **Action:**
    *   Call legacy `DWDate_Datum_LE` with `datum1="20231027"`, `datum2="20231026"`.
    *   Call BigQuery `DWDate_Datum_LE` with `p_datum1='20231027'`, `p_datum2='20231026'`.
*   **Expected Output:** Legacy return code `1`, BQ `p_is_le=FALSE`.
*   **Pass/Fail Criterion:** Both systems indicate `P1 <= P2` is false.

```python
def test_dwdate_datum_le_greater_than():
    mock_dir = setup_legacy_env()
    try:
        datum1 = "20231027"
        datum2 = "20231026"
        legacy_output, legacy_rc = run_legacy_function("DWDate_Datum_LE", datum1, datum2)
        assert legacy_rc == 1

        bq_output = call_bq_procedure("DWDate_Datum_LE", {"p_datum1": datum1, "p_datum2": datum2}, {"p_is_le": "BOOL"})
        assert bq_output["p_is_le"] is False

    finally:
        teardown_legacy_env(mock_dir)
```

---

## 4. `DWDate_Gib_Zeitraum`

*   **Purpose:** Verify that the BigQuery `DWDate_Gib_Zeitraum` procedure correctly calculates date periods (start and end) based on offset and granularity, matching the legacy script's behavior.
*   **Legacy Logic (Simulated):** The mock `sqlplus` for `d_alis_datum_zeitraum.sql` will return start and end dates based on `TEST_CURRENT_DATE`, offset, and granularity, then parsed by `grep`/`cut`.
*   **BQ Logic:** Uses `CURRENT_DATE()`, `DATE_ADD`, `DATE_TRUNC`, `LAST_DAY` with conditional logic for `p_stufe`.

### Test Case 4.1: Daily Period (Offset 0)

*   **Purpose:** Get the current day's period.
*   **Setup:** `TEST_CURRENT_DATE = 2023-10-26`.
*   **Action:**
    *   Call legacy `DWDate_Gib_Zeitraum` with `Offset=0`, `Stufe="D"`, `Format="YYYYMMDD"`, `Var_Start="START"`, `Var_Ende="ENDE"`.
    *   Call BigQuery `DWDate_Gib_Zeitraum` with `p_offset=0`, `p_stufe='D'`, `p_format='YYYYMMDD'`.
*   **Expected Output:** `START=20231026`, `ENDE=20231026`.
*   **Pass/Fail Criterion:** Both systems return the same start and end dates.

```python
def test_dwdate_gib_zeitraum_daily_current():
    mock_dir = setup_legacy_env()
    try:
        offset = 0
        stufe = "D"
        format_str = "YYYYMMDD"
        var_start = "START"
        var_ende = "ENDE"

        legacy_output, legacy_rc = run_legacy_function("DWDate_Gib_Zeitraum", offset, stufe, format_str, var_start, var_ende)
        assert legacy_rc == 0
        # Legacy output is like "START=20231026\nENDE=20231026"
        legacy_start = legacy_output.split('\n')[0].split('=')[1]
        legacy_ende = legacy_output.split('\n')[1].split('=')[1]
        
        expected_start = "20231026"
        expected_ende = "20231026"
        assert legacy_start == expected_start and legacy_ende == expected_ende

        bq_output = call_bq_procedure(
            "DWDate_Gib_Zeitraum",
            {"p_offset": offset, "p_stufe": stufe, "p_format": format_str},
            {"p_start": "STRING", "p_ende": "STRING"}
        )
        assert bq_output["p_start"] == expected_start and bq_output["p_ende"] == expected_ende

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 4.2: Monthly Period (Offset -1)

*   **Purpose:** Get the previous month's period.
*   **Setup:** `TEST_CURRENT_DATE = 2023-10-26`.
*   **Action:**
    *   Call legacy `DWDate_Gib_Zeitraum` with `Offset=-1`, `Stufe="M"`, `Format="YYYYMMDD"`, `Var_Start="START"`, `Var_Ende="ENDE"`.
    *   Call BigQuery `DWDate_Gib_Zeitraum` with `p_offset=-1`, `p_stufe='M'`, `p_format='YYYYMMDD'`.
*   **Expected Output:** `START=20230901`, `ENDE=20230930`.
*   **Pass/Fail Criterion:** Both systems return the same start and end dates.

```python
def test_dwdate_gib_zeitraum_monthly_previous():
    mock_dir = setup_legacy_env()
    try:
        offset = -1
        stufe = "M"
        format_str = "YYYYMMDD"
        var_start = "START"
        var_ende = "ENDE"

        legacy_output, legacy_rc = run_legacy_function("DWDate_Gib_Zeitraum", offset, stufe, format_str, var_start, var_ende)
        assert legacy_rc == 0
        legacy_start = legacy_output.split('\n')[0].split('=')[1]
        legacy_ende = legacy_output.split('\n')[1].split('=')[1]
        
        expected_start = "20230901"
        expected_ende = "20230930"
        assert legacy_start == expected_start and legacy_ende == expected_ende

        bq_output = call_bq_procedure(
            "DWDate_Gib_Zeitraum",
            {"p_offset": offset, "p_stufe": stufe, "p_format": format_str},
            {"p_start": "STRING", "p_ende": "STRING"}
        )
        assert bq_output["p_start"] == expected_start and bq_output["p_ende"] == expected_ende

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 4.3: Yearly Period (Offset 1)

*   **Purpose:** Get the next year's period.
*   **Setup:** `TEST_CURRENT_DATE = 2023-10-26`.
*   **Action:**
    *   Call legacy `DWDate_Gib_Zeitraum` with `Offset=1`, `Stufe="Y"`, `Format="YYYYMMDD"`, `Var_Start="START"`, `Var_Ende="ENDE"`.
    *   Call BigQuery `DWDate_Gib_Zeitraum` with `p_offset=1`, `p_stufe='Y'`, `p_format='YYYYMMDD'`.
*   **Expected Output:** `START=20240101`, `ENDE=20241231`.
*   **Pass/Fail Criterion:** Both systems return the same start and end dates.

```python
def test_dwdate_gib_zeitraum_yearly_next():
    mock_dir = setup_legacy_env()
    try:
        offset = 1
        stufe = "Y"
        format_str = "YYYYMMDD"
        var_start = "START"
        var_ende = "ENDE"

        legacy_output, legacy_rc = run_legacy_function("DWDate_Gib_Zeitraum", offset, stufe, format_str, var_start, var_ende)
        assert legacy_rc == 0
        legacy_start = legacy_output.split('\n')[0].split('=')[1]
        legacy_ende = legacy_output.split('\n')[1].split('=')[1]
        
        expected_start = "20240101"
        expected_ende = "20241231"
        assert legacy_start == expected_start and legacy_ende == expected_ende

        bq_output = call_bq_procedure(
            "DWDate_Gib_Zeitraum",
            {"p_offset": offset, "p_stufe": stufe, "p_format": format_str},
            {"p_start": "STRING", "p_ende": "STRING"}
        )
        assert bq_output["p_start"] == expected_start and bq_output["p_ende"] == expected_ende

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 4.4: Invalid Stufe

*   **Purpose:** Verify error handling for an invalid `p_stufe`.
*   **Action:**
    *   Call legacy `DWDate_Gib_Zeitraum` with `Offset=0`, `Stufe="X"`, `Format="YYYYMMDD"`, `Var_Start="START"`, `Var_Ende="ENDE"`.
    *   Attempt to call BigQuery `DWDate_Gib_Zeitraum` with `p_offset=0`, `p_stufe='X'`, `p_format='YYYYMMDD'`.
*   **Expected Output:** Legacy return code `1` (or non-zero), BQ raises an error.
*   **Pass/Fail Criterion:** Both systems indicate an error for invalid input.

```python
def test_dwdate_gib_zeitraum_invalid_stufe():
    mock_dir = setup_legacy_env()
    try:
        offset = 0
        stufe = "X" # Invalid
        format_str = "YYYYMMDD"
        var_start = "START"
        var_ende = "ENDE"

        legacy_output, legacy_rc = run_legacy_function("DWDate_Gib_Zeitraum", offset, stufe, format_str, var_start, var_ende)
        assert legacy_rc != 0, f"Legacy script passed unexpectedly with RC {legacy_rc}"
        assert "Invalid Stufe" in legacy_output # Check for error message from mock

        with pytest.raises(Exception) as excinfo:
            call_bq_procedure(
                "DWDate_Gib_Zeitraum",
                {"p_offset": offset, "p_stufe": stufe, "p_format": format_str},
                {"p_start": "STRING", "p_ende": "STRING"}
            )
        assert "Invalid p_stufe" in str(excinfo.value)

    finally:
        teardown_legacy_env(mock_dir)
```

---

## 5. `LetzterTagDesMonat`

*   **Purpose:** Verify that the BigQuery `LetzterTagDesMonat` procedure correctly identifies if a given date is the last day of its month, including leap year handling.
*   **Legacy Logic:** Manual parsing, leap year calculation, array lookup. Returns 0 if last day, 1 otherwise.
*   **BQ Logic:** `PARSE_DATE('%Y%m%d', p_datum)` and `v_date = LAST_DAY(v_date)`.

### Test Case 5.1: Last Day of a Standard Month

*   **Purpose:** Check a date that is the last day of a 31-day month.
*   **Action:**
    *   Call legacy `LetzterTagDesMonats` with `datum="20231031"`.
    *   Call BigQuery `LetzterTagDesMonat` with `p_datum='20231031'`.
*   **Expected Output:** Legacy return code `0`, BQ `p_is_last_day=TRUE`.
*   **Pass/Fail Criterion:** Both systems indicate it's the last day.

```python
def test_letztertagdesmonat_standard_last_day():
    mock_dir = setup_legacy_env()
    try:
        datum = "20231031"
        legacy_output, legacy_rc = run_legacy_function("LetzterTagDesMonats", datum)
        assert legacy_rc == 0

        bq_output = call_bq_procedure("LetzterTagDesMonat", {"p_datum": datum}, {"p_is_last_day": "BOOL"})
        assert bq_output["p_is_last_day"] is True

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 5.2: Not the Last Day of a Month

*   **Purpose:** Check a date that is not the last day.
*   **Action:**
    *   Call legacy `LetzterTagDesMonats` with `datum="20231015"`.
    *   Call BigQuery `LetzterTagDesMonat` with `p_datum='20231015'`.
*   **Expected Output:** Legacy return code `1`, BQ `p_is_last_day=FALSE`.
*   **Pass/Fail Criterion:** Both systems indicate it's not the last day.

```python
def test_letztertagdesmonat_not_last_day():
    mock_dir = setup_legacy_env()
    try:
        datum = "20231015"
        legacy_output, legacy_rc = run_legacy_function("LetzterTagDesMonats", datum)
        assert legacy_rc == 1

        bq_output = call_bq_procedure("LetzterTagDesMonat", {"p_datum": datum}, {"p_is_last_day": "BOOL"})
        assert bq_output["p_is_last_day"] is False

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 5.3: Leap Year February Last Day

*   **Purpose:** Check February 29th in a leap year.
*   **Action:**
    *   Call legacy `LetzterTagDesMonats` with `datum="20240229"`.
    *   Call BigQuery `LetzterTagDesMonat` with `p_datum='20240229'`.
*   **Expected Output:** Legacy return code `0`, BQ `p_is_last_day=TRUE`.
*   **Pass/Fail Criterion:** Both systems indicate it's the last day.

```python
def test_letztertagdesmonat_leap_year_feb_last_day():
    mock_dir = setup_legacy_env()
    try:
        datum = "20240229" # 2024 is a leap year
        legacy_output, legacy_rc = run_legacy_function("LetzterTagDesMonats", datum)
        assert legacy_rc == 0

        bq_output = call_bq_procedure("LetzterTagDesMonat", {"p_datum": datum}, {"p_is_last_day": "BOOL"})
        assert bq_output["p_is_last_day"] is True

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 5.4: Non-Leap Year February Last Day

*   **Purpose:** Check February 28th in a non-leap year.
*   **Action:**
    *   Call legacy `LetzterTagDesMonats` with `datum="20230228"`.
    *   Call BigQuery `LetzterTagDesMonat` with `p_datum='20230228'`.
*   **Expected Output:** Legacy return code `0`, BQ `p_is_last_day=TRUE`.
*   **Pass/Fail Criterion:** Both systems indicate it's the last day.

```python
def test_letztertagdesmonat_non_leap_year_feb_last_day():
    mock_dir = setup_legacy_env()
    try:
        datum = "20230228" # 2023 is not a leap year
        legacy_output, legacy_rc = run_legacy_function("LetzterTagDesMonats", datum)
        assert legacy_rc == 0

        bq_output = call_bq_procedure("LetzterTagDesMonat", {"p_datum": datum}, {"p_is_last_day": "BOOL"})
        assert bq_output["p_is_last_day"] is True

    finally:
        teardown_legacy_env(mock_dir)
```

---

## 6. `TageimMonat`

*   **Purpose:** Verify that the BigQuery `TageimMonat` procedure correctly calculates the number of days in a specific month of a year, including leap year handling.
*   **Legacy Logic:** Manual leap year calculation, array lookup. `echo`s the number of days.
*   **BQ Logic:** `DATE`, `LAST_DAY`, `DATE_DIFF`.

### Test Case 6.1: Days in a 31-day Month

*   **Purpose:** Check a standard 31-day month.
*   **Action:**
    *   Call legacy `TageimMonat` with `jahr=2023`, `monat=10`.
    *   Call BigQuery `TageimMonat` with `p_jahr=2023`, `p_monat=10`.
*   **Expected Output:** `31`.
*   **Pass/Fail Criterion:** Both systems return `31`.

```python
def test_tageimmonat_31_days():
    mock_dir = setup_legacy_env()
    try:
        jahr = 2023
        monat = 10
        legacy_output, legacy_rc = run_legacy_function("TageimMonat", jahr, monat)
        assert legacy_rc == 0
        assert int(legacy_output) == 31

        bq_output = call_bq_procedure("TageimMonat", {"p_jahr": jahr, "p_monat": monat}, {"p_tage": "INT64"})
        assert bq_output["p_tage"] == 31

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 6.2: Days in February (Leap Year)

*   **Purpose:** Check February in a leap year.
*   **Action:**
    *   Call legacy `TageimMonat` with `jahr=2024`, `monat=2`.
    *   Call BigQuery `TageimMonat` with `p_jahr=2024`, `p_monat=2`.
*   **Expected Output:** `29`.
*   **Pass/Fail Criterion:** Both systems return `29`.

```python
def test_tageimmonat_feb_leap_year():
    mock_dir = setup_legacy_env()
    try:
        jahr = 2024
        monat = 2
        legacy_output, legacy_rc = run_legacy_function("TageimMonat", jahr, monat)
        assert legacy_rc == 0
        assert int(legacy_output) == 29

        bq_output = call_bq_procedure("TageimMonat", {"p_jahr": jahr, "p_monat": monat}, {"p_tage": "INT64"})
        assert bq_output["p_tage"] == 29

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 6.3: Days in February (Non-Leap Year)

*   **Purpose:** Check February in a non-leap year.
*   **Action:**
    *   Call legacy `TageimMonat` with `jahr=2023`, `monat=2`.
    *   Call BigQuery `TageimMonat` with `p_jahr=2023`, `p_monat=2`.
*   **Expected Output:** `28`.
*   **Pass/Fail Criterion:** Both systems return `28`.

```python
def test_tageimmonat_feb_non_leap_year():
    mock_dir = setup_legacy_env()
    try:
        jahr = 2023
        monat = 2
        legacy_output, legacy_rc = run_legacy_function("TageimMonat", jahr, monat)
        assert legacy_rc == 0
        assert int(legacy_output) == 28

        bq_output = call_bq_procedure("TageimMonat", {"p_jahr": jahr, "p_monat": monat}, {"p_tage": "INT64"})
        assert bq_output["p_tage"] == 28

    finally:
        teardown_legacy_env(mock_dir)
```

---

## 7. `AddiereDatum`

*   **Purpose:** Verify that the BigQuery `AddiereDatum` procedure correctly adds a specified number of days to a given date, handling month and year rollovers.
*   **Legacy Logic:** Manual parsing, arithmetic, and `while` loops for rollovers, `tail -3c` for padding. `echo`s the result.
*   **BQ Logic:** `PARSE_DATE('%Y%m%d', p_datum)`, `DATE_ADD`, `FORMAT_DATE`.

### Test Case 7.1: Add Days Within Same Month

*   **Purpose:** Add a small number of days without crossing month boundaries.
*   **Action:**
    *   Call legacy `AddiereDatum` with `datum="20231015"`, `tage=5`.
    *   Call BigQuery `AddiereDatum` with `p_datum='20231015'`, `p_tage=5`.
*   **Expected Output:** `20231020`.
*   **Pass/Fail Criterion:** Both systems return `20231020`.

```python
def test_addieredatum_within_month():
    mock_dir = setup_legacy_env()
    try:
        datum = "20231015"
        tage = 5
        legacy_output, legacy_rc = run_legacy_function("AddiereDatum", datum, tage)
        assert legacy_rc == 0
        assert legacy_output == "20231020"

        bq_output = call_bq_procedure("AddiereDatum", {"p_datum": datum, "p_tage": tage}, {"p_result": "STRING"})
        assert bq_output["p_result"] == "20231020"

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 7.2: Add Days Across Month Boundary

*   **Purpose:** Add days that cross into the next month.
*   **Action:**
    *   Call legacy `AddiereDatum` with `datum="20231025"`, `tage=10`.
    *   Call BigQuery `AddiereDatum` with `p_datum='20231025'`, `p_tage=10`.
*   **Expected Output:** `20231104`.
*   **Pass/Fail Criterion:** Both systems return `20231104`.

```python
def test_addieredatum_across_month_boundary():
    mock_dir = setup_legacy_env()
    try:
        datum = "20231025"
        tage = 10
        legacy_output, legacy_rc = run_legacy_function("AddiereDatum", datum, tage)
        assert legacy_rc == 0
        assert legacy_output == "20231104"

        bq_output = call_bq_procedure("AddiereDatum", {"p_datum": datum, "p_tage": tage}, {"p_result": "STRING"})
        assert bq_output["p_result"] == "20231104"

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 7.3: Add Days Across Year Boundary

*   **Purpose:** Add days that cross into the next year.
*   **Action:**
    *   Call legacy `AddiereDatum` with `datum="20231225"`, `tage=10`.
    *   Call BigQuery `AddiereDatum` with `p_datum='20231225'`, `p_tage=10`.
*   **Expected Output:** `20240104`.
*   **Pass/Fail Criterion:** Both systems return `20240104`.

```python
def test_addieredatum_across_year_boundary():
    mock_dir = setup_legacy_env()
    try:
        datum = "20231225"
        tage = 10
        legacy_output, legacy_rc = run_legacy_function("AddiereDatum", datum, tage)
        assert legacy_rc == 0
        assert legacy_output == "20240104"

        bq_output = call_bq_procedure("AddiereDatum", {"p_datum": datum, "p_tage": tage}, {"p_result": "STRING"})
        assert bq_output["p_result"] == "20240104"

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 7.4: Add Days Across Leap Year February

*   **Purpose:** Add days that cross February 29th in a leap year.
*   **Action:**
    *   Call legacy `AddiereDatum` with `datum="20240228"`, `tage=2`.
    *   Call BigQuery `AddiereDatum` with `p_datum='20240228'`, `p_tage=2`.
*   **Expected Output:** `20240301`.
*   **Pass/Fail Criterion:** Both systems return `20240301`.

```python
def test_addieredatum_across_leap_year_feb():
    mock_dir = setup_legacy_env()
    try:
        datum = "20240228" # 2024 is a leap year
        tage = 2
        legacy_output, legacy_rc = run_legacy_function("AddiereDatum", datum, tage)
        assert legacy_rc == 0
        assert legacy_output == "20240301"

        bq_output = call_bq_procedure("AddiereDatum", {"p_datum": datum, "p_tage": tage}, {"p_result": "STRING"})
        assert bq_output["p_result"] == "20240301"

    finally:
        teardown_legacy_env(mock_dir)
```

### Test Case 7.5: Subtract Days

*   **Purpose:** Subtract days (negative `tage`).
*   **Action:**
    *   Call legacy `AddiereDatum` with `datum="20231015"`, `tage=-20`.
    *   Call BigQuery `AddiereDatum` with `p_datum='20231015'`, `p_tage=-20`.
*   **Expected Output:** `20230925`.
*   **Pass/Fail Criterion:** Both systems return `20230925`.

```python
def test_addieredatum_subtract_days():
    mock_dir = setup_legacy_env()
    try:
        datum = "20231015"
        tage = -20
        legacy_output, legacy_rc = run_legacy_function("AddiereDatum", datum, tage)
        assert legacy_rc == 0
        assert legacy_output == "20230925"

        bq_output = call_bq_procedure("AddiereDatum", {"p_datum": datum, "p_tage": tage}, {"p_result": "STRING"})
        assert bq_output["p_result"] == "20230925"

    finally:
        teardown_legacy_env(mock_dir)
```