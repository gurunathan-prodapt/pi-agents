As a senior data-migration QA engineer, I've developed a comprehensive suite of validation tests for the migration of `gestern.ksh` to Google BigQuery. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

The core challenge for this migration is the implicit input (system date) of the legacy script and the explicit handling of dates in BigQuery. To address this, the tests simulate specific system dates for both the legacy and BigQuery environments.

### Pre-requisites for Running Tests:

1.  **Legacy Script Availability**: The `gestern.ksh` script must be present at the specified path (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`) and be executable (`chmod +x`).
2.  **`faketime` Utility**: The `faketime` utility must be installed on the system where the Python tests are executed. This allows simulating specific dates for the legacy script.
3.  **BigQuery Access**: A Google Cloud project with BigQuery API enabled, and appropriate authentication configured for the Python client (e.g., `gcloud auth application-default login` or service account key).
4.  **BigQuery Script Deployment**: The `gestern_bq.sql` script should be deployed in BigQuery. For testing purposes, it's recommended to wrap its logic in a BigQuery Stored Procedure that accepts a `DATE` parameter, allowing `CURRENT_DATE()` to be overridden. Alternatively, the test runner can dynamically construct the SQL with `DATE 'YYYY-MM-DD'` literals. The provided Python test code assumes the latter approach for simplicity in demonstration.

---

### Python Test Harness Setup

The following Python code provides a `pytest` framework for running the validation tests. It includes helper functions to execute the legacy KornShell script with `faketime` and to execute the BigQuery SQL with a simulated date.

```python
import subprocess
import pytest
from google.cloud import bigquery
from datetime import date, timedelta
import os

# --- Configuration ---
LEGACY_SCRIPT_PATH = "vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh"
# Ensure the script is executable
if os.path.exists(LEGACY_SCRIPT_PATH):
    os.chmod(LEGACY_SCRIPT_PATH, 0o755)
else:
    pytest.skip(f"Legacy script not found at {LEGACY_SCRIPT_PATH}. Skipping legacy script tests.", allow_module_level=True)

# BigQuery client (initialize once for all tests)
# In a real scenario, you'd configure your project ID and authentication
# client = bigquery.Client(project="your-gcp-project-id")
# For demonstration, we'll use a mock client or direct calculation for BQ results
# If you have a real BQ project, uncomment and configure:
# try:
#     client = bigquery.Client(project="your-gcp-project-id")
# except Exception as e:
#     pytest.skip(f"BigQuery client initialization failed: {e}. Skipping BigQuery tests.", allow_module_level=True)

# --- Helper Functions ---

def run_legacy_script(test_date_str, timezone="UTC"):
    """
    Runs the legacy ksh script using faketime to simulate a specific date and timezone.
    Returns a dictionary of the parsed output.
    """
    # faketime expects a full datetime string. We use 12:00:00 to avoid day boundary issues
    # with timezones if only a date is provided.
    faketime_cmd = f"faketime -f '{test_date_str} 12:00:00 {timezone}' {LEGACY_SCRIPT_PATH}"
    
    try:
        result = subprocess.run(
            faketime_cmd,
            shell=True, # shell=True is needed for faketime command string
            capture_output=True,
            text=True,
            check=True
        )
        output_parts = result.stdout.strip().split()
        if len(output_parts) == 4:
            return {
                "today_ymd": output_parts[0],
                "yesterday_ymd": output_parts[1],
                "today_ym": output_parts[2],
                "yesterday_ym": output_parts[3],
            }
        elif "Fehler !!!!" in result.stdout:
            return {"error": "Fehler !!!!"}
        else:
            pytest.fail(f"Legacy script returned unexpected output format for date {test_date_str}: {result.stdout}")
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Legacy script failed for date {test_date_str}: {e.stderr}")
    except FileNotFoundError:
        pytest.fail(f"Legacy script not found at {LEGACY_SCRIPT_PATH}. Please ensure it's accessible and executable.")

def run_bigquery_script(simulated_date_str):
    """
    Runs the BigQuery script, replacing CURRENT_DATE() with a simulated date.
    Returns a dictionary of the results.
    """
    bq_sql_template = """
    DECLARE today_date DATE DEFAULT DATE '{simulated_date}';
    DECLARE yesterday_date DATE DEFAULT DATE_SUB(DATE '{simulated_date}', INTERVAL 1 DAY);

    DECLARE Var_Datum_Heute STRING DEFAULT FORMAT_DATE('%Y%m%d', today_date);
    DECLARE Var_Datum_Gestern STRING DEFAULT FORMAT_DATE('%Y%m%d', yesterday_date);
    DECLARE Var_Monat_Heute STRING DEFAULT FORMAT_DATE('%Y%m', today_date);
    DECLARE Var_Monat_Gestern STRING DEFAULT FORMAT_DATE('%Y%m', yesterday_date);

    SELECT
      Var_Datum_Heute AS today_ymd,
      Var_Datum_Gestern AS yesterday_ymd,
      Var_Monat_Heute AS today_ym,
      Var_Monat_Gestern AS yesterday_ym;
    """
    
    query = bq_sql_template.format(simulated_date=simulated_date_str)
    
    # --- Actual BigQuery Execution (uncomment and configure for real testing) ---
    # try:
    #     query_job = client.query(query)
    #     results = query_job.result()
    #     for row in results:
    #         return dict(row)
    #     pytest.fail(f"BigQuery script returned no rows for date {simulated_date_str}")
    # except Exception as e:
    #     pytest.fail(f"BigQuery script execution failed for date {simulated_date_str}: {e}")

    # --- Mock BigQuery Execution (for demonstration without a live BQ connection) ---
    # This part simulates the *expected* BigQuery output based on its logic.
    # In a real test, this would be replaced by the actual client.query() call.
    today_dt = date.fromisoformat(simulated_date_str)
    yesterday_dt = today_dt - timedelta(days=1)

    return {
        "today_ymd": today_dt.strftime('%Y%m%d'),
        "yesterday_ymd": yesterday_dt.strftime('%Y%m%d'),
        "today_ym": today_dt.strftime('%Y%m'),
        "yesterday_ym": yesterday_dt.strftime('%Y%m'),
    }

# --- Test Cases ---
```

---

### 1. Output Parity & Transformation Correctness Tests

These tests verify that the BigQuery script produces the same date calculations and formatted output as the legacy script for various date scenarios, including critical month and year transitions. The BigQuery script is expected to handle leap years correctly according to the Gregorian calendar, which is an improvement over the legacy script's "rudimentary" check as per the design document.

#### Test Case 1.1: Standard Day Transition

*   **Purpose**: Verify correct calculation for a typical day within a month.
*   **Setup**: Simulate `CURRENT_DATE()` as `2023-10-15`.
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2023-10-15'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2023-10-15'`.
*   **Pass/Fail Criterion**: The output values (`today_ymd`, `yesterday_ymd`, `today_ym`, `yesterday_ym`) from both scripts must be identical.
    *   Expected Output: `20231015 20231014 202310 202310`

```python
def test_transformation_correctness_standard_day_transition():
    test_date_str = "2023-10-15"
    legacy_output = run_legacy_script(test_date_str)
    bq_output = run_bigquery_script(test_date_str)

    assert legacy_output == {
        "today_ymd": "20231015",
        "yesterday_ymd": "20231014",
        "today_ym": "202310",
        "yesterday_ym": "202310",
    }
    assert bq_output == legacy_output, f"BQ output mismatch for {test_date_str}"
```

#### Test Case 1.2: Month Transition (Day 1)

*   **Purpose**: Verify correct calculation when today is the 1st of a month, transitioning to the previous month.
*   **Setup**: Simulate `CURRENT_DATE()` as `2023-10-01`.
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2023-10-01'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2023-10-01'`.
*   **Pass/Fail Criterion**: The output values from both scripts must be identical.
    *   Expected Output: `20231001 20230930 202310 202309`

```python
def test_transformation_correctness_month_transition_day_1():
    test_date_str = "2023-10-01"
    legacy_output = run_legacy_script(test_date_str)
    bq_output = run_bigquery_script(test_date_str)

    assert legacy_output == {
        "today_ymd": "20231001",
        "yesterday_ymd": "20230930",
        "today_ym": "202310",
        "yesterday_ym": "202309",
    }
    assert bq_output == legacy_output, f"BQ output mismatch for {test_date_str}"
```

#### Test Case 1.3: Year Transition (Jan 1st)

*   **Purpose**: Verify correct calculation when today is January 1st, transitioning to the previous year.
*   **Setup**: Simulate `CURRENT_DATE()` as `2024-01-01`.
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2024-01-01'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2024-01-01'`.
*   **Pass/Fail Criterion**: The output values from both scripts must be identical.
    *   Expected Output: `20240101 20231231 202401 202312`

```python
def test_transformation_correctness_year_transition_jan_1():
    test_date_str = "2024-01-01"
    legacy_output = run_legacy_script(test_date_str)
    bq_output = run_bigquery_script(test_date_str)

    assert legacy_output == {
        "today_ymd": "20240101",
        "yesterday_ymd": "20231231",
        "today_ym": "202401",
        "yesterday_ym": "202312",
    }
    assert bq_output == legacy_output, f"BQ output mismatch for {test_date_str}"
```

#### Test Case 1.4: Leap Year - Feb 29th

*   **Purpose**: Verify correct calculation for February 29th in a leap year (yesterday is Feb 28th).
*   **Setup**: Simulate `CURRENT_DATE()` as `2024-02-29` (2024 is a leap year).
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2024-02-29'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2024-02-29'`.
*   **Pass/Fail Criterion**: The output values from both scripts must be identical.
    *   Expected Output: `20240229 20240228 202402 202402`

```python
def test_transformation_correctness_leap_year_feb_29():
    test_date_str = "2024-02-29"
    legacy_output = run_legacy_script(test_date_str)
    bq_output = run_bigquery_script(test_date_str)

    assert legacy_output == {
        "today_ymd": "20240229",
        "yesterday_ymd": "20240228",
        "today_ym": "202402",
        "yesterday_ym": "202402",
    }
    assert bq_output == legacy_output, f"BQ output mismatch for {test_date_str}"
```

#### Test Case 1.5: Leap Year - March 1st (after Feb 29th)

*   **Purpose**: Verify correct calculation for March 1st in a leap year (yesterday is Feb 29th). This is a critical test for the BigQuery's "greater precision" in leap year handling.
*   **Setup**: Simulate `CURRENT_DATE()` as `2024-03-01` (2024 is a leap year).
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2024-03-01'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2024-03-01'`.
*   **Pass/Fail Criterion**: The output values from both scripts must be identical.
    *   Expected Output: `20240301 20240229 202403 202402`

```python
def test_transformation_correctness_leap_year_march_1():
    test_date_str = "2024-03-01"
    legacy_output = run_legacy_script(test_date_str)
    bq_output = run_bigquery_script(test_date_str)

    assert legacy_output == {
        "today_ymd": "20240301",
        "yesterday_ymd": "20240229",
        "today_ym": "202403",
        "yesterday_ym": "202402",
    }
    assert bq_output == legacy_output, f"BQ output mismatch for {test_date_str}"
```

#### Test Case 1.6: Non-Leap Year - March 1st

*   **Purpose**: Verify correct calculation for March 1st in a non-leap year (yesterday is Feb 28th).
*   **Setup**: Simulate `CURRENT_DATE()` as `2023-03-01` (2023 is not a leap year).
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2023-03-01'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2023-03-01'`.
*   **Pass/Fail Criterion**: The output values from both scripts must be identical.
    *   Expected Output: `20230301 20230228 202303 202302`

```python
def test_transformation_correctness_non_leap_year_march_1():
    test_date_str = "2023-03-01"
    legacy_output = run_legacy_script(test_date_str)
    bq_output = run_bigquery_script(test_date_str)

    assert legacy_output == {
        "today_ymd": "20230301",
        "yesterday_ymd": "20230228",
        "today_ym": "202303",
        "yesterday_ym": "202302",
    }
    assert bq_output == legacy_output, f"BQ output mismatch for {test_date_str}"
```

#### Test Case 1.7: Edge Case - Year 2000 (Divisible by 100 but also 400)

*   **Purpose**: Test the legacy script's rudimentary leap year logic against a year divisible by 100 and 400 (e.g., 2000). The legacy script's `year % 100 > 0` condition would fail for 2000, incorrectly treating Feb 29th as Feb 28th. BigQuery's `DATE_SUB` will correctly identify 2000 as a leap year. This test highlights the "greater precision" of BigQuery.
*   **Setup**: Simulate `CURRENT_DATE()` as `2000-03-01`.
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2000-03-01'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2000-03-01'`.
*   **Pass/Fail Criterion**:
    *   Legacy script output for `yesterday_ymd` should be `20000228`.
    *   BigQuery script output for `yesterday_ymd` should be `20000229`.
    *   The test passes if these *expected differences* are observed, confirming the BigQuery's improved accuracy as per the design document.

```python
def test_transformation_correctness_year_2000_leap_year_discrepancy():
    test_date_str = "2000-03-01"
    legacy_output = run_legacy_script(test_date_str)
    bq_output = run_bigquery_script(test_date_str)

    # Legacy script's rudimentary leap year check for 2000-03-01 (yesterday is Feb)
    # 2000 % 4 == 0 (True)
    # 2000 % 100 > 0 (False) -> so the leap year condition is False
    # Thus, it calculates Feb 28th, not Feb 29th.
    assert legacy_output["yesterday_ymd"] == "20000228", "Legacy script should incorrectly calculate Feb 28th for 2000-03-01"
    
    # BigQuery's DATE_SUB is Gregorian calendar compliant and correctly calculates Feb 29th.
    assert bq_output["yesterday_ymd"] == "20000229", "BigQuery should correctly calculate Feb 29th for 2000-03-01"
    
    # The overall test passes if these specific differences are observed,
    # confirming the BQ's "greater precision" as intended by the design.
    assert legacy_output["today_ymd"] == bq_output["today_ymd"]
    assert legacy_output["today_ym"] == bq_output["today_ym"]
    assert legacy_output["yesterday_ym"] == bq_output["yesterday_ym"]
    
    print(f"Test 1.7: Legacy yesterday: {legacy_output['yesterday_ymd']}, BQ yesterday: {bq_output['yesterday_ymd']}")
    print("This difference is expected and confirms BigQuery's improved leap year accuracy.")
```

---

### 2. External-System Replacements / Timezone Handling

The design document highlights timezone as a risk, noting that the legacy script uses the server's local timezone, while BigQuery's `CURRENT_DATE()` defaults to UTC. This test verifies the impact of this difference.

#### Test Case 2.1: Timezone Discrepancy at Day Boundary

*   **Purpose**: Verify how timezone differences affect the "today" and "yesterday" calculation when the local time is "today" but UTC is still "yesterday". This tests the implicit external system (system clock/timezone) replacement.
*   **Setup**:
    *   Assume legacy system runs in `Europe/Berlin` (CET/CEST).
    *   Simulate `CURRENT_DATE()` for legacy as `2023-10-01 00:30:00 CET` (which is `2023-09-30 22:30:00 UTC`).
    *   Simulate `CURRENT_DATE()` for BigQuery as `2023-09-30` (since `CURRENT_DATE()` in BQ is UTC by default).
*   **Action**:
    1.  Execute `gestern.ksh` with `faketime '2023-10-01 00:30:00 Europe/Berlin'`.
    2.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2023-09-30'` (representing the UTC date at that time).
*   **Pass/Fail Criterion**:
    *   The legacy script should output dates based on `2023-10-01`.
    *   The BigQuery script (using UTC `CURRENT_DATE()`) should output dates based on `2023-09-30`.
    *   The test passes if these *expected differences* are observed, confirming the timezone behavior as described in the design document. This highlights a necessary adjustment if the legacy system's timezone was critical.

```python
def test_external_system_replacement_timezone_discrepancy():
    # Simulate legacy system running in Europe/Berlin (CET/CEST)
    # At 2023-10-01 00:30:00 CET, UTC is 2023-09-30 22:30:00
    legacy_test_datetime_str = "2023-10-01 00:30:00"
    legacy_timezone = "Europe/Berlin"
    
    # BigQuery's CURRENT_DATE() would resolve to 2023-09-30 if run at this UTC time
    bq_simulated_date_str = "2023-09-30"

    legacy_output = run_legacy_script(legacy_test_datetime_str, timezone=legacy_timezone)
    bq_output = run_bigquery_script(bq_simulated_date_str)

    # Legacy output should reflect 2023-10-01 as today
    assert legacy_output["today_ymd"] == "20231001"
    assert legacy_output["yesterday_ymd"] == "20230930"

    # BigQuery output should reflect 2023-09-30 as today (due to UTC CURRENT_DATE())
    assert bq_output["today_ymd"] == "20230930"
    assert bq_output["yesterday_ymd"] == "20230929"

    # This test passes if the outputs are different as expected,
    # confirming the timezone behavior described in the design document.
    assert legacy_output != bq_output
    print(f"Test 2.1: Legacy output (CET): {legacy_output}")
    print(f"Test 2.1: BQ output (UTC): {bq_output}")
    print("This difference is expected due to timezone handling as per design document.")

# Recommendation: If the legacy system's timezone is critical, the BigQuery script
# should be modified to explicitly use that timezone, e.g.,
# DECLARE today_date DATE DEFAULT CURRENT_DATE('Europe/Berlin');
# And this test would then expect parity.
```

---

### 3. Data Quality / Row Count / Schema Assertions

These tests ensure the BigQuery output adheres to the expected structure and data characteristics.

#### Test Case 3.1: Output Schema and Data Types

*   **Purpose**: Verify that the BigQuery script produces a result set with the expected column names and data types.
*   **Setup**: Execute the BigQuery script with a sample date.
*   **Action**:
    1.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2023-01-01'`.
    2.  Inspect the schema of the returned result set.
*   **Pass/Fail Criterion**: The result set must contain exactly four columns with the names `today_ymd`, `yesterday_ymd`, `today_ym`, `yesterday_ym`, and all must be of `STRING` data type.

```python
def test_data_quality_output_schema_and_types():
    # This test requires actual BigQuery client execution to inspect schema
    # For demonstration, we'll assert on the structure of the returned dictionary.
    # In a real scenario, you'd use client.query(...).result().schema
    
    test_date_str = "2023-01-01"
    bq_output = run_bigquery_script(test_date_str) # This mock returns a dict

    # Assert column names
    expected_columns = ["today_ymd", "yesterday_ymd", "today_ym", "yesterday_ym"]
    assert sorted(list(bq_output.keys())) == sorted(expected_columns), \
        "BigQuery output columns do not match expected schema."

    # Assert data types (based on the mock, all are strings)
    for key, value in bq_output.items():
        assert isinstance(value, str), f"Column '{key}' is not of type STRING."
        # Further validation: check length and format
        if 'ymd' in key:
            assert len(value) == 8, f"Column '{key}' (YYYYMMDD) has incorrect length."
            assert value.isdigit(), f"Column '{key}' (YYYYMMDD) contains non-digit characters."
        elif 'ym' in key:
            assert len(value) == 6, f"Column '{key}' (YYYYMM) has incorrect length."
            assert value.isdigit(), f"Column '{key}' (YYYYMM) contains non-digit characters."

    # For a real BigQuery client, you'd do:
    # query = "SELECT Var_Datum_Heute AS today_ymd, Var_Datum_Gestern AS yesterday_ymd, ..."
    # job = client.query(query)
    # schema = job.result().schema
    # assert len(schema) == 4
    # for field in schema:
    #     assert field.name in expected_columns
    #     assert field.field_type == "STRING"
```

#### Test Case 3.2: Output Row Count

*   **Purpose**: Verify that the BigQuery script always produces exactly one row of output.
*   **Setup**: Execute the BigQuery script with a sample date.
*   **Action**:
    1.  Execute `gestern_bq.sql` with `CURRENT_DATE()` replaced by `DATE '2023-01-01'`.
    2.  Count the number of rows in the result set.
*   **Pass/Fail Criterion**: The result set must contain exactly one row.

```python
def test_data_quality_output_row_count():
    # This test requires actual BigQuery client execution to count rows.
    # The mock `run_bigquery_script` always returns a single dictionary,
    # implicitly satisfying this. For a real test:
    
    test_date_str = "2023-01-01"
    
    # --- Actual BigQuery Execution (uncomment and configure for real testing) ---
    # bq_sql_template = """
    # DECLARE today_date DATE DEFAULT DATE '{simulated_date}';
    # DECLARE yesterday_date DATE DEFAULT DATE_SUB(DATE '{simulated_date}', INTERVAL 1 DAY);
    # DECLARE Var_Datum_Heute STRING DEFAULT FORMAT_DATE('%Y%m%d', today_date);
    # DECLARE Var_Datum_Gestern STRING DEFAULT FORMAT_DATE('%Y%m%d', yesterday_date);
    # DECLARE Var_Monat_Heute STRING DEFAULT FORMAT_DATE('%Y%m', today_date);
    # DECLARE Var_Monat_Gestern STRING DEFAULT FORMAT_DATE('%Y%m', yesterday_date);
    # SELECT Var_Datum_Heute AS today_ymd, Var_Datum_Gestern AS yesterday_ymd, Var_Monat_Heute AS today_ym, Var_Monat_Gestern AS yesterday_ym;
    # """
    # query = bq_sql_template.format(simulated_date=test_date_str)
    # try:
    #     query_job = client.query(query)
    #     results = query_job.result()
    #     row_count = sum(1 for _ in results) # Iterate to count rows
    #     assert row_count == 1, f"BigQuery script returned {row_count} rows, expected 1."
    # except Exception as e:
    #     pytest.fail(f"BigQuery script execution failed for row count test: {e}")

    # --- Mock BigQuery Execution (for demonstration) ---
    bq_output = run_bigquery_script(test_date_str)
    assert isinstance(bq_output, dict) and len(bq_output) == 4, \
        "Mock BigQuery output did not return a single dictionary with 4 elements."
    print("Test 3.2: BigQuery script returned a single logical row (as a dictionary).")

```