As a senior data-migration QA engineer, I've analyzed the migration design for `gestern.ksh` to BigQuery. The core challenge is ensuring behavioral equivalence, especially concerning date calculations, formatting, and handling of edge cases like leap years. A critical discrepancy has been identified regarding the output format due to a subtle bug in the original KornShell script's `date` command usage.

The following tests are designed to validate the migrated BigQuery code against the legacy KornShell script, covering output parity, transformation correctness, external system replacements, and data quality.

---

## Setup for Running Tests

To execute the provided tests, you'll need:

1.  **Legacy Script:** The original `gestern.ksh` script, located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`. For testing, we'll assume it's placed at `./legacy_scripts/gestern.ksh` relative to the test runner.
2.  **Legacy Script Wrapper:** A shell script (`wrapper_gestern.sh`) to mock the `date` command for the legacy script, allowing controlled input dates.
3.  **Migrated BigQuery Procedure:** The BigQuery script, modified into a stored procedure (`gestern_calculator_testable`), deployed in your GCP project and dataset.
4.  **Python Environment:** Python 3.x with `pytest` and `google-cloud-bigquery` installed.
5.  **GCP Credentials:** Authenticated `gcloud` or service account credentials with BigQuery access.

### `wrapper_gestern.sh` (Legacy Script Mocking)

This script will be used by the Python tests to execute the legacy `gestern.ksh` with a specific "current date".

```bash
#!/bin/bash
# wrapper_gestern.sh
# Usage: ./wrapper_gestern.sh YYYY-MM-DD
# This script mocks the 'date' command for gestern.ksh to allow controlled testing.

# Get the directory of the wrapper script
WRAPPER_DIR=$(dirname "$0")
TEMP_BIN_DIR="$WRAPPER_DIR/temp_bin"
LEGACY_SCRIPT_PATH="$WRAPPER_DIR/legacy_scripts/gestern.ksh" # Adjust path as needed

# Clean up previous temp_bin if it exists, then create it
rm -rf "$TEMP_BIN_DIR"
mkdir -p "$TEMP_BIN_DIR"
trap "rm -rf $TEMP_BIN_DIR" EXIT # Clean up on exit

# Create a mock 'date' command
# The original script uses 'date '+ %d %m %Y'' which produces a leading space.
# We replicate this behavior for strict parity testing.
MOCKED_DATE_STR=$(date -d "$1" '+ %d %m %Y')
cat <<EOF > "$TEMP_BIN_DIR/date"
#!/bin/bash
echo "$MOCKED_DATE_STR"
EOF
chmod +x "$TEMP_BIN_DIR/date"

# Prepend the temporary directory to PATH
# This ensures our mock 'date' is found before the system 'date'
PATH="$TEMP_BIN_DIR:$PATH"

# Execute the original script
exec "$LEGACY_SCRIPT_PATH"
```

### `gestern_calculator_testable` (Migrated BigQuery Stored Procedure)

This is the BigQuery code, adapted into a stored procedure to accept a test date as input.

```sql
-- BigQuery Script / Stored Procedure
-- Name: `gestern_calculator_testable`
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_dataset.gestern_calculator_testable`(
  IN p_current_date DATE,
  OUT o_today_date STRING,
  OUT o_yesterday_date STRING,
  OUT o_today_month STRING,
  OUT o_yesterday_month STRING
)
BEGIN
  DECLARE Var_Nummer_Null INT64 DEFAULT 0;
  DECLARE Var_Nummer_Heute_Tag INT64;
  DECLARE Var_Nummer_Heute_Monat INT64;
  DECLARE Var_Nummer_Heute_Jahr INT64;
  DECLARE Var_Nummer_Gestern_Tag INT64;
  DECLARE Var_Nummer_Gestern_Monat INT64;
  DECLARE Var_Nummer_Gestern_Jahr INT64;

  -- Datum ermitteln
  SET Var_Nummer_Heute_Tag = EXTRACT(DAY FROM p_current_date);
  SET Var_Nummer_Heute_Monat = EXTRACT(MONTH FROM p_current_date);
  SET Var_Nummer_Heute_Jahr = EXTRACT(YEAR FROM p_current_date);

  -- Vortag berechnen
  IF Var_Nummer_Heute_Tag > 1 THEN
    SET Var_Nummer_Gestern_Tag = Var_Nummer_Heute_Tag - 1;
    SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;
  ELSEIF Var_Nummer_Heute_Tag = 1 THEN
    IF Var_Nummer_Heute_Monat > 1 THEN
      SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat - 1;
      SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;

      CASE Var_Nummer_Gestern_Monat
        WHEN 1 THEN SET Var_Nummer_Gestern_Tag = 31;
        WHEN 2 THEN SET Var_Nummer_Gestern_Tag = 28;
        WHEN 3 THEN SET Var_Nummer_Gestern_Tag = 31;
        WHEN 5 THEN SET Var_Nummer_Gestern_Tag = 31;
        WHEN 7 THEN SET Var_Nummer_Gestern_Tag = 31;
        WHEN 8 THEN SET Var_Nummer_Gestern_Tag = 31;
        WHEN 10 THEN SET Var_Nummer_Gestern_Tag = 31;
        WHEN 12 THEN SET Var_Nummer_Gestern_Tag = 31;
        ELSE SET Var_Nummer_Gestern_Tag = 30;
      END CASE;

      -- Schaltjahr logic from original script (note: this logic is slightly flawed for years divisible by 400 but not 100, e.g., 2000)
      IF MOD(Var_Nummer_Heute_Jahr, 4) = 0
         AND MOD(Var_Nummer_Heute_Jahr, 100) > 0
         AND Var_Nummer_Gestern_Monat = 2 THEN
        SET Var_Nummer_Gestern_Tag = 29;
      END IF;

    ELSE -- Var_Nummer_Heute_Monat = 1 (January 1st)
      SET Var_Nummer_Gestern_Monat = 12;
      SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr - 1;
      SET Var_Nummer_Gestern_Tag = 31;
    END IF;
  ELSE
    -- This 'ELSE' branch corresponds to an impossible condition for Var_Nummer_Heute_Tag
    -- (not > 1 and not == 1). Original script prints "Fehler !!!!".
    -- For a stored procedure, we'll return error indicators.
    SET o_today_date = 'ERROR';
    SET o_yesterday_date = 'ERROR';
    SET o_today_month = 'ERROR';
    SET o_yesterday_month = 'ERROR';
    RETURN;
  END IF;

  -- Datum formatieren (BigQuery's LPAD ensures YYYYMMDD format without leading spaces)
  SET o_today_date = CONCAT(
    CAST(Var_Nummer_Heute_Jahr AS STRING),
    LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0'),
    LPAD(CAST(Var_Nummer_Heute_Tag AS STRING), 2, '0')
  );

  SET o_today_month = CONCAT(
    CAST(Var_Nummer_Heute_Jahr AS STRING),
    LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0')
  );

  SET o_yesterday_date = CONCAT(
    CAST(Var_Nummer_Gestern_Jahr AS STRING),
    LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0'),
    LPAD(CAST(Var_Nummer_Gestern_Tag AS STRING), 2, '0')
  );

  SET o_yesterday_month = CONCAT(
    CAST(Var_Nummer_Gestern_Jahr AS STRING),
    LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0')
  );

END;
```

---

## Migration Validation Tests

### 1. Output Parity & Transformation Correctness (Combined Test Suite)

This comprehensive test suite verifies that the BigQuery script's date calculations and formatting match the legacy script's behavior across various scenarios, including edge cases. It also highlights a critical formatting discrepancy.

**Purpose:** To prove that the migrated BigQuery code produces the exact same output values as the legacy KornShell script for a range of input dates, covering:
*   Normal day-to-day calculations.
*   Month transitions (e.g., 1st of the month).
*   Year transitions (January 1st).
*   Leap year logic (February 29th, non-leap February 28th).
*   Specific leap year edge cases (e.g., year 2000, where the original script's logic is known to be slightly flawed).
*   **Critical Note:** This test explicitly checks for the *actual* output of the legacy script, which includes a leading space in the day part of `Var_Datum_Heute` and `Var_Datum_Gestern` due to the `date '+ %d %m %Y'` format string. The BigQuery script, using `LPAD`, produces `YYYYMMDD` without this space. Therefore, this test is **expected to fail** for the provided BigQuery code, highlighting a behavioral difference that needs to be addressed (either by modifying BigQuery to replicate the space or by accepting the BigQuery's "corrected" formatting as a desired outcome of the migration).

**Setup:**
*   The `wrapper_gestern.sh` script is available and configured to point to the `gestern.ksh` legacy script.
*   The `gestern_calculator_testable` BigQuery stored procedure is deployed in `your_gcp_project.your_dataset`.
*   Python environment with `pytest` and `google-cloud-bigquery` is set up.

**Action:**
1.  For each parameterized `test_date`:
    a.  Execute the `wrapper_gestern.sh` with `test_date` to get the legacy script's output.
    b.  Call the `gestern_calculator_testable` BigQuery stored procedure with `test_date` to get the migrated script's output.
    c.  Compare the four output values (TodayDate, YesterdayDate, TodayMonth, YesterdayMonth) from both executions.

**Pass/Fail Criterion:**
*   **Pass:** The four output values from the BigQuery procedure must exactly match the corresponding four space-separated values from the legacy script's output.
*   **Fail:** If any output value differs.
*   **Expected Outcome:** This test is expected to **FAIL** due to the formatting discrepancy described in the "Purpose" section. The legacy script will output `YYYYMM DD` (e.g., `202310 26`), while the BigQuery script will output `YYYYMMDD` (e.g., `20231026`). This indicates a behavioral difference in output formatting.

**Runnable Test Code (pytest):**

```python
import subprocess
import pytest
from google.cloud import bigquery
import os

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset")
BQ_PROCEDURE_ID = "gestern_calculator_testable"
# Paths are relative to where you run pytest, assuming wrapper_gestern.sh is in the same dir
# and legacy_scripts/gestern.ksh is a subdirectory.
WRAPPER_SCRIPT_PATH = "./wrapper_gestern.sh"

# --- Helper Functions ---
def run_legacy_script(test_date_str):
    """Runs the legacy ksh script with a mocked date."""
    try:
        # Ensure the wrapper script is executable
        subprocess.run(["chmod", "+x", WRAPPER_SCRIPT_PATH], check=True, capture_output=True)
        result = subprocess.run(
            [WRAPPER_SCRIPT_PATH, test_date_str],
            capture_output=True, text=True, check=True,
            # Ensure the temporary bin directory created by the wrapper is in PATH
            env={**os.environ, "PATH": f"{os.path.dirname(WRAPPER_SCRIPT_PATH)}/temp_bin:{os.environ['PATH']}"}
        )
        return result.stdout.strip().split()
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Legacy script failed for date {test_date_str}: {e.stderr}")
    except FileNotFoundError:
        pytest.fail(f"Legacy script wrapper not found at {WRAPPER_SCRIPT_PATH}")

def run_bq_procedure(test_date_str):
    """Calls the BigQuery stored procedure with the given date."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{BQ_PROCEDURE_ID}`(
      DATE('{test_date_str}'),
      o_today_date => @today_date,
      o_yesterday_date => @yesterday_date,
      o_today_month => @today_month,
      o_yesterday_month => @yesterday_month
    );
    SELECT @today_date AS TodayDate, @yesterday_date AS YesterdayDate,
           @today_month AS TodayMonth, @yesterday_month AS YesterdayMonth;
    """
    try:
        job = client.query(query)
        result = job.result()
        for row in result:
            return [row.TodayDate, row.YesterdayDate, row.TodayMonth, row.YesterdayMonth]
        pytest.fail(f"BigQuery procedure returned no results for date {test_date_str}")
    except Exception as e:
        pytest.fail(f"BigQuery procedure failed for date {test_date_str}: {e}")

# --- Test Cases ---
@pytest.mark.parametrize("test_date, expected_legacy_output", [
    # Normal day (day > 1)
    ("2023-10-26", ["202310 26", "202310 25", "202310", "202310"]),
    ("2023-11-15", ["202311 15", "202311 14", "202311", "202311"]),

    # Start of month (day == 1, month > 1)
    ("2023-05-01", ["202305 01", "20230430", "202305", "202304"]), # April has 30 days
    ("2023-07-01", ["202307 01", "20230630", "202307", "202306"]), # June has 30 days
    ("2023-03-01", ["202303 01", "20230228", "202303", "202302"]), # Non-leap year Feb has 28 days

    # Start of year (day == 1, month == 1)
    ("2023-01-01", ["202301 01", "20221231", "202301", "202212"]),

    # Leap Year Scenarios (based on original script's logic: year % 4 == 0 AND year % 100 > 0)
    ("2024-03-01", ["202403 01", "20240229", "202403", "202402"]), # 2024 is a leap year (2024%4=0, 2024%100=24>0)
    ("2004-03-01", ["200403 01", "20040229", "200403", "200402"]), # 2004 is a leap year (2004%4=0, 2004%100=4>0)

    # Non-Leap Year Scenarios
    ("2023-03-01", ["202303 01", "20230228", "202303", "202302"]), # 2023 is not a leap year
    ("1900-03-01", ["190003 01", "19000228", "190003", "190002"]), # 1900 is not a leap year (1900%4=0, 1900%100=0, so original logic says NOT leap)

    # Leap Year Edge Case: Year 2000 (divisible by 400)
    # Original script's logic: (year % 4 == 0 AND year % 100 > 0)
    # For 2000: (2000 % 4 == 0 AND 2000 % 100 > 0) -> (True AND False) -> False.
    # So, the original script incorrectly treats 2000 as a non-leap year.
    # The migrated code must replicate this specific (flawed) behavior for parity.
    ("2000-03-01", ["200003 01", "20000228", "200003", "200002"]), # Expected to be 20000228, replicating legacy bug
])
def test_date_calculation_and_formatting_parity(test_date, expected_legacy_output):
    """
    Purpose: Verify output parity for various date scenarios, covering normal days,
             month/year transitions, and leap year logic (including its specific implementation).
             This test explicitly checks for the exact string output, including the
             leading space in the day part of the legacy script's output.
    Setup: Legacy script and BQ procedure are accessible and configured.
    Action: Run both with the parameterized test_date.
    Pass/Fail: Outputs must be identical in value, format, and type.
               NOTE: This test is expected to FAIL for the provided BigQuery code
               due to a formatting discrepancy (BigQuery uses LPAD to remove the
               leading space, while the legacy script's 'date' command format
               string introduces it). This highlights a behavioral difference
               that needs to be addressed (either by modifying BQ or accepting
               the fix).
    """
    print(f"\n--- Testing date: {test_date} ---")
    legacy_output = run_legacy_script(test_date)
    bq_output = run_bq_procedure(test_date)

    print(f"Legacy Output: {legacy_output}")
    print(f"BigQuery Output: {bq_output}")
    print(f"Expected Legacy Output: {expected_legacy_output}")

    assert legacy_output == expected_legacy_output, \
        f"Legacy output mismatch for {test_date}: Expected {expected_legacy_output}, Got {legacy_output}"
    assert bq_output == expected_legacy_output, \
        f"BigQuery output mismatch for {test_date}: Expected {expected_legacy_output}, Got {bq_output}"
    assert legacy_output == bq_output, \
        f"Output parity failed for {test_date}: Legacy {legacy_output}, BQ {bq_output}"

```

### 2. External System Replacements

**Purpose:** To verify that the BigQuery script correctly replaces the functionality of external commands used by the legacy KornShell script (`date` and `expr`) with native BigQuery functions and operators, without introducing new external dependencies.

**Setup:**
*   The `gestern_calculator_testable` BigQuery stored procedure is deployed.
*   The BigQuery code (`gestern_bq.sql`) is available for review.

**Action:**
1.  Review the BigQuery stored procedure code (`gestern_bq.sql`).
2.  Confirm that `CURRENT_DATE()` and `EXTRACT()` are used to get current date components, replacing the `date` command.
3.  Confirm that arithmetic operators (`-`, `+`) and `MOD()` function are used for calculations, replacing the `expr` command.
4.  Confirm that no other external system calls (e.g., UDFs that call external APIs, external tables, SFTP/S3 operations) are present.

**Pass/Fail Criterion:**
*   **Pass:** The BigQuery code must exclusively use BigQuery SQL functions and operators for date extraction and arithmetic. No direct or indirect calls to external systems (like `date` or `expr` via a UDF that executes shell commands) should be present. The successful execution of the `test_date_calculation_and_formatting_parity` test suite also implicitly confirms the functional replacement.
*   **Fail:** If the BigQuery code relies on any external system calls or non-native BigQuery mechanisms to replicate the functionality of `date` or `expr`.

### 3. Data Quality / Schema Assertions (for BigQuery Output)

**Purpose:** To verify that the BigQuery script's output adheres to the expected data types, lengths, and format (YYYYMMDD/YYYYMM), assuming the *intended* clean format without the legacy script's leading space bug.

**Setup:**
*   The `gestern_calculator_testable` BigQuery stored procedure is deployed.
*   A sample test date, e.g., `2023-11-15`.

**Action:**
1.  Call the BigQuery stored procedure `gestern_calculator_testable` with `p_current_date = '2023-11-15'`.
2.  Inspect the data types and format of the returned output parameters.

**Pass/Fail Criterion:**
*   **Pass:**
    *   All four output parameters (`o_today_date`, `o_yesterday_date`, `o_today_month`, `o_yesterday_month`) must be of `STRING` type.
    *   `o_today_date` and `o_yesterday_date` must be exactly 8 characters long and contain only digits (matching `YYYYMMDD` format).
    *   `o_today_month` and `o_yesterday_month` must be exactly 6 characters long and contain only digits (matching `YYYYMM` format).
*   **Fail:** If any output parameter deviates from the specified type, length, or format.

**Runnable Test Code (pytest):**

```python
import pytest
from google.cloud import bigquery
import os

# --- Configuration (same as above) ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_dataset")
BQ_PROCEDURE_ID = "gestern_calculator_testable"

# --- Helper Function (same as above) ---
def run_bq_procedure(test_date_str):
    """Calls the BigQuery stored procedure with the given date."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.{BQ_PROCEDURE_ID}`(
      DATE('{test_date_str}'),
      o_today_date => @today_date,
      o_yesterday_date => @yesterday_date,
      o_today_month => @today_month,
      o_yesterday_month => @yesterday_month
    );
    SELECT @today_date AS TodayDate, @yesterday_date AS YesterdayDate,
           @today_month AS TodayMonth, @yesterday_month AS YesterdayMonth;
    """
    try:
        job = client.query(query)
        result = job.result()
        for row in result:
            return [row.TodayDate, row.YesterdayDate, row.TodayMonth, row.YesterdayMonth]
        pytest.fail(f"BigQuery procedure returned no results for date {test_date_str}")
    except Exception as e:
        pytest.fail(f"BigQuery procedure failed for date {test_date_str}: {e}")

# --- Test Case ---
@pytest.mark.parametrize("test_date", [
    "2023-11-15",
    "2023-01-01",
    "2024-03-01", # Leap year
    "2023-03-01", # Non-leap year
])
def test_bq_output_data_quality_and_schema(test_date):
    """
    Purpose: Verify that the BigQuery script's output adheres to the expected
             data types, lengths, and YYYYMMDD/YYYYMM format (without internal spaces).
    Setup: BQ procedure is accessible and configured.
    Action: Call the BQ procedure with various test dates.
    Pass/Fail: Output values must be strings, match expected lengths, and contain only digits.
    """
    print(f"\n--- Testing BQ data quality for date: {test_date} ---")
    bq_output = run_bq_procedure(test_date)

    assert len(bq_output) == 4, f"Expected 4 output values, got {len(bq_output)}"

    today_date, yesterday_date, today_month, yesterday_month = bq_output

    # Check types
    assert isinstance(today_date, str), f"TodayDate is not a string: {type(today_date)}"
    assert isinstance(yesterday_date, str), f"YesterdayDate is not a string: {type(yesterday_date)}"
    assert isinstance(today_month, str), f"TodayMonth is not a string: {type(today_month)}"
    assert isinstance(yesterday_month, str), f"YesterdayMonth is not a string: {type(yesterday_month)}"

    # Check lengths and format (digits only)
    assert len(today_date) == 8, f"TodayDate length mismatch: {today_date}"
    assert today_date.isdigit(), f"TodayDate contains non-digit characters: {today_date}"

    assert len(yesterday_date) == 8, f"YesterdayDate length mismatch: {yesterday_date}"
    assert yesterday_date.isdigit(), f"YesterdayDate contains non-digit characters: {yesterday_date}"

    assert len(today_month) == 6, f"TodayMonth length mismatch: {today_month}"
    assert today_month.isdigit(), f"TodayMonth contains non-digit characters: {today_month}"

    assert len(yesterday_month) == 6, f"YesterdayMonth length mismatch: {yesterday_month}"
    assert yesterday_month.isdigit(), f"YesterdayMonth contains non-digit characters: {yesterday_month}"

    print(f"BigQuery output for {test_date} passed data quality checks.")
```