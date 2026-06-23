This document outlines migration validation tests for the `gestern.ksh` KornShell script, which is being migrated to a Google BigQuery SQL script. The tests are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality.

## Testing Strategy for Date Control

The core challenge for testing this utility script is its dependency on the "current date". To achieve deterministic and repeatable testing, the "current date" will be controlled for both the legacy KSH script and the BigQuery SQL script.

*   **Legacy KSH Script:** A mock `date` command will be used. A temporary directory containing an executable `date` script will be created. This mock `date` script will accept a `YYYY-MM-DD` argument and output the date in the format `'+ %d %m %Y'` as expected by `gestern.ksh`. The `PATH` environment variable will be temporarily modified to prioritize this mock `date` command.
*   **BigQuery SQL Script:** The `DECLARE today_date DATE DEFAULT CURRENT_DATE();` line in `bq_gestern.sql` will be dynamically replaced with `DECLARE today_date DATE DEFAULT DATE 'YYYY-MM-DD';` for each test case to fix the "current date".

## Helper Functions (Python/Pytest)

The following Python helper functions will be used to execute and compare the outputs of the legacy KSH script and the migrated BigQuery SQL script.

```python
import subprocess
import os
import tempfile
from datetime import date, timedelta
from google.cloud import bigquery
import pytest

# --- Configuration ---
# Path to the original legacy KornShell script
LEGACY_KSH_PATH = "vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh"
# Path to the migrated BigQuery SQL script
BIGQUERY_SQL_PATH = "bq_gestern.sql"
# BigQuery project ID for running tests
BIGQUERY_PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "your-gcp-project-id")

# --- Helper Functions ---

def run_legacy_ksh(test_date_str: str, timezone: str = "UTC") -> dict:
    """
    Runs the legacy KSH script with a mocked 'date' command for a specific test date.
    Returns a dictionary of the parsed output.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        mock_date_script_path = os.path.join(tmpdir, "date")
        with open(mock_date_script_path, "w") as f:
            # The original script uses `date '+ %d %m %Y'` which has a leading space.
            # We need to replicate that.
            # The TZ variable ensures the 'date -d' command interprets the date string
            # and formats it according to the specified timezone, then outputs it.
            f.write(f"""#!/bin/bash
            # Mock date command for testing
            TZ='{timezone}' date -d "{test_date_str}" '+ %d %m %Y'
            """)
        os.chmod(mock_date_script_path, 0o755)

        env = os.environ.copy()
        env["PATH"] = f"{tmpdir}:{env['PATH']}"
        # Set TZ for the ksh script itself, though the mock date command is primary.
        env["TZ"] = timezone

        try:
            result = subprocess.run(
                ["ksh", LEGACY_KSH_PATH],
                capture_output=True,
                text=True,
                check=True,
                env=env
            )
            output_parts = result.stdout.strip().split()
            if len(output_parts) != 4:
                raise ValueError(f"Unexpected output format from KSH: {result.stdout}")
            return {
                "today_date_yyyymmdd": output_parts[0],
                "yesterday_date_yyyymmdd": output_parts[1],
                "today_month_yyyymm": output_parts[2],
                "yesterday_month_yyyymm": output_parts[3],
            }
        except subprocess.CalledProcessError as e:
            print(f"KSH script failed: {e.stderr}")
            raise
        except ValueError as e:
            print(f"Error parsing KSH output: {e}")
            raise

def run_bigquery_sql(test_date_str: str, bq_client: bigquery.Client) -> dict:
    """
    Runs the BigQuery SQL script with a specific test date injected.
    Returns a dictionary of the parsed output.
    """
    with open(BIGQUERY_SQL_PATH, "r") as f:
        sql_template = f.read()

    # Inject the test date into the SQL
    # This replaces the CURRENT_DATE() call with a fixed date for testing.
    modified_sql = sql_template.replace(
        "DECLARE today_date DATE DEFAULT CURRENT_DATE();",
        f"DECLARE today_date DATE DEFAULT DATE '{test_date_str}';"
    )

    # Execute the BigQuery query
    query_job = bq_client.query(modified_sql, project=BIGQUERY_PROJECT_ID)
    results = query_job.result()

    rows = list(results)
    if not rows:
        raise ValueError("BigQuery query returned no results.")
    if len(rows) > 1:
        raise ValueError("BigQuery query returned more than one row.")

    row = rows[0]
    return {
        "today_date_yyyymmdd": row.today_date_yyyymmdd,
        "yesterday_date_yyyymmdd": row.yesterday_date_yyyymmdd,
        "today_month_yyyymm": row.today_month_yyyymm,
        "yesterday_month_yyyymm": row.yesterday_month_yyyymm,
    }

# Pytest fixture for BigQuery client
@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client instance for tests."""
    return bigquery.Client(project=BIGQUERY_PROJECT_ID)

```

---

## Test Case 1: Output Parity - Standard Day

*   **Purpose:** Verify that the BigQuery script correctly calculates and formats dates for a typical day, ensuring basic functional equivalence. This covers general transformation correctness and output parity.
*   **Setup:**
    *   Choose a `test_date` in the middle of a month, not near year/month boundaries or leap years. Example: `2023-10-26`.
    *   Ensure the legacy `gestern.ksh` script and the `bq_gestern.sql` script are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking the `date` command to return `2023-10-26` (assuming UTC or a timezone where this date is consistent). Capture its standard output.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2023-10-26'` as `today_date`. Capture its result.
*   **Pass/Fail Criterion:** The four output values (today's date YYYYMMDD, yesterday's date YYYYMMDD, today's month YYYYMM, yesterday's month YYYYMM) from the BigQuery script must exactly match those from the legacy KSH script.

```python
def test_output_parity_standard_day(bq_client):
    test_date_str = "2023-10-26"

    legacy_output = run_legacy_ksh(test_date_str)
    bq_output = run_bigquery_sql(test_date_str, bq_client)

    assert legacy_output == {
        "today_date_yyyymmdd": "20231026",
        "yesterday_date_yyyymmdd": "20231025",
        "today_month_yyyymm": "202310",
        "yesterday_month_yyyymm": "202310",
    }
    assert bq_output == legacy_output
```

## Test Case 2: Output Parity - Month Transition (Start of Month)

*   **Purpose:** Verify correct handling of month transitions when `today_date` is the first day of a month. This tests the KSH script's `if (( $Var_Nummer_Heute_Tag == 1 ))` logic and BigQuery's `DATE_SUB` function's inherent month transition handling. This covers transformation correctness and output parity.
*   **Setup:**
    *   Choose a `test_date` that is the 1st of a month, but not January 1st. Example: `2023-03-01`.
    *   Ensure scripts are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking `date` to return `2023-03-01`. Capture output.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2023-03-01'` as `today_date`. Capture result.
*   **Pass/Fail Criterion:** The four output values from the BigQuery script must exactly match those from the legacy KSH script.

```python
def test_output_parity_month_start(bq_client):
    test_date_str = "2023-03-01" # Yesterday should be 2023-02-28 (non-leap year)

    legacy_output = run_legacy_ksh(test_date_str)
    bq_output = run_bigquery_sql(test_date_str, bq_client)

    assert legacy_output == {
        "today_date_yyyymmdd": "20230301",
        "yesterday_date_yyyymmdd": "20230228",
        "today_month_yyyymm": "202303",
        "yesterday_month_yyyymm": "202302",
    }
    assert bq_output == legacy_output
```

## Test Case 3: Output Parity - Year Transition (Start of Year)

*   **Purpose:** Verify correct handling of year transitions when `today_date` is January 1st. This tests the KSH script's `if (( $Var_Nummer_Heute_Monat > 1 ))` else branch and BigQuery's `DATE_SUB` function across year boundaries. This covers transformation correctness and output parity.
*   **Setup:**
    *   Choose a `test_date` that is January 1st. Example: `2024-01-01`.
    *   Ensure scripts are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking `date` to return `2024-01-01`. Capture output.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2024-01-01'` as `today_date`. Capture result.
*   **Pass/Fail Criterion:** The four output values from the BigQuery script must exactly match those from the legacy KSH script.

```python
def test_output_parity_year_start(bq_client):
    test_date_str = "2024-01-01" # Yesterday should be 2023-12-31

    legacy_output = run_legacy_ksh(test_date_str)
    bq_output = run_bigquery_sql(test_date_str, bq_client)

    assert legacy_output == {
        "today_date_yyyymmdd": "20240101",
        "yesterday_date_yyyymmdd": "20231231",
        "today_month_yyyymm": "202401",
        "yesterday_month_yyyymm": "202312",
    }
    assert bq_output == legacy_output
```

## Test Case 4: Output Parity - Leap Year Handling (March 1st in Leap Year)

*   **Purpose:** Verify correct handling of leap years, specifically when `today_date` is March 1st in a leap year, meaning `yesterday_date` should be February 29th. This tests the KSH script's explicit leap year detection logic and BigQuery's `DATE_SUB` function's inherent leap year awareness. This covers transformation correctness and output parity for an edge case.
*   **Setup:**
    *   Choose a `test_date` that is March 1st of a leap year. Example: `2024-03-01`.
    *   Ensure scripts are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking `date` to return `2024-03-01`. Capture output.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2024-03-01'` as `today_date`. Capture result.
*   **Pass/Fail Criterion:** The four output values from the BigQuery script must exactly match those from the legacy KSH script.

```python
def test_output_parity_leap_year_march_1st(bq_client):
    test_date_str = "2024-03-01" # 2024 is a leap year, yesterday should be 2024-02-29

    legacy_output = run_legacy_ksh(test_date_str)
    bq_output = run_bigquery_sql(test_date_str, bq_client)

    assert legacy_output == {
        "today_date_yyyymmdd": "20240301",
        "yesterday_date_yyyymmdd": "20240229",
        "today_month_yyyymm": "202403",
        "yesterday_month_yyyymm": "202402",
    }
    assert bq_output == legacy_output
```

## Test Case 5: Output Parity - Non-Leap Year Handling (March 1st in Non-Leap Year)

*   **Purpose:** Verify correct handling of non-leap years, specifically when `today_date` is March 1st in a non-leap year, meaning `yesterday_date` should be February 28th. This tests the KSH script's logic for non-leap years and BigQuery's `DATE_SUB` function. This covers transformation correctness and output parity for an edge case.
*   **Setup:**
    *   Choose a `test_date` that is March 1st of a non-leap year. Example: `2023-03-01`.
    *   Ensure scripts are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking `date` to return `2023-03-01`. Capture output.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2023-03-01'` as `today_date`. Capture result.
*   **Pass/Fail Criterion:** The four output values from the BigQuery script must exactly match those from the legacy KSH script.

```python
def test_output_parity_non_leap_year_march_1st(bq_client):
    test_date_str = "2023-03-01" # 2023 is a non-leap year, yesterday should be 2023-02-28

    legacy_output = run_legacy_ksh(test_date_str)
    bq_output = run_bigquery_sql(test_date_str, bq_client)

    assert legacy_output == {
        "today_date_yyyymmdd": "20230301",
        "yesterday_date_yyyymmdd": "20230228",
        "today_month_yyyymm": "202303",
        "yesterday_month_yyyymm": "202302",
    }
    assert bq_output == legacy_output
```

## Test Case 6: Output Parity - End of Month (31-day month)

*   **Purpose:** Verify correct handling when `today_date` is the last day of a 31-day month. This covers transformation correctness and output parity.
*   **Setup:**
    *   Choose a `test_date` that is the 31st of a 31-day month. Example: `2023-10-31`.
    *   Ensure scripts are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking `date` to return `2023-10-31`. Capture output.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2023-10-31'` as `today_date`. Capture result.
*   **Pass/Fail Criterion:** The four output values from the BigQuery script must exactly match those from the legacy KSH script.

```python
def test_output_parity_end_of_31_day_month(bq_client):
    test_date_str = "2023-10-31" # Yesterday should be 2023-10-30

    legacy_output = run_legacy_ksh(test_date_str)
    bq_output = run_bigquery_sql(test_date_str, bq_client)

    assert legacy_output == {
        "today_date_yyyymmdd": "20231031",
        "yesterday_date_yyyymmdd": "20231030",
        "today_month_yyyymm": "202310",
        "yesterday_month_yyyymm": "202310",
    }
    assert bq_output == legacy_output
```

## Test Case 7: Output Parity - End of Month (30-day month)

*   **Purpose:** Verify correct handling when `today_date` is the last day of a 30-day month. This covers transformation correctness and output parity.
*   **Setup:**
    *   Choose a `test_date` that is the 30th of a 30-day month. Example: `2023-09-30`.
    *   Ensure scripts are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking `date` to return `2023-09-30`. Capture output.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2023-09-30'` as `today_date`. Capture result.
*   **Pass/Fail Criterion:** The four output values from the BigQuery script must exactly match those from the legacy KSH script.

```python
def test_output_parity_end_of_30_day_month(bq_client):
    test_date_str = "2023-09-30" # Yesterday should be 2023-09-29

    legacy_output = run_legacy_ksh(test_date_str)
    bq_output = run_bigquery_sql(test_date_str, bq_client)

    assert legacy_output == {
        "today_date_yyyymmdd": "20230930",
        "yesterday_date_yyyymmdd": "20230929",
        "today_month_yyyymm": "202309",
        "yesterday_month_yyyymm": "202309",
    }
    assert bq_output == legacy_output
```

## Test Case 8: Timezone Discrepancy Check (Critical Risk)

*   **Purpose:** Highlight the potential discrepancy if the legacy KSH script runs in a specific non-UTC timezone (e.g., `Europe/Berlin`) and the BigQuery script uses `CURRENT_DATE()` (which defaults to UTC). This test verifies the *behavior* of the migration design's current implementation against a known risk identified in the design document.
*   **Setup:**
    *   Choose a `test_date` and `test_time` such that the date changes across the UTC boundary when viewed from a specific timezone. Example: `2023-10-26 01:00:00` in `Europe/Berlin` (UTC+2 due to DST) is `2023-10-25 23:00:00` UTC.
    *   Ensure scripts are accessible.
*   **Action:**
    1.  Execute the legacy `gestern.ksh` script, mocking the `date` command to return `2023-10-26` *as if it were run in `Europe/Berlin` at 01:00:00*. The mock `date` command will be configured with `TZ='Europe/Berlin'` and `date -d "2023-10-26"`.
    2.  Execute the `bq_gestern.sql` script, injecting `DATE '2023-10-25'` as `today_date`. This `today_date` represents the *actual UTC date* that corresponds to the KSH script's execution time in the specified timezone.
*   **Pass/Fail Criterion:**
    *   **Pass:** The test correctly asserts that the outputs *do not match* when the timezone difference causes a date shift. This confirms the understanding of the risk and demonstrates the current BigQuery implementation's behavior.
    *   **Fail:** The outputs *do match*, indicating either the timezone assumption was wrong, or the BigQuery `CURRENT_DATE()` behavior was misunderstood, or the mock setup is incorrect.
    *   **Note:** For the BigQuery script to be truly equivalent in this scenario, `CURRENT_DATE('Europe/Berlin')` would be required in the BigQuery script. This test demonstrates why.

```python
def test_timezone_discrepancy_check(bq_client):
    # Scenario: KSH script runs in Europe/Berlin at 01:00 AM on 2023-10-26
    # Europe/Berlin is UTC+2 at this time (CEST).
    # So, 2023-10-26 01:00:00 CEST is 2023-10-25 23:00:00 UTC.

    # KSH script's perspective (local date in Berlin)
    ksh_local_date_str = "2023-10-26"
    ksh_timezone = "Europe/Berlin"

    # BigQuery's perspective (UTC date corresponding to the KSH run time)
    bq_utc_date_str = "2023-10-25"

    # Run legacy KSH, mocking its 'date' command to output based on ksh_local_date_str
    legacy_output = run_legacy_ksh(ksh_local_date_str, timezone=ksh_timezone)

    # Run BigQuery SQL, injecting the *actual UTC date*
    bq_output = run_bigquery_sql(bq_utc_date_str, bq_client)

    # Assert KSH output is as expected for its local date
    assert legacy_output == {
        "today_date_yyyymmdd": "20231026",
        "yesterday_date_yyyymmdd": "20231025",
        "today_month_yyyymm": "202310",
        "yesterday_month_yyyymm": "202310",
    }

    # Assert BQ output is as expected for its UTC date
    assert bq_output == {
        "today_date_yyyymmdd": "20231025",
        "yesterday_date_yyyymmdd": "20231024",
        "today_month_yyyymm": "202310",
        "yesterday_month_yyyymm": "202310",
    }

    # The critical assertion: outputs should NOT match due to timezone difference
    assert bq_output != legacy_output, \
        "Timezone discrepancy not detected! BigQuery output unexpectedly matched KSH output."

    # This test passes if it correctly identifies the mismatch, highlighting the risk.
    # A "fix" would involve modifying the BQ script to use CURRENT_DATE('Europe/Berlin').
```