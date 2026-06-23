As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migrated `k_ausd_bp_ta_cntrct_dist.ksh` job. These tests aim to ensure the BigQuery Stored Procedure `r_ausd_bp_ta_cntrct_dist` and its associated components are functionally equivalent to the legacy KornShell script.

The tests are organized into sections covering output parity, transformation correctness, external system replacements, and data quality assertions. Each test case includes its purpose, setup, action, and concrete pass/fail criteria, with runnable code examples where applicable.

---

## Migration Validation Tests: `k_ausd_bp_ta_cntrct_dist.ksh` to `r_ausd_bp_ta_cntrct_dist`

**Assumptions for Testing:**
*   A Google Cloud Project and BigQuery Dataset are available.
*   The DDLs for `sof_ta_bpr_basis`, `sof_ta_cntrct_dist`, and `job_log_table` (if used) have been executed.
*   The BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_cntrct_dist` has been deployed.
*   The `invoke_r_ausd_bp_ta_cntrct_dist.py` script is available and executable.
*   `google-cloud-bigquery` Python library is installed.
*   Authentication to GCP is configured (e.g., via `gcloud auth application-default login`).

**Test Environment Setup (Python/Pytest Helpers):**

```python
# conftest.py or a common test_utils.py
import pytest
from google.cloud import bigquery
import subprocess
import os
from datetime import datetime

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_bigquery_dataset")
SP_FULL_PATH = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_cntrct_dist"
SOURCE_TABLE_FULL_PATH = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_basis"
TARGET_TABLE_FULL_PATH = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_dist"
JOB_LOG_TABLE_FULL_PATH = f"{PROJECT_ID}.{DATASET_ID}.job_log_table" # Only if job logging is active

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    return bigquery.Client(project=PROJECT_ID)

def execute_bq_query(client: bigquery.Client, query: str):
    """Executes a BigQuery SQL query."""
    query_job = client.query(query)
    return query_job.result()

def call_stored_procedure(
    job_kennung: str,
    eintrags_nr: str,
    stichtag: str, # DDMMYYYY
    wiederanlauf_wert: str,
    expect_success: bool = True
):
    """
    Invokes the BigQuery Stored Procedure via the Python orchestrator script.
    Returns the subprocess result.
    """
    script_path = "invoke_r_ausd_bp_ta_cntrct_dist.py" # Adjust path if needed
    command = [
        "python", script_path,
        "--project_id", PROJECT_ID,
        "--dataset_id", DATASET_ID,
        "--job_kennung", job_kennung,
        "--eintrags_nr", eintrags_nr,
        "--stichtag", stichtag,
        "--wiederanlauf_wert", wiederanlauf_wert
    ]
    print(f"\nExecuting command: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True, check=False) # check=False to capture errors
    if expect_success and result.returncode != 0:
        pytest.fail(f"Stored procedure call failed unexpectedly:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
    elif not expect_success and result.returncode == 0:
        pytest.fail(f"Stored procedure call succeeded unexpectedly:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
    return result

def setup_source_table(client: bigquery.Client, data: list[dict]):
    """Clears and populates the source table with test data."""
    execute_bq_query(client, f"TRUNCATE TABLE {SOURCE_TABLE_FULL_PATH};")
    if data:
        rows_to_insert = [bigquery.Row(row) for row in data]
        errors = client.insert_rows_json(SOURCE_TABLE_FULL_PATH, rows_to_insert)
        if errors:
            pytest.fail(f"Failed to insert rows into source table: {errors}")

def get_target_table_data(client: bigquery.Client):
    """Fetches all data from the target table."""
    query = f"SELECT CNTRCT_ID FROM {TARGET_TABLE_FULL_PATH} ORDER BY CNTRCT_ID;"
    rows = execute_bq_query(client, query)
    return [row.CNTRCT_ID for row in rows]

def get_target_table_row_count(client: bigquery.Client):
    """Fetches the row count from the target table."""
    query = f"SELECT COUNT(*) FROM {TARGET_TABLE_FULL_PATH};"
    rows = execute_bq_query(client, query)
    return next(iter(rows)).f0_

def get_job_log_entry(client: bigquery.Client, job_kennung: str, eintrags_nr: str):
    """Fetches the latest job log entry for a given job_kennung and eintrags_nr."""
    query = f"""
    SELECT status, message, processed_records
    FROM {JOB_LOG_TABLE_FULL_PATH}
    WHERE job_kennung = '{job_kennung}' AND entry_nr = '{eintrags_nr}'
    ORDER BY created_at DESC
    LIMIT 1;
    """
    rows = execute_bq_query(client, query)
    return next(iter(rows), None)

# Initial DDL setup (run once before tests)
# Ensure these DDLs are applied in your BigQuery environment
# CREATE OR REPLACE TABLE `project.dataset.sof_ta_bpr_basis` (CNTRCT_ID STRING NOT NULL);
# CREATE OR REPLACE TABLE `project.dataset.sof_ta_cntrct_dist` (CNTRCT_ID STRING NOT NULL);
# CREATE OR REPLACE TABLE `project.dataset.job_log_table` (job_kennung STRING NOT NULL, entry_nr STRING, stichtag_date DATE, status STRING, message STRING, processed_records INT64, created_at TIMESTAMP);
```

---

### Test Case 1: Successful Execution - Output Parity & Row Count

*   **Purpose:** Verify the migrated job executes successfully with valid inputs, produces the expected output in the target table, and reports the correct record count. This covers basic output parity and row count assertion.
*   **Setup:**
    1.  Ensure `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_cntrct_dist` tables exist.
    2.  Populate `project.dataset.sof_ta_bpr_basis` with sample data, including duplicates.
    3.  Clear `project.dataset.sof_ta_cntrct_dist`.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with valid parameters.
    ```python
    # In your pytest file (e.g., test_migration.py)
    def test_successful_execution(bq_client):
        # Setup source data
        source_data = [
            {"CNTRCT_ID": "C1001"},
            {"CNTRCT_ID": "C1002"},
            {"CNTRCT_ID": "C1001"}, # Duplicate
            {"CNTRCT_ID": "C1003"},
            {"CNTRCT_ID": "C1004"},
            {"CNTRCT_ID": "C1002"}, # Duplicate
        ]
        setup_source_table(bq_client, source_data)

        job_kennung = "TEST_JOB_SUCCESS"
        eintrags_nr = "12345"
        stichtag = datetime.now().strftime('%d%m%Y')
        wiederanlauf_wert = "0"

        # Action: Call the SP
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a success code (0).
    2.  The target table `project.dataset.sof_ta_cntrct_dist` contains only the distinct `CNTRCT_ID`s from the source.
    3.  The number of records in the target table matches the distinct count from the source.
    4.  The output message from the SP (captured in `result.stdout`) indicates successful completion and the correct record count.
    5.  (Optional, if job logging is active): A successful entry is recorded in `job_log_table` with the correct `processed_records` count.
    ```python
        # Assertions
        assert result.returncode == 0
        assert "Stored Procedure" in result.stdout and "executed successfully" in result.stdout

        expected_distinct_ids = sorted(list(set([d["CNTRCT_ID"] for d in source_data])))
        actual_target_ids = get_target_table_data(bq_client)
        assert actual_target_ids == expected_distinct_ids

        expected_count = len(expected_distinct_ids)
        actual_count = get_target_table_row_count(bq_client)
        assert actual_count == expected_count
        assert f"Processed records: {expected_count}" in result.stdout

        # Optional: Check job log table
        # log_entry = get_job_log_entry(bq_client, job_kennung, eintrags_nr)
        # assert log_entry is not None
        # assert log_entry.status == 'SUCCESS'
        # assert log_entry.processed_records == expected_count
    ```

---

### Test Case 2: Transformation Correctness - `TRUNCATE` Behavior

*   **Purpose:** Verify that the target table is correctly truncated before new data is inserted, preventing accumulation of old data.
*   **Setup:**
    1.  Ensure `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_cntrct_dist` tables exist.
    2.  Populate `project.dataset.sof_ta_cntrct_dist` with some initial data.
    3.  Populate `project.dataset.sof_ta_bpr_basis` with new, distinct data.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with valid parameters.
    ```python
    def test_truncate_behavior(bq_client):
        # Setup target table with existing data
        initial_target_data = [
            {"CNTRCT_ID": "OLD_C1"},
            {"CNTRCT_ID": "OLD_C2"},
        ]
        bq_client.insert_rows_json(TARGET_TABLE_FULL_PATH, [bigquery.Row(row) for row in initial_target_data])
        assert get_target_table_row_count(bq_client) == 2

        # Setup source table with new data
        source_data = [
            {"CNTRCT_ID": "NEW_C1"},
            {"CNTRCT_ID": "NEW_C2"},
        ]
        setup_source_table(bq_client, source_data)

        job_kennung = "TEST_TRUNCATE"
        eintrags_nr = "12346"
        stichtag = datetime.now().strftime('%d%m%Y')
        wiederanlauf_wert = "0"

        # Action: Call the SP
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a success code (0).
    2.  The target table `project.dataset.sof_ta_cntrct_dist` contains *only* the data from the latest run (i.e., `NEW_C1`, `NEW_C2`), and none of the `OLD_C` data.
    3.  The row count matches the distinct count of the *new* source data.
    ```python
        # Assertions
        assert result.returncode == 0

        expected_distinct_ids = sorted(list(set([d["CNTRCT_ID"] for d in source_data])))
        actual_target_ids = get_target_table_data(bq_client)
        assert actual_target_ids == expected_distinct_ids
        assert get_target_table_row_count(bq_client) == len(expected_distinct_ids)
    ```

---

### Test Case 3: Transformation Correctness - `DISTINCT` Logic

*   **Purpose:** Verify that the `DISTINCT` clause correctly removes duplicate `CNTRCT_ID` values from the source table before inserting into the target.
*   **Setup:**
    1.  Ensure `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_cntrct_dist` tables exist.
    2.  Populate `project.dataset.sof_ta_bpr_basis` with data containing multiple duplicates.
    3.  Clear `project.dataset.sof_ta_cntrct_dist`.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with valid parameters.
    ```python
    def test_distinct_logic(bq_client):
        source_data = [
            {"CNTRCT_ID": "A1"},
            {"CNTRCT_ID": "B2"},
            {"CNTRCT_ID": "A1"},
            {"CNTRCT_ID": "C3"},
            {"CNTRCT_ID": "B2"},
            {"CNTRCT_ID": "A1"},
            {"CNTRCT_ID": "D4"},
        ]
        setup_source_table(bq_client, source_data)

        job_kennung = "TEST_DISTINCT"
        eintrags_nr = "12347"
        stichtag = datetime.now().strftime('%d%m%Y')
        wiederanlauf_wert = "0"

        # Action: Call the SP
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a success code (0).
    2.  The target table `project.dataset.sof_ta_cntrct_dist` contains each unique `CNTRCT_ID` from the source exactly once.
    3.  The row count matches the number of unique `CNTRCT_ID`s in the source.
    ```python
        # Assertions
        assert result.returncode == 0

        expected_distinct_ids = sorted(list(set([d["CNTRCT_ID"] for d in source_data])))
        actual_target_ids = get_target_table_data(bq_client)
        assert actual_target_ids == expected_distinct_ids
        assert get_target_table_row_count(bq_client) == len(expected_distinct_ids)
    ```

---

### Test Case 4: Transformation Correctness - Empty Source Table

*   **Purpose:** Verify the job handles an empty source table gracefully, resulting in an empty target table.
*   **Setup:**
    1.  Ensure `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_cntrct_dist` tables exist.
    2.  Clear `project.dataset.sof_ta_bpr_basis`.
    3.  Clear `project.dataset.sof_ta_cntrct_dist`.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with valid parameters.
    ```python
    def test_empty_source_table(bq_client):
        setup_source_table(bq_client, []) # Empty source

        job_kennung = "TEST_EMPTY_SOURCE"
        eintrags_nr = "12348"
        stichtag = datetime.now().strftime('%d%m%Y')
        wiederanlauf_wert = "0"

        # Action: Call the SP
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a success code (0).
    2.  The target table `project.dataset.sof_ta_cntrct_dist` is empty.
    3.  The reported record count is 0.
    ```python
        # Assertions
        assert result.returncode == 0
        assert get_target_table_row_count(bq_client) == 0
        assert "Processed records: 0" in result.stdout
    ```

---

### Test Case 5: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify the BigQuery Stored Procedure correctly identifies and raises an error for a missing `p_JobKennung` parameter, mirroring the legacy script's `pruefeParameterGesetzt Jobkennung p_JobKennung` check.
*   **Setup:** None specific, just ensure tables exist.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with `p_JobKennung` as an empty string.
    ```python
    def test_missing_jobkennung(bq_client):
        job_kennung = "" # Missing/empty
        eintrags_nr = "12349"
        stichtag = datetime.now().strftime('%d%m%Y')
        wiederanlauf_wert = "0"

        # Action: Call the SP, expecting failure
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, expect_success=False)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a non-zero error code.
    2.  The error message in `result.stderr` or `result.stdout` contains "FEHLER: Jobkennung fehlt".
    3.  The target table `project.dataset.sof_ta_cntrct_dist` remains unchanged (no data inserted/truncated).
    ```python
        # Assertions
        assert result.returncode != 0
        assert "FEHLER: Jobkennung fehlt" in result.stderr or "FEHLER: Jobkennung fehlt" in result.stdout
        # Verify no changes to target table (e.g., by checking its state before and after, or ensuring it's empty)
        assert get_target_table_row_count(bq_client) == 0 # Assuming target was empty before test
    ```

---

### Test Case 6: Parameter Validation - Missing `p_Stichtag`

*   **Purpose:** Verify the BigQuery Stored Procedure correctly identifies and raises an error for a missing `p_Stichtag` parameter, mirroring the legacy script's `pruefeParameterGesetzt Stichtag p_Stichtag` check.
*   **Setup:** None specific.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with `p_Stichtag` as an empty string.
    ```python
    def test_missing_stichtag(bq_client):
        job_kennung = "TEST_MISSING_STICHTAG"
        eintrags_nr = "12350"
        stichtag = "" # Missing/empty
        wiederanlauf_wert = "0"

        # Action: Call the SP, expecting failure
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, expect_success=False)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a non-zero error code.
    2.  The error message in `result.stderr` or `result.stdout` contains "FEHLER: Stichtag fehlt".
    3.  The target table `project.dataset.sof_ta_cntrct_dist` remains unchanged.
    ```python
        # Assertions
        assert result.returncode != 0
        assert "FEHLER: Stichtag fehlt" in result.stderr or "FEHLER: Stichtag fehlt" in result.stdout
        assert get_target_table_row_count(bq_client) == 0
    ```

---

### Test Case 7: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose:** Verify the BigQuery Stored Procedure correctly identifies and raises an error for a missing `p_EintragsNr` parameter, mirroring the legacy script's `pruefeParameterGesetzt EintragsNr p_EintragsNr` check.
*   **Setup:** None specific.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with `p_EintragsNr` as an empty string.
    ```python
    def test_missing_eintragsnr(bq_client):
        job_kennung = "TEST_MISSING_EINTRAGSNR"
        eintrags_nr = "" # Missing/empty
        stichtag = datetime.now().strftime('%d%m%Y')
        wiederanlauf_wert = "0"

        # Action: Call the SP, expecting failure
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, expect_success=False)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a non-zero error code.
    2.  The error message in `result.stderr` or `result.stdout` contains "FEHLER: EintragsNr fehlt".
    3.  The target table `project.dataset.sof_ta_cntrct_dist` remains unchanged.
    ```python
        # Assertions
        assert result.returncode != 0
        assert "FEHLER: EintragsNr fehlt" in result.stderr or "FEHLER: EintragsNr fehlt" in result.stdout
        assert get_target_table_row_count(bq_client) == 0
    ```

---

### Test Case 8: Date Validation - Invalid `p_Stichtag` Format

*   **Purpose:** Verify the BigQuery Stored Procedure correctly identifies and raises an error for an invalid `p_Stichtag` format, mirroring the legacy script's `DWDate_Datum_Check` functionality.
*   **Setup:** None specific.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script with `p_Stichtag` in an incorrect format (e.g., `YYYY-MM-DD`).
    ```python
    def test_invalid_stichtag_format(bq_client):
        job_kennung = "TEST_INVALID_DATE"
        eintrags_nr = "12351"
        stichtag = "2023-01-01" # Invalid format
        wiederanlauf_wert = "0"

        # Action: Call the SP, expecting failure
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, expect_success=False)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a non-zero error code.
    2.  The error message in `result.stderr` or `result.stdout` contains "FEHLER: Ungültiges Datumsformat für p_Stichtag. Erwartet DDMMYYYY".
    3.  The target table `project.dataset.sof_ta_cntrct_dist` remains unchanged.
    ```python
        # Assertions
        assert result.returncode != 0
        assert "FEHLER: Ungültiges Datumsformat für p_Stichtag. Erwartet DDMMYYYY" in result.stderr or \
               "FEHLER: Ungültiges Datumsformat für p_Stichtag. Erwartet DDMMYYYY" in result.stdout
        assert get_target_table_row_count(bq_client) == 0
    ```

---

### Test Case 9: External System Replacement - `p_wiederanlaufWert` Default

*   **Purpose:** Verify that `p_wiederanlaufWert` defaults to '0' if not provided or empty, as specified in the migration design and legacy script. (Note: The provided SP code assigns `v_restart` but doesn't use it in the core logic. This test validates the assignment itself.)
*   **Setup:** None specific.
*   **Action:**
    Call the `invoke_bigquery_stored_procedure` Python script without providing `p_wiederanlaufWert` (or providing an empty string).
    ```python
    def test_wiederanlaufwert_default(bq_client):
        # This test requires inspecting the SP's internal state or logs if v_restart isn't used.
        # Since the SP prints its parameters, we can check the log message.
        job_kennung = "TEST_W_DEFAULT"
        eintrags_nr = "12352"
        stichtag = datetime.now().strftime('%d%m%Y')
        wiederanlauf_wert = "" # Empty value

        # Action: Call the SP
        result = call_stored_procedure(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert)
    ```
*   **Pass/Fail Criterion:**
    1.  The Python script exits with a success code (0).
    2.  The log message from the SP (captured in `result.stdout`) indicates `WiederanlaufWert=0` for the `v_restart` variable.
    ```python
        # Assertions
        assert result.returncode == 0
        # Check the log message for the default value
        assert "WiederanlaufWert=0" in result.stdout
    ```

---

### Test Case 10: Data Quality - `CNTRCT_ID` Data Type and `NOT NULL` Constraint

*   **Purpose:** Verify that `CNTRCT_ID` values are correctly handled as `STRING` and that the `NOT NULL` constraint is enforced, preventing invalid data from being inserted.
*   **Setup:**
    1.  Ensure `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_cntrct_dist` tables exist with `CNTRCT_ID STRING NOT NULL`.
    2.  Populate `project.dataset.sof_ta_bpr_basis` with valid string `CNTRCT_ID`s.
    3.  Attempt to insert a row with `NULL` for `CNTRCT_ID` into `sof_ta_bpr_basis` (this should fail at the DDL level, but if the source allows it, the SP should handle it).
*   **Action:**
    1.  Populate `sof_ta_bpr_basis` with valid string data.
    2.  Run the SP.
    3.  (Hypothetical, if source allows NULLs): Attempt to insert a row with `NULL` `CNTRCT_ID` into `sof_ta_bpr_basis` and run the SP.
    ```python
    def test_cntrct_id_data_type_and_not_null(bq_client):
        # Test 1: Valid string data
        source_data_valid = [
            {"CNTRCT_ID": "VALID_CONTRACT_123"},
            {"CNTRCT_ID": "ANOTHER_VALID_ID"},
        ]
        setup_source_table(bq_client, source_data_valid)
        call_stored_procedure("TEST_VALID_TYPES", "12353", datetime.now().strftime('%d%m%Y'), "0")
        actual_target_ids = get_target_table_data(bq_client)
        assert actual_target_ids == sorted([d["CNTRCT_ID"] for d in source_data_valid])

        # Test 2: What if source table allows NULLs for CNTRCT_ID and SP tries to insert?
        # The DDL for sof_ta_cntrct_dist specifies CNTRCT_ID STRING NOT NULL.
        # If sof_ta_bpr_basis could have NULL CNTRCT_ID, the INSERT would fail.
        # This scenario is implicitly tested by the DDL constraint.
        # If the source table (sof_ta_bpr_basis) *could* contain NULL CNTRCT_ID,
        # and the target (sof_ta_cntrct_dist) is NOT NULL, the SP call would fail.
        # This is a good check for data integrity.
        # For this test, we'll simulate by trying to insert NULL into the source,
        # which should fail if source DDL is also NOT NULL.
        # If source DDL allows NULL, then the SP's INSERT would fail.

        # Example of how to test if source allowed NULL and target didn't:
        # setup_source_table(bq_client, [{"CNTRCT_ID": "C1"}, {"CNTRCT_ID": None}])
        # result_null_insert = call_stored_procedure("TEST_NULL_INSERT", "12354", datetime.now().strftime('%d%m%Y'), "0", expect_success=False)
        # assert "Cannot insert NULL value into non-nullable column" in result_null_insert.stderr
        # assert get_target_table_row_count(bq_client) == 0 # Or previous state if transaction was not atomic
    ```
*   **Pass/Fail Criterion:**
    1.  Valid string `CNTRCT_ID`s are correctly inserted.
    2.  If the source table (`sof_ta_bpr_basis`) were to contain `NULL` `CNTRCT_ID`s (assuming its DDL allowed it), the `INSERT` into `sof_ta_cntrct_dist` (which is `NOT NULL`) should fail, and the SP should report an error. This confirms the `NOT NULL` constraint is respected.

---

### Test Case 11: External System Replacement - `gestern.ksh` Functionality

*   **Purpose:** Verify that if the original `d_ausd_bp_ta_cntrct_dist.sql` script implicitly used `p_datum_heute` or `p_datum_gestern` for date-based filtering or processing, the migrated BigQuery SP correctly implements this logic using BigQuery's native date functions.
*   **Context:** The provided generated SP code *does not* use `v_stichtag_date` in the `WHERE` clause of the `INSERT` statement, nor does it use `CURRENT_DATE()` or `DATE_SUB`. This is a **critical observation** from the migration design. The legacy script passes `p_Stichtag`, `p_datum_heute`, `p_datum_gestern` to `starteSQLSkript`, implying they *could* be used by `d_ausd_bp_ta_cntrct_dist.sql`. The current BigQuery SP only validates `p_Stichtag` but doesn't use it for filtering.
*   **Setup:**
    1.  This test requires a deeper understanding of the original `d_ausd_bp_ta_cntrct_dist.sql` content.
    2.  If `d_ausd_bp_ta_cntrct_dist.sql` *did* filter by a date column (e.g., `BUSINESS_DATE`) using `p_Stichtag` or `p_datum_gestern`, then the `sof_ta_bpr_basis` table would need a `BUSINESS_DATE` column.
*   **Action:**
    *   **If `d_ausd_bp_ta_cntrct_dist.sql` *did not* use any date parameters for filtering:** The current BigQuery SP is behaviorally equivalent in this regard. No specific test is needed beyond the successful execution.
    *   **If `d_ausd_bp_ta_cntrct_dist.sql` *did* use `p_Stichtag` for filtering:**
        1.  Modify `sof_ta_bpr_basis` to include a `BUSINESS_DATE DATE` column.
        2.  Modify the BigQuery SP `r_ausd_bp_ta_cntrct_dist` to include `WHERE business_date = v_stichtag_date` in the `INSERT` statement.
        3.  Populate `sof_ta_bpr_basis` with data for `v_stichtag_date` and other dates.
        4.  Run the SP.
*   **Pass/Fail Criterion (if date filtering is implemented):**
    1.  Only `CNTRCT_ID`s associated with `v_stichtag_date` in `sof_ta_bpr_basis` are inserted into `sof_ta_cntrct_dist`.
    2.  Data for other dates is correctly excluded.

    ```python
    # This is a conceptual test, as the provided SP code does not use v_stichtag_date for filtering.
    # If the original SQL script *did* filter by date, the SP would need modification.
    # Example if filtering was added to SP:
    # INSERT INTO `project.dataset.sof_ta_cntrct_dist` (CNTRCT_ID)
    # SELECT DISTINCT cntrct_id FROM `project.dataset.sof_ta_bpr_basis` WHERE business_date = v_stichtag_date;

    def test_date_filtering_logic_if_applicable(bq_client):
        # This test is only relevant if the core SQL logic in d_ausd_bp_ta_cntrct_dist.sql
        # actually used p_Stichtag or other date parameters for filtering.
        # Based on the provided generated code, it does NOT.
        # If it did, the setup and assertions would look like this:

        # # Setup: Source table with a date column
        # bq_client.query(f"CREATE OR REPLACE TABLE {SOURCE_TABLE_FULL_PATH} (CNTRCT_ID STRING NOT NULL, BUSINESS_DATE DATE);").result()
        #
        # stichtag_str = "01012023"
        # stichtag_date = datetime.strptime(stichtag_str, '%d%m%Y').date()
        #
        # source_data = [
        #     {"CNTRCT_ID": "C_TODAY_1", "BUSINESS_DATE": stichtag_date},
        #     {"CNTRCT_ID": "C_TODAY_2", "BUSINESS_DATE": stichtag_date},
        #     {"CNTRCT_ID": "C_YESTERDAY_1", "BUSINESS_DATE": (stichtag_date - timedelta(days=1))},
        #     {"CNTRCT_ID": "C_TODAY_1", "BUSINESS_DATE": stichtag_date}, # Duplicate for today
        # ]
        # setup_source_table(bq_client, source_data)
        #
        # # Action: Call the SP
        # result = call_stored_procedure("TEST_DATE_FILTER", "12355", stichtag_str, "0")
        #
        # # Assertions
        # assert result.returncode == 0
        # expected_ids = sorted(list(set([d["CNTRCT_ID"] for d in source_data if d["BUSINESS_DATE"] == stichtag_date])))
        # actual_ids = get_target_table_data(bq_client)
        # assert actual_ids == expected_ids
        # assert get_target_table_row_count(bq_client) == len(expected_ids)

        pytest.skip("This test is conceptual. The provided BigQuery SP does not use p_Stichtag for data filtering. If the original SQL did, the SP needs modification.")
    ```

---

### Test Case 12: Data Quality / Schema Assertion

*   **Purpose:** Verify that the target table `sof_ta_cntrct_dist` has the correct schema (column names, data types, nullability) as defined in the DDL and expected by the transformation.
*   **Setup:** Ensure the DDL for `project.dataset.sof_ta_cntrct_dist` has been executed.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` to retrieve the table schema.
    ```python
    def test_target_table_schema(bq_client):
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM {DATASET_ID}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'sof_ta_cntrct_dist'
        ORDER BY ordinal_position;
        """
        rows = execute_bq_query(bq_client, query)
        schema = []
        for row in rows:
            schema.append({"column_name": row.column_name, "data_type": row.data_type, "is_nullable": row.is_nullable})
    ```
*   **Pass/Fail Criterion:**
    The retrieved schema matches the expected schema:
    ```json
    [
        {"column_name": "CNTRCT_ID", "data_type": "STRING", "is_nullable": "NO"}
    ]
    ```
    ```python
        # Assertions
        expected_schema = [
            {"column_name": "CNTRCT_ID", "data_type": "STRING", "is_nullable": "NO"}
        ]
        assert schema == expected_schema
    ```

---

These tests provide a robust framework for validating the migration. The most critical aspect identified during this design is the potential discrepancy regarding date filtering using `p_Stichtag` in the core SQL logic. This should be thoroughly investigated with the original `d_ausd_bp_ta_cntrct_dist.sql` content.