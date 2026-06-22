As a senior data-migration QA engineer, I've analyzed the provided KornShell script (`k_ausd_v_ta_vvl_upgrade.ksh`) and its BigQuery migration design. The migration aims to re-implement the orchestration logic and the underlying SQL data transformation in BigQuery.

A critical observation is that the core data transformation logic within `d_ausd_v_ta_vvl_upgrade.sql` was *not provided* and is currently a placeholder in the BigQuery stored procedure `project.dataset.d_ausd_v_ta_vvl_upgrade`. This means tests for the *actual data transformation* (joins, aggregations, filters, type handling, NULL handling, edge cases) cannot be fully implemented at this stage. My tests will focus on the *orchestration logic*, *parameter handling*, *error handling*, *logging*, and *job status management* as implemented in `project.dataset.r_ausd_vertrag_control`, and the *interface* to the placeholder `d_ausd_v_ta_vvl_upgrade`.

The tests below are designed to prove behavioral equivalence for the orchestration layer and ensure the new BigQuery solution handles various scenarios as the legacy script would.

---

## Global Setup for All Tests

Before running any tests, ensure the BigQuery environment is set up.

**Purpose:** To prepare the BigQuery environment by creating the necessary dataset and deploying all DDLs and stored procedures. This ensures a clean slate for each test run.

**Setup:**
1.  Ensure you have a Google Cloud project configured and authenticated.
2.  Replace `project` and `dataset` placeholders with your actual BigQuery project ID and dataset name.
3.  Execute the provided DDLs and stored procedure definitions in BigQuery.

**Action (BigQuery SQL):**

```sql
-- Create Dataset (if it doesn't exist)
CREATE SCHEMA IF NOT EXISTS project.dataset;

-- Deploy DDLs
-- DDL for job_error_log
CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    error_code STRING,
    error_message STRING,
    error_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- DDL for job_run_log
CREATE TABLE IF NOT EXISTS project.dataset.job_run_log (
    job_run_id STRING NOT NULL DEFAULT GENERATE_UUID(),
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED'
    processed_records INT64,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- DDL for job_table
CREATE TABLE IF NOT EXISTS project.dataset.job_table (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    job_status STRING NOT NULL, -- e.g., 'ACTIVE', 'INACTIVE'
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
);

-- Deploy Stored Procedures
-- project.dataset.d_ausd_v_ta_vvl_upgrade (Placeholder)
CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_v_ta_vvl_upgrade(
    IN p_eintrags_nr STRING,
    IN p_job_kennung STRING,
    OUT processed_records INT64
)
BEGIN
    -- Simulate some processing time
    -- SELECT SLEEP(1); -- Not directly available in BQ scripting, but can be simulated with a dummy query
    -- For now, we just set it to 0 and log a message.
    -- In a real scenario, the MERGE/UPDATE/INSERT statement would be here,
    -- and `processed_records` would be set to @@row_count.
    SET processed_records = 0; -- No records processed in this placeholder
    SELECT FORMAT('Placeholder procedure project.dataset.d_ausd_v_ta_vvl_upgrade executed for JobKennung: %s, EintragsNr: %s. No actual data transformation occurred.', p_job_kennung, p_eintrags_nr);
END;

-- project.dataset.r_ausd_vertrag_control
CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag_control(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_job_run_id STRING;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_processed_records INT64;
    DECLARE v_job_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_code STRING;
    DECLARE v_is_active BOOL;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_job_run_id = GENERATE_UUID();
    SET v_job_status = 'RUNNING';

    -- 1. Parameter Validation
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_code = '193'; -- Notwendiges Argument fehlt
        SET v_error_message = 'Parameter p_JobKennung is missing or empty.';
        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_code, error_message)
        VALUES (COALESCE(p_job_kennung, 'UNKNOWN'), COALESCE(p_eintrags_nr, 'UNKNOWN'), v_error_code, v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SET v_error_code = '193'; -- Notwendiges Argument fehlt
        SET v_error_message = 'Parameter p_EintragsNr is missing or empty.';
        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_code, error_message)
        VALUES (COALESCE(p_job_kennung, 'UNKNOWN'), COALESCE(p_eintrags_nr, 'UNKNOWN'), v_error_code, v_error_message);
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- 2. Job Status Management: Check if this job instance is already active
    SELECT COUNT(1) > 0
    INTO v_is_active
    FROM project.dataset.job_table
    WHERE job_kennung = p_job_kennung
      AND eintrags_nr = p_eintrags_nr
      AND job_status = 'ACTIVE';

    IF v_is_active THEN
        SET v_job_status = 'SKIPPED';
        SET v_error_message = FORMAT('JobKennung: %s, EintragsNr: %s is already active. Skipping execution.', p_job_kennung, p_eintrags_nr);
        INSERT INTO project.dataset.job_run_log (
            job_run_id, job_kennung, eintrags_nr, start_timestamp, end_timestamp, status, processed_records
        ) VALUES (
            v_job_run_id, p_job_kennung, p_eintrags_nr, v_start_timestamp, CURRENT_TIMESTAMP(), v_job_status, NULL
        );
        -- Log to console as well
        SELECT v_error_message AS message;
        RETURN; -- Exit early if already active
    END IF;

    -- If not active, deactivate any previous active entries for this specific job instance
    -- This handles cases where a previous run might have failed to update its status to INACTIVE/COMPLETED
    UPDATE project.dataset.job_table
    SET job_status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_job_kennung
      AND eintrags_nr = p_eintrags_nr
      AND job_status = 'ACTIVE';

    -- Upsert/Insert the current job instance as ACTIVE
    MERGE INTO project.dataset.job_table AS T
    USING (SELECT p_job_kennung AS job_kennung, p_eintrags_nr AS eintrags_nr) AS S
    ON T.job_kennung = S.job_kennung AND T.eintrags_nr = S.eintrags_nr
    WHEN MATCHED THEN
        UPDATE SET T.job_status = 'ACTIVE', T.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintrags_nr, job_status)
        VALUES (S.job_kennung, S.eintrags_nr, 'ACTIVE');

    -- Initial logging of job start
    INSERT INTO project.dataset.job_run_log (
        job_run_id, job_kennung, eintrags_nr, start_timestamp, status
    ) VALUES (
        v_job_run_id, p_job_kennung, p_eintrags_nr, v_start_timestamp, 'RUNNING'
    );

    BEGIN
        -- 3. Execute the core SQL transformation procedure
        CALL project.dataset.d_ausd_v_ta_vvl_upgrade(p_eintrags_nr, p_job_kennung, v_processed_records);

        -- 4. Log completion and update job status
        SET v_job_status = 'COMPLETED';
        UPDATE project.dataset.job_run_log
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_job_status,
            processed_records = v_processed_records
        WHERE job_run_id = v_job_run_id;

        UPDATE project.dataset.job_table
        SET job_status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_job_kennung
          AND eintrags_nr = p_eintrags_nr;

        SELECT FORMAT('JobKennung: %s, EintragsNr: %s completed successfully. Processed records: %d.', p_job_kennung, p_eintrags_nr, v_processed_records) AS message;

    EXCEPTION WHEN ERROR THEN
        -- 5. Error Handling
        SET v_job_status = 'FAILED';
        SET v_error_message = @@error.message;
        SET v_error_code = 'GENERIC_SQL_ERROR'; -- Or parse @@error.message for more specific codes

        INSERT INTO project.dataset.job_error_log (job_kennung, eintrags_nr, error_code, error_message)
        VALUES (p_job_kennung, p_eintrags_nr, v_error_code, v_error_message);

        UPDATE project.dataset.job_run_log
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_job_status
        WHERE job_run_id = v_job_run_id;

        UPDATE project.dataset.job_table
        SET job_status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_job_kennung
          AND eintrags_nr = p_eintrags_nr;

        RAISE USING MESSAGE FORMAT('JobKennung: %s, EintragsNr: %s failed with error: %s', p_job_kennung, p_eintrags_nr, v_error_message);
    END;
END;
```

**Helper Function (Python/Pytest):**

```python
import pytest
from google.cloud import bigquery
import time

PROJECT_ID = "your-gcp-project-id"  # Replace with your project ID
DATASET_ID = "dataset" # Replace with your dataset ID

client = bigquery.Client(project=PROJECT_ID)

def clear_log_tables():
    """Clears all log and job status tables for a clean test run."""
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_run_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_table`").result()
    print("Log and job status tables cleared.")

def call_bigquery_procedure(procedure_name, job_kennung, eintrags_nr):
    """Calls a BigQuery stored procedure and returns the job result."""
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.{procedure_name}`('{job_kennung}', '{eintrags_nr}')"
    print(f"Executing: {query}")
    try:
        job = client.query(query)
        job.result() # Wait for the job to complete
        return {"status": "SUCCESS", "message": "Procedure executed successfully."}
    except Exception as e:
        return {"status": "FAILED", "message": str(e)}

def get_table_data(table_name, filter_clause=None):
    """Fetches data from a BigQuery table."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
    if filter_clause:
        query += f" WHERE {filter_clause}"
    rows = client.query(query).result()
    return [dict(row) for row in rows]

@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Fixture to clear tables before each test."""
    clear_log_tables()
    yield
    # Optional: clear tables after each test as well, or just rely on autouse fixture for before.
    # clear_log_tables()
```

---

## Test Case 1: Successful Execution with Valid Parameters

**Purpose:** To verify that the migrated control procedure (`r_ausd_vertrag_control`) executes successfully when provided with valid parameters, logs the run, and correctly updates the job status. This covers output parity for a successful run and basic logging/job status management.

**Setup:**
*   Ensure log and job status tables are empty (handled by `setup_and_teardown` fixture).
*   Define `p_JobKennung = 'TEST_JOB_001'` and `p_EintragsNr = 'ENTRY_001'`.

**Action:**
*   Call `project.dataset.r_ausd_vertrag_control` with the defined parameters.

**Pass/Fail Criterion:**
1.  The procedure call completes without raising an error.
2.  `job_run_log` contains exactly one entry for `TEST_JOB_001`/`ENTRY_001` with `status = 'COMPLETED'`.
3.  `job_run_log` entry shows `processed_records = 0` (due to placeholder `d_ausd_v_ta_vvl_upgrade`).
4.  `job_table` contains exactly one entry for `TEST_JOB_001`/`ENTRY_001` with `job_status = 'INACTIVE'`.
5.  `job_error_log` is empty.

**Runnable Test Code (Pytest):**

```python
def test_successful_execution(setup_and_teardown):
    job_kennung = "TEST_JOB_001"
    eintrags_nr = "ENTRY_001"

    result = call_bigquery_procedure("r_ausd_vertrag_control", job_kennung, eintrags_nr)
    assert result["status"] == "SUCCESS", f"Procedure failed: {result['message']}"

    # Verify job_run_log
    run_logs = get_table_data("job_run_log", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
    assert len(run_logs) == 1, "Expected one entry in job_run_log"
    assert run_logs[0]["status"] == "COMPLETED", "Job status should be COMPLETED"
    assert run_logs[0]["processed_records"] == 0, "Processed records should be 0 for placeholder"
    assert run_logs[0]["start_timestamp"] is not None
    assert run_logs[0]["end_timestamp"] is not None

    # Verify job_table
    job_status = get_table_data("job_table", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
    assert len(job_status) == 1, "Expected one entry in job_table"
    assert job_status[0]["job_status"] == "INACTIVE", "Job status in job_table should be INACTIVE after completion"

    # Verify job_error_log is empty
    error_logs = get_table_data("job_error_log")
    assert len(error_logs) == 0, "job_error_log should be empty"
```

---

## Test Case 2: Missing `p_JobKennung` Parameter

**Purpose:** To verify that the control procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and terminating execution, mirroring the legacy script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.

**Setup:**
*   Ensure log and job status tables are empty.
*   Define `p_JobKennung = NULL` (or empty string) and `p_EintragsNr = 'ENTRY_002'`.

**Action:**
*   Call `project.dataset.r_ausd_vertrag_control` with `p_JobKennung = ''` and `p_EintragsNr = 'ENTRY_002'`.

**Pass/Fail Criterion:**
1.  The procedure call raises an error.
2.  The error message contains "Parameter p_JobKennung is missing or empty."
3.  `job_error_log` contains exactly one entry with `job_kennung = 'UNKNOWN'`, `eintrags_nr = 'ENTRY_002'`, `error_code = '193'`, and the expected error message.
4.  `job_run_log` is empty (no run initiated for invalid parameters).
5.  `job_table` is empty (no job status recorded for invalid parameters).

**Runnable Test Code (Pytest):**

```python
def test_missing_job_kennung_parameter(setup_and_teardown):
    job_kennung = "" # Simulates empty/missing parameter
    eintrags_nr = "ENTRY_002"

    result = call_bigquery_procedure("r_ausd_vertrag_control", job_kennung, eintrags_nr)
    assert result["status"] == "FAILED", "Procedure should have failed due to missing parameter"
    assert "Parameter p_JobKennung is missing or empty." in result["message"]

    # Verify job_error_log
    error_logs = get_table_data("job_error_log", f"eintrags_nr = '{eintrags_nr}'")
    assert len(error_logs) == 1, "Expected one error entry in job_error_log"
    assert error_logs[0]["job_kennung"] == "UNKNOWN", "JobKennung should be 'UNKNOWN' for missing parameter"
    assert error_logs[0]["eintrags_nr"] == eintrags_nr
    assert error_logs[0]["error_code"] == "193", "Error code should be 193 (missing argument)"
    assert "Parameter p_JobKennung is missing or empty." in error_logs[0]["error_message"]

    # Verify job_run_log is empty
    run_logs = get_table_data("job_run_log")
    assert len(run_logs) == 0, "job_run_log should be empty for failed parameter validation"

    # Verify job_table is empty
    job_status = get_table_data("job_table")
    assert len(job_status) == 0, "job_table should be empty for failed parameter validation"
```

---

## Test Case 3: Missing `p_EintragsNr` Parameter

**Purpose:** To verify that the control procedure correctly identifies and handles a missing `p_EintragsNr` parameter, logging an error and terminating execution.

**Setup:**
*   Ensure log and job status tables are empty.
*   Define `p_JobKennung = 'TEST_JOB_003'` and `p_EintragsNr = NULL` (or empty string).

**Action:**
*   Call `project.dataset.r_ausd_vertrag_control` with `p_JobKennung = 'TEST_JOB_003'` and `p_EintragsNr = ''`.

**Pass/Fail Criterion:**
1.  The procedure call raises an error.
2.  The error message contains "Parameter p_EintragsNr is missing or empty."
3.  `job_error_log` contains exactly one entry with `job_kennung = 'TEST_JOB_003'`, `eintrags_nr = 'UNKNOWN'`, `error_code = '193'`, and the expected error message.
4.  `job_run_log` is empty.
5.  `job_table` is empty.

**Runnable Test Code (Pytest):**

```python
def test_missing_eintrags_nr_parameter(setup_and_teardown):
    job_kennung = "TEST_JOB_003"
    eintrags_nr = "" # Simulates empty/missing parameter

    result = call_bigquery_procedure("r_ausd_vertrag_control", job_kennung, eintrags_nr)
    assert result["status"] == "FAILED", "Procedure should have failed due to missing parameter"
    assert "Parameter p_EintragsNr is missing or empty." in result["message"]

    # Verify job_error_log
    error_logs = get_table_data("job_error_log", f"job_kennung = '{job_kennung}'")
    assert len(error_logs) == 1, "Expected one error entry in job_error_log"
    assert error_logs[0]["job_kennung"] == job_kennung
    assert error_logs[0]["eintrags_nr"] == "UNKNOWN", "EintragsNr should be 'UNKNOWN' for missing parameter"
    assert error_logs[0]["error_code"] == "193", "Error code should be 193 (missing argument)"
    assert "Parameter p_EintragsNr is missing or empty." in error_logs[0]["error_message"]

    # Verify job_run_log is empty
    run_logs = get_table_data("job_run_log")
    assert len(run_logs) == 0, "job_run_log should be empty for failed parameter validation"

    # Verify job_table is empty
    job_status = get_table_data("job_table")
    assert len(job_status) == 0, "job_table should be empty for failed parameter validation"
```

---

## Test Case 4: Job Already Active (Skip Logic)

**Purpose:** To verify the "active jobs are ignored" logic. If a job with the same `job_kennung` and `eintrags_nr` is already marked as `ACTIVE` in `job_table`, the new invocation should be skipped, log a `SKIPPED` status, and not proceed with the data transformation. This directly replaces the legacy `starteSQLSkript` behavior.

**Setup:**
*   Ensure log and job status tables are empty.
*   Define `p_JobKennung = 'TEST_JOB_004'` and `p_EintragsNr = 'ENTRY_004'`.
*   Manually insert an `ACTIVE` entry into `job_table` for these parameters.

**Action:**
*   Call `project.dataset.r_ausd_vertrag_control` with the defined parameters.

**Pass/Fail Criterion:**
1.  The procedure call completes without raising an error.
2.  `job_run_log` contains exactly one entry for `TEST_JOB_004`/`ENTRY_004` with `status = 'SKIPPED'`.
3.  `job_table` still contains the original `ACTIVE` entry for `TEST_JOB_004`/`ENTRY_004`. No new entry or modification to the existing one should occur by the skipped run.
4.  `job_error_log` is empty.
5.  The `d_ausd_v_ta_vvl_upgrade` procedure should *not* have been called (verified by checking `job_run_log` for the placeholder's specific log message, or by asserting no `COMPLETED` status).

**Runnable Test Code (Pytest):**

```python
def test_job_already_active_skips_execution(setup_and_teardown):
    job_kennung = "TEST_JOB_004"
    eintrags_nr = "ENTRY_004"

    # Manually set job as ACTIVE
    insert_active_job_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_table` (job_kennung, eintrags_nr, job_status)
    VALUES ('{job_kennung}', '{eintrags_nr}', 'ACTIVE')
    """
    client.query(insert_active_job_query).result()
    print(f"Inserted active job: {job_kennung}/{eintrags_nr}")

    # Call the control procedure
    result = call_bigquery_procedure("r_ausd_vertrag_control", job_kennung, eintrags_nr)
    assert result["status"] == "SUCCESS", f"Procedure failed: {result['message']}"
    assert f"JobKennung: {job_kennung}, EintragsNr: {eintrags_nr} is already active. Skipping execution." in result["message"]

    # Verify job_run_log
    run_logs = get_table_data("job_run_log", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
    assert len(run_logs) == 1, "Expected one entry in job_run_log for the skipped job"
    assert run_logs[0]["status"] == "SKIPPED", "Job status should be SKIPPED"
    assert run_logs[0]["processed_records"] is None, "Processed records should be NULL for skipped job"

    # Verify job_table - should still be ACTIVE from initial setup
    job_status = get_table_data("job_table", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
    assert len(job_status) == 1, "Expected one entry in job_table"
    assert job_status[0]["job_status"] == "ACTIVE", "Job status in job_table should remain ACTIVE"

    # Verify job_error_log is empty
    error_logs = get_table_data("job_error_log")
    assert len(error_logs) == 0, "job_error_log should be empty"
```

---

## Test Case 5: Deactivation of Old Active Jobs

**Purpose:** To verify that if a job with the same `job_kennung` and `eintrags_nr` was previously active (e.g., due to a crash or incomplete previous run), the new run correctly deactivates the old entry before marking itself as `ACTIVE`. This covers the "old active jobs are deactivated" logic.

**Setup:**
*   Ensure log and job status tables are empty.
*   Define `p_JobKennung = 'TEST_JOB_005'` and `p_EintragsNr = 'ENTRY_005'`.
*   Manually insert an `ACTIVE` entry into `job_table` for these parameters, simulating a previous crashed run.

**Action:**
*   Call `project.dataset.r_ausd_vertrag_control` with the defined parameters.

**Pass/Fail Criterion:**
1.  The procedure call completes successfully.
2.  `job_run_log` contains one `RUNNING` entry (initial log) and one `COMPLETED` entry for `TEST_JOB_005`/`ENTRY_005`.
3.  `job_table` contains exactly one entry for `TEST_JOB_005`/`ENTRY_005` with `job_status = 'INACTIVE'` (the newly completed run). The original `ACTIVE` entry should have been updated to `INACTIVE`.
4.  `job_error_log` is empty.

**Runnable Test Code (Pytest):**

```python
def test_deactivates_old_active_jobs(setup_and_teardown):
    job_kennung = "TEST_JOB_005"
    eintrags_nr = "ENTRY_005"

    # Manually set job as ACTIVE, simulating a crashed previous run
    insert_old_active_job_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_table` (job_kennung, eintrags_nr, job_status, created_at)
    VALUES ('{job_kennung}', '{eintrags_nr}', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
    """
    client.query(insert_old_active_job_query).result()
    print(f"Inserted old active job: {job_kennung}/{eintrags_nr}")

    # Call the control procedure
    result = call_bigquery_procedure("r_ausd_vertrag_control", job_kennung, eintrags_nr)
    assert result["status"] == "SUCCESS", f"Procedure failed: {result['message']}"

    # Verify job_run_log
    run_logs = get_table_data("job_run_log", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
    assert len(run_logs) == 2, "Expected two entries in job_run_log (RUNNING and COMPLETED)"
    assert any(log["status"] == "RUNNING" for log in run_logs)
    assert any(log["status"] == "COMPLETED" for log in run_logs)
    assert all(log["processed_records"] == 0 for log in run_logs if log["status"] == "COMPLETED")

    # Verify job_table - the old ACTIVE entry should be updated to INACTIVE
    job_status = get_table_data("job_table", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
    assert len(job_status) == 1, "Expected one entry in job_table (the updated one)"
    assert job_status[0]["job_status"] == "INACTIVE", "Job status in job_table should be INACTIVE after completion"
    assert job_status[0]["updated_at"] > job_status[0]["created_at"], "Updated_at should be later than created_at"

    # Verify job_error_log is empty
    error_logs = get_table_data("job_error_log")
    assert len(error_logs) == 0, "job_error_log should be empty"
```

---

## Test Case 6: Internal Data Transformation Procedure Failure

**Purpose:** To verify that the control procedure correctly handles errors originating from the underlying data transformation procedure (`d_ausd_v_ta_vvl_upgrade`), logging the failure, updating job status, and re-raising the error. This covers error handling and external system replacement (Oracle SQL script failure).

**Setup:**
*   Ensure log and job status tables are empty.
*   Define `p_JobKennung = 'TEST_JOB_006'` and `p_EintragsNr = 'ENTRY_006'`.
*   **Temporarily modify `d_ausd_v_ta_vvl_upgrade` to simulate a failure.**

**Action:**
1.  Modify `d_ausd_v_ta_vvl_upgrade` to `RAISE` an error.
2.  Call `project.dataset.r_ausd_vertrag_control` with the defined parameters.
3.  Revert `d_ausd_v_ta_vvl_upgrade` to its original placeholder state.

**Pass/Fail Criterion:**
1.  The procedure call raises an error.
2.  The error message contains the simulated error from `d_ausd_v_ta_vvl_upgrade`.
3.  `job_error_log` contains exactly one entry for `TEST_JOB_006`/`ENTRY_006` with `error_code = 'GENERIC_SQL_ERROR'` (or a more specific code if parsed) and the simulated error message.
4.  `job_run_log` contains two entries: one `RUNNING` and one `FAILED` for `TEST_JOB_006`/`ENTRY_006`.
5.  `job_table` contains exactly one entry for `TEST_JOB_006`/`ENTRY_006` with `job_status = 'INACTIVE'`.

**Runnable Test Code (Pytest):**

```python
def test_internal_transformation_procedure_failure(setup_and_teardown):
    job_kennung = "TEST_JOB_006"
    eintrags_nr = "ENTRY_006"
    simulated_error_message = "Simulated error during data transformation!"

    # 1. Temporarily modify d_ausd_v_ta_vvl_upgrade to raise an error
    modified_d_ausd_proc = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_vvl_upgrade`(
        IN p_eintrags_nr STRING,
        IN p_job_kennung STRING,
        OUT processed_records INT64
    )
    BEGIN
        RAISE USING MESSAGE '{simulated_error_message}';
    END;
    """
    client.query(modified_d_ausd_proc).result()
    print("d_ausd_v_ta_vvl_upgrade modified to raise error.")

    try:
        # 2. Call the control procedure
        result = call_bigquery_procedure("r_ausd_vertrag_control", job_kennung, eintrags_nr)
        assert result["status"] == "FAILED", "Procedure should have failed due to internal error"
        assert simulated_error_message in result["message"]

        # 3. Verify job_error_log
        error_logs = get_table_data("job_error_log", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
        assert len(error_logs) == 1, "Expected one error entry in job_error_log"
        assert error_logs[0]["job_kennung"] == job_kennung
        assert error_logs[0]["eintrags_nr"] == eintrags_nr
        assert error_logs[0]["error_code"] == "GENERIC_SQL_ERROR"
        assert simulated_error_message in error_logs[0]["error_message"]

        # 4. Verify job_run_log
        run_logs = get_table_data("job_run_log", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
        assert len(run_logs) == 2, "Expected two entries in job_run_log (RUNNING and FAILED)"
        assert any(log["status"] == "RUNNING" for log in run_logs)
        assert any(log["status"] == "FAILED" for log in run_logs)

        # 5. Verify job_table
        job_status = get_table_data("job_table", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'")
        assert len(job_status) == 1, "Expected one entry in job_table"
        assert job_status[0]["job_status"] == "INACTIVE", "Job status in job_table should be INACTIVE after failure"

    finally:
        # Revert d_ausd_v_ta_vvl_upgrade to its original placeholder state
        original_d_ausd_proc = f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_vvl_upgrade`(
            IN p_eintrags_nr STRING,
            IN p_job_kennung STRING,
            OUT processed_records INT64
        )
        BEGIN
            SET processed_records = 0;
            SELECT FORMAT('Placeholder procedure {PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_vvl_upgrade executed for JobKennung: %s, EintragsNr: %s. No actual data transformation occurred.', p_job_kennung, p_eintrags_nr);
        END;
        """
        client.query(original_d_ausd_proc).result()
        print("d_ausd_v_ta_vvl_upgrade reverted to original placeholder.")
```

---

## Test Case 7: `d_ausd_v_ta_vvl_upgrade` Interface and Record Count (Placeholder)

**Purpose:** To verify that the `r_ausd_vertrag_control` procedure correctly calls `d_ausd_v_ta_vvl_upgrade` and captures its `processed_records` `OUT` parameter. This tests the interface between the control and transformation layers, and the handling of the record count (which replaces the temporary file in the legacy script).

**Setup:**
*   Ensure log and job status tables are empty.
*   Define `p_JobKennung = 'TEST_JOB_007'` and `p_EintragsNr = 'ENTRY_007'`.
*   **Temporarily modify `d_ausd_v_ta_vvl_upgrade` to return a specific `processed_records` count.**

**Action:**
1.  Modify `d_ausd_v_ta_vvl_upgrade` to set `processed_records` to a non-zero value (e.g., 123).
2.  Call `project.dataset.r_ausd_vertrag_control` with the defined parameters.
3.  Revert `d_ausd_v_ta_vvl_upgrade` to its original placeholder state.

**Pass/Fail Criterion:**
1.  The procedure call completes successfully.
2.  `job_run_log` contains a `COMPLETED` entry for `TEST_JOB_007`/`ENTRY_007` with `processed_records = 123`.
3.  `job_error_log` is empty.

**Runnable Test Code (Pytest):**

```python
def test_d_ausd_v_ta_vvl_upgrade_interface_and_record_count(setup_and_teardown):
    job_kennung = "TEST_JOB_007"
    eintrags_nr = "ENTRY_007"
    expected_processed_records = 123

    # 1. Temporarily modify d_ausd_v_ta_vvl_upgrade to return a specific count
    modified_d_ausd_proc = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_vvl_upgrade`(
        IN p_eintrags_nr STRING,
        IN p_job_kennung STRING,
        OUT processed_records INT64
    )
    BEGIN
        SET processed_records = {expected_processed_records};
        SELECT FORMAT('Simulated data transformation for JobKennung: %s, EintragsNr: %s. Processed %d records.', p_job_kennung, p_eintrags_nr, processed_records);
    END;
    """
    client.query(modified_d_ausd_proc).result()
    print(f"d_ausd_v_ta_vvl_upgrade modified to return {expected_processed_records} records.")

    try:
        # 2. Call the control procedure
        result = call_bigquery_procedure("r_ausd_vertrag_control", job_kennung, eintrags_nr)
        assert result["status"] == "SUCCESS", f"Procedure failed: {result['message']}"

        # 3. Verify job_run_log
        run_logs = get_table_data("job_run_log", f"job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}' AND status = 'COMPLETED'")
        assert len(run_logs) == 1, "Expected one COMPLETED entry in job_run_log"
        assert run_logs[0]["processed_records"] == expected_processed_records, f"Expected processed_records to be {expected_processed_records}"

        # 4. Verify job_error_log is empty
        error_logs = get_table_data("job_error_log")
        assert len(error_logs) == 0, "job_error_log should be empty"

    finally:
        # Revert d_ausd_v_ta_vvl_upgrade to its original placeholder state
        original_d_ausd_proc = f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_vvl_upgrade`(
            IN p_eintrags_nr STRING,
            IN p_job_kennung STRING,
            OUT processed_records INT64
        )
        BEGIN
            SET processed_records = 0;
            SELECT FORMAT('Placeholder procedure {PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_vvl_upgrade executed for JobKennung: %s, EintragsNr: %s. No actual data transformation occurred.', p_job_kennung, p_eintrags_nr);
        END;
        """
        client.query(original_d_ausd_proc).result()
        print("d_ausd_v_ta_vvl_upgrade reverted to original placeholder.")
```

---

## Test Case 8: Schema Assertions for Log Tables

**Purpose:** To verify that the DDLs for `job_error_log`, `job_run_log`, and `job_table` are correctly applied and their schemas match the expected structure and data types. This covers data quality and schema assertions.

**Setup:**
*   Ensure all DDLs have been deployed (handled by Global Setup).

**Action:**
*   Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for each table.

**Pass/Fail Criterion:**
*   Each table exists.
*   Each table has the expected columns with the correct data types and nullability constraints as defined in the DDLs.

**Runnable Test Code (Pytest):**

```python
def test_schema_assertions_for_log_tables(setup_and_teardown):
    expected_schemas = {
        "job_error_log": {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "eintrags_nr": {"data_type": "STRING", "is_nullable": "NO"},
            "error_code": {"data_type": "STRING", "is_nullable": "YES"},
            "error_message": {"data_type": "STRING", "is_nullable": "YES"},
            "error_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
        },
        "job_run_log": {
            "job_run_id": {"data_type": "STRING", "is_nullable": "NO"},
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "eintrags_nr": {"data_type": "STRING", "is_nullable": "NO"},
            "start_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
            "end_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "YES"},
            "status": {"data_type": "STRING", "is_nullable": "YES"},
            "processed_records": {"data_type": "INT64", "is_nullable": "YES"},
            "log_timestamp": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
        },
        "job_table": {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "eintrags_nr": {"data_type": "STRING", "is_nullable": "NO"},
            "job_status": {"data_type": "STRING", "is_nullable": "NO"},
            "created_at": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
            "updated_at": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
        },
    }

    for table_name, expected_cols in expected_schemas.items():
        print(f"Checking schema for table: {table_name}")
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY column_name
        """
        schema_rows = client.query(query).result()
        actual_schema = {row["column_name"]: {"data_type": row["data_type"], "is_nullable": row["is_nullable"]} for row in schema_rows}

        assert len(actual_schema) == len(expected_cols), f"Table {table_name} has unexpected number of columns."
        for col_name, expected_props in expected_cols.items():
            assert col_name in actual_schema, f"Column {col_name} missing in table {table_name}."
            assert actual_schema[col_name]["data_type"] == expected_props["data_type"], \
                f"Column {col_name} in {table_name}: Expected data_type {expected_props['data_type']}, got {actual_schema[col_name]['data_type']}."
            assert actual_schema[col_name]["is_nullable"] == expected_props["is_nullable"], \
                f"Column {col_name} in {table_name}: Expected is_nullable {expected_props['is_nullable']}, got {actual_schema[col_name]['is_nullable']}."
        print(f"Schema for {table_name} verified successfully.")

```