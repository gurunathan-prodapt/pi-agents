As a senior data-migration QA engineer, I've designed a suite of tests to validate the migration of `k_ausd_v_ta_p_discount_rr.ksh` to Google BigQuery. The tests focus on ensuring behavioral equivalence, data integrity, and correct handling of control flow and error conditions as specified in the migration design document.

The migration involves:
*   **Orchestration:** `k_ausd_v_ta_p_discount_rr.ksh` -> `project.dataset.r_ausd_vertrag` (BigQuery Stored Procedure)
*   **Data Transformation:** `d_ausd_v_ta_p_discount_rr.sql` -> `project.dataset.d_ausd_v_ta_p_discount_rr` (BigQuery Stored Procedure)
*   **Job Control/Logging:** Temporary files, implicit job tables, shell error handling -> `project.dataset.job_control`, `project.dataset.job_error_log`, `project.dataset.job_audit` (BigQuery Tables)

The tests are structured to cover output parity, transformation correctness, external system replacements (job control, logging), and data quality assertions.

---

### Test Setup Prerequisites

Before running any tests, ensure the following:
1.  **BigQuery Project and Dataset:** Replace `your_gcp_project_id` and `your_bigquery_dataset_id` with your actual GCP project ID and BigQuery dataset ID in all SQL and Python code snippets.
2.  **BigQuery Client:** A Python environment with the `google-cloud-bigquery` library installed and authenticated to access your BigQuery project.
3.  **DDL Execution:** The DDL for `job_control`, `job_error_log`, `job_audit`, and the target table `sof_ta_p_discount_rr` (implied by `d_ausd_v_ta_p_discount_rr` procedure) must be executed in the target BigQuery dataset. The source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`) must also exist with appropriate schemas.
4.  **Stored Procedures:** The `d_ausd_v_ta_p_discount_rr` and `r_ausd_vertrag` stored procedures must be deployed to the target BigQuery dataset.

The following Python helper functions will be used for setting up test data and asserting results:

```python
# test_utils.py (or similar)
from google.cloud import bigquery
import uuid
import time
import os

# Replace with your actual project and dataset IDs
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your_gcp_project_id")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "your_bigquery_dataset_id")

client = bigquery.Client(project=PROJECT_ID)

def execute_query(query, dry_run=False):
    """Executes a BigQuery SQL query."""
    job_config = bigquery.QueryJobConfig(dry_run=dry_run)
    query_job = client.query(query, job_config=job_config)
    if dry_run:
        print(f"Dry run query: {query}")
        print(f"Bytes processed: {query_job.total_bytes_processed}")
        return None
    return query_job.result()

def call_procedure(procedure_name, *args):
    """Calls a BigQuery stored procedure."""
    arg_strings = []
    for arg in args:
        if isinstance(arg, str):
            arg_strings.append(f"'{arg}'")
        elif arg is None:
            arg_strings.append("NULL")
        else:
            arg_strings.append(str(arg))
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.{procedure_name}`({', '.join(arg_strings)});"
    print(f"Executing: {query}")
    try:
        execute_query(query)
        return True, None
    except Exception as e:
        print(f"Procedure call failed: {e}")
        return False, str(e)

def setup_base_tables():
    """Clears control tables and populates source tables with base data."""
    print("Setting up base tables...")
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`;")
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`;")
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_audit`;")
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_p_discount_rr`;")

    # Create dummy source tables if they don't exist and populate with base data
    execute_query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_discount_rr` (
            cntrct_id STRING, discount_id STRING, disc_vector_ty STRING,
            cntrct_obj_version INT64, cntrct_template_id STRING,
            disc_invoice_item_id STRING, rabatt FLOAT64, rabatthoehe FLOAT64,
            rabattierte_rech_pos STRING
        );
    """)
    execute_query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_discount_rr` VALUES
        ('C1', 'D1', 'TYPEA', 1, 'T1', 'II1', 10.0, 100.0, 'RP1'),
        ('C2', 'D2', 'TYPEB', 1, 'T2', 'II2', 5.0, 50.0, 'RP2'),
        ('C3', 'D3', 'TYPEC', 1, 'T3', 'II3', 12.0, 120.0, 'RP3'),
        ('C_NO_MATCH', 'D4', 'TYPED', 1, 'T4', 'II4', 20.0, 200.0, 'RP4');
    """)

    execute_query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs` (
            cntrct_id STRING, obj_version INT64, contract_number STRING
        );
    """)
    execute_query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs` VALUES
        ('C1', 1, 'CN1001'),
        ('C2', 1, 'CN1002'),
        ('C3', 1, 'CN1003');
    """)

    execute_query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_templ` (
            cntrct_template_id STRING, cds_description STRING
        );
    """)
    execute_query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_templ` VALUES
        ('T1', 'Template A Desc'),
        ('T2', 'Template B Desc'),
        ('T3', 'Template C Desc');
    """)
    print("Base tables setup complete.")

def get_table_row_count(table_name):
    """Returns the row count of a specified table."""
    query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`;"
    for row in execute_query(query):
        return row[0]
    return 0

def get_table_data(table_name, order_by_col=None):
    """Fetches all data from a table as a list of dictionaries."""
    order_clause = f"ORDER BY {order_by_col}" if order_by_col else ""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` {order_clause};"
    return [dict(row) for row in execute_query(query)]

def get_job_control_entry(job_kennung, eintrags_nr, status=None):
    """Fetches the latest job_control entry for given parameters."""
    status_filter = f"AND status = '{status}'" if status else ""
    query = f"""
        SELECT job_run_id, job_name, job_kennung, eintrags_nr, status, message, start_time, end_time
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = {eintrags_nr} {status_filter}
        ORDER BY start_time DESC LIMIT 1;
    """
    rows = list(execute_query(query))
    return rows[0] if rows else None

def get_job_audit_entry(job_kennung, eintrags_nr):
    """Fetches the latest job_audit entry for given parameters."""
    query = f"""
        SELECT processed_records, status, message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_audit`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = {eintrags_nr}
        ORDER BY start_time DESC LIMIT 1;
    """
    rows = list(execute_query(query))
    return rows[0] if rows else None

def get_job_error_log_entry(job_kennung, eintrags_nr):
    """Fetches the latest job_error_log entry for given parameters."""
    query = f"""
        SELECT error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = {eintrags_nr}
        ORDER BY error_time DESC LIMIT 1;
    """
    rows = list(execute_query(query))
    return rows[0] if rows else None

```

---

### Test Case 1: Successful End-to-End Execution

**Purpose:** Verify that the migrated orchestration and data processing procedures execute successfully, transform data correctly, and log job status and record counts as expected. This covers output parity, transformation correctness, and data quality/row count assertions for the happy path.

**Setup:**
1.  Ensure all control tables (`job_control`, `job_error_log`, `job_audit`) and the target data table (`sof_ta_p_discount_rr`) are empty.
2.  Populate the source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`) with a representative set of valid data that should result in successful joins and insertions.

**Action:**
Call the main orchestration stored procedure `r_ausd_vertrag` with valid `p_job_kennung` and `p_eintrags_nr` parameters.

```python
# In your test file (e.g., test_migration.py)
from test_utils import setup_base_tables, call_procedure, get_table_row_count, get_table_data, get_job_control_entry, get_job_audit_entry

def test_successful_end_to_end_execution():
    setup_base_tables() # Clears control tables and populates source data

    job_kennung = "TEST_JOB_SUCCESS"
    eintrags_nr = 12345

    # Expected data after transformation
    expected_target_data = [
        {'cntrct_id': 'C1', 'discount_id': 'D1', 'disc_vector_ty': 'TYPEA', 'cntrct_obj_version': 1, 'cntrct_template_id': 'T1', 'disc_invoice_item_id': 'II1', 'rabatt': 10.0, 'rabatthoehe': 100.0, 'rabattierte_rech_pos': 'RP1', 'contract_number': 'CN1001', 'std_vertrag': 'Template A Desc'},
        {'cntrct_id': 'C2', 'discount_id': 'D2', 'disc_vector_ty': 'TYPEB', 'cntrct_obj_version': 1, 'cntrct_template_id': 'T2', 'disc_invoice_item_id': 'II2', 'rabatt': 5.0, 'rabatthoehe': 50.0, 'rabattierte_rech_pos': 'RP2', 'contract_number': 'CN1002', 'std_vertrag': 'Template B Desc'},
        {'cntrct_id': 'C3', 'discount_id': 'D3', 'disc_vector_ty': 'TYPEC', 'cntrct_obj_version': 1, 'cntrct_template_id': 'T3', 'disc_invoice_item_id': 'II3', 'rabatt': 12.0, 'rabatthoehe': 120.0, 'rabattierte_rech_pos': 'RP3', 'contract_number': 'CN1003', 'std_vertrag': 'Template C Desc'}
    ]

    success, error_msg = call_procedure("r_ausd_vertrag", job_kennung, eintrags_nr)

    assert success, f"Procedure call failed: {error_msg}"

    # Verify target table content
    actual_target_data = get_table_data("sof_ta_p_discount_rr", order_by_col="cntrct_id")
    assert len(actual_target_data) == len(expected_target_data), \
        f"Expected {len(expected_target_data)} records, got {len(actual_target_data)}"
    assert actual_target_data == expected_target_data, \
        f"Target data mismatch. Expected: {expected_target_data}, Got: {actual_target_data}"

    # Verify job_control entry
    job_control_entry = get_job_control_entry(job_kennung, eintrags_nr)
    assert job_control_entry is not None, "No job_control entry found."
    assert job_control_entry["status"] == "COMPLETED", f"Job status is not COMPLETED: {job_control_entry['status']}"
    assert "Job completed successfully" in job_control_entry["message"], \
        f"Job control message incorrect: {job_control_entry['message']}"

    # Verify job_audit entry
    job_audit_entry = get_job_audit_entry(job_kennung, eintrags_nr)
    assert job_audit_entry is not None, "No job_audit entry found."
    assert job_audit_entry["status"] == "COMPLETED", f"Audit status is not COMPLETED: {job_audit_entry['status']}"
    assert job_audit_entry["processed_records"] == len(expected_target_data), \
        f"Processed records count mismatch. Expected: {len(expected_target_data)}, Got: {job_audit_entry['processed_records']}"
    assert "Main data processing completed" in job_audit_entry["message"], \
        f"Job audit message incorrect: {job_audit_entry['message']}"

    # Verify no error log entries
    error_log_count = get_table_row_count("job_error_log")
    assert error_log_count == 0, f"Unexpected error log entries: {error_log_count}"

```

**Pass/Fail Criterion:**
*   The `r_ausd_vertrag` procedure completes without raising a `BQEXCEPTION`.
*   The `sof_ta_p_discount_rr` table contains exactly 3 records, matching the expected transformed data.
*   The `job_control` table has one entry for `job_kennung='TEST_JOB_SUCCESS'` and `eintrags_nr=12345` with `status='COMPLETED'`.
*   The `job_audit` table has one entry for the same job, with `status='COMPLETED'` and `processed_records=3`.
*   The `job_error_log` table remains empty.

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the `r_ausd_vertrag` procedure correctly identifies and handles missing required parameters, logging an error and exiting gracefully (raising `BQEXCEPTION` as per BigQuery SP design). This covers transformation correctness (error handling) and external system replacements (error logging).

**Setup:**
1.  Ensure all control tables are empty.

**Action:**
Call `r_ausd_vertrag` with `p_job_kennung` as `NULL` or an empty string, and a valid `p_eintrags_nr`.

```python
from test_utils import setup_base_tables, call_procedure, get_table_row_count, get_job_control_entry, get_job_error_log_entry

def test_parameter_validation_missing_job_kennung():
    setup_base_tables()

    job_kennung_null = None
    job_kennung_empty = ""
    eintrags_nr = 54321

    # Test with NULL job_kennung
    success_null, error_msg_null = call_procedure("r_ausd_vertrag", job_kennung_null, eintrags_nr)
    assert not success_null, f"Procedure unexpectedly succeeded with NULL p_job_kennung: {error_msg_null}"
    assert "Required parameter p_job_kennung is missing or empty" in error_msg_null, \
        f"Error message for NULL p_job_kennung mismatch: {error_msg_null}"

    error_log_entry_null = get_job_error_log_entry(job_kennung_null, eintrags_nr)
    assert error_log_entry_null is not None, "No error log entry found for NULL p_job_kennung."
    assert "Required parameter p_job_kennung is missing or empty" in error_log_entry_null["error_message"]

    job_control_entry_null = get_job_control_entry(job_kennung_null, eintrags_nr)
    assert job_control_entry_null is None, "Unexpected job_control entry for NULL p_job_kennung."
    # The procedure raises BQEXCEPTION and exits before inserting into job_control for this specific error.

    # Clear error log for next test
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`;")

    # Test with empty string job_kennung
    success_empty, error_msg_empty = call_procedure("r_ausd_vertrag", job_kennung_empty, eintrags_nr)
    assert not success_empty, f"Procedure unexpectedly succeeded with empty p_job_kennung: {error_msg_empty}"
    assert "Required parameter p_job_kennung is missing or empty" in error_msg_empty, \
        f"Error message for empty p_job_kennung mismatch: {error_msg_empty}"

    error_log_entry_empty = get_job_error_log_entry(job_kennung_empty, eintrags_nr)
    assert error_log_entry_empty is not None, "No error log entry found for empty p_job_kennung."
    assert "Required parameter p_job_kennung is missing or empty" in error_log_entry_empty["error_message"]

    job_control_entry_empty = get_job_control_entry(job_kennung_empty, eintrags_nr)
    assert job_control_entry_empty is None, "Unexpected job_control entry for empty p_job_kennung."

    # Verify no data was processed
    target_row_count = get_table_row_count("sof_ta_p_discount_rr")
    assert target_row_count == 0, f"Data was processed unexpectedly: {target_row_count} records."

```

**Pass/Fail Criterion:**
*   The `r_ausd_vertrag` procedure call (for both NULL and empty string `p_job_kennung`) results in a `BQEXCEPTION`.
*   The `job_error_log` table contains two entries, each indicating "Required parameter p_job_kennung is missing or empty".
*   The `job_control` table does *not* contain any `STARTING`, `RUNNING`, or `COMPLETED` entries for these runs, as the procedure should exit early.
*   The `sof_ta_p_discount_rr` table remains empty.

---

### Test Case 3: Active Job Ignoring

**Purpose:** Verify the job control mechanism correctly identifies and ignores subsequent calls for an already active job, replicating the "aktive Jobs werden ignoriert" behavior of the legacy script. This covers external system replacements (job control).

**Setup:**
1.  Ensure all control tables are empty.
2.  Manually insert a `RUNNING` entry into `job_control` for a specific `job_kennung` and `eintrags_nr`.

**Action:**
1.  Call `r_ausd_vertrag` with the same `job_kennung` and `eintrags_nr` as the manually inserted `RUNNING` job.
2.  Call `r_ausd_vertrag` with *different* `job_kennung` and `eintrags_nr` to ensure other jobs can run.

```python
from test_utils import setup_base_tables, call_procedure, execute_query, get_job_control_entry, get_table_row_count
import uuid
import time

def test_active_job_ignoring():
    setup_base_tables()

    job_kennung_active = "ACTIVE_JOB"
    eintrags_nr_active = 111
    job_run_id_active = str(uuid.uuid4())

    # 1. Manually insert an 'ACTIVE' job into job_control
    execute_query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_control` (job_run_id, job_name, job_kennung, eintrags_nr, status, start_time, last_updated, message)
        VALUES ('{job_run_id_active}', 'r_ausd_vertrag', '{job_kennung_active}', {eintrags_nr_active}, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'Simulated active job.');
    """)
    print(f"Inserted simulated active job: {job_kennung_active}, {eintrags_nr_active}")

    # 2. Call r_ausd_vertrag with the same parameters (should be ignored)
    success_ignored, error_msg_ignored = call_procedure("r_ausd_vertrag", job_kennung_active, eintrags_nr_active)
    assert success_ignored, f"Ignored job call unexpectedly failed: {error_msg_ignored}" # Should return True, as it exits cleanly

    # Verify the second job was ignored
    ignored_job_entry = get_job_control_entry(job_kennung_active, eintrags_nr_active, status='IGNORED')
    assert ignored_job_entry is not None, "No 'IGNORED' job_control entry found for the second call."
    assert "Job with p_job_kennung" in ignored_job_entry["message"] and "is already active. Ignoring current request." in ignored_job_entry["message"], \
        f"Ignored job message incorrect: {ignored_job_entry['message']}"

    # Verify the original active job's status was not changed
    original_active_job_entry = get_job_control_entry(job_kennung_active, eintrags_nr_active, status='RUNNING')
    assert original_active_job_entry is not None, "Original active job entry not found or status changed."
    assert original_active_job_entry["job_run_id"] == job_run_id_active, "Original active job ID mismatch."

    # Verify no data was processed by the ignored job
    target_row_count = get_table_row_count("sof_ta_p_discount_rr")
    assert target_row_count == 0, f"Data was processed unexpectedly by ignored job: {target_row_count} records."

    # 3. Call r_ausd_vertrag with different parameters (should run successfully)
    job_kennung_new = "NEW_JOB_RUN"
    eintrags_nr_new = 222
    success_new, error_msg_new = call_procedure("r_ausd_vertrag", job_kennung_new, eintrags_nr_new)
    assert success_new, f"New job call failed: {error_msg_new}"

    # Verify the new job completed successfully
    new_job_control_entry = get_job_control_entry(job_kennung_new, eintrags_nr_new, status='COMPLETED')
    assert new_job_control_entry is not None, "New job did not complete successfully."
    assert "Job completed successfully" in new_job_control_entry["message"]

    # Verify data was processed by the new job
    target_row_count_after_new_job = get_table_row_count("sof_ta_p_discount_rr")
    assert target_row_count_after_new_job > 0, "New job did not process any data."

```

**Pass/Fail Criterion:**
*   The first call to `r_ausd_vertrag` (with parameters matching the pre-inserted active job) returns successfully (does not raise `BQEXCEPTION`).
*   The `job_control` table contains a new entry for this call with `status='IGNORED'` and a message indicating it was ignored due to an active job.
*   The original `RUNNING` entry in `job_control` remains unchanged.
*   The `sof_ta_p_discount_rr` table remains empty after the ignored run.
*   The second call to `r_ausd_vertrag` (with different parameters) completes successfully, processes data, and updates `job_control` to `COMPLETED`.

---

### Test Case 4: Data Transformation - No Matching Joins

**Purpose:** Verify the data processing procedure (`d_ausd_v_ta_p_discount_rr`) correctly handles scenarios where no records match the join conditions, resulting in zero insertions. This covers transformation correctness (join logic, NULL handling implicitly) and row count assertions.

**Setup:**
1.  Ensure all control tables and `sof_ta_p_discount_rr` are empty.
2.  Populate `sof_ta_discount_rr` with data that has no matching `cntrct_id` or `cntrct_template_id` in the other source tables.

**Action:**
Call the main orchestration stored procedure `r_ausd_vertrag` with valid parameters.

```python
from test_utils import setup_base_tables, call_procedure, execute_query, get_table_row_count, get_job_audit_entry

def test_data_transformation_no_matching_joins():
    setup_base_tables()

    # Clear source tables and insert data that won't join
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_discount_rr`;")
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs`;")
    execute_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_templ`;")

    execute_query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_discount_rr` VALUES
        ('C_NO_MATCH_1', 'D1', 'TYPEA', 1, 'T_NO_MATCH_1', 'II1', 10.0, 100.0, 'RP1');
    """)
    execute_query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs` VALUES
        ('C_OTHER', 1, 'CN_OTHER');
    """)
    execute_query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_templ` VALUES
        ('T_OTHER', 'Other Template Desc');
    """)

    job_kennung = "TEST_NO_MATCH"
    eintrags_nr = 67890

    success, error_msg = call_procedure("r_ausd_vertrag", job_kennung, eintrags_nr)

    assert success, f"Procedure call failed: {error_msg}"

    # Verify target table is empty
    target_row_count = get_table_row_count("sof_ta_p_discount_rr")
    assert target_row_count == 0, f"Expected 0 records, got {target_row_count}"

    # Verify job_audit records 0 processed records
    job_audit_entry = get_job_audit_entry(job_kennung, eintrags_nr)
    assert job_audit_entry is not None, "No job_audit entry found."
    assert job_audit_entry["status"] == "COMPLETED", f"Audit status is not COMPLETED: {job_audit_entry['status']}"
    assert job_audit_entry["processed_records"] == 0, \
        f"Processed records count mismatch. Expected: 0, Got: {job_audit_entry['processed_records']}"

```

**Pass/Fail Criterion:**
*   The `r_ausd_vertrag` procedure completes without raising a `BQEXCEPTION`.
*   The `sof_ta_p_discount_rr` table remains empty (0 records).
*   The `job_audit` table has one entry for the job with `status='COMPLETED'` and `processed_records=0`.
*   The `job_error_log` table remains empty.

---

### Test Case 5: Error During Data Processing

**Purpose:** Verify that the orchestration procedure correctly handles failures within the data processing procedure (`d_ausd_v_ta_p_discount_rr`), logs the error, and updates the job status to `FAILED`. This covers transformation correctness (error handling) and external system replacements (error logging, job control).

**Setup:**
1.  Ensure all control tables are empty.
2.  Temporarily modify the `d_ausd_v_ta_p_discount_rr` procedure to force an error (e.g., by attempting to insert a string into an INT64 column, or by explicitly adding `RAISE BQEXCEPTION` at the beginning of its `BEGIN` block).
    *   **Note:** For a real test, you might create a temporary version of `d_ausd_v_ta_p_discount_rr` that fails, or simulate a data-related failure by providing malformed input data that violates schema constraints. For this example, we'll assume a direct `RAISE BQEXCEPTION` for simplicity.

**Action:**
Call the main orchestration stored procedure `r_ausd_vertrag` with valid parameters.

```python
from test_utils import setup_base_tables, call_procedure, execute_query, get_table_row_count, get_job_control_entry, get_job_error_log_entry, get_job_audit_entry

# Helper to temporarily modify d_ausd_v_ta_p_discount_rr to fail
def modify_d_ausd_to_fail(should_fail=True):
    if should_fail:
        # Create a failing version of the procedure
        failing_proc_sql = f"""
            CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_p_discount_rr`(
                IN p_job_kennung STRING,
                IN p_eintrags_nr INT64,
                OUT p_processed_records INT64
            )
            OPTIONS(description="Failing version for testing.")
            BEGIN
                RAISE BQEXCEPTION MESSAGE 'Simulated data processing error for testing.';
            END;
        """
        execute_query(failing_proc_sql)
        print("d_ausd_v_ta_p_discount_rr modified to fail.")
    else:
        # Recreate the original procedure (assuming it's in a file or known string)
        # This would typically load from the original SQL file.
        original_proc_sql = """
            CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset_id.d_ausd_v_ta_p_discount_rr`(
                IN p_job_kennung STRING,
                IN p_eintrags_nr INT64,
                OUT p_processed_records INT64
            )
            OPTIONS(
                description="Migrated core data processing logic for ta_p_discount_rr table. Replaces d_ausd_v_ta_p_discount_rr.sql."
            )
            BEGIN
                DECLARE v_records_inserted INT64 DEFAULT 0;
                TRUNCATE TABLE `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_p_discount_rr`;
                INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_p_discount_rr` (
                    cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id,
                    disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos, contract_number, std_vertrag
                )
                SELECT
                    da.cntrct_id, da.discount_id, da.disc_vector_ty, da.cntrct_obj_version, da.cntrct_template_id,
                    da.disc_invoice_item_id, da.rabatt, da.rabatthoehe, da.rabattierte_rech_pos,
                    c.contract_number, ct.cds_description AS std_vertrag
                FROM
                    `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_discount_rr` AS da,
                    `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_cntrct_crs` AS c,
                    `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_cntrct_templ` AS ct
                WHERE
                    da.cntrct_id            = c.cntrct_id
                    AND da.cntrct_obj_version   = c.obj_version
                    AND da.cntrct_template_id   = ct.cntrct_template_id;
                SET v_records_inserted = @@row_count;
                SET p_processed_records = v_records_inserted;
            END;
        """
        # Replace placeholders in the original_proc_sql
        original_proc_sql = original_proc_sql.replace("your_gcp_project_id", PROJECT_ID)
        original_proc_sql = original_proc_sql.replace("your_bigquery_dataset_id", DATASET_ID)
        execute_query(original_proc_sql)
        print("d_ausd_v_ta_p_discount_rr restored to original.")


def test_error_during_data_processing():
    setup_base_tables()
    modify_d_ausd_to_fail(True) # Make d_ausd_v_ta_p_discount_rr fail

    job_kennung = "TEST_FAIL_DP"
    eintrags_nr = 98765

    success, error_msg = call_procedure("r_ausd_vertrag", job_kennung, eintrags_nr)

    assert not success, f"Procedure unexpectedly succeeded despite forced error: {error_msg}"
    assert "Simulated data processing error for testing." in error_msg, \
        f"Error message mismatch: {error_msg}"

    # Verify job_control entry
    job_control_entry = get_job_control_entry(job_kennung, eintrags_nr)
    assert job_control_entry is not None, "No job_control entry found."
    assert job_control_entry["status"] == "FAILED", f"Job status is not FAILED: {job_control_entry['status']}"
    assert "Error during data processing" in job_control_entry["message"], \
        f"Job control message incorrect: {job_control_entry['message']}"

    # Verify job_error_log entry
    error_log_entry = get_job_error_log_entry(job_kennung, eintrags_nr)
    assert error_log_entry is not None, "No job_error_log entry found."
    assert "Simulated data processing error for testing." in error_log_entry["error_message"], \
        f"Error log message incorrect: {error_log_entry['error_message']}"

    # Verify job_audit entry for failure
    job_audit_entry = get_job_audit_entry(job_kennung, eintrags_nr)
    assert job_audit_entry is not None, "No job_audit entry found for failed job."
    assert job_audit_entry["status"] == "FAILED", f"Audit status is not FAILED: {job_audit_entry['status']}"
    assert job_audit_entry["processed_records"] == 0, \
        f"Processed records count mismatch for failed job. Expected: 0, Got: {job_audit_entry['processed_records']}"

    # Verify no data was processed
    target_row_count = get_table_row_count("sof_ta_p_discount_rr")
    assert target_row_count == 0, f"Data was processed unexpectedly: {target_row_count} records."

    modify_d_ausd_to_fail(False) # Restore d_ausd_v_ta_p_discount_rr

```

**Pass/Fail Criterion:**
*   The `r_ausd_vertrag` procedure call results in a `BQEXCEPTION`.
*   The `job_control` table has one entry for the job with `status='FAILED'`.
*   The `job_error_log` table contains one entry for the job, detailing the simulated error.
*   The `job_audit` table has one entry for the job with `status='FAILED'` and `processed_records=0`.
*   The `sof_ta_p_discount_rr` table remains empty.

---

### Test Case 6: Schema and Data Type Integrity of Target Table

**Purpose:** Verify that the target table `sof_ta_p_discount_rr` has the correct schema and data types as implied by the `INSERT` statement in `d_ausd_v_ta_p_discount_rr.sql`. This is a schema assertion.

**Setup:**
N/A (This test directly queries BigQuery metadata).

**Action:**
Query the BigQuery information schema to retrieve the schema of `sof_ta_p_discount_rr`.

```python
from test_utils import PROJECT_ID, DATASET_ID, client

def test_target_table_schema_integrity():
    table_id = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_p_discount_rr"
    table = client.get_table(table_id)

    # Expected schema based on the INSERT statement
    expected_schema = {
        "cntrct_id": "STRING",
        "discount_id": "STRING",
        "disc_vector_ty": "STRING",
        "cntrct_obj_version": "INT64",
        "cntrct_template_id": "STRING",
        "disc_invoice_item_id": "STRING",
        "rabatt": "FLOAT64",
        "rabatthoehe": "FLOAT64",
        "rabattierte_rech_pos": "STRING",
        "contract_number": "STRING",
        "std_vertrag": "STRING"
    }

    actual_schema = {field.name: field.field_type for field in table.schema}

    assert len(actual_schema) == len(expected_schema), \
        f"Schema column count mismatch. Expected {len(expected_schema)}, Got {len(actual_schema)}"

    for col_name, col_type in expected_schema.items():
        assert col_name in actual_schema, f"Missing column in target table: {col_name}"
        assert actual_schema[col_name] == col_type, \
            f"Data type mismatch for column {col_name}. Expected {col_type}, Got {actual_schema[col_name]}"

```

**Pass/Fail Criterion:**
*   The `sof_ta_p_discount_rr` table exists.
*   The schema of `sof_ta_p_discount_rr` exactly matches the expected column names and data types derived from the `INSERT` statement in `d_ausd_v_ta_p_discount_rr.sql`.

---