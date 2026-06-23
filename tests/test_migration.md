The migration of `k_ausd_v_ta_inv_def.ksh` to a BigQuery Stored Procedure `r_ausd_vertrag_control` involves significant changes in technology and control flow. The following tests aim to validate the behavioral equivalence, transformation correctness, and data integrity of the migrated solution.

**Assumptions:**
*   The BigQuery project and dataset (`my_project.my_dataset`) are configured.
*   The DDLs for `job_table`, `job_error_log`, `job_run_log`, and `ta_inv_def_result` have been executed.
*   The core SQL logic from `d_ausd_v_ta_inv_def.sql` has been successfully migrated to the BigQuery Stored Procedure `my_project.my_dataset.d_ausd_v_ta_inv_def` and is assumed to be functionally correct in its data transformation aspects (joins, aggregations, filters, type/NULL handling) when provided with equivalent input data. The tests here focus on its *orchestration* by `r_ausd_vertrag_control`.
*   The source tables (`dwtk_meldungen`, `cds$ta_inv_definition`, `cds$ta_inv_cont_config`, `cds$ta_care_description`) exist in BigQuery and are populated with test data as required.

---

### Test Setup (Python/Pytest)

The following Python code snippet provides helper functions for setting up and asserting conditions in BigQuery, which will be used in the individual test cases.

```python
import pytest
from google.cloud import bigquery
import time
import uuid

# --- Configuration ---
PROJECT_ID = "my_project"
DATASET_ID = "my_dataset"
CONTROL_PROCEDURE = f"{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control"
CORE_SQL_PROCEDURE = f"{PROJECT_ID}.{DATASET_ID}.d_ausd_v_ta_inv_def" # Assumed to be deployed
JOB_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_table"
JOB_ERROR_LOG = f"{PROJECT_ID}.{DATASET_ID}.job_error_log"
JOB_RUN_LOG = f"{PROJECT_ID}.{DATASET_ID}.job_run_log"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.ta_inv_def_result"

# Source tables for d_ausd_v_ta_inv_def
DWTK_MELDUNGEN = f"{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen"
CDS_INV_DEFINITION = f"{PROJECT_ID}.{DATASET_ID}.cds$ta_inv_definition"
CDS_INV_CONT_CONFIG = f"{PROJECT_ID}.{DATASET_ID}.cds$ta_inv_cont_config"
CDS_CARE_DESCRIPTION = f"{PROJECT_ID}.{DATASET_ID}.cds$ta_care_description"

client = bigquery.Client(project=PROJECT_ID)

def _clean_tables():
    """Cleans up all relevant tables before a test."""
    client.query(f"TRUNCATE TABLE `{JOB_TABLE}`").result()
    client.query(f"TRUNCATE TABLE `{JOB_ERROR_LOG}`").result()
    client.query(f"TRUNCATE TABLE `{JOB_RUN_LOG}`").result()
    client.query(f"TRUNCATE TABLE `{TARGET_TABLE}`").result()
    # Clean source tables if they are test-specific, otherwise assume they are static
    client.query(f"TRUNCATE TABLE `{DWTK_MELDUNGEN}`").result()
    client.query(f"TRUNCATE TABLE `{CDS_INV_DEFINITION}`").result()
    client.query(f"TRUNCATE TABLE `{CDS_INV_CONT_CONFIG}`").result()
    client.query(f"TRUNCATE TABLE `{CDS_CARE_DESCRIPTION}`").result()

def _populate_source_data(num_rows_to_insert=1):
    """Populates source tables to ensure d_ausd_v_ta_inv_def can insert data."""
    # This is a simplified mock. In a real scenario, this would be more complex
    # to cover various join conditions and data types.
    client.query(f"""
        INSERT INTO `{DWTK_MELDUNGEN}` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00 UTC');
    """).result()
    for i in range(num_rows_to_insert):
        client.query(f"""
            INSERT INTO `{CDS_INV_DEFINITION}` (inv_definition_id, acc_ref_id, inv_pay_ty_cv, inv_media_cv, billcycle_id, sales_tax_freed, inv_cont_config_id, insert_at, modified_at, valid_from, valid_to, is_production)
            VALUES ({i+1}, 'ACC{i+1}', 'PAY_TYPE_{i}', 'MEDIA_{i}', {100+i}, FALSE, {200+i}, '2023-01-01', NULL, '2023-01-01', NULL, 1);
        """).result()
        client.query(f"""
            INSERT INTO `{CDS_INV_CONT_CONFIG}` (inv_cont_config_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production)
            VALUES ({200+i}, {300+i}, '2023-01-01', NULL, '2023-01-01', NULL, 1);
        """).result()
        client.query(f"""
            INSERT INTO `{CDS_CARE_DESCRIPTION}` (cds_description_id, cds_description)
            VALUES ({300+i}, 'Description {i}');
        """).result()
    return num_rows_to_insert

def _get_table_row_count(table_name):
    """Returns the number of rows in a given table."""
    query = f"SELECT COUNT(*) FROM `{table_name}`"
    return client.query(query).result().total_rows

def _get_job_table_entry(job_kennung, eintrags_nr):
    """Retrieves a specific entry from job_table."""
    query = f"""
        SELECT job_kennung, eintrags_nr, active_flag, last_update_timestamp
        FROM `{JOB_TABLE}`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
    """
    rows = list(client.query(query).result())
    return rows[0] if rows else None

def _get_job_run_log_entries(job_kennung, eintrags_nr):
    """Retrieves entries from job_run_log."""
    query = f"""
        SELECT job_kennung, eintrags_nr, records_processed, status
        FROM `{JOB_RUN_LOG}`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp
    """
    return list(client.query(query).result())

def _get_job_error_log_entries(job_kennung=None, eintrags_nr=None):
    """Retrieves entries from job_error_log."""
    where_clause = []
    if job_kennung is not None:
        where_clause.append(f"job_kennung = '{job_kennung}'")
    if eintrags_nr is not None:
        where_clause.append(f"eintrags_nr = '{eintrags_nr}'")
    
    query = f"""
        SELECT job_kennung, eintrags_nr, error_message
        FROM `{JOB_ERROR_LOG}`
        {'WHERE ' + ' AND '.join(where_clause) if where_clause else ''}
        ORDER BY error_timestamp
    """
    return list(client.query(query).result())

# --- DDL for Job Management Tables (should be run once before tests) ---
# These DDLs are provided in the migration design and should be executed
# as part of the BigQuery environment setup.
#
# CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_table` (
#     job_kennung STRING NOT NULL,
#     eintrags_nr STRING NOT NULL,
#     active_flag BOOLEAN NOT NULL,
#     last_update_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
#     PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
# );
#
# CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_error_log` (
#     error_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
#     job_kennung STRING,
#     eintrags_nr STRING,
#     error_message STRING NOT NULL
# );
#
# CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_run_log` (
#     run_id STRING OPTIONS(description="Unique identifier for each job run"),
#     start_timestamp TIMESTAMP NOT NULL,
#     end_timestamp TIMESTAMP NOT NULL,
#     job_kennung STRING NOT NULL,
#     eintrags_nr STRING NOT NULL,
#     records_processed INT64,
#     status STRING,
#     PRIMARY KEY (run_id) NOT ENFORCED
# );
#
# CREATE TABLE IF NOT EXISTS `my_project.my_dataset.ta_inv_def_result` (
#   inv_definition_id INT64,
#   acc_ref_id STRING,
#   inv_pay_ty_cv STRING,
#   inv_media_cv STRING,
#   billcycle_id INT64,
#   sales_tax_freed BOOLEAN,
#   inv_cont_config_id INT64,
#   rechn_inh_konfig_text STRING
# );
```

---

### Test Case 1: Successful Execution - Happy Path

*   **Purpose:** Verify that the migrated control script executes successfully when all parameters are provided, the core SQL logic runs, and job status and run logs are updated correctly. This covers output parity for job logs and transformation correctness for control flow.
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
    2.  Populate mock data into source tables (`dwtk_meldungen`, `cds$ta_inv_definition`, etc.) such that `d_ausd_v_ta_inv_def` will insert a known number of rows (e.g., 5 rows) into `ta_inv_def_result`.
    3.  No existing entries in `job_table` for the specific `p_JobKennung` and `p_EintragsNr`.
*   **Action:**
    Execute the BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('TEST_JOB_1', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes without error.
    2.  `my_project.my_dataset.job_table` contains one entry: `('TEST_JOB_1', 'ENTRY_001', TRUE, <current_timestamp>)`.
    3.  `my_project.my_dataset.job_run_log` contains one entry with:
        *   `job_kennung = 'TEST_JOB_1'`
        *   `eintrags_nr = 'ENTRY_001'`
        *   `records_processed = 5` (or whatever the mock data yields)
        *   `status = 'SUCCESS'`
        *   `start_timestamp` and `end_timestamp` are populated and `end_timestamp > start_timestamp`.
    4.  `my_project.my_dataset.job_error_log` is empty.
    5.  `my_project.my_dataset.ta_inv_def_result` contains the expected 5 rows, matching the output of the legacy `d_ausd_v_ta_inv_def.sql` script when run with the same input data.

```python
def test_successful_execution():
    _clean_tables()
    expected_rows = _populate_source_data(num_rows_to_insert=5)

    client.query(f"CALL {CONTROL_PROCEDURE}('TEST_JOB_1', 'ENTRY_001');").result()

    # Assert job_table state
    job_entry = _get_job_table_entry('TEST_JOB_1', 'ENTRY_001')
    assert job_entry is not None
    assert job_entry.active_flag is True

    # Assert job_run_log state
    run_logs = _get_job_run_log_entries('TEST_JOB_1', 'ENTRY_001')
    assert len(run_logs) == 1
    assert run_logs[0].status == 'SUCCESS'
    assert run_logs[0].records_processed == expected_rows

    # Assert job_error_log is empty
    error_logs = _get_job_error_log_entries()
    assert len(error_logs) == 0

    # Assert target table content (row count)
    target_row_count = _get_table_row_count(TARGET_TABLE)
    assert target_row_count == expected_rows
    # For full output parity, a golden file comparison would be needed here.
```

---

### Test Case 2: Parameter Validation - Missing JobKennung

*   **Purpose:** Verify that the migrated script correctly handles missing `p_JobKennung` by logging an error and exiting early, mimicking the legacy `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
*   **Action:**
    Execute the BigQuery Stored Procedure with a `NULL` `p_JobKennung`:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control(NULL, 'ENTRY_002');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure returns immediately (or throws a controlled error message).
    2.  `my_project.my_dataset.job_error_log` contains one entry with:
        *   `job_kennung IS NULL`
        *   `eintrags_nr = 'ENTRY_002'`
        *   `error_message` containing 'Missing required parameters'.
    3.  `my_project.my_dataset.job_table` is empty.
    4.  `my_project.my_dataset.job_run_log` contains one entry with `status = 'FAILED'` and `records_processed = NULL`.
    5.  `my_project.my_dataset.ta_inv_def_result` is empty.

```python
def test_missing_job_kennung_parameter():
    _clean_tables()

    client.query(f"CALL {CONTROL_PROCEDURE}(NULL, 'ENTRY_002');").result()

    # Assert job_error_log state
    error_logs = _get_job_error_log_entries(eintrags_nr='ENTRY_002')
    assert len(error_logs) == 1
    assert error_logs[0].job_kennung is None
    assert 'Missing required parameters' in error_logs[0].error_message

    # Assert job_table is empty
    assert _get_table_row_count(JOB_TABLE) == 0

    # Assert job_run_log state (should have a FAILED entry)
    run_logs = _get_job_run_log_entries(job_kennung=None, eintrags_nr='ENTRY_002')
    assert len(run_logs) == 1
    assert run_logs[0].status == 'FAILED'
    assert run_logs[0].records_processed is None

    # Assert target table is empty
    assert _get_table_row_count(TARGET_TABLE) == 0
```

---

### Test Case 3: Parameter Validation - Missing EintragsNr

*   **Purpose:** Verify that the migrated script correctly handles missing `p_EintragsNr` by logging an error and exiting early.
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
*   **Action:**
    Execute the BigQuery Stored Procedure with a `NULL` `p_EintragsNr`:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('TEST_JOB_3', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure returns immediately.
    2.  `my_project.my_dataset.job_error_log` contains one entry with:
        *   `job_kennung = 'TEST_JOB_3'`
        *   `eintrags_nr IS NULL`
        *   `error_message` containing 'Missing required parameters'.
    3.  `my_project.my_dataset.job_table` is empty.
    4.  `my_project.my_dataset.job_run_log` contains one entry with `status = 'FAILED'` and `records_processed = NULL`.
    5.  `my_project.my_dataset.ta_inv_def_result` is empty.

```python
def test_missing_eintrags_nr_parameter():
    _clean_tables()

    client.query(f"CALL {CONTROL_PROCEDURE}('TEST_JOB_3', NULL);").result()

    # Assert job_error_log state
    error_logs = _get_job_error_log_entries(job_kennung='TEST_JOB_3')
    assert len(error_logs) == 1
    assert error_logs[0].eintrags_nr is None
    assert 'Missing required parameters' in error_logs[0].error_message

    # Assert job_table is empty
    assert _get_table_row_count(JOB_TABLE) == 0

    # Assert job_run_log state (should have a FAILED entry)
    run_logs = _get_job_run_log_entries(job_kennung='TEST_JOB_3', eintrags_nr=None)
    assert len(run_logs) == 1
    assert run_logs[0].status == 'FAILED'
    assert run_logs[0].records_processed is None

    # Assert target table is empty
    assert _get_table_row_count(TARGET_TABLE) == 0
```

---

### Test Case 4: Job Management - Deactivate Older Active Jobs

*   **Purpose:** Verify that the `UPDATE` statement correctly deactivates existing active jobs with the same `job_kennung` but different `eintrags_nr`, mimicking the legacy script's behavior of "alte aktive Jobs werden einfach dekativiert".
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
    2.  Insert the following data into `my_project.my_dataset.job_table`:
        *   `('JOB_A', 'ENTRY_X', TRUE, '2023-01-01 10:00:00 UTC')` -- Should be deactivated
        *   `('JOB_A', 'ENTRY_Y', FALSE, '2023-01-01 10:00:00 UTC')` -- Should remain deactivated
        *   `('JOB_B', 'ENTRY_Z', TRUE, '2023-01-01 10:00:00 UTC')` -- Should remain active (different job_kennung)
    3.  Populate mock data for `d_ausd_v_ta_inv_def` to succeed (e.g., 2 rows).
*   **Action:**
    Execute the BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('JOB_A', 'ENTRY_NEW');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes successfully.
    2.  `my_project.my_dataset.job_table` contains:
        *   `('JOB_A', 'ENTRY_X', FALSE, <new_timestamp>)` -- Deactivated
        *   `('JOB_A', 'ENTRY_Y', FALSE, '2023-01-01 10:00:00 UTC')` -- Unchanged
        *   `('JOB_B', 'ENTRY_Z', TRUE, '2023-01-01 10:00:00 UTC')` -- Unchanged
        *   `('JOB_A', 'ENTRY_NEW', TRUE, <current_timestamp>)` -- Newly inserted/updated
    3.  `my_project.my_dataset.job_run_log` has a 'SUCCESS' entry for `('JOB_A', 'ENTRY_NEW')`.
    4.  `my_project.my_dataset.job_error_log` is empty.

```python
def test_deactivate_older_active_jobs():
    _clean_tables()
    client.query(f"""
        INSERT INTO `{JOB_TABLE}` (job_kennung, eintrags_nr, active_flag, last_update_timestamp) VALUES
        ('JOB_A', 'ENTRY_X', TRUE, '2023-01-01 10:00:00 UTC'),
        ('JOB_A', 'ENTRY_Y', FALSE, '2023-01-01 10:00:00 UTC'),
        ('JOB_B', 'ENTRY_Z', TRUE, '2023-01-01 10:00:00 UTC');
    """).result()
    expected_rows = _populate_source_data(num_rows_to_insert=2)

    client.query(f"CALL {CONTROL_PROCEDURE}('JOB_A', 'ENTRY_NEW');").result()

    # Assert job_table states
    job_x = _get_job_table_entry('JOB_A', 'ENTRY_X')
    assert job_x is not None and job_x.active_flag is False and job_x.last_update_timestamp > bigquery.ScalarQueryParameter("TIMESTAMP", "2023-01-01 10:00:00 UTC")
    
    job_y = _get_job_table_entry('JOB_A', 'ENTRY_Y')
    assert job_y is not None and job_y.active_flag is False and str(job_y.last_update_timestamp) == '2023-01-01 10:00:00 UTC' # Should be unchanged

    job_z = _get_job_table_entry('JOB_B', 'ENTRY_Z')
    assert job_z is not None and job_z.active_flag is True and str(job_z.last_update_timestamp) == '2023-01-01 10:00:00 UTC' # Should be unchanged

    job_new = _get_job_table_entry('JOB_A', 'ENTRY_NEW')
    assert job_new is not None and job_new.active_flag is True

    # Assert job_run_log for the new job
    run_logs = _get_job_run_log_entries('JOB_A', 'ENTRY_NEW')
    assert len(run_logs) == 1 and run_logs[0].status == 'SUCCESS' and run_logs[0].records_processed == expected_rows

    assert _get_table_row_count(JOB_ERROR_LOG) == 0
    assert _get_table_row_count(TARGET_TABLE) == expected_rows
```

---

### Test Case 5: Job Management - Update Existing Job Entry

*   **Purpose:** Verify that if a job with the same `job_kennung` and `eintrags_nr` already exists in `job_table`, it is updated (specifically `active_flag` and `last_update_timestamp`) rather than a new entry being created.
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
    2.  Insert the following data into `my_project.my_dataset.job_table`:
        *   `('JOB_B', 'ENTRY_001', FALSE, '2023-01-01 10:00:00 UTC')` -- Existing entry, should be updated to TRUE
    3.  Populate mock data for `d_ausd_v_ta_inv_def` to succeed (e.g., 3 rows).
*   **Action:**
    Execute the BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('JOB_B', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes successfully.
    2.  `my_project.my_dataset.job_table` contains exactly one entry for `('JOB_B', 'ENTRY_001')` with:
        *   `active_flag = TRUE`
        *   `last_update_timestamp` updated to a recent timestamp (later than '2023-01-01 10:00:00 UTC').
    3.  `my_project.my_dataset.job_run_log` has a 'SUCCESS' entry for `('JOB_B', 'ENTRY_001')`.
    4.  `my_project.my_dataset.job_error_log` is empty.

```python
def test_update_existing_job_entry():
    _clean_tables()
    client.query(f"""
        INSERT INTO `{JOB_TABLE}` (job_kennung, eintrags_nr, active_flag, last_update_timestamp)
        VALUES ('JOB_B', 'ENTRY_001', FALSE, '2023-01-01 10:00:00 UTC');
    """).result()
    expected_rows = _populate_source_data(num_rows_to_insert=3)

    client.query(f"CALL {CONTROL_PROCEDURE}('JOB_B', 'ENTRY_001');").result()

    # Assert job_table state
    job_entry = _get_job_table_entry('JOB_B', 'ENTRY_001')
    assert job_entry is not None
    assert job_entry.active_flag is True
    assert job_entry.last_update_timestamp > bigquery.ScalarQueryParameter("TIMESTAMP", "2023-01-01 10:00:00 UTC")

    # Assert job_table has only one entry for this job_kennung/eintrags_nr
    assert _get_table_row_count(JOB_TABLE) == 1

    # Assert job_run_log state
    run_logs = _get_job_run_log_entries('JOB_B', 'ENTRY_001')
    assert len(run_logs) == 1
    assert run_logs[0].status == 'SUCCESS'
    assert run_logs[0].records_processed == expected_rows

    assert _get_table_row_count(JOB_ERROR_LOG) == 0
    assert _get_table_row_count(TARGET_TABLE) == expected_rows
```

---

### Test Case 6: Core SQL Logic Failure and Error Logging

*   **Purpose:** Verify that if the nested `d_ausd_v_ta_inv_def` stored procedure fails, the `r_ausd_vertrag_control` procedure catches the error, logs it to `job_error_log`, and records a 'FAILED' status in `job_run_log`.
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
    2.  Populate `job_table` with `('JOB_FAIL', 'ENTRY_001', FALSE, '2023-01-01 10:00:00 UTC')` to ensure the `MERGE` statement runs successfully before the `CALL` to the failing procedure.
    3.  **Crucially, temporarily replace `my_project.my_dataset.d_ausd_v_ta_inv_def` with a version that explicitly raises an error.**
        ```sql
        CREATE OR REPLACE PROCEDURE `my_project.my_dataset.d_ausd_v_ta_inv_def`()
        BEGIN
            RAISE USING MESSAGE = 'Simulated error in d_ausd_v_ta_inv_def';
        END;
        ```
*   **Action:**
    Execute the BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('JOB_FAIL', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion:**
    1.  The `r_ausd_vertrag_control` procedure completes (the `EXCEPTION` block handles the error).
    2.  `my_project.my_dataset.job_error_log` contains one entry with:
        *   `job_kennung = 'JOB_FAIL'`
        *   `eintrags_nr = 'ENTRY_001'`
        *   `error_message` containing 'Simulated error in d_ausd_v_ta_inv_def'.
    3.  `my_project.my_dataset.job_run_log` contains one entry with:
        *   `job_kennung = 'JOB_FAIL'`
        *   `eintrags_nr = 'ENTRY_001'`
        *   `records_processed = NULL`
        *   `status = 'FAILED'`
    4.  `my_project.my_dataset.job_table` contains `('JOB_FAIL', 'ENTRY_001', TRUE, <current_timestamp>)`. (The `active_flag` is set to TRUE *before* the `CALL` to the core logic, so it remains TRUE even if the core logic fails. This is a behavioral difference from a shell script that might `exit` and not update the job table, and should be confirmed as intended behavior).
    5.  `my_project.my_dataset.ta_inv_def_result` is empty (as the `TRUNCATE` would have happened, but `INSERT` failed).

```python
def test_core_sql_logic_failure_and_error_logging():
    _clean_tables()
    client.query(f"""
        INSERT INTO `{JOB_TABLE}` (job_kennung, eintrags_nr, active_flag, last_update_timestamp)
        VALUES ('JOB_FAIL', 'ENTRY_001', FALSE, '2023-01-01 10:00:00 UTC');
    """).result()

    # Temporarily replace d_ausd_v_ta_inv_def with a failing version
    client.query(f"""
        CREATE OR REPLACE PROCEDURE `{CORE_SQL_PROCEDURE}`()
        BEGIN
            RAISE USING MESSAGE = 'Simulated error in d_ausd_v_ta_inv_def';
        END;
    """).result()

    client.query(f"CALL {CONTROL_PROCEDURE}('JOB_FAIL', 'ENTRY_001');").result()

    # Assert job_error_log state
    error_logs = _get_job_error_log_entries('JOB_FAIL', 'ENTRY_001')
    assert len(error_logs) == 1
    assert 'Simulated error in d_ausd_v_ta_inv_def' in error_logs[0].error_message

    # Assert job_run_log state
    run_logs = _get_job_run_log_entries('JOB_FAIL', 'ENTRY_001')
    assert len(run_logs) == 1
    assert run_logs[0].status == 'FAILED'
    assert run_logs[0].records_processed is None

    # Assert job_table state (active_flag should be TRUE as it was set before the CALL)
    job_entry = _get_job_table_entry('JOB_FAIL', 'ENTRY_001')
    assert job_entry is not None
    assert job_entry.active_flag is True

    # Assert target table is empty (TRUNCATE would have happened, INSERT failed)
    assert _get_table_row_count(TARGET_TABLE) == 0

    # Restore the original d_ausd_v_ta_inv_def procedure (manual step or fixture teardown)
    # This part is crucial for subsequent tests and would typically be handled by pytest fixtures.
    # For this example, assume a mechanism to restore the correct procedure.
```

---

### Test Case 7: Output Parity - Record Count

*   **Purpose:** Verify that the `records_processed` value in `job_run_log` accurately reflects the number of rows inserted by `d_ausd_v_ta_inv_def` into `ta_inv_def_result`. This directly replaces the legacy temporary file mechanism (`eval "v_records=`cat $tmpFile`"`).
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
    2.  Populate source tables such that `d_ausd_v_ta_inv_def` will insert a specific, known number of rows (e.g., 10 rows) into `ta_inv_def_result`.
*   **Action:**
    Execute the BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('COUNT_TEST', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes successfully.
    2.  `my_project.my_dataset.job_run_log` contains one entry for `('COUNT_TEST', 'ENTRY_001')` with `records_processed = 10`.
    3.  `SELECT COUNT(*) FROM my_project.my_dataset.ta_inv_def_result` returns 10.

```python
def test_record_count_parity():
    _clean_tables()
    expected_rows = _populate_source_data(num_rows_to_insert=10)

    client.query(f"CALL {CONTROL_PROCEDURE}('COUNT_TEST', 'ENTRY_001');").result()

    # Assert job_run_log state
    run_logs = _get_job_run_log_entries('COUNT_TEST', 'ENTRY_001')
    assert len(run_logs) == 1
    assert run_logs[0].status == 'SUCCESS'
    assert run_logs[0].records_processed == expected_rows

    # Assert target table row count
    target_row_count = _get_table_row_count(TARGET_TABLE)
    assert target_row_count == expected_rows
```

---

### Test Case 8: Data Quality - Schema and Type Handling

*   **Purpose:** Verify that the schema of `ta_inv_def_result` matches the expected output and that data types are correctly handled during the migration from Oracle to BigQuery. This is primarily a test for the `d_ausd_v_ta_inv_def` procedure, but `r_ausd_vertrag_control` orchestrates its execution.
*   **Setup:**
    1.  Ensure `ta_inv_def_result` exists with the expected BigQuery schema (e.g., `inv_definition_id` as `INT64`, `rechn_inh_konfig_text` as `STRING`).
    2.  Populate source tables with a diverse set of data, including edge cases for data types (e.g., max length strings, zero/negative numbers, dates at boundaries, NULLs).
*   **Action:**
    Execute the BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('SCHEMA_TEST', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes successfully.
    2.  The schema of `my_project.my_dataset.ta_inv_def_result` matches the DDL defined for it.
    3.  A sample of rows (or all rows if feasible) from `ta_inv_def_result` are inspected to ensure:
        *   No data truncation for strings.
        *   Numeric values are correct.
        *   Date/timestamp values are correct.
        *   NULL values are correctly propagated or handled as per transformation logic.
    4.  This would typically involve comparing a snapshot of `ta_inv_def_result` with a golden dataset generated from the legacy system.

```python
def test_data_quality_and_schema_handling():
    _clean_tables()
    _populate_source_data(num_rows_to_insert=1) # Populate with diverse data for schema check

    # Execute the control procedure
    client.query(f"CALL {CONTROL_PROCEDURE}('SCHEMA_TEST', 'ENTRY_001');").result()

    # Assert schema of the target table
    table = client.get_table(TARGET_TABLE)
    schema_fields = {field.name: field.field_type for field in table.schema}
    
    expected_schema = {
        'inv_definition_id': 'INT64',
        'acc_ref_id': 'STRING',
        'inv_pay_ty_cv': 'STRING',
        'inv_media_cv': 'STRING',
        'billcycle_id': 'INT64',
        'sales_tax_freed': 'BOOL',
        'inv_cont_config_id': 'INT64',
        'rechn_inh_konfig_text': 'STRING'
    }
    assert schema_fields == expected_schema

    # For full data quality, retrieve data and compare with expected golden data.
    # Example:
    # query = f"SELECT * FROM `{TARGET_TABLE}` ORDER BY inv_definition_id"
    # rows = list(client.query(query).result())
    # assert len(rows) == 1
    # assert rows[0].inv_definition_id == 1
    # assert rows[0].acc_ref_id == 'ACC1'
    # ... and so on for all columns and expected values.
```

---

### Test Case 9: External System Replacement - Oracle to BigQuery

*   **Purpose:** Confirm that the implicit Oracle interaction (via `sqlplus` wrapper) is correctly replaced by direct BigQuery table reads. This verifies the "External-system replacements" requirement.
*   **Setup:**
    1.  Ensure the BigQuery source tables (`dwtk_meldungen`, `cds$ta_inv_definition`, `cds$ta_inv_cont_config`, `cds$ta_care_description`) are populated.
    2.  Ensure no Oracle connection details are configured or used within the BigQuery environment where the stored procedure runs.
*   **Action:**
    Execute the BigQuery Stored Procedure:
    ```sql
    CALL my_project.my_dataset.r_ausd_vertrag_control('EXTERNAL_TEST', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure completes successfully.
    2.  No errors related to Oracle connectivity are observed in BigQuery logs.
    3.  BigQuery audit logs (accessible via Cloud Logging) confirm reads from the specified BigQuery source tables (`cds$ta_inv_definition`, etc.) and writes to `ta_inv_def_result`. The absence of `sqlplus` or Oracle client calls in the execution environment is key.
    4.  The data in `ta_inv_def_result` is consistent with the data in the BigQuery source tables, confirming the transformation logic used BigQuery sources.

```python
def test_external_system_replacement():
    _clean_tables()
    expected_rows = _populate_source_data(num_rows_to_insert=1)

    # Execute the control procedure
    client.query(f"CALL {CONTROL_PROCEDURE}('EXTERNAL_TEST', 'ENTRY_001');").result()

    # Assert successful completion and data in target table
    assert _get_table_row_count(TARGET_TABLE) == expected_rows
    run_logs = _get_job_run_log_entries('EXTERNAL_TEST', 'ENTRY_001')
    assert len(run_logs) == 1 and run_logs[0].status == 'SUCCESS'

    # Manual/Observational Check:
    # 1. Review BigQuery audit logs in Cloud Logging for the procedure execution.
    #    Look for 'tabledata.list' (reads) and 'tabledata.insertAll' (writes) operations
    #    on the BigQuery source and target tables.
    # 2. Confirm absence of any external system connection attempts (e.g., Oracle JDBC/SQL*Net)
    #    in the BigQuery execution environment logs. This is more of an architectural
    #    validation than a direct test assertion.
```

---

### Test Case 10: Concurrency / Idempotency

*   **Purpose:** Verify the behavior when the control script is called multiple times, especially with the same parameters, to ensure job management logic is robust and handles concurrent/repeated calls correctly.
*   **Setup:**
    1.  Clean all relevant tables using `_clean_tables()`.
    2.  Populate source tables for `d_ausd_v_ta_inv_def` to insert a known number of rows (e.g., 5 rows).
*   **Action:**
    1.  `CALL my_project.my_dataset.r_ausd_vertrag_control('CONCUR_JOB', 'ENTRY_001');`
    2.  Immediately after (or with a short delay), `CALL my_project.my_dataset.r_ausd_vertrag_control('CONCUR_JOB', 'ENTRY_001');`
*   **Pass/Fail Criterion:**
    1.  Both procedure calls complete successfully.
    2.  `my_project.my_dataset.job_table` contains exactly one entry for `('CONCUR_JOB', 'ENTRY_001')` with `active_flag = TRUE` and `last_update_timestamp` reflecting the *second* run's timestamp.
    3.  `my_project.my_dataset.job_run_log` contains *two* entries for `('CONCUR_JOB', 'ENTRY_001')`, both with `status = 'SUCCESS'` and `records_processed = 5`.
    4.  `my_project.my_dataset.ta_inv_def_result` contains 5 rows. (Since `d_ausd_v_ta_inv_def` uses `TRUNCATE` then `INSERT`, the second run will clear and re-insert, resulting in the same final state).

```python
def test_concurrency_idempotency():
    _clean_tables()
    expected_rows = _populate_source_data(num_rows_to_insert=5)

    # First call
    client.query(f"CALL {CONTROL_PROCEDURE}('CONCUR_JOB', 'ENTRY_001');").result()
    time.sleep(1) # Simulate a slight delay before second call

    # Second call
    client.query(f"CALL {CONTROL_PROCEDURE}('CONCUR_JOB', 'ENTRY_001');").result()

    # Assert job_table state
    job_entry = _get_job_table_entry('CONCUR_JOB', 'ENTRY_001')
    assert job_entry is not None
    assert job_entry.active_flag is True
    # The timestamp should be from the second run, so it's recent
    assert job_entry.last_update_timestamp > bigquery.ScalarQueryParameter("TIMESTAMP", "2023-01-01 10:00:00 UTC")

    # Assert job_table has only one entry for this job_kennung/eintrags_nr
    assert _get_table_row_count(JOB_TABLE) == 1

    # Assert job_run_log state (two entries)
    run_logs = _get_job_run_log_entries('CONCUR_JOB', 'ENTRY_001')
    assert len(run_logs) == 2
    assert all(log.status == 'SUCCESS' for log in run_logs)
    assert all(log.records_processed == expected_rows for log in run_logs)
    assert run_logs[1].start_timestamp > run_logs[0].start_timestamp # Second run started after first

    # Assert target table row count (should be the same as TRUNCATE+INSERT)
    target_row_count = _get_table_row_count(TARGET_TABLE)
    assert target_row_count == expected_rows
```