The migration of `k_ausd_v_ta_cntrct_crs2.ksh` to BigQuery involves re-platforming a KornShell orchestrator and its underlying SQL logic into BigQuery Stored Procedures. The following tests aim to validate this migration across various aspects, ensuring behavioral equivalence and data integrity.

**Assumptions for Testing:**

*   **Placeholder Logic:** Since the detailed SQL logic for `d_ausd_v_ta_cntrct_crs2.sql` and Oracle packages (`DWPA_UTIL_SKRIPT`, `CR`) was not provided, the tests for `sp_d_ausd_v_ta_cntrct_crs2` will be based on the placeholder logic provided in the generated code. A true output parity test would require a detailed understanding and execution of the original Oracle SQL. For the purpose of these tests, we will define a "known good" output based on the placeholder logic.
*   **Legacy System Access:** It's assumed that for "Output Parity" tests, a mechanism exists to run the legacy KornShell script and extract its output from the Oracle database for comparison. For the code examples, we will simulate the "legacy output" based on the assumed transformation.
*   **BigQuery Environment:** A BigQuery project and dataset (`bq_dataset`) are assumed to be configured, and the DDLs and Stored Procedures provided in the generated code have been deployed.
*   **Pytest/Python Client:** The test code examples use `pytest` and the `google-cloud-bigquery` Python client library for interaction with BigQuery. Helper functions are conceptual and would need full implementation.

---

## Helper Functions (Conceptual Python for Pytest)

```python
import os
from google.cloud import bigquery
import uuid
import time
from datetime import datetime, timezone

# Assume BQ_PROJECT and BQ_DATASET are set as environment variables or config
BQ_PROJECT = os.getenv("BQ_PROJECT", "your-gcp-project-id")
BQ_DATASET = os.getenv("BQ_DATASET", "bq_dataset")

client = bigquery.Client(project=BQ_PROJECT)

def execute_bq_query(query: str):
    """Executes a BigQuery SQL query and returns results."""
    print(f"\nExecuting BQ Query:\n{query}")
    query_job = client.query(query)
    return query_job.result()

def call_bq_stored_procedure(sp_name: str, params: dict = None):
    """Calls a BigQuery stored procedure and returns its output (if any)."""
    param_str = ""
    if params:
        param_list = []
        for k, v in params.items():
            if isinstance(v, str):
                param_list.append(f"{k} => '{v}'")
            elif isinstance(v, bool):
                param_list.append(f"{k} => {str(v).upper()}")
            elif isinstance(v, int):
                param_list.append(f"{k} => {v}")
            elif v is None:
                param_list.append(f"{k} => NULL")
            else: # For other types, assume direct string representation is fine or needs custom handling
                param_list.append(f"{k} => {v}")
        param_str = ", ".join(param_list)
    
    query = f"CALL `{BQ_PROJECT}.{BQ_DATASET}.{sp_name}`({param_str});"
    print(f"\nCalling BQ Stored Procedure:\n{query}")
    query_job = client.query(query)
    # For procedures with OUT parameters, you'd need to parse the results differently
    # For simplicity, this assumes the procedure might print messages or just execute.
    return query_job.result()

def get_table_data(table_name: str):
    """Fetches all data from a BigQuery table, ordered for consistent comparison."""
    query = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{table_name}` ORDER BY 1, 2, 3;" # Order by multiple columns for robustness
    return [dict(row) for row in execute_bq_query(query)]

def truncate_table(table_name: str):
    """Truncates a BigQuery table."""
    query = f"TRUNCATE TABLE `{BQ_PROJECT}.{BQ_DATASET}.{table_name}`;"
    execute_bq_query(query)

def insert_data(table_name: str, columns: list, values: list[tuple]):
    """Inserts data into a BigQuery table."""
    if not values:
        return
    cols_str = ", ".join([f"`{c}`" for c in columns])
    values_str_list = []
    for row_values in values:
        formatted_row = []
        for val in row_values:
            if isinstance(val, str):
                formatted_row.append(f"'{val}'")
            elif isinstance(val, datetime):
                formatted_row.append(f"TIMESTAMP('{val.isoformat()}')")
            elif val is None:
                formatted_row.append("NULL")
            else:
                formatted_row.append(str(val))
        values_str_list.append(f"({', '.join(formatted_row)})")
    
    query = f"INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.{table_name}` ({cols_str}) VALUES {', '.join(values_str_list)};"
    execute_bq_query(query)

def get_job_run_log_entry(run_id: str):
    """Fetches a specific job run log entry."""
    query = f"""
    SELECT run_id, job_kennung, eintrags_nr, status, records_processed, error_message
    FROM `{BQ_PROJECT}.{BQ_DATASET}.job_run_log`
    WHERE run_id = '{run_id}';
    """
    result = list(execute_bq_query(query))
    return dict(result[0]) if result else None

def set_job_status(job_kennung: str, status: str):
    """Helper to set job status in job_table."""
    truncate_table("job_table") # Ensure clean state for job_table
    insert_data("job_table", 
                ["job_kennung", "job_description", "status", "last_update_time", "updated_by"],
                [(job_kennung, f"Description for {job_kennung}", status, datetime.now(timezone.utc), "test_setup")])

```

---

## 1. Output Parity Tests

### Test Case 1.1: Successful Execution - Record Count & Target Data Parity

*   **Purpose:** Verify that a successful run of the migrated job (`control_k_ausd_v_ta_cntrct_crs2`) produces the same record count and identical data in target tables (`sof_ta_cntrct_crs2`, `via`) as the legacy job, given the same initial state. This test assumes the placeholder logic in `sp_d_ausd_v_ta_cntrct_crs2` is the "known good" transformation.
*   **Setup:**
    1.  Clear target tables: `bq_dataset.sof_ta_cntrct_crs2`, `bq_dataset.via`, `bq_dataset.job_run_log`.
    2.  Populate `bq_dataset.sof_ta_cntrct_crs` with a known set of input data.
    3.  Set `bq_dataset.job_table` for `TA_CNTRCT_CRS2` to 'ACTIVE'.
    4.  *(Legacy System)*: Populate Oracle `SOF$TA_CNTRCT_CRS` with identical data.
*   **Action:**
    1.  *(Legacy System)*: Execute `k_ausd_v_ta_cntrct_crs2.ksh -j TA_CNTRCT_CRS2 -f 12345`.
    2.  *(Legacy System)*: Extract data from Oracle `SOF$TA_CNTRCT_CRS2` and `VIA`, and capture the reported record count.
    3.  Execute migrated BigQuery Stored Procedure: `CALL bq_dataset.control_k_ausd_v_ta_cntrct_crs2('TA_CNTRCT_CRS2', '12345');`
    4.  Retrieve data from BigQuery `bq_dataset.sof_ta_cntrct_crs2`, `bq_dataset.via`, and the `records_processed` from `bq_dataset.job_run_log`.
*   **Pass/Fail Criterion:**
    *   The `records_processed` value from `job_run_log` matches the record count reported by the legacy job.
    *   The data in BigQuery `bq_dataset.sof_ta_cntrct_crs2` is identical to the data extracted from Oracle `SOF$TA_CNTRCT_CRS2`.
    *   The data in BigQuery `bq_dataset.via` is identical to the data extracted from Oracle `VIA`.

*   **Runnable Test Code (Pytest / SQL Assertions):**

    ```python
    import pytest
    from datetime import datetime, timezone

    def test_successful_execution_output_parity():
        job_kennung = "TA_CNTRCT_CRS2"
        eintrags_nr = "12345"
        
        # --- Setup ---
        truncate_table("sof_ta_cntrct_crs")
        truncate_table("sof_ta_cntrct_crs2")
        truncate_table("via")
        truncate_table("job_run_log")
        set_job_status(job_kennung, "ACTIVE")

        # Sample input data for sof_ta_cntrct_crs
        input_data = [
            ("CNTRCT001", "CRS_A", datetime(2023, 1, 1, tzinfo=timezone.utc).date(), datetime(2024, 1, 1, tzinfo=timezone.utc).date(), datetime(2023, 1, 15, tzinfo=timezone.utc)),
            ("CNTRCT002", "CRS_B", datetime(2023, 2, 1, tzinfo=timezone.utc).date(), datetime(2024, 2, 1, tzinfo=timezone.utc).date(), datetime(2023, 2, 15, tzinfo=timezone.utc)),
        ]
        insert_data("sof_ta_cntrct_crs", 
                    ["cntrct_id", "crs_code", "start_date", "end_date", "load_date"],
                    input_data)

        # --- Simulate Legacy Output (based on placeholder logic) ---
        # Assuming legacy also inserts 2 records and logs 2 records
        expected_legacy_records_processed = 2
        expected_sof_ta_cntrct_crs2_legacy = [
            {'cntrct_id': 'CNTRCT001', 'crs_code_new': 'CRS_A_NEW', 'status': 'UPDATED', 'processed_date': None}, # processed_date will be dynamic
            {'cntrct_id': 'CNTRCT002', 'crs_code_new': 'CRS_B_NEW', 'status': 'UPDATED', 'processed_date': None},
        ]
        # For VIA, we expect two entries, content will be dynamic (UUID, timestamp)
        expected_via_legacy_count = 2

        # --- Action (Migrated) ---
        # Call the main control procedure
        # Note: For OUT parameters, BigQuery client library might require specific handling
        # For simplicity, we'll check job_run_log for results.
        run_uuid = str(uuid.uuid4()) # Simulate run_id generation for logging
        call_bq_stored_procedure(
            "control_k_ausd_v_ta_cntrct_crs2",
            {"p_job_kennung": job_kennung, "p_eintrags_nr": eintrags_nr}
        )
        
        # Retrieve the actual run_id from job_run_log based on job_kennung and eintrags_nr
        # (This is a workaround as control_k_ausd_v_ta_cntrct_crs2 doesn't return run_id)
        log_entries = list(execute_bq_query(f"""
            SELECT run_id, records_processed, status FROM `{BQ_PROJECT}.{BQ_DATASET}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY start_time DESC LIMIT 1
        """))
        assert log_entries, "No log entry found for the job run."
        migrated_run_id = log_entries[0].run_id
        migrated_records_processed = log_entries[0].records_processed
        migrated_status = log_entries[0].status

        migrated_sof_ta_cntrct_crs2_data = get_table_data("sof_ta_cntrct_crs2")
        migrated_via_data = get_table_data("via")

        # --- Pass/Fail Criterion ---
        assert migrated_status == "SUCCESS", f"Migrated job failed with status: {migrated_status}"
        assert migrated_records_processed == expected_legacy_records_processed, \
            f"Record count mismatch. Expected: {expected_legacy_records_processed}, Got: {migrated_records_processed}"

        # Compare sof_ta_cntrct_crs2 data (ignoring dynamic processed_date)
        assert len(migrated_sof_ta_cntrct_crs2_data) == len(expected_sof_ta_cntrct_crs2_legacy)
        for i in range(len(migrated_sof_ta_cntrct_crs2_data)):
            assert migrated_sof_ta_cntrct_crs2_data[i]['cntrct_id'] == expected_sof_ta_cntrct_crs2_legacy[i]['cntrct_id']
            assert migrated_sof_ta_cntrct_crs2_data[i]['crs_code_new'] == expected_sof_ta_cntrct_crs2_legacy[i]['crs_code_new']
            assert migrated_sof_ta_cntrct_crs2_data[i]['status'] == expected_sof_ta_cntrct_crs2_legacy[i]['status']
            # processed_date is dynamic, so we only check it's not null
            assert migrated_sof_ta_cntrct_crs2_data[i]['processed_date'] is not None

        # Compare VIA data (check count and general structure, content is dynamic)
        assert len(migrated_via_data) == expected_via_legacy_count
        for entry in migrated_via_data:
            assert entry['entry_id'].startswith(job_kennung)
            assert "Processed" in entry['message']
            assert entry['log_time'] is not None

    ```

---

## 2. Transformation Correctness Tests

*Disclaimer: These tests are based on the placeholder logic in `sp_d_ausd_v_ta_cntrct_crs2` and general BigQuery behavior, as the original SQL was not provided.*

### Test Case 2.1: Core Transformation - Basic Data Flow & Record Count

*   **Purpose:** Verify that `sp_d_ausd_v_ta_cntrct_crs2` correctly reads from `sof_ta_cntrct_crs`, performs the placeholder transformation (inserting new records, updating existing ones), writes to `sof_ta_cntrct_crs2` and `via`, and returns the accurate count of processed records.
*   **Setup:**
    1.  Clear target tables: `bq_dataset.sof_ta_cntrct_crs2`, `bq_dataset.via`.
    2.  Populate `bq_dataset.sof_ta_cntrct_crs` with sample data.
    3.  Pre-populate `bq_dataset.sof_ta_cntrct_crs2` with some data to test updates.
*   **Action:** Call `bq_dataset.sp_d_ausd_v_ta_cntrct_crs2` with dummy parameters and capture the `p_records_processed` output.
*   **Pass/Fail Criterion:**
    *   The `p_records_processed` value matches the expected number of inserts + updates based on the placeholder logic and setup data.
    *   `bq_dataset.sof_ta_cntrct_crs2` contains the expected new and updated records.
    *   `bq_dataset.via` contains log entries corresponding to the processed records.

*   **Runnable Test Code (Pytest / SQL Assertions):**

    ```python
    import pytest
    from datetime import datetime, timezone

    def test_sp_d_ausd_v_ta_cntrct_crs2_basic_data_flow():
        job_kennung = "TEST_JOB"
        eintrags_nr = "999"
        
        # --- Setup ---
        truncate_table("sof_ta_cntrct_crs")
        truncate_table("sof_ta_cntrct_crs2")
        truncate_table("via")

        # Input data: 2 new, 1 existing (to be updated by placeholder logic)
        insert_data("sof_ta_cntrct_crs", 
                    ["cntrct_id", "crs_code", "start_date", "end_date", "load_date"],
                    [
                        ("NEW_CNTRCT_1", "CRS_X", datetime(2023, 1, 1).date(), datetime(2024, 1, 1).date(), datetime(2023, 1, 15, tzinfo=timezone.utc)),
                        ("NEW_CNTRCT_2", "CRS_Y", datetime(2023, 2, 1).date(), datetime(2024, 2, 1).date(), datetime(2023, 2, 15, tzinfo=timezone.utc)),
                        ("EXISTING_CNTRCT", "CRS_Z", datetime(2023, 3, 1).date(), datetime(2024, 3, 1).date(), datetime(2023, 3, 15, tzinfo=timezone.utc)),
                    ])
        
        # Pre-populate sof_ta_cntrct_crs2 with one record that will be "updated" by the placeholder logic
        insert_data("sof_ta_cntrct_crs2",
                    ["cntrct_id", "crs_code_new", "status", "processed_date"],
                    [("EXISTING_CNTRCT", "CRS_Z_OLD", "INITIAL", datetime(2022, 1, 1, tzinfo=timezone.utc))])

        # --- Action ---
        # Call the core transformation SP directly
        # Note: BigQuery SPs with OUT parameters need special handling in Python client.
        # For this example, we'll assume a way to capture the OUT param or query it from logs.
        # The provided SP code doesn't directly return it, but the control SP captures it.
        # For direct testing, we'd modify sp_d_ausd_v_ta_cntrct_crs2 to return it or use a temp table.
        # Let's assume a wrapper or direct query for the OUT param for this test.
        # A more robust way would be to call control_k_ausd_v_ta_cntrct_crs2 and check job_run_log.
        
        # For direct SP testing, we'd need to declare a variable for OUT param:
        # query = f"""
        #     DECLARE records_processed INT64;
        #     CALL `{BQ_PROJECT}.{BQ_DATASET}.sp_d_ausd_v_ta_cntrct_crs2`('{job_kennung}', '{eintrags_nr}', records_processed);
        #     SELECT records_processed;
        # """
        # result = list(execute_bq_query(query))
        # actual_records_processed = result[0].records_processed if result else 0

        # Based on the placeholder logic:
        # 1 insert (first record from sof_ta_cntrct_crs)
        # 1 update (the pre-existing record in sof_ta_cntrct_crs2)
        # Total expected processed: 2
        
        # Let's call the control SP to get the count from job_run_log for simplicity
        set_job_status(job_kennung, "ACTIVE") # Ensure job is active for control SP
        call_bq_stored_procedure(
            "control_k_ausd_v_ta_cntrct_crs2",
            {"p_job_kennung": job_kennung, "p_eintrags_nr": eintrags_nr}
        )
        log_entry = list(execute_bq_query(f"""
            SELECT records_processed, status FROM `{BQ_PROJECT}.{BQ_DATASET}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY start_time DESC LIMIT 1
        """))[0]
        actual_records_processed = log_entry.records_processed
        
        # --- Pass/Fail Criterion ---
        expected_records_processed = 2 # 1 insert + 1 update from placeholder logic
        assert actual_records_processed == expected_records_processed, \
            f"Expected {expected_records_processed} records processed, but got {actual_records_processed}"

        # Verify sof_ta_cntrct_crs2 content
        target_data = get_table_data("sof_ta_cntrct_crs2")
        assert len(target_data) == 2 # 1 new insert, 1 updated
        
        # Check the inserted record
        inserted_record = next((r for r in target_data if r['cntrct_id'] == 'NEW_CNTRCT_1'), None)
        assert inserted_record is not None
        assert inserted_record['crs_code_new'] == 'CRS_X_NEW'
        assert inserted_record['status'] == 'PROCESSED'
        assert inserted_record['processed_date'] is not None

        # Check the updated record
        updated_record = next((r for r in target_data if r['cntrct_id'] == 'EXISTING_CNTRCT'), None)
        assert updated_record is not None
        assert updated_record['crs_code_new'] == 'CRS_Z_OLD' # Placeholder logic doesn't change this
        assert updated_record['status'] == 'UPDATED'
        assert updated_record['processed_date'] is not None

        # Verify via content
        via_data = get_table_data("via")
        assert len(via_data) == expected_records_processed
        assert all(entry['entry_id'].startswith(job_kennung) for entry in via_data)
        assert all("Processed" in entry['message'] for entry in via_data)
    ```

### Test Case 2.2: Transformation - NULL Handling

*   **Purpose:** Verify how `sp_d_ausd_v_ta_cntrct_crs2` handles NULL values in source columns, especially for columns that are NOT NULL in target tables or used in transformations.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate `bq_dataset.sof_ta_cntrct_crs` with records where `crs_code` (used in `crs_code || '_NEW'`) is NULL.
*   **Action:** Call `bq_dataset.sp_d_ausd_v_ta_cntrct_crs2` (via `control_k_ausd_v_ta_cntrct_crs2`).
*   **Pass/Fail Criterion:**
    *   No unexpected errors are raised due to NULLs.
    *   The `crs_code_new` column in `sof_ta_cntrct_crs2` correctly reflects the concatenation with NULL (e.g., `NULL` if `crs_code` is NULL in BigQuery).
    *   The `records_processed` count is accurate.

*   **Runnable Test Code (Pytest / SQL Assertions):**

    ```python
    import pytest
    from datetime import datetime, timezone

    def test_sp_d_ausd_v_ta_cntrct_crs2_null_handling():
        job_kennung = "NULL_TEST_JOB"
        eintrags_nr = "100"

        # --- Setup ---
        truncate_table("sof_ta_cntrct_crs")
        truncate_table("sof_ta_cntrct_crs2")
        truncate_table("via")
        truncate_table("job_run_log")
        set_job_status(job_kennung, "ACTIVE")

        # Input data with NULL crs_code
        insert_data("sof_ta_cntrct_crs", 
                    ["cntrct_id", "crs_code", "start_date", "end_date", "load_date"],
                    [
                        ("CNTRCT_NULL_CRS", None, datetime(2023, 1, 1).date(), datetime(2024, 1, 1).date(), datetime(2023, 1, 15, tzinfo=timezone.utc)),
                        ("CNTRCT_VALID_CRS", "VALID", datetime(2023, 2, 1).date(), datetime(2024, 2, 1).date(), datetime(2023, 2, 15, tzinfo=timezone.utc)),
                    ])
        
        # --- Action ---
        call_bq_stored_procedure(
            "control_k_ausd_v_ta_cntrct_crs2",
            {"p_job_kennung": job_kennung, "p_eintrags_nr": eintrags_nr}
        )
        
        log_entry = get_job_run_log_entry(
            list(execute_bq_query(f"""
                SELECT run_id FROM `{BQ_PROJECT}.{BQ_DATASET}.job_run_log`
                WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
                ORDER BY start_time DESC LIMIT 1
            """))[0].run_id
        )
        
        # --- Pass/Fail Criterion ---
        assert log_entry['status'] == "SUCCESS", f"Job failed: {log_entry['error_message']}"
        assert log_entry['records_processed'] == 2 # Both records should be processed (inserted)

        target_data = get_table_data("sof_ta_cntrct_crs2")
        assert len(target_data) == 2

        null_crs_record = next((r for r in target_data if r['cntrct_id'] == 'CNTRCT_NULL_CRS'), None)
        assert null_crs_record is not None
        assert null_crs_record['crs_code_new'] is None # BigQuery: NULL || '_NEW' results in NULL
        assert null_crs_record['status'] == 'PROCESSED'

        valid_crs_record = next((r for r in target_data if r['cntrct_id'] == 'CNTRCT_VALID_CRS'), None)
        assert valid_crs_record is not None
        assert valid_crs_record['crs_code_new'] == 'VALID_NEW'
    ```

---

## 3. External-System Replacements Tests

### Test Case 3.1: Job Activation Check

*   **Purpose:** Verify that the `control_k_ausd_v_ta_cntrct_crs2` procedure correctly uses `sp_job_prepare` to check if the job is active and aborts if it's not, mirroring the legacy script's implied behavior.
*   **Setup:**
    1.  Clear `bq_dataset.job_run_log`.
    2.  Set `bq_dataset.job_table` for `TA_CNTRCT_CRS2` to 'INACTIVE'.
*   **Action:** Call `bq_dataset.control_k_ausd_v_ta_cntrct_crs2('TA_CNTRCT_CRS2', '12345');`
*   **Pass/Fail Criterion:**
    *   The procedure call raises an error indicating the job is inactive.
    *   An entry is recorded in `bq_dataset.job_run_log` with `status = 'FAILED'` and an appropriate `error_message`.

*   **Runnable Test Code (Pytest / SQL Assertions):**

    ```python
    import pytest

    def test_job_activation_check_inactive_job():
        job_kennung = "TA_CNTRCT_CRS2"
        eintrags_nr = "12345"

        # --- Setup ---
        truncate_table("job_run_log")
        set_job_status(job_kennung, "INACTIVE")

        # --- Action ---
        with pytest.raises(Exception) as excinfo:
            call_bq_stored_procedure(
                "control_k_ausd_v_ta_cntrct_crs2",
                {"p_job_kennung": job_kennung, "p_eintrags_nr": eintrags_nr}
            )
        
        # Retrieve the actual run_id from job_run_log based on job_kennung and eintrags_nr
        log_entry = list(execute_bq_query(f"""
            SELECT run_id, status, error_message FROM `{BQ_PROJECT}.{BQ_DATASET}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY start_time DESC LIMIT 1
        """))[0]

        # --- Pass/Fail Criterion ---
        assert "Job TA_CNTRCT_CRS2 is not active. Aborting." in str(excinfo.value)
        assert log_entry.status == "FAILED"
        assert "Job TA_CNTRCT_CRS2 is not active. Aborting." in log_entry.error_message
    ```

### Test Case 3.2: Error Handling and Logging

*   **Purpose:** Verify that errors originating from `sp_d_ausd_v_ta_cntrct_crs2` are correctly caught by `control_k_ausd_v_ta_cntrct_crs2`, logged to `job_run_log` with `status='FAILED'`, and propagated as a `RAISE` statement. This replaces the shell's `DWMSG_MeldeFehler` and exit codes.
*   **Setup:**
    1.  Clear `bq_dataset.job_run_log`.
    2.  Set `bq_dataset.job_table` for `ERROR_JOB` to 'ACTIVE'.
    3.  Modify `sp_d_ausd_v_ta_cntrct_crs2` temporarily to force an error (e.g., `RAISE USING MESSAGE = 'Simulated error in core logic';`).
*   **Action:** Call `bq_dataset.control_k_ausd_v_ta_cntrct_crs2('ERROR_JOB', '54321');`
*   **Pass/Fail Criterion:**
    *   The procedure call raises an error containing the simulated error message.
    *   An entry is recorded in `bq_dataset.job_run_log` with `status = 'FAILED'` and the `error_message` matching the simulated error.

*   **Runnable Test Code (Pytest / SQL Assertions):**

    ```python
    import pytest

    # --- Pre-requisite: Temporarily modify sp_d_ausd_v_ta_cntrct_crs2 to raise an error ---
    # This would be done via a setup fixture or direct DDL execution before the test.
    # Example modification:
    # CREATE OR REPLACE PROCEDURE `bq_dataset.sp_d_ausd_v_ta_cntrct_crs2`(
    #     IN p_job_kennung STRING, IN p_eintrags_nr STRING, OUT p_records_processed INT64
    # )
    # BEGIN
    #     RAISE USING MESSAGE = 'Simulated error in core logic for testing.';
    #     -- Original logic would follow here
    # END;
    # --- End Pre-requisite ---

    def test_error_handling_core_logic_failure():
        job_kennung = "ERROR_JOB"
        eintrags_nr = "54321"
        simulated_error_message = "Simulated error in core logic for testing."

        # --- Setup ---
        truncate_table("job_run_log")
        set_job_status(job_kennung, "ACTIVE")

        # --- Action ---
        with pytest.raises(Exception) as excinfo:
            call_bq_stored_procedure(
                "control_k_ausd_v_ta_cntrct_crs2",
                {"p_job_kennung": job_kennung, "p_eintrags_nr": eintrags_nr}
            )
        
        # Retrieve the actual run_id from job_run_log
        log_entry = list(execute_bq_query(f"""
            SELECT run_id, status, error_message FROM `{BQ_PROJECT}.{BQ_DATASET}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY start_time DESC LIMIT 1
        """))[0]

        # --- Pass/Fail Criterion ---
        assert simulated_error_message in str(excinfo.value)
        assert log_entry.status == "FAILED"
        assert simulated_error_message in log_entry.error_message

    # --- Post-requisite: Revert sp_d_ausd_v_ta_cntrct_crs2 to its original (non-erroring) state ---
    # This would be done via a teardown fixture or direct DDL execution after the test.
    ```

---

## 4. Data Quality / Row Count / Schema Assertions

### Test Case 4.1: Target Table Schema Conformance

*   **Purpose:** Verify that the DDLs for target tables (`sof_ta_cntrct_crs2`, `via`, `job_table`, `job_run_log`) match the expected schema, column names, data types, and nullability constraints as defined in the migration design and generated DDLs.
*   **Setup:** Ensure all DDLs have been deployed to the BigQuery dataset.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for table and column details for each target table.
*   **Pass/Fail Criterion:**
    *   Each table exists.
    *   Column names, data types, and `is_nullable` properties for each column match the specifications in the provided DDLs.

*   **Runnable Test Code (Pytest / SQL Assertions):**

    ```python
    import pytest

    def get_bq_table_schema(table_name: str):
        """Fetches table schema from INFORMATION_SCHEMA."""
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        return [dict(row) for row in execute_bq_query(query)]

    def test_target_table_schema_conformance():
        expected_schemas = {
            "sof_ta_cntrct_crs2": [
                {'column_name': 'cntrct_id', 'data_type': 'STRING', 'is_nullable': 'NO'},
                {'column_name': 'crs_code_new', 'data_type': 'STRING', 'is_nullable': 'YES'},
                {'column_name': 'status', 'data_type': 'STRING', 'is_nullable': 'YES'},
                {'column_name': 'processed_date', 'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
            ],
            "via": [
                {'column_name': 'entry_id', 'data_type': 'STRING', 'is_nullable': 'NO'},
                {'column_name': 'message', 'data_type': 'STRING', 'is_nullable': 'YES'},
                {'column_name': 'log_time', 'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
            ],
            "job_table": [
                {'column_name': 'job_kennung', 'data_type': 'STRING', 'is_nullable': 'NO'},
                {'column_name': 'job_description', 'data_type': 'STRING', 'is_nullable': 'YES'},
                {'column_name': 'status', 'data_type': 'STRING', 'is_nullable': 'NO'},
                {'column_name': 'last_update_time', 'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
                {'column_name': 'updated_by', 'data_type': 'STRING', 'is_nullable': 'YES'},
            ],
            "job_run_log": [
                {'column_name': 'run_id', 'data_type': 'STRING', 'is_nullable': 'NO'},
                {'column_name': 'job_kennung', 'data_type': 'STRING', 'is_nullable': 'NO'},
                {'column_name': 'eintrags_nr', 'data_type': 'STRING', 'is_nullable': 'YES'},
                {'column_name': 'start_time', 'data_type': 'TIMESTAMP', 'is_nullable': 'NO'},
                {'column_name': 'end_time', 'data_type': 'TIMESTAMP', 'is_nullable': 'YES'},
                {'column_name': 'status', 'data_type': 'STRING', 'is_nullable': 'NO'},
                {'column_name': 'records_processed', 'data_type': 'INT64', 'is_nullable': 'YES'},
                {'column_name': 'error_message', 'data_type': 'STRING', 'is_nullable': 'YES'},
            ],
        }

        for table_name, expected_schema in expected_schemas.items():
            print(f"\nChecking schema for table: {table_name}")
            actual_schema = get_bq_table_schema(table_name)
            
            assert len(actual_schema) == len(expected_schema), \
                f"Column count mismatch for {table_name}. Expected {len(expected_schema)}, got {len(actual_schema)}"
            
            for i, col_expected in enumerate(expected_schema):
                col_actual = actual_schema[i]
                assert col_actual['column_name'] == col_expected['column_name'], \
                    f"Column name mismatch in {table_name} at position {i}. Expected {col_expected['column_name']}, got {col_actual['column_name']}"
                assert col_actual['data_type'] == col_expected['data_type'], \
                    f"Data type mismatch for {table_name}.{col_expected['column_name']}. Expected {col_expected['data_type']}, got {col_actual['data_type']}"
                assert col_actual['is_nullable'] == col_expected['is_nullable'], \
                    f"Nullability mismatch for {table_name}.{col_expected['column_name']}. Expected {col_expected['is_nullable']}, got {col_actual['is_nullable']}"
    ```

### Test Case 4.2: Data Integrity - Uniqueness in `sof_ta_cntrct_crs2`

*   **Purpose:** Verify that `cntrct_id` in `sof_ta_cntrct_crs2` remains unique if it's intended to be a primary key or unique identifier, even after multiple runs or specific data conditions.
*   **Setup:**
    1.  Clear target tables.
    2.  Populate `bq_dataset.sof_ta_cntrct_crs` with data that would lead to duplicate `cntrct_id`s if the `MERGE` or `INSERT/UPDATE` logic is flawed (e.g., running the same input twice).
    3.  Set `bq_dataset.job_table` for `UNIQUE_TEST_JOB` to 'ACTIVE'.
*   **Action:**
    1.  Run `control_k_ausd_v_ta_cntrct_crs2` once.
    2.  Run `control_k_ausd_v_ta_cntrct_crs2` again with the same input.
*   **Pass/Fail Criterion:**
    *   A query for duplicate `cntrct_id`s in `bq_dataset.sof_ta_cntrct_crs2` returns zero rows.

*   **Runnable Test Code (Pytest / SQL Assertions):**

    ```python
    import pytest
    from datetime import datetime, timezone

    def test_data_integrity_sof_ta_cntrct_crs2_uniqueness():
        job_kennung = "UNIQUE_TEST_JOB"
        eintrags_nr = "200"

        # --- Setup ---
        truncate_table("sof_ta_cntrct_crs")
        truncate_table("sof_ta_cntrct_crs2")
        truncate_table("via")
        truncate_table("job_run_log")
        set_job_status(job_kennung, "ACTIVE")

        # Input data
        input_data = [
            ("UNIQUE_1", "CRS_U1", datetime(2023, 1, 1).date(), datetime(2024, 1, 1).date(), datetime(2023, 1, 15, tzinfo=timezone.utc)),
            ("UNIQUE_2", "CRS_U2", datetime(2023, 2, 1).date(), datetime(2024, 2, 1).date(), datetime(2023, 2, 15, tzinfo=timezone.utc)),
        ]
        insert_data("sof_ta_cntrct_crs", 
                    ["cntrct_id", "crs_code", "start_date", "end_date", "load_date"],
                    input_data)

        # --- Action ---
        # First run
        call_bq_stored_procedure(
            "control_k_ausd_v_ta_cntrct_crs2",
            {"p_job_kennung": job_kennung, "p_eintrags_nr": eintrags_nr}
        )
        
        # Second run with same input (should update existing, not insert duplicates)
        call_bq_stored_procedure(
            "control_k_ausd_v_ta_cntrct_crs2",
            {"p_job_kennung": job_kennung, "p_eintrags_nr": eintrags_nr + "_rerun"} # Different eintrags_nr for log
        )

        # --- Pass/Fail Criterion ---
        # Check for duplicate cntrct_id in sof_ta_cntrct_crs2
        duplicate_check_query = f"""
        SELECT cntrct_id, COUNT(*) as count
        FROM `{BQ_PROJECT}.{BQ_DATASET}.sof_ta_cntrct_crs2`
        GROUP BY cntrct_id
        HAVING COUNT(*) > 1;
        """
        duplicates = list(execute_bq_query(duplicate_check_query))
        assert len(duplicates) == 0, f"Found duplicate cntrct_id in sof_ta_cntrct_crs2: {duplicates}"
        
        # Also check total count to ensure updates happened, not just ignoring
        total_records_query = f"SELECT COUNT(*) FROM `{BQ_PROJECT}.{BQ_DATASET}.sof_ta_cntrct_crs2`;"
        total_records = list(execute_bq_query(total_records_query))[0][0]
        assert total_records == len(input_data), "Expected total records to match input data count after updates."
    ```