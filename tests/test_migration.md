As a senior data-migration QA engineer, I've designed a suite of validation tests for the migrated BigQuery job, `sp_k_ausd_v_ta_acc_ref_control` and `sp_d_ausd_v_ta_acc_ref_transform`. These tests aim to ensure behavioral equivalence, data integrity, and correct handling of all specified transformation and orchestration logic.

The tests are structured into distinct test cases, each with a clear purpose, setup, action, and pass/fail criteria. Where applicable, runnable `pytest` code with BigQuery SQL assertions is provided.

**Assumptions for Test Execution:**
*   A BigQuery project and dataset are configured, referred to as `project.dataset`.
*   All DDLs for the BigQuery tables (`dwtk_meldungen`, `cds_ta_acc_ref`, `sof_ta_acc_ref`, `job_table`, `error_log`, `job_log`) have been executed.
*   The BigQuery stored procedures (`sp_d_ausd_v_ta_acc_ref_transform`, `sp_k_ausd_v_ta_acc_ref_control`) have been deployed.
*   A Python environment with the `google-cloud-bigquery` library is set up for running `pytest` tests.
*   The `client = bigquery.Client()` object is authenticated and authorized to interact with the BigQuery project.

---

### Helper Functions for Pytest Tests

These helper functions will be used across multiple test cases to manage test data and assert outcomes. They would typically reside in a `conftest.py` file or a dedicated test utility module.

```python
import pytest
from google.cloud import bigquery
import datetime
import time

client = bigquery.Client()
PROJECT_ID = "project"
DATASET_ID = "dataset"

def clear_all_tables():
    """Clears all relevant tables before a test run."""
    client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen` WHERE TRUE").result()
    client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.cds_ta_acc_ref` WHERE TRUE").result()
    client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_acc_ref` WHERE TRUE").result()
    client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_table` WHERE TRUE").result()
    client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_log` WHERE TRUE").result()
    client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.error_log` WHERE TRUE").result()
    # Ensure tables are empty before proceeding
    time.sleep(1) # Give BigQuery a moment to reflect deletes

def insert_dwtk_and_cds_data_for_success():
    """Inserts minimal data to allow a successful transformation with 1 record."""
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen` (job_kennung, timecreated)
        VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 00:00:00'))
    """).result()
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.cds_ta_acc_ref` (acc_ref_id, account_reference, insert_at, modified_at, valid_from, valid_to, is_production)
        VALUES (1, 'ACC_REF_OK', TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2023-01-01'), NULL, 1)
    """).result()

def get_table_data(table_id, order_by_cols=""):
    """Fetches all data from a table, optionally ordered."""
    order_clause = f"ORDER BY {order_by_cols}" if order_by_cols else ""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}` {order_clause}"
    rows = client.query(query).result()
    return [tuple(row.values()) for row in rows]

def get_sof_table_content():
    """Fetches acc_ref_id and account_reference from sof_ta_acc_ref, ordered."""
    query = f"SELECT acc_ref_id, account_reference FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_acc_ref` ORDER BY acc_ref_id"
    rows = client.query(query).result()
    return [tuple(row.values()) for row in rows]

def get_sof_record_count():
    """Returns the row count of sof_ta_acc_ref."""
    query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_acc_ref`"
    return list(client.query(query).result())[0][0]

def get_job_table_entries():
    """Fetches relevant columns from job_table, ordered."""
    query = f"SELECT job_kennung, eintrags_nr, tab_name, status FROM `{PROJECT_ID}.{DATASET_ID}.job_table` ORDER BY job_kennung, eintrags_nr"
    rows = client.query(query).result()
    return [tuple(row.values()) for row in rows]

def get_job_table_status(job_kennung, eintrags_nr):
    """Returns the status of a specific job from job_table."""
    query = f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_table` WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'"
    result = list(client.query(query).result())
    return result[0].status if result else None

def get_job_log_entries():
    """Fetches relevant columns from job_log, ordered."""
    query = f"SELECT job_kennung, eintrags_nr, tab_name, record_count, created_at FROM `{PROJECT_ID}.{DATASET_ID}.job_log` ORDER BY created_at"
    rows = client.query(query).result()
    return [tuple(row.values()) for row in rows]

def get_error_log_entries():
    """Fetches relevant columns from error_log, ordered."""
    query = f"SELECT job_kennung, eintrags_nr, error_code, error_message, created_at FROM `{PROJECT_ID}.{DATASET_ID}.error_log` ORDER BY created_at"
    rows = client.query(query).result()
    return [tuple(row.values()) for row in rows]

def setup_dwtk_and_cds_data(dwtk_meldungen_data, cds_ta_acc_ref_data):
    """Helper to populate source tables for transformation tests."""
    clear_all_tables()

    if dwtk_meldungen_data:
        values = ", ".join([f"('{job_kennung}', TIMESTAMP('{timecreated}'))" for job_kennung, timecreated in dwtk_meldungen_data])
        client.query(f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen` (job_kennung, timecreated) VALUES {values}").result()

    if cds_ta_acc_ref_data:
        values = ", ".join([
            f"({ar_id}, {ar_ref}, TIMESTAMP('{ins_at}'), {mod_at}, TIMESTAMP('{val_from}'), {val_to}, {is_prod})"
            for ar_id, ar_ref, ins_at, mod_at, val_from, val_to, is_prod in cds_ta_acc_ref_data
        ])
        client.query(f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.cds_ta_acc_ref` (acc_ref_id, account_reference, insert_at, modified_at, valid_from, valid_to, is_production) VALUES {values}").result()

```

---

### Test Case 1: End-to-End Output Parity (Happy Path)

*   **Purpose**: Verify that the migrated BigQuery job produces the exact same final data in `sof_ta_acc_ref` as the legacy Oracle job, given identical input data. This covers output parity and overall transformation correctness.
*   **Setup**:
    1.  Clear all relevant BigQuery tables (`dwtk_meldungen`, `cds_ta_acc_ref`, `sof_ta_acc_ref`, `job_table`, `job_log`, `error_log`).
    2.  Populate `project.dataset.dwtk_meldungen` with a specific `timecreated` for `BERT_DROP_TEMP_TABLE` (e.g., '2023-01-15 10:00:00'). This sets `v_datum` to '20230115'.
    3.  Populate `project.dataset.cds_ta_acc_ref` with a diverse set of records, including cases that should pass and fail the filters, and records with NULLs in `modified_at` and `valid_to`.
    4.  **Crucially**: Obtain the expected output data for `sof_ta_acc_ref` by running the *legacy* Oracle job with the *exact same input data* and capturing its final state. This "legacy output" is then used as the ground truth.
*   **Action**:
    1.  Call the BigQuery stored procedure: `CALL project.dataset.sp_k_ausd_v_ta_acc_ref_control('TEST_JOB', '12345');`
*   **Pass/Fail Criterion**:
    1.  The row count in `project.dataset.sof_ta_acc_ref` must exactly match the row count from the legacy `sof$ta_acc_ref`.
    2.  A deep comparison (e.g., hash of sorted rows, or row-by-row comparison) of `project.dataset.sof_ta_acc_ref` with the legacy `sof$ta_acc_ref` shows no differences.
    3.  The `job_log` table contains one entry for `('TEST_JOB', '12345', 'ta_acc_ref')` with `record_count` matching the final count.
    4.  The `job_table` entry for `('TEST_JOB', '12345')` has `status = 'COMPLETED'`.
    5.  The `error_log` table is empty.

```python
# test_parity.py
def test_end_to_end_output_parity():
    clear_all_tables()

    # Define the expected legacy output based on a specific input dataset
    # In a real scenario, this would be loaded from a golden file or a legacy system dump.
    # For this example, we manually derive it based on the input data and v_datum = '20230115'.
    legacy_sof_ta_acc_ref_data = [
        (1, 'ACC_REF_1'),
        (4, 'ACC_REF_4'),
        (5, 'ACC_REF_5'),
    ]
    expected_record_count = len(legacy_sof_ta_acc_ref_data)

    # Populate source data for BigQuery, mirroring the Oracle source for the legacy run
    setup_dwtk_and_cds_data(
        dwtk_meldungen_data=[('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00')],
        cds_ta_acc_ref_data=[
            # (acc_ref_id, account_reference, insert_at, modified_at, valid_from, valid_to, is_production)
            (1, "'ACC_REF_1'", '2023-01-01', 'NULL', '2023-01-01', 'NULL', 1), # Pass: insert_at <= v_datum, modified_at IS NULL, valid_from <= v_datum, valid_to IS NULL, is_production = 1
            (2, "'ACC_REF_2'", '2023-01-20', 'NULL', '2023-01-01', 'NULL', 1), # Fail: insert_at > v_datum
            (3, "'ACC_REF_3'", '2023-01-05', "'2023-01-10'", '2023-01-01', 'NULL', 1), # Fail: modified_at <= v_datum (and not NULL)
            (4, "'ACC_REF_4'", '2023-01-05', "'2023-01-20'", '2023-01-01', 'NULL', 1), # Pass: modified_at > v_datum
            (5, "'ACC_REF_5'", '2023-01-05', 'NULL', '2023-01-01', "'2023-01-20'", 1), # Pass: valid_to > v_datum
            (6, "'ACC_REF_6'", '2023-01-05', 'NULL', '2023-01-01', "'2023-01-10'", 1), # Fail: valid_to <= v_datum (and not NULL)
            (7, "'ACC_REF_7'", '2023-01-05', 'NULL', '2023-01-01', 'NULL', 0)  # Fail: is_production = 0
        ]
    )

    # Action: Call the BigQuery stored procedure
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('TEST_JOB', '12345');").result()

    # Assertions
    actual_sof_data = get_sof_table_content()
    assert sorted(actual_sof_data) == sorted(legacy_sof_ta_acc_ref_data), "Output data in sof_ta_acc_ref does not match legacy output."

    job_log_query = f"SELECT job_kennung, eintrags_nr, tab_name, record_count FROM `{PROJECT_ID}.{DATASET_ID}.job_log` WHERE job_kennung = 'TEST_JOB' AND eintrags_nr = '12345'"
    job_log_entry = list(client.query(job_log_query).result())
    assert len(job_log_entry) == 1, "Expected one job_log entry."
    assert job_log_entry[0].record_count == expected_record_count, f"Job log record count mismatch. Expected {expected_record_count}, got {job_log_entry[0].record_count}."

    job_table_query = f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_table` WHERE job_kennung = 'TEST_JOB' AND eintrags_nr = '12345'"
    job_table_status = list(client.query(job_table_query).result())
    assert len(job_table_status) == 1, "Expected one job_table entry."
    assert job_table_status[0].status == 'COMPLETED', "Job status in job_table is not 'COMPLETED'."

    error_log_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.error_log` WHERE job_kennung = 'TEST_JOB' AND eintrags_nr = '12345'"
    error_log_count = list(client.query(error_log_query).result())[0][0]
    assert error_log_count == 0, "Error log should be empty for a successful run."
```

---

### Test Case 2: Transformation Correctness - Cut-off Date (`v_datum`) Logic

*   **Purpose**: Verify that the `v_datum` is correctly determined from `dwtk_meldungen` and defaults to '19000101' when no relevant entry is found, as per the Oracle `NVL` and `MAX` logic.
*   **Setup**:
    1.  Clear all relevant BigQuery tables.
*   **Action**:
    1.  **Scenario A: `dwtk_meldungen` has relevant data.**
        *   Insert `('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-03-10 12:00:00'))` into `dwtk_meldungen`.
        *   Populate `cds_ta_acc_ref` with data that would pass *only* if `v_datum` is '20230310'.
        *   Call `sp_d_ausd_v_ta_acc_ref_transform()`.
    2.  **Scenario B: `dwtk_meldungen` is empty.**
        *   Clear `dwtk_meldungen`.
        *   Populate `cds_ta_acc_ref` with data that would pass *only* if `v_datum` is '19000101'.
        *   Call `sp_d_ausd_v_ta_acc_ref_transform()`.
    3.  **Scenario C: `dwtk_meldungen` has data, but not for `BERT_DROP_TEMP_TABLE`.**
        *   Insert `('OTHER_JOB', TIMESTAMP('2023-04-01 00:00:00'))` into `dwtk_meldungen`.
        *   Populate `cds_ta_acc_ref` with data that would pass *only* if `v_datum` is '19000101'.
        *   Call `sp_d_ausd_v_ta_acc_ref_transform()`.
*   **Pass/Fail Criterion**:
    *   For each scenario, the `sof_ta_acc_ref` table must contain the exact number of records expected based on the correctly derived `v_datum` and the filtering logic.

```python
# test_transformation.py
def test_v_datum_logic_scenario_a():
    # Scenario A: dwtk_meldungen has relevant data (v_datum = '20230310')
    setup_dwtk_and_cds_data(
        dwtk_meldungen_data=[('BERT_DROP_TEMP_TABLE', '2023-03-10 12:00:00')],
        cds_ta_acc_ref_data=[
            (1, "'ACC_REF_A1'", '2023-03-01', 'NULL', '2023-03-01', 'NULL', 1), # Pass
            (2, "'ACC_REF_A2'", '2023-03-15', 'NULL', '2023-03-01', 'NULL', 1), # Fail (insert_at > v_datum)
        ]
    )
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_d_ausd_v_ta_acc_ref_transform`();").result()
    assert get_sof_record_count() == 1, "Scenario A: Expected 1 record for v_datum '20230310'."

def test_v_datum_logic_scenario_b_empty_dwtk():
    # Scenario B: dwtk_meldungen is empty (v_datum = '19000101')
    setup_dwtk_and_cds_data(
        dwtk_meldungen_data=[],
        cds_ta_acc_ref_data=[
            (1, "'ACC_REF_B1'", '1900-01-01', 'NULL', '1900-01-01', 'NULL', 1), # Pass
            (2, "'ACC_REF_B2'", '1900-01-02', 'NULL', '1900-01-01', 'NULL', 1), # Fail (insert_at > v_datum)
        ]
    )
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_d_ausd_v_ta_acc_ref_transform`();").result()
    assert get_sof_record_count() == 1, "Scenario B: Expected 1 record for v_datum '19000101' (empty dwtk_meldungen)."

def test_v_datum_logic_scenario_c_no_relevant_job_kennung():
    # Scenario C: dwtk_meldungen has data, but not for BERT_DROP_TEMP_TABLE (v_datum = '19000101')
    setup_dwtk_and_cds_data(
        dwtk_meldungen_data=[('OTHER_JOB', '2023-04-01 00:00:00')],
        cds_ta_acc_ref_data=[
            (1, "'ACC_REF_C1'", '1900-01-01', 'NULL', '1900-01-01', 'NULL', 1), # Pass
            (2, "'ACC_REF_C2'", '1900-01-02', 'NULL', '1900-01-01', 'NULL', 1), # Fail (insert_at > v_datum)
        ]
    )
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_d_ausd_v_ta_acc_ref_transform`();").result()
    assert get_sof_record_count() == 1, "Scenario C: Expected 1 record for v_datum '19000101' (no relevant job_kennung)."
```

---

### Test Case 3: Transformation Correctness - Filtering Logic (Edge Cases & NULLs)

*   **Purpose**: Verify that all filter conditions (`insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`) and their NULL handling are correctly translated and applied, matching Oracle's behavior.
*   **Setup**:
    1.  Clear all relevant BigQuery tables.
    2.  Set a fixed `v_datum` by inserting `('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 00:00:00'))` into `dwtk_meldungen`. This means `v_datum` will be '20230115'.
    3.  Populate `cds_ta_acc_ref` with records specifically designed to test each filter condition, including boundary values and NULLs.
*   **Action**:
    1.  Call `project.dataset.sp_d_ausd_v_ta_acc_ref_transform()`.
*   **Pass/Fail Criterion**: The `sof_ta_acc_ref` table contains exactly the records expected to pass based on the `v_datum` '20230115' and the filter logic.

```python
# test_transformation.py
def test_filtering_logic():
    # v_datum is '20230115'
    test_data = [
        # (acc_ref_id, account_reference, insert_at, modified_at, valid_from, valid_to, is_production)
        # All conditions pass
        (1, "'Pass_All'", '2023-01-01', 'NULL', '2023-01-01', 'NULL', 1),
        (2, "'Pass_Modified_After'", '2023-01-01', "'2023-01-16'", '2023-01-01', 'NULL', 1),
        (3, "'Pass_Valid_To_After'", '2023-01-01', 'NULL', '2023-01-01', "'2023-01-16'", 1),
        (4, "'Pass_All_Boundary'", '2023-01-15', 'NULL', '2023-01-15', 'NULL', 1),

        # Fail: insert_at > v_datum
        (10, "'Fail_Insert_After'", '2023-01-16', 'NULL', '2023-01-01', 'NULL', 1),
        # Fail: modified_at <= v_datum (and not NULL)
        (11, "'Fail_Modified_Before'", '2023-01-01', "'2023-01-14'", '2023-01-01', 'NULL', 1),
        (12, "'Fail_Modified_On'", '2023-01-01', "'2023-01-15'", '2023-01-01', 'NULL', 1),
        # Fail: valid_from > v_datum
        (13, "'Fail_Valid_From_After'", '2023-01-01', 'NULL', '2023-01-16', 'NULL', 1),
        # Fail: valid_to <= v_datum (and not NULL)
        (14, "'Fail_Valid_To_Before'", '2023-01-01', 'NULL', '2023-01-01', "'2023-01-14'", 1),
        (15, "'Fail_Valid_To_On'", '2023-01-01', 'NULL', '2023-01-01', "'2023-01-15'", 1),
        # Fail: is_production = 0
        (16, "'Fail_Is_Production_0'", '2023-01-01', 'NULL', '2023-01-01', 'NULL', 0),
    ]
    setup_dwtk_and_cds_data(
        dwtk_meldungen_data=[('BERT_DROP_TEMP_TABLE', '2023-01-15 00:00:00')],
        cds_ta_acc_ref_data=test_data
    )

    # Action
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_d_ausd_v_ta_acc_ref_transform`();").result()

    # Expected results (IDs 1, 2, 3, 4 should pass)
    expected_sof_data = [
        (1, 'Pass_All'),
        (2, 'Pass_Modified_After'),
        (3, 'Pass_Valid_To_After'),
        (4, 'Pass_All_Boundary'),
    ]
    actual_sof_data = get_sof_table_content()
    assert sorted(actual_sof_data) == sorted(expected_sof_data), "Filtering logic produced incorrect results."
```

---

### Test Case 4: Orchestration - Parameter Validation

*   **Purpose**: Verify that `sp_k_ausd_v_ta_acc_ref_control` correctly validates input parameters (`p_JobKennung`, `p_EintragsNr`) and logs errors for missing/empty ones, mimicking the `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.
*   **Setup**:
    1.  Clear `job_table`, `job_log`, `error_log`.
*   **Action**:
    1.  Call `sp_k_ausd_v_ta_acc_ref_control` with various combinations of NULL/empty parameters.
*   **Pass/Fail Criterion**:
    1.  For each scenario, the procedure call must raise an error (e.g., `SIGNAL SQLSTATE '45000'`).
    2.  An entry must be present in `error_log` with `error_code = 193` and an appropriate `error_message`.
    3.  No entry should be created in `job_log`.
    4.  The `job_table` should not have an 'ACTIVE' entry for the attempted job. If an entry was created before the error, its status should be 'FAILED'.

```python
# test_orchestration.py
@pytest.mark.parametrize("job_kennung, eintrags_nr, expected_error_msg_part", [
    (None, '123', 'p_JobKennung darf nicht leer sein'),
    ('', '123', 'p_JobKennung darf nicht leer sein'),
    ('JOB', None, 'p_EintragsNr darf nicht leer sein'),
    ('JOB', '', 'p_EintragsNr darf nicht leer sein'),
])
def test_parameter_validation(job_kennung, eintrags_nr, expected_error_msg_part):
    clear_all_tables()
    
    # BigQuery stored procedures treat empty string and NULL differently for STRING parameters.
    # The SP code explicitly checks for IS NULL OR = ''.
    job_kennung_param = job_kennung if job_kennung is not None else ''
    eintrags_nr_param = eintrags_nr if eintrags_nr is not None else ''

    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('{job_kennung_param}', '{eintrags_nr_param}');").result()

    assert expected_error_msg_part in str(excinfo.value), f"Error message mismatch for {job_kennung_param}, {eintrags_nr_param}"

    error_log_entries = get_error_log_entries()
    # Filter for the specific job_kennung and eintrags_nr, as other tests might have run
    filtered_error_logs = [e for e in error_log_entries if e[0] == job_kennung_param and e[1] == eintrags_nr_param]
    assert len(filtered_error_logs) == 1, "Expected one error log entry."
    assert filtered_error_logs[0][2] == 193, "Expected error_code 193."
    assert expected_error_msg_part in filtered_error_logs[0][3], "Error message in log mismatch."

    # Check job_table status (should be FAILED if an entry was created, or not exist)
    job_status = get_job_table_status(job_kennung_param, eintrags_nr_param)
    if job_kennung_param and eintrags_nr_param: # If parameters were valid enough to create a job_table entry before the error
         assert job_status == 'FAILED', "Job status should be 'FAILED' after parameter validation error."
    else: # If parameters were too invalid to even create a job_table entry (e.g., first param fails)
         assert job_status is None, "No job_table entry should be created if validation fails early."

    job_log_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_log` WHERE job_kennung = '{job_kennung_param}' AND eintrags_nr = '{eintrags_nr_param}'"
    job_log_count = list(client.query(job_log_count_query).result())[0][0]
    assert job_log_count == 0, "No job_log entry should be created for failed parameter validation."
```

---

### Test Case 5: Orchestration - Job Management Logic

*   **Purpose**: Verify that the `job_table` is correctly updated for active job deactivation, new job registration, and status updates (COMPLETED/FAILED), replacing the `starteSQLSkript` functionality.
*   **Setup**:
    1.  Clear all relevant BigQuery tables.
*   **Action**:
    1.  **Scenario A: New job, successful run.** Call `sp_k_ausd_v_ta_acc_ref_control('JOB_A', '1')`.
    2.  **Scenario B: Existing active job for same `v_TabName`, new job runs successfully.** Insert an 'ACTIVE' job for `ta_acc_ref`, then call `sp_k_ausd_v_ta_acc_ref_control('JOB_B', '2')`.
    3.  **Scenario C: Job fails during transformation.** Simulate a transformation failure (e.g., by dropping a source table) and call `sp_k_ausd_v_ta_acc_ref_control('JOB_C', '3')`.
*   **Pass/Fail Criterion**:
    1.  **Scenario A**: `job_table` has `('JOB_A', '1')` with `status = 'COMPLETED'`. `job_log` has an entry for `('JOB_A', '1')`. `error_log` is empty.
    2.  **Scenario B**: `job_table` has `('OLD_JOB', '999')` with `status = 'DEACTIVATED'`. `job_table` has `('JOB_B', '2')` with `status = 'COMPLETED'`. `job_log` has an entry for `('JOB_B', '2')`. `error_log` is empty.
    3.  **Scenario C**: The call raises an error. `job_table` has `('JOB_C', '3')` with `status = 'FAILED'`. `job_log` has no entry for `('JOB_C', '3')`. `error_log` has an entry for `('JOB_C', '3')` with a relevant error message.

```python
# test_orchestration.py
def test_job_management_scenario_a_new_job_success():
    clear_all_tables()
    insert_dwtk_and_cds_data_for_success()

    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('JOB_A', '1');").result()

    job_table_entries = get_job_table_entries()
    assert len(job_table_entries) == 1
    assert ('JOB_A', '1', 'ta_acc_ref', 'COMPLETED') in job_table_entries

    job_log_entries = get_job_log_entries()
    assert len(job_log_entries) == 1
    assert job_log_entries[0][0:4] == ('JOB_A', '1', 'ta_acc_ref', 1) # Assuming 1 record from setup

    error_log_entries = get_error_log_entries()
    assert len(error_log_entries) == 0

def test_job_management_scenario_b_deactivate_old_job():
    clear_all_tables()
    insert_dwtk_and_cds_data_for_success()

    # Insert an old active job
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_table` (job_kennung, eintrags_nr, tab_name, status, created_at, updated_at)
        VALUES ('OLD_JOB', '999', 'ta_acc_ref', 'ACTIVE', TIMESTAMP('2023-01-01'), TIMESTAMP('2023-01-01'))
    """).result()

    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('JOB_B', '2');").result()

    job_table_entries = get_job_table_entries()
    assert len(job_table_entries) == 2
    assert ('OLD_JOB', '999', 'ta_acc_ref', 'DEACTIVATED') in job_table_entries
    assert ('JOB_B', '2', 'ta_acc_ref', 'COMPLETED') in job_table_entries

    job_log_entries = get_job_log_entries()
    assert len(job_log_entries) == 1
    assert job_log_entries[0][0:4] == ('JOB_B', '2', 'ta_acc_ref', 1)

    error_log_entries = get_error_log_entries()
    assert len(error_log_entries) == 0

def test_job_management_scenario_c_job_fails_during_transformation():
    clear_all_tables()
    # Insert a job that will fail
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_table` (job_kennung, eintrags_nr, tab_name, status, created_at, updated_at)
        VALUES ('JOB_C', '3', 'ta_acc_ref', 'ACTIVE', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """).result()

    # Simulate a failure in sp_d_ausd_v_ta_acc_ref_transform by dropping a source table
    client.query(f"DROP TABLE `{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen`").result()

    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('JOB_C', '3');").result()

    # Restore the table for other tests
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen`
        (
            job_kennung STRING,
            timecreated TIMESTAMP
        );
    """).result()

    assert "Not found: Table" in str(excinfo.value) or "Table not found" in str(excinfo.value)

    job_table_entries = get_job_table_entries()
    assert len(job_table_entries) == 1
    assert ('JOB_C', '3', 'ta_acc_ref', 'FAILED') in job_table_entries

    job_log_entries = get_job_log_entries()
    assert len(job_log_entries) == 0, "No job_log entry should be created for a failed job."

    error_log_entries = get_error_log_entries()
    assert len(error_log_entries) == 1
    assert error_log_entries[0][0] == 'JOB_C'
    assert error_log_entries[0][1] == '3'
    assert error_log_entries[0][2] == -1 # Generic error code if not explicitly set by SP
    assert "Not found: Table" in error_log_entries[0][3] or "Table not found" in error_log_entries[0][3]
```

---

### Test Case 6: Data Quality - Schema and Nullability

*   **Purpose**: Verify that the target table `sof_ta_acc_ref` has the expected schema and that NULL values are handled as expected (e.g., no unexpected NOT NULL violations).
*   **Setup**:
    1.  Clear all relevant BigQuery tables.
    2.  Populate `dwtk_meldungen` and `cds_ta_acc_ref` with data, including a record with a NULL `account_reference` to test nullability.
*   **Action**:
    1.  Call `project.dataset.sp_k_ausd_v_ta_acc_ref_control('DQ_JOB', '1')`.
*   **Pass/Fail Criterion**:
    1.  The schema of `project.dataset.sof_ta_acc_ref` matches the DDL provided in the migration design.
    2.  No errors related to schema mismatch or NULL constraint violations occur during execution.
    3.  If `account_reference` can be NULL in the source and target, verify that NULLs are correctly propagated.

```python
# test_data_quality.py
def test_schema_and_nullability():
    clear_all_tables()
    # Set v_datum to allow data to pass
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dwtk_meldungen` (job_kennung, timecreated)
        VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 00:00:00'))
    """).result()

    # Insert data including a NULL account_reference to test nullability if allowed
    setup_dwtk_and_cds_data(
        dwtk_meldungen_data=[('BERT_DROP_TEMP_TABLE', '2023-01-15 00:00:00')],
        cds_ta_acc_ref_data=[
            (1, "'ValidRef'", '2023-01-01', 'NULL', '2023-01-01', 'NULL', 1),
            (2, "NULL", '2023-01-01', 'NULL', '2023-01-01', 'NULL', 1) # Test NULL account_reference
        ]
    )

    # Action
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('DQ_JOB', '1');").result()

    # Assertions
    # 1. Schema check
    table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.sof_ta_acc_ref")
    schema_fields = {field.name: (field.field_type, field.mode) for field in table.schema}
    expected_schema = {
        'acc_ref_id': ('INT64', 'NULLABLE'),
        'account_reference': ('STRING', 'NULLABLE') # Based on DDL, no NOT NULL specified
    }
    assert schema_fields == expected_schema, "Schema of sof_ta_acc_ref does not match expected DDL."

    # 2. Nullability propagation
    # Check if the record with NULL account_reference was inserted successfully
    query_null_check = f"SELECT acc_ref_id, account_reference FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_acc_ref` WHERE acc_ref_id = 2"
    null_record = list(client.query(query_null_check).result())
    assert len(null_record) == 1, "Record with NULL account_reference was not inserted."
    assert null_record[0].account_reference is None, "NULL account_reference was not propagated correctly."

    # 3. No errors during execution (checked by job_table status)
    job_status = get_job_table_status('DQ_JOB', '1')
    assert job_status == 'COMPLETED', "Job should complete successfully without schema/nullability errors."

    error_log_entries = get_error_log_entries()
    assert len(error_log_entries) == 0, "Error log should be empty for a successful run."
```

---

### Test Case 7: Idempotency (Running the job multiple times)

*   **Purpose**: Verify that running the job multiple times with the same inputs produces the same final state in `sof_ta_acc_ref` and consistent logging, due to the truncate/insert and job management logic.
*   **Setup**:
    1.  Clear all relevant BigQuery tables.
    2.  Populate `dwtk_meldungen` and `cds_ta_acc_ref` with test data.
*   **Action**:
    1.  Call `sp_k_ausd_v_ta_acc_ref_control('IDEMPOTENT_JOB', '1')` for the first time.
    2.  Capture the state of `sof_ta_acc_ref`, `job_table`, `job_log`.
    3.  Call `sp_k_ausd_v_ta_acc_ref_control('IDEMPOTENT_JOB', '1')` for the second time.
*   **Pass/Fail Criterion**:
    1.  The final data in `sof_ta_acc_ref` after the second run is identical to the data after the first run.
    2.  The `job_table` entry for `('IDEMPOTENT_JOB', '1')` remains `COMPLETED`.
    3.  A new `job_log` entry is created for the second run, with the same `record_count`.
    4.  No errors are logged.

```python
# test_idempotency.py
def test_idempotency():
    clear_all_tables()
    insert_dwtk_and_cds_data_for_success() # This inserts 1 record into cds_ta_acc_ref

    job_kennung = 'IDEMPOTENT_JOB'
    eintrags_nr = '1'
    expected_record_count = 1 # Based on insert_dwtk_and_cds_data_for_success

    # First run
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('{job_kennung}', '{eintrags_nr}');").result()

    first_run_sof_data = get_sof_table_content()
    first_run_job_table = get_job_table_entries()
    first_run_job_log = get_job_log_entries()

    assert len(first_run_sof_data) == expected_record_count
    assert ('IDEMPOTENT_JOB', '1', 'ta_acc_ref', 'COMPLETED') in first_run_job_table
    assert len(first_run_job_log) == 1
    assert first_run_job_log[0][0:4] == ('IDEMPOTENT_JOB', '1', 'ta_acc_ref', expected_record_count)
    assert len(get_error_log_entries()) == 0

    # Second run
    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('{job_kennung}', '{eintrags_nr}');").result()

    second_run_sof_data = get_sof_table_content()
    second_run_job_table = get_job_table_entries()
    second_run_job_log = get_job_log_entries()

    # Assertions for second run
    assert second_run_sof_data == first_run_sof_data, "sof_ta_acc_ref content changed after second run."
    assert len(second_run_sof_data) == expected_record_count

    # Job table should still show 'COMPLETED' for the job
    assert ('IDEMPOTENT_JOB', '1', 'ta_acc_ref', 'COMPLETED') in second_run_job_table

    # Job log should have a new entry for the second run
    assert len(second_run_job_log) == 2, "Expected two job_log entries after two runs."
    # Check that the latest entry has the correct record count
    latest_job_log_entry = second_run_job_log[-1] # Assuming order by created_at implicitly
    assert latest_job_log_entry[0] == job_kennung
    assert latest_job_log_entry[1] == eintrags_nr
    assert latest_job_log_entry[3] == expected_record_count

    assert len(get_error_log_entries()) == 0, "Error log should be empty after idempotent runs."
```

---

### Test Case 8: External System Replacement - Logging Tables

*   **Purpose**: Verify that the `job_log` and `error_log` tables correctly capture the information that was previously handled by temporary files and shell error messages, and that timestamps are recorded.
*   **Setup**:
    1.  Clear `job_log`, `error_log`.
    2.  Populate source tables for a successful run.
*   **Action**:
    1.  Call `sp_k_ausd_v_ta_acc_ref_control('LOG_TEST_SUCCESS', '1')`.
    2.  Call `sp_k_ausd_v_ta_acc_ref_control` with invalid parameters to trigger an error (e.g., `('LOG_TEST_FAIL', '')`).
*   **Pass/Fail Criterion**:
    1.  `job_log` contains an entry for `('LOG_TEST_SUCCESS', '1')` with the correct `record_count` and a recent `created_at` timestamp.
    2.  `error_log` contains an entry for `('LOG_TEST_FAIL', '')` with `error_code = 193`, the expected error message, and a recent `created_at` timestamp.
    3.  The `job_log` table should only contain entries for successful runs.

```python
# test_logging.py
def test_logging_tables():
    clear_all_tables()
    insert_dwtk_and_cds_data_for_success() # This inserts 1 record into cds_ta_acc_ref

    # Scenario 1: Successful run
    job_kennung_success = 'LOG_TEST_SUCCESS'
    eintrags_nr_success = '1'
    expected_record_count = 1

    client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('{job_kennung_success}', '{eintrags_nr_success}');").result()

    job_log_entries_success = get_job_log_entries()
    assert len(job_log_entries_success) == 1
    assert job_log_entries_success[0][0] == job_kennung_success
    assert job_log_entries_success[0][1] == eintrags_nr_success
    assert job_log_entries_success[0][2] == 'ta_acc_ref'
    assert job_log_entries_success[0][3] == expected_record_count
    # Check created_at is recent (within last 60 seconds, for example)
    assert (datetime.datetime.now(datetime.timezone.utc) - job_log_entries_success[0][4].replace(tzinfo=datetime.timezone.utc)).total_seconds() < 60

    # Scenario 2: Failed run (invalid parameter)
    job_kennung_fail = 'LOG_TEST_FAIL'
    eintrags_nr_fail = '' # Empty EintragsNr to trigger error

    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_acc_ref_control`('{job_kennung_fail}', '{eintrags_nr_fail}');").result()

    error_log_entries_fail = get_error_log_entries()
    assert len(error_log_entries_fail) == 1
    assert error_log_entries_fail[0][0] == job_kennung_fail
    assert error_log_entries_fail[0][1] == eintrags_nr_fail
    assert error_log_entries_fail[0][2] == 193
    assert 'p_EintragsNr darf nicht leer sein' in error_log_entries_fail[0][3]
    # Check created_at is recent
    assert (datetime.datetime.now(datetime.timezone.utc) - error_log_entries_fail[0][4].replace(tzinfo=datetime.timezone.utc)).total_seconds() < 60

    # Verify job_log still only has the one success entry
    job_log_entries_after_fail = get_job_log_entries()
    assert len(job_log_entries_after_fail) == 1
```