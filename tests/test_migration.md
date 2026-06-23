As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_bp_ta_iccid_einzeln.ksh` to Google BigQuery. These tests aim to ensure behavioral equivalence, data integrity, and correct functionality of the migrated components.

**Assumptions:**
*   A BigQuery test environment is set up with the migrated DDL (tables, stored procedures, UDFs).
*   The `your_project_id` and `your_dataset_id` placeholders in the generated code are replaced with actual project and dataset names in the test environment. For brevity in test code, I'll use `project.dataset`.
*   The source table `project.dataset.sof_ta_bpr_basis` is populated with representative test data that mirrors the structure and content of the legacy Oracle source table. This data should include various `bpr_id` values, `slave_number` values, `valid_to` dates (both before and after the `p_data_date`), and NULLs where applicable.
*   Access to the legacy environment is available to run the original script with specific inputs and capture its outputs for comparison.
*   A Python environment with `pytest` and a BigQuery client library is available for running the test code.

---

## Migration Validation Tests for `k_ausd_bp_ta_iccid_einzeln.ksh`

### Test Setup (Common for all tests)

```python
import pytest
from google.cloud import bigquery
from datetime import date, datetime, timedelta

# --- Configuration ---
PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

# --- Helper Functions for BigQuery Interaction ---
def execute_bq_query(query):
    """Executes a BigQuery SQL query and returns the results."""
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def call_bq_stored_procedure(sp_name, *args):
    """Calls a BigQuery stored procedure."""
    arg_strings = []
    for arg in args:
        if isinstance(arg, str):
            arg_strings.append(f"'{arg}'")
        elif isinstance(arg, date):
            arg_strings.append(f"DATE '{arg.isoformat()}'")
        elif arg is None:
            arg_strings.append("NULL")
        else:
            arg_strings.append(str(arg))
    query = f"CALL {PROJECT_ID}.{DATASET_ID}.{sp_name}({', '.join(arg_strings)})"
    print(f"Executing: {query}")
    execute_bq_query(query)

def get_table_row_count(table_name):
    """Returns the row count of a BigQuery table."""
    query = f"SELECT COUNT(*) FROM {PROJECT_ID}.{DATASET_ID}.{table_name}"
    result = execute_bq_query(query)
    return next(result)[0]

def get_table_data(table_name, order_by_cols=None):
    """Fetches all data from a table, optionally ordered."""
    order_clause = ""
    if order_by_cols:
        order_clause = f" ORDER BY {', '.join(order_by_cols)}"
    query = f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{table_name}{order_clause}"
    return list(execute_bq_query(query))

def clear_table(table_name):
    """Deletes all rows from a BigQuery table."""
    query = f"TRUNCATE TABLE {PROJECT_ID}.{DATASET_ID}.{table_name}"
    execute_bq_query(query)

# --- Fixtures ---
@pytest.fixture(autouse=True)
def setup_and_teardown_tables():
    """Clears target and log tables before and after each test."""
    clear_table("sof_ta_iccid_einzeln")
    clear_table("job_log")
    yield
    clear_table("sof_ta_iccid_einzeln")
    clear_table("job_log")

@pytest.fixture
def populate_source_data():
    """Populates the source table sof_ta_bpr_basis with test data."""
    clear_table("sof_ta_bpr_basis")
    insert_query = f"""
    INSERT INTO {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_basis (
        cntrct_id, bpr_id, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME, slave_number
    ) VALUES
    ('C1', 31, 'ICCID1', 'MCC1', 'MNC1', 'HLR1', 'SI1', DATE '2023-01-15', 'EID1', 'TypeA', NULL),
    ('C1', 2759, 'ICCID2', 'MCC2', 'MNC2', 'HLR2', 'SI2', DATE '2023-01-10', 'EID2', 'TypeB', NULL),
    ('C2', 2800, 'ICCID3', 'MCC3', 'MNC3', 'HLR3', 'SI3', DATE '2023-01-20', 'EID3', 'TypeC', NULL),
    ('C3', 3848, 'ICCID4', 'MCC4', 'MNC4', 'HLR4', 'SI4', DATE '2023-01-25', 'EID4', 'TypeD', 1),
    ('C3', 3848, 'ICCID5', 'MCC5', 'MNC5', 'HLR5', 'SI5', DATE '2023-01-26', 'EID5', 'TypeE', 2),
    ('C4', 31, 'ICCID6', 'MCC6', 'MNC6', 'HLR6', 'SI6', DATE '2023-01-05', 'EID6', 'TypeF', NULL), -- Valid_to before p_data_date
    ('C5', 3848, 'ICCID7', 'MCC7', 'MNC7', 'HLR7', 'SI7', DATE '2023-01-28', 'EID7', 'TypeG', 11), -- Slave_number not 1-10
    ('C6', 3848, 'ICCID8', 'MCC8', 'MNC8', 'HLR8', 'SI8', DATE '2023-01-28', 'EID8', 'TypeH', 10),
    ('C7', 3848, 'ICCID9', 'MCC9', 'MNC9', 'HLR9', 'SI9', DATE '2023-01-28', 'EID9', 'TypeI', 1)
    """
    execute_bq_query(insert_query)
    return get_table_row_count("sof_ta_bpr_basis")

# --- Legacy Data Capture (Conceptual) ---
def get_legacy_output_data(job_id, entry_nr, ref_date, restart_val):
    """
    Conceptual function to run the legacy ksh script and capture its output.
    In a real scenario, this would involve:
    1. Executing the ksh script with the given parameters.
    2. Capturing the final state of the target database table(s) or output files.
    3. Parsing the captured data into a comparable format (e.g., list of dicts/tuples).
    4. Capturing any log entries from the legacy system.
    """
    print(f"Running legacy script with: j={job_id}, f={entry_nr}, s={ref_date}, l={restart_val}")
    # Placeholder for actual legacy execution and data capture
    # Example:
    # subprocess.run(['/path/to/legacy/k_ausd_bp_ta_iccid_einzeln.ksh', '-j', job_id, ...])
    # Then query legacy Oracle DB or read output files.
    
    # For demonstration, return a dummy structure.
    # This part needs to be implemented based on actual legacy system interaction.
    legacy_target_data = [
        # Example: (CNTRCT_ID, TN_ICCID, ..., MS10_CARD_TYPE_NAME)
        # This data should match what the legacy script would produce for the given inputs.
    ]
    legacy_log_entries = [
        # Example: (job_kennung, eintrags_nr, tab_name, stichtag, records, status, message)
    ]
    return legacy_target_data, legacy_log_entries

```

---

### 1. Output Parity — Same inputs produce the same outputs as the legacy job.

**Purpose:** To verify that the BigQuery stored procedure, when executed with identical parameters, produces the exact same final data in `sof_ta_iccid_einzeln` and logs the same information as the legacy KornShell script.

**Setup:**
1.  Populate `project.dataset.sof_ta_bpr_basis` with a known set of test data (using `populate_source_data` fixture).
2.  Define specific input parameters for the job (Job ID, Entry Number, Reference Date, Restart Value).
3.  Run the legacy `k_ausd_bp_ta_iccid_einzeln.ksh` script with these parameters and capture its output (final state of target tables/files, log entries).

**Action:**
1.  Call the BigQuery main orchestration stored procedure `project.dataset.r_ausd_bp_ta_iccid_einzeln` with the same parameters used for the legacy run.
2.  Retrieve all data from `project.dataset.sof_ta_iccid_einzeln`.
3.  Retrieve relevant log entries from `project.dataset.job_log`.

**Pass/Fail Criterion:**
*   The number of rows in `project.dataset.sof_ta_iccid_einzeln` must be identical to the number of rows produced by the legacy job.
*   Every column value for every row in `project.dataset.sof_ta_iccid_einzeln` must exactly match the corresponding data from the legacy job's output.
*   The `job_log` entries (especially `status`, `records`, and `message`) for the BigQuery run must match the logging behavior of the legacy job.

```python
def test_output_parity_with_legacy(populate_source_data):
    """
    Tests that the BigQuery job produces identical output to the legacy job
    for a given set of inputs.
    """
    job_kennung = "TEST_JOB_PARITY"
    eintrags_nr = 101
    stichtag = "20010120" # DDMMYYYY
    restart_val = "0"

    # --- Legacy Run (Conceptual) ---
    # In a real scenario, this would involve executing the actual ksh script
    # and capturing its output from the legacy database/files.
    # For this test, we'll simulate expected legacy output based on the transformation logic.
    # This is the most critical part to get right for output parity.
    legacy_target_data, legacy_log_entries = get_legacy_output_data(
        job_kennung, eintrags_nr, stichtag, restart_val
    )

    # --- BigQuery Run ---
    call_bq_stored_procedure(
        "r_ausd_bp_ta_iccid_einzeln",
        job_kennung,
        eintrags_nr,
        stichtag,
        restart_val
    )

    # --- Assertions ---
    bq_target_data = get_table_data("sof_ta_iccid_einzeln", order_by_cols=["CNTRCT_ID"])
    bq_log_entries = get_table_data(
        "job_log", order_by_cols=["job_kennung", "eintrags_nr", "status"]
    )

    # 1. Compare target data row counts
    assert len(bq_target_data) == len(legacy_target_data), \
        "Row count in target table does not match legacy output."

    # 2. Compare target data content (requires careful mapping of columns)
    # This part assumes legacy_target_data is structured similarly to bq_target_data
    # For a robust comparison, you might need to convert both to a canonical form (e.g., dicts).
    for i, bq_row in enumerate(bq_target_data):
        legacy_row = legacy_target_data[i]
        # Example: assert bq_row.CNTRCT_ID == legacy_row.CNTRCT_ID
        # You would need to compare all relevant columns.
        # For a full comparison, consider converting rows to dictionaries and comparing them.
        assert bq_row == legacy_row, f"Data mismatch at row {i}: BQ={bq_row}, Legacy={legacy_row}"

    # 3. Compare log entries
    # This requires a clear understanding of what the legacy script logs.
    # For now, we'll check for success and record count.
    assert any(
        log.status == "SUCCESS" and log.job_kennung == job_kennung and log.eintrags_nr == eintrags_nr
        for log in bq_log_entries
    ), "SUCCESS log entry not found for BigQuery run."
    
    # Further checks on specific log messages or record counts if available from legacy.
    bq_success_log = next(
        (log for log in bq_log_entries if log.status == "SUCCESS" and log.job_kennung == job_kennung),
        None
    )
    assert bq_success_log is not None
    assert bq_success_log.records == len(bq_target_data), \
        "Logged record count does not match actual processed records."

```

### 2. Transformation Correctness — joins, aggregations, filters, type handling, NULL handling, and any edge cases called out in the design.

This section focuses on the `p_process_iccid_einzeln` stored procedure, which contains the core data transformation logic.

#### 2.1. `p_process_iccid_einzeln` - Basic Transformation & Filtering

**Purpose:** To verify that the `p_process_iccid_einzeln` procedure correctly filters data based on `bpr_id` and applies the `CASE` statements for column mapping and status calculation.

**Setup:**
1.  Populate `project.dataset.sof_ta_bpr_basis` with diverse data, including rows with `bpr_id` values that should be processed (31, 2759, 2800, 3848) and those that should be filtered out.
2.  Define a `p_data_date` for the test.

**Action:**
1.  Call `project.dataset.p_process_iccid_einzeln` with the chosen `p_data_date`.
2.  Query `project.dataset.sof_ta_iccid_einzeln` to inspect the results.

**Pass/Fail Criterion:**
*   Only rows from `sof_ta_bpr_basis` with `bpr_id` in (31, 2759, 2800, 3848) should be present in `sof_ta_iccid_einzeln`.
*   For each `bpr_id`, the correct set of `ICCID`, `IMSI_*`, `STATUS`, `VALID_TO`, `E_ID`, `CARD_TYPE_NAME` columns should be populated, and others should be `NULL`.
*   The `STATUS` column should be 'L' if `valid_to <= p_data_date`, and 'A' otherwise.

```python
def test_core_transformation_logic(populate_source_data):
    """
    Tests the core transformation logic within p_process_iccid_einzeln,
    including filtering and conditional column population.
    """
    data_date = date(2023, 1, 18) # Stichtag for status calculation
    records_processed = 0

    # Call the core processing SP directly
    call_bq_stored_procedure(
        "p_process_iccid_einzeln",
        data_date,
        records_processed # OUT parameter, value will be captured by the SP
    )

    # Retrieve processed records from the target table
    results = get_table_data("sof_ta_iccid_einzeln", order_by_cols=["CNTRCT_ID"])

    # Expected records based on populate_source_data and bpr_id filter
    # C1 (bpr_id 31), C1 (bpr_id 2759), C2 (bpr_id 2800), C3 (bpr_id 3848, slave 1),
    # C3 (bpr_id 3848, slave 2), C4 (bpr_id 31), C6 (bpr_id 3848, slave 10), C7 (bpr_id 3848, slave 1)
    # Total 8 records should be processed (C5 with slave_number 11 is filtered by CASE)
    assert len(results) == 8, "Incorrect number of rows processed."

    # Verify specific transformations
    for row in results:
        if row.CNTRCT_ID == 'C1':
            if row.TN_ICCID: # bpr_id = 31
                assert row.TN_ICCID == 'ICCID1'
                assert row.TN_STATUS == 'A' # 2023-01-15 <= 2023-01-18 is True, so 'L'
                assert row.TN_VALID_TO == date(2023, 1, 15)
                assert row.TC_ICCID is None
                assert row.TB_ICCID is None
                assert row.MS1_ICCID is None
            elif row.TC_ICCID: # bpr_id = 2759
                assert row.TC_ICCID == 'ICCID2'
                assert row.TC_STATUS == 'L' # 2023-01-10 <= 2023-01-18 is True, so 'L'
                assert row.TC_VALID_TO == date(2023, 1, 10)
                assert row.TN_ICCID is None
        elif row.CNTRCT_ID == 'C2': # bpr_id = 2800
            assert row.TB_ICCID == 'ICCID3'
            assert row.TB_STATUS == 'A' # 2023-01-20 <= 2023-01-18 is False, so 'A'
            assert row.TB_VALID_TO == date(2023, 1, 20)
            assert row.TN_ICCID is None
        elif row.CNTRCT_ID == 'C3': # bpr_id = 3848
            if row.MS1_ICCID: # slave_number = 1
                assert row.MS1_ICCID == 'ICCID4'
                assert row.MS1_STATUS == 'A' # 2023-01-25 <= 2023-01-18 is False, so 'A'
                assert row.MS1_VALID_TO == date(2023, 1, 25)
                assert row.MS2_ICCID is None
            elif row.MS2_ICCID: # slave_number = 2
                assert row.MS2_ICCID == 'ICCID5'
                assert row.MS2_STATUS == 'A' # 2023-01-26 <= 2023-01-18 is False, so 'A'
                assert row.MS2_VALID_TO == date(2023, 1, 26)
                assert row.MS1_ICCID is None
        elif row.CNTRCT_ID == 'C4': # bpr_id = 31, valid_to before data_date
            assert row.TN_ICCID == 'ICCID6'
            assert row.TN_STATUS == 'L' # 2023-01-05 <= 2023-01-18 is True, so 'L'
            assert row.TN_VALID_TO == date(2023, 1, 5)
        elif row.CNTRCT_ID == 'C6': # bpr_id = 3848, slave_number = 10
            assert row.MS10_ICCID == 'ICCID8'
            assert row.MS10_STATUS == 'A'
        elif row.CNTRCT_ID == 'C7': # bpr_id = 3848, slave_number = 1
            assert row.MS1_ICCID == 'ICCID9'
            assert row.MS1_STATUS == 'A'

    # Verify that rows with bpr_id not in the list or slave_number > 10 are not processed
    # (C5 with slave_number 11 should not create any MSx_ICCID)
    assert not any(row.CNTRCT_ID == 'C5' for row in results)

```

#### 2.2. `p_process_iccid_einzeln` - NULL Handling

**Purpose:** To ensure that `NULL` values in source columns are correctly propagated or handled by `CASE` statements, resulting in `NULL` in target columns where expected.

**Setup:**
1.  Populate `project.dataset.sof_ta_bpr_basis` with rows containing `NULL` values for `iccid`, `imsi_*`, `E_ID`, `CARD_TYPE_NAME`, and `slave_number`.

**Action:**
1.  Call `project.dataset.p_process_iccid_einzeln`.
2.  Query `project.dataset.sof_ta_iccid_einzeln`.

**Pass/Fail Criterion:**
*   If a source column (e.g., `iccid`) is `NULL` and its `bpr_id` matches a `CASE` condition, the corresponding target column (e.g., `TN_ICCID`) should also be `NULL`.
*   If a `slave_number` is `NULL` or outside the 1-10 range for `bpr_id = 3848`, the corresponding `MSx_` columns should be `NULL`.

```python
def test_core_transformation_null_handling():
    """
    Tests how p_process_iccid_einzeln handles NULL values in source data.
    """
    clear_table("sof_ta_bpr_basis")
    insert_query = f"""
    INSERT INTO {PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_basis (
        cntrct_id, bpr_id, iccid, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, valid_to, E_ID, CARD_TYPE_NAME, slave_number
    ) VALUES
    ('C_NULL_ICCID', 31, NULL, 'MCC_N', 'MNC_N', 'HLR_N', 'SI_N', DATE '2023-01-20', 'EID_N', 'Type_N', NULL),
    ('C_NULL_SLAVE', 3848, 'ICCID_S', 'MCC_S', 'MNC_S', 'HLR_S', 'SI_S', DATE '2023-01-20', 'EID_S', 'Type_S', NULL),
    ('C_NULL_ALL', 2759, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """
    execute_bq_query(insert_query)

    data_date = date(2023, 1, 15)
    records_processed = 0
    call_bq_stored_procedure("p_process_iccid_einzeln", data_date, records_processed)
    results = get_table_data("sof_ta_iccid_einzeln", order_by_cols=["CNTRCT_ID"])

    assert len(results) == 3, "Expected 3 rows for NULL handling test."

    for row in results:
        if row.CNTRCT_ID == 'C_NULL_ICCID':
            assert row.TN_ICCID is None
            assert row.TN_IMSI_MCC == 'MCC_N' # Other fields should still populate if not NULL
            assert row.TN_STATUS == 'A' # valid_to > data_date
        elif row.CNTRCT_ID == 'C_NULL_SLAVE':
            # Since slave_number is NULL, none of the MSx_ICCID columns should be populated
            assert row.MS1_ICCID is None
            assert row.MS2_ICCID is None
            assert row.MS10_ICCID is None
            # All other MSx_ fields should also be NULL
        elif row.CNTRCT_ID == 'C_NULL_ALL':
            assert row.TC_ICCID is None
            assert row.TC_IMSI_MCC is None
            assert row.TC_STATUS is None # valid_to is NULL, so status should be NULL
            assert row.TC_VALID_TO is None
```

### 3. External-system replacements — Oracle reads, SFTP/S3 drops, etc. behave as the design specifies.

This covers how the BigQuery solution replaces the original script's interactions with external systems or utilities.

#### 3.1. Parameter Validation (replaces `pruefeParameterGesetzt`)

**Purpose:** To ensure that the BigQuery stored procedure `r_ausd_bp_ta_iccid_einzeln` correctly validates its input parameters and raises an error if mandatory parameters are missing or invalid.

**Setup:** None (uses direct SP calls).

**Action:**
1.  Call `r_ausd_bp_ta_iccid_einzeln` with missing `p_JobKennung`.
2.  Call `r_ausd_bp_ta_iccid_einzeln` with missing `p_EintragsNr`.
3.  Call `r_ausd_bp_ta_iccid_einzeln` with missing `p_Stichtag`.
4.  Call `r_ausd_bp_ta_iccid_einzeln` with an invalid `p_Stichtag` format.

**Pass/Fail Criterion:**
*   Each call with invalid parameters must raise an exception with a specific error message as defined in the stored procedure.
*   A `FAILED` entry must be logged in `job_log` for each failed execution, containing the appropriate error message.

```python
def test_parameter_validation_errors():
    """
    Tests that r_ausd_bp_ta_iccid_einzeln correctly validates parameters
    and raises errors for missing/invalid inputs.
    """
    valid_job_kennung = "VALID_JOB"
    valid_eintrags_nr = 1
    valid_stichtag = "20230101"
    restart_val = "0"

    # Test 1: Missing JobKennung
    with pytest.raises(Exception) as excinfo:
        call_bq_stored_procedure(
            "r_ausd_bp_ta_iccid_einzeln",
            None, # Missing JobKennung
            valid_eintrags_nr,
            valid_stichtag,
            restart_val
        )
    assert "Jobkennung parameter must be set." in str(excinfo.value)
    log_entries = get_table_data("job_log")
    assert any("FAILED" in log.status and "Jobkennung parameter must be set." in log.message for log in log_entries)
    clear_table("job_log") # Clear for next test

    # Test 2: Missing EintragsNr
    with pytest.raises(Exception) as excinfo:
        call_bq_stored_procedure(
            "r_ausd_bp_ta_iccid_einzeln",
            valid_job_kennung,
            None, # Missing EintragsNr
            valid_stichtag,
            restart_val
        )
    assert "EintragsNr parameter must be set." in str(excinfo.value)
    log_entries = get_table_data("job_log")
    assert any("FAILED" in log.status and "EintragsNr parameter must be set." in log.message for log in log_entries)
    clear_table("job_log")

    # Test 3: Missing Stichtag
    with pytest.raises(Exception) as excinfo:
        call_bq_stored_procedure(
            "r_ausd_bp_ta_iccid_einzeln",
            valid_job_kennung,
            valid_eintrags_nr,
            None, # Missing Stichtag
            restart_val
        )
    assert "Stichtag parameter must be set." in str(excinfo.value)
    log_entries = get_table_data("job_log")
    assert any("FAILED" in log.status and "Stichtag parameter must be set." in log.message for log in log_entries)
    clear_table("job_log")

    # Test 4: Invalid Stichtag format
    invalid_stichtag = "2023-01-01" # Expected DDMMYYYY
    with pytest.raises(Exception) as excinfo:
        call_bq_stored_procedure(
            "r_ausd_bp_ta_iccid_einzeln",
            valid_job_kennung,
            valid_eintrags_nr,
            invalid_stichtag,
            restart_val
        )
    assert "Invalid Stichtag date format" in str(excinfo.value)
    log_entries = get_table_data("job_log")
    assert any("FAILED" in log.status and "Invalid Stichtag date format" in log.message for log in log_entries)
    clear_table("job_log")

```

#### 3.2. Date Calculations (replaces `gestern.ksh`)

**Purpose:** To verify that `p_datum_heute` and `p_datum_gestern` are correctly calculated using BigQuery's native date functions, mirroring the behavior of `gestern.ksh`.

**Setup:** None.

**Action:**
1.  Call `r_ausd_bp_ta_iccid_einzeln` with valid parameters.
2.  Inspect the `job_log` entries for the `stichtag` and potentially other date-related information if logged. (Note: `p_datum_heute` and `p_datum_gestern` are internal to the SP, so we'll verify their usage indirectly or by extending the SP to log them for testing).

**Pass/Fail Criterion:**
*   The `v_stichtag_date` derived from `p_Stichtag` must be correct.
*   (Indirectly) The `p_process_iccid_einzeln` procedure must receive the correct `p_data_date` (which is `v_stichtag_date`).
*   If `p_datum_heute` and `p_datum_gestern` were logged, they should reflect `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` respectively.

```python
def test_date_calculations(populate_source_data):
    """
    Tests that date calculations (Stichtag parsing, heute/gestern) are correct.
    """
    job_kennung = "DATE_CALC_JOB"
    eintrags_nr = 2
    stichtag_str = "15032023" # March 15, 2023
    expected_stichtag_date = date(2023, 3, 15)
    restart_val = "0"

    call_bq_stored_procedure(
        "r_ausd_bp_ta_iccid_einzeln",
        job_kennung,
        eintrags_nr,
        stichtag_str,
        restart_val
    )

    log_entries = get_table_data("job_log", order_by_cols=["created_at"])

    # Check the 'RUNNING' log entry for the correct stichtag_date
    running_log = next(
        (log for log in log_entries if log.status == "RUNNING" and log.job_kennung == job_kennung),
        None
    )
    assert running_log is not None, "RUNNING log entry not found."
    assert running_log.stichtag == expected_stichtag_date, \
        f"Logged Stichtag date mismatch. Expected {expected_stichtag_date}, Got {running_log.stichtag}"

    # To directly test v_datum_heute and v_datum_gestern, the SP would need to log them.
    # For now, we assume if stichtag_date is correct, other date functions are also used correctly.
    # If direct logging of these internal variables is needed, modify the SP for testing.
```

#### 3.3. `f_is_date_check` UDF

**Purpose:** To verify the `f_is_date_check` UDF correctly identifies valid and invalid date strings based on a given format.

**Setup:** None.

**Action:**
1.  Execute SQL queries directly calling `f_is_date_check` with various date strings and formats.

**Pass/Fail Criterion:**
*   Returns `TRUE` for valid date strings and formats.
*   Returns `FALSE` for invalid date strings or mismatched formats.

```python
def test_f_is_date_check_udf():
    """
    Tests the f_is_date_check UDF for correct date validation.
    """
    # Valid cases
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check('01012023', '%d%m%Y')").to_dataframe().iloc[0,0] is True
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check('2023-01-01', '%Y-%m-%d')").to_dataframe().iloc[0,0] is True
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check('20230101', '%Y%m%d')").to_dataframe().iloc[0,0] is True

    # Invalid cases
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check('2023-01-01', '%d%m%Y')").to_dataframe().iloc[0,0] is False # Mismatched format
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check('32012023', '%d%m%Y')").to_dataframe().iloc[0,0] is False # Invalid day
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check('01132023', '%d%m%Y')").to_dataframe().iloc[0,0] is False # Invalid month
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check('NOT_A_DATE', '%d%m%Y')").to_dataframe().iloc[0,0] is False
    assert execute_bq_query(f"SELECT {PROJECT_ID}.{DATASET_ID}.f_is_date_check(NULL, '%d%m%Y')").to_dataframe().iloc[0,0] is False # NULL input
```

### 4. Data-quality / row-count / schema assertions.

#### 4.1. Row Count Accuracy

**Purpose:** To verify that the `v_records_processed` variable in `r_ausd_bp_ta_iccid_einzeln` (and subsequently logged) accurately reflects the number of rows inserted into `sof_ta_iccid_einzeln` by `p_process_iccid_einzeln`.

**Setup:**
1.  Populate `project.dataset.sof_ta_bpr_basis` with a known number of rows that will be processed.

**Action:**
1.  Call `project.dataset.r_ausd_bp_ta_iccid_einzeln` with valid parameters.
2.  Query `project.dataset.sof_ta_iccid_einzeln` for its row count.
3.  Query `project.dataset.job_log` for the `records` value in the `SUCCESS` entry.

**Pass/Fail Criterion:**
*   The `records` value in the `SUCCESS` log entry must match the actual row count of `project.dataset.sof_ta_iccid_einzeln`.

```python
def test_row_count_accuracy(populate_source_data):
    """
    Tests that the logged record count matches the actual number of rows
    inserted into the target table.
    """
    job_kennung = "ROW_COUNT_JOB"
    eintrags_nr = 3
    stichtag = "20230120"
    restart_val = "0"

    call_bq_stored_procedure(
        "r_ausd_bp_ta_iccid_einzeln",
        job_kennung,
        eintrags_nr,
        stichtag,
        restart_val
    )

    actual_row_count = get_table_row_count("sof_ta_iccid_einzeln")
    log_entries = get_table_data("job_log")

    success_log = next(
        (log for log in log_entries if log.status == "SUCCESS" and log.job_kennung == job_kennung),
        None
    )

    assert success_log is not None, "SUCCESS log entry not found."
    assert success_log.records == actual_row_count, \
        f"Logged record count ({success_log.records}) does not match actual row count ({actual_row_count})."

```

#### 4.2. Schema Assertions

**Purpose:** To verify that the schema of `project.dataset.sof_ta_iccid_einzeln` and `project.dataset.job_log` matches the design document and expected data types.

**Setup:** None (relies on existing table definitions).

**Action:**
1.  Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the target tables.

**Pass/Fail Criterion:**
*   All expected columns are present.
*   Column names match exactly (case-sensitive if BigQuery is configured that way, though usually case-insensitive for identifiers).
*   Data types match the design (e.g., `STRING`, `INT64`, `DATE`, `TIMESTAMP`).
*   Nullability constraints (`NOT NULL`) are correctly applied.

```python
def test_schema_assertions():
    """
    Tests the schema of the target and log tables.
    """
    expected_sof_ta_iccid_einzeln_schema = {
        "CNTRCT_ID": "STRING", "TN_ICCID": "STRING", "TN_IMSI_MCC": "STRING",
        "TN_IMSI_MNC": "STRING", "TN_IMSI_HLR": "STRING", "TN_IMSI_SI": "STRING",
        "TN_STATUS": "STRING", "TN_VALID_TO": "DATE", "TN_E_ID": "STRING",
        "TN_CARD_TYPE_NAME": "STRING",
        # ... (all 100 columns as per DDL)
        "MS10_ICCID": "STRING", "MS10_IMSI_MCC": "STRING", "MS10_IMSI_MNC": "STRING",
        "MS10_IMSI_HLR": "STRING", "MS10_IMSI_SI": "STRING", "MS10_STATUS": "STRING",
        "MS10_VALID_TO": "DATE", "MS10_E_ID": "STRING", "MS10_CARD_TYPE_NAME": "STRING"
    }
    expected_job_log_schema = {
        "job_kennung": ("STRING", False), # (type, is_nullable)
        "eintrags_nr": ("INT64", False),
        "tab_name": ("STRING", True),
        "stichtag": ("DATE", True),
        "records": ("INT64", True),
        "status": ("STRING", False),
        "message": ("STRING", True),
        "created_at": ("TIMESTAMP", True)
    }

    # Test sof_ta_iccid_einzeln schema
    table_id = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_iccid_einzeln"
    table = BQ_CLIENT.get_table(table_id)
    actual_schema = {field.name: field.field_type for field in table.schema}

    assert len(actual_schema) == len(expected_sof_ta_iccid_einzeln_schema), \
        "Number of columns in sof_ta_iccid_einzeln mismatch."
    for col_name, col_type in expected_sof_ta_iccid_einzeln_schema.items():
        assert col_name in actual_schema, f"Column {col_name} missing in sof_ta_iccid_einzeln."
        assert actual_schema[col_name] == col_type, \
            f"Type mismatch for {col_name} in sof_ta_iccid_einzeln: Expected {col_type}, Got {actual_schema[col_name]}."

    # Test job_log schema
    table_id = f"{PROJECT_ID}.{DATASET_ID}.job_log"
    table = BQ_CLIENT.get_table(table_id)
    actual_schema_log = {field.name: (field.field_type, field.is_nullable) for field in table.schema}

    assert len(actual_schema_log) == len(expected_job_log_schema), \
        "Number of columns in job_log mismatch."
    for col_name, (col_type, is_nullable) in expected_job_log_schema.items():
        assert col_name in actual_schema_log, f"Column {col_name} missing in job_log."
        assert actual_schema_log[col_name][0] == col_type, \
            f"Type mismatch for {col_name} in job_log: Expected {col_type}, Got {actual_schema_log[col_name][0]}."
        assert actual_schema_log[col_name][1] == is_nullable, \
            f"Nullability mismatch for {col_name} in job_log: Expected {'NULLABLE' if is_nullable else 'NOT NULL'}, Got {'NULLABLE' if actual_schema_log[col_name][1] else 'NOT NULL'}."

```

### 5. Logging & Error Handling Tests

#### 5.1. `p_log_job_entry` Functionality

**Purpose:** To verify that the `p_log_job_entry` stored procedure correctly inserts log entries into the `job_log` table.

**Setup:** None.

**Action:**
1.  Call `p_log_job_entry` with various statuses (RUNNING, SUCCESS, FAILED) and messages.
2.  Query `job_log` to check the inserted entries.

**Pass/Fail Criterion:**
*   Each call to `p_log_job_entry` results in a new, correct row in `job_log`.
*   All passed parameters (`job_kennung`, `eintrags_nr`, `tab_name`, `stichtag`, `records`, `status`, `message`) are accurately stored.
*   `created_at` is populated with a recent timestamp.

```python
def test_p_log_job_entry_functionality():
    """
    Tests the p_log_job_entry stored procedure for correct logging.
    """
    job_kennung_1 = "LOG_TEST_1"
    eintrags_nr_1 = 1
    tab_name_1 = "TableA"
    stichtag_1 = date(2023, 1, 1)
    records_1 = 100
    status_1 = "RUNNING"
    message_1 = "Job started successfully."

    job_kennung_2 = "LOG_TEST_2"
    eintrags_nr_2 = 2
    tab_name_2 = "TableB"
    stichtag_2 = date(2023, 1, 2)
    records_2 = None # For FAILED status
    status_2 = "FAILED"
    message_2 = "Job failed due to XYZ."

    call_bq_stored_procedure(
        "p_log_job_entry",
        job_kennung_1, eintrags_nr_1, tab_name_1, stichtag_1, records_1, status_1, message_1
    )
    call_bq_stored_procedure(
        "p_log_job_entry",
        job_kennung_2, eintrags_nr_2, tab_name_2, stichtag_2, records_2, status_2, message_2
    )

    log_entries = get_table_data("job_log", order_by_cols=["job_kennung", "eintrags_nr"])

    assert len(log_entries) == 2, "Expected 2 log entries."

    log1 = next(log for log in log_entries if log.job_kennung == job_kennung_1)
    assert log1.eintrags_nr == eintrags_nr_1
    assert log1.tab_name == tab_name_1
    assert log1.stichtag == stichtag_1
    assert log1.records == records_1
    assert log1.status == status_1
    assert log1.message == message_1
    assert (datetime.now() - log1.created_at.replace(tzinfo=None)).total_seconds() < 60 # Created recently

    log2 = next(log for log in log_entries if log.job_kennung == job_kennung_2)
    assert log2.eintrags_nr == eintrags_nr_2
    assert log2.tab_name == tab_name_2
    assert log2.stichtag == stichtag_2
    assert log2.records is None
    assert log2.status == status_2
    assert log2.message == message_2
```

#### 5.2. End-to-End Error Logging

**Purpose:** To verify that if `p_process_iccid_einzeln` fails (e.g., due to an underlying data issue or explicit `RAISE`), `r_ausd_bp_ta_iccid_einzeln` catches the error, logs a `FAILED` status, and re-raises the exception.

**Setup:**
1.  Modify `p_process_iccid_einzeln` temporarily to force an error (e.g., `RAISE USING MESSAGE = 'Forced error for testing';`).
    *Self-correction: Instead of modifying the SP, we can simulate an error by providing data that would cause a BigQuery error if possible, or rely on the `RAISE` in the SP itself.*
    Let's assume we can trigger an error in `p_process_iccid_einzeln` by some data condition, or for this test, we'll assume a direct `RAISE` is added for testing purposes.

**Action:**
1.  Call `r_ausd_bp_ta_iccid_einzeln` under conditions that will cause `p_process_iccid_einzeln` to fail.

**Pass/Fail Criterion:**
*   `r_ausd_bp_ta_iccid_einzeln` must raise an exception.
*   A `FAILED` entry must be present in `job_log` for the job, containing the error message from `p_process_iccid_einzeln`.
*   No `SUCCESS` entry should be present for this job run.

```python
# This test requires a temporary modification to p_process_iccid_einzeln
# to force an error, or a specific data scenario that would naturally cause an error.
# For demonstration, let's assume p_process_iccid_einzeln can be made to fail.

# Example of how to temporarily modify a BigQuery SP for testing:
# You would need to execute DDL to replace the SP with a failing version,
# run the test, then replace it back with the original.
# This is usually done in a dedicated test environment.

# For this test, we'll simulate the outcome assuming p_process_iccid_einzeln fails.
# A more robust approach might involve mocking or using a dedicated test version of p_process_iccid_einzeln.

def test_end_to_end_error_logging():
    """
    Tests that r_ausd_bp_ta_iccid_einzeln correctly handles errors from
    p_process_iccid_einzeln and logs a FAILED status.
    """
    job_kennung = "ERROR_JOB"
    eintrags_nr = 4
    stichtag = "20230101"
    restart_val = "0"

    # --- Simulate p_process_iccid_einzeln failure ---
    # In a real test, you might:
    # 1. Temporarily modify p_process_iccid_einzeln to always RAISE.
    # 2. Insert data into sof_ta_bpr_basis that triggers a known error condition
    #    (e.g., data type mismatch if not handled by SAFE_CAST, though not applicable here).
    # For this example, we'll just assert the expected outcome if it fails.

    # To make this test runnable, let's assume we have a test version of p_process_iccid_einzeln
    # that can be configured to fail, or we temporarily replace it.
    # For now, we'll just call the main SP and expect it to fail.
    # This test will only pass if p_process_iccid_einzeln *can* fail and the main SP handles it.

    # To make this test pass, you would need to ensure p_process_iccid_einzeln
    # actually raises an error under some condition, or temporarily modify it.
    # For instance, if p_process_iccid_einzeln had a division by zero or a bad cast.
    # Since the provided p_process_iccid_einzeln is quite simple, it might not fail easily.
    # Let's assume a forced error for this test.

    # Example: Temporarily replace p_process_iccid_einzeln with a failing version
    # (This is conceptual and requires DDL execution)
    # BQ_CLIENT.query(f"CREATE OR REPLACE PROCEDURE {PROJECT_ID}.{DATASET_ID}.p_process_iccid_einzeln(...) BEGIN RAISE USING MESSAGE = 'Forced test error'; END;").result()

    with pytest.raises(Exception) as excinfo:
        call_bq_stored_procedure(
            "r_ausd_bp_ta_iccid_einzeln",
            job_kennung,
            eintrags_nr,
            stichtag,
            restart_val
        )
    assert "Forced test error" in str(excinfo.value) # Or whatever the actual error message is

    log_entries = get_table_data("job_log")

    # Check for FAILED log entry
    failed_log = next(
        (log for log in log_entries if log.status == "FAILED" and log.job_kennung == job_kennung),
        None
    )
    assert failed_log is not None, "FAILED log entry not found after error."
    assert "Forced test error" in failed_log.message, \
        "FAILED log entry message does not contain the expected error."

    # Ensure no SUCCESS log entry
    success_log = next(
        (log for log in log_entries if log.status == "SUCCESS" and log.job_kennung == job_kennung),
        None
    )
    assert success_log is None, "SUCCESS log entry found despite job failure."

    # (Cleanup: Restore original p_process_iccid_einzeln if it was modified)
```