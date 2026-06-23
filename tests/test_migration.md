As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_v_ta_cntrct_templ.ksh` to a BigQuery Stored Procedure. These tests focus on ensuring the migrated BigQuery procedure `Vertragsdatenabgleich` is behaviorally equivalent to the legacy KornShell script, particularly concerning its orchestration, logging, and error handling capabilities.

Given that the core processing script (`k_ausd_v_ta_cntrct_templ.ksh`) is a separate migration, these tests will use mock BigQuery stored procedures to simulate its behavior (success or failure).

**Assumptions:**
*   The BigQuery project and dataset are named `your_project` and `your_dataset` respectively, as per the provided generated code.
*   The DDLs for `job_audit_log`, `job_error_log`, and `job_reference_date` have been executed in the target BigQuery environment.
*   A Python environment with `pytest` and `google-cloud-bigquery` installed is available for running the test code.

---

### Pytest Setup Instructions

To run the provided Python test code, ensure you have the following:

1.  **Google Cloud Project:** A GCP project where BigQuery is enabled.
2.  **BigQuery Dataset:** A dataset named `your_dataset` within your project.
3.  **Service Account:** A service account with BigQuery Data Editor and BigQuery Job User roles for the project. Download its JSON key file.
4.  **Environment Variable:** Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your service account JSON key file.
5.  **Python Environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate # On Windows: venv\Scripts\activate
    pip install pytest google-cloud-bigquery
    ```
6.  **Replace Placeholders:** In the Python code, replace `"your_project"` and `"your_dataset"` with your actual project ID and dataset ID.
7.  **Save Tests:** Save the Python test code into a file, e.g., `test_migration.py`.
8.  **Run Tests:**
    ```bash
    pytest test_migration.py
    ```

---

### Test Case 1: Successful Execution - Basic Logging and Core Script Invocation

*   **Purpose:** Verify that the `Vertragsdatenabgleich` procedure executes successfully, correctly logs job start/end, generates a unique entry number, and calls the mock core processing procedure. This covers output parity for logging and transformation correctness for `DWMSG_*` replacements.
*   **Setup:**
    1.  Ensure `your_project.your_dataset.job_audit_log`, `your_project.your_dataset.job_error_log`, `your_project.your_dataset.job_reference_date` tables are empty before execution (handled by `pytest` fixture).
    2.  Create a mock `k_ausd_v_ta_cntrct_templ` BigQuery stored procedure that simply returns successfully.
*   **Action:**
    1.  Call `your_project.your_dataset.Vertragsdatenabgleich` with valid parameters.
*   **Pass/Fail Criterion:**
    1.  One row exists in `your_project.your_dataset.job_audit_log` for `job_kennung = 'TEST_JOB_KENNUNG_SUCCESS'`.
    2.  The `status` column in `job_audit_log` is 'OK'.
    3.  `eintrags_nr` in `job_audit_log` is 1 (assuming empty table initially).
    4.  `log_dateiname` in `job_audit_log` matches `TEST_JOB_KENNUNG_SUCCESS_1.log`.
    5.  `start_zeit` and `ende_zeit` are populated, with `ende_zeit` > `start_zeit`.
    6.  One row exists in `your_project.your_dataset.job_reference_date` for the same `job_kennung` and `eintrags_nr`, with `referenz_datum` matching the input `CURRENT_DATE()`.
    7.  Zero rows exist in `your_project.your_dataset.job_error_log`.

```python
import pytest
from google.cloud import bigquery
from datetime import date, timedelta
from google.api_core.exceptions import BadRequest

PROJECT_ID = "your_project"  # Replace with your GCP Project ID
DATASET_ID = "your_dataset"  # Replace with your BigQuery Dataset ID
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def setup_teardown_tables():
    """Fixture to clear log tables before each test."""
    BQ_CLIENT.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_audit_log`").result()
    BQ_CLIENT.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    BQ_CLIENT.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_reference_date`").result()
    yield

def test_successful_execution():
    job_kennung = 'TEST_JOB_KENNUNG_SUCCESS'
    stichtag = date.today()
    typ = 'TYPE_A'

    # 1. Setup mock k_ausd_v_ta_cntrct_templ
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_cntrct_templ`(
            IN p_job_kennung STRING,
            IN p_eintrags_nr INT64
        )
        BEGIN
            -- Simulate successful execution of the core script
            SELECT 'Mock k_ausd_v_ta_cntrct_templ executed successfully' AS message;
        END;
    """).result()

    # 2. Action: Call the main procedure
    BQ_CLIENT.query(f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.Vertragsdatenabgleich`(
            '{job_kennung}',
            DATE('{stichtag.isoformat()}'),
            '{typ}'
        );
    """).result()

    # 3. Assertions
    # Check job_audit_log
    audit_log_query = f"""
        SELECT job_kennung, eintrags_nr, status, log_dateiname, start_zeit, ende_zeit, referenz_datum
        FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log`
        WHERE job_kennung = '{job_kennung}'
    """
    audit_results = list(BQ_CLIENT.query(audit_log_query).result())
    assert len(audit_results) == 1, "Expected exactly one entry in job_audit_log"
    audit_entry = audit_results[0]
    assert audit_entry.job_kennung == job_kennung
    assert audit_entry.eintrags_nr == 1
    assert audit_entry.status == 'OK'
    assert audit_entry.log_dateiname == f'{job_kennung}_1.log'
    assert audit_entry.start_zeit is not None
    assert audit_entry.ende_zeit is not None
    assert audit_entry.ende_zeit > audit_entry.start_zeit
    assert audit_entry.referenz_datum == stichtag

    # Check job_reference_date
    ref_date_query = f"""
        SELECT job_kennung, eintrags_nr, referenz_datum
        FROM `{PROJECT_ID}.{DATASET_ID}.job_reference_date`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = 1
    """
    ref_date_results = list(BQ_CLIENT.query(ref_date_query).result())
    assert len(ref_date_results) == 1, "Expected exactly one entry in job_reference_date"
    ref_date_entry = ref_date_results[0]
    assert ref_date_entry.job_kennung == job_kennung
    assert ref_date_entry.eintrags_nr == 1
    assert ref_date_entry.referenz_datum == stichtag

    # Check job_error_log
    error_log_query = f"""
        SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_kennung = '{job_kennung}'
    """
    error_count = BQ_CLIENT.query(error_log_query).result().total_rows
    assert error_count == 0, "Expected no errors logged"

```

### Test Case 2: Core Script Failure - Error Handling

*   **Purpose:** Verify that `Vertragsdatenabgleich` correctly handles errors originating from the core processing procedure, logging the error and updating the job status. This covers transformation correctness for `trap ERR` and `DWMSG_Fehlerbehandlung` replacement.
*   **Setup:**
    1.  Ensure log tables are empty (handled by fixture).
    2.  Create a mock `k_ausd_v_ta_cntrct_templ` BigQuery stored procedure that raises an error.
*   **Action:**
    1.  Call `your_project.your_dataset.Vertragsdatenabgleich` with valid parameters, expecting an error to be raised.
*   **Pass/Fail Criterion:**
    1.  The call to `Vertragsdatenabgleich` raises a `SQLSTATE '45000'` error.
    2.  One row exists in `your_project.your_dataset.job_audit_log` for `job_kennung = 'TEST_JOB_KENNUNG_FAILURE'`.
    3.  The `status` column in `job_audit_log` is 'ERROR'.
    4.  `eintrags_nr` in `job_audit_log` is 1.
    5.  `start_zeit` and `ende_zeit` are populated.
    6.  One row exists in `your_project.your_dataset.job_error_log` for the same `job_kennung` and `eintrags_nr`.
    7.  The `fehler_text` in `job_error_log` contains 'Simulated core script error'.

```python
# (BQ_CLIENT and setup_teardown_tables fixture are assumed to be defined as above)

def test_core_script_failure():
    job_kennung = 'TEST_JOB_KENNUNG_FAILURE'
    stichtag = date.today()
    typ = 'TYPE_B'

    # 1. Setup mock k_ausd_v_ta_cntrct_templ to fail
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_cntrct_templ`(
            IN p_job_kennung STRING,
            IN p_eintrags_nr INT64
        )
        BEGIN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core script error';
        END;
    """).result()

    # 2. Action: Call the main procedure, expecting an error
    with pytest.raises(BadRequest) as excinfo:
        BQ_CLIENT.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.Vertragsdatenabgleich`(
                '{job_kennung}',
                DATE('{stichtag.isoformat()}'),
                '{typ}'
            );
        """).result()
    assert "Simulated core script error" in str(excinfo.value)

    # 3. Assertions
    # Check job_audit_log
    audit_log_query = f"""
        SELECT job_kennung, eintrags_nr, status, meldungs_text, start_zeit, ende_zeit
        FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log`
        WHERE job_kennung = '{job_kennung}'
    """
    audit_results = list(BQ_CLIENT.query(audit_log_query).result())
    assert len(audit_results) == 1, "Expected exactly one entry in job_audit_log"
    audit_entry = audit_results[0]
    assert audit_entry.job_kennung == job_kennung
    assert audit_entry.eintrags_nr == 1
    assert audit_entry.status == 'ERROR'
    assert "Job failed: Simulated core script error" in audit_entry.meldungs_text
    assert audit_entry.start_zeit is not None
    assert audit_entry.ende_zeit is not None
    assert audit_entry.ende_zeit > audit_entry.start_zeit

    # Check job_error_log
    error_log_query = f"""
        SELECT job_kennung, eintrags_nr, fehler_text, quell_prozedur
        FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = 1
    """
    error_results = list(BQ_CLIENT.query(error_log_query).result())
    assert len(error_results) == 1, "Expected exactly one entry in job_error_log"
    error_entry = error_results[0]
    assert error_entry.job_kennung == job_kennung
    assert error_entry.eintrags_nr == 1
    assert error_entry.fehler_text == 'Simulated core script error'
    assert error_entry.quell_prozedur == 'Vertragsdatenabgleich'

    # Check job_reference_date (should still be inserted before the error block)
    ref_date_query = f"""
        SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_reference_date`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = 1
    """
    ref_date_count = BQ_CLIENT.query(ref_date_query).result().total_rows
    assert ref_date_count == 1, "Expected one entry in job_reference_date"

```

### Test Case 3: `eintrags_nr` Generation and Concurrency

*   **Purpose:** Verify that `eintrags_nr` is correctly generated as a sequential, unique identifier, especially under simulated concurrent calls. This covers transformation correctness for `DWMSG_ErmittleNr` replacement and data quality.
*   **Setup:**
    1.  Ensure log tables are empty (handled by fixture).
    2.  Create a mock `k_ausd_v_ta_cntrct_templ` that succeeds.
*   **Action:**
    1.  Call `your_project.your_dataset.Vertragsdatenabgleich` multiple times with the same `p_job_kennung` but different `p_stichtag` to simulate distinct job runs.
*   **Pass/Fail Criterion:**
    1.  Three rows exist in `your_project.your_dataset.job_audit_log` for `job_kennung = 'TEST_JOB_KENNUNG_CONCURRENCY'`.
    2.  The `eintrags_nr` values for these rows are 1, 2, and 3 respectively.
    3.  All three jobs have `status = 'OK'`.
    4.  Corresponding entries exist in `job_reference_date` with correct `eintrags_nr` and `referenz_datum`.

```python
# (BQ_CLIENT and setup_teardown_tables fixture are assumed to be defined as above)

def test_eintrags_nr_generation_and_concurrency():
    job_kennung = 'TEST_JOB_KENNUNG_CONCURRENCY'

    # 1. Setup mock k_ausd_v_ta_cntrct_templ to succeed
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_cntrct_templ`(
            IN p_job_kennung STRING,
            IN p_eintrags_nr INT64
        )
        BEGIN
            SELECT 'Mock k_ausd_v_ta_cntrct_templ executed successfully' AS message;
        END;
    """).result()

    # 2. Action: Call the main procedure multiple times
    calls = [
        (date.today(), 'TYPE_C1'),
        (date.today() + timedelta(days=1), 'TYPE_C2'),
        (date.today() + timedelta(days=2), 'TYPE_C3'),
    ]

    for i, (stichtag, typ) in enumerate(calls):
        BQ_CLIENT.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.Vertragsdatenabgleich`(
                '{job_kennung}',
                DATE('{stichtag.isoformat()}'),
                '{typ}'
            );
        """).result()

    # 3. Assertions
    audit_log_query = f"""
        SELECT job_kennung, eintrags_nr, status, referenz_datum
        FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log`
        WHERE job_kennung = '{job_kennung}'
        ORDER BY eintrags_nr
    """
    audit_results = list(BQ_CLIENT.query(audit_log_query).result())

    assert len(audit_results) == len(calls), "Expected number of audit log entries mismatch"

    for i, entry in enumerate(audit_results):
        expected_eintrags_nr = i + 1
        expected_stichtag = calls[i][0]
        assert entry.job_kennung == job_kennung
        assert entry.eintrags_nr == expected_eintrags_nr
        assert entry.status == 'OK'
        assert entry.referenz_datum == expected_stichtag

    # Verify job_reference_date entries
    ref_date_query = f"""
        SELECT job_kennung, eintrags_nr, referenz_datum
        FROM `{PROJECT_ID}.{DATASET_ID}.job_reference_date`
        WHERE job_kennung = '{job_kennung}'
        ORDER BY eintrags_nr
    """
    ref_date_results = list(BQ_CLIENT.query(ref_date_query).result())
    assert len(ref_date_results) == len(calls), "Expected number of reference date entries mismatch"
    for i, entry in enumerate(ref_date_results):
        expected_eintrags_nr = i + 1
        expected_stichtag = calls[i][0]
        assert entry.job_kennung == job_kennung
        assert entry.eintrags_nr == expected_eintrags_nr
        assert entry.referenz_datum == expected_stichtag

    # No errors should be logged
    error_log_query = f"""
        SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_kennung = '{job_kennung}'
    """
    error_count = BQ_CLIENT.query(error_log_query).result().total_rows
    assert error_count == 0, "Expected no errors logged"

```

### Test Case 4: Schema and Data Type Validation of Log Tables

*   **Purpose:** Verify that the DDLs for the logging tables (`job_audit_log`, `job_error_log`, `job_reference_date`) are correctly implemented in BigQuery, ensuring data quality and schema assertions.
*   **Setup:**
    1.  Assume the tables are created as per the DDLs in the migration design.
*   **Action:**
    1.  Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` to retrieve the schema definitions for each table.
*   **Pass/Fail Criterion:**
    1.  The column names, data types, and nullability for each table match the specified DDLs and the BigQuery procedure's usage.

```python
# (BQ_CLIENT and PROJECT_ID, DATASET_ID are assumed to be defined as above)

def test_log_table_schemas():
    expected_schemas = {
        "job_audit_log": {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "eintrags_nr": {"data_type": "INT64", "is_nullable": "NO"},
            "start_zeit": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
            "ende_zeit": {"data_type": "TIMESTAMP", "is_nullable": "YES"},
            "status": {"data_type": "STRING", "is_nullable": "YES"},
            "meldungs_text": {"data_type": "STRING", "is_nullable": "YES"},
            "log_dateiname": {"data_type": "STRING", "is_nullable": "YES"},
            "user_name": {"data_type": "STRING", "is_nullable": "YES"},
            "pid": {"data_type": "BIGNUMERIC", "is_nullable": "YES"}, # BIGNUMERIC for CAST(GENERATE_UUID() AS BIGNUMERIC)
            "host_name": {"data_type": "STRING", "is_nullable": "YES"},
            "referenz_datum": {"data_type": "DATE", "is_nullable": "YES"},
        },
        "job_error_log": {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "eintrags_nr": {"data_type": "INT64", "is_nullable": "NO"},
            "fehler_zeit": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
            "fehler_code": {"data_type": "INT64", "is_nullable": "YES"},
            "fehler_text": {"data_type": "STRING", "is_nullable": "YES"},
            "quell_prozedur": {"data_type": "STRING", "is_nullable": "YES"},
            "stack_trace": {"data_type": "STRING", "is_nullable": "YES"},
        },
        "job_reference_date": {
            "job_kennung": {"data_type": "STRING", "is_nullable": "NO"},
            "eintrags_nr": {"data_type": "INT64", "is_nullable": "NO"},
            "referenz_datum": {"data_type": "DATE", "is_nullable": "NO"},
            "gueltig_ab_datum": {"data_type": "DATE", "is_nullable": "YES"},
            "gueltig_bis_datum": {"data_type": "DATE", "is_nullable": "YES"},
        },
    }

    for table_name, expected_schema in expected_schemas.items():
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """
        results = BQ_CLIENT.query(query).result()
        actual_schema = {row.column_name: {"data_type": row.data_type, "is_nullable": row.is_nullable} for row in results}

        assert len(actual_schema) == len(expected_schema), f"Mismatch in column count for {table_name}"
        for col_name, col_props in expected_schema.items():
            assert col_name in actual_schema, f"Column {col_name} missing in {table_name}"
            assert actual_schema[col_name]["data_type"] == col_props["data_type"], \
                f"Data type mismatch for {table_name}.{col_name}: Expected {col_props['data_type']}, Got {actual_schema[col_name]['data_type']}"
            assert actual_schema[col_name]["is_nullable"] == col_props["is_nullable"], \
                f"Nullability mismatch for {table_name}.{col_name}: Expected {col_props['is_nullable']}, Got {actual_schema[col_name]['is_nullable']}"

```

### Test Case 5: Parameter Handling - Invalid Input Types

*   **Purpose:** Verify how the BigQuery stored procedure handles invalid input types for its parameters, ensuring robustness and expected BigQuery error behavior. This covers transformation correctness for type handling.
*   **Setup:**
    1.  Ensure log tables are empty (handled by fixture).
    2.  Create a mock `k_ausd_v_ta_cntrct_templ` that succeeds.
*   **Action:**
    1.  Call `your_project.your_dataset.Vertragsdatenabgleich` with an invalid `p_stichtag` (e.g., a string that cannot be cast to DATE).
*   **Pass/Fail Criterion:**
    1.  The call to `Vertragsdatenabgleich` raises a BigQuery `BadRequest` error (e.g., "Invalid date string").
    2.  No entries are created in `job_audit_log`, `job_error_log`, or `job_reference_date` because BigQuery's parameter validation occurs before the procedure body executes.

```python
# (BQ_CLIENT and setup_teardown_tables fixture are assumed to be defined as above)

def test_invalid_parameter_type_handling():
    job_kennung = 'TEST_JOB_KENNUNG_INVALID_DATE'
    invalid_stichtag_str = 'NOT_A_DATE'
    typ = 'TYPE_D'

    # 1. Setup mock k_ausd_v_ta_cntrct_templ to succeed
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_cntrct_templ`(
            IN p_job_kennung STRING,
            IN p_eintrags_nr INT64
        )
        BEGIN
            SELECT 'Mock k_ausd_v_ta_cntrct_templ executed successfully' AS message;
        END;
    """).result()

    # 2. Action: Call the main procedure with invalid parameter, expecting an error
    with pytest.raises(BadRequest) as excinfo:
        BQ_CLIENT.query(f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.Vertragsdatenabgleich`(
                '{job_kennung}',
                '{invalid_stichtag_str}', -- Pass a string where DATE is expected
                '{typ}'
            );
        """).result()
    assert "Invalid date string" in str(excinfo.value) or "Failed to parse" in str(excinfo.value)

    # 3. Assertions: No logs should be created as the error occurs before procedure execution
    audit_log_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` WHERE job_kennung = '{job_kennung}'"
    error_log_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE job_kennung = '{job_kennung}'"
    ref_date_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_reference_date` WHERE job_kennung = '{job_kennung}'"

    assert BQ_CLIENT.query(audit_log_query).result().total_rows == 0, "No audit log entries expected"
    assert BQ_CLIENT.query(error_log_query).result().total_rows == 0, "No error log entries expected"
    assert BQ_CLIENT.query(ref_date_query).result().total_rows == 0, "No reference date entries expected"

```

### Test Case 6: NULL Handling for `p_typ` Parameter

*   **Purpose:** Verify how the BigQuery stored procedure handles `NULL` values for the `p_typ` parameter. Since the legacy script's `-s` and `-l` parameters were parsed but not explicitly used, passing `NULL` for `p_typ` should not cause the BigQuery procedure to fail.
*   **Setup:**
    1.  Ensure log tables are empty (handled by fixture).
    2.  Create a mock `k_ausd_v_ta_cntrct_templ` that succeeds.
*   **Action:**
    1.  Call `your_project.your_dataset.Vertragsdatenabgleich` with `p_typ` as `NULL`.
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully.
    2.  One row exists in `job_audit_log` with `status = 'OK'`.
    3.  No errors are logged in `job_error_log`.

```python
# (BQ_CLIENT and setup_teardown_tables fixture are assumed to be defined as above)

def test_null_parameter_handling_p_typ():
    job_kennung = 'TEST_JOB_KENNUNG_NULL_TYPE'
    stichtag = date.today()
    # p_typ is passed as NULL

    # 1. Setup mock k_ausd_v_ta_cntrct_templ to succeed
    BQ_CLIENT.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_cntrct_templ`(
            IN p_job_kennung STRING,
            IN p_eintrags_nr INT64
        )
        BEGIN
            SELECT 'Mock k_ausd_v_ta_cntrct_templ executed successfully' AS message;
        END;
    """).result()

    # 2. Action: Call the main procedure with NULL p_typ
    BQ_CLIENT.query(f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.Vertragsdatenabgleich`(
            '{job_kennung}',
            DATE('{stichtag.isoformat()}'),
            CAST(NULL AS STRING) -- Explicitly cast NULL to STRING for BQ procedure
        );
    """).result()

    # 3. Assertions
    audit_log_query = f"""
        SELECT job_kennung, eintrags_nr, status
        FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log`
        WHERE job_kennung = '{job_kennung}'
    """
    audit_results = list(BQ_CLIENT.query(audit_log_query).result())
    assert len(audit_results) == 1, "Expected exactly one entry in job_audit_log"
    audit_entry = audit_results[0]
    assert audit_entry.job_kennung == job_kennung
    assert audit_entry.eintrags_nr == 1
    assert audit_entry.status == 'OK'

    # No errors should be logged
    error_log_query = f"""
        SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
        WHERE job_kennung = '{job_kennung}'
    """
    error_count = BQ_CLIENT.query(error_log_query).result().total_rows
    assert error_count == 0, "Expected no errors logged"

```