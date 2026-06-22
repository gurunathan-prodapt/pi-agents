As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_v_ta_c_bfc.ksh` to `isrpt.BERT_V_TA_C_BFC` in BigQuery. The core challenge is that this is an orchestration script, delegating actual data transformation to a sub-script (`k_ausd_v_ta_c_bfc.ksh`), which is a placeholder in the BigQuery migration. My tests will focus on the wrapper's behavior: parameter handling, logging, error orchestration, and interaction with the (mocked) core logic.

The tests are designed to prove behavioral equivalence, covering output parity, transformation correctness (of the orchestration logic), external system replacements (logging system), and data quality/schema assertions for the logging mechanism.

---

### **Test Setup Prerequisites (Pytest Fixtures)**

Before running any tests, ensure the BigQuery client is configured and the necessary tables/procedures exist. The `dw_job_log` table will be truncated before each test to ensure a clean state. Mock procedures for `isrpt.k_ausd_v_ta_c_bfc` will be used to simulate success and failure scenarios.

```python
import pytest
from google.cloud import bigquery
import os
import time
from datetime import datetime

# --- Configuration ---
# Replace with your GCP project ID
PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = "isrpt" # As per the generated code

# --- BigQuery Client Fixture ---
@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=PROJECT_ID)

# --- dw_job_log Table Setup/Teardown Fixture ---
@pytest.fixture(autouse=True)
def setup_and_teardown_dw_job_log(bq_client):
    """Ensures dw_job_log table is empty before and after each test."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.dw_job_log"
    print(f"\nTruncating table: {table_id}")
    bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()
    yield # Run the test
    print(f"Truncating table after test: {table_id}")
    bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()

# --- Helper Functions ---
def call_main_procedure(bq_client, p_h=None, p_s=None, p_l=None):
    """
    Calls the main BigQuery stored procedure BERT_V_TA_C_BFC and returns its SELECT results.
    Handles potential exceptions raised by the procedure.
    """
    params = []
    params.append(f"p_h => {f\"'{p_h}'\" if p_h is not None else 'NULL'}")
    params.append(f"p_s => {f\"'{p_s}'\" if p_s is not None else 'NULL'}")
    params.append(f"p_l => {f\"'{p_l}'\" if p_l is not None else 'NULL'}")

    param_str = ", ".join(params)
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_V_TA_C_BFC`({param_str});"
    print(f"Executing: {query}")
    try:
        job = bq_client.query(query)
        # Procedures that end with a SELECT statement will return results.
        # We need to fetch these results.
        rows = list(job.result())
        return rows
    except Exception as e:
        print(f"Procedure call failed: {e}")
        raise # Re-raise to allow pytest.raises to catch it

def get_log_entries(bq_client):
    """Fetches all entries from the dw_job_log table."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.dw_job_log"
    query = f"SELECT * FROM `{table_id}` ORDER BY created_at ASC, eintrags_nr ASC, log_level ASC;"
    rows = bq_client.query(query).result()
    return [dict(row) for row in rows]

def replace_k_ausd_v_ta_c_bfc(bq_client, content_sql):
    """Temporarily replaces the k_ausd_v_ta_c_bfc procedure with custom SQL."""
    proc_id = f"{PROJECT_ID}.{DATASET_ID}.k_ausd_v_ta_c_bfc"
    print(f"Replacing procedure: {proc_id}")
    bq_client.query(f"DROP PROCEDURE IF EXISTS `{proc_id}`;").result()
    bq_client.query(content_sql).result()

# --- Mock k_ausd_v_ta_c_bfc Procedures ---
ORIGINAL_K_AUSD_V_TA_C_BFC_SQL = """
CREATE OR REPLACE PROCEDURE `isrpt.k_ausd_v_ta_c_bfc`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr INT64
)
OPTIONS(
  description="Placeholder for the core logic of k_ausd_v_ta_c_bfc.ksh"
)
BEGIN
  INSERT INTO `isrpt.dw_job_log`
  (job_kennung, eintrags_nr, log_level, log_text, created_at)
  VALUES
  (p_job_kennung, p_eintrags_nr, 'I', 'Executing core logic for k_ausd_v_ta_c_bfc (placeholder)', CURRENT_TIMESTAMP());

  INSERT INTO `isrpt.dw_job_log`
  (job_kennung, eintrags_nr, log_level, log_text, created_at)
  VALUES
  (p_job_kennung, p_eintrags_nr, 'I', 'Core logic execution completed successfully (placeholder)', CURRENT_TIMESTAMP());
END;
"""

FAILING_K_AUSD_V_TA_C_BFC_SQL = """
CREATE OR REPLACE PROCEDURE `isrpt.k_ausd_v_ta_c_bfc`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr INT64
)
OPTIONS(
  description="MOCK: Failing core logic for k_ausd_v_ta_c_bfc.ksh"
)
BEGIN
  INSERT INTO `isrpt.dw_job_log`
  (job_kennung, eintrags_nr, log_level, log_text, created_at)
  VALUES
  (p_job_kennung, p_eintrags_nr, 'E', 'Simulated error in k_ausd_v_ta_c_bfc', CURRENT_TIMESTAMP());
  RAISE BQEXCEPTION('Simulated error in core script');
END;
"""

# --- k_ausd_v_ta_c_bfc Restore Fixture ---
@pytest.fixture(autouse=True)
def restore_k_ausd_v_ta_c_bfc(bq_client):
    """Ensures the original k_ausd_v_ta_c_bfc is in place before and after tests."""
    replace_k_ausd_v_ta_c_bfc(bq_client, ORIGINAL_K_AUSD_V_TA_C_BFC_SQL)
    yield # Run the test
    replace_k_ausd_v_ta_c_bfc(bq_client, ORIGINAL_K_AUSD_V_TA_C_BFC_SQL)

```

---

### **Test Case 1: Successful Execution with Valid Parameters**

**Purpose:**
To verify that the migrated BigQuery stored procedure `isrpt.BERT_V_TA_C_BFC` executes successfully when provided with all required parameters. This test ensures correct orchestration of logging and core procedures, accurate logging of the job's lifecycle in `dw_job_log`, and proper final status reporting.

**Covers:** Output parity (logging, final status), Transformation correctness (orchestration flow, variable handling), Data quality/row count/schema assertions (`dw_job_log` content).

**Setup:**
1.  The `isrpt.dw_job_log` table is empty (handled by fixture).
2.  The `isrpt.k_ausd_v_ta_c_bfc` procedure is set to its default (successful placeholder) implementation (handled by fixture).

**Action:**
Execute the `isrpt.BERT_V_TA_C_BFC` procedure with valid, non-NULL values for `p_s` and `p_l`, and `p_h` as NULL.

```python
def test_successful_execution(bq_client):
    # Action
    result_rows = call_main_procedure(bq_client, p_s='test_s_val', p_l='test_l_val')

    # Pass/Fail Criteria - Output Parity (final status)
    # The procedure's final SELECT statement should return 'OK'.
    assert len(result_rows) == 1, "Expected one row for final job status."
    assert result_rows[0].job_status == 'OK', "Final job status should be 'OK'."

    # Pass/Fail Criteria - Data Quality / Row Count / Schema Assertions (dw_job_log)
    log_entries = get_log_entries(bq_client)

    # Expected log messages in order of appearance
    expected_log_texts = [
        "Jobstart: ", # Will contain CURRENT_USER() and params
        "SetzeStichtagInfo",
        "Executing core logic for k_ausd_v_ta_c_bfc (placeholder)",
        "Core logic execution completed successfully (placeholder)",
        "Die Abarbeitung wurde ohne erkennbare Fehler beendet",
        "Job beendet - OK"
    ]

    assert len(log_entries) == len(expected_log_texts), \
        f"Expected {len(expected_log_texts)} log entries, but got {len(log_entries)}."

    # Verify log levels and partial text matches for each entry
    assert log_entries[0]['log_level'] == 'I'
    assert log_entries[0]['log_text'].startswith('Jobstart: ')
    assert 'test_s_val' in log_entries[0]['log_text']
    assert 'test_l_val' in log_entries[0]['log_text']

    assert log_entries[1]['log_level'] == 'I'
    assert log_entries[1]['log_text'] == 'SetzeStichtagInfo'
    # Verify stichtag format and value (current date)
    assert log_entries[1]['stichtag'] == datetime.now().strftime('%d%m%Y')

    assert log_entries[2]['log_level'] == 'I'
    assert log_entries[2]['log_text'] == 'Executing core logic for k_ausd_v_ta_c_bfc (placeholder)'

    assert log_entries[3]['log_level'] == 'I'
    assert log_entries[3]['log_text'] == 'Core logic execution completed successfully (placeholder)'

    assert log_entries[4]['log_level'] == 'I'
    assert log_entries[4]['log_text'] == 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'

    assert log_entries[5]['log_level'] == 'I'
    assert log_entries[5]['log_text'] == 'Job beendet - OK'

    # Verify eintrags_nr consistency and job_kennung
    first_eintrags_nr = log_entries[0]['eintrags_nr']
    assert all(entry['eintrags_nr'] == first_eintrags_nr for entry in log_entries), \
        "All log entries for a single job run should have the same eintrags_nr."
    assert first_eintrags_nr > 0, "eintrags_nr should be greater than 0 for a new job."
    assert all(entry['job_kennung'] == 'BERT_V_TA_C_BFC' for entry in log_entries), \
        "All log entries should have the correct job_kennung."

    # Verify the "stdout" messages (job details printout)
    # The BQ procedure's SELECT statements for job details are not easily captured
    # as separate result sets from the final 'job_status' SELECT.
    # If direct capture is needed, the procedure would need to insert these into a temp table.
    # For now, we rely on the log entries and final status.
```

---

### **Test Case 2: Help Message Display**

**Purpose:**
To verify that when the `-h` parameter is provided, the procedure correctly displays the usage information and exits immediately without performing any other job logic or logging.

**Covers:** Output parity (help message), Transformation correctness (parameter handling, early exit).

**Setup:**
1.  The `isrpt.dw_job_log` table is empty (handled by fixture).

**Action:**
Execute the `isrpt.BERT_V_TA_C_BFC` procedure with `p_h='h'`.

```python
def test_help_message_display(bq_client):
    # Action
    result_rows = call_main_procedure(bq_client, p_h='h')

    # Pass/Fail Criteria - Output Parity (help message content)
    # The BQ procedure returns a SELECT statement for the help message.
    assert len(result_rows) == 1, "Expected one row containing help message details."
    help_output = result_rows[0]

    assert help_output.Programm == 'Bindefristcache'
    assert help_output.Version == 'V1.0.0'
    assert help_output.Aufruf == 'Aufruf: Parameter'
    assert help_output.Hilfe == '-h zeigt diese Seite an'
    # Note: The BQ migration adds placeholders for -s and -l in the help message,
    # which were not explicitly in the original KSH usage function. This is a minor
    # behavioral difference but acceptable if documented.
    assert help_output['f0_'] == '-s <string>  -- Placeholder for option s' # Column names are f0_, f1_ etc if not aliased
    assert help_output['f1_'] == '-l <string>  -- Placeholder for option l'

    # Pass/Fail Criteria - Data Quality / Row Count (dw_job_log)
    log_entries = get_log_entries(bq_client)
    assert len(log_entries) == 0, "No logging should occur when the help message is displayed."
```

---

### **Test Case 3: Missing Required Parameter (-s)**

**Purpose:**
To verify that the procedure correctly identifies a missing required parameter (`-s`), logs an error, and exits gracefully without attempting to call the core processing script.

**Covers:** Transformation correctness (parameter validation, error handling), Data quality/row count/schema assertions (error logging).

**Setup:**
1.  The `isrpt.dw_job_log` table is empty (handled by fixture).

**Action:**
Execute the `isrpt.BERT_V_TA_C_BFC` procedure with `p_s=NULL` and a valid `p_l`.

```python
def test_missing_s_parameter(bq_client):
    # Action
    result_rows = call_main_procedure(bq_client, p_s=None, p_l='valid_l_val')

    # Pass/Fail Criteria - Output Parity (final status/usage message)
    # The procedure returns a SELECT statement for the usage message on parameter error.
    assert len(result_rows) == 1, "Expected one row for parameter error usage message."
    error_output = result_rows[0]
    assert error_output.action == 'usage'
    assert error_output.programm == 'Bindefristcache'
    assert error_output.version == 'V1.0.0'
    assert error_output.error_detail == 'Missing parameter: -s'

    # Pass/Fail Criteria - Data Quality / Row Count / Schema Assertions (dw_job_log)
    log_entries = get_log_entries(bq_client)
    assert len(log_entries) == 1, "Only the parameter error should be logged."

    error_entry = log_entries[0]
    assert error_entry['log_level'] == 'E'
    assert error_entry['err_nr'] == 193 # Error number for missing argument
    assert error_entry['err_arg'] == 's'
    assert error_entry['log_text'].startswith('Parameterfehler: Missing required parameter -s')
    assert error_entry['job_kennung'] == 'BERT_V_TA_C_BFC'
    assert error_entry['eintrags_nr'] == 0 # DW_EintragsNr is 0 before dwmsg_ermittle_nr is called
```

---

### **Test Case 4: Missing Required Parameter (-l)**

**Purpose:**
To verify that the procedure correctly identifies a missing required parameter (`-l`), logs an error, and exits gracefully without attempting to call the core processing script.

**Covers:** Transformation correctness (parameter validation, error handling), Data quality/row count/schema assertions (error logging).

**Setup:**
1.  The `isrpt.dw_job_log` table is empty (handled by fixture).

**Action:**
Execute the `isrpt.BERT_V_TA_C_BFC` procedure with a valid `p_s` and `p_l=NULL`.

```python
def test_missing_l_parameter(bq_client):
    # Action
    result_rows = call_main_procedure(bq_client, p_s='valid_s_val', p_l=None)

    # Pass/Fail Criteria - Output Parity (final status/usage message)
    assert len(result_rows) == 1, "Expected one row for parameter error usage message."
    error_output = result_rows[0]
    assert error_output.action == 'usage'
    assert error_output.programm == 'Bindefristcache'
    assert error_output.version == 'V1.0.0'
    assert error_output.error_detail == 'Missing parameter: -l'

    # Pass/Fail Criteria - Data Quality / Row Count / Schema Assertions (dw_job_log)
    log_entries = get_log_entries(bq_client)
    assert len(log_entries) == 1, "Only the parameter error should be logged."

    error_entry = log_entries[0]
    assert error_entry['log_level'] == 'E'
    assert error_entry['err_nr'] == 193
    assert error_entry['err_arg'] == 'l'
    assert error_entry['log_text'].startswith('Parameterfehler: Missing required parameter -l')
    assert error_entry['job_kennung'] == 'BERT_V_TA_C_BFC'
    assert error_entry['eintrags_nr'] == 0
```

---

### **Test Case 5: Error During Core Script Execution**

**Purpose:**
To verify that if the called core processing script (`isrpt.k_ausd_v_ta_c_bfc`) encounters an error and raises an exception, the wrapper procedure correctly catches it, logs the error in `dw_job_log`, and propagates the error status to the caller. This simulates the `trap ERR` mechanism of the KSH script.

**Covers:** Transformation correctness (error handling, `EXCEPTION WHEN ERROR THEN` block), Data quality/row count/schema assertions (error logging).

**Setup:**
1.  The `isrpt.dw_job_log` table is empty (handled by fixture).
2.  Temporarily replace `isrpt.k_ausd_v_ta_c_bfc` with the `FAILING_K_AUSD_V_TA_C_BFC_SQL` mock (handled by fixture setup).

**Action:**
Execute the `isrpt.BERT_V_TA_C_BFC` procedure with valid `p_s` and `p_l`. Expect the procedure call to raise an exception.

```python
def test_error_in_core_script(bq_client):
    # Setup: Replace k_ausd_v_ta_c_bfc with a failing version
    replace_k_ausd_v_ta_c_bfc(bq_client, FAILING_K_AUSD_V_TA_C_BFC_SQL)

    # Action: Expect the procedure call to raise an exception
    with pytest.raises(Exception) as excinfo:
        call_main_procedure(bq_client, p_s='test_s_val', p_l='test_l_val')

    # Pass/Fail Criteria - Output Parity (error propagation)
    # The exception message should indicate the job failure and the underlying error.
    assert "Execution failed for job BERT_V_TA_C_BFC: Simulated error in core script" in str(excinfo.value), \
        "The procedure should raise an exception with a descriptive error message."

    # Pass/Fail Criteria - Data Quality / Row Count / Schema Assertions (dw_job_log)
    log_entries = get_log_entries(bq_client)

    # Expected log messages: Jobstart, StichtagInfo, Core script error, Wrapper's AppError.
    expected_log_texts = [
        "Jobstart: ",
        "SetzeStichtagInfo",
        "Simulated error in k_ausd_v_ta_c_bfc", # From the failing core script
        "AppError: Abbruch - " # From the wrapper's EXCEPTION block
    ]

    assert len(log_entries) == len(expected_log_texts), \
        f"Expected {len(expected_log_texts)} log entries, but got {len(log_entries)}."

    # Verify log levels and partial text matches
    assert log_entries[0]['log_level'] == 'I'
    assert log_entries[0]['log_text'].startswith('Jobstart: ')

    assert log_entries[1]['log_level'] == 'I'
    assert log_entries[1]['log_text'] == 'SetzeStichtagInfo'

    assert log_entries[2]['log_level'] == 'E'
    assert log_entries[2]['log_text'] == 'Simulated error in k_ausd_v_ta_c_bfc'
    assert log_entries[2]['job_kennung'] == 'BERT_V_TA_C_BFC'
    first_eintrags_nr = log_entries[0]['eintrags_nr']
    assert log_entries[2]['eintrags_nr'] == first_eintrags_nr

    assert log_entries[3]['log_level'] == 'E'
    assert log_entries[3]['log_text'].startswith('AppError: Abbruch -')
    assert log_entries[3]['job_kennung'] == 'BERT_V_TA_C_BFC'
    assert log_entries[3]['eintrags_nr'] == first_eintrags_nr

    # Verify that success messages and status update were NOT logged
    assert not any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in entry['log_text'] for entry in log_entries), \
        "Success message should not be logged on error."
    assert not any("Job beendet - OK" in entry['log_text'] for entry in log_entries), \
        "Job OK status should not be logged on error."

    # The final SELECT v_status AS job_status will not be reached if RAISE is called,
    # so no result_rows are expected from the procedure call itself.
```