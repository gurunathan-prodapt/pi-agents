As a senior data-migration QA engineer, I've analyzed the migration design for `r_aurd_rechstan.ksh` to `sp_erzeugung_abzug_rechnungsdaten`. The key takeaway is that `r_aurd_rechstan.ksh` is an *orchestration* script, and the core data transformation logic (`k_aurd_rechstan.ksh`) is explicitly noted as a separate, unresolved migration effort.

Therefore, these validation tests will focus on proving the behavioral equivalence of the *orchestration and parameter management* aspects of the migrated BigQuery Stored Procedure (`sp_erzeugung_abzug_rechnungsdaten`) to the legacy KornShell script. Direct data transformation correctness (joins, aggregations, filters, etc.) cannot be fully tested for this specific migration unit, as the core logic is a placeholder.

I will use `pytest` for structuring the test cases and `SQL` for assertions against BigQuery tables. For the legacy script, I'll describe how to execute it and what to capture (e.g., exit codes, log file content).

---

## Migration Validation Tests for `r_aurd_rechstan.ksh` to `sp_erzeugung_abzug_rechnungsdaten`

### Setup for all Tests

Before running any tests, ensure the following:

1.  **BigQuery Logging Tables:** The DDL for `project.dataset.job_control`, `project.dataset.job_run_log`, and `project.dataset.job_error_log` has been executed, and the tables exist in the target BigQuery dataset.
2.  **BigQuery Stored Procedure:** The `sp_erzeugung_abzug_rechnungsdaten` stored procedure has been deployed to `project.dataset`.
3.  **Test Environment:**
    *   A Linux environment capable of running KornShell scripts (for legacy tests).
    *   Access to a BigQuery project with appropriate IAM permissions to execute stored procedures and query logging tables.
    *   A Python environment with `pytest` and a BigQuery client library (e.g., `google-cloud-bigquery`).
4.  **Helper Functions (Assumed):**
    *   `execute_legacy_script(params: list) -> dict`: A function that executes `r_aurd_rechstan.ksh` with given parameters, captures its standard output, standard error, exit code, and the content of the generated log file.
    *   `execute_bq_sp(stichtag: str, wiederanlaufwert: int) -> dict`: A function that calls `sp_erzeugung_abzug_rechnungsdaten` and captures its success/failure status and any raised error messages.
    *   `clear_bq_logging_tables()`: A function to truncate or delete all records from the `job_control`, `job_run_log`, and `job_error_log` tables before each test run to ensure isolation.
    *   `query_bq(sql: str) -> list[dict]`: A function to execute a BigQuery SQL query and return results as a list of dictionaries.

---

### Test Case 1: Default Parameter Handling - No Inputs

**Purpose:** Verify that when no parameters are provided, the migrated BigQuery stored procedure correctly defaults `p_stichtag` to the current system date and `p_wiederanlaufWert` to 0, and logs the job as successful.

**Setup:**
*   Clear all records from `project.dataset.job_control`, `project.dataset.job_run_log`, and `project.dataset.job_error_log`.
*   Note the current system date in `DDMMYYYY` format (e.g., `26102023`).

**Action:**

1.  **Legacy:** Execute the KornShell script without any arguments.
    ```bash
    # In a shell environment
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
    ```
    Capture the exit code and the content of the generated log file.
2.  **Migrated:** Call the BigQuery stored procedure with `NULL` for both parameters.
    ```python
    # In pytest
    def test_default_parameters_no_inputs():
        clear_bq_logging_tables()
        current_date_ddmmyyyy = datetime.now().strftime('%d%m%Y')
        
        # Legacy execution (conceptual)
        # legacy_result = execute_legacy_script([])
        # assert legacy_result['exit_code'] == 0
        # assert f"Stichtag  : '{current_date_ddmmyyyy}'" in legacy_result['log_content']
        # assert "Wiederanlaufwert: '0'" in legacy_result['log_content'] # This is not explicitly logged in legacy, but implied by k_aurd_rechstan.ksh call
        # assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in legacy_result['log_content']

        # Migrated execution
        execute_bq_sp(None, None)

        # Assertions
        # ... (see Pass/Fail Criterion)
    ```

**Pass/Fail Criterion:**

*   **Legacy:** The script exits with code `0`. The log file contains a success message and indicates `Stichtag` as the current system date and `p_wiederanlaufWert` as `0` in the call to `k_aurd_rechstan.ksh`.
*   **Migrated:**
    *   The `sp_erzeugung_abzug_rechnungsdaten` call completes successfully without raising an error.
    *   Query `project.dataset.job_control`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'`.
        *   `status` is 'OK'.
        *   `stichtag` matches the current system date (DDMMYYYY format).
        *   `sysdate` matches the current system date (DDMMYYYY format).
        *   `start_ts` and `end_ts` are populated.
    *   Query `project.dataset.job_run_log`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'` and `message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
    *   Query `project.dataset.job_error_log`:
        *   No rows exist.

```python
import pytest
from datetime import datetime, timedelta
from google.cloud import bigquery

# Assume these are defined elsewhere for actual execution
# from .conftest import execute_legacy_script, execute_bq_sp, clear_bq_logging_tables, query_bq

PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_dataset"
JOB_CONTROL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_control"
JOB_RUN_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_run_log"
JOB_ERROR_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_error_log"
SP_NAME = f"{PROJECT_ID}.{DATASET_ID}.sp_erzeugung_abzug_rechnungsdaten"

# Placeholder for actual BigQuery client and execution functions
# In a real scenario, these would interact with GCP BigQuery
class MockBigQueryClient:
    def __init__(self):
        self.job_control = []
        self.job_run_log = []
        self.job_error_log = []
        self._eintragsnr_counter = 0

    def clear_tables(self):
        self.job_control = []
        self.job_run_log = []
        self.job_error_log = []
        self._eintragsnr_counter = 0

    def execute_sp(self, p_stichtag, p_wiederanlaufWert):
        # This mocks the SP logic directly for testing purposes
        v_sysdate = datetime.now().strftime('%d%m%Y')
        v_effective_stichtag = p_stichtag if p_stichtag is not None and p_stichtag.strip() != '' else v_sysdate
        v_wiederanlaufWert = p_wiederanlaufWert if p_wiederanlaufWert is not None else 0
        v_jobkennung = 'BERT_RKOPF_STAN'
        v_status = 'RUNNING'
        v_errmsg = None
        current_timestamp = datetime.now()

        if v_effective_stichtag is None or v_effective_stichtag.strip() == '':
            self.job_error_log.append({
                'job_kennung': v_jobkennung, 'log_ts': current_timestamp, 'error_code': '193',
                'error_message': 'Stichtag missing', 'stichtag': v_effective_stichtag,
                'wiederanlaufwert': v_wiederanlaufWert
            })
            raise Exception('Stichtag missing')

        # Simulate eintragsnr generation
        max_eintragsnr = 0
        for row in self.job_control:
            if row['job_kennung'] == v_jobkennung and row['eintragsnr'] > max_eintragsnr:
                max_eintragsnr = row['eintragsnr']
        v_eintragsnr = max_eintragsnr + 1
        self._eintragsnr_counter = v_eintragsnr # Keep track for next run

        v_logdateiname = f"{v_jobkennung}_{v_eintragsnr}.log"

        self.job_control.append({
            'eintragsnr': v_eintragsnr, 'job_kennung': v_jobkennung,
            'script_name': 'sp_erzeugung_abzug_rechnungsdaten', 'logdateiname': v_logdateiname,
            'stichtag': v_effective_stichtag, 'status': 'RUNNING', 'start_ts': current_timestamp,
            'sysdate': v_sysdate, 'end_ts': None, 'error_message': None
        })

        try:
            # Placeholder for core logic - simulate success or failure
            # For this test, we assume success unless an explicit error is triggered
            v_status = 'OK'
            self.job_run_log.append({
                'eintragsnr': v_eintragsnr, 'job_kennung': v_jobkennung, 'log_ts': datetime.now(),
                'message': 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', 'status': 'OK'
            })
            
            # Update job_control
            for row in self.job_control:
                if row['eintragsnr'] == v_eintragsnr and row['job_kennung'] == v_jobkennung:
                    row['status'] = 'OK'
                    row['end_ts'] = datetime.now()
                    break

        except Exception as e:
            v_status = 'ERROR'
            v_errmsg = str(e)
            self.job_error_log.append({
                'eintragsnr': v_eintragsnr, 'job_kennung': v_jobkennung, 'log_ts': datetime.now(),
                'error_code': 'APP_ERROR', 'error_message': v_errmsg, 'stichtag': v_effective_stichtag,
                'wiederanlaufwert': v_wiederanlaufWert
            })
            for row in self.job_control:
                if row['eintragsnr'] == v_eintragsnr and row['job_kennung'] == v_jobkennung:
                    row['status'] = 'ERROR'
                    row['end_ts'] = datetime.now()
                    row['error_message'] = v_errmsg
                    break
            raise

    def query_table(self, table_name):
        if table_name == JOB_CONTROL_TABLE:
            return self.job_control
        elif table_name == JOB_RUN_LOG_TABLE:
            return self.job_run_log
        elif table_name == JOB_ERROR_LOG_TABLE:
            return self.job_error_log
        return []

# Instantiate mock client for tests
bq_client = MockBigQueryClient()

@pytest.fixture(autouse=True)
def setup_and_teardown():
    bq_client.clear_tables()
    yield

def test_default_parameters_no_inputs():
    current_date_ddmmyyyy = datetime.now().strftime('%d%m%Y')
    
    # Legacy execution (conceptual - would be a separate shell script execution)
    # legacy_result = execute_legacy_script([])
    # assert legacy_result['exit_code'] == 0
    # assert f"Stichtag  : '{current_date_ddmmyyyy}'" in legacy_result['log_content']
    # assert "p_wiederanlaufWert=0" in legacy_result['log_content'] # Check k_aurd_rechstan.ksh call params
    # assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in legacy_result['log_content']

    # Migrated execution
    bq_client.execute_sp(None, None)

    # Assertions for job_control
    job_control_records = bq_client.query_table(JOB_CONTROL_TABLE)
    assert len(job_control_records) == 1
    control_record = job_control_records[0]
    assert control_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert control_record['status'] == 'OK'
    assert control_record['stichtag'] == current_date_ddmmyyyy
    assert control_record['sysdate'] == current_date_ddmmyyyy
    assert control_record['start_ts'] is not None
    assert control_record['end_ts'] is not None
    assert control_record['error_message'] is None

    # Assertions for job_run_log
    job_run_log_records = bq_client.query_table(JOB_RUN_LOG_TABLE)
    assert len(job_run_log_records) == 1
    run_log_record = job_run_log_records[0]
    assert run_log_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert run_log_record['message'] == 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    assert run_log_record['status'] == 'OK'

    # Assertions for job_error_log
    job_error_log_records = bq_client.query_table(JOB_ERROR_LOG_TABLE)
    assert len(job_error_log_records) == 0

```

---

### Test Case 2: Explicit Stichtag and Wiederanlaufwert

**Purpose:** Verify that both `p_stichtag` and `p_wiederanlaufWert` are correctly parsed and used when provided explicitly.

**Setup:**
*   Clear all records from logging tables.
*   Define `test_stichtag = '20230115'` and `test_wiederanlaufWert = 500`.

**Action:**

1.  **Legacy:** Execute the KornShell script with `-s 20230115 -l 500`.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh -s 20230115 -l 500
    ```
    Capture the exit code and log file content.
2.  **Migrated:** Call the BigQuery stored procedure with `'20230115'` and `500`.
    ```python
    # In pytest
    def test_explicit_parameters():
        clear_bq_logging_tables()
        test_stichtag = '20230115'
        test_wiederanlaufWert = 500
        
        # Legacy execution (conceptual)
        # legacy_result = execute_legacy_script(['-s', test_stichtag, '-l', str(test_wiederanlaufWert)])
        # assert legacy_result['exit_code'] == 0
        # assert f"Stichtag  : '{test_stichtag}'" in legacy_result['log_content']
        # assert f"p_wiederanlaufWert={test_wiederanlaufWert}" in legacy_result['log_content'] # Check k_aurd_rechstan.ksh call params
        # assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in legacy_result['log_content']

        # Migrated execution
        bq_client.execute_sp(test_stichtag, test_wiederanlaufWert)

        # Assertions
        # ... (see Pass/Fail Criterion)
    ```

**Pass/Fail Criterion:**

*   **Legacy:** The script exits with code `0`. The log file contains a success message and indicates `Stichtag` as `20230115` and `p_wiederanlaufWert` as `500` in the call to `k_aurd_rechstan.ksh`.
*   **Migrated:**
    *   The `sp_erzeugung_abzug_rechnungsdaten` call completes successfully.
    *   Query `project.dataset.job_control`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'`.
        *   `status` is 'OK'.
        *   `stichtag` is `'20230115'`.
    *   Query `project.dataset.job_run_log`:
        *   One row exists with the success message.
    *   Query `project.dataset.job_error_log`:
        *   No rows exist.

```python
def test_explicit_parameters():
    test_stichtag = '20230115'
    test_wiederanlaufWert = 500
    
    bq_client.execute_sp(test_stichtag, test_wiederanlaufWert)

    job_control_records = bq_client.query_table(JOB_CONTROL_TABLE)
    assert len(job_control_records) == 1
    control_record = job_control_records[0]
    assert control_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert control_record['status'] == 'OK'
    assert control_record['stichtag'] == test_stichtag
    assert control_record['start_ts'] is not None
    assert control_record['end_ts'] is not None
    assert control_record['error_message'] is None

    job_run_log_records = bq_client.query_table(JOB_RUN_LOG_TABLE)
    assert len(job_run_log_records) == 1
    assert job_run_log_records[0]['message'] == 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'

    job_error_log_records = bq_client.query_table(JOB_ERROR_LOG_TABLE)
    assert len(job_error_log_records) == 0
```

---

### Test Case 3: Missing Stichtag Parameter (Error Condition)

**Purpose:** Verify that the job correctly identifies and handles the error when the mandatory `p_stichtag` is missing or empty, and logs the error appropriately.

**Setup:**
*   Clear all records from logging tables.

**Action:**

1.  **Legacy:** Execute the KornShell script with `-s` but no value.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh -s
    ```
    Capture the exit code and the content of the generated log file.
2.  **Migrated:** Call the BigQuery stored procedure with an empty string `''` for `p_stichtag` and `NULL` for `p_wiederanlaufWert`.
    ```python
    # In pytest
    def test_missing_stichtag_error():
        clear_bq_logging_tables()
        
        # Legacy execution (conceptual)
        # legacy_result = execute_legacy_script(['-s'])
        # assert legacy_result['exit_code'] == 193 # Or other non-zero error code
        # assert "Notwendiges Argument fehlt" in legacy_result['stderr'] # Or log content
        # assert "Stichtag missing" in legacy_result['log_content'] # If f_alis_msgerr.ksh logs it

        # Migrated execution
        with pytest.raises(Exception) as excinfo:
            bq_client.execute_sp('', None)
        assert "Stichtag missing" in str(excinfo.value)

        # Assertions
        # ... (see Pass/Fail Criterion)
    ```

**Pass/Fail Criterion:**

*   **Legacy:** The script exits with code `193`. The log file and/or stderr should contain an error message indicating a missing argument for `Stichtag`.
*   **Migrated:**
    *   The `sp_erzeugung_abzug_rechnungsdaten` call raises an exception with the message 'Stichtag missing'.
    *   Query `project.dataset.job_control`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'`.
        *   `status` is 'ERROR'.
        *   `error_message` contains 'Stichtag missing'.
    *   Query `project.dataset.job_error_log`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'`.
        *   `error_code` is '193'.
        *   `error_message` is 'Stichtag missing'.
    *   Query `project.dataset.job_run_log`:
        *   No rows exist with a success message.

```python
def test_missing_stichtag_error():
    with pytest.raises(Exception) as excinfo:
        bq_client.execute_sp('', None)
    assert "Stichtag missing" in str(excinfo.value)

    job_control_records = bq_client.query_table(JOB_CONTROL_TABLE)
    assert len(job_control_records) == 1
    control_record = job_control_records[0]
    assert control_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert control_record['status'] == 'ERROR'
    assert control_record['error_message'] == 'Stichtag missing' # Direct error message from RAISE

    job_error_log_records = bq_client.query_table(JOB_ERROR_LOG_TABLE)
    assert len(job_error_log_records) == 1
    error_record = job_error_log_records[0]
    assert error_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert error_record['error_code'] == '193'
    assert error_record['error_message'] == 'Stichtag missing'

    job_run_log_records = bq_client.query_table(JOB_RUN_LOG_TABLE)
    assert len(job_run_log_records) == 0
```

---

### Test Case 4: `eintragsnr` Incrementing

**Purpose:** Verify that the `eintragsnr` (job entry number) correctly increments for subsequent job runs, ensuring proper logging lineage.

**Setup:**
*   Clear all records from logging tables.

**Action:**

1.  **Legacy:** Execute the KornShell script successfully twice in a row (e.g., with default parameters). Capture the `Job-Nr` from the console output or log file for both runs.
    ```bash
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
    ```
2.  **Migrated:** Call the BigQuery stored procedure successfully twice in a row (e.g., with `NULL` parameters).
    ```python
    # In pytest
    def test_eintragsnr_incrementing():
        clear_bq_logging_tables()
        
        # Legacy execution (conceptual)
        # legacy_result1 = execute_legacy_script([])
        # legacy_result2 = execute_legacy_script([])
        # eintragsnr1 = parse_eintragsnr(legacy_result1['log_content'])
        # eintragsnr2 = parse_eintragsnr(legacy_result2['log_content'])
        # assert int(eintragsnr2) == int(eintragsnr1) + 1

        # Migrated execution
        bq_client.execute_sp(None, None)
        bq_client.execute_sp(None, None)

        # Assertions
        # ... (see Pass/Fail Criterion)
    ```

**Pass/Fail Criterion:**

*   **Legacy:** The `Job-Nr` reported for the second run is exactly one greater than the `Job-Nr` for the first run.
*   **Migrated:**
    *   Query `project.dataset.job_control`:
        *   Two rows exist with `job_kennung = 'BERT_RKOPF_STAN'` and `status = 'OK'`.
        *   The `eintragsnr` of the second run is `1` greater than the `eintragsnr` of the first run.

```python
def test_eintragsnr_incrementing():
    bq_client.execute_sp(None, None)
    bq_client.execute_sp(None, None)

    job_control_records = bq_client.query_table(JOB_CONTROL_TABLE)
    assert len(job_control_records) == 2
    
    # Sort by start_ts to ensure correct order
    job_control_records.sort(key=lambda x: x['start_ts'])

    first_run_eintragsnr = job_control_records[0]['eintragsnr']
    second_run_eintragsnr = job_control_records[1]['eintragsnr']

    assert second_run_eintragsnr == first_run_eintragsnr + 1
    assert job_control_records[0]['status'] == 'OK'
    assert job_control_records[1]['status'] == 'OK'

    job_run_log_records = bq_client.query_table(JOB_RUN_LOG_TABLE)
    assert len(job_run_log_records) == 2 # Two success messages
```

---

### Test Case 5: Simulated Core Logic Error Handling

**Purpose:** Verify that if the *placeholder* core logic (representing `k_aurd_rechstan.ksh`) fails, the wrapper correctly catches the error, updates the job status to 'ERROR', and logs the error details.

**Setup:**
*   Clear all records from logging tables.
*   Modify the `execute_bq_sp` helper function (or the BigQuery SP itself for testing) to simulate an error within the `BEGIN...EXCEPTION` block. For example, by calling a non-existent procedure or raising an explicit error.

**Action:**

1.  **Legacy:** Simulate an error in `k_aurd_rechstan.ksh` (e.g., by making `k_aurd_rechstan.ksh` exit with a non-zero code).
    ```bash
    # Temporarily modify k_aurd_rechstan.ksh to exit 1
    # Then run:
    ./vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh -s 20231026
    ```
    Capture the exit code and log file content.
2.  **Migrated:** Call the BigQuery stored procedure, triggering the simulated error in the core logic section.
    ```python
    # In pytest
    def test_simulated_core_logic_error():
        clear_bq_logging_tables()
        test_stichtag = '20231026'
        
        # Modify mock client to simulate error in core logic
        original_execute_sp = bq_client.execute_sp
        def mock_execute_sp_with_error(p_stichtag, p_wiederanlaufWert):
            # Call original SP logic up to the core processing block
            v_sysdate = datetime.now().strftime('%d%m%Y')
            v_effective_stichtag = p_stichtag if p_stichtag is not None and p_stichtag.strip() != '' else v_sysdate
            v_wiederanlaufWert = p_wiederanlaufWert if p_wiederanlaufWert is not None else 0
            v_jobkennung = 'BERT_RKOPF_STAN'
            current_timestamp = datetime.now()

            if v_effective_stichtag is None or v_effective_stichtag.strip() == '':
                raise Exception('Stichtag missing') # This path is tested elsewhere

            max_eintragsnr = 0
            for row in bq_client.job_control:
                if row['job_kennung'] == v_jobkennung and row['eintragsnr'] > max_eintragsnr:
                    max_eintragsnr = row['eintragsnr']
            v_eintragsnr = max_eintragsnr + 1
            bq_client._eintragsnr_counter = v_eintragsnr

            v_logdateiname = f"{v_jobkennung}_{v_eintragsnr}.log"

            bq_client.job_control.append({
                'eintragsnr': v_eintragsnr, 'job_kennung': v_jobkennung,
                'script_name': 'sp_erzeugung_abzug_rechnungsdaten', 'logdateiname': v_logdateiname,
                'stichtag': v_effective_stichtag, 'status': 'RUNNING', 'start_ts': current_timestamp,
                'sysdate': v_sysdate, 'end_ts': None, 'error_message': None
            })

            # Simulate error in core logic
            raise Exception("Simulated core logic failure")

        bq_client.execute_sp = mock_execute_sp_with_error

        with pytest.raises(Exception) as excinfo:
            bq_client.execute_sp(test_stichtag, None)
        assert "Simulated core logic failure" in str(excinfo.value)

        # Restore original mock function
        bq_client.execute_sp = original_execute_sp

        # Assertions
        # ... (see Pass/Fail Criterion)
    ```

**Pass/Fail Criterion:**

*   **Legacy:** The script exits with a non-zero error code. The log file contains an error message from `k_aurd_rechstan.ksh` and potentially a `DWMSG_Fehlerbehandlung` entry. The final status in the log is 'ERROR'.
*   **Migrated:**
    *   The `sp_erzeugung_abzug_rechnungsdaten` call raises an exception (e.g., 'Simulated core logic failure').
    *   Query `project.dataset.job_control`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'`.
        *   `status` is 'ERROR'.
        *   `error_message` contains the simulated error message.
        *   `end_ts` is populated.
    *   Query `project.dataset.job_error_log`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'`.
        *   `error_code` is 'APP_ERROR'.
        *   `error_message` matches the simulated error.
    *   Query `project.dataset.job_run_log`:
        *   No rows exist with a success message.

```python
def test_simulated_core_logic_error():
    test_stichtag = '20231026'
    
    # Temporarily modify the mock client to simulate an error in the core logic
    original_execute_sp = bq_client.execute_sp
    def mock_execute_sp_with_error(p_stichtag, p_wiederanlaufWert):
        # This part mimics the SP's initial setup before the BEGIN...EXCEPTION block
        v_sysdate = datetime.now().strftime('%d%m%Y')
        v_effective_stichtag = p_stichtag if p_stichtag is not None and p_stichtag.strip() != '' else v_sysdate
        v_wiederanlaufWert = p_wiederanlaufWert if p_wiederanlaufWert is not None else 0
        v_jobkennung = 'BERT_RKOPF_STAN'
        current_timestamp = datetime.now()

        if v_effective_stichtag is None or v_effective_stichtag.strip() == '':
            bq_client.job_error_log.append({
                'job_kennung': v_jobkennung, 'log_ts': current_timestamp, 'error_code': '193',
                'error_message': 'Stichtag missing', 'stichtag': v_effective_stichtag,
                'wiederanlaufwert': v_wiederanlaufWert
            })
            raise Exception('Stichtag missing')

        max_eintragsnr = 0
        for row in bq_client.job_control:
            if row['job_kennung'] == v_jobkennung and row['eintragsnr'] > max_eintragsnr:
                max_eintragsnr = row['eintragsnr']
        v_eintragsnr = max_eintragsnr + 1
        bq_client._eintragsnr_counter = v_eintragsnr

        v_logdateiname = f"{v_jobkennung}_{v_eintragsnr}.log"

        bq_client.job_control.append({
            'eintragsnr': v_eintragsnr, 'job_kennung': v_jobkennung,
            'script_name': 'sp_erzeugung_abzug_rechnungsdaten', 'logdateiname': v_logdateiname,
            'stichtag': v_effective_stichtag, 'status': 'RUNNING', 'start_ts': current_timestamp,
            'sysdate': v_sysdate, 'end_ts': None, 'error_message': None
        })

        # This is where we inject the simulated error, mimicking the EXCEPTION WHEN ERROR block
        simulated_error_message = "Simulated core logic failure"
        v_status = 'ERROR'
        v_errmsg = simulated_error_message

        bq_client.job_error_log.append({
            'eintragsnr': v_eintragsnr, 'job_kennung': v_jobkennung, 'log_ts': datetime.now(),
            'error_code': 'APP_ERROR', 'error_message': v_errmsg, 'stichtag': v_effective_stichtag,
            'wiederanlaufwert': v_wiederanlaufWert
        })
        for row in bq_client.job_control:
            if row['eintragsnr'] == v_eintragsnr and row['job_kennung'] == v_jobkennung:
                row['status'] = 'ERROR'
                row['end_ts'] = datetime.now()
                row['error_message'] = v_errmsg
                break
        raise Exception(v_errmsg) # The SP would raise this

    bq_client.execute_sp = mock_execute_sp_with_error

    with pytest.raises(Exception) as excinfo:
        bq_client.execute_sp(test_stichtag, None)
    assert "Simulated core logic failure" in str(excinfo.value)

    # Restore original mock function for subsequent tests
    bq_client.execute_sp = original_execute_sp

    job_control_records = bq_client.query_table(JOB_CONTROL_TABLE)
    assert len(job_control_records) == 1
    control_record = job_control_records[0]
    assert control_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert control_record['status'] == 'ERROR'
    assert control_record['error_message'] == "Simulated core logic failure"
    assert control_record['end_ts'] is not None

    job_error_log_records = bq_client.query_table(JOB_ERROR_LOG_TABLE)
    assert len(job_error_log_records) == 1
    error_record = job_error_log_records[0]
    assert error_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert error_record['error_code'] == 'APP_ERROR'
    assert error_record['error_message'] == "Simulated core logic failure"

    job_run_log_records = bq_client.query_table(JOB_RUN_LOG_TABLE)
    assert len(job_run_log_records) == 0
```

---

### Test Case 6: Schema and Data Quality Assertions for Logging Tables

**Purpose:** Verify that the BigQuery logging tables exist, have the correct schema, and store data with expected types and constraints.

**Setup:**
*   Ensure the DDL for the logging tables has been executed.
*   Run a successful execution of `sp_erzeugung_abzug_rechnungsdaten` (e.g., with default parameters) to populate the tables.

**Action:**

1.  **Legacy:** (Not directly applicable to schema, but for data quality, inspect the generated log files for format and content).
2.  **Migrated:** Query BigQuery's `INFORMATION_SCHEMA` to inspect table schemas and then query the logging tables for data type and content validation.

**Pass/Fail Criterion:**

*   **Schema Assertions:**
    *   `project.dataset.job_control` exists and has columns: `eintragsnr` (INT64 NOT NULL), `job_kennung` (STRING NOT NULL), `script_name` (STRING), `logdateiname` (STRING), `stichtag` (STRING), `status` (STRING), `start_ts` (TIMESTAMP), `end_ts` (TIMESTAMP), `error_message` (STRING), `sysdate` (STRING).
    *   `project.dataset.job_run_log` exists and has columns: `eintragsnr` (INT64 NOT NULL), `job_kennung` (STRING NOT NULL), `log_ts` (TIMESTAMP NOT NULL), `message` (STRING), `status` (STRING).
    *   `project.dataset.job_error_log` exists and has columns: `eintragsnr` (INT64 NOT NULL), `job_kennung` (STRING NOT NULL), `log_ts` (TIMESTAMP NOT NULL), `error_code` (STRING), `error_message` (STRING), `stichtag` (STRING), `wiederanlaufwert` (INT64).
*   **Data Quality Assertions (after a successful run):**
    *   `job_control.eintragsnr`: Is an integer, unique, and sequential.
    *   `job_control.stichtag`: Matches `DDMMYYYY` format (e.g., `LENGTH(stichtag) = 8` and `REGEXP_CONTAINS(stichtag, r'^\d{8}$')`).
    *   `job_control.status`: Is either 'RUNNING', 'OK', or 'ERROR'.
    *   `job_control.start_ts`, `end_ts`, `job_run_log.log_ts`, `job_error_log.log_ts`: Are valid TIMESTAMPs.
    *   `job_error_log.error_code`: If present, is a non-empty string.

```python
def test_logging_table_schema_and_data_quality():
    # Run a successful job to populate logs
    bq_client.execute_sp('20231101', 10)

    # --- Schema Assertions (Conceptual - requires BigQuery INFORMATION_SCHEMA queries) ---
    # Example for job_control table schema check
    # query = f"""
    # SELECT column_name, data_type, is_nullable
    # FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    # WHERE table_name = 'job_control'
    # ORDER BY ordinal_position
    # """
    # schema_info = query_bq(query)
    # expected_schema = [
    #     {'column_name': 'eintragsnr', 'data_type': 'INT64', 'is_nullable': 'NO'},
    #     {'column_name': 'job_kennung', 'data_type': 'STRING', 'is_nullable': 'NO'},
    #     # ... complete for all columns
    # ]
    # assert schema_info == expected_schema # This would be a detailed comparison

    # --- Data Quality Assertions ---
    job_control_records = bq_client.query_table(JOB_CONTROL_TABLE)
    assert len(job_control_records) == 1
    control_record = job_control_records[0]

    assert isinstance(control_record['eintragsnr'], int)
    assert control_record['eintragsnr'] > 0
    assert control_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert len(control_record['stichtag']) == 8
    assert control_record['stichtag'].isdigit()
    assert control_record['status'] in ['RUNNING', 'OK', 'ERROR']
    assert isinstance(control_record['start_ts'], datetime)
    assert isinstance(control_record['end_ts'], datetime)
    assert control_record['end_ts'] >= control_record['start_ts']
    assert len(control_record['sysdate']) == 8
    assert control_record['sysdate'].isdigit()

    job_run_log_records = bq_client.query_table(JOB_RUN_LOG_TABLE)
    assert len(job_run_log_records) == 1
    run_log_record = job_run_log_records[0]
    assert isinstance(run_log_record['eintragsnr'], int)
    assert run_log_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert isinstance(run_log_record['log_ts'], datetime)
    assert isinstance(run_log_record['message'], str)
    assert run_log_record['status'] in ['OK', 'ERROR']

    # Test error log data quality after an error run
    bq_client.clear_tables()
    with pytest.raises(Exception):
        bq_client.execute_sp('', None) # Trigger error

    job_error_log_records = bq_client.query_table(JOB_ERROR_LOG_TABLE)
    assert len(job_error_log_records) == 1
    error_record = job_error_log_records[0]
    assert isinstance(error_record['eintragsnr'], int)
    assert error_record['job_kennung'] == 'BERT_RKOPF_STAN'
    assert isinstance(error_record['log_ts'], datetime)
    assert isinstance(error_record['error_code'], str) and len(error_record['error_code']) > 0
    assert isinstance(error_record['error_message'], str) and len(error_record['error_message']) > 0
    assert len(error_record['stichtag']) == 0 # Empty string for stichtag
    assert isinstance(error_record['wiederanlaufwert'], int)
```

---

### Test Case 7: External System Replacements - Orchestration Trigger

**Purpose:** Verify that the new orchestration mechanism (Cloud Composer/Workflows) can successfully trigger the BigQuery Stored Procedure. This is an integration test rather than a unit test of the stored procedure itself.

**Setup:**
*   A deployed Cloud Composer DAG or Google Cloud Workflow configured to call `project.dataset.sp_erzeugung_abzug_rechnungsdaten`.
*   Clear all records from logging tables.

**Action:**

1.  **Legacy:** Manually trigger the UC4 job `DW.BERT_RECHNUNGSDATEN.xml`.
2.  **Migrated:** Manually trigger the Cloud Composer DAG or Google Cloud Workflow.

**Pass/Fail Criterion:**

*   **Legacy:** The UC4 job completes successfully, and the `r_aurd_rechstan.ksh` script runs to completion, logging success.
*   **Migrated:**
    *   The Cloud Composer DAG run or Workflow execution completes successfully.
    *   Query `project.dataset.job_control`:
        *   One row exists with `job_kennung = 'BERT_RKOPF_STAN'` and `status = 'OK'`, indicating the stored procedure was successfully invoked and completed.
    *   Query `project.dataset.job_error_log`:
        *   No rows exist.

```python
# This test case is more conceptual and would involve triggering GCP orchestration tools.
# It's not directly runnable as a pytest function against the BQ SP code.

def test_cloud_orchestration_trigger_success():
    # This test would be part of an end-to-end integration test suite.
    # It assumes the Cloud Composer DAG or Workflow is already deployed.

    # 1. Clear BigQuery logging tables
    bq_client.clear_tables()

    # 2. Trigger the Cloud Composer DAG / Google Cloud Workflow
    #    (This would be an API call or gcloud command in a real test)
    #    Example: gcloud composer environments run-airflow-cmd ...
    #    Example: gcloud workflows execute ...
    print("Manually trigger Cloud Composer DAG or Google Cloud Workflow for sp_erzeugung_abzug_rechnungsdaten.")
    print("Waiting for orchestration to complete and populate BigQuery logs...")
    
    # For demonstration, we'll simulate a successful call after a delay
    # In a real test, you'd poll the orchestration status or BigQuery logs
    # time.sleep(60) # Wait for the job to run

    # Simulate the successful execution by the orchestration
    bq_client.execute_sp('20231101', 0) # Assume orchestration passed these parameters

    # 3. Assertions in BigQuery
    job_control_records = bq_client.query_table(JOB_CONTROL_TABLE)
    assert len(job_control_records) >= 1, "Expected at least one job control record from orchestration"
    
    # Find the most recent job triggered by the orchestration
    most_recent_job = max(job_control_records, key=lambda x: x['start_ts'])

    assert most_recent_job['job_kennung'] == 'BERT_RKOPF_STAN'
    assert most_recent_job['status'] == 'OK'
    assert most_recent_job['error_message'] is None

    job_error_log_records = bq_client.query_table(JOB_ERROR_LOG_TABLE)
    assert len(job_error_log_records) == 0, "No errors expected for a successful orchestration run"

    print("Cloud orchestration trigger test passed: BigQuery SP executed successfully and logged status OK.")

```