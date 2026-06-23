As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_bp_ta_rn_einzeln.ksh` to a BigQuery Stored Procedure. These tests aim to ensure behavioral equivalence, data integrity, and correct error handling in the target environment.

**Assumptions & Pre-requisites:**

1.  **BigQuery Environment:** A BigQuery project and dataset (`project.dataset`) are available.
2.  **Audit Tables:** The `job_control`, `job_messages`, and `job_error_log` tables have been created in `project.dataset` as per the provided DDL.
3.  **Migrated Stored Procedure:** The `project.dataset.ausd_bp_ta_rn_einzeln` stored procedure has been deployed.
4.  **Mock Core Procedure:** A mock `project.dataset.k_ausd_bp_ta_rn_einzeln` stored procedure is deployed to simulate the behavior of the downstream script. This mock procedure will log its invocation and can be configured to succeed or fail based on specific input parameters (e.g., a special `p_stichtag` value).

**Mock `k_ausd_bp_ta_rn_einzeln` Stored Procedure (for testing purposes):**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_rn_einzeln`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_dwh_eintragsnr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Log the call to this mock procedure for verification
  INSERT INTO `project.dataset.job_messages`
  (
    job_entry_nr,
    job_name,
    message_type,
    message_text,
    created_at
  )
  VALUES
  (
    p_dwh_eintragsnr,
    p_jobkennung,
    'DEBUG',
    CONCAT(
      'Mock k_ausd_bp_ta_rn_einzeln called with: ',
      'stichtag=', p_stichtag,
      ', restart=', CAST(p_wiederanlaufWert AS STRING)
    ),
    CURRENT_TIMESTAMP()
  );

  -- Simulate failure if p_stichtag is 'FAIL'
  IF p_stichtag = 'FAIL' THEN
    RAISE USING MESSAGE = 'Simulated failure in k_ausd_bp_ta_rn_einzeln due to specific stichtag value.';
  END IF;

  -- Simulate some successful work if not failing
  -- SELECT 'Core logic executed successfully' AS status;
END;
```

---

## Migration Validation Test Cases

### Test Case 1: Successful Execution with All Parameters Provided

**Purpose:** Verify that the stored procedure correctly processes all provided parameters (`p_stichtag`, `p_wiederanlaufWert`) and orchestrates a successful execution of the core logic, logging the outcome appropriately. This covers output parity and transformation correctness for parameter handling.

**Setup:**
1.  Clear all rows from `project.dataset.job_control`, `project.dataset.job_messages`, and `project.dataset.job_error_log`.
2.  Define a specific `stichtag` (e.g., '01012023') and `wiederanlaufWert` (e.g., 12345).

**Action:**
Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` with the defined `p_stichtag` and `p_wiederanlaufWert`.

```python
# pytest example
import pytest
from google.cloud import bigquery
from datetime import datetime

bq_client = bigquery.Client()
PROJECT_ID = "project"
DATASET_ID = "dataset"
SP_NAME = "ausd_bp_ta_rn_einzeln"
MOCK_SP_NAME = "k_ausd_bp_ta_rn_einzeln"

def _clear_audit_tables():
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_messages`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()

def _call_main_sp(stichtag: str = None, wiederanlaufWert: int = None):
    params = []
    if stichtag is not None:
        params.append(f"p_stichtag => '{stichtag}'")
    else:
        params.append("p_stichtag => NULL")
    if wiederanlaufWert is not None:
        params.append(f"p_wiederanlaufWert => {wiederanlaufWert}")
    else:
        params.append("p_wiederanlaufWert => NULL")

    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`({', '.join(params)})"
    return bq_client.query(query).result()

def test_successful_execution_all_params():
    _clear_audit_tables()
    
    test_stichtag = '01012023'
    test_wiederanlaufWert = 12345

    _call_main_sp(stichtag=test_stichtag, wiederanlaufWert=test_wiederanlaufWert)

    # Assertions
    job_control_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    job_messages_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_messages` ORDER BY created_at").result())

    assert len(job_control_rows) == 1
    jc_row = job_control_rows[0]
    assert jc_row.job_name == 'AUSD_BP_TA_RN_EINZELN'
    assert jc_row.stichtag == test_stichtag
    assert jc_row.restart_value == test_wiederanlaufWert
    assert jc_row.status == 'OK'
    assert jc_row.success_message == 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    assert jc_row.finished_at is not None
    assert jc_row.error_message is None

    # Check messages, including the mock SP call
    assert len(job_messages_rows) >= 2 # At least DEBUG from mock and INFO from main SP
    assert any(f"Mock {MOCK_SP_NAME} called with: stichtag={test_stichtag}, restart={test_wiederanlaufWert}" in msg.message_text for msg in job_messages_rows)
    assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in msg.message_text for msg in job_messages_rows)
    assert any(msg.message_type == 'INFO' for msg in job_messages_rows)
    assert any(msg.message_type == 'DEBUG' for msg in job_messages_rows)

    job_error_log_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
    assert len(job_error_log_rows) == 0
```

**Pass/Fail Criterion:**
*   Exactly one row in `job_control` with `status = 'OK'`, `stichtag = '01012023'`, `restart_value = 12345`, and `success_message` populated.
*   At least two rows in `job_messages`: one `DEBUG` message confirming the mock core procedure was called with the correct parameters, and one `INFO` message for successful completion.
*   Zero rows in `job_error_log`.

---

### Test Case 2: Defaulting `p_stichtag` to Current System Date

**Purpose:** Verify that when `p_stichtag` is not provided (or is NULL/empty), it correctly defaults to the current system date in `DDMMYYYY` format, mirroring the legacy script's behavior. This covers transformation correctness (date handling, NULL handling).

**Setup:**
1.  Clear all rows from audit tables.
2.  Determine today's date in `DDMMYYYY` format (e.g., '25102023').

**Action:**
Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` with `p_stichtag = NULL` and `p_wiederanlaufWert = NULL`.

```python
# pytest example
from datetime import datetime

def test_default_stichtag():
    _clear_audit_tables()
    
    expected_sysdate = datetime.now().strftime('%d%m%Y')

    _call_main_sp(stichtag=None, wiederanlaufWert=None)

    # Assertions
    job_control_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    assert len(job_control_rows) == 1
    jc_row = job_control_rows[0]
    assert jc_row.job_name == 'AUSD_BP_TA_RN_EINZELN'
    assert jc_row.stichtag == expected_sysdate # Should default to current sysdate
    assert jc_row.sysdate_ddmmyyyy == expected_sysdate
    assert jc_row.restart_value == 0 # Should default to 0
    assert jc_row.status == 'OK'

    job_messages_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_messages` ORDER BY created_at").result())
    assert any(f"Mock {MOCK_SP_NAME} called with: stichtag={expected_sysdate}, restart=0" in msg.message_text for msg in job_messages_rows)
    
    job_error_log_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
    assert len(job_error_log_rows) == 0
```

**Pass/Fail Criterion:**
*   Exactly one row in `job_control` with `status = 'OK'`, `stichtag` matching today's `DDMMYYYY` date, and `restart_value = 0`.
*   The `DEBUG` message from the mock core procedure confirms it received the defaulted `stichtag` and `restart_value`.
*   Zero rows in `job_error_log`.

---

### Test Case 3: Defaulting `p_wiederanlaufWert` to 0

**Purpose:** Verify that when `p_wiederanlaufWert` is not provided (or is NULL), it correctly defaults to `0`, mirroring the legacy script's behavior. This covers transformation correctness (NULL handling).

**Setup:**
1.  Clear all rows from audit tables.
2.  Define a specific `stichtag` (e.g., '01012023').

**Action:**
Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` with `p_stichtag = '01012023'` and `p_wiederanlaufWert = NULL`.

```python
# pytest example
def test_default_wiederanlaufWert():
    _clear_audit_tables()
    
    test_stichtag = '01012023'

    _call_main_sp(stichtag=test_stichtag, wiederanlaufWert=None)

    # Assertions
    job_control_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    assert len(job_control_rows) == 1
    jc_row = job_control_rows[0]
    assert jc_row.job_name == 'AUSD_BP_TA_RN_EINZELN'
    assert jc_row.stichtag == test_stichtag
    assert jc_row.restart_value == 0 # Should default to 0
    assert jc_row.status == 'OK'

    job_messages_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_messages` ORDER BY created_at").result())
    assert any(f"Mock {MOCK_SP_NAME} called with: stichtag={test_stichtag}, restart=0" in msg.message_text for msg in job_messages_rows)
    
    job_error_log_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
    assert len(job_error_log_rows) == 0
```

**Pass/Fail Criterion:**
*   Exactly one row in `job_control` with `status = 'OK'`, `stichtag = '01012023'`, and `restart_value = 0`.
*   The `DEBUG` message from the mock core procedure confirms it received the defaulted `restart_value`.
*   Zero rows in `job_error_log`.

---

### Test Case 4: Missing `p_stichtag` (Validation Error)

**Purpose:** Verify that if `p_stichtag` is explicitly empty or invalid after trimming (and thus cannot default to `v_sysdate`), the procedure correctly identifies this as a missing required parameter, logs an error, and raises an exception, mirroring the legacy script's `pruefeParameterGesetzt` and `exit $ErrNr` behavior. This covers transformation correctness (validation, error handling).

**Setup:**
1.  Clear all rows from audit tables.

**Action:**
Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` with `p_stichtag = ''` (empty string) and `p_wiederanlaufWert = NULL`. The call is expected to fail.

```python
# pytest example
import pytest

def test_missing_stichtag_validation_error():
    _clear_audit_tables()
    
    with pytest.raises(Exception) as excinfo:
        _call_main_sp(stichtag='', wiederanlaufWert=None)

    # Assert that the correct error message is raised
    assert "Error 193: Stichtag" in str(excinfo.value)

    # Assertions on audit tables
    job_control_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    assert len(job_control_rows) == 0 # No job_control entry should be created before validation

    job_messages_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_messages`").result())
    assert len(job_messages_rows) == 0

    job_error_log_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
    assert len(job_error_log_rows) == 1
    jel_row = job_error_log_rows[0]
    assert jel_row.job_name == 'AUSD_BP_TA_RN_EINZELN'
    assert jel_row.error_number == 193
    assert jel_row.error_argument == 'Stichtag'
    assert jel_row.message == 'Required parameter missing'
```

**Pass/Fail Criterion:**
*   The BigQuery Stored Procedure call fails with an exception containing "Error 193: Stichtag".
*   Exactly one row in `job_error_log` with `error_number = 193`, `error_argument = 'Stichtag'`, and `message = 'Required parameter missing'`.
*   Zero rows in `job_control` and `job_messages` (as the error occurs before job control entry creation).

---

### Test Case 5: Core Logic (`k_ausd_bp_ta_rn_einzeln`) Failure

**Purpose:** Verify that if the invoked core logic (mock `k_ausd_bp_ta_rn_einzeln`) fails, the main orchestration procedure correctly catches the error, updates the `job_control` status to 'ERROR', logs the error message, and re-raises the exception. This covers external system replacements (logging) and transformation correctness (error handling).

**Setup:**
1.  Clear all rows from audit tables.
2.  Define a specific `stichtag` (e.g., 'FAIL') that triggers a simulated failure in the mock `k_ausd_bp_ta_rn_einzeln` procedure.
3.  Define `wiederanlaufWert` (e.g., 54321).

**Action:**
Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` with `p_stichtag = 'FAIL'` and `p_wiederanlaufWert = 54321`. The call is expected to fail.

```python
# pytest example
import pytest

def test_core_logic_failure():
    _clear_audit_tables()
    
    test_stichtag = 'FAIL' # This triggers failure in mock k_ausd_bp_ta_rn_einzeln
    test_wiederanlaufWert = 54321

    with pytest.raises(Exception) as excinfo:
        _call_main_sp(stichtag=test_stichtag, wiederanlaufWert=test_wiederanlaufWert)

    # Assert that the correct error message is raised
    assert "Simulated failure in k_ausd_bp_ta_rn_einzeln" in str(excinfo.value)

    # Assertions on audit tables
    job_control_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
    assert len(job_control_rows) == 1
    jc_row = job_control_rows[0]
    assert jc_row.job_name == 'AUSD_BP_TA_RN_EINZELN'
    assert jc_row.stichtag == test_stichtag
    assert jc_row.restart_value == test_wiederanlaufWert
    assert jc_row.status == 'ERROR'
    assert jc_row.success_message is None
    assert jc_row.error_message is not None
    assert "Simulated failure in k_ausd_bp_ta_rn_einzeln" in jc_row.error_message
    assert jc_row.finished_at is not None

    job_messages_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_messages` ORDER BY created_at").result())
    assert len(job_messages_rows) >= 2 # DEBUG from mock, ERROR from main SP
    assert any(f"Mock {MOCK_SP_NAME} called with: stichtag={test_stichtag}, restart={test_wiederanlaufWert}" in msg.message_text for msg in job_messages_rows)
    assert any("Simulated failure in k_ausd_bp_ta_rn_einzeln" in msg.message_text and msg.message_type == 'ERROR' for msg in job_messages_rows)

    job_error_log_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
    assert len(job_error_log_rows) == 0 # This specific error path doesn't use job_error_log, but updates job_control and job_messages
```

**Pass/Fail Criterion:**
*   The BigQuery Stored Procedure call fails with an exception containing "Simulated failure in k_ausd_bp_ta_rn_einzeln".
*   Exactly one row in `job_control` with `status = 'ERROR'`, `stichtag = 'FAIL'`, `restart_value = 54321`, and `error_message` populated with the exception details.
*   At least two rows in `job_messages`: one `DEBUG` message from the mock core procedure, and one `ERROR` message from the main procedure's exception handler.
*   Zero rows in `job_error_log` (as this path uses `job_control.error_message` and `job_messages` for error details).

---

### Test Case 6: `job_entry_nr` Increment and Uniqueness

**Purpose:** Verify that the `job_entry_nr` is correctly generated as an incrementing sequence based on the `job_control` table, ensuring uniqueness and proper job tracking. This covers data quality and row-count assertions.

**Setup:**
1.  Clear all rows from audit tables.

**Action:**
1.  Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` with valid parameters (e.g., `p_stichtag = '01012023'`, `p_wiederanlaufWert = 1`).
2.  Call the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln` again with different valid parameters (e.g., `p_stichtag = '02012023'`, `p_wiederanlaufWert = 2`).

```python
# pytest example
def test_job_entry_nr_increment():
    _clear_audit_tables()
    
    # First call
    _call_main_sp(stichtag='01012023', wiederanlaufWert=1)
    
    # Second call
    _call_main_sp(stichtag='02012023', wiederanlaufWert=2)

    # Assertions
    job_control_rows = list(bq_client.query(f"SELECT job_entry_nr, stichtag, restart_value FROM `{PROJECT_ID}.{DATASET_ID}.job_control` ORDER BY job_entry_nr").result())
    
    assert len(job_control_rows) == 2
    
    # Check first entry
    assert job_control_rows[0].job_entry_nr == 1
    assert job_control_rows[0].stichtag == '01012023'
    assert job_control_rows[0].restart_value == 1

    # Check second entry
    assert job_control_rows[1].job_entry_nr == 2
    assert job_control_rows[1].stichtag == '02012023'
    assert job_control_rows[1].restart_value == 2

    # Verify uniqueness of job_entry_nr
    job_entry_nrs = [row.job_entry_nr for row in job_control_rows]
    assert len(job_entry_nrs) == len(set(job_entry_nrs))
```

**Pass/Fail Criterion:**
*   Exactly two rows in `job_control`, both with `status = 'OK'`.
*   The `job_entry_nr` for the first call is `1`, and for the second call is `2`.
*   All `job_entry_nr` values are unique.

---

### Test Case 7: Schema and Data Type Assertions for Audit Tables

**Purpose:** Verify that the audit tables (`job_control`, `job_messages`, `job_error_log`) conform to the expected schema and data types, ensuring data integrity and compatibility with downstream reporting/monitoring tools. This covers schema assertions.

**Setup:**
1.  Ensure the audit tables are created as per the DDL in the migration design.

**Action:**
Query the schema of each audit table in BigQuery.

```python
# pytest example
def test_audit_table_schema():
    expected_job_control_schema = {
        'job_entry_nr': 'INT64',
        'job_name': 'STRING',
        'source_script': 'STRING',
        'log_name': 'STRING',
        'stichtag': 'STRING',
        'sysdate_ddmmyyyy': 'STRING',
        'restart_value': 'INT64',
        'status': 'STRING',
        'created_at': 'TIMESTAMP',
        'finished_at': 'TIMESTAMP',
        'success_message': 'STRING',
        'error_message': 'STRING'
    }
    expected_job_messages_schema = {
        'job_entry_nr': 'INT64',
        'job_name': 'STRING',
        'message_type': 'STRING',
        'message_text': 'STRING',
        'created_at': 'TIMESTAMP'
    }
    expected_job_error_log_schema = {
        'job_name': 'STRING',
        'error_number': 'INT64',
        'error_argument': 'STRING',
        'created_at': 'TIMESTAMP',
        'message': 'STRING'
    }

    def _get_table_schema(table_id):
        table = bq_client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{table_id}")
        return {field.name: field.field_type for field in table.schema}

    # Assert job_control schema
    actual_job_control_schema = _get_table_schema('job_control')
    assert actual_job_control_schema == expected_job_control_schema

    # Assert job_messages schema
    actual_job_messages_schema = _get_table_schema('job_messages')
    assert actual_job_messages_schema == expected_job_messages_schema

    # Assert job_error_log schema
    actual_job_error_log_schema = _get_table_schema('job_error_log')
    assert actual_job_error_log_schema == expected_job_error_log_schema
```

**Pass/Fail Criterion:**
*   The schema (column names and data types) of `project.dataset.job_control` exactly matches the expected DDL.
*   The schema of `project.dataset.job_messages` exactly matches the expected DDL.
*   The schema of `project.dataset.job_error_log` exactly matches the expected DDL.

---