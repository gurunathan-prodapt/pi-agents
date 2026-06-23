The migration of `k_ausd_bp_ta_bpr_beschr.ksh` to BigQuery involves re-implementing its orchestration logic, parameter handling, date calculations, and interaction with a business SQL script (`d_ausd_bp_ta_bpr_beschr.sql`) and an audit logging mechanism. The following tests aim to validate the behavioral equivalence of the migrated BigQuery stored procedure `r_ausd_bp_ta_bpr_beschr` against the original KornShell script's design.

For the purpose of these tests, we assume the `d_ausd_bp_ta_bpr_beschr_proc` (the migrated business logic) is implemented as a placeholder that inserts a single row into `PoolBasisprodukt` and logs its invocation parameters to the `job_audit_table`. This allows us to verify the orchestration's interaction with it.

**Assumptions:**
*   BigQuery project and dataset are `project.dataset`.
*   All DDLs and stored procedures (`PoolBasisprodukt`, `job_audit_table`, `d_ausd_bp_ta_bpr_beschr_proc`, `r_ausd_bp_ta_bpr_beschr`) have been deployed.
*   A Python testing framework (e.g., `pytest`) with a BigQuery client is used for execution.

---

## Test 1: Successful Execution with Valid Parameters

**Purpose:** Verify the end-to-end successful execution of the migrated orchestration procedure, including parameter passing, date derivation, invocation of business logic, record counting, and successful audit logging.

**Setup:**
1.  Ensure `project.dataset.PoolBasisprodukt` and `project.dataset.job_audit_table` are empty.
2.  The `d_ausd_bp_ta_bpr_beschr_proc` is deployed as follows (or equivalent, inserting one row and logging its call):
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN p_Stichtag STRING, -- Original 'DDMMYYYY' string
      IN p_RestartValue INT64,
      IN p_DatumHeute DATE,
      IN p_DatumGestern DATE
    )
    BEGIN
      DECLARE v_stichtag_date DATE DEFAULT SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

      -- Placeholder logic: inserts one row into PoolBasisprodukt
      INSERT INTO `project.dataset.PoolBasisprodukt` (id, data, stichtag_date)
      VALUES (GENERATE_UUID(), 'Data for ' || p_JobKennung || ' and ' || p_Stichtag || ' (EintragsNr: ' || p_EintragsNr || ')', v_stichtag_date);

      -- Log parameters received by this procedure for verification
      INSERT INTO `project.dataset.job_audit_table` (
        tab_name, job_status, load_type, stichtag, run_date, job_kind, restart_flag, record_count, message, insert_timestamp
      )
      VALUES (
        'd_ausd_bp_ta_bpr_beschr_proc_log', 'I', 'P', p_Stichtag, v_stichtag_date, 'J', CASE WHEN p_RestartValue = 1 THEN 'Y' ELSE 'N' END, 1,
        FORMAT('Called with JobKennung=%s, EintragsNr=%s, RestartValue=%d, DatumHeute=%s, DatumGestern=%s',
               p_JobKennung, p_EintragsNr, p_RestartValue, CAST(p_DatumHeute AS STRING), CAST(p_DatumGestern AS STRING)),
        CURRENT_TIMESTAMP()
      );
    END;
    ```

**Action:**
Execute the main orchestration procedure with valid parameters:
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_01',
  p_EintragsNr => 'ENTRY_001',
  p_Stichtag => '01012023',
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call completes without raising any BigQuery exceptions.
*   `project.dataset.PoolBasisprodukt` contains exactly one row with `stichtag_date = '2023-01-01'`.
*   `project.dataset.job_audit_table` contains two new rows:
    *   One row for `tab_name = 'PoolBasisprodukt'` with `job_status = 'A'`, `record_count = 1`, `stichtag = '01012023'`, `run_date = '2023-01-01'`, `restart_flag = 'N'`, and `message = 'Job executed successfully'`.
    *   One row for `tab_name = 'd_ausd_bp_ta_bpr_beschr_proc_log'` confirming the business logic was called with correct parameters, including `p_RestartValue = 0`, `p_DatumHeute = CURRENT_DATE()`, and `p_DatumGestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.

**Runnable Test Code (Python/Pytest):**
```python
import pytest
from google.cloud import bigquery
from datetime import date, timedelta

PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT_ID)

def _execute_bq_query(query):
    query_job = BQ_CLIENT.query(query)
    return query_job.result()

def _clear_tables():
    _execute_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt`")
    _execute_bq_query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_audit_table`")

@pytest.fixture(autouse=True)
def setup_and_teardown():
    _clear_tables()
    yield
    _clear_tables()

def test_successful_execution():
    stichtag = "01012023"
    job_kennung = "TEST_JOB_01"
    eintrags_nr = "ENTRY_001"
    wiederanlauf_wert = 0

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    _execute_bq_query(call_proc_sql)

    # Verify PoolBasisprodukt
    pool_basis_produkt_rows = list(_execute_bq_query(f"SELECT stichtag_date FROM `{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt`"))
    assert len(pool_basis_produkt_rows) == 1
    assert pool_basis_produkt_rows[0].stichtag_date == date(2023, 1, 1)

    # Verify job_audit_table
    audit_rows = list(_execute_bq_query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table` ORDER BY insert_timestamp ASC"))
    assert len(audit_rows) == 2

    # Check d_ausd_bp_ta_bpr_beschr_proc_log entry
    proc_log_entry = audit_rows[0]
    assert proc_log_entry.tab_name == 'd_ausd_bp_ta_bpr_beschr_proc_log'
    assert proc_log_entry.job_status == 'I'
    assert proc_log_entry.load_type == 'P'
    assert proc_log_entry.stichtag == stichtag
    assert proc_log_entry.run_date == date(2023, 1, 1)
    assert proc_log_entry.job_kind == 'J'
    assert proc_log_entry.restart_flag == 'N'
    assert proc_log_entry.record_count == 1
    assert f"JobKennung={job_kennung}" in proc_log_entry.message
    assert f"EintragsNr={eintrags_nr}" in proc_log_entry.message
    assert f"RestartValue={wiederanlauf_wert}" in proc_log_entry.message
    assert f"DatumHeute={date.today()}" in proc_log_entry.message
    assert f"DatumGestern={date.today() - timedelta(days=1)}" in proc_log_entry.message

    # Check main orchestration log entry
    main_log_entry = audit_rows[1]
    assert main_log_entry.tab_name == 'PoolBasisprodukt'
    assert main_log_entry.job_status == 'A'
    assert main_log_entry.load_type == 'I'
    assert main_log_entry.stichtag == stichtag
    assert main_log_entry.run_date == date(2023, 1, 1)
    assert main_log_entry.job_kind == 'J'
    assert main_log_entry.restart_flag == 'N'
    assert main_log_entry.record_count == 1
    assert main_log_entry.message == 'Job executed successfully'
```

---

## Test 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the orchestration procedure correctly identifies and handles a missing `p_JobKennung` parameter, raising an error and logging the failure.

**Setup:**
1.  Ensure `project.dataset.job_audit_table` is empty.

**Action:**
Execute the main orchestration procedure with `p_JobKennung` as `NULL`.
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => NULL,
  p_EintragsNr => 'ENTRY_001',
  p_Stichtag => '01012023',
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call raises a BigQuery exception with the message 'Jobkennung fehlt'.
*   `project.dataset.job_audit_table` contains exactly one row with `job_status = 'E'` and `message` containing 'Jobkennung fehlt'.

**Runnable Test Code (Python/Pytest):**
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest # For catching BigQuery errors

# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown fixtures as above) ...

def test_missing_jobkennung_parameter():
    stichtag = "01012023"
    eintrags_nr = "ENTRY_001"
    wiederanlauf_wert = 0

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => NULL,
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    with pytest.raises(BadRequest) as excinfo:
        _execute_bq_query(call_proc_sql)

    assert "Jobkennung fehlt" in str(excinfo.value)

    # Verify job_audit_table
    audit_rows = list(_execute_bq_query(f"SELECT job_status, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table`"))
    assert len(audit_rows) == 1
    assert audit_rows[0].job_status == 'E'
    assert "Jobkennung fehlt" in audit_rows[0].message
```

---

## Test 3: Parameter Validation - Missing `p_Stichtag`

**Purpose:** Verify that the orchestration procedure correctly identifies and handles a missing `p_Stichtag` parameter, raising an error and logging the failure.

**Setup:**
1.  Ensure `project.dataset.job_audit_table` is empty.

**Action:**
Execute the main orchestration procedure with `p_Stichtag` as `NULL`.
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_02',
  p_EintragsNr => 'ENTRY_002',
  p_Stichtag => NULL,
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call raises a BigQuery exception with the message 'Stichtag fehlt'.
*   `project.dataset.job_audit_table` contains exactly one row with `job_status = 'E'` and `message` containing 'Stichtag fehlt'.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown fixtures as above) ...

def test_missing_stichtag_parameter():
    job_kennung = "TEST_JOB_02"
    eintrags_nr = "ENTRY_002"
    wiederanlauf_wert = 0

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => NULL,
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    with pytest.raises(BadRequest) as excinfo:
        _execute_bq_query(call_proc_sql)

    assert "Stichtag fehlt" in str(excinfo.value)

    audit_rows = list(_execute_bq_query(f"SELECT job_status, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table`"))
    assert len(audit_rows) == 1
    assert audit_rows[0].job_status == 'E'
    assert "Stichtag fehlt" in audit_rows[0].message
```

---

## Test 4: Parameter Validation - Missing `p_EintragsNr`

**Purpose:** Verify that the orchestration procedure correctly identifies and handles a missing `p_EintragsNr` parameter, raising an error and logging the failure.

**Setup:**
1.  Ensure `project.dataset.job_audit_table` is empty.

**Action:**
Execute the main orchestration procedure with `p_EintragsNr` as `NULL`.
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_03',
  p_EintragsNr => NULL,
  p_Stichtag => '01012023',
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call raises a BigQuery exception with the message 'EintragsNr fehlt'.
*   `project.dataset.job_audit_table` contains exactly one row with `job_status = 'E'` and `message` containing 'EintragsNr fehlt'.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown fixtures as above) ...

def test_missing_eintragsnr_parameter():
    job_kennung = "TEST_JOB_03"
    stichtag = "01012023"
    wiederanlauf_wert = 0

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => NULL,
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    with pytest.raises(BadRequest) as excinfo:
        _execute_bq_query(call_proc_sql)

    assert "EintragsNr fehlt" in str(excinfo.value)

    audit_rows = list(_execute_bq_query(f"SELECT job_status, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table`"))
    assert len(audit_rows) == 1
    assert audit_rows[0].job_status == 'E'
    assert "EintragsNr fehlt" in audit_rows[0].message
```

---

## Test 5: Date Validation - Invalid `p_Stichtag` Format

**Purpose:** Verify that the orchestration procedure correctly identifies and handles an invalid `p_Stichtag` format (not DDMMYYYY), raising an error and logging the failure.

**Setup:**
1.  Ensure `project.dataset.job_audit_table` is empty.

**Action:**
Execute the main orchestration procedure with `p_Stichtag` in an invalid format (e.g., '2023-01-01').
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_04',
  p_EintragsNr => 'ENTRY_004',
  p_Stichtag => '2023-01-01', -- Invalid format
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call raises a BigQuery exception with the message 'Ungueltiges Datumformat fuer Stichtag. Erwartet DDMMYYYY.'.
*   `project.dataset.job_audit_table` contains exactly one row with `job_status = 'E'` and `message` containing 'Ungueltiges Datumformat'.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown fixtures as above) ...

def test_invalid_stichtag_format():
    job_kennung = "TEST_JOB_04"
    eintrags_nr = "ENTRY_004"
    stichtag = "2023-01-01" # Invalid format
    wiederanlauf_wert = 0

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    with pytest.raises(BadRequest) as excinfo:
        _execute_bq_query(call_proc_sql)

    assert "Ungueltiges Datumformat fuer Stichtag. Erwartet DDMMYYYY." in str(excinfo.value)

    audit_rows = list(_execute_bq_query(f"SELECT job_status, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table`"))
    assert len(audit_rows) == 1
    assert audit_rows[0].job_status == 'E'
    assert "Ungueltiges Datumformat" in audit_rows[0].message
```

---

## Test 6: Default `p_wiederanlaufWert`

**Purpose:** Verify that `p_wiederanlaufWert` defaults to `0` (and `restart_flag` to 'N') when not explicitly provided.

**Setup:**
1.  Ensure `project.dataset.PoolBasisprodukt` and `project.dataset.job_audit_table` are empty.
2.  `d_ausd_bp_ta_bpr_beschr_proc` is deployed as in Test 1.

**Action:**
Execute the main orchestration procedure without providing `p_wiederanlaufWert` (or passing `NULL`).
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_05',
  p_EintragsNr => 'ENTRY_005',
  p_Stichtag => '02012023',
  p_wiederanlaufWert => NULL -- Explicitly NULL, or omit if procedure allows
);
```

**Pass/Fail Criterion:**
*   The call completes without raising any BigQuery exceptions.
*   `project.dataset.job_audit_table` contains two rows:
    *   The main log entry (`tab_name = 'PoolBasisprodukt'`) has `restart_flag = 'N'`.
    *   The `d_ausd_bp_ta_bpr_beschr_proc_log` entry confirms `RestartValue = 0` was passed to the business logic.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown fixtures as above) ...

def test_default_wiederanlaufwert():
    stichtag = "02012023"
    job_kennung = "TEST_JOB_05"
    eintrags_nr = "ENTRY_005"

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => NULL
    );
    """
    _execute_bq_query(call_proc_sql)

    audit_rows = list(_execute_bq_query(f"SELECT tab_name, restart_flag, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table` ORDER BY insert_timestamp ASC"))
    assert len(audit_rows) == 2

    # Check d_ausd_bp_ta_bpr_beschr_proc_log entry
    proc_log_entry = audit_rows[0]
    assert proc_log_entry.tab_name == 'd_ausd_bp_ta_bpr_beschr_proc_log'
    assert proc_log_entry.restart_flag == 'N'
    assert "RestartValue=0" in proc_log_entry.message

    # Check main orchestration log entry
    main_log_entry = audit_rows[1]
    assert main_log_entry.tab_name == 'PoolBasisprodukt'
    assert main_log_entry.restart_flag == 'N'
```

---

## Test 7: Explicit `p_wiederanlaufWert`

**Purpose:** Verify that an explicitly provided `p_wiederanlaufWert` is correctly passed and reflected in the audit log (`restart_flag = 'Y'` for `1`).

**Setup:**
1.  Ensure `project.dataset.PoolBasisprodukt` and `project.dataset.job_audit_table` are empty.
2.  `d_ausd_bp_ta_bpr_beschr_proc` is deployed as in Test 1.

**Action:**
Execute the main orchestration procedure with `p_wiederanlaufWert = 1`.
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_06',
  p_EintragsNr => 'ENTRY_006',
  p_Stichtag => '03012023',
  p_wiederanlaufWert => 1
);
```

**Pass/Fail Criterion:**
*   The call completes without raising any BigQuery exceptions.
*   `project.dataset.job_audit_table` contains two rows:
    *   The main log entry (`tab_name = 'PoolBasisprodukt'`) has `restart_flag = 'Y'`.
    *   The `d_ausd_bp_ta_bpr_beschr_proc_log` entry confirms `RestartValue = 1` was passed to the business logic.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown fixtures as above) ...

def test_explicit_wiederanlaufwert():
    stichtag = "03012023"
    job_kennung = "TEST_JOB_06"
    eintrags_nr = "ENTRY_006"
    wiederanlauf_wert = 1

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => {wiederanlauf_wert}
    );
    """
    _execute_bq_query(call_proc_sql)

    audit_rows = list(_execute_bq_query(f"SELECT tab_name, restart_flag, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table` ORDER BY insert_timestamp ASC"))
    assert len(audit_rows) == 2

    # Check d_ausd_bp_ta_bpr_beschr_proc_log entry
    proc_log_entry = audit_rows[0]
    assert proc_log_entry.tab_name == 'd_ausd_bp_ta_bpr_beschr_proc_log'
    assert proc_log_entry.restart_flag == 'Y'
    assert "RestartValue=1" in proc_log_entry.message

    # Check main orchestration log entry
    main_log_entry = audit_rows[1]
    assert main_log_entry.tab_name == 'PoolBasisprodukt'
    assert main_log_entry.restart_flag == 'Y'
```

---

## Test 8: Record Count - Multiple Records

**Purpose:** Verify that the `v_records` variable in the orchestration procedure correctly counts multiple records inserted by the business logic, and this count is reflected in the audit log.

**Setup:**
1.  Ensure `project.dataset.PoolBasisprodukt` and `project.dataset.job_audit_table` are empty.
2.  Modify `d_ausd_bp_ta_bpr_beschr_proc` to insert *three* rows for the given `stichtag_date`.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`(
      IN p_EintragsNr STRING, IN p_JobKennung STRING, IN p_Stichtag STRING,
      IN p_RestartValue INT64, IN p_DatumHeute DATE, IN p_DatumGestern DATE
    )
    BEGIN
      DECLARE v_stichtag_date DATE DEFAULT SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
      INSERT INTO `project.dataset.PoolBasisprodukt` (id, data, stichtag_date)
      VALUES
        (GENERATE_UUID(), 'Data 1', v_stichtag_date),
        (GENERATE_UUID(), 'Data 2', v_stichtag_date),
        (GENERATE_UUID(), 'Data 3', v_stichtag_date);
      INSERT INTO `project.dataset.job_audit_table` (...) VALUES ('d_ausd_bp_ta_bpr_beschr_proc_log', ..., 3, ...); -- Adjust record_count
    END;
    ```

**Action:**
Execute the main orchestration procedure with valid parameters.
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_07',
  p_EintragsNr => 'ENTRY_007',
  p_Stichtag => '04012023',
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call completes without raising any BigQuery exceptions.
*   `project.dataset.PoolBasisprodukt` contains exactly three rows with `stichtag_date = '2023-01-04'`.
*   `project.dataset.job_audit_table` contains two rows:
    *   The main log entry (`tab_name = 'PoolBasisprodukt'`) has `record_count = 3`.
    *   The `d_ausd_bp_ta_bpr_beschr_proc_log` entry confirms 3 records were processed by the business logic.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown fixtures as above) ...

# Helper to temporarily redefine d_ausd_bp_ta_bpr_beschr_proc
def _redefine_d_ausd_proc(insert_count=1, raise_error=False):
    error_stmt = "RAISE USING MESSAGE = 'Simulated business logic error';" if raise_error else ""
    insert_values = ""
    if insert_count > 0:
        insert_values = "VALUES " + ", ".join([
            f"(GENERATE_UUID(), 'Data {i+1}', v_stichtag_date)" for i in range(insert_count)
        ]) + ";"
    
    proc_definition = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.d_ausd_bp_ta_bpr_beschr_proc`(
      IN p_EintragsNr STRING, IN p_JobKennung STRING, IN p_Stichtag STRING,
      IN p_RestartValue INT64, IN p_DatumHeute DATE, IN p_DatumGestern DATE
    )
    BEGIN
      DECLARE v_stichtag_date DATE DEFAULT SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
      IF v_stichtag_date IS NULL THEN SET v_stichtag_date = CURRENT_DATE(); END IF; -- For error cases where stichtag is invalid

      {error_stmt}

      IF NOT {raise_error} THEN
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt` (id, data, stichtag_date)
        {insert_values}

        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_audit_table` (
          tab_name, job_status, load_type, stichtag, run_date, job_kind, restart_flag, record_count, message, insert_timestamp
        )
        VALUES (
          'd_ausd_bp_ta_bpr_beschr_proc_log', 'I', 'P', p_Stichtag, v_stichtag_date, 'J', CASE WHEN p_RestartValue = 1 THEN 'Y' ELSE 'N' END, {insert_count},
          FORMAT('Called with JobKennung=%s, EintragsNr=%s, RestartValue=%d, DatumHeute=%s, DatumGestern=%s',
                 p_JobKennung, p_EintragsNr, p_RestartValue, CAST(p_DatumHeute AS STRING), CAST(p_DatumGestern AS STRING)),
          CURRENT_TIMESTAMP()
        );
      END IF;
    END;
    """
    _execute_bq_query(proc_definition)

def test_record_count_multiple_records():
    _redefine_d_ausd_proc(insert_count=3) # Setup for 3 records
    stichtag = "04012023"
    job_kennung = "TEST_JOB_07"
    eintrags_nr = "ENTRY_007"

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => 0
    );
    """
    _execute_bq_query(call_proc_sql)

    pool_basis_produkt_rows = list(_execute_bq_query(f"SELECT stichtag_date FROM `{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt`"))
    assert len(pool_basis_produkt_rows) == 3
    assert all(row.stichtag_date == date(2023, 1, 4) for row in pool_basis_produkt_rows)

    audit_rows = list(_execute_bq_query(f"SELECT tab_name, record_count FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table` ORDER BY insert_timestamp ASC"))
    assert len(audit_rows) == 2

    proc_log_entry = audit_rows[0]
    assert proc_log_entry.tab_name == 'd_ausd_bp_ta_bpr_beschr_proc_log'
    assert proc_log_entry.record_count == 3

    main_log_entry = audit_rows[1]
    assert main_log_entry.tab_name == 'PoolBasisprodukt'
    assert main_log_entry.record_count == 3
```

---

## Test 9: Record Count - Zero Records

**Purpose:** Verify that the `v_records` variable in the orchestration procedure correctly counts zero records when the business logic inserts none, and this count is reflected in the audit log.

**Setup:**
1.  Ensure `project.dataset.PoolBasisprodukt` and `project.dataset.job_audit_table` are empty.
2.  Modify `d_ausd_bp_ta_bpr_beschr_proc` to insert *zero* rows.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`(
      IN p_EintragsNr STRING, IN p_JobKennung STRING, IN p_Stichtag STRING,
      IN p_RestartValue INT64, IN p_DatumHeute DATE, IN p_DatumGestern DATE
    )
    BEGIN
      DECLARE v_stichtag_date DATE DEFAULT SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
      -- No INSERT statement here
      INSERT INTO `project.dataset.job_audit_table` (...) VALUES ('d_ausd_bp_ta_bpr_beschr_proc_log', ..., 0, ...); -- Adjust record_count
    END;
    ```

**Action:**
Execute the main orchestration procedure with valid parameters.
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_08',
  p_EintragsNr => 'ENTRY_008',
  p_Stichtag => '05012023',
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call completes without raising any BigQuery exceptions.
*   `project.dataset.PoolBasisprodukt` contains zero rows.
*   `project.dataset.job_audit_table` contains two rows:
    *   The main log entry (`tab_name = 'PoolBasisprodukt'`) has `record_count = 0`.
    *   The `d_ausd_bp_ta_bpr_beschr_proc_log` entry confirms 0 records were processed by the business logic.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown, _redefine_d_ausd_proc fixtures as above) ...

def test_record_count_zero_records():
    _redefine_d_ausd_proc(insert_count=0) # Setup for 0 records
    stichtag = "05012023"
    job_kennung = "TEST_JOB_08"
    eintrags_nr = "ENTRY_008"

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => 0
    );
    """
    _execute_bq_query(call_proc_sql)

    pool_basis_produkt_rows = list(_execute_bq_query(f"SELECT stichtag_date FROM `{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt`"))
    assert len(pool_basis_produkt_rows) == 0

    audit_rows = list(_execute_bq_query(f"SELECT tab_name, record_count FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table` ORDER BY insert_timestamp ASC"))
    assert len(audit_rows) == 2

    proc_log_entry = audit_rows[0]
    assert proc_log_entry.tab_name == 'd_ausd_bp_ta_bpr_beschr_proc_log'
    assert proc_log_entry.record_count == 0

    main_log_entry = audit_rows[1]
    assert main_log_entry.tab_name == 'PoolBasisprodukt'
    assert main_log_entry.record_count == 0
```

---

## Test 10: Error Handling in Business Logic

**Purpose:** Verify that errors originating from the `d_ausd_bp_ta_bpr_beschr_proc` (business logic) are caught by the orchestration procedure, logged as a failure, and then re-raised.

**Setup:**
1.  Ensure `project.dataset.PoolBasisprodukt` and `project.dataset.job_audit_table` are empty.
2.  Modify `d_ausd_bp_ta_bpr_beschr_proc` to `RAISE` an error.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`(
      IN p_EintragsNr STRING, IN p_JobKennung STRING, IN p_Stichtag STRING,
      IN p_RestartValue INT64, IN p_DatumHeute DATE, IN p_DatumGestern DATE
    )
    BEGIN
      RAISE USING MESSAGE = 'Simulated business logic error';
    END;
    ```

**Action:**
Execute the main orchestration procedure with valid parameters.
```sql
CALL `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  p_JobKennung => 'TEST_JOB_09',
  p_EintragsNr => 'ENTRY_009',
  p_Stichtag => '06012023',
  p_wiederanlaufWert => 0
);
```

**Pass/Fail Criterion:**
*   The call raises a BigQuery exception with the message 'Simulated business logic error' (or similar, as the orchestration procedure re-raises it).
*   `project.dataset.job_audit_table` contains exactly one row with `job_status = 'E'` and `message` containing 'Simulated business logic error'.
*   `project.dataset.PoolBasisprodukt` remains empty.

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query, _clear_tables, setup_and_teardown, _redefine_d_ausd_proc fixtures as above) ...

def test_error_in_business_logic():
    _redefine_d_ausd_proc(raise_error=True) # Setup to raise an error
    stichtag = "06012023"
    job_kennung = "TEST_JOB_09"
    eintrags_nr = "ENTRY_009"

    call_proc_sql = f"""
    CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bpr_beschr`(
      p_JobKennung => '{job_kennung}',
      p_EintragsNr => '{eintrags_nr}',
      p_Stichtag => '{stichtag}',
      p_wiederanlaufWert => 0
    );
    """
    with pytest.raises(BadRequest) as excinfo:
        _execute_bq_query(call_proc_sql)

    assert "Simulated business logic error" in str(excinfo.value)

    # Verify PoolBasisprodukt is empty
    pool_basis_produkt_rows = list(_execute_bq_query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt`"))
    assert pool_basis_produkt_rows[0][0] == 0

    # Verify job_audit_table contains error entry
    audit_rows = list(_execute_bq_query(f"SELECT job_status, message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_table`"))
    assert len(audit_rows) == 1
    assert audit_rows[0].job_status == 'E'
    assert "Simulated business logic error" in audit_rows[0].message
```

---

## Test 11: Schema Assertions for `job_audit_table`

**Purpose:** Verify that the `job_audit_table` schema matches the design specifications, ensuring all expected columns and their data types are present.

**Setup:**
1.  Ensure `project.dataset.job_audit_table` has been created according to its DDL.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `job_audit_table`.

**Pass/Fail Criterion:**
*   The table `project.dataset.job_audit_table` exists.
*   It contains the following columns with the specified data types:
    *   `tab_name` (STRING)
    *   `job_status` (STRING)
    *   `load_type` (STRING)
    *   `stichtag` (STRING)
    *   `run_date` (DATE)
    *   `job_kind` (STRING)
    *   `restart_flag` (STRING)
    *   `record_count` (INT64)
    *   `message` (STRING)
    *   `insert_timestamp` (TIMESTAMP)

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query fixtures as above) ...

def test_job_audit_table_schema():
    expected_schema = {
        "tab_name": "STRING",
        "job_status": "STRING",
        "load_type": "STRING",
        "stichtag": "STRING",
        "run_date": "DATE",
        "job_kind": "STRING",
        "restart_flag": "STRING",
        "record_count": "INT64",
        "message": "STRING",
        "insert_timestamp": "TIMESTAMP",
    }

    query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_audit_table'
    """
    rows = list(_execute_bq_query(query))
    actual_schema = {row.column_name: row.data_type for row in rows}

    assert actual_schema == expected_schema
```

---

## Test 12: Schema Assertions for `PoolBasisprodukt` Table

**Purpose:** Verify that the `PoolBasisprodukt` table schema matches the design specifications (or the placeholder schema provided), ensuring all expected columns and their data types are present.

**Setup:**
1.  Ensure `project.dataset.PoolBasisprodukt` has been created according to its DDL.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `PoolBasisprodukt` table.

**Pass/Fail Criterion:**
*   The table `project.dataset.PoolBasisprodukt` exists.
*   It contains the following columns with the specified data types (based on the placeholder DDL):
    *   `id` (STRING)
    *   `data` (STRING)
    *   `stichtag_date` (DATE)
    *   (Note: This test should be updated once the actual schema from `d_ausd_bp_ta_bpr_beschr.sql` is known and implemented.)

**Runnable Test Code (Python/Pytest):**
```python
# ... (BQ_CLIENT, _execute_bq_query fixtures as above) ...

def test_pool_basis_produkt_table_schema():
    expected_schema = {
        "id": "STRING",
        "data": "STRING",
        "stichtag_date": "DATE",
    }

    query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'PoolBasisprodukt'
    """
    rows = list(_execute_bq_query(query))
    actual_schema = {row.column_name: row.data_type for row in rows}

    assert actual_schema == expected_schema
```