As a senior data-migration QA engineer, I've analyzed the provided KornShell script (`k_ausd_bp_ta_bcp_msisdn.ksh`) and its BigQuery migration design. The migration involves re-implementing orchestration logic, parameter handling, date calculations, and core SQL processing within a BigQuery Stored Procedure, along with replacing file-based record counting and a commented-out job management system with a BigQuery `job_control_table`.

The following test cases are designed to validate the migrated solution across output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

### General Test Setup

These tests assume a Python-based testing framework (e.g., Pytest) with the `google-cloud-bigquery` client library.

**Prerequisites:**
1.  A Google Cloud Project is set up.
2.  A BigQuery dataset named `dataset` exists within the project.
3.  All DDLs (`job_control_table.sql`, `sof_ta_bpr_bcp.sql`, `sof_ta_rn_vertrag.sql`, `dwtk_meldungen.sql`, `sof_ta_bcp_msisdn.sql`) have been deployed to create the necessary tables in the `dataset`.
4.  The BigQuery Stored Procedure `dataset.r_ausd_bp_ta_bcp_msisdn` has been deployed.

**Pytest Fixtures (for all tests):**

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from datetime import datetime, date, timedelta

# Replace with your actual project ID
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "dataset"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def setup_teardown_tables(bq_client):
    """
    Clears relevant tables before each test to ensure a clean state.
    This includes source, target, and job control tables.
    """
    tables_to_clear = [
        f"`{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp`",
        f"`{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag`",
        f"`{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn`",
        f"`{PROJECT_ID}.{DATASET_ID}.job_control_table`",
        # Add other tables if they are modified by the SP and need to be reset
    ]
    for table_path in tables_to_clear:
        bq_client.query(f"TRUNCATE TABLE {table_path}").result()
    yield
    # Optional: Add post-test cleanup if necessary, though TRUNCATE before each test is usually sufficient.
```

---

### Test Case 1: Successful Execution - Happy Path

**Purpose:** Verify that the BigQuery stored procedure executes successfully with valid parameters, processes data correctly according to the `INNER JOIN` and `DISTINCT` logic, and logs the job status and record count. This covers output parity and basic transformation correctness.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` is populated with:
    ```sql
    INSERT INTO `dataset.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
    ('C1', 'B1', 'CR1'),
    ('C2', 'B2', 'CR2'),
    ('C3', 'B3', 'CR3'),
    ('C4', 'B4', 'CR4_NO_MATCH');
    ```
2.  `dataset.sof_ta_rn_vertrag` is populated with:
    ```sql
    INSERT INTO `dataset.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
    ('CR1', '11111'),
    ('CR2', '22222'),
    ('CR2', '33333'), -- Multiple MSISDNs for same contract_id_ref
    ('CR5_NO_MATCH', '44444');
    ```
3.  Input parameters for the stored procedure:
    `p_JobKennung = 'TEST_JOB_001'`
    `p_EintragsNr = 'ENTRY_001'`
    `p_Stichtag = '01012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure `dataset.r_ausd_bp_ta_bcp_msisdn` with the defined parameters.

**Pass/Fail Criterion:**
1.  The `dataset.sof_ta_bcp_msisdn` table contains the expected 3 distinct rows:
    *   `('C1', 'B1', 'CR1', '11111')`
    *   `('C2', 'B2', 'CR2', '22222')`
    *   `('C2', 'B2', 'CR2', '33333')`
2.  The `dataset.job_control_table` contains an entry for `TEST_JOB_001` with:
    *   `status = 'SUCCESS'`
    *   `processed_records = 3`
    *   `stichtag = DATE('2023-01-01')`
    *   `error_message` is `NULL`.
    *   `start_timestamp` and `end_timestamp` are populated, with `end_timestamp` > `start_timestamp`.

```python
def test_successful_execution_happy_path(bq_client):
    # Setup: Populate source tables
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
        ('C1', 'B1', 'CR1'),
        ('C2', 'B2', 'CR2'),
        ('C3', 'B3', 'CR3'),
        ('C4', 'B4', 'CR4_NO_MATCH');
    """).result()

    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
        ('CR1', '11111'),
        ('CR2', '22222'),
        ('CR2', '33333'),
        ('CR5_NO_MATCH', '44444');
    """).result()

    # Action: Execute stored procedure
    job_kennung = 'TEST_JOB_001'
    eintrags_nr = 'ENTRY_001'
    stichtag = '01012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    bq_client.query(query).result()

    # Assertions
    expected_data = [
        {'cntrct_id': 'C1', 'bpr_id': 'B1', 'cntrct_id_ref': 'CR1', 'tn_tel_msisdn': '11111'},
        {'cntrct_id': 'C2', 'bpr_id': 'B2', 'cntrct_id_ref': 'CR2', 'tn_tel_msisdn': '22222'},
        {'cntrct_id': 'C2', 'bpr_id': 'B2', 'cntrct_id_ref': 'CR2', 'tn_tel_msisdn': '33333'}
    ]
    result_rows = bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn` ORDER BY cntrct_id, tn_tel_msisdn").result()
    actual_data = [dict(row) for row in result_rows]
    assert len(actual_data) == len(expected_data)
    assert actual_data == sorted(expected_data, key=lambda x: (x['cntrct_id'], x['tn_tel_msisdn']))

    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, status, processed_records, error_message, start_timestamp, end_timestamp
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]

    assert job_log['job_kennung'] == job_kennung
    assert job_log['eintrags_nr'] == eintrags_nr
    assert job_log['stichtag'] == date(2023, 1, 1)
    assert job_log['status'] == 'SUCCESS'
    assert job_log['processed_records'] == len(expected_data)
    assert job_log['error_message'] is None
    assert job_log['start_timestamp'] is not None
    assert job_log['end_timestamp'] is not None
    assert job_log['end_timestamp'] > job_log['start_timestamp']
```

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the stored procedure correctly identifies and raises an error for a missing or empty `p_JobKennung` parameter, and logs the failure.

**Setup:**
1.  `dataset.job_control_table` is cleared.
2.  Input parameters:
    `p_JobKennung = ''` (empty string)
    `p_EintragsNr = 'ENTRY_002'`
    `p_Stichtag = '02012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Attempt to execute the BigQuery stored procedure with `p_JobKennung` as an empty string.

**Pass/Fail Criterion:**
1.  The stored procedure execution fails with a `BadRequest` error containing "p_JobKennung is missing or empty".
2.  An entry exists in `dataset.job_control_table` with:
    *   `job_kennung = ''`
    *   `status = 'FAILED'`
    *   `stichtag` is `NULL` (as the error occurs before date parsing/update).
    *   `error_message` containing "p_JobKennung is missing or empty".

```python
def test_parameter_validation_missing_jobkennung(bq_client):
    # Setup: No source data needed for this validation test
    job_kennung = ''
    eintrags_nr = 'ENTRY_002'
    stichtag = '02012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    
    # Action: Execute stored procedure and expect failure
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(query).result()

    # Assertions
    assert "p_JobKennung is missing or empty" in str(excinfo.value)

    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, status, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]

    assert job_log['job_kennung'] == job_kennung
    assert job_log['eintrags_nr'] == eintrags_nr
    assert job_log['stichtag'] is None
    assert job_log['status'] == 'FAILED'
    assert "p_JobKennung is missing or empty" in job_log['error_message']
```

---

### Test Case 3: Parameter Validation - Invalid `p_Stichtag` Format

**Purpose:** Verify that the stored procedure correctly identifies and raises an error for an invalid `p_Stichtag` format, and logs the failure.

**Setup:**
1.  `dataset.job_control_table` is cleared.
2.  Input parameters:
    `p_JobKennung = 'TEST_JOB_003'`
    `p_EintragsNr = 'ENTRY_003'`
    `p_Stichtag = '2023-01-01'` (invalid format, expects DDMMYYYY)
    `p_wiederanlaufWert = '0'`

**Action:**
Attempt to execute the BigQuery stored procedure with the invalid `p_Stichtag`.

**Pass/Fail Criterion:**
1.  The stored procedure execution fails with a `BadRequest` error containing "Invalid date format for p_Stichtag".
2.  An entry exists in `dataset.job_control_table` with:
    *   `job_kennung = 'TEST_JOB_003'`
    *   `status = 'FAILED'`
    *   `stichtag` is `NULL` (as the error occurs during date parsing).
    *   `error_message` containing "Invalid date format for p_Stichtag".

```python
def test_parameter_validation_invalid_stichtag_format(bq_client):
    # Setup: No source data needed
    job_kennung = 'TEST_JOB_003'
    eintrags_nr = 'ENTRY_003'
    stichtag = '2023-01-01' # Invalid format
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    
    # Action: Execute stored procedure and expect failure
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(query).result()

    # Assertions
    assert "Invalid date format for p_Stichtag" in str(excinfo.value)

    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, status, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]

    assert job_log['job_kennung'] == job_kennung
    assert job_log['eintrags_nr'] == eintrags_nr
    assert job_log['stichtag'] is None
    assert job_log['status'] == 'FAILED'
    assert "Invalid date format for p_Stichtag" in job_log['error_message']
```

---

### Test Case 4: Transformation Correctness - No Matching Data

**Purpose:** Verify that when there are no matching records between source tables based on the `INNER JOIN` condition, the target table remains empty, and the job logs a processed record count of 0.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` is populated with data that has no corresponding `cntrct_id_ref` in `dataset.sof_ta_rn_vertrag`:
    ```sql
    INSERT INTO `dataset.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
    ('C10', 'B10', 'CR10_NO_MATCH'),
    ('C11', 'B11', 'CR11_NO_MATCH');
    ```
2.  `dataset.sof_ta_rn_vertrag` is populated with data that has no corresponding `cntrct_id` in `dataset.sof_ta_bpr_bcp`:
    ```sql
    INSERT INTO `dataset.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
    ('CR20_NO_MATCH', '55555');
    ```
3.  Input parameters:
    `p_JobKennung = 'TEST_JOB_004'`
    `p_EintragsNr = 'ENTRY_004'`
    `p_Stichtag = '03012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure.

**Pass/Fail Criterion:**
1.  `dataset.sof_ta_bcp_msisdn` is empty (0 rows).
2.  `dataset.job_control_table` contains an entry for `TEST_JOB_004` with:
    *   `status = 'SUCCESS'`
    *   `processed_records = 0`
    *   `error_message` is `NULL`.

```python
def test_transformation_correctness_no_matching_data(bq_client):
    # Setup: Populate source tables with non-matching data
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
        ('C10', 'B10', 'CR10_NO_MATCH'),
        ('C11', 'B11', 'CR11_NO_MATCH');
    """).result()

    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
        ('CR20_NO_MATCH', '55555');
    """).result()

    # Action: Execute stored procedure
    job_kennung = 'TEST_JOB_004'
    eintrags_nr = 'ENTRY_004'
    stichtag = '03012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    bq_client.query(query).result()

    # Assertions
    result_rows = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn`").result()
    assert list(result_rows)[0][0] == 0

    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, status, processed_records, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]

    assert job_log['job_kennung'] == job_kennung
    assert job_log['eintrags_nr'] == eintrags_nr
    assert job_log['stichtag'] == date(2023, 1, 3)
    assert job_log['status'] == 'SUCCESS'
    assert job_log['processed_records'] == 0
    assert job_log['error_message'] is None
```

---

### Test Case 5: Transformation Correctness - `DISTINCT` Behavior

**Purpose:** Verify that the `DISTINCT` clause in the `INSERT ... SELECT` statement correctly handles duplicate rows resulting from the join, ensuring only unique combinations of `CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN` are inserted.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` is populated with:
    ```sql
    INSERT INTO `dataset.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
    ('C_DUP1', 'B_DUP1', 'CR_DUP1'),
    ('C_DUP2', 'B_DUP2', 'CR_DUP2'),
    ('C_DUP2', 'B_DUP2', 'CR_DUP2'); -- Duplicate in source bpr_bcp
    ```
2.  `dataset.sof_ta_rn_vertrag` is populated with:
    ```sql
    INSERT INTO `dataset.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
    ('CR_DUP1', 'MSISDN_A'),
    ('CR_DUP1', 'MSISDN_B'), -- Multiple MSISDNs for same contract_id_ref
    ('CR_DUP2', 'MSISDN_C');
    ```
3.  Input parameters:
    `p_JobKennung = 'TEST_JOB_005'`
    `p_EintragsNr = 'ENTRY_005'`
    `p_Stichtag = '04012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure.

**Pass/Fail Criterion:**
1.  The `dataset.sof_ta_bcp_msisdn` table contains exactly 3 unique rows:
    *   `('C_DUP1', 'B_DUP1', 'CR_DUP1', 'MSISDN_A')`
    *   `('C_DUP1', 'B_DUP1', 'CR_DUP1', 'MSISDN_B')`
    *   `('C_DUP2', 'B_DUP2', 'CR_DUP2', 'MSISDN_C')`
2.  The `processed_records` in `dataset.job_control_table` is 3.

```python
def test_transformation_correctness_distinct_behavior(bq_client):
    # Setup: Populate source tables to test DISTINCT
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
        ('C_DUP1', 'B_DUP1', 'CR_DUP1'),
        ('C_DUP2', 'B_DUP2', 'CR_DUP2'),
        ('C_DUP2', 'B_DUP2', 'CR_DUP2'); -- Duplicate in source bpr_bcp
    """).result()

    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
        ('CR_DUP1', 'MSISDN_A'),
        ('CR_DUP1', 'MSISDN_B'),
        ('CR_DUP2', 'MSISDN_C');
    """).result()

    # Action: Execute stored procedure
    job_kennung = 'TEST_JOB_005'
    eintrags_nr = 'ENTRY_005'
    stichtag = '04012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    bq_client.query(query).result()

    # Assertions
    expected_data = [
        {'cntrct_id': 'C_DUP1', 'bpr_id': 'B_DUP1', 'cntrct_id_ref': 'CR_DUP1', 'tn_tel_msisdn': 'MSISDN_A'},
        {'cntrct_id': 'C_DUP1', 'bpr_id': 'B_DUP1', 'cntrct_id_ref': 'CR_DUP1', 'tn_tel_msisdn': 'MSISDN_B'},
        {'cntrct_id': 'C_DUP2', 'bpr_id': 'B_DUP2', 'cntrct_id_ref': 'CR_DUP2', 'tn_tel_msisdn': 'MSISDN_C'}
    ]
    result_rows = bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn` ORDER BY cntrct_id, tn_tel_msisdn").result()
    actual_data = [dict(row) for row in result_rows]
    assert len(actual_data) == len(expected_data)
    assert actual_data == sorted(expected_data, key=lambda x: (x['cntrct_id'], x['tn_tel_msisdn']))

    job_log_rows = bq_client.query(f"""
        SELECT processed_records
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]
    assert job_log['processed_records'] == len(expected_data)
```

---

### Test Case 6: External System Replacement - `job_control_table` for Job Status and Record Count

**Purpose:** Verify that the `job_control_table` accurately records the job's lifecycle, including start, end, status, and processed record count, replacing the legacy temporary file and commented-out FOS system.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` is populated with: `('C_LOG', 'B_LOG', 'CR_LOG')`.
2.  `dataset.sof_ta_rn_vertrag` is populated with: `('CR_LOG', 'MSISDN_LOG')`.
3.  Input parameters for a successful run:
    `p_JobKennung = 'TEST_JOB_006'`
    `p_EintragsNr = 'ENTRY_006'`
    `p_Stichtag = '05012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure.

**Pass/Fail Criterion:**
1.  A `job_control_table` entry exists for `TEST_JOB_006`.
2.  `start_timestamp` and `end_timestamp` are populated and `end_timestamp` is after `start_timestamp`.
3.  `status` is 'SUCCESS'.
4.  `processed_records` matches the actual count of rows in `dataset.sof_ta_bcp_msisdn` (which should be 1).
5.  `error_message` is `NULL`.

```python
def test_external_system_replacement_job_control_table(bq_client):
    # Setup: Populate source tables
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
        ('C_LOG', 'B_LOG', 'CR_LOG');
    """).result()
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
        ('CR_LOG', 'MSISDN_LOG');
    """).result()

    # Action: Execute stored procedure
    job_kennung = 'TEST_JOB_006'
    eintrags_nr = 'ENTRY_006'
    stichtag = '05012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    bq_client.query(query).result()

    # Assertions
    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, start_timestamp, end_timestamp, status, processed_records, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]

    assert job_log['job_kennung'] == job_kennung
    assert job_log['eintrags_nr'] == eintrags_nr
    assert job_log['stichtag'] == date(2023, 1, 5)
    assert job_log['status'] == 'SUCCESS'
    assert job_log['processed_records'] == 1
    assert job_log['error_message'] is None
    assert job_log['start_timestamp'] is not None
    assert job_log['end_timestamp'] is not None
    assert job_log['end_timestamp'] > job_log['start_timestamp']
```

---

### Test Case 7: Data Quality - Target Table Schema and Constraints

**Purpose:** Verify that the target table `dataset.sof_ta_bcp_msisdn` adheres to its defined schema, including `NOT NULL` constraints, and that no `NULL` values are inserted into required columns.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` is populated with: `('C_NN', 'B_NN', 'CR_NN')`.
2.  `dataset.sof_ta_rn_vertrag` is populated with: `('CR_NN', 'MSISDN_NN')`.
3.  Input parameters for a successful run:
    `p_JobKennung = 'TEST_JOB_007'`
    `p_EintragsNr = 'ENTRY_007'`
    `p_Stichtag = '06012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure. Then, query the schema and data of `dataset.sof_ta_bcp_msisdn`.

**Pass/Fail Criterion:**
1.  The schema of `dataset.sof_ta_bcp_msisdn` matches the DDL, specifically that `cntrct_id`, `bpr_id`, `cntrct_id_ref`, and `tn_tel_msisdn` are `STRING` and `REQUIRED` (NOT NULL).
2.  No `NULL` values are present in any of the `REQUIRED` columns in the actual data after insertion.

```python
def test_data_quality_target_table_schema_constraints(bq_client):
    # Setup: Populate source tables with valid data
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
        ('C_NN', 'B_NN', 'CR_NN');
    """).result()
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
        ('CR_NN', 'MSISDN_NN');
    """).result()

    # Action: Execute stored procedure
    job_kennung = 'TEST_JOB_007'
    eintrags_nr = 'ENTRY_007'
    stichtag = '06012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    bq_client.query(query).result()

    # Assertions
    table_ref = bq_client.dataset(DATASET_ID).table('sof_ta_bcp_msisdn')
    table = bq_client.get_table(table_ref)

    expected_schema = {
        'cntrct_id': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'bpr_id': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'cntrct_id_ref': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'tn_tel_msisdn': {'field_type': 'STRING', 'mode': 'REQUIRED'}
    }
    actual_schema = {field.name: {'field_type': field.field_type, 'mode': field.mode} for field in table.schema}
    assert actual_schema == expected_schema

    null_check_query = f"""
        SELECT COUNT(*)
        FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn`
        WHERE cntrct_id IS NULL OR bpr_id IS NULL OR cntrct_id_ref IS NULL OR tn_tel_msisdn IS NULL;
    """
    null_count_rows = bq_client.query(null_check_query).result()
    assert list(null_count_rows)[0][0] == 0

    row_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn`;"
    row_count_rows = bq_client.query(row_count_query).result()
    assert list(row_count_rows)[0][0] == 1
```

---

### Test Case 8: Error Handling - Internal SQL Error

**Purpose:** Verify that if an error occurs during the core data processing SQL (e.g., due to a schema mismatch or constraint violation), the stored procedure catches it, logs the failure, and records an error message.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` is populated with: `('C_ERR', 'B_ERR', 'CR_ERR')`.
2.  `dataset.sof_ta_rn_vertrag` is populated with: `('CR_ERR', 'MSISDN_ERR')`.
3.  **Simulate an internal SQL error:** Temporarily drop a required column (`tn_tel_msisdn`) from the target table `dataset.sof_ta_bcp_msisdn` *before* the SP execution. This will cause the `INSERT` statement to fail.
4.  Input parameters:
    `p_JobKennung = 'TEST_JOB_008'`
    `p_EintragsNr = 'ENTRY_008'`
    `p_Stichtag = '07012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure.

**Pass/Fail Criterion:**
1.  The stored procedure execution fails with a `BadRequest` error.
2.  `dataset.sof_ta_bcp_msisdn` remains empty (due to `TRUNCATE` and failed `INSERT`).
3.  `dataset.job_control_table` contains an entry for `TEST_JOB_008` with:
    *   `status = 'FAILED'`
    *   `processed_records` is `NULL`.
    *   `error_message` containing "BigQuery SQL Error" and details about the column mismatch.

```python
def test_error_handling_internal_sql_error(bq_client):
    # Setup: Populate source tables
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
        ('C_ERR', 'B_ERR', 'CR_ERR');
    """).result()
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
        ('CR_ERR', 'MSISDN_ERR');
    """).result()

    # Simulate an internal SQL error by dropping a required column
    bq_client.query(f"ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn` DROP COLUMN tn_tel_msisdn").result()

    # Action: Execute stored procedure and expect failure
    job_kennung = 'TEST_JOB_008'
    eintrags_nr = 'ENTRY_008'
    stichtag = '07012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    
    try:
        bq_client.query(query).result()
        pytest.fail("Stored procedure was expected to fail but succeeded.")
    except BadRequest as excinfo:
        assert "BigQuery SQL Error" in str(excinfo.value)
        assert "tn_tel_msisdn" in str(excinfo.value) # Specific to our simulated error
    finally:
        # Recreate the dropped column for subsequent tests
        bq_client.query(f"ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn` ADD COLUMN tn_tel_msisdn STRING NOT NULL").result()

    # Assertions for job_control_table
    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, status, processed_records, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]

    assert job_log['job_kennung'] == job_kennung
    assert job_log['eintrags_nr'] == eintrags_nr
    assert job_log['stichtag'] == date(2023, 1, 7)
    assert job_log['status'] == 'FAILED'
    assert job_log['processed_records'] is None
    assert "BigQuery SQL Error" in job_log['error_message']
    assert "tn_tel_msisdn" in job_log['error_message']
```

---

### Test Case 9: Idempotency - Multiple Runs

**Purpose:** Verify that running the stored procedure multiple times with the same parameters produces the same final state in the target table and correctly updates the `job_control_table` for each run. This confirms the `TRUNCATE` behavior.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` is populated with: `('C_IDEM', 'B_IDEM', 'CR_IDEM')`.
2.  `dataset.sof_ta_rn_vertrag` is populated with: `('CR_IDEM', 'MSISDN_IDEM')`.
3.  Input parameters:
    `p_JobKennung = 'TEST_JOB_009'`
    `p_EintragsNr = 'ENTRY_009'`
    `p_Stichtag = '08012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure twice with the same parameters.

**Pass/Fail Criterion:**
1.  After the second run, `dataset.sof_ta_bcp_msisdn` contains the exact same data (1 row) as after the first run.
2.  `dataset.job_control_table` contains two distinct entries for the same job, each with `status = 'SUCCESS'`, `processed_records = 1`, and different `start_timestamp`/`end_timestamp`.

```python
def test_idempotency_multiple_runs(bq_client):
    # Setup: Populate source tables
    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_bcp` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
        ('C_IDEM', 'B_IDEM', 'CR_IDEM');
    """).result()

    bq_client.query(f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_rn_vertrag` (cntrct_id, tn_tel_msisdn) VALUES
        ('CR_IDEM', 'MSISDN_IDEM');
    """).result()

    job_kennung = 'TEST_JOB_009'
    eintrags_nr = 'ENTRY_009'
    stichtag = '08012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    
    # Action: Execute stored procedure twice
    bq_client.query(query).result()
    first_run_data_rows = bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn` ORDER BY cntrct_id").result()
    first_run_data = [dict(row) for row in first_run_data_rows]
    first_run_record_count = len(first_run_data)

    bq_client.query(query).result()
    second_run_data_rows = bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn` ORDER BY cntrct_id").result()
    second_run_data = [dict(row) for row in second_run_data_rows]
    second_run_record_count = len(second_run_data)

    # Assertions
    assert first_run_data == second_run_data
    assert first_run_record_count == 1

    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, status, processed_records, start_timestamp, end_timestamp
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp ASC
    """).result()
    job_logs = [dict(row) for row in job_log_rows]

    assert len(job_logs) == 2
    assert job_logs[0]['status'] == 'SUCCESS'
    assert job_logs[0]['processed_records'] == first_run_record_count
    assert job_logs[1]['status'] == 'SUCCESS'
    assert job_logs[1]['processed_records'] == second_run_record_count
    assert job_logs[1]['start_timestamp'] > job_logs[0]['end_timestamp']
```

---

### Test Case 10: Edge Case - Empty Source Tables

**Purpose:** Verify that the stored procedure handles empty source tables gracefully, resulting in an empty target table and a processed record count of 0.

**Setup:**
1.  `dataset.sof_ta_bpr_bcp` and `dataset.sof_ta_rn_vertrag` are empty (handled by `setup_teardown_tables` fixture).
2.  Input parameters:
    `p_JobKennung = 'TEST_JOB_010'`
    `p_EintragsNr = 'ENTRY_010'`
    `p_Stichtag = '09012023'`
    `p_wiederanlaufWert = '0'`

**Action:**
Execute the BigQuery stored procedure.

**Pass/Fail Criterion:**
1.  `dataset.sof_ta_bcp_msisdn` is empty (0 rows).
2.  `dataset.job_control_table` contains an entry for `TEST_JOB_010` with:
    *   `status = 'SUCCESS'`
    *   `processed_records = 0`
    *   `error_message` is `NULL`.

```python
def test_edge_case_empty_source_tables(bq_client):
    # Setup: Source tables are empty (ensured by fixture)

    # Action: Execute stored procedure
    job_kennung = 'TEST_JOB_010'
    eintrags_nr = 'ENTRY_010'
    stichtag = '09012023'
    wiederanlauf_wert = '0'

    query = f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_bp_ta_bcp_msisdn`(
            '{job_kennung}',
            '{eintrags_nr}',
            '{stichtag}',
            '{wiederanlauf_wert}'
        );
    """
    bq_client.query(query).result()

    # Assertions
    result_rows = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bcp_msisdn`").result()
    assert list(result_rows)[0][0] == 0

    job_log_rows = bq_client.query(f"""
        SELECT job_kennung, eintrags_nr, stichtag, status, processed_records, error_message
        FROM `{PROJECT_ID}.{DATASET_ID}.job_control_table`
        WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        ORDER BY start_timestamp DESC LIMIT 1
    """).result()
    job_log = [dict(row) for row in job_log_rows][0]

    assert job_log['job_kennung'] == job_kennung
    assert job_log['eintrags_nr'] == eintrags_nr
    assert job_log['stichtag'] == date(2023, 1, 9)
    assert job_log['status'] == 'SUCCESS'
    assert job_log['processed_records'] == 0
    assert job_log['error_message'] is None
```