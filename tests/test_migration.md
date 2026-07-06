# Migration Validation Test Suite: `ausd_bp_ta_bpr_basis`

This document provides a comprehensive, production-grade test suite to validate the migration of the `ausd_bp_ta_bpr_basis` pipeline from its legacy Oracle/KornShell implementation to Google Cloud Platform (BigQuery, Dataform, and Cloud Composer).

---

## Section 1: End-to-End Output Parity Test

### Purpose
To prove that given identical upstream source data, the migrated BigQuery/Dataform pipeline produces the exact same output dataset as the legacy Oracle pipeline.

### Setup
1. **Legacy Environment**:
   - Populate Oracle tables `rma$ta_sim`, `rma$ta_sim_card_type`, `sof$ta_bpr_basis_his`, and `isbert_schema.dwtk_meldungen` with a controlled, representative snapshot of production-like data (at least 10,000 rows, including edge cases).
   - Set the execution date in `dwtk_meldungen` to `2023-10-25 10:00:00` (which translates to `v_datum = '20231025'`).
   - Run the legacy shell script: `./r_ausd_bp_ta_bpr_basis.ksh -s 25102023`.
   - Export the resulting Oracle tables `sof$ta_sim` and `sof$ta_bpr_basis` to CSV format.

2. **Target Environment**:
   - Load the exact same raw snapshot data into the corresponding BigQuery tables: `carmen.rma_ta_sim`, `carmen.rma_ta_sim_card_type`, `isbert_schema.sof_ta_bpr_basis_his`, and `isbert_schema.dwtk_meldungen`.
   - Execute the Dataform workflow to populate target tables `isbert_schema.sof_ta_sim` and `isbert_schema.sof_ta_bpr_basis`.

### Action
Execute a Python-based parity validation script using `pytest` that pulls data from both environments, normalizes schemas, sorts the records, and performs a deep equality assertion.

```python
# test_e2e_parity.py
import pandas as pd
import pytest
from google.cloud import bigquery
import cx_Oracle

def test_sof_ta_bpr_basis_parity():
    # 1. Extract from Legacy Oracle
    dsn = cx_Oracle.makedsn("oracle-host", 1521, service_name="orcl")
    conn = cx_Oracle.connect(user="isbert_schema", password="password", dsn=dsn)
    oracle_query = """
        SELECT cntrct_id, bpr_id, bpr_instance_id, iccid, imsi_mcc, imsi_mnc, 
               imsi_hlr, imsi_si, valid_to, slave_number, e_id, card_type_name
        FROM sof$ta_bpr_basis
        ORDER BY cntrct_id, bpr_id, bpr_instance_id
    """
    df_legacy = pd.read_sql(oracle_query, con=conn)
    conn.close()

    # 2. Extract from Target BigQuery
    bq_client = bigquery.Client(project="gcp-project-id")
    bq_query = """
        SELECT cntrct_id, bpr_id, bpr_instance_id, iccid, imsi_mcc, imsi_mnc, 
               imsi_hlr, imsi_si, valid_to, slave_number, e_id, card_type_name
        FROM `gcp-project-id.isbert_schema.sof_ta_bpr_basis`
        ORDER BY cntrct_id, bpr_id, bpr_instance_id
    """
    df_target = bq_client.query(bq_query).to_dataframe()

    # 3. Normalize Data Types for Comparison
    for df in [df_legacy, df_target]:
        df.columns = [col.upper() for col in df.columns]
        df['VALID_TO'] = pd.to_datetime(df['VALID_TO']).dt.date
        # Fill NaNs/Nulls with a standard placeholder to ensure safe comparison
        df.fillna({
            'ICCID': 'NULL_VAL',
            'IMSI_MCC': 'NULL_VAL',
            'IMSI_MNC': 'NULL_VAL',
            'IMSI_HLR': 'NULL_VAL',
            'IMSI_SI': 'NULL_VAL',
            'SLAVE_NUMBER': -999,
            'E_ID': 'NULL_VAL',
            'CARD_TYPE_NAME': 'NULL_VAL'
        }, inplace=True)

    # 4. Assert Parity
    pd.testing.assert_frame_equal(df_legacy, df_target, check_dtype=False, check_exact=True)
```

### Pass/Fail Criterion
* **Pass**: The Dataframe assertion passes with zero row mismatches, zero column mismatches, and identical values across all fields.
* **Fail**: Any difference in row count, column structure, or cell values between the Oracle and BigQuery target tables.

---

## Section 2: Transformation Correctness & Edge Cases

### Test Case 2.1: Dynamic `v_datum` Extraction (Step 00)
#### Purpose
Verify that the dynamic extraction of `v_datum` from `dwtk_meldungen` correctly handles missing records, multiple records, and fallback defaults.

#### Setup
Insert test metadata records into `isbert_schema.dwtk_meldungen`:
* Scenario A: No records with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
* Scenario B: Multiple records with `job_kennung = 'BERT_DROP_TEMP_TABLE'` on different dates.

#### Action
Execute the following SQL assertions in BigQuery:

```sql
-- Test Scenario A: Expect Default '19000101'
TRUNCATE TABLE `isbert_schema.dwtk_meldungen`;

ASSERT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
) = '19000101' 
AS "Scenario A Failed: Default date was not 19000101";

-- Test Scenario B: Expect Max Date '20231025'
INSERT INTO `isbert_schema.dwtk_meldungen` (job_kennung, timecreated)
VALUES 
  ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-20 08:00:00')),
  ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-25 14:30:00')),
  ('OTHER_JOB', TIMESTAMP('2023-10-28 12:00:00'));

ASSERT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
) = '20231025' 
AS "Scenario B Failed: Max date was not 20231025";
```

#### Pass/Fail Criterion
* **Pass**: Both assertions execute successfully without throwing errors.
* **Fail**: Any assertion fails, indicating incorrect date derivation logic.

---

### Test Case 2.2: ICCID String Concatenation and Formatting (Step 02)
#### Purpose
Verify that the 5-part ICCID fields from `rma_ta_sim` are concatenated correctly with hyphens, matching the legacy Oracle behavior.

#### Setup
Insert a mock record into `rma_ta_sim` with distinct values for each ICCID component:
* `iccid_mi` = '89', `iccid_ii` = '49', `iccid_iai` = '011', `iccid_nr` = '1234567890', `iccid_cd` = '2'.

#### Action
Run the following validation query:

```sql
WITH test_iccid AS (
  SELECT
    CONCAT(sim.iccid_mi, '-', sim.iccid_ii, '-', sim.iccid_iai, '-', sim.iccid_nr, '-', sim.iccid_cd) AS iccid
  FROM (
    SELECT '89' AS iccid_mi, '49' AS iccid_ii, '011' AS iccid_iai, '1234567890' AS iccid_nr, '2' AS iccid_cd
  ) sim
)
SELECT iccid FROM test_iccid;
```

#### Pass/Fail Criterion
* **Pass**: The output is exactly `'89-49-011-1234567890-2'`.
* **Fail**: The output is null, missing hyphens, or incorrectly ordered.

---

### Test Case 2.3: Temporal Filtering Logic (Step 02)
#### Purpose
Verify that the temporal filters on `insert_at`, `modified_at`, `valid_from`, and `valid_to` correctly include or exclude records relative to `v_datum`.

#### Setup
Set `v_datum` to `'20231025'`. Insert records into `rma_ta_sim` representing different temporal states:
1. **Record 1 (Valid Active)**: `insert_at` = '2023-10-01', `modified_at` = NULL, `valid_from` = '2023-10-01', `valid_to` = NULL. (Should be **INCLUDED**)
2. **Record 2 (Historically Modified)**: `insert_at` = '2023-10-01', `modified_at` = '2023-10-20', `valid_from` = '2023-10-01', `valid_to` = NULL. (Should be **EXCLUDED** because modified before `v_datum`)
3. **Record 3 (Future Valid)**: `insert_at` = '2023-10-01', `modified_at` = NULL, `valid_from` = '2023-11-01', `valid_to` = NULL. (Should be **EXCLUDED** because valid from is in the future)
4. **Record 4 (Expired)**: `insert_at` = '2023-10-01', `modified_at` = NULL, `valid_from` = '2023-10-01', `valid_to` = '2023-10-24'. (Should be **EXCLUDED** because expired before `v_datum`)

#### Action
Execute the Dataform model `sof_ta_sim` and query the output:

```sql
ASSERT (
  SELECT COUNT(*) FROM `isbert_schema.sof_ta_sim`
) = 1
AS "Temporal filtering failed: Expected exactly 1 valid active record.";
```

#### Pass/Fail Criterion
* **Pass**: Only Record 1 is present in `sof_ta_sim`.
* **Fail**: Any of the invalid/expired records (2, 3, or 4) are present, or Record 1 is missing.

---

### Test Case 2.4: Deduplication via Analytical Window Function (Step 03)
#### Purpose
Verify that the `MAX(...) OVER (PARTITION BY cntrct_id, bpr_id)` analytical function correctly selects only the latest active product instance and handles `NULL` values in `valid_to` by defaulting to `4712-12-31`.

#### Setup
Insert three historical records for the same contract (`cntrct_id` = 9999) and product (`bpr_id` = 31) into `sof_ta_bpr_basis_his`:
* Record A: `bpri_com_id` = 101, `valid_to` = DATE '2022-12-31'
* Record B: `bpri_com_id` = 102, `valid_to` = DATE '2023-10-25'
* Record C: `bpri_com_id` = 103, `valid_to` = NULL (Active, should default to `4712-12-31`)

#### Action
Execute the Dataform model `sof_ta_bpr_basis` and query the output:

```sql
-- Assert that only the active record (Record C) with the maximum valid_to date is selected
ASSERT (
  SELECT bpr_instance_id 
  FROM `isbert_schema.sof_ta_bpr_basis` 
  WHERE cntrct_id = 9999 AND bpr_id = 31
) = 103
AS "Deduplication failed: Did not select the active record with NULL (4712-12-31) valid_to.";
```

#### Pass/Fail Criterion
* **Pass**: Only the record with `bpri_com_id = 103` is loaded into the target table.
* **Fail**: Older historical records (101 or 102) are loaded, or no records are loaded.

---

### Test Case 2.5: Left Outer Join Correctness (Step 03)
#### Purpose
Verify that the left outer join between `sof_ta_bpr_basis_his` and `sof_ta_sim` behaves correctly, ensuring that product instances without matching SIM records are still preserved with a `NULL` card type name.

#### Setup
Insert two records into `sof_ta_bpr_basis_his`:
* Record 1: `iccid` = 'MATCH-123' (Exists in `sof_ta_sim` with `card_type_name` = 'eSIM')
* Record 2: `iccid` = 'MISS-456' (Does not exist in `sof_ta_sim`)

#### Action
Execute the Dataform model `sof_ta_bpr_basis` and query the output:

```sql
-- Assert that the matching record has the correct card type
ASSERT (
  SELECT card_type_name FROM `isbert_schema.sof_ta_bpr_basis` WHERE iccid = 'MATCH-123'
) = 'eSIM'
AS "Join failed: Matching ICCID did not pull the correct card_type_name.";

-- Assert that the non-matching record is preserved with NULL card type
ASSERT (
  SELECT card_type_name FROM `isbert_schema.sof_ta_bpr_basis` WHERE iccid = 'MISS-456'
) IS NULL
AS "Join failed: Non-matching ICCID record was dropped or card_type_name was not NULL.";
```

#### Pass/Fail Criterion
* **Pass**: Both assertions execute successfully.
* **Fail**: The non-matching record is missing (indicating an inner join was used instead of a left join) or the matching record has incorrect details.

---

## Section 3: Orchestration & Parameter Validation Tests

### Purpose
To verify that the Cloud Composer Airflow DAG correctly validates input parameters, handles execution failures, and triggers Dataform compilations and executions in the correct sequence.

### Setup
Deploy the DAG `ausd_bp_ta_bpr_basis_orchestration` to the Cloud Composer environment.

### Action
Execute the DAG with different configuration payloads using the Airflow CLI or UI.

```python
# test_dag_validation.py
import pytest
from airflow.models import DagBag
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.timezone import utcnow

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="ausd_bp_ta_bpr_basis_orchestration")
    assert dag is not None
    assert len(dag.tasks) == 3

def test_parameter_validation_success(dagbag):
    dag = dagbag.get_dag(dag_id="ausd_bp_ta_bpr_basis_orchestration")
    # Simulate a run with a valid date format (DDMMYYYY)
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=utcnow(),
        data_interval=(utcnow(), utcnow()),
        start_date=utcnow(),
        run_type=DagRunType.MANUAL,
        conf={"stichtag": "25102023"}
    )
    
    ti = dagrun.get_task_instance(task_id="validate_params")
    ti.run(ignore_ti_state=True)
    assert ti.state == "success"

def test_parameter_validation_failure(dagbag):
    dag = dagbag.get_dag(dag_id="ausd_bp_ta_bpr_basis_orchestration")
    # Simulate a run with an invalid date format (YYYY-MM-DD)
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=utcnow(),
        data_interval=(utcnow(), utcnow()),
        start_date=utcnow(),
        run_type=DagRunType.MANUAL,
        conf={"stichtag": "2023-10-25"}
    )
    
    ti = dagrun.get_task_instance(task_id="validate_params")
    with pytest.raises(Exception):
        ti.run(ignore_ti_state=True)
```

### Pass/Fail Criterion
* **Pass**: 
  * The DAG loads without import errors.
  * Passing a valid `stichtag` (e.g., `'25102023'`) succeeds.
  * Passing an invalid `stichtag` (e.g., `'2023-10-25'`) fails the validation task immediately, preventing downstream Dataform execution.
* **Fail**: Any of the above conditions are not met.

---

## Section 4: Data Quality & Schema Assertions

### Purpose
To enforce strict schema validation, nullability constraints, and business-rule assertions on the final target BigQuery tables.

### Setup
Ensure the Dataform execution has completed and populated the target tables.

### Action
Execute the following suite of data quality checks in BigQuery:

```sql
-- Assertion 1: Primary Key Uniqueness
-- (cntrct_id, bpr_id) must be unique in the final target table
ASSERT (
  SELECT MAX(cnt) FROM (
    SELECT cntrct_id, bpr_id, COUNT(*) as cnt
    FROM `isbert_schema.sof_ta_bpr_basis`
    GROUP BY cntrct_id, bpr_id
  )
) = 1
AS "Data Quality Error: Primary key violation on (cntrct_id, bpr_id).";

-- Assertion 2: Mandatory Fields Null Check
-- Critical business keys must never be NULL
ASSERT (
  SELECT COUNT(*) 
  FROM `isbert_schema.sof_ta_bpr_basis`
  WHERE cntrct_id IS NULL OR bpr_id IS NULL OR bpr_instance_id IS NULL
) = 0
AS "Data Quality Error: Found NULL values in mandatory fields (cntrct_id, bpr_id, bpr_instance_id).";

-- Assertion 3: Schema and Data Type Validation
-- Verify that columns match expected types exactly
ASSERT (
  SELECT COUNT(*)
  FROM `isbert_schema.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'sof_ta_bpr_basis'
    AND (
      (column_name = 'cntrct_id' AND data_type != 'INT64') OR
      (column_name = 'bpr_id' AND data_type != 'INT64') OR
      (column_name = 'valid_to' AND data_type != 'DATE') OR
      (column_name = 'card_type_name' AND data_type != 'STRING')
    )
) = 0
AS "Data Quality Error: Schema mismatch or unexpected data types in sof_ta_bpr_basis.";
```

### Pass/Fail Criterion
* **Pass**: All assertions execute successfully, indicating zero data quality or schema violations.
* **Fail**: Any assertion fails, indicating a regression in data integrity or schema structure.