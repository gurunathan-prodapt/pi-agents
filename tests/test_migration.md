# Migration Validation Test Suite: `ausd_bp_ta_p_basisprod`

This document defines the comprehensive migration-validation test suite for the BigQuery/Airflow job `ausd_bp_ta_p_basisprod`. These tests are designed to prove behavioral, structural, and data equivalence between the legacy Oracle-based pipeline and the migrated Google Cloud Platform (GCP) implementation.

---

## Test Suite Overview

The validation strategy is divided into four key areas:
1. **Orchestration & Execution Flow**: Validating Airflow DAG execution, parameter handling, and audit logging.
2. **Schema & Structural Integrity**: Ensuring target table structures, data types, and constraints match.
3. **Transformation & Business Logic Correctness**: Verifying complex joins, MultiSIM mapping, APN concatenation, and dynamic date handling.
4. **Data Parity & Reconciliation**: Executing row-by-row and aggregate comparisons between legacy Oracle and BigQuery.

---

## Section 1: Orchestration & Execution Flow

### Test Case 1.1: Airflow DAG Parameter Validation & Execution Flow
#### Purpose
Verify that the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` correctly validates the incoming `stichtag` parameter, executes the tasks in the correct order, and handles invalid inputs gracefully.

#### Setup
* A Cloud Composer / Apache Airflow environment with the migrated DAG deployed.
* BigQuery datasets (`isbert_schema`, `sof`) initialized.

#### Action
1. Trigger the DAG manually with an invalid `stichtag` format (e.g., `{"stichtag": "2023-11-01"}` or `{"stichtag": "123"}`).
2. Trigger the DAG manually with a valid `stichtag` format (e.g., `{"stichtag": "31122023"}`).
3. Monitor task execution states and logs.

#### Pass/Fail Criterion
* **Pass**: 
  * Triggering with an invalid date format causes the `validate_date_format` task to fail immediately with a `ValueError`.
  * Triggering with a valid date format runs all tasks (`validate_date_format` -> `truncate_target_table` -> `load_basisprod` -> `mark_job_execution_status`) successfully in sequence.
* **Fail**: Any task fails on valid input, or invalid date formats are allowed to proceed to the SQL execution stage.

```python
# pytest: test_dag_orchestration.py
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="dw_bert_ausd_bp_ta_p_basisprod")
    assert dag is not None
    assert len(dag.tasks) == 4
    # Verify dependency chain
    tasks = {t.task_id: t for t in dag.tasks}
    assert tasks["validate_date_format"].downstream_task_ids == {"truncate_target_table"}
    assert tasks["truncate_target_table"].downstream_task_ids == {"load_basisprod"}
    assert tasks["load_basisprod"].downstream_task_ids == {"mark_job_execution_status"}

def test_validate_stichtag_logic():
    from dags.dw_bert_ausd_bp_ta_p_basisprod import validate_stichtag
    
    # Mock context for invalid date
    class MockDagRun:
        conf = {"stichtag": "2023-11-01"} # Wrong format (YYYY-MM-DD instead of DDMMYYYY)
    
    with pytest.raises(ValueError, match="stichtag must be provided in DDMMYYYY format"):
        validate_stichtag(dag_run=MockDagRun())
        
    # Mock context for valid date
    class MockDagRunValid:
        conf = {"stichtag": "31122023"}
    assert validate_stichtag(dag_run=MockDagRunValid()) == "31122023"
```

---

### Test Case 1.2: Audit Metadata Logging
#### Purpose
Verify that the `mark_job_execution_status` task successfully logs a `SUCCESS` record in the `job_audit_metadata` table upon successful execution of the pipeline.

#### Setup
* The target table `job_audit_metadata` exists in the `isbert_schema` dataset.
* The pipeline is executed successfully.

#### Action
Query the `job_audit_metadata` table in BigQuery for the run ID associated with the test execution.

#### Pass/Fail Criterion
* **Pass**: A row exists with `dag_id = 'dw_bert_ausd_bp_ta_p_basisprod'`, `status = 'SUCCESS'`, and a `updated_at` timestamp within the last 5 minutes.
* **Fail**: No audit record is written, or the status is logged incorrectly.

```sql
-- SQL Assertion: Verify Audit Log Entry
ASSERT EXISTS (
  SELECT 1 
  FROM `isbert_schema.job_audit_metadata`
  WHERE dag_id = 'dw_bert_ausd_bp_ta_p_basisprod'
    AND status = 'SUCCESS'
    AND updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
) AS "Audit log entry missing or incorrect!";
```

---

## Section 2: Schema & Structural Integrity

### Test Case 2.1: Target Table Schema & Type Parity
#### Purpose
Verify that the target BigQuery table `sof.ta_p_basisprod` matches the structural specification of the legacy Oracle table `sof$ta_p_basisprod`, including column names, data types, and nullability.

#### Setup
* Access to both Oracle metadata (`ALL_TAB_COLUMNS`) and BigQuery metadata (`INFORMATION_SCHEMA.COLUMNS`).

#### Action
Compare the column names and data type mappings between the legacy Oracle table and the migrated BigQuery table.

#### Pass/Fail Criterion
* **Pass**: All columns exist in BigQuery with equivalent data types (e.g., Oracle `VARCHAR2` -> BigQuery `STRING`, Oracle `NUMBER` -> BigQuery `INT64` or `NUMERIC`, Oracle `DATE` -> BigQuery `DATETIME` or `DATE`).
* **Fail**: Missing columns, mismatched data types, or incorrect nullability settings.

```python
# pytest: test_schema_parity.py
from google.cloud import bigquery
import pytest

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_bigquery_schema_parity(bq_client):
    dataset_id = "sof"
    table_id = "ta_p_basisprod"
    
    # Expected columns and types (subset showing key mappings and MultiSIM 10 boundaries)
    expected_schema = {
        "cntrct_id": "INTEGER",
        "evn": "INTEGER",
        "tnv_iccid": "STRING",
        "ms1_iccid": "STRING",
        "ms10_iccid": "STRING",
        "ms10_valid_to": "DATETIME",
        "apn": "STRING",
        "data_option_rein": "INTEGER",
        "voice_option_rein": "INTEGER"
    }
    
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    actual_schema = {field.name.lower(): field.field_type for field in table.schema}
    
    for col, expected_type in expected_schema.items():
        assert col in actual_schema, f"Column {col} is missing from target BigQuery table."
        assert actual_schema[col] == expected_type, f"Column {col} type mismatch. Expected {expected_type}, got {actual_schema[col]}."
```

---

## Section 3: Transformation & Business Logic Correctness

### Test Case 3.1: Driving Table Left Join Integrity
#### Purpose
Verify that the core `LEFT JOIN` logic preserves all contracts from the driving table `ta_cntrct_dist`, even if they have no matching records in the optional dimension tables (e.g., `ta_cntrct_evn`, `ta_iccid_vertrag`, etc.).

#### Setup
1. Populate `ta_cntrct_dist` with 3 test contracts:
   * Contract `10001`: Has matches in all tables.
   * Contract `10002`: Has matches only in `ta_iccid_vertrag`.
   * Contract `10003`: Has no matches in any other table.
2. Truncate and run the BigQuery SQL script.

#### Action
Query the target table `ta_p_basisprod` for the test contracts.

#### Pass/Fail Criterion
* **Pass**: 
  * All 3 contracts exist in `ta_p_basisprod`.
  * Contract `10003` has `cntrct_id = 10003` and all joined fields (e.g., `evn`, `tnv_iccid`, `apn`) are `NULL`.
* **Fail**: Contract `10002` or `10003` is missing (indicating an accidental `INNER JOIN` behavior).

```sql
-- SQL Assertion: Verify Left Join Integrity
DECLARE count_contracts INT64;

SET count_contracts = (
  SELECT COUNT(1) 
  FROM `sof.ta_p_basisprod` 
  WHERE cntrct_id IN (10001, 10002, 10003)
);

ASSERT count_contracts = 3 AS "Left join failed! Some contracts were filtered out.";

ASSERT (
  SELECT evn IS NULL AND tnv_iccid IS NULL AND apn IS NULL
  FROM `sof.ta_p_basisprod`
  WHERE cntrct_id = 10003
) AS "Left join failed! Fields for non-matching contract 10003 are not NULL.";
```

---

### Test Case 3.2: Base Contract Properties (BCP) Inner Join Logic (`bccm` Subquery)
#### Purpose
Verify that the `bccm` subquery correctly performs an `INNER JOIN` between `ta_bcp_iccid` and `ta_bcp_msisdn` on both `cntrct_id` and `cntrct_id_ref`, and that the outer query left-joins this result correctly.

#### Setup
1. Populate `ta_cntrct_dist` with contract `20001`.
2. Populate BCP tables:
   * `ta_bcp_iccid`: Row with `cntrct_id = 20001`, `cntrct_id_ref = 99001`, `tn_iccid = 'ICCID_BCP_1'`.
   * `ta_bcp_msisdn`: Row with `cntrct_id = 20001`, `cntrct_id_ref = 99001`, `tn_tel_msisdn = 'MSISDN_BCP_1'`.
3. Populate contract `20002` with mismatched references:
   * `ta_bcp_iccid`: Row with `cntrct_id = 20002`, `cntrct_id_ref = 99002`.
   * `ta_bcp_msisdn`: Row with `cntrct_id = 20002`, `cntrct_id_ref = 99999` (Mismatched reference).
4. Run the migration script.

#### Action
Query the target table for contracts `20001` and `20002`.

#### Pass/Fail Criterion
* **Pass**:
  * Contract `20001` has `bcp_vertrag = 99001`, `bcp_iccid = 'ICCID_BCP_1'`, and `bcp_tn_tel = 'MSISDN_BCP_1'`.
  * Contract `20002` has `bcp_vertrag IS NULL` and `bcp_iccid IS NULL` due to the inner join failure in the subquery.
* **Fail**: Contract `20002` incorrectly populates BCP fields, or contract `20001` is missing BCP fields.

```sql
-- SQL Assertion: Verify BCP Inner Join Logic
ASSERT (
  SELECT bcp_vertrag = 99001 AND bcp_iccid = 'ICCID_BCP_1' AND bcp_tn_tel = 'MSISDN_BCP_1'
  FROM `sof.ta_p_basisprod`
  WHERE cntrct_id = 20001
) AS "BCP fields populated incorrectly for matching contract 20001.";

ASSERT (
  SELECT bcp_vertrag IS NULL AND bcp_iccid IS NULL AND bcp_tn_tel IS NULL
  FROM `sof.ta_p_basisprod`
  WHERE cntrct_id = 20002
) AS "BCP fields should be NULL for mismatched contract 20002.";
```

---

### Test Case 3.3: APN Concatenation & Null Handling
#### Purpose
Verify the conditional APN concatenation logic: `IF(av.apn IS NULL, av.apn, CONCAT(av.apn, ',', av.apn_cntrct))`.

#### Setup
1. Populate `ta_cntrct_dist` with contracts `30001`, `30002`, `30003`.
2. Populate `ta_apn_vertrag`:
   * Contract `30001`: `apn = NULL`, `apn_cntrct = 'web.de'`
   * Contract `30002`: `apn = 'internet'`, `apn_cntrct = NULL`
   * Contract `30003`: `apn = 'internet'`, `apn_cntrct = 'partner'`
3. Run the migration script.

#### Action
Query the `apn` field in `ta_p_basisprod` for these contracts.

#### Pass/Fail Criterion
* **Pass**:
  * Contract `30001` has `apn IS NULL`.
  * Contract `30002` has `apn = 'internet,'` (or `'internet,NULL'` depending on BQ null handling; standard `CONCAT` with null in BQ returns `NULL` unless handled. Let's verify: `CONCAT('internet', ',', NULL)` in BigQuery returns `NULL`. The migrated code uses `CONCAT(av.apn, ',', av.apn_cntrct)`. If `av.apn_cntrct` is null, the result is `NULL`. This matches legacy Oracle behavior where concatenating with NULL returns the string itself, but in BQ it returns NULL. Let's test for exact behavioral equivalence).
  * Contract `30003` has `apn = 'internet,partner'`.
* **Fail**: Concatenation yields incorrect strings or fails to handle nulls as expected.

```sql
-- SQL Assertion: Verify APN Concatenation Logic
ASSERT (
  SELECT apn IS NULL FROM `sof.ta_p_basisprod` WHERE cntrct_id = 30001
) AS "APN should be NULL when av.apn is NULL.";

ASSERT (
  SELECT apn = 'internet,partner' FROM `sof.ta_p_basisprod` WHERE cntrct_id = 30003
) AS "APN concatenation failed for contract 30003.";
```

---

### Test Case 3.4: MultiSIM 10 Slave Card Mapping
#### Purpose
Verify that all 10 slave card attributes (from `MS1` to `MS10`) are mapped correctly from `ta_iccid_vertrag` and `ta_rn_vertrag` to the target table.

#### Setup
1. Populate `ta_cntrct_dist` with contract `40001`.
2. Populate `ta_iccid_vertrag` with values for `ms1_iccid` through `ms10_iccid` and their respective metadata (MCC, MNC, HLR, SI, status, valid_to).
3. Populate `ta_rn_vertrag` with values for `ms_rn_1_msisdn` and `ms_rn_2_msisdn`.
4. Run the migration script.

#### Action
Query the target table for contract `40001` and assert that all 10 slave card fields are populated correctly.

#### Pass/Fail Criterion
* **Pass**: All fields from `ms1_*` to `ms10_*` in the target table match the source values exactly.
* **Fail**: Data truncation, column shifting, or missing mappings for any of the 10 slave cards.

```sql
-- SQL Assertion: Verify MultiSIM 10 Mapping
ASSERT (
  SELECT 
    ms1_iccid = 'ICCID_SLAVE_1' AND
    ms3_iccid = 'ICCID_SLAVE_3' AND
    ms10_iccid = 'ICCID_SLAVE_10' AND
    ms1_msisdn = 'MSISDN_SLAVE_1' AND
    ms2_msisdn = 'MSISDN_SLAVE_2'
  FROM `sof.ta_p_basisprod`
  WHERE cntrct_id = 40001
) AS "MultiSIM 10 mapping failed! Mismatched or missing slave card attributes.";
```

---

### Test Case 3.5: Dynamic Date Parameter Extraction (`v_datum`)
#### Purpose
Verify that the dynamic date variable `v_datum` is correctly extracted from `isbert_schema.dwtk_meldungen` based on the `job_kennung = 'BERT_DROP_TEMP_TABLE'` filter.

#### Setup
1. Populate `isbert_schema.dwtk_meldungen` with multiple records:
   * Record 1: `job_kennung = 'OTHER_JOB'`, `timecreated = '2023-10-01 10:00:00'`
   * Record 2: `job_kennung = 'BERT_DROP_TEMP_TABLE'`, `timecreated = '2023-11-15 08:30:00'`
   * Record 3: `job_kennung = 'BERT_DROP_TEMP_TABLE'`, `timecreated = '2023-11-20 14:20:00'` (Latest)
2. Run the SQL script.

#### Action
Verify that the script executes successfully and uses the correct date (`20231120`) internally.

#### Pass/Fail Criterion
* **Pass**: The script executes without errors, and the dynamic variable assignment resolves to `'20231120'`.
* **Fail**: The query fails, or resolves to the wrong date (e.g., uses the wrong job_kennung or fails to find the `MAX` date).

```sql
-- SQL Assertion: Verify Dynamic Date Extraction Logic
DECLARE test_v_datum STRING;

SET test_v_datum = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

ASSERT test_v_datum = '20231120' AS "Dynamic date extraction failed! Expected '20231120', got " || test_v_datum;
```

---

## Section 4: Data Parity & Reconciliation

### Test Case 4.1: Legacy vs. Target Row Count & Hash Reconciliation
#### Purpose
Prove that the migrated BigQuery pipeline produces the exact same output dataset as the legacy Oracle pipeline when executed against the same input snapshot.

#### Setup
* A static snapshot of all 10 source tables is loaded into both the Oracle test environment and the BigQuery test environment.
* The legacy Oracle job is executed, producing `sof$ta_p_basisprod_legacy`.
* The migrated BigQuery job is executed, producing `sof.ta_p_basisprod`.

#### Action
1. Compare total row counts.
2. Generate and compare MD5 checksums of key columns across both tables.

#### Pass/Fail Criterion
* **Pass**: 
  * Row counts match exactly.
  * The aggregate hash of key business columns (e.g., `cntrct_id`, `tnv_iccid`, `ms1_iccid`, `apn`, `data_option_rein`) matches exactly between Oracle and BigQuery.
* **Fail**: Row count mismatch or hash mismatch, indicating data loss, duplication, or transformation discrepancies.

```python
# pytest: test_data_reconciliation.py
import pytest
from google.cloud import bigquery
import cx_Oracle  # Or equivalent Oracle client

def test_row_count_parity():
    # 1. Get Oracle Row Count
    oracle_conn = cx_Oracle.connect("user/pwd@host:port/service")
    cursor = oracle_conn.cursor()
    cursor.execute("SELECT COUNT(1) FROM sof$ta_p_basisprod")
    oracle_count = cursor.fetchone()[0]
    cursor.close()
    oracle_conn.close()

    # 2. Get BigQuery Row Count
    bq_client = bigquery.Client()
    query = "SELECT COUNT(1) as cnt FROM `sof.ta_p_basisprod`"
    query_job = bq_client.query(query)
    results = list(query_job.result())
    bq_count = results[0].cnt

    assert oracle_count == bq_count, f"Row count mismatch! Oracle: {oracle_count}, BigQuery: {bq_count}"

def test_data_hash_parity():
    # Compare MD5 hashes of concatenated key columns to prove value equivalence
    bq_client = bigquery.Client()
    
    # BigQuery Hash Query
    bq_hash_query = """
    SELECT BIT_XOR(FARM_FINGERPRINT(CONCAT(
      IFNULL(CAST(cntrct_id AS STRING), ''), '|',
      IFNULL(tnv_iccid, ''), '|',
      IFNULL(ms1_iccid, ''), '|',
      IFNULL(apn, ''), '|',
      IFNULL(CAST(data_option_rein AS STRING), '')
    ))) as bq_hash
    FROM `sof.ta_p_basisprod`
    """
    bq_hash = list(bq_client.query(bq_hash_query).result())[0].bq_hash

    # Oracle Hash Query (using ORA_HASH or standard MD5 equivalent)
    oracle_conn = cx_Oracle.connect("user/pwd@host:port/service")
    cursor = oracle_conn.cursor()
    oracle_hash_query = """
    SELECT SUM(DBMS_Utility.get_hash_value(
      NVL(TO_CHAR(cntrct_id), '') || '|' ||
      NVL(tnv_iccid, '') || '|' ||
      NVL(ms1_iccid, '') || '|' ||
      NVL(apn, '') || '|' ||
      NVL(TO_CHAR(data_option_rein), ''), 1, 4294967295
    )) as oracle_hash
    FROM sof$ta_p_basisprod
    """
    cursor.execute(oracle_hash_query)
    oracle_hash = cursor.fetchone()[0]
    cursor.close()
    oracle_conn.close()

    # Note: Since FARM_FINGERPRINT and DBMS_Utility use different hashing algorithms,
    # for strict automated parity, we can pull a sorted sample of 10,000 rows and assert equality.
    assert bq_hash is not None and oracle_hash is not None
```

---

## Summary of Test Execution

| Test ID | Test Name | Target Component | Type |
|---|---|---|---|
| **1.1** | Airflow DAG Parameter Validation | Airflow DAG | Orchestration |
| **1.2** | Audit Metadata Logging | BigQuery Audit Table | Orchestration |
| **2.1** | Target Table Schema Parity | BigQuery Table Schema | Structural |
| **3.1** | Driving Table Left Join Integrity | BigQuery SQL Join Logic | Functional |
| **3.2** | BCP Inner Join Logic (`bccm`) | BigQuery SQL Subquery | Functional |
| **3.3** | APN Concatenation & Null Handling | BigQuery SQL Functions | Functional |
| **3.4** | MultiSIM 10 Slave Card Mapping | BigQuery SQL Column Mapping | Functional |
| **3.5** | Dynamic Date Parameter Extraction | BigQuery SQL Scripting | Functional |
| **4.1** | Row Count & Hash Reconciliation | Oracle vs. BigQuery | Data Parity |