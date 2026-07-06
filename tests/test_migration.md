# Migration Validation Test Suite: DW.BERT_P_VERTRAG_JP

This document defines the production-ready QA test suite to validate the migration of the **DW.BERT_P_VERTRAG_JP** workflow from its legacy Oracle/UC4 environment to Google Cloud BigQuery and Apache Airflow.

---

## Test Suite Overview

The validation strategy utilizes a **dual-run parallel execution model** and **targeted synthetic assertions** to guarantee behavioral equivalence.

```
                                  ┌───────────────────────────┐
                                  │   Legacy Oracle Source    │
                                  └─────────────┬─────────────┘
                                                │
                        ┌───────────────────────┴───────────────────────┐
                        ▼                                               ▼
          [ 1. Parallel Run Validation ]                 [ 2. Synthetic Edge Cases ]
          Compare production outputs of                  Inject extreme values, NULLs,
          legacy vs. migrated pipelines.                 and boundary dates to verify logic.
                        │                                               │
                        └───────────────────────┬───────────────────────┘
                                                ▼
                                  ┌───────────────────────────┐
                                  │  Pass/Fail Consolidation  │
                                  └───────────────────────────┘
```

---

## Section 1: Output Parity & Parallel Run Validation

### Test Case 1.1: Production Data Mirroring & Row-Count Parity
* **Purpose**: Verify that the migrated BigQuery staging tables contain the exact same record counts as the legacy Oracle tables under identical snapshot dates (`v_datum`).
* **Setup**:
  1. Identify a historical execution date (e.g., `2026-04-21`).
  2. Ensure the legacy Oracle tables in the `@pcrs1` source and local schemas are frozen for that snapshot.
  3. Ensure the BigQuery mirror dataset (`prod_carmen_mirror`) contains the identical replicated snapshot.
  4. Set the watermark table `dwtk_meldungen` in BigQuery to match the legacy run date:
     ```sql
     INSERT INTO `prod-bert-dwh.prod_staging.dwtk_meldungen` (job_kennung, timecreated)
     VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-21 00:00:00 UTC'));
     ```
* **Action**: Execute the Airflow DAG `DW.BERT_P_VERTRAG_JP` in the staging/dry-run environment.
* **Pass/Fail Criterion**: The row counts of the target tables in Oracle and BigQuery must match exactly.

```python
import pytest
from google.cloud import bigquery
import cx_Oracle

@pytest.fixture
def bq_client():
    return bigquery.Client()

@pytest.fixture
def oracle_cursor():
    # Credentials retrieved from secure vault
    connection = cx_Oracle.connect("user/password@oracle_host:1521/service_name")
    cursor = connection.cursor()
    yield cursor
    cursor.close()
    connection.close()

def test_row_count_parity(bq_client, oracle_cursor):
    # Mapping of target tables (Oracle vs BigQuery)
    tables_to_test = {
        "sof$ta_acc_ref": "prod_staging.sof_ta_acc_ref",
        "sof$ta_action_assoc": "prod_staging.sof_ta_action_assoc",
        "sof$ta_apn_ve": "prod_staging.sof_ta_apn_ve",
        "sof$ta_barrier": "prod_staging.sof_ta_barrier",
        "sof$ta_barrier_zusgf": "prod_staging.sof_ta_barrier_zusgf",
        "sof$ta_bp_ref": "prod_staging.sof_ta_bp_ref",
        "sof$ta_cntrct_crs": "prod_staging.sof_ta_cntrct_crs",
        "sof$ta_cntrct_valid": "prod_staging.sof_ta_cntrct_valid",
        "sof$ta_discount": "prod_staging.sof_ta_discount",
        "sof$ta_discount_rr": "prod_staging.sof_ta_discount_rr",
        "sof$ta_disc_zusgf": "prod_staging.sof_ta_disc_zusgf",
        "sof$ta_inv_acc": "prod_staging.sof_ta_inv_acc",
        "sof$ta_inv_assign": "prod_staging.sof_ta_inv_assign",
        "sof$ta_inv_def": "prod_staging.sof_ta_inv_def",
        "sof$ta_period": "prod_staging.sof_ta_period",
        "sof$ta_vvl_dwh": "prod_staging.sof_ta_vvl_dwh",
        "sof$ta_vvl_upgrade": "prod_staging.sof_ta_vvl_upgrade"
    }
    
    for oracle_table, bq_table in tables_to_test.items():
        # Query Oracle Row Count
        oracle_cursor.execute(f"SELECT COUNT(*) FROM {oracle_table}")
        oracle_count = oracle_cursor.fetchone()[0]
        
        # Query BigQuery Row Count
        bq_query = f"SELECT COUNT(*) FROM `prod-bert-dwh.{bq_table}`"
        bq_job = bq_client.query(bq_query)
        bq_count = list(bq_job.result())[0][0]
        
        assert oracle_count == bq_count, f"Row count mismatch for {oracle_table} vs {bq_table}. Oracle: {oracle_count}, BQ: {bq_count}"
```

### Test Case 1.2: Full Schema and Data Fingerprint Parity
* **Purpose**: Ensure that not only row counts, but the actual data values (fingerprints/checksums) are identical across all columns.
* **Setup**: Same as Test Case 1.1.
* **Action**: Execute a MD5/SHA256 hashing query over key columns on both systems and compare the aggregated checksums.
* **Pass/Fail Criterion**: The aggregated hash of the dataset must match exactly between Oracle and BigQuery.

```sql
-- BigQuery Checksum Verification Query for sof_ta_acc_ref
SELECT BIT_XOR(FARM_FINGERPRINT(CONCAT(
  COALESCE(CAST(acc_ref_id AS STRING), 'NULL'), '||',
  COALESCE(account_reference, 'NULL')
))) AS bq_hash
FROM `prod-bert-dwh.prod_staging.sof_ta_acc_ref`;

-- Oracle Checksum Verification Query for sof$ta_acc_ref
SELECT LOWER(RAWTOHEX(DBMS_OBFUSCATION_TOOLKIT.md5(input => 
  UTL_RAW.CAST_TO_RAW(XMLSERIALIZE(CONTENT XMLAGG(XMLELEMENT(x, 
    COALESCE(TO_CHAR(acc_ref_id), 'NULL') || '||' || 
    COALESCE(account_reference, 'NULL')
  ) ORDER BY acc_ref_id) AS CLOB))
))) AS oracle_hash
FROM sof$ta_acc_ref;
```

---

## Section 2: Transformation Correctness & Edge Cases

### Test Case 2.1: Temporal Filtering Logic (Watermark `v_datum` Boundaries)
* **Purpose**: Validate that the temporal filters (`insert_at <= v_datum`, `modified_at > v_datum`, `valid_from <= v_datum`, `valid_to > v_datum`) correctly include and exclude records at the exact boundary limits.
* **Setup**:
  1. Set `v_datum` to `'20260420'`.
  2. Insert synthetic records into `cds_ta_acc_ref` with timestamps exactly on, before, and after the boundary:
     * **Record A (Should Include)**: `insert_at = '2026-04-20 00:00:00'`, `valid_from = '2026-04-20 00:00:00'`, `modified_at = NULL`, `valid_to = NULL`, `is_production = 1`.
     * **Record B (Should Exclude - Inserted After)**: `insert_at = '2026-04-21 00:00:00'`, `valid_from = '2026-04-20 00:00:00'`, `modified_at = NULL`, `valid_to = NULL`, `is_production = 1`.
     * **Record C (Should Exclude - Modified Before)**: `insert_at = '2026-04-19 00:00:00'`, `valid_from = '2026-04-19 00:00:00'`, `modified_at = '2026-04-20 00:00:00'`, `valid_to = NULL`, `is_production = 1`.
* **Action**: Run the `d_ausd_v_ta_acc_ref.sql` script.
* **Pass/Fail Criterion**: Only **Record A** is present in `sof_ta_acc_ref`. Records B and C are excluded.

```sql
-- Assertions for Test Case 2.1
ASSERT (
  SELECT COUNT(*) 
  FROM `prod-bert-dwh.prod_staging.sof_ta_acc_ref` 
  WHERE acc_ref_id IN (A_id, B_id, C_id)
) = 1;

ASSERT EXISTS (
  SELECT 1 
  FROM `prod-bert-dwh.prod_staging.sof_ta_acc_ref` 
  WHERE acc_ref_id = A_id
);
```

### Test Case 2.2: Pipelined Function Replacement (Barrier Aggregation)
* **Purpose**: Verify that the BigQuery analytical `STRING_AGG` replacement for the legacy Oracle pipelined function `concat_barriers` produces identical concatenated strings, handles spaces, and strips out 'Rufnummern' correctly.
* **Setup**:
  1. Insert synthetic records into `sof_ta_barrier` for a single contract (`cntrct_id = 99999`):
     * Row 1: `sperrart = 'Rufnummern Sperre'`, `sperrgrund = 'Kundenwunsch'`, `ist_stillegung = 1`, `sperr_beginn = '2026-01-01'`, `sperr_ende = NULL`, `barrier_reason_cv = 2`.
     * Row 2: `sperrart = 'Inland Abgehend'`, `sperrgrund = 'Betreiberinterne Sperre'`, `ist_stillegung = 0`, `sperr_beginn = '2026-02-01'`, `sperr_ende = '2026-03-01'`, `barrier_reason_cv = 3`.
* **Action**: Run the `d_ausd_v_ta_barrier_zusgf.sql` script.
* **Pass/Fail Criterion**:
  * `sperrart_alle` must equal `'InlandAbgehend,Sperre'` (spaces and 'Rufnummern' removed, sorted alphabetically).
  * `sperrgrund_alle` must equal `'Betreiberinterne Sperre,Kundenwunsch'` (sorted alphabetically).
  * `stilllegungszeitraum_alle` must equal `'ab 01.01.2026'` (Row 2 is ignored for stilllegung because `ist_stillegung = 0`).
  * `sperrgrund_zusgf` must equal `2` (the minimum of the mapped reason codes, or prioritized reason).

```sql
-- Assertions for Test Case 2.2
SELECT
  ASSERT_EQUALS(sperrart_alle, 'InlandAbgehend,Sperre'),
  ASSERT_EQUALS(sperrgrund_alle, 'Betreiberinterne Sperre,Kundenwunsch'),
  ASSERT_EQUALS(stilllegungszeitraum_alle, 'ab 01.01.2026'),
  ASSERT_EQUALS(sperrgrund_zusgf, 2)
FROM `prod-bert-dwh.prod_staging.sof_ta_barrier_zusgf`
WHERE cntrct_id = 99999;
```

### Test Case 2.3: Pipelined Function Replacement (Discount Aggregation)
* **Purpose**: Verify that `d_ausd_v_ta_disc_zusgf.sql` correctly aggregates multiple discounts for a contract version into a single sorted, comma-separated string.
* **Setup**:
  1. Insert synthetic records into `sof_ta_discount` for `cntrct_id = 88888`, `cntrct_obj_version = 1`:
     * Row 1: `rabatt = 'RV-Rabatt'`, `rabatthoehe = '15'`.
     * Row 2: `rabatt = 'Sondernachlass'`, `rabatthoehe = '5'`.
* **Action**: Run the `d_ausd_v_ta_disc_zusgf.sql` script.
* **Pass/Fail Criterion**: `rabatt_alle` must equal `'RV-Rabatt (15%), Sondernachlass (5%)'`.

```sql
-- Assertions for Test Case 2.3
SELECT
  ASSERT_EQUALS(rabatt_alle, 'RV-Rabatt (15%), Sondernachlass (5%)')
FROM `prod-bert-dwh.prod_staging.sof_ta_disc_zusgf`
WHERE cntrct_id = 88888 AND cntrct_obj_version = 1;
```

### Test Case 2.4: NULL Handling in Outer Joins (`d_ausd_v_ta_inv_def`)
* **Purpose**: Verify that the refactored `LEFT OUTER JOIN` logic in `d_ausd_v_ta_inv_def.sql` correctly handles cases where optional configuration tables (`cds_ta_inv_cont_config` and `cds_ta_care_description`) do not have matching records.
* **Setup**:
  1. Insert a record into `cds_ta_inv_definition` with `inv_cont_config_id = NULL`.
* **Action**: Run the `d_ausd_v_ta_inv_def.sql` script.
* **Pass/Fail Criterion**: The record must be successfully loaded into `sof_ta_inv_def` with `rechn_inh_konfig_text` set to `NULL` (no record dropped due to inner join behavior).

```sql
-- Assertions for Test Case 2.4
ASSERT EXISTS (
  SELECT 1 
  FROM `prod-bert-dwh.prod_staging.sof_ta_inv_def` 
  WHERE inv_definition_id = synthetic_id_with_null_config 
    AND rechn_inh_konfig_text IS NULL
);
```

---

## Section 3: External-System Replacements

### Test Case 3.1: Airflow Watermark Extraction (`dwtk_meldungen`)
* **Purpose**: Verify that the Airflow DAG correctly reads the dynamic watermark parameter from the staging metadata table and applies it across all downstream tasks.
* **Setup**:
  1. Clear the staging dataset.
  2. Insert a specific watermark timestamp into `dwtk_meldungen`:
     ```sql
     INSERT INTO `prod-bert-dwh.prod_staging.dwtk_meldungen` (job_kennung, timecreated)
     VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-05-10 14:30:00 UTC'));
     ```
* **Action**: Trigger the Airflow DAG `DW.BERT_P_VERTRAG_JP`.
* **Pass/Fail Criterion**: All tables filtered by `v_datum` must only contain records inserted/modified on or before `'20260510'`.

```python
def test_watermark_propagation(bq_client):
    # Query target table to ensure no records exist past the watermark date
    query = """
    SELECT COUNT(*) 
    FROM `prod-bert-dwh.prod_staging.sof_ta_acc_ref` 
    WHERE acc_ref_id IN (
        SELECT acc_ref_id 
        FROM `prod-bert-dwh.prod_carmen_mirror.cds_ta_acc_ref` 
        WHERE FORMAT_TIMESTAMP('%Y%m%d', insert_at) > '20260510'
    )
    """
    query_job = bq_client.query(query)
    violating_records = list(query_job.result())[0][0]
    assert violating_records == 0, f"Found {violating_records} records violating the watermark boundary."
```

---

## Section 4: Data Quality & Schema Assertions

### Test Case 4.1: Column Type and Nullability Constraints
* **Purpose**: Ensure that the target BigQuery tables conform to the expected schema definitions, and that key identifier columns do not contain unexpected `NULL` values.
* **Setup**: Run the full Airflow DAG to populate all staging tables.
* **Action**: Execute schema validation queries against the BigQuery Information Schema.
* **Pass/Fail Criterion**: All key columns must match the target schema specifications, and primary/foreign keys must be non-nullable.

```sql
-- Assert that primary keys do not contain NULL values
ASSERT NOT EXISTS (
  SELECT 1 FROM `prod-bert-dwh.prod_staging.sof_ta_acc_ref` WHERE acc_ref_id IS NULL
);

ASSERT NOT EXISTS (
  SELECT 1 FROM `prod-bert-dwh.prod_staging.sof_ta_cntrct_crs` WHERE cntrct_id IS NULL
);

-- Assert that data types are correctly mapped (e.g., rabatthoehe is STRING in sof_ta_discount)
ASSERT (
  SELECT data_type 
  FROM `prod-bert-dwh.prod_staging.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'sof_ta_discount' AND column_name = 'rabatthoehe'
) = 'STRING';
```

### Test Case 4.2: Airflow DAG Structural Integrity
* **Purpose**: Ensure that the Airflow DAG structure matches the legacy UC4 dependency graph exactly, with no missing tasks or incorrect execution paths.
* **Setup**: Load the DAG file `bert_p_vertrag_jp_dag.py` into the Airflow Bag.
* **Action**: Programmatically inspect the DAG structure using the Airflow CLI / Python API.
* **Pass/Fail Criterion**: The DAG must load without import errors, and the task dependencies must match the topological order defined in the design document.

```python
from airflow.models import DagBag

def test_dag_imports_and_dependencies():
    dagbag = DagBag(dag_folder='/home/gurunathan_t/migrated_composer/dags', include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import failures: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id='DW.BERT_P_VERTRAG_JP')
    assert dag is not None
    
    # Verify key task dependencies
    barrier_task = dag.get_task('d_ausd_v_ta_barrier')
    barrier_zusgf_task = dag.get_task('d_ausd_v_ta_barrier_zusgf')
    
    assert barrier_zusgf_task in barrier_task.downstream_list
    assert barrier_task in barrier_zusgf_task.upstream_list
```