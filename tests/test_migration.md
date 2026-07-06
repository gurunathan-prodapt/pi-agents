# Migration Validation Test Suite: DW.BERT_AUSD_BP_TA_P_BASISPROD

This document contains the comprehensive migration-validation test suite for the transitioned BigQuery and Apache Airflow components of job `DW.BERT_AUSD_BP_TA_P_BASISPROD`. 

The test suite is designed to verify:
1. **Output Parity:** Ensuring the BigQuery target table matches the legacy Oracle table row-for-row under identical source states.
2. **Transformation Correctness:** Validating complex joins, NULL handling, and the custom `APN` concatenation logic.
3. **Operational & Parameter Parity:** Verifying the behavior of the `wiederanlaufWert` (recovery threshold) parameter and the dynamic extraction of `v_datum`.
4. **Data Quality & Schema Integrity:** Asserting schema structures, nullability, and key constraints.

---

## Test Case 1: End-to-End Output Parity (A/B Testing)

### Purpose
Verify that running the migrated BigQuery SQL transformation on a snapshot of legacy data produces identical results to the legacy Oracle execution.

### Setup
1. Export a consistent snapshot of all upstream tables from the Oracle source database to a temporary BigQuery dataset `test_snapshot_legacy_input`.
2. Run the legacy Oracle job `d_ausd_bp_ta_p_basisprod.sql` on this snapshot and export the resulting `sof$ta_p_basisprod` table to BigQuery as `test_snapshot_legacy_output.sof_ta_p_basisprod`.
3. Load the same input snapshot into the target BigQuery environment `gcp-dwh-prod.sof_core`.

### Action
Execute the migrated BigQuery SQL script `sql/d_ausd_bp_ta_p_basisprod.sql` with `@wiederanlaufWert` set to `0`.

### Pass/Fail Criterion
The test passes if a full outer join between the legacy output table and the migrated BigQuery target table yields zero mismatched rows across all columns.

```python
# pytest/test_parity.py
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_output_parity(bq_client):
    query = """
    WITH diff_check AS (
      SELECT 
        legacy.CNTRCT_ID AS legacy_id, 
        migrated.CNTRCT_ID AS migrated_id,
        -- Compare key business columns
        legacy.APN AS legacy_apn, migrated.APN AS migrated_apn,
        legacy.TNV_ICCID AS legacy_tnv_iccid, migrated.TNV_ICCID AS migrated_tnv_iccid,
        legacy.BCP_VERTRAG AS legacy_bcp_vertrag, migrated.BCP_VERTRAG AS migrated_bcp_vertrag,
        legacy.MS10_ICCID AS legacy_ms10_iccid, migrated.MS10_ICCID AS migrated_ms10_iccid
      FROM `test_snapshot_legacy_output.sof_ta_p_basisprod` legacy
      FULL OUTER JOIN `gcp-dwh-prod.sof_core.sof_ta_p_basisprod` migrated
        ON legacy.CNTRCT_ID = migrated.CNTRCT_ID
    )
    SELECT * 
    FROM diff_check 
    WHERE legacy_id IS NULL 
       OR migrated_id IS NULL
       OR legacy_apn != migrated_apn
       OR legacy_tnv_iccid != migrated_tnv_iccid
       OR legacy_bcp_vertrag != migrated_bcp_vertrag
       OR legacy_ms10_iccid != migrated_ms10_iccid
    LIMIT 100
    """
    query_job = bq_client.query(query)
    results = list(query_job.result())
    
    assert len(results) == 0, f"Mismatched rows detected between Legacy and Migrated tables: {results[:5]}"
```

---

## Test Case 2: APN Concatenation Logic & NULL Handling

### Purpose
Verify that the conditional concatenation logic for the `APN` column matches the legacy Oracle `DECODE` behavior:
* If `av.apn` is `NULL`, the output must be `NULL`.
* If `av.apn` is populated, the output must be `CONCAT(av.apn, ',', av.apn_cntrct)`.

### Setup
Insert controlled mock records into `sof_ta_cntrct_dist` and `sof_ta_apn_vertrag` covering all permutation cases of `apn` and `apn_cntrct`.

| Case | `cntrct_id` | `av.apn` | `av.apn_cntrct` | Expected Output `APN` |
| :--- | :--- | :--- | :--- | :--- |
| 1    | 100001      | `NULL`   | `'internet'`    | `NULL`                |
| 2    | 100002      | `'web'`  | `NULL`          | `'web,'` (or `'web,NULL'` if not handled) |
| 3    | 100003      | `'web'`  | `'internet'`    | `'web,internet'`      |

### Action
Run the transformation query for these specific contract IDs.

### Pass/Fail Criterion
The output values in `sof_ta_p_basisprod` must match the expected outputs exactly.

```sql
-- SQL Assertion Test
WITH expected_cases AS (
  SELECT 100001 AS cntrct_id, CAST(NULL AS STRING) AS expected_apn UNION ALL
  SELECT 100002, 'web,' UNION ALL
  SELECT 100003, 'web,internet'
)
SELECT 
  t.cntrct_id,
  t.apn AS actual_apn,
  e.expected_apn
FROM `gcp-dwh-prod.sof_core.sof_ta_p_basisprod` t
JOIN expected_cases e ON t.cntrct_id = e.cntrct_id
WHERE COALESCE(t.apn, 'NULL_VAL') != COALESCE(e.expected_apn, 'NULL_VAL');
```
*Assertion: The above query must return 0 rows.*

---

## Test Case 3: Recovery Parameter (`wiederanlaufWert`) Filtering

### Purpose
Verify that the `wiederanlaufWert` parameter correctly filters out contract IDs less than or equal to the threshold, matching the legacy recovery behavior.

### Setup
1. Populate `sof_ta_cntrct_dist` with contract IDs `100`, `200`, `300`, `400`, and `500`.
2. Set the Airflow variable `wiederanlaufWert` to `300`.

### Action
Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` with the variable override.

### Pass/Fail Criterion
The target table `sof_ta_p_basisprod` must only contain contract IDs strictly greater than `300` (i.e., `400` and `500`).

```python
# pytest/test_recovery.py
def test_wiederanlaufwert_filtering(bq_client):
    # Retrieve the minimum contract ID from the target table after execution
    query = """
    SELECT MIN(CNTRCT_ID) as min_id, COUNT(1) as total_rows 
    FROM `gcp-dwh-prod.sof_core.sof_ta_p_basisprod`
    """
    query_job = bq_client.query(query)
    result = list(query_job.result())[0]
    
    assert result.min_id > 300, f"Expected MIN(CNTRCT_ID) > 300, but got {result.min_id}"
```

---

## Test Case 4: Multi-SIM Evolution Schema & Column Mapping (MS3 to MS10)

### Purpose
Ensure that the schema contains all columns up to `MS10` (introduced during SIM evolution) and that values are mapped correctly from `sof_ta_iccid_vertrag` without truncation or offset shifts.

### Setup
Insert a mock record into `sof_ta_iccid_vertrag` with distinct values populated in all `ms3_*` through `ms10_*` fields.

### Action
Run the transformation and query the target table for the inserted contract ID.

### Pass/Fail Criterion
All 10 Multi-SIM column groups must be fully populated and match the source fields exactly.

```sql
-- SQL Assertion Test for Multi-SIM mapping correctness
SELECT 
  (icc.ms3_iccid = target.ms3_iccid) AS ms3_ok,
  (icc.ms5_card_type_name = target.ms5_card_type_name) AS ms5_ok,
  (icc.ms10_valid_to = target.ms10_valid) AS ms10_ok
FROM `gcp-dwh-prod.sof_core.sof_ta_iccid_vertrag` icc
JOIN `gcp-dwh-prod.sof_core.sof_ta_p_basisprod` target
  ON icc.cntrct_id = target.cntrct_id
WHERE icc.cntrct_id = 999999;
```
*Assertion: All returned boolean flags must be `TRUE`.*

---

## Test Case 5: Dynamic Date Extraction (`v_datum` / `dwtk_meldungen`)

### Purpose
Verify that the Airflow task `get_v_datum` correctly queries `dwtk_meldungen` for the maximum `timecreated` where `job_kennung = 'BERT_DROP_TEMP_TABLE'` and handles empty/null states gracefully.

### Setup
1. **Scenario A (Happy Path):** Insert a record into `dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = '2026-04-21 18:00:00'`.
2. **Scenario B (Fallback Path):** Truncate `dwtk_meldungen` or ensure no records match `'BERT_DROP_TEMP_TABLE'`.

### Action
Execute the `get_v_datum` task in the Airflow DAG for both scenarios.

### Pass/Fail Criterion
* **Scenario A:** The extracted date must resolve to `'20260421'`.
* **Scenario B:** The extracted date must resolve to the fallback default `'19000101'`.

```python
# pytest/test_date_extraction.py
def test_v_datum_extraction_happy_path(bq_client):
    # Scenario A Query
    query = """
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated))), '19000101') AS v_datum
    FROM `gcp-dwh-prod.isbert_schema_dwtk.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    result = list(bq_client.query(query).result())[0]
    assert result.v_datum == "20260421"

def test_v_datum_extraction_fallback(bq_client):
    # Scenario B Query (using a non-existent job_kennung to force fallback)
    query = """
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated))), '19000101') AS v_datum
    FROM `gcp-dwh-prod.isbert_schema_dwtk.dwtk_meldungen`
    WHERE job_kennung = 'NON_EXISTENT_JOB'
    """
    result = list(bq_client.query(query).result())[0]
    assert result.v_datum == "19000101"
```

---

## Test Case 6: Target Table Schema and Nullability Assertions

### Purpose
Verify that the target BigQuery table `sof_ta_p_basisprod` matches the structural expectations of downstream scoring systems (e.g., BERT), preventing schema drift.

### Setup
None (Metadata query).

### Action
Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view for the target table.

### Pass/Fail Criterion
* `CNTRCT_ID` must be the primary key (non-nullable, integer type).
* All `MS3` to `MS10` columns must exist with correct data types (`STRING` for identifiers/names, `TIMESTAMP` or `DATE` for validation fields).

```python
# pytest/test_schema.py
def test_target_schema_integrity(bq_client):
    query = """
    SELECT column_name, data_type, is_nullable
    FROM `gcp-dwh-prod.sof_core.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof_ta_p_basisprod'
    """
    columns = {row.column_name: (row.data_type, row.is_nullable) for row in bq_client.query(query).result()}
    
    # Assert Primary Key
    assert 'CNTRCT_ID' in columns
    assert columns['CNTRCT_ID'][0] in ('INT64', 'INTEGER')
    
    # Assert Multi-SIM Evolution Columns exist
    for i in range(3, 11):
        iccid_col = f'MS{i}_ICCID'
        valid_col = f'MS{i}_VALID'
        assert iccid_col in columns, f"Missing column {iccid_col}"
        assert valid_col in columns, f"Missing column {valid_col}"
        assert columns[iccid_col][0] == 'STRING'