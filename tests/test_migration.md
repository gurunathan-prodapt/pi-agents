# Migration Validation Test Suite: `ausd_bp_ta_bpr_instance`

This document details the migration-validation test suite designed to verify the behavioral equivalence of the migrated BigQuery/Airflow job `ausd_bp_ta_bpr_instance` against its legacy Oracle counterpart.

---

## Test Case 1: Dynamic Date Resolution (`v_datum`)

### Purpose
Verify that the execution date (`v_datum`) is correctly resolved under two scenarios:
1. **Fallback Scenario**: When no explicit `stichtag` parameter is passed via Airflow `dag_run.conf`, the job must dynamically resolve `v_datum` using the maximum `timecreated` timestamp for the prerequisite job `BERT_DROP_TEMP_TABLE` from `isbert_schema.dwtk_meldungen`.
2. **Override Scenario**: When an explicit `stichtag` parameter is provided in the Airflow configuration, it must override the database lookup.

### Setup
1. Create a mock `isbert_schema.dwtk_meldungen` table in the test dataset.
2. Insert the following test records:
   ```sql
   INSERT INTO `isbert_schema.dwtk_meldungen` (job_kennung, timecreated) 
   VALUES 
     ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-02-15 08:30:00 UTC')),
     ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-02-16 14:20:00 UTC')), -- Max date
     ('OTHER_JOB', TIMESTAMP('2026-02-17 09:00:00 UTC'));
   ```
3. Create empty mock tables for `cds.ta_cntrct` and `pds.ta_bpri_com`.
4. Create an empty target table `sof.ta_bpr_instance`.

### Action
Execute the date resolution logic using the BigQuery client in Python. We will test both the fallback and override behaviors.

```python
import pytest
from google.cloud import bigquery

PROJECT_ID = "gcp-project"  # Replace with test project ID
DATASET_PREFIX = "test_dataset"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_date_resolution_fallback(bq_client):
    # Scenario 1: stichtag is empty string (Fallback to dwtk_meldungen)
    stichtag_param = ""
    
    query = f"""
    DECLARE v_datum STRING;
    SET v_datum = COALESCE(
      NULLIF('{stichtag_param}', ''),
      (
        SELECT FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated)))
        FROM `{PROJECT_ID}.isbert_schema.dwtk_meldungen`
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
      ),
      '19000101'
    );
    SELECT v_datum AS resolved_date;
    """
    
    query_job = bq_client.query(query)
    results = list(query_job.result())
    resolved_date = results[0]["resolved_date"]
    
    # Assert that the maximum date for 'BERT_DROP_TEMP_TABLE' is resolved (2026-02-16 -> "20260216")
    assert resolved_date == "20260216"

def test_date_resolution_override(bq_client):
    # Scenario 2: stichtag is explicitly provided
    stichtag_param = "20260520"
    
    query = f"""
    DECLARE v_datum STRING;
    SET v_datum = COALESCE(
      NULLIF('{stichtag_param}', ''),
      (
        SELECT FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated)))
        FROM `{PROJECT_ID}.isbert_schema.dwtk_meldungen`
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
      ),
      '19000101'
    );
    SELECT v_datum AS resolved_date;
    """
    
    query_job = bq_client.query(query)
    results = list(query_job.result())
    resolved_date = results[0]["resolved_date"]
    
    # Assert that the explicit parameter overrides the table lookup
    assert resolved_date == "20260520"
```

### Pass/Fail Criterion
* **Pass**: Fallback resolves exactly to `'20260216'`. Override resolves exactly to `'20260520'`.
* **Fail**: Any other date is returned, or the query fails with a syntax/schema error.

---

## Test Case 2: Contract Filtering and Join Logic

### Purpose
Verify that the complex business rules and temporal filters applied to `cds.ta_cntrct` and `pds.ta_bpri_com` are executed correctly. This ensures only active/reactivatable, production-ready, and temporally valid contracts and product instances are migrated.

### Setup
Populate the source tables with test cases designed to validate boundary conditions around `v_datum = '20260216'`.

#### 1. `cds.ta_cntrct` Test Data
| cntrct_id | cntrct_st | redundant_owner_id | insert_at | modified_at | valid_from | valid_to | is_production | cntrct_ty | cntrct_parent | Description / Expected Outcome |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `C1` | 5 | 1 | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | 3 | NULL | **Valid** (Active, standard type) |
| `C2` | 6 | 1 | 2026-01-01 | 2026-03-01 | 2026-01-01 | 2026-03-01 | 1 | 3 | NULL | **Valid** (Reactivatable, modified/ends in future) |
| `C3` | 4 | 1 | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | 3 | NULL | **Invalid** (Status is 4, not 5 or 6) |
| `C4` | 5 | 2 | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | 3 | NULL | **Invalid** (redundant_owner_id != 1) |
| `C5` | 5 | 1 | 2026-02-17 | NULL | 2026-01-01 | NULL | 1 | 3 | NULL | **Invalid** (insert_at > v_datum) |
| `C6` | 5 | 1 | 2026-01-01 | 2026-02-10 | 2026-01-01 | 2026-02-10 | 1 | 3 | NULL | **Invalid** (modified_at & valid_to <= v_datum) |
| `C7` | 5 | 1 | 2026-01-01 | NULL | 2026-01-01 | NULL | 0 | 3 | NULL | **Invalid** (is_production != 1) |
| `C8` | 5 | 1 | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | 1 | NULL | **Invalid** (cntrct_ty = 1 and parent is NULL) |
| `C9` | 5 | 1 | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | 1 | `C1` | **Valid** (cntrct_ty = 1 but parent is NOT NULL) |

#### 2. `pds.ta_bpri_com` Test Data
| cntrct_id | bpri_com_id | bpr_id | insert_at | modified_at | valid_from | valid_to | is_production | Description / Expected Outcome |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `C1` | 1001 | `B1` | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | **Valid** |
| `C2` | 1002 | `B1` | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | **Valid** |
| `C9` | 1003 | `B2` | 2026-01-01 | NULL | 2026-01-01 | NULL | 1 | **Valid** |
| `C1` | 1004 | `B1` | 2026-02-17 | NULL | 2026-01-01 | NULL | 1 | **Invalid** (insert_at > v_datum) |
| `C1` | 1005 | `B1` | 2026-01-01 | NULL | 2026-01-01 | NULL | 0 | **Invalid** (is_production != 1) |

### Action
1. Truncate target table `sof.ta_bpr_instance`.
2. Run the compiled BigQuery SQL script with `v_datum = '20260216'` and `wiederanlauf_wert = '0'`.
3. Query the target table to verify loaded records.

```sql
-- Execute the migration logic
DECLARE v_datum STRING DEFAULT '20260216';

TRUNCATE TABLE `sof.ta_bpr_instance`;

INSERT INTO `sof.ta_bpr_instance` (
  CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF
)
SELECT
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id AS bpr_instance_id,
  CONCAT(COALESCE(bp.iccid_mi, ''), '-', COALESCE(bp.iccid_ii, ''), '-', COALESCE(bp.iccid_iai, ''), '-', COALESCE(bp.iccid_nr, ''), '-', COALESCE(bp.iccid_cd, '')) AS iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  bp.cntrct_id_ref
FROM `cds.ta_cntrct` AS c
JOIN `pds.ta_bpri_com` AS bp
  ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND c.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (c.modified_at IS NULL OR c.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND c.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (c.valid_to IS NULL OR c.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  AND bp.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND bp.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (bp.valid_to IS NULL OR bp.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND bp.is_production = 1
  AND bp.bpri_com_id > 0;
```

### Pass/Fail Criterion
* **Pass**: The target table contains exactly 3 records corresponding to `bpr_instance_id` values `1001`, `1002`, and `1003`.
* **Fail**: Any invalid record (e.g., `1004`, `1005`) is loaded, or any valid record is missing.

---

## Test Case 3: ICCID Concatenation and NULL Handling

### Purpose
In Oracle, concatenating strings with `NULL` values treats `NULL` as an empty string (e.g., `'A' || NULL || 'B'` yields `'AB'`). In standard BigQuery SQL, `CONCAT('A', NULL, 'B')` returns `NULL`. 

This test proves that the migrated code handles potential `NULL` values in the `iccid` components (`iccid_mi`, `iccid_ii`, `iccid_iai`, `iccid_nr`, `iccid_cd`) safely without causing the entire `iccid` field to resolve to `NULL`.

### Setup
1. Insert a record into `cds.ta_cntrct` that passes all filters.
2. Insert a record into `pds.ta_bpri_com` with some `NULL` values in the `iccid` components:
   ```sql
   INSERT INTO `pds.ta_bpri_com` (
     cntrct_id, bpri_com_id, bpr_id, 
     iccid_mi, iccid_ii, iccid_iai, iccid_nr, iccid_cd, 
     insert_at, valid_from, is_production
   ) VALUES (
     'C_ICCID_TEST', 9999, 'B1', 
     '89', NULL, '123', NULL, '9', -- iccid_ii and iccid_nr are NULL
     DATE('2026-01-01'), DATE('2026-01-01'), 1
   );
   ```

### Action
1. Execute the migration SQL.
2. Query the target table for the generated `iccid` value:
   ```sql
   SELECT iccid 
   FROM `sof.ta_bpr_instance` 
   WHERE bpr_instance_id = 9999;
   ```

### Pass/Fail Criterion
* **Pass**: The returned `iccid` is `'89--123--9'` (or empty strings substituted for `NULL` components, preserving the delimiters without resolving the entire string to `NULL`).
* **Fail**: The returned `iccid` is `NULL`, or the query fails.

*Note: If the test fails, the migration code must be updated to use `CONCAT(COALESCE(bp.iccid_mi, ''), '-', COALESCE(bp.iccid_ii, ''), ...)` to guarantee behavioral equivalence.*

---

## Test Case 4: Restart Logic (`wiederanlauf_wert`)

### Purpose
Verify that the restart parameter `wiederanlauf_wert` (passed via Airflow configuration) is correctly applied to filter out records with `bpri_com_id` less than or equal to the specified value.

### Setup
1. Ensure the source tables contain valid records with `bpri_com_id` values: `1001`, `1002`, and `1003`.
2. Set the Airflow configuration parameter `wiederanlauf_wert` to `1001`.

### Action
Execute the migration SQL with `wiederanlauf_wert = 1001`.

```python
def test_restart_logic(bq_client):
    wiederanlauf_wert = 1001
    v_datum = "20260216"
    
    # Truncate target
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.sof.ta_bpr_instance`").result()
    
    # Run insert with filter
    query = f"""
    INSERT INTO `{PROJECT_ID}.sof.ta_bpr_instance` (
      CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID
    )
    SELECT
      bp.cntrct_id,
      bp.bpr_id,
      bp.bpri_com_id AS bpr_instance_id,
      'TEST'
    FROM `{PROJECT_ID}.cds.ta_cntrct` AS c
    JOIN `{PROJECT_ID}.pds.ta_bpri_com` AS bp
      ON c.cntrct_id = bp.cntrct_id
    WHERE c.cntrct_st IN (5, 6)
      AND c.redundant_owner_id = 1
      AND c.insert_at <= PARSE_DATE('%Y%m%d', '{v_datum}')
      AND (c.modified_at IS NULL OR c.modified_at > PARSE_DATE('%Y%m%d', '{v_datum}'))
      AND c.valid_from <= PARSE_DATE('%Y%m%d', '{v_datum}')
      AND (c.valid_to IS NULL OR c.valid_to > PARSE_DATE('%Y%m%d', '{v_datum}'))
      AND c.is_production = 1
      AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
      AND bp.insert_at <= PARSE_DATE('%Y%m%d', '{v_datum}')
      AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', '{v_datum}'))
      AND bp.valid_from <= PARSE_DATE('%Y%m%d', '{v_datum}')
      AND (bp.valid_to IS NULL OR bp.valid_to > PARSE_DATE('%Y%m%d', '{v_datum}'))
      AND bp.is_production = 1
      AND bp.bpri_com_id > {wiederanlauf_wert};
    """
    bq_client.query(query).result()
    
    # Verify target contents
    res_query = f"SELECT BPR_INSTANCE_ID FROM `{PROJECT_ID}.sof.ta_bpr_instance` ORDER BY BPR_INSTANCE_ID"
    results = [row["BPR_INSTANCE_ID"] for row in bq_client.query(res_query).result()]
    
    # Assert that 1001 is excluded, but 1002 and 1003 are included
    assert results == [1002, 1003]
```

### Pass/Fail Criterion
* **Pass**: Only records with `bpri_com_id` strictly greater than `1001` are loaded.
* **Fail**: Record `1001` is loaded, or no records are loaded.

---

## Test Case 5: Schema and Data Quality Assertions

### Purpose
Verify that the target table `sof.ta_bpr_instance` adheres to the required schema constraints (nullability, data types) and that no duplicate records are generated.

### Setup
Run the full migration job successfully using the test dataset.

### Action & Assertions
Run the following validation queries against the populated target table.

```python
def test_schema_and_data_quality(bq_client):
    # 1. Uniqueness Constraint: (CNTRCT_ID, BPR_INSTANCE_ID) must be unique
    dup_query = f"""
    SELECT CNTRCT_ID, BPR_INSTANCE_ID, COUNT(*) as cnt
    FROM `{PROJECT_ID}.sof.ta_bpr_instance`
    GROUP BY CNTRCT_ID, BPR_INSTANCE_ID
    HAVING cnt > 1
    """
    dups = list(bq_client.query(dup_query).result())
    assert len(dups) == 0, f"Found duplicate keys in target table: {dups}"

    # 2. Nullability Constraint: Key columns must not be NULL
    null_query = f"""
    SELECT COUNT(*) as null_cnt
    FROM `{PROJECT_ID}.sof.ta_bpr_instance`
    WHERE CNTRCT_ID IS NULL OR BPR_ID IS NULL OR BPR_INSTANCE_ID IS NULL
    """
    null_cnt = list(bq_client.query(null_query).result())[0]["null_cnt"]
    assert null_cnt == 0, "Target table contains NULL values in primary key columns."

    # 3. Schema Data Type Verification
    schema_query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{PROJECT_ID}.sof.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ta_bpr_instance'
    """
    schema_rows = {row["column_name"]: (row["data_type"], row["is_nullable"]) for row in bq_client.query(schema_query).result()}
    
    # Assert key columns match expected BigQuery types
    assert schema_rows["CNTRCT_ID"][0] in ("STRING", "INT64")
    assert schema_rows["BPR_ID"][0] in ("STRING", "INT64")
    assert schema_rows["BPR_INSTANCE_ID"][0] == "INT64"
    assert schema_rows["ICCID"][0] == "STRING"
```

### Pass/Fail Criterion
* **Pass**: All assertions pass (no duplicates, no nulls in key columns, schema matches target specifications).
* **Fail**: Any assertion fails.