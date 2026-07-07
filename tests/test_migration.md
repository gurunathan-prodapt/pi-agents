# Migration Validation Test Suite: DW.BERT_AUSD_BP_TA_MSISDN_HIS

This document defines the migration-validation tests to verify that the migrated BigQuery-native pipeline (`sof_ta_msisdn_his` Dataform / Airflow DAG) is behaviorally equivalent to the legacy Oracle-based Automic job.

---

## 1. Output Parity & End-to-End Functional Test

### Purpose
To prove that running the migrated BigQuery pipeline with identical source data yields the exact same output rows in `sof.ta_msisdn_his` as the legacy Oracle job.

### Setup
1. **Legacy Environment (Oracle):**
   * Populate `isbert_schema.dwtk_meldungen` with a watermark record:
     ```sql
     INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) 
     VALUES ('BERT_DROP_TEMP_TABLE', TO_DATE('2026-04-20 14:30:00', 'YYYY-MM-DD HH24:MI:SS'));
     ```
   * Populate `pds$ta_callnumber@pcrs1` with a representative set of test records:
     ```sql
     INSERT INTO pds$ta_callnumber@pcrs1 (bpri_com_id, cc, ndc, sn, callnumber_role_id, valid_to, insert_at, modified_at, valid_from, is_production)
     VALUES (1001, '49', '171', '1234567', 1, TO_DATE('2099-12-31', 'YYYY-MM-DD'), TO_DATE('2026-04-19', 'YYYY-MM-DD'), NULL, TO_DATE('2026-04-19', 'YYYY-MM-DD'), 1);
     ```
   * Run the legacy script `d_ausd_bp_ta_msisdn_his.sql`.
   * Export the resulting table `sof$ta_msisdn_his` to a CSV file (`legacy_output.csv`).

2. **Target Environment (BigQuery):**
   * Populate `isbert_schema.dwtk_meldungen` with the identical watermark record.
   * Populate `pds.ta_callnumber` with the identical source records.
   * Run the Dataform execution or the Airflow DAG task `execute_msisdn_history_update`.

### Action
Execute a comparison query in BigQuery using a full outer join to detect any discrepancies in row count, column values, or unexpected nulls.

```sql
-- BigQuery validation query
WITH legacy_data AS (
  -- Loaded from legacy_output.csv for comparison
  SELECT * FROM `your_test_dataset.legacy_sof_ta_msisdn_his`
),
migrated_data AS (
  SELECT bpri_com_id, msisdn, callnumber_role_id, valid_to 
  FROM `sof.ta_msisdn_his`
)
SELECT 
  COALESCE(l.bpri_com_id, m.bpri_com_id) AS bpri_com_id,
  l.msisdn AS legacy_msisdn,
  m.msisdn AS migrated_msisdn,
  l.callnumber_role_id AS legacy_role,
  m.callnumber_role_id AS migrated_role,
  l.valid_to AS legacy_valid_to,
  m.valid_to AS migrated_valid_to,
  CASE 
    WHEN l.bpri_com_id IS NULL THEN 'Missing in Legacy'
    WHEN m.bpri_com_id IS NULL THEN 'Missing in Migrated'
    WHEN l.msisdn != m.msisdn 
         OR l.callnumber_role_id != m.callnumber_role_id 
         OR DATE(l.valid_to) != DATE(m.valid_to) THEN 'Value Mismatch'
    ELSE 'Match'
  END AS comparison_status
FROM legacy_data l
FULL OUTER JOIN migrated_data m 
  ON l.bpri_com_id = m.bpri_com_id 
  AND l.msisdn = m.msisdn
WHERE l.bpri_com_id IS NULL 
   OR m.bpri_com_id IS NULL 
   OR l.msisdn != m.msisdn 
   OR l.callnumber_role_id != m.callnumber_role_id 
   OR DATE(l.valid_to) != DATE(m.valid_to);
```

### Pass/Fail Criterion
* **Pass:** The validation query returns 0 rows, proving absolute parity between the legacy and migrated outputs.
* **Fail:** Any rows are returned with status `Missing in Legacy`, `Missing in Migrated`, or `Value Mismatch`.

---

## 2. Transformation Correctness: MSISDN Concatenation & Type Handling

### Purpose
To verify that the concatenation of country code (`cc`), national destination code (`ndc`), and subscriber number (`sn`) behaves identically to Oracle, specifically handling potential nulls, numeric-to-string conversions, and leading zeros.

### Setup
Populate `pds.ta_callnumber` with edge cases for MSISDN parts:
* Case A (Standard): `cc = 49`, `ndc = 171`, `sn = 1234567`
* Case B (Leading Zeros): `cc = 49`, `ndc = 0171`, `sn = 0012345` (stored as strings or numbers)
* Case C (Null values in components): `cc = 49`, `ndc = NULL`, `sn = 1234567`

### Action
Execute the transformation logic and query the target table.

```sql
-- Insert test cases
INSERT INTO `pds.ta_callnumber` (bpri_com_id, cc, ndc, sn, callnumber_role_id, valid_to, insert_at, modified_at, valid_from, is_production)
VALUES 
  (2001, '49', '171', '1234567', 1, TIMESTAMP('2099-12-31 00:00:00'), TIMESTAMP('2026-04-19 00:00:00'), NULL, TIMESTAMP('2026-04-19 00:00:00'), 1),
  (2002, '49', '0171', '0012345', 1, TIMESTAMP('2099-12-31 00:00:00'), TIMESTAMP('2026-04-19 00:00:00'), NULL, TIMESTAMP('2026-04-19 00:00:00'), 1),
  (2003, '49', NULL, '1234567', 1, TIMESTAMP('2099-12-31 00:00:00'), TIMESTAMP('2026-04-19 00:00:00'), NULL, TIMESTAMP('2026-04-19 00:00:00'), 1);

-- Run the transformation block...
```

### Pass/Fail Criterion
* **Pass:** 
  * `bpri_com_id = 2001` produces `msisdn = '491711234567'`.
  * `bpri_com_id = 2002` preserves leading zeros: `msisdn = '4901710012345'`.
  * `bpri_com_id = 2003` handles the null gracefully (BigQuery `CONCAT` returns `NULL` if any argument is `NULL`, matching standard SQL behavior).
* **Fail:** Any concatenated string truncates leading zeros or fails to handle nulls correctly.

---

## 3. Watermark & Date Filter Edge Cases

### Purpose
To verify that the date filters (`insert_at`, `modified_at`, `valid_from`) correctly include or exclude records based on the dynamically resolved watermark date (`v_process_date`).

### Setup
1. Set the watermark date in `isbert_schema.dwtk_meldungen` to `2026-04-20`.
2. Populate `pds.ta_callnumber` with the following scenarios relative to the watermark date (`2026-04-20`):

| ID | Scenario | `insert_at` | `modified_at` | `valid_from` | `is_production` | Expected Outcome |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 3001 | Standard Valid | `2026-04-19` | `NULL` | `2026-04-19` | `1` | **Include** |
| 3002 | Future Insert | `2026-04-21` | `NULL` | `2026-04-19` | `1` | **Exclude** (Inserted after watermark) |
| 3003 | Modified in Past | `2026-04-15` | `2026-04-19` | `2026-04-15` | `1` | **Exclude** (Modified before watermark) |
| 3004 | Modified in Future| `2026-04-15` | `2026-04-21` | `2026-04-15` | `1` | **Include** (Still active at watermark) |
| 3005 | Future Valid From | `2026-04-19` | `NULL` | `2026-04-21` | `1` | **Exclude** (Not yet valid at watermark) |
| 3006 | Non-Production | `2026-04-19` | `NULL` | `2026-04-19` | `0` | **Exclude** (`is_production != 1`) |

### Action
Run the BigQuery transformation and execute the following assertion query:

```sql
SELECT bpri_com_id, 
       CASE 
         WHEN bpri_com_id IN (3001, 3004) THEN 'Should Be Included'
         ELSE 'Should Be Excluded'
       END AS expected_status,
       CASE 
         WHEN bpri_com_id IN (SELECT bpri_com_id FROM `sof.ta_msisdn_his`) THEN 'Included'
         ELSE 'Excluded'
       END AS actual_status
FROM UNNEST([3001, 3002, 3003, 3004, 3005, 3006]) AS bpri_com_id;
```

### Pass/Fail Criterion
* **Pass:** All IDs match their expected status (only `3001` and `3004` are present in the target table).
* **Fail:** Any record is incorrectly included or excluded.

---

## 4. Idempotency & Truncate Validation

### Purpose
To prove that the target table `sof.ta_msisdn_his` is successfully truncated before insertion, ensuring that multiple runs of the job do not duplicate data or leave stale records.

### Setup
1. Manually insert 5 dummy records into `sof.ta_msisdn_his` that do not exist in the source table.
2. Ensure the source table `pds.ta_callnumber` has valid records.

### Action
Run the Airflow DAG or Dataform execution block.

### Pass/Fail Criterion
* **Pass:** 
  * The 5 dummy records are completely removed.
  * The final row count of `sof.ta_msisdn_his` matches exactly the count of valid source records for the given watermark.
* **Fail:** Stale dummy records remain in the table, or row counts accumulate across runs.

---

## 5. Airflow DAG Integration & Variable Handling

### Purpose
To verify that the Airflow DAG correctly parses the GCP Project ID variable, resolves the dynamic SQL query, and executes successfully without syntax or permission errors.

### Setup
A Python-based test environment using `pytest` and the Airflow local client.

### Action
Run a unit test on the DAG structure and parameter rendering:

```python
import pytest
from airflow.models import DagBag, Variable

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_msisdn_his")
    assert dag_bag.import_errors == {}
    assert dag is not None

def test_dag_tasks_and_variable_rendering(monkeypatch):
    # Mock Airflow Variable for GCP Project ID
    monkeypatch.setattr(Variable, "get", lambda key: "test-gcp-project")
    
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_msisdn_his")
    task = dag.get_task("execute_msisdn_history_update")
    
    # Verify task configuration contains the mocked project ID
    query_str = task.configuration["query"]["query"]
    assert "test-gcp-project.sof.ta_msisdn_his" in query_str
    assert "test-gcp-project.pds.ta_callnumber" in query_str
```

### Pass/Fail Criterion
* **Pass:** The DAG imports with zero errors, and the template fields correctly render the GCP project variable.
* **Fail:** Import errors are raised, or variables fail to render within the SQL template.