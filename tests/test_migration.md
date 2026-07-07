# Migration Validation Test Suite: `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`

This document outlines the comprehensive migration-validation test suite designed to verify that the modernized Cloud Composer and Dataform-based BigQuery pipeline is behaviorally equivalent to the legacy Oracle SQL*Plus and UC4-orchestrated job.

---

## Section 1: Output Parity & Row-Count Validation

### Test Case 1.1: End-to-End Output Parity (Reconciliation)
*   **Purpose**: Prove that running the modernized BigQuery Dataform model on a snapshot of legacy input data produces identical outputs to the legacy Oracle run.
*   **Setup**:
    1.  Extract a snapshot of the source table `sof$ta_cntrct_crs` from the legacy Oracle database.
    2.  Load this snapshot into the BigQuery source table `isbert_schema.sof_ta_cntrct_crs`.
    3.  Run the legacy Oracle script `d_ausd_v_ta_cntrct_crs2.sql` on the same snapshot in a test schema to populate `sof$ta_cntrct_crs2`. Export this result to a temporary BigQuery table `isbert_schema.legacy_sof_ta_cntrct_crs2_expected`.
    4.  Execute the Dataform model `sof_ta_cntrct_crs2` to populate the target table `isbert_schema.sof_ta_cntrct_crs2`.
*   **Action**: Execute a full outer join comparison query between the legacy expected table and the migrated target table.
*   **Pass/Fail Criterion**: The query below must return exactly `0` mismatched rows.

```sql
-- SQL Assertion: Full Outer Join Parity Check
WITH legacy AS (
  SELECT * FROM `gcp-project-id.isbert_schema.legacy_sof_ta_cntrct_crs2_expected`
),
migrated AS (
  SELECT * FROM `gcp-project-id.isbert_schema.sof_ta_cntrct_crs2`
),
mismatches AS (
  SELECT
    COALESCE(l.cntrct_id, m.cntrct_id) AS cntrct_id,
    -- Compare all fields using a hash or field-by-field comparison
    TO_JSON_STRING(l) AS legacy_row,
    TO_JSON_STRING(m) AS migrated_row
  FROM legacy l
  FULL OUTER JOIN migrated m 
    ON l.cntrct_id = m.cntrct_id
  WHERE 
    l.cntrct_id IS NULL 
    OR m.cntrct_id IS NULL
    OR l.obj_version != m.obj_version
    OR l.contract_number != m.contract_number
    OR l.cntrct_template_id != m.cntrct_template_id
    OR l.cntrct_validity_id != m.cntrct_validity_id
    OR l.valid_from != m.valid_from
    OR l.com_per_ext_rea_cv != m.com_per_ext_rea_cv
    OR l.billcycle_id != m.billcycle_id
    OR l.vo_code != m.vo_code
    OR l.cntrct_start_date != m.cntrct_start_date
    OR l.cntrct_st != m.cntrct_st
    OR COALESCE(l.cntrct_parent, -1) != COALESCE(m.cntrct_parent, -1)
    OR l.cntrct_ty != m.cntrct_ty
    OR COALESCE(l.cost_centre, 'NULL') != COALESCE(m.cost_centre, 'NULL')
    OR COALESCE(l.cost_centre_user, 'NULL') != COALESCE(m.cost_centre_user, 'NULL')
    OR l.commitment_reference_date != m.commitment_reference_date
    OR COALESCE(l.order_number, 'NULL') != COALESCE(m.order_number, 'NULL')
    OR COALESCE(l.rv_num, 'NULL') != COALESCE(m.rv_num, 'NULL')
)
SELECT COUNT(*) AS mismatch_count FROM mismatches;
```

---

## Section 2: Transformation Correctness & Edge Cases

### Test Case 2.1: Frame Contract Parent Exclusion (Filter Correctness)
*   **Purpose**: Verify that contracts where `cntrct_ty = 10` (Frame Contracts / RV) are strictly excluded from the target table, while child contracts are preserved.
*   **Setup**: Insert mock records into `isbert_schema.sof_ta_cntrct_crs`:
    *   Record A: `cntrct_id = 101`, `cntrct_ty = 10` (Parent Frame Contract)
    *   Record B: `cntrct_id = 102`, `cntrct_ty = 20`, `cntrct_parent = 101` (Child Contract)
    *   Record C: `cntrct_id = 103`, `cntrct_ty = 30`, `cntrct_parent = NULL` (Standard Contract)
*   **Action**: Execute the Dataform compilation and run.
*   **Pass/Fail Criterion**: 
    *   Record A (`cntrct_id = 101`) must **not** exist in `sof_ta_cntrct_crs2`.
    *   Record B and C must exist in `sof_ta_cntrct_crs2`.

```sql
-- Assertion Query
SELECT 
  SUM(CASE WHEN cntrct_id = 101 THEN 1 ELSE 0 END) AS parent_count,
  SUM(CASE WHEN cntrct_id IN (102, 103) THEN 1 ELSE 0 END) AS child_count
FROM `gcp-project-id.isbert_schema.sof_ta_cntrct_crs2`
HAVING parent_count = 0 AND child_count = 2;
```

### Test Case 2.2: Parent-Child Relationship Resolution (`rv_num` Mapping)
*   **Purpose**: Verify that `rv_num` is correctly populated with the parent's `contract_number` only when the parent's `cntrct_ty` is `10`.
*   **Setup**: Insert mock records into `isbert_schema.sof_ta_cntrct_crs`:
    *   Record 1: `cntrct_id = 201`, `cntrct_ty = 10`, `contract_number = 'RV_PARENT_99'` (Valid Frame Parent)
    *   Record 2: `cntrct_id = 202`, `cntrct_ty = 15`, `contract_number = 'NON_RV_PARENT'` (Invalid Parent Type)
    *   Record 3: `cntrct_id = 301`, `cntrct_ty = 1`, `cntrct_parent = 201` (Child of Valid Parent)
    *   Record 4: `cntrct_id = 302`, `cntrct_ty = 1`, `cntrct_parent = 202` (Child of Invalid Parent)
    *   Record 5: `cntrct_id = 303`, `cntrct_ty = 1`, `cntrct_parent = 999` (Orphan Child - Parent missing)
*   **Action**: Execute the Dataform compilation and run.
*   **Pass/Fail Criterion**:
    *   `cntrct_id = 301` must have `rv_num = 'RV_PARENT_99'`.
    *   `cntrct_id = 302` must have `rv_num IS NULL` (since parent type is not 10).
    *   `cntrct_id = 303` must have `rv_num IS NULL` (due to the `LEFT OUTER JOIN` behavior).

```sql
-- Assertion Query
SELECT cntrct_id, rv_num 
FROM `gcp-project-id.isbert_schema.sof_ta_cntrct_crs2`
WHERE cntrct_id IN (301, 302, 303)
ORDER BY cntrct_id;

-- Expected Output:
-- 301 | "RV_PARENT_99"
-- 302 | NULL
-- 303 | NULL
```

---

## Section 3: External-System Replacements & Orchestration

### Test Case 3.1: Airflow DAG Execution & Dataform Compilation
*   **Purpose**: Verify that the Cloud Composer DAG compiles and executes the specific Dataform target (`sof_ta_cntrct_crs2`) successfully without syntax or permission errors.
*   **Setup**: Deploy `dags/dag_dw_bert_ausd_v_ta_cntrct_crs2.py` to the Airflow DAGs folder in the Cloud Composer environment.
*   **Action**: Trigger the DAG manually via the Airflow UI or CLI.
*   **Pass/Fail Criterion**:
    *   The task `compile_dataform` completes with status `SUCCESS`.
    *   The task `execute_ta_cntrct_crs2_transformation` completes with status `SUCCESS`.
    *   The Dataform execution log confirms that only the target `isbert_schema.sof_ta_cntrct_crs2` was rebuilt.

```python
# Pytest integration test for Airflow DAG validation
import pytest
from airflow.models import DagBag

def test_dag_loaded_and_valid():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_v_ta_cntrct_crs2")
    
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 4  # start, compile_dataform, execute_ta_cntrct_crs2_transformation, end
```

### Test Case 3.2: Truncate-and-Reload Behavior (Idempotency)
*   **Purpose**: Prove that the Dataform model behaves as an overwrite table (equivalent to the legacy `TRUNCATE TABLE` + `INSERT INTO` pattern) and does not duplicate records on consecutive runs.
*   **Setup**: Ensure `isbert_schema.sof_ta_cntrct_crs` has 100 records.
*   **Action**: Run the Dataform pipeline twice consecutively.
*   **Pass/Fail Criterion**: The row count of `isbert_schema.sof_ta_cntrct_crs2` must be exactly equal to the expected filtered count after both the first and second runs (no accumulation of duplicate records).

```sql
-- Assertion Query
SELECT COUNT(*) AS total_rows 
FROM `gcp-project-id.isbert_schema.sof_ta_cntrct_crs2`;
-- Must return the exact filtered count (e.g., 95 if 5 parent contracts were excluded), not 190.
```

---

## Section 4: Data Quality, Schema, and Null Handling Assertions

### Test Case 4.1: Schema and Nullability Assertions
*   **Purpose**: Ensure that the target table schema matches the expected BigQuery types and that critical fields are populated correctly.
*   **Setup**: Run the Dataform pipeline to populate `isbert_schema.sof_ta_cntrct_crs2`.
*   **Action**: Query the BigQuery `INFORMATION_SCHEMA` and check for unexpected NULL values in primary keys.
*   **Pass/Fail Criterion**:
    *   `cntrct_id` must be of type `INT64` (or `NUMERIC` depending on source mapping) and contain **zero** NULL values.
    *   `contract_number` must be of type `STRING` and contain **zero** NULL values.

```sql
-- Assertion 1: Null Check on Primary Keys
SELECT 
  COUNTIF(cntrct_id IS NULL) AS null_ids,
  COUNTIF(contract_number IS NULL) AS null_contract_numbers
FROM `gcp-project-id.isbert_schema.sof_ta_cntrct_crs2`
HAVING null_ids = 0 AND null_contract_numbers = 0;

-- Assertion 2: Column Type Verification
SELECT column_name, data_type 
FROM `gcp-project-id.isbert_schema.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_cntrct_crs2'
  AND column_name IN ('cntrct_id', 'contract_number', 'rv_num');
-- Expected:
-- cntrct_id       | INT64 (or NUMERIC)
-- contract_number | STRING
-- rv_num          | STRING
```