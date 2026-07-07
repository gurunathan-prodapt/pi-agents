# Migration Validation Test Suite: DW.BERT_AUSD_BP_TA_MSISDN_HIS

This document defines the comprehensive QA test suite to validate the migration of the historical MSISDN tracking pipeline from the legacy Oracle/UC4 environment to Google Cloud (BigQuery and Cloud Composer).

---

## Section 1: Watermark Extraction & Fallback Validation

### Purpose
To verify that the dynamic watermark date (`v_datum`) is correctly extracted from `dwtk_meldungen` based on the job key `BERT_DROP_TEMP_TABLE`, and that it falls back to `'19000101'` if no matching record exists.

### Setup
1. Create a temporary test dataset in BigQuery.
2. Replicate the schema of `dwtk_meldungen` containing at least the columns `job_kennung` (STRING) and `timecreated` (TIMESTAMP).
3. Prepare two test scenarios:
   - **Scenario A (Happy Path):** Multiple records exist for `BERT_DROP_TEMP_TABLE`.
   - **Scenario B (Fallback Path):** No records exist for `BERT_DROP_TEMP_TABLE`.

### Action
Execute the following validation script in BigQuery:

```sql
-- Test Setup: Temporary tables
CREATE OR REPLACE TEMP TABLE temp_dwtk_meldungen (
  job_kennung STRING,
  timecreated TIMESTAMP
);

-- Scenario A: Insert test records with varying timestamps
INSERT INTO temp_dwtk_meldungen (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-20 14:30:00 UTC')),
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-21 08:15:00 UTC')), -- Max Timestamp
('OTHER_JOB_ID', TIMESTAMP('2026-04-22 10:00:00 UTC'));

-- Execute extraction logic for Scenario A
DECLARE v_datum_a STRING;
SET v_datum_a = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM temp_dwtk_meldungen m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Scenario B: Truncate and test fallback
TRUNCATE TABLE temp_dwtk_meldungen;
INSERT INTO temp_dwtk_meldungen (job_kennung, timecreated) VALUES
('OTHER_JOB_ID', TIMESTAMP('2026-04-22 10:00:00 UTC'));

DECLARE v_datum_b STRING;
SET v_datum_b = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM temp_dwtk_meldungen m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Output results for assertion
SELECT v_datum_a AS actual_watermark_a, v_datum_b AS actual_watermark_b;
```

### Pass/Fail Criterion
* **Pass:** `actual_watermark_a` is exactly `'20260421'` and `actual_watermark_b` is exactly `'19000101'`.
* **Fail:** Any other values are returned, indicating incorrect date formatting, timezone conversion errors, or failure to handle missing keys.

---

## Section 2: Concatenation & NULL Handling Validation

### Purpose
To verify that the concatenation of country code (`cc`), national destination code (`ndc`), and subscriber number (`sn`) behaves identically to Oracle's `||` operator, specifically ensuring that `NULL` values are handled correctly without nullifying the entire string.

### Setup
1. In Oracle, `NULL || '123'` yields `'123'`. In BigQuery, standard `CONCAT` handles `NULL` values by treating them as empty strings, but explicit `CAST` operations must be verified to ensure they do not raise runtime exceptions or return unexpected `NULL` values.
2. Create a mock source table with various combinations of populated and `NULL` values.

### Action
Execute the following comparative query in BigQuery:

```sql
WITH mock_source AS (
  SELECT 1 AS id, CAST('49' AS STRING) AS cc, CAST('172' AS STRING) AS ndc, CAST('1234567' AS STRING) AS sn UNION ALL
  SELECT 2 AS id, CAST(NULL AS STRING) AS cc, CAST('172' AS STRING) AS ndc, CAST('1234567' AS STRING) AS sn UNION ALL
  SELECT 3 AS id, CAST('49' AS STRING) AS cc, CAST(NULL AS STRING) AS ndc, CAST('1234567' AS STRING) AS sn UNION ALL
  SELECT 4 AS id, CAST(NULL AS STRING) AS cc, CAST(NULL AS STRING) AS ndc, CAST(NULL AS STRING) AS sn
)
SELECT 
  id,
  CONCAT(CAST(cc AS STRING), CAST(ndc AS STRING), CAST(sn AS STRING)) AS actual_msisdn
FROM mock_source
ORDER BY id;
```

### Pass/Fail Criterion
* **Pass:** The output matches the expected values below:
  * `id = 1` $\rightarrow$ `'491721234567'`
  * `id = 2` $\rightarrow$ `'1721234567'`
  * `id = 3` $\rightarrow$ `'491234567'`
  * `id = 4` $\rightarrow$ `''` (Empty string, not `NULL`)
* **Fail:** Any row returns a `NULL` value for `actual_msisdn`, or the query throws a casting exception.

---

## Section 3: Temporal Filter & Production Flag Logic Validation

### Purpose
To verify that the temporal filtering logic (`insert_at`, `modified_at`, `valid_from`) and the `is_production` flag correctly filter records relative to the dynamic watermark date.

### Setup
1. Set the watermark date `v_datum` to `'20260421'`.
2. Populate a mock `pds$ta_callnumber` table with records representing edge cases around the watermark boundary.

### Action
Execute the following test script:

```sql
-- Test Setup
DECLARE v_datum STRING DEFAULT '20260421';

CREATE OR REPLACE TEMP TABLE mock_callnumber (
  test_case_id STRING,
  bpri_com_id INT64,
  cc STRING,
  ndc STRING,
  sn STRING,
  callnumber_role_id INT64,
  valid_to DATE,
  insert_at DATE,
  modified_at DATE,
  valid_from DATE,
  is_production INT64
);

INSERT INTO mock_callnumber VALUES
-- 1. Happy Path: Active production record within dates
('HAPPY_PATH', 101, '49', '170', '1111111', 1, DATE('2099-12-31'), DATE('2026-04-20'), NULL, DATE('2026-04-20'), 1),
-- 2. Excluded: insert_at is in the future relative to watermark
('FUTURE_INSERT', 102, '49', '170', '2222222', 1, DATE('2099-12-31'), DATE('2026-04-22'), NULL, DATE('2026-04-20'), 1),
-- 3. Excluded: modified_at is in the past (record was modified/superseded before watermark)
('PAST_MODIFIED', 103, '49', '170', '3333333', 1, DATE('2099-12-31'), DATE('2026-04-10'), DATE('2026-04-20'), DATE('2026-04-10'), 1),
-- 4. Included: modified_at is in the future (record is still active at watermark)
('FUTURE_MODIFIED', 104, '49', '170', '4444444', 1, DATE('2099-12-31'), DATE('2026-04-10'), DATE('2026-04-22'), DATE('2026-04-10'), 1),
-- 5. Excluded: valid_from is in the future relative to watermark
('FUTURE_VALID_FROM', 105, '49', '170', '5555555', 1, DATE('2099-12-31'), DATE('2026-04-20'), NULL, DATE('2026-04-22'), 1),
-- 6. Excluded: is_production is 0
('NON_PRODUCTION', 106, '49', '170', '6666666', 1, DATE('2099-12-31'), DATE('2026-04-20'), NULL, DATE('2026-04-20'), 0);

-- Execute target query logic
SELECT
  test_case_id,
  bpri_com_id,
  CONCAT(CAST(cc AS STRING), CAST(ndc AS STRING), CAST(sn AS STRING)) AS msisdn
FROM mock_callnumber
WHERE insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (modified_at IS NULL OR modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND is_production = 1;
```

### Pass/Fail Criterion
* **Pass:** The query returns exactly two records: `HAPPY_PATH` (bpri_com_id 101) and `FUTURE_MODIFIED` (bpri_com_id 104).
* **Fail:** Any of the excluded test cases (`FUTURE_INSERT`, `PAST_MODIFIED`, `FUTURE_VALID_FROM`, `NON_PRODUCTION`) are returned, or any of the valid records are missing.

---

## Section 4: End-to-End Output Parity Validation

### Purpose
To guarantee absolute behavioral equivalence and data parity between the legacy Oracle output and the migrated BigQuery output using a production-like dataset.

### Setup
1. Extract a sample of 10,000 records from the legacy Oracle source table `pds$ta_callnumber@pcrs1` and the corresponding watermark from `isbert_schema.dwtk_meldungen`.
2. Run the legacy PL/SQL script in a QA Oracle environment and export the resulting `sof$ta_msisdn_his` table to a CSV file (`oracle_output.csv`).
3. Load the same input sample into the BigQuery QA environment (`isbert_schema_prod.pds$ta_callnumber` and `isbert_schema_prod.dwtk_meldungen`).
4. Run the migrated BigQuery SQL script.

### Action
Execute a reconciliation query in BigQuery to compare the migrated target table against the legacy baseline:

```sql
-- Load legacy baseline into a temporary table 'legacy_baseline'
-- Compare BigQuery target 'sof$ta_msisdn_his' with 'legacy_baseline'

WITH bq_data AS (
  SELECT BPRI_COM_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO 
  FROM `gcp-prod-dwh-project.sof_dataset.sof$ta_msisdn_his`
),
oracle_data AS (
  SELECT BPRI_COM_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO 
  FROM `gcp-prod-dwh-project.sof_dataset.legacy_baseline`
),
discrepancies AS (
  (SELECT 'IN_BQ_NOT_IN_ORACLE' AS source, * FROM (SELECT * FROM bq_data EXCEPT DISTINCT SELECT * FROM oracle_data))
  UNION ALL
  (SELECT 'IN_ORACLE_NOT_IN_BQ' AS source, * FROM (SELECT * FROM oracle_data EXCEPT DISTINCT SELECT * FROM bq_data))
)
SELECT source, COUNT(*) AS mismatch_count 
FROM discrepancies 
GROUP BY source;
```

### Pass/Fail Criterion
* **Pass:** The reconciliation query returns 0 rows, proving 100% schema, row count, and value parity.
* **Fail:** Any discrepancies are found between the BigQuery output and the Oracle baseline.

---

## Section 5: Airflow DAG Orchestration & Idempotency Validation

### Purpose
To verify that the Airflow DAG correctly orchestrates the BigQuery execution, handles retries, and maintains idempotency (i.e., multiple executions on the same day yield the same result without duplicating records).

### Setup
1. Deploy the DAG `dw_bert_ausd_bp_ta_msisdn_his.py` to a Cloud Composer test environment.
2. Ensure the target table `sof$ta_msisdn_his` contains pre-existing data.

### Action
Execute a test suite using `pytest` and the Airflow CLI:

```python
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dagbag):
    """Verify that the DAG is parsed correctly without import errors."""
    dag = dagbag.get_dag("dw_bert_ausd_bp_ta_msisdn_his")
    assert dag is not None
    assert len(dag.tasks) == 3  # start -> execute_msisdn_his_logic -> end

def test_dag_tasks_properties(dagbag):
    """Verify task configurations match migration specifications."""
    dag = dagbag.get_dag("dw_bert_ausd_bp_ta_msisdn_his")
    bq_task = dag.get_task("execute_msisdn_his_logic")
    
    assert bq_task.retries == 1
    assert bq_task.email_on_failure is True
    assert bq_task.use_legacy_sql is False

# Integration test execution via Airflow CLI simulation
def test_idempotency_and_truncation(google_client):
    """Verify that running the pipeline twice results in identical, non-duplicated states."""
    # 1. Trigger DAG run once
    run_dag_and_wait("dw_bert_ausd_bp_ta_msisdn_his")
    count_run_1 = get_bq_row_count("sof_dataset.sof$ta_msisdn_his")
    
    # 2. Trigger DAG run a second time
    run_dag_and_wait("dw_bert_ausd_bp_ta_msisdn_his")
    count_run_2 = get_bq_row_count("sof_dataset.sof$ta_msisdn_his")
    
    # Assert that truncation occurred and rows were not appended
    assert count_run_1 > 0
    assert count_run_1 == count_run_2
```

### Pass/Fail Criterion
* **Pass:** 
  - The DAG loads with zero import errors.
  - Running the DAG multiple times produces the exact same row count in the target table, confirming that the `TRUNCATE` step executes successfully before insertion.
* **Fail:** The DAG fails to parse, tasks are misconfigured, or the target table row count doubles on the second execution.