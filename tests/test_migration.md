Here is a comprehensive migration-validation test suite designed to verify the behavioral equivalence of the migrated `ausd_bp_ta_msisdn_his` job on GCP against its legacy Oracle counterpart.

---

# Migration Validation Test Suite: `ausd_bp_ta_msisdn_his`

## Test Case 1: End-to-End Output Parity (Golden Dataset)
### Purpose
Verify that given identical source data in both environments, the migrated BigQuery pipeline produces the exact same output dataset as the legacy Oracle pipeline.

### Setup
1. **Legacy Oracle Environment:**
   * Populate `isbert_schema.dwtk_meldungen` with a watermark record:
     ```sql
     INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) 
     VALUES ('BERT_DROP_TEMP_TABLE', TO_DATE('2023-10-25 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));
     ```
   * Populate `pds.ta_callnumber` with a representative set of active and inactive products:
     ```sql
     INSERT INTO pds.ta_callnumber (bpri_com_id, cc, ndc, sn, callnumber_role_id, valid_to, valid_from, insert_at, modified_at, is_production)
     VALUES (1001, '49', '170', '1234567', 1, TO_DATE('2099-12-31', 'YYYY-MM-DD'), TO_DATE('2023-01-01', 'YYYY-MM-DD'), TO_DATE('2023-01-01', 'YYYY-MM-DD'), NULL, 1);
     ```
2. **Target BigQuery Environment:**
   * Replicate the exact same records into `gcp-project-id.isbert_dataset.dwtk_meldungen` and `gcp-project-id.pds_dataset.ta_callnumber`.
   * Ensure the target table `gcp-project-id.sof_dataset.ta_msisdn_his` is empty.

### Action
1. Run the legacy Oracle SQL*Plus script `d_ausd_bp_ta_msisdn_his.sql`.
2. Run the migrated BigQuery SQL script `queries/d_ausd_bp_ta_msisdn_his.sql` with `@wiederanlaufwert = 0`.
3. Extract the results from both target tables into CSV format, sorted by `BPRI_COM_ID` and `MSISDN`.

### Pass/Fail Criterion
* **Pass:** The MD5 checksum of the sorted legacy output CSV matches the MD5 checksum of the sorted BigQuery output CSV exactly.
* **Fail:** Any discrepancy in row count, column values, or data types between the two outputs.

---

## Test Case 2: Null Concatenation Semantics (Oracle vs. BigQuery)
### Purpose
Verify that the BigQuery implementation preserves Oracle's string concatenation behavior where concatenating a `NULL` value behaves like an empty string (instead of turning the entire concatenated string `NULL`).

### Setup
In the BigQuery source table `gcp-project-id.pds_dataset.ta_callnumber`, insert records with various `NULL` combinations in the MSISDN components:

```sql
INSERT INTO `gcp-project-id.pds_dataset.ta_callnumber` 
  (bpri_com_id, cc, ndc, sn, callnumber_role_id, valid_to, valid_from, insert_at, modified_at, is_production)
VALUES 
  (2001, '49', NULL, '1234567', 1, '2099-12-31', '2023-01-01', '2023-01-01', NULL, 1), -- Missing NDC
  (2002, NULL, '170', '1234567', 1, '2099-12-31', '2023-01-01', '2023-01-01', NULL, 1), -- Missing CC
  (2003, NULL, NULL, '1234567', 1, '2099-12-31', '2023-01-01', '2023-01-01', NULL, 1);  -- Missing CC and NDC
```
Ensure a valid watermark exists in `dwtk_meldungen` (e.g., `2023-10-25`).

### Action
Execute the BigQuery migration query.

### Pass/Fail Criterion
* **Pass:** The target table `ta_msisdn_his` contains the following concatenated values:
  * `bpri_com_id = 2001` $\rightarrow$ `MSISDN = '491234567'`
  * `bpri_com_id = 2002` $\rightarrow$ `MSISDN = '1701234567'`
  * `bpri_com_id = 2003` $\rightarrow$ `MSISDN = '1234567'`
* **Fail:** Any of the resulting `MSISDN` values are `NULL` or do not match the expected concatenated string.

---

## Test Case 3: Watermark Date Filtering Boundaries
### Purpose
Validate that the date filters (`insert_at`, `modified_at`, `valid_from`) correctly include or exclude records based on the dynamic watermark date retrieved from `dwtk_meldungen`.

### Setup
1. Set the watermark date in `dwtk_meldungen` to `2023-10-15`:
   ```sql
   INSERT INTO `gcp-project-id.isbert_dataset.dwtk_meldungen` (job_kennung, timecreated)
   VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP '2023-10-15 12:00:00 UTC');
   ```
2. Insert test cases into `ta_callnumber` to test boundary conditions:
   ```sql
   INSERT INTO `gcp-project-id.pds_dataset.ta_callnumber` 
     (bpri_com_id, cc, ndc, sn, callnumber_role_id, valid_to, valid_from, insert_at, modified_at, is_production)
   VALUES 
     -- Case A: Valid record (all dates within watermark boundaries)
     (3001, '49', '170', '111', 1, '2099-12-31', '2023-10-14', '2023-10-14', NULL, 1),
     -- Case B: Invalid - insert_at is after watermark
     (3002, '49', '170', '222', 1, '2099-12-31', '2023-10-14', '2023-10-16', NULL, 1),
     -- Case C: Invalid - valid_from is after watermark
     (3003, '49', '170', '333', 1, '2099-12-31', '2023-10-16', '2023-10-14', NULL, 1),
     -- Case D: Valid - modified_at is after watermark (means modification hasn't taken effect yet for this historical snapshot)
     (3004, '49', '170', '444', 1, '2099-12-31', '2023-10-14', '2023-10-14', '2023-10-16', 1),
     -- Case E: Invalid - modified_at is on or before watermark (modification has already occurred)
     (3005, '49', '170', '544', 1, '2099-12-31', '2023-10-14', '2023-10-14', '2023-10-14', 1),
     -- Case F: Invalid - is_production is 0
     (3006, '49', '170', '666', 1, '2099-12-31', '2023-10-14', '2023-10-14', NULL, 0);
   ```

### Action
Execute the BigQuery migration query.

### Pass/Fail Criterion
* **Pass:** Only `bpri_com_id` `3001` and `3004` are loaded into `ta_msisdn_his`.
* **Fail:** Any of the invalid records (`3002`, `3003`, `3005`, `3006`) are present in the target table.

---

## Test Case 4: Restart Parameter (`@wiederanlaufwert`) Filtering
### Purpose
Verify that the restart mechanism correctly filters out records with a `bpri_com_id` less than or equal to the passed `@wiederanlaufwert` parameter.

### Setup
1. Ensure the watermark date is set to `2023-10-15`.
2. Insert three valid records into `ta_callnumber`:
   ```sql
   INSERT INTO `gcp-project-id.pds_dataset.ta_callnumber` 
     (bpri_com_id, cc, ndc, sn, callnumber_role_id, valid_to, valid_from, insert_at, modified_at, is_production)
   VALUES 
     (4001, '49', '170', '100', 1, '2099-12-31', '2023-10-10', '2023-10-10', NULL, 1),
     (4002, '49', '170', '200', 1, '2099-12-31', '2023-10-10', '2023-10-10', NULL, 1),
     (4003, '49', '170', '300', 1, '2099-12-31', '2023-10-10', '2023-10-10', NULL, 1);
   ```

### Action
Execute the BigQuery query passing `@wiederanlaufwert = 4002`.

### Pass/Fail Criterion
* **Pass:** Only the record with `bpri_com_id = 4003` is inserted into `ta_msisdn_his`.
* **Fail:** Records with `bpri_com_id` `4001` or `4002` are found in the target table.

---

## Test Case 5: Airflow DAG Parameter Passing & Execution
### Purpose
Verify that the Airflow DAG correctly parses the `wiederanlaufwert` parameter from the DAG run configuration and injects it into the BigQuery job parameters.

### Setup
Deploy the DAG `bereitstellung_basisprodukte_bert.py` to a test Airflow environment (or run via a local `pytest` harness using the Airflow `DagBag`).

### Action
Run a Python unit test that parses the DAG and checks the templated parameters of the `BigQueryInsertJobOperator` task.

```python
# test_dag_configuration.py
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.context import Context

def test_dag_parameter_injection():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="bereitstellung_basisprodukte_bert")
    assert dag is not None

    # Retrieve the specific task
    task = dag.get_task("execute_msisdn_history")
    
    # Create a mock DagRun with custom configuration
    conf = {"wiederanlaufwert": 9999}
    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_id="test_run_1",
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        conf=conf
    )
    
    # Render templates for this task context
    context = Context(dag_run=dag_run, task=task)
    task.render_template_fields(context)
    
    # Extract the rendered query parameters
    query_params = task.configuration["query"]["queryParameters"]
    wiederanlauf_param = next(p for p in query_params if p["name"] == "wiederanlaufwert")
    
    # Assertions
    assert wiederanlauf_param["parameterValue"]["value"] == "9999"
    assert wiederanlauf_param["parameterType"]["type"] == "INT64"
```

### Pass/Fail Criterion
* **Pass:** The test executes successfully, proving that the DAG dynamically resolves and injects the `wiederanlaufwert` parameter into the BigQuery operator.
* **Fail:** The parameter is missing, has the wrong type, or defaults to `0` despite the custom configuration.

---

## Test Case 6: Data Quality, Schema, and Row-Count Assertions
### Purpose
Ensure that the target table `ta_msisdn_his` conforms to strict data quality rules post-migration (e.g., no unexpected NULLs, correct data types, and no duplicate active records).

### Setup
Run the complete migration pipeline using a production-like dataset.

### Action
Execute the following validation queries against the target BigQuery table:

```sql
-- Assertion A: Check for NULLs in mandatory fields
SELECT COUNT(1) AS null_failures
FROM `gcp-project-id.sof_dataset.ta_msisdn_his`
WHERE BPRI_COM_ID IS NULL OR MSISDN IS NULL;

-- Assertion B: Check for duplicate active records (uniqueness constraint validation)
SELECT COUNT(1) AS duplicate_failures
FROM (
  SELECT BPRI_COM_ID, MSISDN, CALLNUMBER_ROLE_ID, COUNT(1) as cnt
  FROM `gcp-project-id.sof_dataset.ta_msisdn_his`
  GROUP BY 1, 2, 3
  HAVING cnt > 1
);

-- Assertion C: Check for invalid MSISDN formats (e.g. empty strings or non-numeric characters)
SELECT COUNT(1) AS format_failures
FROM `gcp-project-id.sof_dataset.ta_msisdn_his`
WHERE MSISDN = '' OR REGEXP_CONTAINS(MSISDN, r'[^0-9]');
```

### Pass/Fail Criterion
* **Pass:** All three queries return a count of `0`.
* **Fail:** Any query returns a count $> 0$, indicating a schema violation or data corruption during migration.