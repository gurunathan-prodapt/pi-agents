Here is the comprehensive test suite designed to validate the migration of the `ausd_bp_ta_apn_carmen` pipeline. 

These tests are structured to prove behavioral equivalence between the legacy Oracle/KornShell implementation and the migrated BigQuery/Airflow implementation.

---

# Migration Validation Test Suite: `ausd_bp_ta_apn_carmen`

## Section 1: Output Parity Tests

### Test Case 1.1: End-to-End Output Parity (Golden Dataset Comparison)
#### Purpose
Verify that given identical source data and an identical dynamic cutoff date, the migrated BigQuery stored procedure produces the exact same output rows (values, schema, and row count) as the legacy Oracle SQL script.

#### Setup
1. **Legacy Environment (Oracle):**
   - Populate source tables `pds$ta_pdp_context_assoc@pcrs1`, `pds$ta_pdp_context@pcrs1`, and `pds$ta_access_point@pcrs1` with a standard "golden" test dataset containing 1,000 mixed records (active, inactive, non-production, null contracts).
   - Insert a control record in `isbert_schema.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = TO_DATE('20231027', 'YYYYMMDD')`.
   - Execute the legacy script `d_ausd_bp_ta_apn_carmen.sql`.
   - Export the resulting target table `sof$ta_apn_carmen` to a CSV file (`legacy_golden_output.csv`).

2. **Target Environment (BigQuery):**
   - Truncate and load the exact same "golden" test dataset into `carmen_replica.pds_ta_pdp_context_assoc`, `carmen_replica.pds_ta_pdp_context`, and `carmen_replica.pds_ta_access_point`.
   - Insert the corresponding control record in `isbert_schema.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = TIMESTAMP('2023-10-27 12:00:00 UTC')`.

#### Action
1. Execute the BigQuery stored procedure:
   ```sql
   CALL `sof.proc_d_ausd_bp_ta_apn_carmen`();
   ```
2. Export the target table `sof.ta_apn_carmen` to a CSV file (`target_migrated_output.csv`).

#### Pass/Fail Criterion
The test passes if `legacy_golden_output.csv` and `target_migrated_output.csv` are identical. This is verified using the following `pytest` script:

```python
import pandas as pd
import pytest

def test_golden_dataset_parity():
    legacy_df = pd.read_csv("legacy_golden_output.csv").sort_values(by=["CNTRCT_ID", "ACCESS_POINT_NAME"]).reset_index(drop=True)
    target_df = pd.read_csv("target_migrated_output.csv").sort_values(by=["CNTRCT_ID", "ACCESS_POINT_NAME"]).reset_index(drop=True)
    
    # Assert exact schema and data match
    pd.testing.assert_frame_equal(legacy_df, target_df, check_dtype=False)
```

---

## Section 2: Transformation Correctness Tests

### Test Case 2.1: Temporal Validity Filtering (Cutoff Date Boundaries)
#### Purpose
Verify that the temporal logic slices are correctly applied in BigQuery using the inclusive/exclusive rules:
*   `insert_at <= cutoff AND (modified_at IS NULL OR modified_at > cutoff)`
*   `valid_from <= cutoff AND (valid_to IS NULL OR valid_to > cutoff)`

#### Setup
1. Set the dynamic cutoff date in `isbert_schema.dwtk_meldungen` to `2023-10-27`.
2. Populate `carmen_replica.pds_ta_pdp_context_assoc` with the following edge cases for contract `1001`:

| Case ID | Description | insert_at | modified_at | valid_from | valid_to | Expected Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **A** | Fully Active (Standard) | 2023-10-01 | NULL | 2023-10-01 | NULL | **Included** |
| **B** | Modified in future | 2023-10-01 | 2023-11-01 | 2023-10-01 | NULL | **Included** |
| **C** | Modified in past | 2023-10-01 | 2023-10-26 | 2023-10-01 | NULL | **Excluded** |
| **D** | Valid in future | 2023-10-01 | NULL | 2023-10-28 | NULL | **Excluded** |
| **E** | Expired on cutoff | 2023-10-01 | NULL | 2023-10-01 | 2023-10-27 | **Excluded** (valid_to must be > cutoff) |
| **F** | Expired in future | 2023-10-01 | NULL | 2023-10-01 | 2023-10-28 | **Included** |

3. Ensure matching active, production records exist in `pds_ta_pdp_context` and `pds_ta_access_point`.

#### Action
```sql
CALL `sof.proc_d_ausd_bp_ta_apn_carmen`();
```

#### Pass/Fail Criterion
The test passes if only Cases **A**, **B**, and **F** are loaded into the target table.

```sql
-- Assert exact matches for contract 1001
ASSERT (
  SELECT COUNT(1) 
  FROM `sof.ta_apn_carmen` 
  WHERE CNTRCT_ID = 1001
) = 3;
```

---

### Test Case 2.2: Production Flag and NULL Handling
#### Purpose
Verify that:
1. Non-production PDP contexts (`is_production != 1`) are filtered out.
2. Records with `CNTRCT_ID IS NULL` are filtered out.

#### Setup
1. Set the dynamic cutoff date to `2023-10-27`.
2. Insert the following records into the source tables:
   *   **Record 1:** `cntrct_id = 9999`, `is_production = 0` (Non-production)
   *   **Record 2:** `cntrct_id = NULL`, `is_production = 1` (Null Contract)
   *   **Record 3:** `cntrct_id = 8888`, `is_production = 1` (Valid Production)

#### Action
```sql
CALL `sof.proc_d_ausd_bp_ta_apn_carmen`();
```

#### Pass/Fail Criterion
The test passes if only contract `8888` is present in the target table.

```sql
-- Assert that non-production and NULL contract records are excluded
ASSERT NOT EXISTS (
  SELECT 1 FROM `sof.ta_apn_carmen` WHERE CNTRCT_ID = 9999 OR CNTRCT_ID IS NULL
);

ASSERT EXISTS (
  SELECT 1 FROM `sof.ta_apn_carmen` WHERE CNTRCT_ID = 8888
);
```

---

### Test Case 2.3: Fallback Cutoff Date Handling
#### Purpose
Verify that if no upstream execution event exists in `isbert_schema.dwtk_meldungen` for `BERT_DROP_TEMP_TABLE`, the stored procedure defaults the cutoff date to `1900-01-01` and executes without failing.

#### Setup
1. Truncate the `isbert_schema.dwtk_meldungen` table (or delete all rows where `job_kennung = 'BERT_DROP_TEMP_TABLE'`).
2. Populate source tables with records having timestamps both before and after `1900-01-01`.

#### Action
```sql
CALL `sof.proc_d_ausd_bp_ta_apn_carmen`();
```

#### Pass/Fail Criterion
The test passes if the procedure runs successfully, defaults the cutoff date to `1900-01-01`, and writes a success log entry containing `Effective date=19000101`.

```sql
-- Assert that the fallback date was used in the execution log
ASSERT EXISTS (
  SELECT 1 
  FROM `isbert_schema.job_log`
  WHERE job_name = 'ausd_bp_ta_apn_carmen'
    AND event_type = 'SUCCESS'
    AND message LIKE '%Effective date=19000101%'
);
```

---

## Section 3: External-System Replacements & Orchestration

### Test Case 3.1: Airflow DAG Execution and Task Flow
#### Purpose
Verify that the Airflow DAG `ausd_bp_ta_apn_carmen_dag` successfully triggers the BigQuery stored procedure and handles execution states correctly.

#### Setup
1. Deploy `dags/ausd_bp_ta_apn_carmen_dag.py` to a local or development Airflow environment (e.g., Cloud Composer local runner).
2. Mock the BigQuery connection `bigquery_default` to point to the test GCP project.

#### Action
Trigger the DAG manually via the Airflow CLI or UI:
```bash
airflow dags trigger ausd_bp_ta_apn_carmen_dag
```

#### Pass/Fail Criterion
The test passes if:
1. The DAG runs and completes with a `SUCCESS` state.
2. The task `run_proc_d_ausd_bp_ta_apn_carmen` completes successfully.

```python
from airflow.models import DagBag

def test_dag_loaded_and_structured():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.get_dag(dag_id="ausd_bp_ta_apn_carmen_dag")
    
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "run_proc_d_ausd_bp_ta_apn_carmen"
```

---

### Test Case 3.2: Transactional Idempotency (Truncate-and-Insert)
#### Purpose
Verify that the target table `sof.ta_apn_carmen` is completely truncated before insertion, ensuring that multiple runs on the same day do not duplicate data (idempotency).

#### Setup
1. Populate `sof.ta_apn_carmen` with 50 dummy records from a previous run.
2. Set up source tables with 10 active records.

#### Action
Execute the stored procedure:
```sql
CALL `sof.proc_d_ausd_bp_ta_apn_carmen`();
```

#### Pass/Fail Criterion
The test passes if the 50 dummy records are completely removed, and the target table contains exactly the 10 records from the current source state.

```sql
-- Assert that the old records were purged and only the 10 new records exist
ASSERT (
  SELECT COUNT(1) FROM `sof.ta_apn_carmen`
) = 10;
```

---

## Section 4: Data Quality & Schema Assertions

### Test Case 4.1: Target Schema and Nullability Constraints
#### Purpose
Verify that the target table `sof.ta_apn_carmen` matches the exact schema specified in the design document and does not contain invalid null values.

#### Setup
Ensure the target table `sof.ta_apn_carmen` has been created using `sql/ddl_sof_ta_apn_carmen.sql`.

#### Action
Query the BigQuery `INFORMATION_SCHEMA`.

#### Pass/Fail Criterion
The test passes if the columns, data types, and nullability rules match the specification.

```sql
-- Assert column names and data types
ASSERT EXISTS (
  SELECT 1 
  FROM `sof.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'ta_apn_carmen'
    AND column_name = 'CNTRCT_ID'
    AND data_type = 'INT64'
);

ASSERT EXISTS (
  SELECT 1 
  FROM `sof.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'ta_apn_carmen'
    AND column_name = 'ACCESS_POINT_NAME'
    AND data_type = 'STRING'
);

-- Assert that no null contract IDs were accidentally written to the target
ASSERT (
  SELECT COUNT(1) 
  FROM `sof.ta_apn_carmen` 
  WHERE CNTRCT_ID IS NULL
) = 0;
```

---

### Test Case 4.2: Job Log Audit Trail Verification
#### Purpose
Verify that every execution of the stored procedure writes a structured audit trail entry to `isbert_schema.job_log` for both success and failure scenarios.

#### Setup
1. Prepare a query that will cause a runtime error (e.g., temporarily rename the source table to trigger a `Table not found` exception).

#### Action
1. Run the procedure under normal conditions.
2. Run the procedure under the error-inducing condition.

#### Pass/Fail Criterion
The test passes if both a `SUCCESS` log and a `FAILURE` log are written to the audit table with correct timestamps and error messages.

```sql
-- Assert success log exists
ASSERT EXISTS (
  SELECT 1 
  FROM `isbert_schema.job_log`
  WHERE job_name = 'ausd_bp_ta_apn_carmen'
    AND event_type = 'SUCCESS'
    AND message LIKE 'Processing completed successfully%'
    AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
);

-- Assert failure log exists
ASSERT EXISTS (
  SELECT 1 
  FROM `isbert_schema.job_log`
  WHERE job_name = 'ausd_bp_ta_apn_carmen'
    AND event_type = 'FAILURE'
    AND message LIKE 'ERROR in proc_d_ausd_bp_ta_apn_carmen%'
    AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
);
```