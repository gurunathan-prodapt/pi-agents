# Migration Validation Test Suite: DW.CCM_WRITE_CONTRACTMAPLOOKUP

This document defines the migration-validation test suite for the migrated job `DW.CCM_WRITE_CONTRACTMAPLOOKUP`. The test suite ensures that the migrated PySpark pipeline running on Dataproc Serverless is behaviorally equivalent to the legacy Ab Initio graph, covering output parity, transformation correctness, external system replacements, and orchestration integrity.

---

## Test Case 1: End-to-End Output Parity & Sorting

### Purpose
Verify that the migrated PySpark pipeline extracts data from BigQuery, sorts it by `vertrags_id` in ascending order, formats it with the correct delimiter (`\x01`), and writes it to GCS in a format identical to the legacy Ab Initio output.

### Setup
1. Create a test BigQuery table `DWH_TA_L_MAP_VT_CARM_DWH` in the test dataset.
2. Populate the table with unsorted test records, including out-of-order IDs.
3. Configure environment variables:
   - `GCP_PROJECT`: Target GCP Project ID.
   - `GCS_BUCKET`: Target GCS Bucket.
   - `BQ_DATASET`: Target BigQuery Dataset.
   - `CCM_PROC_ContractMapLookupFilename`: `gs://<GCS_BUCKET>/test_ccm_proc/ContractMapLookup.txt`

```sql
-- Setup: Populate test table with unsorted records
CREATE OR REPLACE TABLE `your-gcp-project.your_dataset.DWH_TA_L_MAP_VT_CARM_DWH` (
  vertrags_id INT64,
  dwh_vertrag_id INT64
);

INSERT INTO `your-gcp-project.your_dataset.DWH_TA_L_MAP_VT_CARM_DWH` (vertrags_id, dwh_vertrag_id)
VALUES
  (999999, 1111111111111111),
  (111111, 2222222222222222),
  (555555, 3333333333333333),
  (222222, 4444444444444444);
```

### Action
1. Execute the migrated PySpark script `BHB_CCM_PROC_WriteContractMapLookup.py`.
2. Download the generated file from GCS.
3. Parse and validate the file contents.

### Pass/Fail Criterion
**Pass:** 
- The PySpark job exits with status `0`.
- The output file exists on GCS at the specified path.
- The output file contains exactly 4 records.
- The records are sorted by `vertrags_id` ascending: `111111`, `222222`, `555555`, `999999`.
- The fields are delimited by `\x01` (`\001`) and lines end with `\n`.

```python
# pytest validation script
import pytest
import os
from google.cloud import storage

def test_output_parity_and_sorting():
    bucket_name = os.environ["GCS_BUCKET"]
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Locate the output file (handling PySpark directory output structure if applicable)
    blobs = list(bucket.list_blobs(prefix="test_ccm_proc/ContractMapLookup.txt/"))
    csv_blobs = [b for b in blobs if b.name.endswith(".csv") or b.name.endswith(".txt")]
    
    assert len(csv_blobs) > 0, "Output file was not generated on GCS"
    
    # Read and concatenate partition files if PySpark wrote a directory
    content = ""
    for blob in csv_blobs:
        content += blob.download_as_text()
        
    lines = [line for line in content.split("\n") if line.strip()]
    
    # Expected sorted output
    expected = [
        "111111\x012222222222222222",
        "222222\x014444444444444444",
        "555555\x013333333333333333",
        "999999\x011111111111111111"
    ]
    
    assert lines == expected, f"Output mismatch.\nExpected: {expected}\nActual: {lines}"
```

---

## Test Case 2: Null Handling and Type Correctness

### Purpose
Verify that the PySpark job correctly handles nullable fields (`dwh_vertrag_id` is nullable in the DML schema) without throwing casting exceptions, and represents NULL values as empty strings in the output file.

### Setup
1. Populate the BigQuery table `DWH_TA_L_MAP_VT_CARM_DWH` with records containing NULL values in `dwh_vertrag_id`.
2. Configure environment variables as in Test Case 1.

```sql
-- Setup: Populate test table with NULL values
TRUNCATE TABLE `your-gcp-project.your_dataset.DWH_TA_L_MAP_VT_CARM_DWH`;

INSERT INTO `your-gcp-project.your_dataset.DWH_TA_L_MAP_VT_CARM_DWH` (vertrags_id, dwh_vertrag_id)
VALUES
  (333333, NULL),
  (444444, 5555555555555555);
```

### Action
1. Execute the migrated PySpark script `BHB_CCM_PROC_WriteContractMapLookup.py`.
2. Download and inspect the output file from GCS.

### Pass/Fail Criterion
**Pass:**
- The PySpark job exits with status `0`.
- The output file contains exactly 2 records.
- The record for `vertrags_id = 333333` is formatted as `333333\x01` (empty string after the delimiter).
- No floating-point representations (e.g., `333333.0`) are present in the output file.

```python
# pytest validation script
def test_null_handling():
    bucket_name = os.environ["GCS_BUCKET"]
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blobs = list(bucket.list_blobs(prefix="test_ccm_proc/ContractMapLookup.txt/"))
    csv_blobs = [b for b in blobs if b.name.endswith(".csv") or b.name.endswith(".txt")]
    
    content = "".join([b.download_as_text() for b in csv_blobs])
    lines = [line for line in content.split("\n") if line.strip()]
    
    expected = [
        "333333\x01",
        "444444\x015555555555555555"
    ]
    
    assert lines == expected, f"Null handling mismatch.\nExpected: {expected}\nActual: {lines}"
```

---

## Test Case 3: BigQuery Stored Procedure Execution (Side Effect Validation)

### Purpose
Verify that the PySpark script successfully calls the BigQuery stored procedure `SetzeLadedatumAbInitio` with the correct parameters, replacing the legacy Oracle PL/SQL call.

### Setup
1. Create a mock stored procedure `SetzeLadedatumAbInitio` in BigQuery that logs calls to a metadata audit table.
2. Create the audit table to capture the parameters.
3. Set environment variables:
   - `BHB_CCM_PROC_TargetObjectName`: `ContractMapLookup_Test.txt`
   - `BHB_CCM_PROC_FirstDay`: `20260101`
   - `BHB_CCM_PROC_LastDayPlus1`: `20260102`

```sql
-- Setup: Create audit table and mock stored procedure
CREATE OR REPLACE TABLE `your-gcp-project.your_dataset.audit_procedure_calls` (
  target_object_name STRING,
  first_day STRING,
  last_day_plus_1 STRING,
  called_at TIMESTAMP
);

CREATE OR REPLACE PROCEDURE `your-gcp-project.your_dataset.SetzeLadedatumAbInitio`(
  target_obj STRING, first_d STRING, last_d STRING
)
BEGIN
  INSERT INTO `your-gcp-project.your_dataset.audit_procedure_calls` (target_object_name, first_day, last_day_plus_1, called_at)
  VALUES (target_obj, first_d, last_d, CURRENT_TIMESTAMP());
END;
```

### Action
1. Execute the migrated PySpark script `BHB_CCM_PROC_WriteContractMapLookup.py`.
2. Query the audit table `audit_procedure_calls`.

### Pass/Fail Criterion
**Pass:**
- The PySpark job executes successfully.
- A new row is written to `audit_procedure_calls` containing:
  - `target_object_name` = `'ContractMapLookup_Test.txt'`
  - `first_day` = `'20260101'`
  - `last_day_plus_1` = `'20260102'`

```python
# pytest validation script
def test_stored_procedure_execution():
    from google.cloud import bigquery
    client = bigquery.Client()
    
    project = os.environ["GCP_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    
    query = f"""
        SELECT target_object_name, first_day, last_day_plus_1 
        FROM `{project}.{dataset}.audit_procedure_calls`
        ORDER BY called_at DESC LIMIT 1
    """
    
    query_job = client.query(query)
    results = list(query_job.result())
    
    assert len(results) == 1, "Stored procedure was not executed/logged."
    row = results[0]
    assert row.target_object_name == "ContractMapLookup_Test.txt"
    assert row.first_day == "20260101"
    assert row.last_day_plus_1 == "20260102"
```

---

## Test Case 4: Airflow DAG Orchestration & Parameter Passing

### Purpose
Verify that the Airflow DAG `dw_ccm_write_contractmaplookup` correctly instantiates, passes parameters to the Dataproc Serverless task, and handles execution context variables.

### Setup
1. Deploy the Airflow DAG `dw_ccm_write_contractmaplookup` to the Cloud Composer environment.
2. Set up Airflow Variables or Environment Variables in Composer:
   - `GCP_PROJECT`
   - `GCS_BUCKET`
   - `BQ_DATASET`

### Action
1. Trigger the Airflow DAG manually with the following configuration JSON:
   ```json
   {
     "target_object_name": "Orchestrated_Lookup.txt",
     "first_day": "20260814",
     "last_day_plus_1": "20260815"
   }
   ```
2. Wait for the DAG run to complete.

### Pass/Fail Criterion
**Pass:**
- The DAG run completes with a status of `SUCCESS`.
- The Dataproc Serverless task logs show that the parameters `Orchestrated_Lookup.txt`, `20260814`, and `20260815` were received and processed.
- The BigQuery stored procedure is called with these exact parameters.

```python
# Airflow DAG structural validation test
from airflow.models import DagBag

def test_dag_loaded():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag_id = "dw_ccm_write_contractmaplookup"
    dag = dagbag.get_dag(dag_id)
    
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "dw_ccm_write_contractmaplookup"
```

---

## Test Case 5: Boundary Case - Empty Source Table

### Purpose
Verify that the PySpark job behaves gracefully when the source BigQuery table is empty. It should write an empty file (or 0-byte file) to GCS and still execute the stored procedure without failure.

### Setup
1. Truncate the BigQuery table `DWH_TA_L_MAP_VT_CARM_DWH`.
2. Clear the audit table `audit_procedure_calls`.

```sql
-- Setup: Truncate source table
TRUNCATE TABLE `your-gcp-project.your_dataset.DWH_TA_L_MAP_VT_CARM_DWH`;
```

### Action
1. Execute the migrated PySpark script `BHB_CCM_PROC_WriteContractMapLookup.py`.
2. Verify GCS output and BigQuery audit logs.

### Pass/Fail Criterion
**Pass:**
- The PySpark job exits with status `0`.
- An empty file (0 records) is created on GCS.
- The stored procedure `SetzeLadedatumAbInitio` is still executed successfully (verified via the audit table).

```python
# pytest validation script
def test_empty_source_table():
    from google.cloud import bigquery, storage
    
    # Check GCS file is empty
    bucket_name = os.environ["GCS_BUCKET"]
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blobs = list(bucket.list_blobs(prefix="test_ccm_proc/ContractMapLookup.txt/"))
    csv_blobs = [b for b in blobs if b.name.endswith(".csv") or b.name.endswith(".txt")]
    
    content = "".join([b.download_as_text() for b in csv_blobs])
    lines = [line for line in content.split("\n") if line.strip()]
    
    assert len(lines) == 0, f"Expected empty file, but got {len(lines)} lines."
    
    # Check stored procedure was still called
    project = os.environ["GCP_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    client = bigquery.Client()
    
    query = f"SELECT COUNT(1) as cnt FROM `{project}.{dataset}.audit_procedure_calls`"
    query_job = client.query(query)
    result = list(query_job.result())[0]
    
    assert result.cnt > 0, "Stored procedure was not called for empty source table."
```