# Migration Validation Test Suite: DW.RPOS_CARM_IMPORT

This document defines the comprehensive migration-validation test suite for the job `DW.RPOS_CARM_IMPORT`. These tests are designed to prove that the migrated Apache Airflow DAG and PySpark pipeline running on Dataproc Serverless are behaviorally equivalent to the legacy Ab Initio graph (`map_rpos_carmen_import.mp`) and KornShell wrapper (`map_rpos_carmen_import.ksh`).

---

## Section 1: End-to-End Output Parity Test (Golden Dataset)

### Purpose
To prove that given the exact same input file and reference database state, the migrated PySpark pipeline produces identical target table states in BigQuery as the legacy Ab Initio job produced in Oracle.

### Setup
1. **Reference Data**: Populate the BigQuery table `DWH$TA_C_VERTRAG` with a controlled set of contract records.
2. **Input File**: Upload a mock input file `CARMEN_B_202305_pos.fix` to `gs://{GCS_BUCKET}/crs/work/` containing:
   * 1 Header record (`H`)
   * 5 Payload records (`P`) representing different business routing scenarios (Factoring, Reselling, Temporary, and standard positions)
   * 1 Trailer record (`X`)
3. **Target Tables**: Ensure target BigQuery tables (`DWH$TA_F_RPOS_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, `DWH$TA_T_RPOS_CARM`) are cleared of any pre-existing test data.

### Action
Execute the Airflow DAG `dw_rpos_carm_import` via manual trigger, passing the target filename as a parameter:
```json
{
  "BHB_Dateiname": "gs://YOUR_BUCKET_NAME/crs/work/CARMEN_B_202305_pos.fix",
  "BHB_Eintragsnr": "9999"
}
```

### Pass/Fail Criterion
* **Pass**: The row counts and column values in all five target BigQuery tables match the expected golden output dataset exactly. No duplicate records are created. The source file is successfully moved to the archive directory `gs://{GCS_BUCKET}/crs/store/`.
* **Fail**: Any row count mismatch, value mismatch (including decimal precision errors), failure to archive the file, or job execution failure.

### Runnable Test Code (pytest + BigQuery)

```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=os.environ.get("GCP_PROJECT"))

@pytest.fixture(scope="module")
def dataset_id():
    return os.environ.get("BQ_DATASET", "DW_HOUSE_SCHEMA")

def test_e2e_output_parity(bq_client, dataset_id):
    # Define expected row counts for each target table based on the 5 mock payload records
    expected_counts = {
        "DWH$TA_F_RPOS_CARM": 5,            # All records route here
        "DWH$TA_F_RPOS_FACT_CARM": 2,       # Decoded as Factoring 'F'
        "DWH$TA_F_GPOS_FACT_CARM": 1,       # Decoded as Gutschriften 'G' (F + P30002)
        "DWH$TA_F_RPOS_RESELLING_CARM": 1,  # Decoded as Reselling 'R'
        "DWH$TA_T_RPOS_CARM": 1             # Temporary routing (pooling = 'P')
    }
    
    for table_name, expected_count in expected_counts.items():
        query = f"SELECT COUNT(*) as cnt FROM `{bq_client.project}.{dataset_id}.{table_name}`"
        query_job = bq_client.query(query)
        results = query_job.result()
        row = next(results)
        
        assert row.cnt == expected_count, f"Table {table_name} failed parity check. Expected {expected_count}, got {row.cnt}"

def test_e2e_value_precision(bq_client, dataset_id):
    # Verify monetary sum precision on the general target table
    query = f"""
        SELECT 
            SUM(rechpos_brutto_eur) as total_brutto,
            SUM(rechpos_netto_eur) as total_netto,
            SUM(rechpos_mwst_eur) as total_mwst
        FROM `{bq_client.project}.{dataset_id}.DWH$TA_F_RPOS_CARM`
    """
    query_job = bq_client.query(query)
    row = next(query_job.result())
    
    # Expected values calculated from mock input file
    assert float(row.total_brutto) == 1190.00
    assert float(row.total_netto) == 1000.00
    assert float(row.total_mwst) == 190.00
```

---

## Section 2: Transformation & Routing Validation Tests

### Test 2.1: Decimal Standardization & Type Validation

#### Purpose
To verify that the PySpark pipeline correctly standardizes German decimal formats (replacing commas with periods) and raises a validation failure if mandatory fields are null or structurally malformed.

#### Setup
1. Upload an input file `CARMEN_B_malformed_pos.fix` containing:
   * A record with a comma decimal: `P;202305;1234567890123;;R123;20230515;10;20;RABATT;119,00;100,00;19,00;...`
   * A record with a missing mandatory field (`rechnung_id` is empty).

#### Action
Run the PySpark pipeline with the malformed file.

#### Pass/Fail Criterion
* **Pass**: The comma decimal record is successfully parsed as `119.00`, `100.00`, and `19.00`. The pipeline raises a `ValueError` with a clear error trace when encountering the record with the missing mandatory `rechnung_id`, halting execution before writing to BigQuery.
* **Fail**: The pipeline silently ignores the missing mandatory field, or fails to parse the German comma decimal format, resulting in null values or casting exceptions.

#### Runnable Test Code (PySpark Unit Test)

```python
from decimal import Decimal
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import functions as F

def test_decimal_standardization_and_validation(spark_session):
    # Mock raw input mimicking the comma replacement step
    raw_data = [("P;202305;1234567890123;;R123;20230515;10;20;RABATT;119,00;100,00;19,00;BHB_G;P;123;P1;PROV1;1;1;F;P30002;B1",)]
    df_raw = spark_session.createDataFrame(raw_data, ["value"])
    
    # Apply comma replacement
    df_replaced = df_raw.select(
        F.substring(F.col("value"), 1, 1).alias("kennzeichen"),
        F.regexp_replace(F.substring(F.col("value"), 2), ",", ".").alias("datensatz_rest")
    )
    
    # Parse fields
    split_col = F.split(F.col("datensatz_rest"), ";")
    df_parsed = df_replaced.select(
        split_col.getItem(8).alias("rechpos_brutto_eur").cast("decimal(15,2)"),
        split_col.getItem(9).alias("rechpos_netto_eur").cast("decimal(15,2)"),
        split_col.getItem(10).alias("rechpos_mwst_eur").cast("decimal(15,2)")
    )
    
    row = df_parsed.first()
    assert row.rechpos_brutto_eur == Decimal("119.00")
    assert row.rechpos_netto_eur == Decimal("100.00")
    assert row.rechpos_mwst_eur == Decimal("19.00")
```

---

### Test 2.2: Contract Join, Validity Proof & Ranking (Windowing)

#### Purpose
To verify that the PySpark pipeline correctly joins input records with `DWH$TA_C_VERTRAG`, applies the validity proof logic against the last day of the processing month, and correctly ranks multiple matching contracts using the window function.

#### Setup
1. **Reference Data (`DWH$TA_C_VERTRAG`)**:
   * Contract A: `vertrag_id_carmen = '999'`, `gueltig_von = '2005-01-01'`, `gueltig_bis = '2005-12-31'`, `dwh_vertrag_id = 100`
   * Contract B: `vertrag_id_carmen = '999'`, `gueltig_von = '2005-06-01'`, `gueltig_bis = '2005-12-31'`, `dwh_vertrag_id = 200` (Newer contract)
2. **Input Record**: `vertrags_id = '999'`, `monats_id = '200507'` (Processing month July 2005. Last day is `2005-07-31`, which falls inside both contracts).

#### Action
Run the PySpark pipeline.

#### Pass/Fail Criterion
* **Pass**: The pipeline joins the record with Contract B (`dwh_vertrag_id = 200`) because it has a newer `gueltig_von` date, satisfying the window ranking specification (`gueltig_von DESC`, `dwh_vertrag_id DESC`).
* **Fail**: The pipeline joins with Contract A, duplicates the record, or fails to join entirely.

#### Runnable Test Code (SQL Assertion)

```sql
-- Assert that the record with vertrags_id = '999' was enriched with the correct ranked contract ID (200)
SELECT 
  rechnung_id,
  vertrags_id,
  dwh_vertrag_id,
  gueltig_von
FROM 
  `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_F_RPOS_CARM`
WHERE 
  vertrags_id = 999;

-- EXPECTED RESULT:
-- dwh_vertrag_id must be 200
-- gueltig_von must be '2005-06-01'
```

---

### Test 2.3: Business Form Decoding & Target Routing

#### Purpose
To verify that the pipeline decodes business forms correctly (specifically the conditional override of `F` to `G` when `vas_kenn = 'P30002'`) and routes records to the correct target tables.

#### Setup
1. **Input Records**:
   * Record 1: `rpos_geschaftsform_kenn = 'F'`, `vas_kenn = 'P30002'` (Should decode to 'G')
   * Record 2: `rpos_geschaftsform_kenn = 'F'`, `vas_kenn = 'OTHER'` (Should remain 'F')
   * Record 3: `rpos_geschaftsform_kenn = 'R'` (Should remain 'R')

#### Action
Run the PySpark pipeline.

#### Pass/Fail Criterion
* **Pass**: 
  * Record 1 is routed to `DWH$TA_F_GPOS_FACT_CARM` (Factoring Gutschriften) and `DWH$TA_F_RPOS_CARM`.
  * Record 2 is routed to `DWH$TA_F_RPOS_FACT_CARM` (Factoring Invoices) and `DWH$TA_F_RPOS_CARM`.
  * Record 3 is routed to `DWH$TA_F_RPOS_RESELLING_CARM` and `DWH$TA_F_RPOS_CARM`.
* **Fail**: Any record is routed to the wrong table, or the conditional override fails to execute.

#### Runnable Test Code (SQL Assertion)

```sql
-- Assert correct routing of decoded 'G'
SELECT COUNT(1) as cnt FROM `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_F_GPOS_FACT_CARM` 
WHERE vas_kenn = 'P30002' AND rpos_geschaftsform_kenn = 'F';
-- Assert count is exactly 1

-- Assert correct routing of standard 'F'
SELECT COUNT(1) as cnt FROM `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_F_RPOS_FACT_CARM` 
WHERE vas_kenn = 'OTHER' AND rpos_geschaftsform_kenn = 'F';
-- Assert count is exactly 1

-- Assert correct routing of Reselling 'R'
SELECT COUNT(1) as cnt FROM `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_F_RPOS_RESELLING_CARM` 
WHERE rpos_geschaftsform_kenn = 'R';
-- Assert count is exactly 1
```

---

### Test 2.4: Temporary Table Routing (Rabatt & Pooling)

#### Purpose
To verify that records qualifying as temporary debit-level positions are correctly routed to `DWH$TA_T_RPOS_CARM`.

#### Setup
1. **Input Records**:
   * Record 1: `rech_leistung_id_carm = 'RABATT'`, `vertrags_id = 0` (Qualifies)
   * Record 2: `pooling = 'P'` (Qualifies)
   * Record 3: `rech_leistung_id_carm = 'RABATT'`, `vertrags_id = 999` (Does NOT qualify)

#### Action
Run the PySpark pipeline.

#### Pass/Fail Criterion
* **Pass**: Only Record 1 and Record 2 are routed to `DWH$TA_T_RPOS_CARM`. Record 3 is excluded from the temporary table.
* **Fail**: Record 3 is found in `DWH$TA_T_RPOS_CARM`, or Record 1/Record 2 are missing.

#### Runnable Test Code (SQL Assertion)

```sql
-- Assert temporary table contents
SELECT 
  rech_leistung_id_carm, 
  vertrags_id, 
  pooling 
FROM 
  `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_T_RPOS_CARM`;

-- EXPECTED ROWS:
-- Row 1: rech_leistung_id_carm = 'RABATT', vertrags_id = 0
-- Row 2: pooling = 'P'
-- Total Row Count must be exactly 2.
```

---

### Test 2.5: Rollup Aggregation Correctness

#### Purpose
To verify that the PySpark pipeline correctly aggregates monetary fields (`rechpos_brutto_eur`, `rechpos_netto_eur`, `rechpos_mwst_eur`) when multiple records share the same natural keys.

#### Setup
1. **Input Records**: Two records with identical natural keys:
   * Record 1: `rechnung_id = 'R1'`, `vertrags_id = '123'`, `rechpos_netto_eur = 100.00`
   * Record 2: `rechnung_id = 'R1'`, `vertrags_id = '123'`, `rechpos_netto_eur = 150.00`

#### Action
Run the PySpark pipeline.

#### Pass/Fail Criterion
* **Pass**: The target table `DWH$TA_F_RPOS_CARM` contains exactly one aggregated record with `rechnung_id = 'R1'`, `vertrags_id = '123'`, and `rechpos_netto_eur = 250.00`.
* **Fail**: The target table contains two separate records (no aggregation), or the aggregated sum is incorrect.

#### Runnable Test Code (SQL Assertion)

```sql
SELECT 
  rechnung_id, 
  vertrags_id, 
  COUNT(*) as record_count, 
  SUM(rechpos_netto_eur) as total_netto
FROM 
  `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_F_RPOS_CARM`
WHERE 
  rechnung_id = 'R1'
GROUP BY 
  rechnung_id, vertrags_id;

-- EXPECTED RESULT:
-- record_count = 1
-- total_netto = 250.00
```

---

## Section 3: Idempotency & Pre-Load Deletes Validation

### Purpose
To prove that the pipeline is fully idempotent. Running the pipeline multiple times with the same input file must not result in duplicate records in the target tables. Pre-load deletes must clear existing records matching the composite keys before inserting.

### Setup
1. Insert a dummy record directly into `DWH$TA_F_RPOS_CARM` with keys:
   * `rechnung_id = 'IDEMP_001'`
   * `rechnung_datum = '2023-05-15'`
   * `standardvertrags_id = 9999`
   * `vertrags_id = 8888`
   * `rechpos_netto_eur = 500.00` (Old value)
2. Prepare an input file containing a record with the exact same keys but `rechpos_netto_eur = 1000.00` (New value).

### Action
Run the PySpark pipeline.

### Pass/Fail Criterion
* **Pass**: The old record with `rechpos_netto_eur = 500.00` is deleted. The target table contains exactly one record for `rechnung_id = 'IDEMP_001'` with `rechpos_netto_eur = 1000.00`.
* **Fail**: Duplicate records exist for the composite key, or the old record is not deleted, or the new record is not inserted.

#### Runnable Test Code (SQL Assertion)

```sql
SELECT 
  rechnung_id, 
  COUNT(*) as record_count, 
  SUM(rechpos_netto_eur) as total_netto
FROM 
  `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_F_RPOS_CARM`
WHERE 
  rechnung_id = 'IDEMP_001'
GROUP BY 
  rechnung_id;

-- EXPECTED RESULT:
-- record_count = 1
-- total_netto = 1000.00
```

---

## Section 4: Audit Logging & Metadata Validation

### Purpose
To verify that the pipeline correctly updates the audit log table `DWH$TA_K_RECH_ABSGRP` and the job execution log table `DWH$TA_K_MELDUNGEN` with correct metrics (record counts, filenames, and trailer information).

### Setup
1. Prepare an input file `CARMEN_B_202305_pos.fix` containing exactly 10 payload records.
2. The trailer record is: `X;Some_Info_BHB_G;20230531;10;Trailer_Content_Text`
3. Set the environment variable `BHB_Eintragsnr = 9999`.

### Action
Run the PySpark pipeline.

### Pass/Fail Criterion
* **Pass**:
  * `DWH$TA_K_RECH_ABSGRP` contains a record with `monats_id = '202305'`, `abs_grp = 'BHB_G'`, `dateiname = 'CARMEN_B_202305_pos.fix'`, and `rechnung_datum = '2023-05-31'`.
  * `DWH$TA_K_MELDUNGEN` contains a record for `entrynr = 9999` with `anzahl_ds_eof = 10`, `dateiname = 'CARMEN_B_202305_pos.fix'`, and `enderecord_text = 'Trailer_Content_Text'`.
* **Fail**: Audit tables are not updated, or contain incorrect record counts, filenames, or dates.

#### Runnable Test Code (SQL Assertion)

```sql
-- Assert DWH$TA_K_RECH_ABSGRP update
SELECT 
  monats_id, 
  abs_grp, 
  dateiname, 
  rechnung_datum
FROM 
  `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_K_RECH_ABSGRP`
WHERE 
  dateiname = 'CARMEN_B_202305_pos.fix';

-- EXPECTED RESULT:
-- monats_id = '202305'
-- abs_grp = 'BHB_G'
-- rechnung_datum = '2023-05-31'

-- Assert DWH$TA_K_MELDUNGEN update
SELECT 
  anzahl_ds_eof, 
  dateiname, 
  enderecord_text
FROM 
  `${GCP_PROJECT}.${BQ_DATASET}.DWH$TA_K_MELDUNGEN`
WHERE 
  entrynr = 9999;

-- EXPECTED RESULT:
-- anzahl_ds_eof = 10
-- dateiname = 'CARMEN_B_202305_pos.fix'
-- enderecord_text = 'Trailer_Content_Text'
```

---

## Section 5: Airflow DAG & Orchestration Validation

### Purpose
To verify that the Airflow DAG `dw_rpos_carm_import` correctly resolves environment variables from Airflow Variables, successfully submits the Dataproc Serverless job, and handles task retries on failure.

### Setup
1. Configure the following Airflow Variables:
   * `GCP_PROJECT` = `test-gcp-project`
   * `GCP_REGION` = `europe-west3`
   * `GCS_BUCKET` = `test-gcs-bucket`
2. Mock the `DataprocSubmitJobOperator` to prevent actual GCP billing charges during DAG structural testing.

### Action
Run a DAG structural integrity test using `pytest`.

### Pass/Fail Criterion
* **Pass**: The DAG loads without syntax errors, contains the task `dw_rpos_carm_import_task`, resolves the correct GCS script URI (`gs://test-gcs-bucket/pyspark_scripts/map_rpos_carmen_import.py`), and has retries set to `1` with a `5` minute delay.
* **Fail**: DAG fails to parse, variables are unresolved, or task properties do not match the design specification.

#### Runnable Test Code (Airflow DAG Unit Test)

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(scope="module")
def set_airflow_variables():
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCP_REGION", "europe-west3")
    Variable.set("GCS_BUCKET", "test-gcs-bucket")
    yield
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")
    Variable.delete("GCS_BUCKET")

def test_dag_integrity(set_airflow_variables):
    dagbag = DagBag(dag_folder="abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_rpos_carm_import")
    
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    assert dag is not None, "Failed to load DAG dw_rpos_carm_import"
    
    # Verify Task configuration
    task = dag.get_task("dw_rpos_carm_import_task")
    assert task is not None
    
    # Verify default args
    assert task.retries == 1
    assert task.retry_delay.total_seconds() == 300  # 5 minutes
    
    # Verify Dataproc job configuration resolution
    job_config = task.job
    pyspark_job = job_config["pyspark_job"]
    assert pyspark_job["main_python_file_uri"] == "gs://test-gcs-bucket/pyspark_scripts/map_rpos_carmen_import.py"
    assert "--job_kennung" in pyspark_job["args"]
```