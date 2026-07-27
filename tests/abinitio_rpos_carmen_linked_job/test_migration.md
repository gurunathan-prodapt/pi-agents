# Migration Validation Test Suite: DW.RPOS_CARM_IMPORT

This document defines the comprehensive migration-validation test suite for the migrated `DW.RPOS_CARM_IMPORT` data pipeline. These tests are designed to prove behavioral equivalence between the legacy Ab Initio graph (`map_rpos_carmen_import.mp`) and the migrated PySpark application running on Google Cloud Dataproc Serverless with BigQuery.

---

## Test Case 1: End-to-End Output Parity (Golden Dataset Test)

### Purpose
To prove that the migrated PySpark pipeline produces identical outputs to the legacy Ab Initio graph when processed with the same input file and reference database state.

### Setup
1. **Reference Data**: Populate the BigQuery table `DWH_TA_C_VERTRAG` with a controlled set of contract records.
2. **Input File**: Create a mock billing file `CARMEN_B_TEST_pos.fix` containing:
   * 1 Header record (`H`)
   * 5 Nutzdaten records (`P`) representing different business scenarios (Factoring, Reselling, Temporary Rebates)
   * 1 Endedatensatz record (`X`)
3. **Target Tables**: Ensure target BigQuery tables (`DWH_TA_F_RPOS_CARM`, `DWH_TA_F_RPOS_FACT_CARM`, `DWH_TA_F_GPOS_FACT_CARM`, `DWH_TA_F_RPOS_RESELLING_CARM`, `DWH_TA_T_RPOS_CARM`) are empty or pre-cleared.
4. **Environment Variables**: Set `GCP_PROJECT`, `BQ_DATASET`, and `GCS_BUCKET` in the test environment.

### Action
1. Upload the mock input file to `gs://{GCS_BUCKET}/crs/work/CARMEN_B_TEST_pos.fix`.
2. Execute the migrated PySpark script `map_rpos_carmen_import.py` via spark-submit or a local Spark session.
3. Extract the resulting records from all target BigQuery tables.
4. Compare the results against a pre-calculated "golden" output dataset generated from the legacy Ab Initio run.

### Pass/Fail Criterion
The test **passes** if the row counts and all column values (excluding system-generated timestamps like `ladedatum`) in the BigQuery target tables match the golden dataset exactly.

```python
import pytest
from pyspark.sql import SparkSession
from google.cloud import bigquery

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("E2E-Parity-Test").getOrCreate()

def test_e2e_parity(spark):
    client = bigquery.Client()
    project = os.environ["GCP_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    
    # Define target tables to validate
    target_tables = [
        "dwh_ta_f_rpos_carm",
        "dwh_ta_f_rpos_fact_carm",
        "dwh_ta_f_gpos_fact_carm",
        "dwh_ta_f_rpos_reselling_carm",
        "dwh_ta_t_rpos_carm"
    ]
    
    for table in target_tables:
        # Fetch migrated data from BigQuery
        migrated_df = spark.read.format("bigquery") \
            .option("table", f"{project}.{dataset}.{table}") \
            .load() \
            .drop("ladedatum")  # Exclude system timestamp from comparison
            
        # Fetch golden data (pre-loaded in a validation dataset)
        golden_df = spark.read.format("bigquery") \
            .option("table", f"{project}.validation_golden.{table}") \
            .load() \
            .drop("ladedatum")
            
        # Assert schema and data equivalence
        assert migrated_df.schema == golden_df.schema, f"Schema mismatch on table {table}"
        
        diff_count = migrated_df.subtract(golden_df).count() + golden_df.subtract(migrated_df).count()
        assert diff_count == 0, f"Data mismatch on table {table}. Diff count: {diff_count}"
```

---

## Test Case 2: Strict Field Validation and Error Handling

### Purpose
To verify that the migrated PySpark pipeline enforces the exact same strict validation rules as the legacy Ab Initio graph and raises the identical literal error messages when encountering invalid or blank fields.

### Setup
1. **Input Files**: Prepare multiple malformed input files, each containing a single validation failure in a specific field (e.g., blank `monats_id`, blank `debitor_id`, etc.).
2. **Reference Data**: Standard mock contract data in `DWH_TA_C_VERTRAG`.

### Action
1. Execute the PySpark pipeline for each malformed input file.
2. Capture the raised exceptions and log outputs.

### Pass/Fail Criterion
The test **passes** if the pipeline crashes immediately with a `ValueError` containing the exact literal error message specified in the legacy design document for that specific field.

```python
import pytest
import subprocess

validation_cases = [
    ("blank_monats_id.fix", "Invalid Data in field monats_id"),
    ("blank_debitor_id.fix", "Invalid Data in field debitor_id"),
    ("blank_rechnung_id.fix", "Invalid Data in field rechnung_id"),
    ("blank_rechnung_datum.fix", "Invalid Data in field rechnung_datum"),
    ("blank_standardvertrags_id.fix", "Invalid Data in field standardvertrags_id"),
    ("blank_vertrags_id.fix", "Invalid Data in field vertrags_id"),
    ("blank_rech_leistung_id_carm.fix", "Invalid Data in field rech_leistung_id_carm"),
    ("blank_rechpos_brutto_eur.fix", "Invalid Data in field rechpos_brutto_eur"),
    ("blank_rechpos_netto_eur.fix", "Invalid Data in field rechpos_netto_eur"),
    ("blank_rechpos_mwst_eur.fix", "Invalid Data in field rechpos_mwst_eur"),
]

@pytest.mark.parametrize("filename,expected_error", validation_cases)
def test_strict_field_validations(filename, expected_error):
    # Set environment variables to point to the specific malformed file
    env = os.environ.copy()
    env["BHB_Dateiname"] = f"gs://{env['GCS_BUCKET']}/crs/work/validation_errors/{filename}"
    
    # Run the PySpark script as a subprocess
    process = subprocess.Popen(
        ["python3", "abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    
    # Assert that the process failed and returned the exact error message
    assert process.returncode != 0, f"Pipeline succeeded but was expected to fail with: {expected_error}"
    assert expected_error in stderr.decode("utf-8") or expected_error in stdout.decode("utf-8"), \
        f"Expected error message '{expected_error}' not found in output."
```

---

## Test Case 3: Contract Enrichment, Temporal Validation, and Ranking

### Purpose
To verify the correctness of the left outer join with `DWH_TA_C_VERTRAG`, the temporal boundary checks (month-end vs. validity dates), the low-value imputation (`\000`), and the ranking logic (`rankindex == 1`).

### Setup
1. **Reference Data**: Populate `DWH_TA_C_VERTRAG` with three overlapping versions of a contract for `vertrag_id_carmen = 12345`:
   * Version A: `dwh_vertrag_id = 1001`, `gueltig_von = 2005-01-01`, `gueltig_bis = 2005-05-31`
   * Version B: `dwh_vertrag_id = 1002`, `gueltig_von = 2005-06-01`, `gueltig_bis = 2005-12-31`
   * Version C: `dwh_vertrag_id = 1003`, `gueltig_von = 2005-06-01`, `gueltig_bis = NULL` (Overlapping with Version B to test ranking)
2. **Input File**: Create an input file with a transaction for `vertrags_id = 12345` and `monats_id = 200506` (Month end = `2005-06-30`).

### Action
1. Run the PySpark pipeline.
2. Query the target table `DWH_TA_F_RPOS_CARM` for the processed transaction.

### Pass/Fail Criterion
The test **passes** if:
1. The transaction is enriched with Version C (`dwh_vertrag_id = 1003`) because it is the most active contract based on the ranking logic (`clean_gueltig_von DESC, clean_dwh_vertrag_id DESC`).
2. If the transaction month end falls outside the contract validity boundaries, the contract fields are correctly nullified (`valid_flag = 1` logic).

```sql
-- Assertion Query 1: Verify correct contract version (Version C) was selected
SELECT dwh_vertrag_id, rahmenvertrag_id
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_f_rpos_carm`
WHERE vertrags_id = 12345 AND monats_id = '200506';

-- EXPECTED RESULT:
-- dwh_vertrag_id = 1003

-- Assertion Query 2: Verify nullification of contract fields when month-end is out of bounds
-- (e.g., transaction for monats_id = 200412, which is before the earliest contract start date of 2005-01-01)
SELECT dwh_vertrag_id, rahmenvertrag_id, dwh_gp_id
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_f_rpos_carm`
WHERE vertrags_id = 12345 AND monats_id = '200412';

-- EXPECTED RESULT:
-- All contract fields (dwh_vertrag_id, rahmenvertrag_id, dwh_gp_id) must be NULL
```

---

## Test Case 4: Business Logic Routing and Aggregation (Rollup)

### Purpose
To verify that:
1. `rpos_geschaftsform_kenn` is correctly remapped from 'F' to 'G' when `vas_kenn` is 'P30002'.
2. Transactions are correctly aggregated (rolled up) and routed to the correct target tables based on business codes and the `typ` flag.

### Setup
1. **Input File**: Prepare an input file with the following records:
   * Row 1: `rpos_geschaftsform_kenn = 'F'`, `vas_kenn = 'P30002'` (Should map to 'G' and route to `DWH_TA_F_GPOS_FACT_CARM`).
   * Row 2: `rpos_geschaftsform_kenn = 'F'`, `vas_kenn = 'OTHER'` (Should map to 'F' and route to `DWH_TA_F_RPOS_FACT_CARM`).
   * Row 3: `rpos_geschaftsform_kenn = 'R'` (Should route to `DWH_TA_F_RPOS_RESELLING_CARM`).
   * Row 4: `rech_leistung_id_carm = 'RABATT'`, `vertrags_id = 0` (Should set `typ = 'T'` and route to `DWH_TA_T_RPOS_CARM`).
   * Row 5 & 6: Duplicate keys with different monetary values (Should be aggregated into a single row in `DWH_TA_F_GPOS_FACT_CARM`).

### Action
1. Run the PySpark pipeline.
2. Query the target tables to verify counts and routed values.

### Pass/Fail Criterion
The test **passes** if all records are routed to their respective target tables with correct aggregations and remapped values.

```sql
-- Assertion 1: Remapping 'F' + 'P30002' -> 'G' and routing to GPOS
SELECT COUNT(1) as cnt 
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_f_gpos_fact_carm` 
WHERE rechnung_id = 'REC_ROW_1';
-- EXPECTED: cnt = 1

-- Assertion 2: Standard Factoring Invoice routing
SELECT COUNT(1) as cnt 
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_f_rpos_fact_carm` 
WHERE rechnung_id = 'REC_ROW_2';
-- EXPECTED: cnt = 1

-- Assertion 3: Reselling routing
SELECT COUNT(1) as cnt 
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_f_rpos_reselling_carm` 
WHERE rechnung_id = 'REC_ROW_3';
-- EXPECTED: cnt = 1

-- Assertion 4: Temporary Rebate routing (typ = 'T')
SELECT COUNT(1) as cnt 
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_t_rpos_carm` 
WHERE rechnung_id = 'REC_ROW_4';
-- EXPECTED: cnt = 1

-- Assertion 5: Rollup Aggregation verification
SELECT rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_f_gpos_fact_carm`
WHERE rechnung_id = 'REC_DUPLICATE';
-- EXPECTED: Sum of Row 5 and Row 6 values
```

---

## Test Case 5: Idempotency (Delete-Before-Insert)

### Purpose
To verify that the pipeline executes a clean transactional reload (Delete-then-Insert) pattern, ensuring that existing records with matching keys are purged before new ones are written, preventing duplicate key violations.

### Setup
1. **Target Tables**: Pre-populate `DWH_TA_F_RPOS_CARM` with a dummy record:
   * `rechnung_id = 'INV999'`, `rechnung_datum = '2005-06-30'`, `standardvertrags_id = 99`, `vertrags_id = 99`, `rechpos_brutto_eur = 500.00`.
2. **Input File**: Prepare an input file containing a record with the exact same keys but a different monetary value:
   * `rechnung_id = 'INV999'`, `rechnung_datum = '2005-06-30'`, `standardvertrags_id = 99`, `vertrags_id = 99`, `rechpos_brutto_eur = 1200.00`.

### Action
1. Run the PySpark pipeline.
2. Query `DWH_TA_F_RPOS_CARM` for `rechnung_id = 'INV999'`.

### Pass/Fail Criterion
The test **passes** if only one record exists in the target table with `rechpos_brutto_eur = 1200.00`, proving the pre-existing record was successfully deleted before the new one was inserted.

```sql
-- Assertion Query: Check for duplicates and verify value update
SELECT COUNT(1) as row_count, SUM(rechpos_brutto_eur) as total_brutto
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_f_rpos_carm`
WHERE rechnung_id = 'INV999' 
  AND rechnung_datum = '2005-06-30'
  AND standardvertrags_id = 99
  AND vertrags_id = 99;

-- EXPECTED RESULT:
-- row_count = 1
-- total_brutto = 1200.00
```

---

## Test Case 6: Control Table Updates (ABSGRP and MELDUNGEN)

### Purpose
To verify that the footer record (`X`) is parsed correctly and updates the audit logging tables `DWH_TA_K_RECH_ABSGRP` and `DWH_TA_K_MELDUNGEN` accurately.

### Setup
1. **Input File**: Prepare an input file with a valid footer record:
   * `X;CARMEN_B_200506_pos.fix;20050701;150;SUCCESS_RUN;20050701120000;`
2. **Control Tables**:
   * Pre-populate `DWH_TA_K_MELDUNGEN` with a record for `entrynr = 9999`.
   * Ensure `DWH_TA_K_RECH_ABSGRP` is in a known state.
3. **Environment Variables**: Set `BHB_Eintragsnr = 9999` and `BHB_Dateiname = CARMEN_B_200506_pos.fix`.

### Action
1. Run the PySpark pipeline.
2. Query both control tables to verify the updates.

### Pass/Fail Criterion
The test **passes** if:
1. `DWH_TA_K_MELDUNGEN` is updated with `anzahl_ds_eof = 150`, `enderecord_text = 'SUCCESS_RUN'`, and `zusatzinfo = 'CARMEN_B_200506_pos.fix'`.
2. `DWH_TA_K_RECH_ABSGRP` contains a merged/upserted record with `monats_id = 200506` (calculated as stichtag `20050701` minus 1 month), `abs_grp = '20050'`, and `rechnungsteil = 'P'`.

```sql
-- Assertion Query 1: Verify dwh_ta_k_meldungen update
SELECT anzahl_ds_eof, dateiname, enderecord_text, zusatzinfo
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_k_meldungen`
WHERE entrynr = 9999;

-- EXPECTED RESULT:
-- anzahl_ds_eof = 150
-- dateiname = 'CARMEN_B_200506_pos.fix'
-- enderecord_text = 'SUCCESS_RUN'
-- zusatzinfo = 'CARMEN_B_200506_pos.fix'

-- Assertion Query 2: Verify dwh_ta_k_rech_absgrp merge/upsert
SELECT monats_id, abs_grp, dateiname, rechnungsteil, rechnung_datum
FROM `GCP_PROJECT.BQ_DATASET.dwh_ta_k_rech_absgrp`
WHERE dateiname = 'CARMEN_B_200506_pos.fix';

-- EXPECTED RESULT:
-- monats_id = 200506
-- abs_grp = '20050'
-- rechnungsteil = 'P'
-- rechnung_datum = '2005-07-01'
```

---

## Test Case 7: Airflow DAG and Dataproc Operator Validation

### Purpose
To verify that the migrated Airflow DAG is structurally sound, has no syntax errors, correctly resolves environment variables, and configures the `DataprocSubmitJobOperator` with the correct parameters.

### Setup
1. **Airflow Environment**: Ensure a local Airflow environment is active with mock Airflow Variables set for `GCP_PROJECT`, `GCP_REGION`, `DATAPROC_CLUSTER`, `GCS_BUCKET`, and `DW_DIR_IMP_SAP`.

### Action
1. Run a pytest suite against the DAG file `dw_rpos_carm_import.py`.

### Pass/Fail Criterion
The test **passes** if the DAG loads without import errors, contains the single task `rpos_carm_import`, and the task properties match the expected Dataproc configuration.

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def setup_airflow_variables(monkeypatch):
    # Mock Airflow variables
    variables = {
        "GCP_PROJECT": "mock-project",
        "GCP_REGION": "europe-west3",
        "DATAPROC_REGION": "europe-west3",
        "DATAPROC_CLUSTER": "mock-cluster",
        "GCS_BUCKET": "mock-bucket",
        "DW_DIR_IMP_SAP": "gs://mock-bucket/sap"
    }
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_rpos_carm_import")
    
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1
    
    task = dag.get_task("rpos_carm_import")
    assert task.region == "europe-west3"
    assert task.project_id == "mock-project"
    
    # Verify PySpark job properties
    job_config = task.job
    pyspark_job = job_config["pyspark_job"]
    assert pyspark_job["main_python_file_uri"] == "gs://mock-bucket/pyspark_scripts/map_rpos_carmen_import.py"
    assert pyspark_job["properties"]["spark.yarn.appMasterEnv.DWH_JOB_KENNUNG"] == "RPOS_CARM_IMPORT"
```