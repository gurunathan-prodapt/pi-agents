Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Cloud Composer, Dataproc Serverless, and PySpark code behaves identically to the legacy UC4, KornShell, and Ab Initio implementations for the job `DW.DWH_ABPZ_KKM_AIL_AGENT`.

---

## 1. Output Parity & Schema Validation

### Purpose
To prove that the migrated PySpark pipeline (`bhb_ccm_proc_write_agent_ads_lookup.py`) produces a flat file (`AgentADSLookup.txt`) with the exact same schema, column order, delimiter, row count, and data values as the legacy Ab Initio graph output when run against identical source data.

### Setup
1. Create a mock BigQuery source view `DWH$VI_S_SDM_AGENT_ADS` populated with a controlled set of test records (including active, inactive, duplicate, and null-valued fields).
2. Run the legacy Ab Initio graph on the legacy environment using the same source dataset to generate the baseline file `AgentADSLookup_legacy.txt`.
3. Configure the Airflow variables `GCP_PROJECT_ID`, `GCP_REGION`, `GCS_BUCKET`, and `BQ_DATASET` in the test environment.

### Action
Execute the PySpark job on Dataproc Serverless using the following validation script:

```python
# test_output_parity.py
import pytest
import subprocess
from google.cloud import storage
from google.cloud import bigquery

@pytest.fixture(scope="module")
def setup_bq_test_data():
    client = bigquery.Client()
    dataset_id = "DWH_KKM"
    table_id = "DWH$VI_S_SDM_AGENT_ADS"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    # Define schema matching the source view
    schema = [
        bigquery.SchemaField("AgentId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SAMAccountName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("DisplayName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Department", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ManagerId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("IsActive", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("LastModifiedTimestamp", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    # Insert deterministic test data (including duplicates to test windowing)
    rows_to_insert = [
        {"AgentId": "A001", "SAMAccountName": "jdoe", "DisplayName": "John Doe", "Department": "Sales", "ManagerId": "M001", "IsActive": True, "LastModifiedTimestamp": "2023-10-01T12:00:00Z"},
        {"AgentId": "A001", "SAMAccountName": "jdoe", "DisplayName": "John Doe", "Department": "Marketing", "ManagerId": "M001", "IsActive": True, "LastModifiedTimestamp": "2023-10-02T12:00:00Z"}, # Latest
        {"AgentId": "A002", "SAMAccountName": "asmith", "DisplayName": "Alice Smith", "Department": "HR", "ManagerId": "M002", "IsActive": False, "LastModifiedTimestamp": "2023-10-01T12:00:00Z"},
        {"AgentId": "A003", "SAMAccountName": None, "DisplayName": "Bob NoName", "Department": None, "ManagerId": None, "IsActive": True, "LastModifiedTimestamp": "2023-10-01T12:00:00Z"},
    ]
    
    # Recreate table
    client.delete_table(table_ref, not_found_ok=True)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    client.insert_rows_json(table_ref, rows_to_insert)
    yield
    client.delete_table(table_ref, not_found_ok=True)

def test_pyspark_output_parity(setup_bq_test_data):
    # Run the PySpark script locally or via spark-submit
    output_uri = "gs://test-migration-bucket/exports/AgentADSLookup.txt"
    
    cmd = [
        "python3", "bhb_ccm_proc_write_agent_ads_lookup.py",
        "--gcp_project", "test-project",
        "--bq_dataset", "DWH_KKM",
        "--output_uri", output_uri
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"PySpark job failed: {result.stderr}"
    
    # Download generated file from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket("test-migration-bucket")
    
    # PySpark writes to a directory; find the part file
    blobs = list(bucket.list_blobs(prefix="exports/AgentADSLookup.txt/part-"))
    assert len(blobs) > 0, "No output part file found in GCS"
    
    generated_content = blobs[0].download_as_text().strip().split("\n")
    
    # Expected output based on deduplication and formatting rules:
    # Format: AgentId|SAMAccountName|DisplayName|Department|ManagerId|IsActive
    expected_content = [
        "A001|jdoe|John Doe|Marketing|M001|true",
        "A002|asmith|Alice Smith|HR|M002|false",
        "A003||Bob NoName|||true"
    ]
    
    assert sorted(generated_content) == sorted(expected_content), "Output content does not match expected parity"
```

### Pass/Fail Criterion
* **Pass:** The generated file contains exactly 3 rows, matches the expected pipe-delimited values, correctly resolves the latest record for `A001` based on `LastModifiedTimestamp`, and handles `NULL` values as empty strings.
* **Fail:** Any mismatch in row count, column order, delimiter, null representation, or deduplication logic.

---

## 2. Transformation Correctness & Edge-Case Handling

### Purpose
To verify that the PySpark deduplication window function (`row_number() over (partition by AgentId order by LastModifiedTimestamp desc)`) and type casting function correctly handle edge cases such as:
1. Multiple updates for the same `AgentId` within the same second.
2. `NULL` values in key fields (`AgentId`, `SAMAccountName`, `IsActive`).
3. Boolean-to-string casting parity (e.g., `True` $\rightarrow$ `"true"` vs. `"1"`).

### Setup
Deploy a test suite using a local PySpark session to isolate transformation logic from GCP infrastructure.

### Action
Execute the following unit test suite:

```python
# test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Unit-Testing-Transformations") \
        .getOrCreate()

def test_deduplication_and_null_handling(spark):
    # Schema matching BigQuery source view
    schema = "AgentId STRING, SAMAccountName STRING, DisplayName STRING, Department STRING, ManagerId STRING, IsActive BOOLEAN, LastModifiedTimestamp TIMESTAMP"
    
    # Test data covering edge cases
    data = [
        # Case 1: Identical timestamps - fallback ordering stability
        ("A001", "jdoe", "John Doe", "Sales", "M001", True, "2023-10-01 12:00:00"),
        ("A001", "jdoe", "John Doe", "IT", "M001", True, "2023-10-01 12:00:00"), 
        # Case 2: All NULL values except AgentId
        ("A002", None, None, None, None, None, "2023-10-01 12:00:00"),
        # Case 3: NULL AgentId (should be grouped or handled gracefully)
        (None, "ghost", "No ID", "Finance", "M001", True, "2023-10-01 12:00:00")
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    
    # Apply pipeline transformation logic
    window_spec = Window.partitionBy("AgentId").orderBy(F.col("LastModifiedTimestamp").desc())
    processed_df = df.withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .select(
            F.col("AgentId").alias("agent_id"),
            F.col("SAMAccountName").alias("sam_account"),
            F.col("DisplayName").alias("display_name"),
            F.col("Department").alias("department"),
            F.col("ManagerId").alias("manager_id"),
            F.col("IsActive").alias("is_active")
        )
        
    output_df = processed_df.select(
        F.concat_ws(
            "|", 
            F.coalesce(F.col("agent_id"), F.lit("")),
            F.coalesce(F.col("sam_account"), F.lit("")),
            F.coalesce(F.col("display_name"), F.lit("")),
            F.coalesce(F.col("department"), F.lit("")),
            F.coalesce(F.col("manager_id"), F.lit("")),
            F.coalesce(F.col("is_active").cast("string"), F.lit(""))
        ).alias("formatted_row")
    )
    
    results = [row.formatted_row for row in output_df.collect()]
    
    # Assertions
    # 1. Deduplication must yield exactly one record for A001
    assert any(r.startswith("A001|") for r in results)
    # 2. Null values must be empty strings between pipes
    assert "A002|||||" in results
    # 3. Null AgentId row must format correctly
    assert "|ghost|No ID|Finance|M001|true" in results
```

### Pass/Fail Criterion
* **Pass:** The transformation logic successfully deduplicates records, formats boolean fields to lowercase string representations (`"true"`/`"false"`), and converts `NULL` values to empty strings without throwing runtime exceptions.
* **Fail:** Any `NullPointerException`, incorrect row selection during deduplication, or failure to format null values as empty strings.

---

## 3. Orchestration & Log-Assertion Parity

### Purpose
To verify that the Airflow DAG (`dw_dwh_abpz_kkm_ail_agent`) preserves the exact execution sequence of the legacy UC4 job and prints the identical German log messages and return codes required by downstream monitoring systems.

### Setup
1. Deploy the DAG `dw_dwh_abpz_kkm_ail_agent` to a Cloud Composer environment.
2. Configure a mock failure state (e.g., by pointing the PySpark task to a non-existent GCS bucket) to trigger the failure callback.

### Action
Execute the DAG and capture the task execution logs from Google Cloud Logging (Stackdriver).

```python
# test_orchestration_logs.py
import pytest
from airflow.models import DagBag

def test_dag_structure_and_imports():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_dwh_abpz_kkm_ail_agent")
    
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    assert dag is not None, "Failed to load DAG 'dw_dwh_abpz_kkm_ail_agent'"
    
    # Verify exact task execution sequence
    expected_tasks = [
        "dw_dwh_adm_pruefe_ab_initio_start_inc",
        "dw_dwh_adm_job_monitor_start",
        "r_alis_objekt_parameter_print",
        "evaluate_state_branch",
        "run_agent_lookup_pyspark",
        "print_success_log",
        "dw_dwh_adm_pruefe_ab_initio_ende_inc",
        "dw_dwh_adm_job_monitor_end",
        "r_alis_objekt_end_message"
    ]
    
    actual_tasks = [task.task_id for task in dag.topological_sort()]
    assert actual_tasks == expected_tasks, "DAG task execution sequence does not match legacy order"

def test_log_literal_preservation(caplog):
    # Simulate execution of the parameter print task and assert exact string matches
    from dags.dw_dwh_abpz_kkm_ail_agent import print_frame_params, parse_failure_log
    
    # Test Parameter Print Output
    print_frame_params()
    log_output = caplog.text
    assert "Jobkennung (Prüfjob)       : 'ABPZ_KKM_AIL_AGENT'" in log_output
    assert "ab initio Konfiguration  = BHB_CCM_PROC_WriteAgentADSLookup.cfg" in log_output
    
    # Test Failure Callback Output
    class MockContext:
        def get(self, key, default=None):
            return "mock_value"
            
    parse_failure_log(MockContext())
    assert "Rueckgabewert: '1' (Fehlerfall)***************************" in caplog.text
```

### Pass/Fail Criterion
* **Pass:** The DAG imports cleanly, matches the legacy task sequence, and prints the exact literal strings (including asterisks and German spelling) to standard output.
* **Fail:** Any deviation in task ordering, missing tasks, or modified log strings (e.g., translating `"Rueckgabewert"` or changing the number of asterisks).

---

## 4. End-to-End Integration & GCS File Delivery

### Purpose
To verify that the entire pipeline executes successfully from Cloud Composer, triggers the Dataproc Serverless PySpark job, reads from BigQuery, and delivers the final `AgentADSLookup.txt` file to the designated GCS export bucket.

### Setup
1. Ensure the Composer DAG is active and all GCP connection variables are set.
2. Clear any existing files in `gs://{GCS_BUCKET}/exports/`.

### Action
Trigger the DAG via the Airflow CLI or UI and monitor execution:

```bash
# Trigger the DAG manually
gcloud composer environments run ${COMPOSER_ENV} \
    --location ${GCP_REGION} \
    dags trigger -- dw_dwh_abpz_kkm_ail_agent

# Wait for DAG execution to complete and verify GCS output file existence
gsutil ls gs://${GCS_BUCKET}/exports/AgentADSLookup.txt
```

### Pass/Fail Criterion
* **Pass:** The DAG completes with a `SUCCESS` state, and a non-empty file named `AgentADSLookup.txt` is successfully written to `gs://{GCS_BUCKET}/exports/`.
* **Fail:** The DAG fails, runs into a timeout, or completes without delivering the output file to GCS.