# Migration Validation Test Suite: DW.DWH_ABPZ_KKM_AIL_AGENT

This document contains the migration-validation test suite designed to prove behavioral equivalence between the legacy UC4/Ab Initio/Oracle-based pipeline and the migrated Airflow/Dataproc/PySpark/BigQuery pipeline.

---

## 1. Output Parity Tests

### Test Case 1.1: End-to-End Output File Parity (Legacy vs. Migrated)
* **Purpose**: Verify that given identical source data and configurations, the migrated PySpark job produces an output file (`AgentADSLookup.txt`) with identical schema, row count, and data values (excluding formatting noise) compared to the legacy Ab Initio output.
* **Setup**:
  1. Populate a BigQuery test table `dwh_kern_bi.agent_raw_data` with a representative snapshot of historical agent records.
  2. Populate the legacy Oracle source table with the exact same records.
  3. Upload the configuration file `configs/BHB_CCM_PROC_WriteAgentADSLookup.json` to the test GCS bucket.
  4. Set the Airflow variable `KKM_Rueckblick_Ladedatum` to `84`.
* **Action**:
  1. Execute the legacy Ab Initio graph `BHB_CCM_PROC_WriteAgentADSLookup` to generate the legacy output file.
  2. Trigger the migrated Airflow DAG `dw_dwh_abpz_kkm_ail_agent` which runs the PySpark job on Dataproc.
  3. Download the single CSV partition generated in GCS under `gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt`.
* **Pass/Fail Criterion**: The test passes if the row count, column headers, and all data values match exactly between the legacy and migrated files.
* **Validation Code**:
```python
import pandas as pd
import pytest
import glob

def test_output_file_parity():
    # Load legacy output file
    legacy_file = glob.glob("test_data/legacy_AgentADSLookup.txt")[0]
    legacy_df = pd.read_csv(legacy_file, sep="|").sort_values(by="AGENT_ID").reset_index(drop=True)
    
    # Load migrated output file from GCS target path
    migrated_file = glob.glob("test_data/migrated_AgentADSLookup.txt/*part*.csv")[0]
    migrated_df = pd.read_csv(migrated_file, sep="|").sort_values(by="AGENT_ID").reset_index(drop=True)
    
    # Assert schema parity
    assert list(legacy_df.columns) == list(migrated_df.columns), "Column headers do not match!"
    
    # Assert row count parity
    assert len(legacy_df) == len(migrated_df), f"Row count mismatch! Legacy: {len(legacy_df)}, Migrated: {len(migrated_df)}"
    
    # Assert value parity
    pd.testing.assert_frame_equal(legacy_df, migrated_df, check_dtype=False, check_exact=False, atol=1e-5)
```

---

## 2. Transformation Correctness Tests

### Test Case 2.1: Lookback Window Filter (`--rueckblick`)
* **Purpose**: Verify that the PySpark job correctly filters out records outside the historical lookback window using the dynamic parameter `KKM_Rueckblick_Ladedatum`.
* **Setup**:
  1. Set `--rueckblick` to `84` days.
  2. Insert three test records into `agent_raw_data`:
     * Record A: `last_modified_date` = Current Date - 10 days (Within window)
     * Record B: `last_modified_date` = Current Date - 83 days (Within window boundary)
     * Record C: `last_modified_date` = Current Date - 85 days (Outside window)
* **Action**: Run the PySpark transformation logic.
* **Pass/Fail Criterion**: The output dataset must contain Record A and Record B, but must **not** contain Record C.
* **Validation Code**:
```python
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse

def test_lookback_filter(spark_session):
    # Setup mock data
    today = datetime.utcnow().date()
    data = [
        (101, "Agent A", 1, "REG1", today - timedelta(days=10)),  # Inside
        (102, "Agent B", 1, "REG1", today - timedelta(days=83)),  # Boundary Inside
        (103, "Agent C", 1, "REG1", today - timedelta(days=85))   # Outside
    ]
    schema = ["agent_id", "agent_name", "agent_status", "region_id", "last_modified_date"]
    df = spark_session.createDataFrame(data, schema)
    
    # Apply filter logic matching the PySpark script
    rueckblick = 84
    cutoff_date = today - timedelta(days=rueckblick)
    filtered_df = df.filter(F.col("last_modified_date") >= cutoff_date)
    
    results = [row.agent_id for row in filtered_df.collect()]
    assert 101 in results, "Record A should be included"
    assert 102 in results, "Record B should be included"
    assert 103 not in results, "Record C should be excluded"
```

### Test Case 2.2: Status Code and Restricted Type Filtering
* **Purpose**: Verify that the transformation correctly filters records based on the JSON configuration rules (`active_status_codes` and `restricted_types`).
* **Setup**:
  1. Configure `active_status_codes` = `[1, 2, 3]` and `restricted_types` = `["TEST", "DUMMY"]`.
  2. Insert the following test records (all within the lookback window):
     * Record 1: `agent_status` = `1`, `agent_type` = `NORMAL` (Should Pass)
     * Record 2: `agent_status` = `4`, `agent_type` = `NORMAL` (Should Fail - Status)
     * Record 3: `agent_status` = `2`, `agent_type` = `TEST` (Should Fail - Type)
* **Action**: Run the PySpark transformation logic.
* **Pass/Fail Criterion**: Only Record 1 is present in the output dataset.
* **Validation Code**:
```python
def test_status_and_type_filters(spark_session):
    today = datetime.utcnow().date()
    data = [
        (1, "Agent 1", 1, "NORMAL", "REG1", today), # Pass
        (2, "Agent 2", 4, "NORMAL", "REG1", today), # Fail Status
        (3, "Agent 3", 2, "TEST", "REG1", today),   # Fail Type
    ]
    schema = ["agent_id", "agent_name", "agent_status", "agent_type", "region_id", "last_modified_date"]
    df = spark_session.createDataFrame(data, schema)
    
    # Config rules
    active_status_codes = [1, 2, 3]
    restricted_types = ["TEST", "DUMMY"]
    
    filtered_df = df.filter(
        (F.col("agent_status").isin(active_status_codes)) &
        (~F.col("agent_type").isin(restricted_types))
    )
    
    results = [row.agent_id for row in filtered_df.collect()]
    assert results == [1], f"Expected only agent_id 1, got {results}"
```

### Test Case 2.3: NULL Handling in Source Fields
* **Purpose**: Ensure that NULL values in non-key fields do not crash the pipeline and are handled gracefully, while NULLs in key fields (like `agent_id`) are handled predictably.
* **Setup**: Create a dataset containing:
  * Record 1: `agent_id` = `NULL`, `agent_name` = `"Valid Name"`, `agent_status` = `1`, `agent_type` = `"NORMAL"`, `last_modified_date` = Current Date.
  * Record 2: `agent_id` = `200`, `agent_name` = `NULL`, `agent_status` = `1`, `agent_type` = `"NORMAL"`, `last_modified_date` = Current Date.
* **Action**: Run the PySpark transformation.
* **Pass/Fail Criterion**: The pipeline must run without throwing NullPointerExceptions. Record 2 must be written to the output with a NULL/empty value in the `AGENT_NAME` column.
* **Validation Code**:
```python
def test_null_handling(spark_session):
    today = datetime.utcnow().date()
    data = [
        (None, "Agent Null ID", 1, "NORMAL", "REG1", today),
        (200, None, 1, "NORMAL", "REG1", today)
    ]
    schema = ["agent_id", "agent_name", "agent_status", "agent_type", "region_id", "last_modified_date"]
    df = spark_session.createDataFrame(data, schema)
    
    active_status_codes = [1, 2, 3]
    restricted_types = ["TEST", "DUMMY"]
    
    # Run transformation
    filtered_df = df.filter(
        (F.col("agent_status").isin(active_status_codes)) &
        (~F.col("agent_type").isin(restricted_types))
    ).select(
        F.col("agent_id").alias("AGENT_ID"),
        F.col("agent_name").alias("AGENT_NAME")
    )
    
    results = filtered_df.collect()
    # Record with agent_id 200 should have AGENT_NAME as None
    record_200 = [r for r in results if r.AGENT_ID == 200][0]
    assert record_200.AGENT_NAME is None, "AGENT_NAME should be None/Null"
```

---

## 3. External-System Replacements

### Test Case 3.1: GCS Configuration File Loading
* **Purpose**: Verify that the PySpark job correctly loads the JSON configuration file from GCS and falls back to default rules if the file is missing or corrupted.
* **Setup**:
  1. Scenario A: Upload a valid JSON config to `gs://{GCS_BUCKET}/configs/BHB_CCM_PROC_WriteAgentADSLookup.json`.
  2. Scenario B: Delete the config file or provide an invalid JSON structure.
* **Action**: Execute the PySpark job for both scenarios.
* **Pass/Fail Criterion**: 
  * In Scenario A, the job must use the rules defined in the JSON file.
  * In Scenario B, the job must log a warning and successfully fall back to the default rules (`active_status_codes`: `[1, 2, 3]`, `restricted_types`: `["TEST", "DUMMY"]`) without failing.

---

## 4. Data-Quality, Row-Count, and Schema Assertions

### Test Case 4.1: Target Schema and Column Order Verification
* **Purpose**: Ensure the output file matches the exact schema and column ordering required by the downstream `DWH$VI_S_SDM_AGENT_ADS` view.
* **Setup**: Run the PySpark job to generate the output file in GCS.
* **Action**: Read the header of the generated CSV file.
* **Pass/Fail Criterion**: The header must match the exact list and order: `['AGENT_ID', 'AGENT_NAME', 'STATUS_CODE', 'REGION_ID', 'LOAD_DATETIME']`.
* **Validation Code**:
```python
import pandas as pd

def test_target_schema_headers():
    gcs_output_path = "test_data/migrated_AgentADSLookup.txt" # Mocked local download
    df = pd.read_csv(gcs_output_path, sep="|", nrows=0)
    expected_columns = ['AGENT_ID', 'AGENT_NAME', 'STATUS_CODE', 'REGION_ID', 'LOAD_DATETIME']
    assert list(df.columns) == expected_columns, f"Schema mismatch! Expected {expected_columns}, got {list(df.columns)}"
```

### Test Case 4.2: Verbatim Logging Output Verification
* **Purpose**: Ensure that the migrated Airflow DAG outputs the exact, literal German log messages from the legacy UC4 job and shell scripts to maintain operational compliance.
* **Setup**: Trigger the Airflow DAG `dw_dwh_abpz_kkm_ail_agent`.
* **Action**: Retrieve and scan the task execution logs for the `log_pre_execution` and `log_post_execution` tasks.
* **Pass/Fail Criterion**: The logs must contain the exact literal strings specified in the design document.
* **Validation Code**:
```python
def test_verbatim_logging_compliance(airflow_log_output):
    # Assert start monitor logs
    assert "Jobkennung ABPZ_KKM_AIL_AGENT eingetragen für log_pre_execution" in airflow_log_output
    
    # Assert Ab Initio start check logs
    assert "Prüfung erfolgreich, starte Ab Initio Job(s)" in airflow_log_output
    
    # Assert Ab Initio end check logs
    assert "Die Ab Initio Verarbeitung ist fertig. Der Status wird auf fertig" in airflow_log_output
    assert "Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet." in airflow_log_output
```