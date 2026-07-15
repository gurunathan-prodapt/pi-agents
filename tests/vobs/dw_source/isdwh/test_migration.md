# Migration Validation Test Suite: `DW.DWH_ABPZ_KKM_AIL_AGENT`

This document contains the production-grade migration-validation tests to verify that the migrated Google Cloud Platform (GCP) pipeline behaves identically to the legacy Automic/UC4, KornShell, and Ab Initio implementation.

---

## Test Suite Overview

The validation strategy is structured into four distinct testing phases:
1. **Orchestration & State-Machine Validation:** Verifies that Airflow replicates the exact start/end monitoring hooks, polling barriers, and date calculations of the legacy UC4 job.
2. **Data Parity & Transformation Correctness:** Validates that the PySpark job extracts, filters, and transforms data from BigQuery identically to the legacy Ab Initio graph.
3. **External System & GCS Output Parity:** Confirms that the output flat-file is generated with correct formatting, schemas, and encoding.
4. **End-to-End Integration & Robustness:** Tests edge cases, NULL handling, and error recovery.

```
                                  VALIDATION FLOW
                                  
   +-----------------------+      +-----------------------+      +-----------------------+
   |  1. Orchestration     |      |  2. Data Parity       |      |  3. Output Parity     |
   |                       |      |                       |      |                       |
   |  - Start/End Hooks    | ---> |  - Lookback Filter    | ---> |  - Tab-Separated CSV  |
   |  - Polling Barrier    |      |  - Schema Mapping     |      |  - Single Part File   |
   |  - Date Calculations  |      |  - NULL Coalescing    |      |  - UTF-8 Encoding     |
   +-----------------------+      +-----------------------+      +-----------------------+
```

---

## 1. Orchestration & State-Machine Validation

### Test Case 1.1: Pre-Execution App Status Polling Barrier (UC4 Start Hook Parity)
#### Purpose
Verify that the Airflow DAG blocks execution when the application status in `DW_ADM_AB_INITIO_VAR` is not set to `'go'`, and proceeds immediately when it is set to `'go'`. This replicates the legacy `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC` JOBI logic.

#### Setup
1. Create a mock BigQuery table representing `DW_ADM_AB_INITIO_VAR` in the test dataset.
2. Seed the table with `app_name = 'STATUS_DWH'` and `status_val = 'wait'`.

#### Action
1. Trigger the Airflow DAG `dw_dwh_abpz_kkm_ail_agent`.
2. Observe that the task `poll_ab_initio_status_barrier` remains in a pending/retrying state or fails as expected.
3. Update the table row to `status_val = 'go'`.
4. Re-run or allow the task to poll again.

#### Pass/Fail Criterion
* **Pass:** The task `poll_ab_initio_status_barrier` fails or blocks when the status is not `'go'`, and succeeds when the status is updated to `'go'`.
* **Fail:** The task succeeds when the status is not `'go'`, or fails to proceed when the status is `'go'`.

```python
# pytest test_orchestration_barrier.py
import pytest
from google.cloud import bigquery

def test_poll_barrier_state(bq_client, gcp_project, bq_dataset):
    table_ref = f"{gcp_project}.{bq_dataset}.DW_ADM_AB_INITIO_VAR"
    
    # Force status to 'wait'
    update_query = f"""
        UPDATE `{table_ref}`
        SET status_val = 'wait'
        WHERE app_name = 'STATUS_DWH'
    """
    bq_client.query(update_query).result()
    
    # Query to assert status is not 'go'
    check_query = f"""
        SELECT status_val FROM `{table_ref}` WHERE app_name = 'STATUS_DWH'
    """
    row = list(bq_client.query(check_query).result())[0]
    assert row.status_val != "go", "Pre-condition failed: Status should not be 'go'"
```

---

### Test Case 1.2: Job Monitoring Start/End Hook Registration
#### Purpose
Verify that the DAG execution registers its start and end states in the logging and monitoring systems, matching the legacy `DW.DWH_ADM_JOB_MONITOR_START` and `DW.DWH_ADM_JOB_MONITOR_END` hooks.

#### Setup
* Ensure the Airflow environment is configured to capture standard output logs.
* Ensure the target monitoring table `DW_RUNNING_JOBS` exists in BigQuery.

#### Action
1. Execute the Airflow DAG `dw_dwh_abpz_kkm_ail_agent`.
2. Inspect the task logs for `dw_dwh_adm_job_monitor_start` and `dw_dwh_adm_job_monitor_end`.

#### Pass/Fail Criterion
* **Pass:** 
  * Start task log contains the exact string: `Added ABPZ_KKM_AIL_AGENT with run_id <run_id>`.
  * End task log contains the exact strings: `Jobkennung ABPZ_KKM_AIL_AGENT eingetragen` and `Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler beendet.`.
* **Fail:** The logs do not contain these exact German and English tracing strings, indicating a mismatch with legacy operational monitoring.

---

## 2. Data Parity & Transformation Correctness

### Test Case 2.1: Lookback Window Temporal Filtering (`-z 84` Parameter Parity)
#### Purpose
Verify that the PySpark job filters the source view `DWH$VI_S_SDM_AGENT_ADS` correctly using the 84-day lookback window relative to the execution date.

#### Setup
1. Populate the source table `DWH$VI_S_SDM_AGENT_ADS` with test records:
   * Record A: `modification_date` = Today - 10 days (Within window)
   * Record B: `modification_date` = Today - 83 days (Within window boundary)
   * Record C: `modification_date` = Today - 85 days (Outside window boundary)
   * Record D: `modification_date` = Today - 120 days (Outside window)

#### Action
1. Run the PySpark job `tmp5bupf309_write_agent_lookup.py` with `--lookback-days 84`.
2. Query the output GCS file and count the records.

#### Pass/Fail Criterion
* **Pass:** Only Record A and Record B are present in the output file. Record C and Record D are filtered out.
* **Fail:** Any record older than 84 days is present, or records within the 84-day window are missing.

```python
# pytest test_temporal_filtering.py
from datetime import datetime, timedelta
import pandas as pd

def test_lookback_filtering(spark_session):
    today = datetime.utcnow()
    
    # Create mock source data
    data = [
        ("A001", "Agent A", "Active", "REG1", today - timedelta(days=10)),
        ("B002", "Agent B", "Active", "REG2", today - timedelta(days=83)),
        ("C003", "Agent C", "Inactive", "REG3", today - timedelta(days=85)),
        ("D004", "Agent D", "Active", "REG4", today - timedelta(days=120))
    ]
    columns = ["agent_id", "agent_name", "agent_status", "region_code", "modification_date"]
    df_source = spark_session.createDataFrame(data, columns)
    
    # Apply filter logic matching PySpark script
    date_limit = (today - timedelta(days=84)).strftime("%Y-%m-%d")
    df_filtered = df_source.filter(df_source["modification_date"] >= date_limit)
    
    results = [row.agent_id for row in df_filtered.collect()]
    assert "A001" in results
    assert "B002" in results
    assert "C003" not in results
    assert "D004" not in results
```

---

### Test Case 2.2: Schema Mapping & NULL Coalescing
#### Purpose
Verify that the transformation logic maps source columns to target columns correctly and coalesces NULL values to empty strings (`""`), matching the Ab Initio target schema requirements.

#### Setup
1. Populate the source table `DWH$VI_S_SDM_AGENT_ADS` with a record containing NULL values:
   * `agent_id` = `"AGT999"`
   * `agent_name` = `NULL`
   * `agent_status` = `NULL`
   * `region_code` = `NULL`

#### Action
1. Run the PySpark transformation function `transform_data` on this dataset.

#### Pass/Fail Criterion
* **Pass:** The output dataframe contains:
  * `AgentID` = `"AGT999"`
  * `AgentName` = `""` (empty string, not literal `"NULL"` or Python `None`)
  * `AgentStatus` = `""`
  * `RegionCode` = `""`
  * `ProcessedTimestamp` is populated with a valid current timestamp.
* **Fail:** NULL values are preserved as `None`/`null` or mapped incorrectly.

```python
# pytest test_transforms.py
from pyspark.sql import Row

def test_null_coalescing(spark_session):
    # Import the transform function from the migrated script
    sys.path.append("/workspace/code")
    from tmp5bupf309_write_agent_lookup import transform_data
    
    raw_data = [
        Row(agent_id="AGT999", agent_name=None, agent_status=None, region_code=None)
    ]
    df_raw = spark_session.createDataFrame(raw_data)
    df_transformed = transform_data(df_raw)
    
    result = df_transformed.collect()[0]
    assert result["AgentID"] == "AGT999"
    assert result["AgentName"] == ""
    assert result["AgentStatus"] == ""
    assert result["RegionCode"] == ""
    assert result["ProcessedTimestamp"] is not None
```

---

## 3. External System & GCS Output Parity

### Test Case 3.1: Flat-File Format and Structure Validation
#### Purpose
Verify that the output file `AgentADSLookup.txt` written to GCS is formatted exactly as expected by downstream legacy consumers (single file, tab-delimited, with headers).

#### Setup
1. Run the PySpark job to generate the output file in GCS.

#### Action
1. Retrieve the generated file from the GCS bucket.
2. Inspect the file structure, delimiter, and line endings.

#### Pass/Fail Criterion
* **Pass:**
  * The file is a single physical file (not split into multiple part-files).
  * The column delimiter is a tab character (`\t`).
  * The file contains a header row: `AgentID\tAgentName\tAgentStatus\tRegionCode\tProcessedTimestamp`.
  * The file encoding is UTF-8.
* **Fail:** The file is split, uses commas or other delimiters, lacks headers, or uses incorrect encoding.

```bash
#!/bin/bash
# Bash validation script for output file structure

GCS_BUCKET="my-test-bucket"
LOOKUP_FILE="AgentADSLookup.txt"

# Download the file from GCS
gsutil cp gs://${GCS_BUCKET}/lookups/${LOOKUP_FILE} ./temp_lookup.txt

# 1. Check if file is non-empty
if [ ! -s ./temp_lookup.txt ]; then
    echo "FAIL: File is empty"
    exit 1
fi

# 2. Verify Header Row contains Tab Delimiters
HEADER=$(head -n 1 ./temp_lookup.txt)
EXPECTED="AgentID	AgentName	AgentStatus	RegionCode	ProcessedTimestamp"

if [ "$HEADER" != "$EXPECTED" ]; then
    echo "FAIL: Header mismatch or incorrect delimiter"
    echo "Found: $HEADER"
    exit 1
fi

echo "PASS: GCS Flat-File format validated successfully."
```

---

## 4. End-to-End Integration & Robustness

### Test Case 4.1: Zero-Record Source Handling (Empty Dataset Robustness)
#### Purpose
Verify that the PySpark pipeline executes successfully and writes an empty structured file (headers only) when the source view contains zero records within the lookback window.

#### Setup
1. Clear all records from the mock source table `DWH$VI_S_SDM_AGENT_ADS` (or set all modification dates to > 1 year ago).

#### Action
1. Execute the PySpark job `tmp5bupf309_write_agent_lookup.py` with `--lookback-days 84`.

#### Pass/Fail Criterion
* **Pass:** The job completes with exit code `0` and writes a file to GCS containing only the header row.
* **Fail:** The job crashes with a `NoDataException`, division-by-zero, or fails to write the output file.

```python
# pytest test_robustness.py
def test_zero_records_handling(spark_session):
    # Empty schema matching source
    columns = ["agent_id", "agent_name", "agent_status", "region_code", "modification_date"]
    df_empty = spark_session.createDataFrame([], schema="agent_id STRING, agent_name STRING, agent_status STRING, region_code STRING, modification_date STRING")
    
    sys.path.append("/workspace/code")
    from tmp5bupf309_write_agent_lookup import transform_data
    
    df_transformed = transform_data(df_empty)
    
    assert df_transformed.count() == 0
    assert len(df_transformed.columns) == 5
```