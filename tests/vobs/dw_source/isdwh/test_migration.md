An enterprise-grade migration validation test suite has been prepared for the **DW.DWH_ABPZ_KKM_AIL_AGENT** job. This suite is designed to verify that the migrated Cloud Composer (Airflow) and Dataproc Serverless (PySpark) pipelines are behaviorally equivalent to the legacy Ab Initio and UC4 implementations.

---

# Section 1: End-to-End Output Parity Validation

## Test Case 1.1: End-to-End Output Parity & Format Verification
### Purpose
Verify that given identical source data in BigQuery (representing the legacy Oracle view `DWH$VI_S_SDM_AGENT_ADS`) and identical execution parameters, the PySpark job produces a tab-delimited flat file (`AgentADSLookup.txt`) that matches the legacy Ab Initio output byte-for-byte (or structurally, accounting for minor CSV/TSV header differences).

### Setup
1. **Source Data Setup**: Populate a BigQuery mock table representing the source view `dw_sdm_agent_ads` with a controlled set of 1,000 records containing diverse characters, numeric IDs, and timestamps spanning the last 100 days.
2. **Legacy Baseline**: Run the legacy Ab Initio graph `BHB_CCM_PROC_WriteAgentADSLookup` with `--run_date 2023-10-15` and `--lookback_days 84` to generate the baseline file `AgentADSLookup_legacy.txt`.
3. **Target Environment**: Ensure the Airflow Variable `GCS_BUCKET` is configured to point to a test bucket (e.g., `gs://dwh_migration_test_bucket`).

### Action
1. Upload the legacy configuration file `BHB_CCM_PROC_WriteAgentADSLookup.cfg` to `gs://dwh_migration_test_bucket/config/ccm_proc/`.
2. Trigger the migrated Airflow DAG `dw_dwh_abpz_kkm_ail_agent` with the following configuration override:
   ```json
   {
     "run_date": "2023-10-15",
     "lookback_days": "84"
   }
   ```
3. Locate the generated output file in GCS: `gs://dwh_migration_test_bucket/lookups/AgentADSLookup.txt/part-*.csv` (coalesced single file).
4. Download both files and run the validation script.

### Pass/Fail Criterion
* **Pass**: The row count of both files matches exactly. The schema, column order, and data values (delimited by `\t`) are identical. Null values are represented identically.
* **Fail**: Row counts differ, column ordering is mismatched, or data values differ (e.g., timestamp formatting differences).

```python
# pytest test_output_parity.py
import pytest
import pandas as pd
from google.cloud import storage

def test_flat_file_parity():
    # Download files from GCS / Local paths
    legacy_df = pd.read_csv("AgentADSLookup_legacy.txt", sep="\t").sort_values(by="AGENT_ID").reset_index(drop=True)
    
    # Read the coalesced PySpark output from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket("dwh_migration_test_bucket")
    blobs = list(bucket.list_blobs(prefix="lookups/AgentADSLookup.txt/part-"))
    
    assert len(blobs) == 1, "PySpark output did not coalesce to a single file!"
    
    target_local_path = "/tmp/AgentADSLookup_migrated.txt"
    blobs[0].download_to_filename(target_local_path)
    
    migrated_df = pd.read_csv(target_local_path, sep="\t").sort_values(by="AGENT_ID").reset_index(drop=True)
    
    # Assert structural and value equivalence
    assert list(legacy_df.columns) == list(migrated_df.columns), "Column headers do not match!"
    assert len(legacy_df) == len(migrated_df), f"Row count mismatch! Legacy: {len(legacy_df)}, Migrated: {len(migrated_df)}"
    pd.testing.assert_frame_equal(legacy_df, migrated_df, check_dtype=False)
```

---

# Section 2: Transformation & Logic Correctness

## Test Case 2.1: Date Calculation & Lookback Window Filtering
### Purpose
Verify that the PySpark date calculation logic (`calculate_data_freshness`) correctly replaces the legacy `h_alis_date.ksh` utility and accurately filters out records outside the lookback window.

### Setup
1. Populate the mock BigQuery source view `dw_sdm_agent_ads` with records having `LAST_UPDATE_TIMESTAMP` values distributed across critical boundaries relative to the run date `2023-10-15` and lookback window of `84` days (Boundary Date: `2023-07-23`):
   * **Record A**: `LAST_UPDATE_TIMESTAMP = '2023-07-22 23:59:59'` (1 second outside window -> should be **excluded**)
   * **Record B**: `LAST_UPDATE_TIMESTAMP = '2023-07-23 00:00:00'` (Exactly on boundary -> should be **included**)
   * **Record C**: `LAST_UPDATE_TIMESTAMP = '2023-10-15 12:00:00'` (Inside window -> should be **included**)
   * **Record D**: `LAST_UPDATE_TIMESTAMP = '2023-10-16 00:00:00'` (Future date relative to run_date -> should be **included** if query only filters by `>= lookback_date`)

### Action
1. Run the PySpark job with `--run_date 2023-10-15` and `--lookback_days 84`.
2. Query the output file generated in GCS.

### Pass/Fail Criterion
* **Pass**: The output file contains exactly Records B, C, and D. Record A is successfully filtered out.
* **Fail**: Record A is present in the output, or Record B is missing, indicating a boundary calculation error (e.g., off-by-one error in date subtraction).

```python
# PySpark Unit Test for Date Calculation Logic
from datetime import datetime
import pytest
from abpz_kkm_ail_agent import calculate_data_freshness

def test_calculate_data_freshness_boundaries():
    run_date = "2023-10-15"
    lookback_days = 84
    
    dates = calculate_data_freshness(run_date, lookback_days)
    
    assert dates["execution_date_str"] == "2023-10-15"
    assert dates["lookback_date_str"] == "2023-07-23" # 15 Oct minus 84 days is 23 July
```

## Test Case 2.2: NULL Value & Special Character Handling
### Purpose
Verify that the PySpark transformation handles `NULL` values in source columns and special characters (e.g., tabs, newlines, German umlauts like `ä`, `ö`, `ü`, `ß` in `AGENT_NAME`) without corrupting the tab-delimited output structure.

### Setup
1. Populate the mock BigQuery source view `dw_sdm_agent_ads` with the following records:
   * **Record E**: `AGENT_ID = 'A001'`, `AGENT_NAME = 'Müller & Söhne GmbH'`, `AGENT_STATUS = NULL`
   * **Record F**: `AGENT_ID = 'A002'`, `AGENT_NAME = 'Agent\tWith\tTabs'`, `AGENT_STATUS = 'Active'`
   * **Record G**: `AGENT_ID = 'A003'`, `AGENT_NAME = 'Agent\nWith\nNewlines'`, `AGENT_STATUS = 'Inactive'`

### Action
1. Run the PySpark job.
2. Read the output file and verify structural integrity.

### Pass/Fail Criterion
* **Pass**: 
  * The output file maintains exactly 3 columns per row.
  * Tab characters inside `AGENT_NAME` are either escaped or quoted to prevent column shifting.
  * Newline characters do not split a single record into multiple lines.
  * German umlauts are correctly encoded in `UTF-8`.
  * `NULL` values are written as empty strings or a standard null representation matching the legacy system.
* **Fail**: The output file has misaligned columns, extra rows due to unescaped newlines, or corrupted encoding (mojibake).

---

# Section 3: External System & Configuration Replacements

## Test Case 3.1: Legacy Configuration Parsing Validation
### Purpose
Verify that the PySpark helper function `load_config_file` correctly parses the legacy Ab Initio `.cfg` file format, ignoring comments, handling whitespaces, and extracting key-value pairs accurately.

### Setup
1. Create a mock configuration file on GCS at `gs://dwh_migration_test_bucket/config/ccm_proc/test_agent.cfg` with the following content:
   ```bash
   # This is a comment
   BHB_Projektverzeichnis="/Projects/TMD/processing/BHB/CCM_PROC" 
   BHB_Graph="BHB_CCM_PROC_WriteAgentADSLookup" 

   # Optional Parameter with spaces
   BHB_Version = "RLS_BHB_nach_74_fix_20071031"
   ```

### Action
1. Execute the `load_config_file` function within a test PySpark session pointing to the mock GCS path.

### Pass/Fail Criterion
* **Pass**: The returned dictionary contains exactly the parsed keys with stripped quotes and whitespaces:
  * `configs["BHB_Projektverzeichnis"] == "/Projects/TMD/processing/BHB/CCM_PROC"`
  * `configs["BHB_Graph"] == "BHB_CCM_PROC_WriteAgentADSLookup"`
  * `configs["BHB_Version"] == "RLS_BHB_nach_74_fix_20071031"`
* **Fail**: Comments are parsed as keys, quotes are left attached to values, or keys with spaces around the `=` sign are missed.

```python
# PySpark Unit Test for Configuration Parser
from pyspark.sql import SparkSession
from abpz_kkm_ail_agent import load_config_file

def test_legacy_config_parsing():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    # Write a local temp file to simulate GCS read
    temp_cfg_path = "/tmp/test_parsing.cfg"
    with open(temp_cfg_path, "w") as f:
        f.write('# Comment\nBHB_Graph="BHB_Graph_Val"\nBHB_Version = \'Version_Val\'\n')
        
    configs = load_config_file(spark, temp_cfg_path)
    
    assert configs["BHB_Graph"] == "BHB_Graph_Val"
    assert configs["BHB_Version"] == "Version_Val"
    assert "Comment" not in configs
    spark.stop()
```

---

# Section 4: Data Quality, Schema, and Logging Assertions

## Test Case 4.1: Schema and Data Type Assertions
### Purpose
Verify that the output schema of the generated flat file matches the expected target definition and that data types are correctly cast during the BigQuery-to-GCS extraction.

### Setup
1. Ensure the BigQuery source view has columns with types: `AGENT_ID` (INTEGER), `AGENT_NAME` (STRING), `AGENT_STATUS` (STRING), `LAST_UPDATE_TIMESTAMP` (TIMESTAMP).

### Action
1. Run the PySpark job.
2. Load the output file into a Spark DataFrame and inspect the schema.

### Pass/Fail Criterion
* **Pass**: The output file columns conform to the expected layout:
  * Column 1: `AGENT_ID` (String representation of integer, no decimal points like `1.0`)
  * Column 2: `AGENT_NAME` (String)
  * Column 3: `AGENT_STATUS` (String)
  * Column 4: `LAST_UPDATE_TIMESTAMP` (Formatted as `YYYY-MM-DD HH:MM:SS` or matching legacy format)
* **Fail**: `AGENT_ID` is exported with floating-point decimals (e.g., `123.0` instead of `123`), or timestamps are exported in raw epoch milliseconds.

## Test Case 4.2: Verbatim Logging & Exit Code Verification
### Purpose
Verify that the PySpark job prints the exact legacy logging statements to stdout upon successful execution and exits with code `0`.

### Setup
1. Configure the PySpark job to run in a standard Dataproc environment.

### Action
1. Execute the PySpark job.
2. Capture the standard output (stdout) logs from the Dataproc job metadata.

### Pass/Fail Criterion
* **Pass**: 
  * The job exits with return code `0`.
  * The stdout contains the exact strings:
    * `Rueckgabewert: '0'`
    * `Der Status fuer den Pruefjob wurde erfolgreich auf BEENDET gesetzt.`
* **Fail**: The job exits with a non-zero code, or the logging statements are missing or contain typos relative to the legacy output.

```python
# Integration test to verify Dataproc stdout logs
def test_dataproc_logging_and_exit_status(google_dataproc_client, job_id):
    job = google_dataproc_client.get_job(project_id="YOUR_GCP_PROJECT_ID", region="YOUR_DATAPROC_REGION", job_id=job_id)
    
    # Verify successful execution status
    assert job.status.state.name == "DONE", f"Job failed with state: {job.status.state.name}"
    
    # Fetch driver logs from GCS staging bucket
    driver_output_uri = job.driver_output_resource_uri
    # Download and read driver_output_uri content...
    logs = download_gcs_uri_content(driver_output_uri)
    
    assert "Rueckgabewert: '0'" in logs
    assert "Der Status fuer den Pruefjob wurde erfolgreich auf BEENDET gesetzt." in logs
```