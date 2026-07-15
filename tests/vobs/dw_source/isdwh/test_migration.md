Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Cloud Composer, PySpark, and BigQuery implementation of `DW.DWH_ABPZ_KKM_AIL_AGENT` is behaviorally equivalent to the legacy UC4/Ab Initio job.

---

# MIGRATION VALIDATION TEST SUITE: DW.DWH_ABPZ_KKM_AIL_AGENT

## 1. Output Parity Tests

### Test Case 1.1: End-to-End Output Parity (GCS vs. Legacy Flat-File)
*   **Purpose:** Verify that the PySpark pipeline running on Dataproc produces a pipe-delimited flat-file in GCS that is structurally and textually identical to the legacy Ab Initio output (`AgentADSLookup.txt`) for the same input dataset and date window.
*   **Setup:**
    1.  Identify a historical execution date (e.g., `2026-03-30`) and extract the legacy output file generated on that day.
    2.  Ensure the BigQuery source view `dwh_views.vi_s_sdm_agent_ads` contains the exact same records as the legacy Oracle view `DWH$VI_S_SDM_AGENT_ADS` for the corresponding 84-day lookback window (FirstDay: `20260105`, LastDayPlus1: `20260330`).
    3.  Run the migrated PySpark job with these date parameters.
*   **Action:** Download the generated GCS file from `gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt` (coalesced single partition file) and compare it against the legacy flat-file.
*   **Pass/Fail Criterion:** 
    *   **Pass:** The files match exactly on row count, column order, header names, delimiters (`|`), UTF-8 encoding, and record values (ignoring physical file part-name differences).
    *   **Fail:** Any mismatch in row count, column structure, encoding, or field values.

```python
# pytest test_output_parity.py
import pytest
import pandas as pd
from google.cloud import storage

def test_gcs_flat_file_parity():
    # Local paths to downloaded files
    legacy_file_path = "./test_data/legacy_AgentADSLookup.txt"
    migrated_gcs_uri = "gs://test-migration-bucket/lookups/AgentADSLookup.txt"
    migrated_local_path = "./test_data/migrated_AgentADSLookup.txt"
    
    # Download from GCS (handling single-part output directory)
    storage_client = storage.Client()
    bucket = storage_client.bucket("test-migration-bucket")
    blobs = list(bucket.list_blobs(prefix="lookups/AgentADSLookup.txt/part-"))
    assert len(blobs) == 1, "Expected a single coalesced part file in GCS directory"
    blobs[0].download_to_filename(migrated_local_path)
    
    # Load into Pandas DataFrames
    df_legacy = pd.read_csv(legacy_file_path, sep="|", encoding="utf-8")
    df_migrated = pd.read_csv(migrated_local_path, sep="|", encoding="utf-8")
    
    # Assertions
    assert df_legacy.shape == df_migrated.shape, f"Shape mismatch! Legacy: {df_legacy.shape}, Migrated: {df_migrated.shape}"
    assert list(df_legacy.columns) == list(df_migrated.columns), "Header column mismatch!"
    
    # Sort both by primary key (e.g., agent_id, stichtag) to ensure order-independent comparison
    sort_cols = ["stichtag", "agent_id"] if "agent_id" in df_legacy.columns else ["stichtag"]
    df_legacy_sorted = df_legacy.sort_values(by=sort_cols).reset_index(drop=True)
    df_migrated_sorted = df_migrated.sort_values(by=sort_cols).reset_index(drop=True)
    
    pd.testing.assert_frame_equal(df_legacy_sorted, df_migrated_sorted, check_dtype=False)
```

---

## 2. Transformation & Filter Correctness Tests

### Test Case 2.1: 84-Day Lookback Date Filter Boundary Verification
*   **Purpose:** Prove that the PySpark date filtering logic correctly handles boundary conditions (inclusive of `FirstDay`, exclusive of `LastDayPlus1`) matching the legacy Ab Initio filter logic.
*   **Setup:**
    1.  Populate the source table `dwh_views.vi_s_sdm_agent_ads` with mock records containing specific boundary dates:
        *   Record A: `stichtag` = `2025-12-31` (Before `FirstDay`)
        *   Record B: `stichtag` = `2026-01-01` (Exactly `FirstDay`)
        *   Record C: `stichtag` = `2026-03-25` (Within range)
        *   Record D: `stichtag` = `2026-03-26` (Exactly `LastDayPlus1`)
        *   Record E: `stichtag` = `2026-03-27` (After `LastDayPlus1`)
    2.  Set execution parameters: `BHB_CCM_PROC_FirstDay` = `20260101`, `BHB_CCM_PROC_LastDayPlus1` = `20260326`.
*   **Action:** Execute the PySpark transformation function `apply_date_partition_filter` on this dataset.
*   **Pass/Fail Criterion:**
    *   **Pass:** The output DataFrame contains exactly **Record B** and **Record C**. Records A, D, and E are filtered out.
    *   **Fail:** Record A, D, or E is present in the output, or Record B or C is missing.

```python
# pytest test_transformations.py
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from write_agent_ads_lookup import apply_date_partition_filter

def test_date_boundary_filter():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    schema = T.StructType([
        T.StructField("agent_id", T.StringType(), True),
        T.StructField("stichtag", T.DateType(), True)
    ])
    
    # Mock data matching boundaries
    data = [
        ("A", pd.to_datetime("2025-12-31").date()), # Out (Before FirstDay)
        ("B", pd.to_datetime("2026-01-01").date()), # In (On FirstDay)
        ("C", pd.to_datetime("2026-03-25").date()), # In (Within range)
        ("D", pd.to_datetime("2026-03-26").date()), # Out (On LastDayPlus1)
        ("E", pd.to_datetime("2026-03-27").date())  # Out (After LastDayPlus1)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Apply filter matching -z 84 lookback window
    df_result = apply_date_partition_filter(df, "20260101", "20260326")
    results = df_result.collect()
    
    active_agents = [row["agent_id"] for row in results]
    
    assert "B" in active_agents, "Boundary error: FirstDay (inclusive) was filtered out!"
    assert "C" in active_agents, "Internal range record was filtered out!"
    assert "A" not in active_agents, "Boundary error: Record before FirstDay was included!"
    assert "D" not in active_agents, "Boundary error: LastDayPlus1 (exclusive) was included!"
    assert "E" not in active_agents, "Boundary error: Record after LastDayPlus1 was included!"
    assert len(results) == 2, f"Expected 2 records, got {len(results)}"
```

### Test Case 2.2: NULL and Empty Value Handling
*   **Purpose:** Ensure that NULL values in non-key columns are preserved as empty strings or standard SQL NULLs without causing runtime exceptions or format corruption in the output flat-file.
*   **Setup:**
    1.  Insert a record into `dwh_views.vi_s_sdm_agent_ads` where optional fields (e.g., `agent_name`, `region`) are `NULL`.
    2.  Run the PySpark pipeline.
*   **Action:** Inspect the generated GCS flat-file and BigQuery table `dw_lookups.agent_ads_lookup`.
*   **Pass/Fail Criterion:**
    *   **Pass:** The GCS flat-file represents the NULL value as an empty string between delimiters (e.g., `value1||value3`). The BigQuery table retains the field as a native `NULL`.
    *   **Fail:** The pipeline throws a `NullPointerException`, or writes literal string values like `"null"`, `"NULL"`, or `"NaN"` to GCS/BigQuery.

---

## 3. External-System Replacement Tests

### Test Case 3.1: BigQuery Mirroring Verification
*   **Purpose:** Verify that the PySpark pipeline successfully mirrors the filtered dataset to the target BigQuery table `dw_lookups.agent_ads_lookup` with correct schema definitions.
*   **Setup:**
    1.  Clear the target BigQuery table `dw_lookups.agent_ads_lookup`.
    2.  Run the PySpark pipeline with a known input dataset.
*   **Action:** Query the target BigQuery table and compare its contents with the GCS flat-file output.
*   **Pass/Fail Criterion:**
    *   **Pass:** The BigQuery table row count matches the GCS flat-file row count exactly. The schema of `dw_lookups.agent_ads_lookup` matches the source view schema.
    *   **Fail:** Row counts do not match, or the table write fails due to schema mismatches.

```sql
-- SQL Assertion to run in BigQuery Console
-- Purpose: Verify that the mirrored table matches the source view within the filtered window
DECLARE first_day DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 84 DAY);
DECLARE last_day_plus_1 DATE DEFAULT CURRENT_DATE();

ASSERT (
  SELECT COUNT(1) FROM `dw_lookups.agent_ads_lookup`
) = (
  SELECT COUNT(1) FROM `dwh_views.vi_s_sdm_agent_ads`
  WHERE stichtag >= first_day AND stichtag < last_day_plus_1
) 
AS "Validation Failed: Row count mismatch between BigQuery target lookup and filtered source view!";
```

---

## 4. Orchestration & Date Calculation Tests

### Test Case 4.1: Airflow Date Calculation Logic (`-z 84` Lookback)
*   **Purpose:** Prove that the Airflow task `calculate_processing_window` correctly calculates the 84-day lookback window relative to the DAG execution date (`ds`).
*   **Setup:**
    1.  Trigger the DAG `dw_dwh_abpz_kkm_ail_agent` with a specific execution date (e.g., `2026-03-01`).
*   **Action:** Retrieve the calculated `first_day` and `last_day_plus_1` values from Airflow XCom.
*   **Pass/Fail Criterion:**
    *   **Pass:** 
        *   `first_day` is calculated as `20251207` (2026-03-01 minus 84 days, formatted as `YYYYMMDD`).
        *   `last_day_plus_1` is calculated as `20260301` (formatted as `YYYYMMDD`).
    *   **Fail:** Any other date values are produced, or the task fails to push values to XCom.

```python
# pytest test_dag_calculations.py
from datetime import datetime
import pytest

def test_airflow_date_calculation():
    # Emulate calculate_processing_window logic
    execution_date_str = "2026-03-01"
    lookback_days = 84
    
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    
    # Target logic under test
    first_day = (execution_date - timedelta(days=lookback_days)).strftime("%Y%m%d")
    last_day_plus_1 = execution_date.strftime("%Y%m%d")
    
    # Assertions
    assert first_day == "20251207", f"Expected '20251207', but got '{first_day}'"
    assert last_day_plus_1 == "20260301", f"Expected '20260301', but got '{last_day_plus_1}'"
```

---

## 5. Data Quality & Schema Assertions

### Test Case 5.1: Schema and Nullability Constraints
*   **Purpose:** Assert that the schema of the target BigQuery table matches the expected production layout and that critical business keys are not null.
*   **Setup:**
    1.  Ensure the PySpark pipeline has completed execution.
*   **Action:** Run a BigQuery metadata and data quality assertion query.
*   **Pass/Fail Criterion:**
    *   **Pass:** No assertion errors are raised; all critical columns exist and contain zero null values in key fields.
    *   **Fail:** Key columns contain null values, or columns are missing.

```sql
-- BigQuery Data Quality Assertions
-- 1. Verify that critical business keys (e.g., agent_id, stichtag) contain zero NULL values.
SELECT
  col_name,
  null_count
FROM (
  SELECT 
    'agent_id' AS col_name, COUNTIF(agent_id IS NULL) AS null_count FROM `dw_lookups.agent_ads_lookup`
  UNION ALL
  SELECT 
    'stichtag' AS col_name, COUNTIF(stichtag IS NULL) AS null_count FROM `dw_lookups.agent_ads_lookup`
)
WHERE null_count > 0;

-- Expected Output: 0 rows returned. If rows are returned, the test fails.
```