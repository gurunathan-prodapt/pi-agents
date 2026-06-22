As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `EXIS_SD_APT_NNA_VOIC` job migration. These tests aim to ensure behavioral equivalence between the legacy Oracle/UC4 system and the new BigQuery/Airflow/GCS/SFTP implementation.

The tests are categorized according to the requirements: Output Parity, Transformation Correctness, External-System Replacements, and Data Quality/Schema Assertions.

---

## Migration Validation Tests: EXIS_SD_APT_NNA_VOIC

### 1. Output Parity Tests

#### Test Case 1.1: End-to-End Output File Content Comparison

*   **Purpose**: To verify that the final gzipped CSV file produced by the migrated Airflow DAG is byte-for-byte identical (or content-identical after sorting) to the file produced by the legacy UC4 job, given the same input data. This is the ultimate proof of behavioral equivalence.
*   **Setup**:
    1.  **Legacy Environment**: Ensure the legacy Oracle DWH tables are populated with a representative dataset for a specific `MONATS_ID` (e.g., `202301`).
    2.  **Migrated Environment**: Ensure the corresponding BigQuery `raw_dwh` tables are populated with an *identical* dataset for the same `MONATS_ID`.
    3.  **Execution**:
        *   Execute the legacy UC4 job for the chosen `MONATS_ID`, capturing its final gzipped CSV output file (e.g., `legacy_output_202301.csv.gz`).
        *   Execute the `dwh_exis_sd_apt_nna_voic_dag` Airflow DAG for an execution date corresponding to the chosen `MONATS_ID` (e.g., `2023-01-01`), ensuring it completes successfully and the file is transferred to the SFTP target. Retrieve this file (e.g., `migrated_output_202301.csv.gz`).
*   **Action**:
    1.  Decompress both `legacy_output_202301.csv.gz` and `migrated_output_202301.csv.gz`.
    2.  If the order of rows is not guaranteed to be identical (which is common in distributed systems), sort both decompressed CSV files by all columns.
    3.  Compare the content of the two (potentially sorted) CSV files.
*   **Pass/Fail Criterion**: The content of the decompressed (and optionally sorted) CSV files must be identical. Any difference in data, column order, or formatting (e.g., number of decimal places, NULL representation) constitutes a failure.

```python
# Example Python (pytest) assertion for content comparison
import gzip
import pandas as pd
from io import StringIO

def test_output_file_parity(legacy_file_path, migrated_file_path):
    """
    Compares the content of two gzipped CSV files.
    Assumes files are gzipped and comma-separated.
    Sorts dataframes to handle potential row order differences.
    """
    def load_and_sort_csv(file_path):
        with gzip.open(file_path, 'rt') as f:
            df = pd.read_csv(f)
        # Sort by all columns to ensure consistent comparison regardless of row order
        return df.sort_values(by=list(df.columns)).reset_index(drop=True)

    legacy_df = load_and_sort_csv(legacy_file_path)
    migrated_df = load_and_sort_csv(migrated_file_path)

    pd.testing.assert_frame_equal(legacy_df, migrated_df, check_dtype=True, check_exact=False, rtol=1e-5)
    # check_exact=False and rtol for floating point comparisons if needed
    # Adjust rtol based on expected precision differences (e.g., Oracle vs BigQuery rounding)

# To run this test:
# pytest --legacy-file=path/to/legacy_output.csv.gz --migrated-file=path/to/migrated_output.csv.gz
# (You'd need to set up pytest fixtures or command-line arguments to pass file paths)
```

---

### 2. Transformation Correctness Tests

#### Test Case 2.1: Join Logic Verification

*   **Purpose**: To ensure all join conditions (`TRF.TARIF_ID=D.TARIF_ID`, `TAR.DWH_TARIF_ID=VER.DWH_TARIF_ID`, `VER.DWH_VERTRAG_ID=NNA.DWH_VERTRAG_ID`, `NNA.LEISTUNGSKLASSE_ID=TVD.LEISTUNGSKLASSE_ID`) are correctly translated from Oracle to BigQuery SQL and produce the same intermediate result sets.
*   **Setup**:
    1.  Populate the BigQuery `raw_dwh` tables (`VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `VI_C_VERTRAG`, `VI_F_NNV_TVD_12_MONATE`, `VI_L_TVD_LEISTUNGSKLASSE`) with a small, controlled dataset that includes:
        *   Rows with matching join keys across all tables.
        *   Rows with non-matching join keys (to test inner join behavior).
        *   Rows with NULL join keys (if applicable to the schema).
    2.  For the legacy system, execute the Oracle SQL query step-by-step (or equivalent subqueries) to capture intermediate results after each join.
*   **Action**:
    1.  Execute the BigQuery SQL query (`d_exis_apt_nna_voice.bq.sql`) in stages, or construct subqueries that mimic the join steps.
    2.  For each join stage, compare the row count and a sample of key columns from the BigQuery output with the corresponding Oracle intermediate results.
*   **Pass/Fail Criterion**:
    *   The row count after each join step in BigQuery must match the row count from the equivalent Oracle join step.
    *   A spot check of key columns (e.g., `DWH_TARIF_ID`, `DWH_VERTRAG_ID`, `LEISTUNGSKLASSE_ID`) in the joined results must confirm correct matching.

```sql
-- Example BigQuery SQL assertion for a specific join step (e.g., TAR to VER)
-- This would be part of a larger test script or manual verification.
-- Assume @FROM_YYYYMM is set for the test.

-- Step 1: Get expected count from Oracle (manual or via legacy query)
-- SELECT COUNT(*) FROM (
--   SELECT TRF.DWH_TARIF_ID, TRF.TARIF_ID, D.MP_MARKTPRODUKT_BEZ, D.MP_EG_JN_BEZ, D.MP_GENERATION_BEZ, TRF.GUELTIG_BIS, D.MP_EG_JN_ID, D.MP_GENERATION_ID
--   FROM DWH$VI_L_MAP_FA_TARIF TRF, BL_D_TARIF D
--   WHERE TRF.TARIF_ID=D.TARIF_ID
-- ) TAR_LEGACY, DWH$VI_C_VERTRAG VER_LEGACY
-- WHERE TAR_LEGACY.DWH_TARIF_ID = VER_LEGACY.DWH_TARIF_ID
-- AND VER_LEGACY.DWH_VERTRAG_ID IN (SELECT DWH_VERTRAG_ID FROM DWH$VI_F_NNV_TVD_12_MONATE WHERE MONATS_ID = <FROM YYYYMM>);

-- Step 2: Query BigQuery for the equivalent join and count
SELECT
  COUNT(*)
FROM (
  SELECT
    TRF.DWH_TARIF_ID,
    TRF.TARIF_ID,
    D.MP_MARKTPRODUKT_BEZ,
    D.MP_EG_JN_BEZ,
    D.MP_GENERATION_BEZ,
    TRF.GUELTIG_BIS,
    D.MP_EG_JN_ID,
    D.MP_GENERATION_ID
  FROM `raw_dwh.VI_L_MAP_FA_TARIF` TRF
  JOIN `raw_dwh.BL_D_TARIF` D
    ON TRF.TARIF_ID = D.TARIF_ID
) TAR
JOIN `raw_dwh.VI_C_VERTRAG` VER
  ON TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
WHERE VER.DWH_VERTRAG_ID IN (SELECT DWH_VERTRAG_ID FROM `raw_dwh.VI_F_NNV_TVD_12_MONATE` WHERE MONATS_ID = CAST(@FROM_YYYYMM AS INT64));

-- Pass/Fail: The COUNT(*) from BigQuery must match the count from Oracle.
```

#### Test Case 2.2: Filter Logic Verification

*   **Purpose**: To validate that all `WHERE` clause conditions are correctly applied in the BigQuery SQL, including `NNA.RAHMENVERTRAG IS NOT NULL`, `NNA.MONATS_ID = CAST(@FROM_YYYYMM AS INT64)`, `TAR.GUELTIG_BIS = DATE '4712-12-31'`, and the complex `LEISTUNGSKLASSE_ID` logic.
*   **Setup**:
    1.  Populate BigQuery `raw_dwh` tables with a dataset that specifically tests each filter condition:
        *   Rows where `RAHMENVERTRAG` is NULL and not NULL.
        *   Rows with `MONATS_ID` matching and not matching the parameter.
        *   Rows with `GUELTIG_BIS` as `4712-12-31` and other dates.
        *   Rows covering all branches of the `LEISTUNGSKLASSE_ID` logic (e.g., `LEISTUNGSKLASSEGR_ID = 1` with `LEISTUNGSKLASSE_ID` < 300, > 399, and between 300-399; `LENGTH(TRIM(...)) = 6` with various `LEISTUNGSKLASSE_ID` values including `622xxx`).
    2.  Define a specific `MONATS_ID` parameter for the test.
*   **Action**:
    1.  Execute the full BigQuery SQL query (`d_exis_apt_nna_voice.bq.sql`) with the defined `MONATS_ID`.
    2.  Query the BigQuery tables directly to identify which rows *should* be included based on the filter logic.
    3.  Compare the output of the BigQuery query with the expected set of rows.
*   **Pass/Fail Criterion**: Only rows that satisfy *all* filter conditions are present in the query result. Rows designed to be excluded by any filter must not appear.

```sql
-- Example BigQuery SQL assertion for filter logic (simplified for illustration)
-- This would be part of a test suite, comparing actual vs. expected row IDs.
-- Assume @FROM_YYYYMM = 202301

-- Expected rows (derived from manual analysis of test data and filter logic)
CREATE TEMPORARY TABLE ExpectedFilteredRows (
  MONATS_ID INT64,
  RAHMENVERTRAG STRING,
  DWH_VERTRAG_ID INT64,
  LEISTUNGSKLASSE_ID INT64,
  GUELTIG_BIS DATE
);
INSERT INTO ExpectedFilteredRows VALUES
  (202301, 'RV1', 101, 100, DATE '4712-12-31'), -- Passes all filters
  (202301, 'RV2', 102, 600001, DATE '4712-12-31'); -- Passes complex LEISTUNGSKLASSE_ID filter

-- Actual rows from the migrated query
CREATE TEMPORARY TABLE ActualFilteredRows AS
SELECT
  NNA.MONATS_ID,
  NNA.RAHMENVERTRAG,
  VER.DWH_VERTRAG_ID, -- Include DWH_VERTRAG_ID for easier comparison
  TVD.LEISTUNGSKLASSE_ID,
  TAR.GUELTIG_BIS
FROM (
  SELECT TRF.DWH_TARIF_ID, TRF.TARIF_ID, TRF.GUELTIG_BIS FROM `raw_dwh.VI_L_MAP_FA_TARIF` TRF JOIN `raw_dwh.BL_D_TARIF` D ON TRF.TARIF_ID = D.TARIF_ID
) TAR
JOIN `raw_dwh.VI_C_VERTRAG` VER ON TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
JOIN `raw_dwh.VI_F_NNV_TVD_12_MONATE` NNA ON VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
JOIN `raw_dwh.VI_L_TVD_LEISTUNGSKLASSE` TVD ON NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
WHERE NNA.RAHMENVERTRAG IS NOT NULL
  AND NNA.MONATS_ID = CAST(202301 AS INT64) -- Hardcode for test
  AND TAR.GUELTIG_BIS = DATE '4712-12-31'
  AND (
    (TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))
    OR (
      LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
      AND TVD.LEISTUNGSKLASSE_ID < 699999
      AND CAST(FLOOR(CAST(TVD.LEISTUNGSKLASSE_ID AS NUMERIC) / 1000) AS INT64) <> 622
    )
  );

-- Assertion: Check if actual and expected rows are identical
SELECT
  (SELECT COUNT(*) FROM ActualFilteredRows) = (SELECT COUNT(*) FROM ExpectedFilteredRows) AS row_count_match,
  (SELECT COUNT(*) FROM ActualFilteredRows EXCEPT DISTINCT SELECT * FROM ExpectedFilteredRows) = 0 AS no_extra_rows,
  (SELECT COUNT(*) FROM ExpectedFilteredRows EXCEPT DISTINCT SELECT * FROM ActualFilteredRows) = 0 AS no_missing_rows;

-- Pass if all three boolean results are TRUE.
```

#### Test Case 2.3: Column Transformation and Type Handling

*   **Purpose**: To verify that calculated columns (`TARIF`, `DAUER_MIN`, `RBETRAG_VBUD_NETTO_EURO`) and data type conversions (`CONCAT`, `ROUND`, `CAST`, `LENGTH`, `TRIM`, `FLOOR`) are correctly implemented and produce numerically identical or equivalent results.
*   **Setup**:
    1.  Populate BigQuery `raw_dwh` tables with a dataset that includes:
        *   Values for `MP_MARKTPRODUKT_BEZ`, `MP_EG_JN_BEZ`, `MP_GENERATION_BEZ` that test `CONCAT` (including NULLs).
        *   Values for `DAUER_SEK` and `RBETRAG_VBUD_NETTO_CENT` that test `ROUND` and division (e.g., integers, decimals, large numbers, zero, NULLs).
        *   Values for `LEISTUNGSKLASSE_ID` that test `CAST` to `STRING`, `NUMERIC`, `INT64`, `LENGTH`, `TRIM`, and `FLOOR`.
    2.  For the legacy system, execute the Oracle SQL query and capture the output for these specific columns.
*   **Action**:
    1.  Execute the BigQuery SQL query (`d_exis_apt_nna_voice.bq.sql`).
    2.  Compare the values of `TARIF`, `DAUER_MIN`, `RBETRAG_VBUD_NETTO_EURO`, and the intermediate calculations for `LEISTUNGSKLASSE_ID` from the BigQuery output with the corresponding values from the Oracle output.
*   **Pass/Fail Criterion**:
    *   `TARIF` column values must be identical (including comma separators and NULL handling).
    *   `DAUER_MIN` and `RBETRAG_VBUD_NETTO_EURO` values must be numerically equivalent, respecting the specified rounding to 2 decimal places. Minor floating-point differences due to different underlying arithmetic engines might be acceptable within a defined tolerance (e.g., `rtol=1e-5`).
    *   The logic for `LEISTUNGSKLASSE_ID` transformations within the `WHERE` clause must yield the same boolean outcome as Oracle.

```sql
-- Example BigQuery SQL assertion for specific column transformations
-- This would be part of a test script, comparing actual vs. expected values for a given row.
-- Assume a specific row identified by a unique key (e.g., DWH_VERTRAG_ID)

SELECT
  (SELECT CONCAT(MP_MARKTPRODUKT_BEZ, ',', MP_EG_JN_BEZ, ',', MP_GENERATION_BEZ) FROM `raw_dwh.BL_D_TARIF` WHERE TARIF_ID = <test_tarif_id>) AS expected_tarif_concat,
  (SELECT ROUND(CAST(DAUER_SEK AS NUMERIC) / 60, 2) FROM `raw_dwh.VI_F_NNV_TVD_12_MONATE` WHERE DWH_VERTRAG_ID = <test_vertrag_id>) AS expected_dauer_min,
  (SELECT ROUND(CAST(RBETRAG_VBUD_NETTO_CENT AS NUMERIC) / 100, 2) FROM `raw_dwh.VI_F_NNV_TVD_12_MONATE` WHERE DWH_VERTRAG_ID = <test_vertrag_id>) AS expected_rbetrag_euro,
  -- For LEISTUNGSKLASSE_ID complex logic, you'd test the boolean outcome for specific values
  (SELECT
    (TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))
    OR (
      LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
      AND TVD.LEISTUNGSKLASSE_ID < 699999
      AND CAST(FLOOR(CAST(TVD.LEISTUNGSKLASSE_ID AS NUMERIC) / 1000) AS INT64) <> 622
    )
  FROM `raw_dwh.VI_L_TVD_LEISTUNGSKLASSE` TVD WHERE LEISTUNGSKLASSE_ID = <test_lk_id>) AS expected_lk_filter_result;

-- Compare these results against the corresponding values from the legacy system for the same input.
```

#### Test Case 2.4: NULL Handling

*   **Purpose**: To ensure NULL values are handled consistently between Oracle and BigQuery in expressions, concatenations, and filter conditions.
*   **Setup**:
    1.  Populate BigQuery `raw_dwh` tables with data where:
        *   `RAHMENVERTRAG` is NULL for some rows.
        *   `MP_MARKTPRODUKT_BEZ`, `MP_EG_JN_BEZ`, `MP_GENERATION_BEZ` contain NULLs in various combinations.
        *   `DAUER_SEK` or `RBETRAG_VBUD_NETTO_CENT` are NULL.
    2.  For the legacy system, execute the Oracle SQL query with similar NULL data and capture the output.
*   **Action**:
    1.  Execute the BigQuery SQL query (`d_exis_apt_nna_voice.bq.sql`).
    2.  Compare the output rows containing NULLs in the BigQuery result with the Oracle output.
*   **Pass/Fail Criterion**:
    *   Rows with `RAHMENVERTRAG IS NULL` must be excluded from the output, matching Oracle's behavior.
    *   `CONCAT` function behavior with NULLs must be identical (BigQuery's `CONCAT` ignores NULLs, which is generally desired and often matches Oracle's `||` behavior unless `CONCAT` is explicitly used in Oracle).
    *   `ROUND` and arithmetic operations with NULL inputs must result in NULL outputs, matching standard SQL behavior.

#### Test Case 2.5: Edge Cases (LEISTUNGSKLASSE_ID Complex Filter)

*   **Purpose**: To specifically test the complex `LEISTUNGSKLASSE_ID` filter logic, which involves multiple conditions, `LENGTH`, `TRIM`, `CAST`, and `FLOOR`.
*   **Setup**:
    1.  Populate `raw_dwh.VI_L_TVD_LEISTUNGSKLASSE` with specific `LEISTUNGSKLASSE_ID` and `LEISTUNGSKLASSEGR_ID` values that hit all branches of the filter:
        *   `LEISTUNGSKLASSEGR_ID = 1` and `LEISTUNGSKLASSE_ID = 100` (pass)
        *   `LEISTUNGSKLASSEGR_ID = 1` and `LEISTUNGSKLASSE_ID = 400` (pass)
        *   `LEISTUNGSKLASSEGR_ID = 1` and `LEISTUNGSKLASSE_ID = 350` (fail)
        *   `LEISTUNGSKLASSEGR_ID = 2` and `LEISTUNGSKLASSE_ID = 600001` (pass, length 6, < 699999, not 622xxx)
        *   `LEISTUNGSKLASSEGR_ID = 2` and `LEISTUNGSKLASSE_ID = 622001` (fail, `FLOOR(ID/1000) = 622`)
        *   `LEISTUNGSKLASSEGR_ID = 2` and `LEISTUNGSKLASSE_ID = 699999` (fail, not < 699999)
        *   `LEISTUNGSKLASSEGR_ID = 2` and `LEISTUNGSKLASSE_ID = 12345` (fail, length not 6)
*   **Action**:
    1.  Execute the BigQuery SQL query (`d_exis_apt_nna_voice.bq.sql`) with the test data.
    2.  Verify which rows are included in the final output.
*   **Pass/Fail Criterion**: Only rows whose `LEISTUNGSKLASSE_ID` and `LEISTUNGSKLASSEGR_ID` values satisfy the complex filter logic are present in the output.

---

### 3. External-System Replacements Tests

#### Test Case 3.1: BigQuery Data Extraction and Temporary Table Creation

*   **Purpose**: To verify that the `BigQueryExecuteQueryOperator` correctly executes the BigQuery SQL query and creates the temporary destination table as expected.
*   **Setup**:
    1.  Ensure BigQuery `raw_dwh` tables are populated with test data.
    2.  Set up an Airflow environment with the `dwh_exis_sd_apt_nna_voic_dag.py` deployed.
    3.  Define a specific execution date for the DAG (e.g., `2023-01-01`).
*   **Action**:
    1.  Trigger the `dwh_exis_sd_apt_nna_voic_dag` in Airflow.
    2.  Monitor the `extract_voice_data_from_bigquery` task.
    3.  After the task completes, query BigQuery's `INFORMATION_SCHEMA` or directly query the temporary table (`temp_nna_voice_export_table_YYYYMMDD`) to check its existence, schema, and row count.
*   **Pass/Fail Criterion**:
    *   The `extract_voice_data_from_bigquery` task completes successfully.
    *   A temporary BigQuery table named `temp_nna_voice_export_table_YYYYMMDD` (where YYYYMMDD corresponds to the execution date) is created in the specified dataset.
    *   The schema of the temporary table matches the expected output schema of the SQL query.
    *   The row count of the temporary table matches the expected number of rows based on the SQL query's logic and input data.

```python
# Example pytest assertion for BigQuery temporary table existence and row count
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_bigquery_extract_task_output(bq_client, airflow_dag_run_id, expected_row_count):
    """
    Verifies the temporary BigQuery table created by the extract task.
    """
    execution_date_nodash = "20230101" # Example, should be derived from dag_run_id or test context
    temp_table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.temp_nna_voice_export_table_{execution_date_nodash}"

    # Wait for Airflow task to complete (this part would be external to pytest, e.g., using Airflow API)
    # For a direct test, you might run the BQ query directly and assert.

    # Check table existence
    try:
        bq_client.get_table(temp_table_id)
        table_exists = True
    except Exception:
        table_exists = False
    assert table_exists, f"Temporary BigQuery table {temp_table_id} was not created."

    # Check row count
    query_job = bq_client.query(f"SELECT COUNT(*) FROM `{temp_table_id}`")
    result = query_job.result()
    actual_row_count = next(result)[0]
    assert actual_row_count == expected_row_count, \
        f"Row count mismatch in {temp_table_id}. Expected: {expected_row_count}, Actual: {actual_row_count}"

    # Optional: Check schema (e.g., column names and types)
    table = bq_client.get_table(temp_table_id)
    expected_schema = [
        bigquery.SchemaField("MONATS_ID", "INT64"),
        bigquery.SchemaField("RAHMENVERTRAG", "STRING"),
        # ... add all expected fields
    ]
    assert len(table.schema) == len(expected_schema)
    for expected_field in expected_schema:
        assert any(f.name == expected_field.name and f.field_type == expected_field.field_type for f in table.schema), \
            f"Schema mismatch for field {expected_field.name}"

```

#### Test Case 3.2: GCS Export Functionality

*   **Purpose**: To verify that the `BigQueryToGCSOperator` correctly exports the data from the temporary BigQuery table to GCS as a gzipped CSV file with the specified format.
*   **Setup**:
    1.  The `extract_voice_data_from_bigquery` task has successfully created the temporary BigQuery table.
    2.  Airflow environment is configured.
*   **Action**:
    1.  Trigger the `dwh_exis_sd_apt_nna_voic_dag` in Airflow.
    2.  Monitor the `export_bigquery_to_gcs` task.
    3.  After the task completes, use `gsutil` or the Google Cloud Console to verify the existence and properties of the exported file in GCS.
*   **Pass/Fail Criterion**:
    *   The `export_bigquery_to_gcs` task completes successfully.
    *   A gzipped CSV file exists in the specified GCS bucket (`gs://your-gcs-export-bucket/dwhm_apt_nna_voice_YYYYMM.csv.gz`) with the correct naming convention.
    *   The file is indeed gzipped and contains valid CSV data with a header and comma delimiter.
    *   The row count within the GCS file matches the row count of the source BigQuery temporary table.

```python
# Example Python (pytest) assertion for GCS file existence and properties
import pytest
from google.cloud import storage
import gzip
import csv
from io import BytesIO

@pytest.fixture(scope="module")
def gcs_client():
    return storage.Client()

def test_gcs_export_file_properties(gcs_client, gcs_bucket_name, expected_gcs_object_name, expected_row_count):
    """
    Verifies the GCS exported file exists, is gzipped, and has correct content.
    """
    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob(expected_gcs_object_name)

    assert blob.exists(), f"GCS object {expected_gcs_object_name} does not exist in bucket {gcs_bucket_name}"

    # Download and inspect content
    downloaded_blob = blob.download_as_bytes()
    
    # Check if it's gzipped
    try:
        with gzip.open(BytesIO(downloaded_blob), 'rt') as f:
            csv_content = f.read()
        is_gzipped = True
    except Exception:
        is_gzipped = False
    assert is_gzipped, f"GCS object {expected_gcs_object_name} is not a valid gzipped file."

    # Check CSV header and row count
    reader = csv.reader(StringIO(csv_content))
    header = next(reader)
    actual_row_count = sum(1 for row in reader) # Count data rows after header

    expected_header = ["MONATS_ID", "RAHMENVERTRAG", "MSISDN", ...] # Define expected header
    assert header == expected_header, f"CSV header mismatch. Expected: {expected_header}, Actual: {header}"
    assert actual_row_count == expected_row_count, \
        f"Row count mismatch in GCS file. Expected: {expected_row_count}, Actual: {actual_row_count}"

```

#### Test Case 3.3: SFTP Transfer Functionality (`sftp_exporter.py`)

*   **Purpose**: To verify that the `sftp_exporter.py` script, invoked by the `PythonOperator`, correctly downloads the gzipped CSV from GCS and uploads it to the external SFTP server.
*   **Setup**:
    1.  The `export_bigquery_to_gcs` task has successfully placed a gzipped CSV in GCS.
    2.  A **mock SFTP server** (e.g., using `paramiko.SFTPClient.open_sftp_server` or a dedicated test SFTP server) is running and configured to accept connections with the specified credentials.
    3.  Airflow connection details for SFTP are correctly configured (e.g., `SFTP_HOST`, `SFTP_PORT`, `SFTP_USERNAME`, `SFTP_PASSWORD`).
*   **Action**:
    1.  Trigger the `dwh_exis_sd_apt_nna_voic_dag` in Airflow.
    2.  Monitor the `sftp_transfer_to_external_system` task.
    3.  After the task completes, inspect the remote directory on the mock SFTP server to confirm the file's presence, name, and content.
*   **Pass/Fail Criterion**:
    *   The `sftp_transfer_to_external_system` task completes successfully.
    *   The gzipped CSV file is found on the mock SFTP server at the specified `sftp_remote_path` with the correct `sftp_filename`.
    *   The content of the file on the SFTP server is identical to the file in GCS.

```python
# Example Python (pytest) for SFTP transfer verification
import pytest
import paramiko
import os
import tempfile
from unittest.mock import patch, MagicMock

# Mock sftp_transfer_gcs_file to run locally for testing purposes
# In a real Airflow test, you'd observe the actual SFTP server.

@patch('sftp_exporter.storage.Client')
@patch('sftp_exporter.paramiko.Transport')
def test_sftp_transfer_success(mock_transport, mock_gcs_client):
    """
    Tests the sftp_transfer_gcs_file function with mocked GCS and SFTP.
    """
    # Setup mock GCS download
    mock_blob = MagicMock()
    mock_blob.download_to_filename.side_effect = lambda path: open(path, 'wb').write(b'gzipped_csv_content')
    mock_gcs_client.return_value.bucket.return_value.blob.return_value = mock_blob

    # Setup mock SFTP upload
    mock_sftp_client = MagicMock()
    mock_transport_instance = MagicMock()
    mock_transport.return_value = mock_transport_instance
    mock_transport_instance.connect.return_value = None
    mock_sftp_client.from_transport.return_value = mock_sftp_client

    # Define test parameters
    gcs_bucket = "test-gcs-bucket"
    gcs_object_name = "test_data.csv.gz"
    sftp_host = "sftp.example.com"
    sftp_port = 22
    sftp_username = "testuser"
    sftp_password = "testpassword"
    sftp_remote_path = "/uploads"
    sftp_filename = "uploaded_test_data.csv.gz"

    from sftp_exporter import sftp_transfer_gcs_file
    sftp_transfer_gcs_file(
        gcs_bucket=gcs_bucket,
        gcs_object_name=gcs_object_name,
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_username=sftp_username,
        sftp_password=sftp_password,
        sftp_remote_path=sftp_remote_path,
        sftp_filename=sftp_filename,
    )

    # Assertions
    mock_gcs_client.return_value.bucket.assert_called_with(gcs_bucket)
    mock_gcs_client.return_value.bucket.return_value.blob.assert_called_with(gcs_object_name)
    mock_blob.download_to_filename.assert_called_once()

    mock_transport.assert_called_with((sftp_host, sftp_port))
    mock_transport_instance.connect.assert_called_with(username=sftp_username, password=sftp_password)
    mock_sftp_client.from_transport.assert_called_with(mock_transport_instance)
    
    # Verify the put call with the correct remote path
    expected_remote_full_path = os.path.join(sftp_remote_path, sftp_filename).replace("\\", "/")
    mock_sftp_client.put.assert_called_once()
    assert mock_sftp_client.put.call_args[0][1] == expected_remote_full_path
    
    mock_sftp_client.close.assert_called_once()
    mock_transport_instance.close.assert_called_once()

```

#### Test Case 3.4: SFTP Authentication and Error Handling

*   **Purpose**: To ensure the `sftp_exporter.py` script handles various SFTP authentication scenarios (password, key) and gracefully fails with appropriate logging for invalid credentials or connection issues.
*   **Setup**:
    1.  A gzipped CSV file is available in GCS.
    2.  A mock SFTP server is configured.
    3.  Prepare Airflow connections or `op_kwargs` with:
        *   Valid username/password.
        *   Invalid username/password.
        *   Valid username/key file (if using key-based auth).
        *   Invalid key file path.
        *   SFTP host unreachable.
*   **Action**:
    1.  Run the `sftp_transfer_to_external_system` task with each of the setup scenarios.
    2.  Monitor Airflow task logs for success/failure and error messages.
*   **Pass/Fail Criterion**:
    *   With valid credentials, the task succeeds, and the file is transferred.
    *   With invalid credentials or unreachable host, the task fails, and informative error messages are logged (e.g., `Authentication failed`, `Connection refused`).
    *   The `sftp_exporter.py` script raises an appropriate exception that causes the Airflow task to fail.

---

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Output File Schema and Header

*   **Purpose**: To verify that the exported CSV file has the correct header row and that the column order matches the `SELECT` statement in the BigQuery SQL.
*   **Setup**:
    1.  Run the full `dwh_exis_sd_apt_nna_voic_dag` Airflow DAG successfully.
    2.  Retrieve the final gzipped CSV file from the SFTP target or GCS.
*   **Action**:
    1.  Decompress the CSV file.
    2.  Read the first line (header) of the CSV.
    3.  Compare it against the explicitly defined list of expected column names in the correct order.
*   **Pass/Fail Criterion**: The header row of the exported CSV must exactly match the expected column names and their order as defined in the `SELECT` clause of `d_exis_apt_nna_voice.bq.sql`.

```python
# Example Python (pytest) assertion for CSV header
import gzip
import csv
from io import StringIO

def test_csv_header_correctness(migrated_file_path):
    """
    Verifies the header of the gzipped CSV output file.
    """
    expected_header = [
        "MONATS_ID", "RAHMENVERTRAG", "MSISDN", "KUNDENKONTO", "T_MOBILE_KUNDENNUMMER",
        "TARIF_ID", "TARIF", "LEISTUNGSKLASSE_ID", "LEISTUNGSKLASSE_TEXT",
        "VERBINDUNGEN", "DAUER_MIN", "RBETRAG_VBUD_NETTO_EURO",
        "MP_EG_JN_ID", "MP_EG_JN_BEZ", "MP_GENERATION_ID", "MP_GENERATION_BEZ"
    ]

    with gzip.open(migrated_file_path, 'rt') as f:
        reader = csv.reader(f)
        actual_header = next(reader) # Read the first row as header

    assert actual_header == expected_header, \
        f"CSV header mismatch. Expected: {expected_header}, Actual: {actual_header}"

```

#### Test Case 4.2: Row Count Parity

*   **Purpose**: To ensure the total number of records exported by the new system is identical to the number of records exported by the legacy system for the same input data and parameters.
*   **Setup**:
    1.  Run both legacy and migrated jobs with identical input data and `MONATS_ID` (as in Test Case 1.1).
    2.  Retrieve the final gzipped CSV files from both systems.
*   **Action**:
    1.  Decompress both CSV files.
    2.  Count the number of data rows (excluding header) in each file.
*   **Pass/Fail Criterion**: The row count from the migrated job's output must be exactly equal to the row count from the legacy job's output.

#### Test Case 4.3: Data Type Consistency in Output

*   **Purpose**: To verify that the data types in the exported CSV are consistent with the expected types and that no data corruption or unexpected type conversions occur during the export process.
*   **Setup**:
    1.  Run the full `dwh_exis_sd_apt_nna_voic_dag` Airflow DAG successfully.
    2.  Retrieve the final gzipped CSV file.
*   **Action**:
    1.  Decompress the CSV file.
    2.  Load a sample of the CSV data into a DataFrame (e.g., Pandas) or parse it programmatically.
    3.  Inspect the inferred data types of the columns and verify that numeric columns are indeed numeric, string columns are strings, etc., and that precision for decimal types is maintained.
*   **Pass/Fail Criterion**:
    *   `MONATS_ID`, `TARIF_ID`, `LEISTUNGSKLASSE_ID`, `VERBINDUNGEN`, `MP_EG_JN_ID`, `MP_GENERATION_ID` should be parsable as integers.
    *   `DAUER_MIN`, `RBETRAG_VBUD_NETTO_EURO` should be parsable as floating-point numbers with 2 decimal places.
    *   Other fields should be parsable as strings.
    *   No data loss or unexpected truncation should occur.

#### Test Case 4.4: BigQuery Table Schema Validation

*   **Purpose**: To validate that the DDLs for the `raw_dwh` tables in BigQuery correctly reflect the source Oracle schemas, especially concerning data types, NULLability, and column names.
*   **Setup**:
    1.  Deploy the provided BigQuery DDLs (`raw_dwh.VI_L_MAP_FA_TARIF.sql`, etc.) to the target GCP project.
    2.  Obtain the schema definitions for the corresponding source Oracle tables.
*   **Action**:
    1.  Query BigQuery's `INFORMATION_SCHEMA` to retrieve the schema details (column name, data type, nullability) for each `raw_dwh` table.
    2.  Compare these BigQuery schemas against the documented Oracle source schemas.
*   **Pass/Fail Criterion**:
    *   All columns present in the Oracle source tables must be present in the corresponding BigQuery tables.
    *   Data types must be compatible and correctly mapped (e.g., Oracle `NUMBER` to BigQuery `INT64` or `NUMERIC`, Oracle `VARCHAR2` to BigQuery `STRING`, Oracle `DATE` to BigQuery `DATE`).
    *   NULLability constraints (`NOT NULL`) must be correctly translated.

```sql
-- Example BigQuery SQL assertion for schema validation
-- This would be run for each raw_dwh table.
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `your-gcp-project-id.raw_dwh.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'VI_L_MAP_FA_TARIF'
ORDER BY
  ordinal_position;

-- Compare this output to the expected schema:
-- Expected:
-- column_name | data_type | is_nullable
-- -------------|-----------|-------------
-- DWH_TARIF_ID | INT64     | NO
-- TARIF_ID     | INT64     | NO
-- GUELTIG_BIS  | DATE      | YES
-- _LAST_MODIFIED_TS | TIMESTAMP | YES
```

#### Test Case 4.5: Parameter Handling (`MONATS_ID`)

*   **Purpose**: To verify that the `MONATS_ID` parameter is correctly derived from the Airflow execution date and accurately passed to the BigQuery query.
*   **Setup**:
    1.  Deploy the `dwh_exis_sd_apt_nna_voic_dag.py` to Airflow.
    2.  Manually trigger the DAG for a specific execution date, e.g., `2023-03-01`.
*   **Action**:
    1.  Monitor the `bigquery_extract_task` in the Airflow UI.
    2.  Inspect the task logs for the actual SQL query executed by the `BigQueryExecuteQueryOperator`. The logs should show the resolved parameter value.
    3.  Alternatively, after the `bigquery_extract_task` completes, query the temporary BigQuery table and filter by `MONATS_ID` to confirm the data corresponds to the expected month.
*   **Pass/Fail Criterion**: The `MONATS_ID` parameter passed to the BigQuery query must be `202303` (for an execution date of `2023-03-01`), correctly formatted as an `INT64`. The data in the temporary table should only contain records for this `MONATS_ID`.

```python
# Example Python (pytest) assertion for parameter resolution
import pytest
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

@provide_session
def get_task_log(dag_id, task_id, execution_date, session=None):
    """Helper to retrieve task logs (simplified, actual log retrieval is more complex)"""
    # In a real test, you'd use Airflow's logging system or API to get the actual rendered SQL.
    # For this example, we'll simulate checking the parameter.
    # A more robust test would involve parsing the actual BQ job configuration.
    pass

def test_monats_id_parameter_resolution(airflow_client): # Assuming an Airflow client fixture
    """
    Verifies that MONATS_ID is correctly resolved and passed.
    """
    dag_id = "dwh_exis_sd_apt_nna_voic_dag"
    execution_date_str = "2023-03-01T00:00:00+00:00"
    expected_monats_id = 202303

    # Trigger the DAG (or assume it's already run for this date)
    # dag_run = airflow_client.trigger_dag(dag_id, execution_date=pendulum.parse(execution_date_str))
    # Wait for task to complete...

    # Simulate checking the parameter that was passed to BigQuery
    # In a real scenario, you might inspect the BigQuery job details via API
    # or parse the Airflow task logs for the rendered SQL.
    
    # For demonstration, let's assume we can get the rendered query_params
    # This is a conceptual check, actual implementation depends on Airflow testing utilities.
    mock_context = {
        "ds": "2023-03-01",
        "macros": MagicMock()
    }
    mock_context["macros"].ds_format.return_value = "202303"

    # This part would be executed by the BigQueryExecuteQueryOperator
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    op = BigQueryExecuteQueryOperator(
        task_id="extract_voice_data_from_bigquery",
        sql="d_exis_apt_nna_voice.bq.sql",
        use_legacy_sql=False,
        query_params=[
            {
                "name": "FROM_YYYYMM",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}"},
            },
        ],
        gcp_conn_id="google_cloud_default",
    )
    
    # Render the template (Airflow does this internally)
    rendered_params = op.render_template_fields(context=mock_context)
    
    actual_monats_id_param = None
    for param in rendered_params["query_params"]:
        if param["name"] == "FROM_YYYYMM":
            actual_monats_id_param = int(param["parameterValue"]["value"])
            break

    assert actual_monats_id_param == expected_monats_id, \
        f"MONATS_ID parameter mismatch. Expected: {expected_monats_id}, Actual: {actual_monats_id_param}"

```