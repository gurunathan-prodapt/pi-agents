As a senior data-migration QA engineer, I've reviewed the migration design document and the generated Airflow DAG and PySpark code for `EXIS_SD_APT_NNA_DATA`. The primary challenge for comprehensive testing is the "Medium Risk" associated with the unknown internal logic of the legacy `r_exis_v2` script and the unidentified source/target systems.

Therefore, these tests will focus on:
*   **Orchestration and Parameter Passing:** Ensuring the Airflow DAG correctly triggers the PySpark job with the right parameters.
*   **External System Interaction:** Verifying the PySpark script can read configuration and source data from GCS and write output to GCS as specified.
*   **Core Logic Re-implementation (Placeholder):** Testing the basic transformations present in the provided PySpark code and the derivation of `MONAT_ID`.
*   **Output Format and Naming:** Validating the output file structure, compression, and naming conventions, highlighting any discrepancies.
*   **Data Quality & Edge Cases:** Basic checks for row counts, schema, and graceful handling of empty or missing inputs.

**Assumptions for Testing:**
*   The "telephone system master data" source is assumed to be a CSV file stored in GCS for the purpose of these tests, allowing the `source_path` argument to be utilized.
*   The `h_exis_apt_nna_daten.var` configuration file is assumed to be a simple CSV or key-value file that PySpark can read.
*   The core transformation logic of `r_exis_v2` is currently represented by the placeholder `concat` operation and the addition of `MONAT_ID` in the PySpark script. More detailed transformation tests would require reverse-engineering the original `r_exis_v2` script.
*   The "target system for distribution" is currently GCS, as per the design. Any further distribution is out of scope for this job's migration.

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-001
#### Purpose: Verify Airflow DAG Triggers PySpark Job with Correct Parameters
This test ensures that the Airflow DAG successfully initiates the Dataproc PySpark job and passes all required arguments as specified in the migration design and generated code.

#### Setup:
1.  Ensure the Airflow environment is running and the `dw_dwh_exis_sd_apt_nna_data` DAG is deployed.
2.  Ensure a Dataproc cluster (`YOUR_DATAPROC_CLUSTER_NAME`) is available and configured correctly.
3.  Ensure the PySpark script `r_exis_v2.py` is uploaded to `gs://YOUR_BUCKET_NAME/pyspark/r_exis_v2.py`.
4.  Set `GCP_PROJECT_ID`, `DATAPROC_REGION`, `DATAPROC_CLUSTER_NAME`, and `GCS_BUCKET` in the DAG to valid values.

#### Action:
1.  Manually trigger the `dw_dwh_exis_sd_apt_nna_data` DAG in Airflow.
2.  Monitor the Airflow task logs for `dwh_exis_sd_apt_nna_data`.
3.  Access the Dataproc job details (via GCP Console or `gcloud dataproc jobs describe`) for the submitted job.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   The `dwh_exis_sd_apt_nna_data` task in Airflow completes successfully.
    *   The Dataproc job is submitted and starts execution.
    *   The Dataproc job logs (driver logs) show the PySpark script receiving the correct arguments:
        *   `--job-kennung EXIS_SD_APT_NNA_DATA`
        *   `--config-path gs://YOUR_BUCKET_NAME/config/h_exis_apt_nna_daten.var`
        *   `--output-path gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/`
        *   `--execution-ts YYYY-MM-DD` (where `YYYY-MM-DD` is the Airflow execution date, e.g., `2024-01-01`).
*   **Fail:** Any of the above conditions are not met, or the task/job fails.

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-002
#### Purpose: Verify `MONAT_ID` Derivation Correctness
This test ensures that the `MONAT_ID` (YYYYMM) is correctly derived from the Airflow execution date (`{{ ds }}`) as specified in the design.

#### Setup:
1.  Same as Test Case 001.
2.  Prepare a dummy source CSV file (e.g., `source_data.csv`) in GCS at `gs://YOUR_BUCKET_NAME/input/source_data.csv` with at least one row and a `data` column.
3.  Modify the DAG to pass `source-path` to the PySpark job:
    ```python
    # ... inside pyspark_job args ...
    "--source-path", f"gs://{GCS_BUCKET}/input/source_data.csv",
    # ...
    ```

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG with a specific `execution_date`, e.g., `2024-03-15`.
2.  Monitor the Dataproc job logs.
3.  After successful completion, inspect the generated output file in GCS.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   The Dataproc job logs contain a line similar to `Derived MONAT_ID: 202403`.
    *   The output CSV file (e.g., `part-00000-*.csv.gz`) contains a column named `MONAT_ID` with the value `202403` for all rows.
*   **Fail:** The `MONAT_ID` in logs or output does not match the `YYYYMM` of the Airflow execution date.

#### Runnable Test Code (Conceptual PySpark assertion after job run):
```python
import pandas as pd
import gzip
from google.cloud import storage

def verify_monat_id(gcs_output_path, expected_monat_id):
    client = storage.Client()
    bucket_name, prefix = gcs_output_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)

    # Find the output directory (e.g., temp_YYYYMMDDHHMMSS)
    blobs = list(bucket.list_blobs(prefix=prefix))
    output_dir_blob = next((b for b in blobs if b.name.endswith('/')), None)
    if not output_dir_blob:
        raise AssertionError(f"No output directory found in {gcs_output_path}")

    # Find the actual part file inside the output directory
    part_file_prefix = output_dir_blob.name
    part_blobs = list(bucket.list_blobs(prefix=part_file_prefix))
    csv_gz_blob = next((b for b in part_blobs if b.name.endswith('.csv.gz')), None)

    if not csv_gz_blob:
        raise AssertionError(f"No .csv.gz file found in {part_file_prefix}")

    # Download and read the gzipped CSV
    blob_content = csv_gz_blob.download_as_bytes()
    with gzip.open(io.BytesIO(blob_content), 'rt') as f:
        df = pd.read_csv(f)

    assert 'MONAT_ID' in df.columns, "MONAT_ID column not found in output."
    assert (df['MONAT_ID'] == int(expected_monat_id)).all(), \
        f"Not all MONAT_ID values match expected {expected_monat_id}."
    print(f"MONAT_ID verification successful. All values are {expected_monat_id}.")

# Example usage (after running the Airflow DAG for execution_date='2024-03-15')
# Replace with actual GCS path and expected MONAT_ID
# verify_monat_id("gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/", "202403")
```

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-003
#### Purpose: Verify Configuration File Handling (External System Replacement)
This test ensures the PySpark script correctly reads the `h_exis_apt_nna_daten.var` equivalent from GCS and uses its values if applicable.

#### Setup:
1.  Same as Test Case 001.
2.  Create a sample configuration file, `h_exis_apt_nna_daten.var`, with content that can be read as a simple CSV (as per the PySpark code's `spark.read.csv` attempt).
    Example `h_exis_apt_nna_daten.var` content:
    ```csv
    key,value
    some_config_value,test_value_123
    another_setting,true
    ```
3.  Upload this file to GCS at `gs://YOUR_BUCKET_NAME/config/h_exis_apt_nna_daten.var`.
4.  Ensure the PySpark script's placeholder logic for using `config_df` is active (e.g., the `if config_df is not None and "some_config_value" in config_df.columns:` block).
5.  Prepare a dummy source CSV file (e.g., `source_data.csv`) in GCS at `gs://YOUR_BUCKET_NAME/input/source_data.csv` with at least one row and a `data` column.
6.  Modify the DAG to pass `source-path` to the PySpark job (as in Test Case 002).

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  Monitor the Dataproc job logs.
3.  After successful completion, inspect the generated output file in GCS.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   The Dataproc job logs contain a line similar to `Loaded configuration from gs://YOUR_BUCKET_NAME/config/h_exis_apt_nna_daten.var. Content: [...]`.
    *   The logs also show `Using config value: test_value_123`.
    *   The output CSV file contains a column named `config_setting` with the value `test_value_123` for all rows (based on the placeholder PySpark logic).
*   **Fail:** The configuration file is not loaded, or its values are not correctly reflected in the logs or output.

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-004
#### Purpose: Verify Source Data Reading (External System Replacement)
This test confirms that the PySpark script can successfully read the "telephone system master data" from the specified GCS `source_path`.

#### Setup:
1.  Same as Test Case 001.
2.  Create a sample source CSV file, `sample_master_data.csv`, with known content and row count.
    Example `sample_master_data.csv` content:
    ```csv
    id,data,status
    1,phone_A,active
    2,phone_B,inactive
    3,phone_C,active
    ```
3.  Upload this file to GCS at `gs://YOUR_BUCKET_NAME/input/sample_master_data.csv`.
4.  Modify the DAG to pass `source-path` to the PySpark job:
    ```python
    # ... inside pyspark_job args ...
    "--source-path", f"gs://{GCS_BUCKET}/input/sample_master_data.csv",
    # ...
    ```

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  Monitor the Dataproc job logs.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   The Dataproc job logs contain a line similar to `Loaded source data from gs://YOUR_BUCKET_NAME/input/sample_master_data.csv. Row count: 3`.
    *   The job completes successfully without errors related to source data loading.
*   **Fail:** The job fails, or logs indicate an inability to read the source data or an incorrect row count.

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-005
#### Purpose: Verify Output Parity - Data Content (Placeholder Transformation)
This test verifies that the data content in the output CSV matches the expected results based on the placeholder transformation logic in the PySpark script.

#### Setup:
1.  Same as Test Case 004, using `sample_master_data.csv`.
2.  Ensure the `MONAT_ID` derivation and `processed_data` concatenation logic are active in the PySpark script.
3.  Ensure the `config_setting` logic is active if a config file is provided (as in Test Case 003).

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG with a known `execution_date` (e.g., `2024-04-01`).
2.  After successful completion, download and inspect the output CSV file from GCS.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   The output CSV contains the original columns (`id`, `data`, `status`).
    *   A `MONAT_ID` column exists with the value `202404` for all rows.
    *   A `processed_data` column exists with values like `phone_A_processed`, `phone_B_processed`, `phone_C_processed`.
    *   If a config file was used, a `config_setting` column exists with the expected value (e.g., `test_value_123`).
*   **Fail:** The output data does not match these expectations.

#### Runnable Test Code (Conceptual PySpark assertion after job run):
```python
import pandas as pd
import gzip
import io
from google.cloud import storage

def verify_output_content(gcs_output_path, expected_monat_id, expected_config_value=None):
    client = storage.Client()
    bucket_name, prefix = gcs_output_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)

    # Find the actual part file
    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_gz_blob = next((b for b in blobs if b.name.endswith('.csv.gz')), None)
    if not csv_gz_blob:
        raise AssertionError(f"No .csv.gz file found in {gcs_output_path}")

    blob_content = csv_gz_blob.download_as_bytes()
    with gzip.open(io.BytesIO(blob_content), 'rt') as f:
        df = pd.read_csv(f)

    # Verify original columns
    assert 'id' in df.columns and 'data' in df.columns and 'status' in df.columns, \
        "Original columns missing."

    # Verify MONAT_ID
    assert 'MONAT_ID' in df.columns, "MONAT_ID column missing."
    assert (df['MONAT_ID'] == int(expected_monat_id)).all(), \
        f"MONAT_ID values do not match {expected_monat_id}."

    # Verify processed_data
    assert 'processed_data' in df.columns, "processed_data column missing."
    assert (df['processed_data'] == df['data'] + '_processed').all(), \
        "processed_data transformation incorrect."

    # Verify config_setting if applicable
    if expected_config_value:
        assert 'config_setting' in df.columns, "config_setting column missing."
        assert (df['config_setting'] == expected_config_value).all(), \
            f"config_setting values do not match {expected_config_value}."

    print("Output content verification successful.")

# Example usage (after running the Airflow DAG for execution_date='2024-04-01')
# verify_output_content("gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/temp_20240401*/", "202404", "test_value_123")
```

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-006
#### Purpose: Verify Output Parity - File Naming, Format, and Compression
This test verifies that the output file is a gzipped CSV and adheres to the expected naming convention, acknowledging the PySpark script's noted limitation.

#### Setup:
1.  Same as Test Case 004.
2.  Ensure the DAG is configured with `GCS_OUTPUT_PATH`.

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  After successful completion, browse the `GCS_OUTPUT_PATH` in the GCS bucket.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   A new directory is created under `gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/` with a name like `temp_YYYYMMDDHHMMSS` (e.g., `temp_20240401103000`). The `YYYYMMDDHHMMSS` part should correspond to the job's execution time.
    *   Inside this directory, there is exactly one file named `part-00000-*.csv.gz`.
    *   The file is a valid gzipped CSV.
*   **Fail:** The output directory or file is not found, the naming convention is incorrect, or the file is not a valid gzipped CSV.

**Note on Discrepancy:** The legacy job produced a single file named `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`. The current PySpark implementation produces a directory containing `part-00000-*.csv.gz`. This is a **behavioral difference** that needs to be addressed if strict output filename parity is required by downstream systems. A follow-up Airflow task (e.g., `GCSObjectRenameOperator` or a `gsutil mv` command via `BashOperator`) would be needed to rename/move the `part-00000-*.csv.gz` file to the desired `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz` at the top level of the output path.

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-007
#### Purpose: Verify Data Quality - Row Count Parity
This test ensures that the number of rows in the output matches the expected count based on the source data and any transformations.

#### Setup:
1.  Same as Test Case 004, using `sample_master_data.csv` with 3 data rows (plus header).
2.  Ensure no transformations that add or remove rows are active (or account for them in the expected count).

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  After successful completion, download and count the data rows in the output CSV file from GCS.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:** The output CSV file contains exactly 3 data rows (excluding header).
*   **Fail:** The row count in the output does not match the expected count.

#### Runnable Test Code (Conceptual PySpark assertion after job run):
```python
import pandas as pd
import gzip
import io
from google.cloud import storage

def verify_row_count(gcs_output_path, expected_count):
    client = storage.Client()
    bucket_name, prefix = gcs_output_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_gz_blob = next((b for b in blobs if b.name.endswith('.csv.gz')), None)
    if not csv_gz_blob:
        raise AssertionError(f"No .csv.gz file found in {gcs_output_path}")

    blob_content = csv_gz_blob.download_as_bytes()
    with gzip.open(io.BytesIO(blob_content), 'rt') as f:
        df = pd.read_csv(f)

    actual_count = len(df)
    assert actual_count == expected_count, \
        f"Row count mismatch. Expected {expected_count}, got {actual_count}."
    print(f"Row count verification successful. Actual: {actual_count}, Expected: {expected_count}.")

# Example usage (after running the Airflow DAG)
# verify_row_count("gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/temp_20240401*/", 3)
```

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-008
#### Purpose: Verify Data Quality - Schema Parity
This test ensures that the output schema (column names and order) matches the expected schema, ideally derived from the legacy output.

#### Setup:
1.  Same as Test Case 004.
2.  Obtain the expected schema (column names and their order) from the legacy `r_exis_v2` output. For this test, we'll assume `id,data,status,processed_data,MONAT_ID,config_setting`.

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  After successful completion, download the output CSV and infer its schema.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:** The column names and their order in the output CSV match the expected schema.
*   **Fail:** The schema differs (e.g., missing columns, extra columns, incorrect order, unexpected data types if type inference is considered).

#### Runnable Test Code (Conceptual PySpark assertion after job run):
```python
import pandas as pd
import gzip
import io
from google.cloud import storage

def verify_schema_parity(gcs_output_path, expected_columns):
    client = storage.Client()
    bucket_name, prefix = gcs_output_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_gz_blob = next((b for b in blobs if b.name.endswith('.csv.gz')), None)
    if not csv_gz_blob:
        raise AssertionError(f"No .csv.gz file found in {gcs_output_path}")

    blob_content = csv_gz_blob.download_as_bytes()
    with gzip.open(io.BytesIO(blob_content), 'rt') as f:
        df = pd.read_csv(f)

    actual_columns = df.columns.tolist()
    assert actual_columns == expected_columns, \
        f"Schema mismatch. Expected {expected_columns}, got {actual_columns}."
    print("Schema parity verification successful.")

# Example usage (after running the Airflow DAG)
# expected_cols = ['id', 'data', 'status', 'processed_data', 'MONAT_ID', 'config_setting']
# verify_schema_parity("gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/temp_20240401*/", expected_cols)
```

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-009
#### Purpose: Verify NULL Handling in Placeholder Transformation
This test checks how the `processed_data` transformation handles NULL values in the source `data` column.

#### Setup:
1.  Same as Test Case 001.
2.  Create a source CSV file with NULLs in the `data` column:
    ```csv
    id,data,status
    1,phone_A,active
    2,,inactive
    3,phone_C,active
    4,,active
    ```
3.  Upload this file to GCS at `gs://YOUR_BUCKET_NAME/input/null_data.csv`.
4.  Modify the DAG to pass `source-path` to the PySpark job:
    ```python
    # ... inside pyspark_job args ...
    "--source-path", f"gs://{GCS_BUCKET}/input/null_data.csv",
    # ...
    ```

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  After successful completion, download and inspect the output CSV file.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   For rows where `data` was `NULL` (empty string in CSV), the `processed_data` column is also `NULL` (or empty string, depending on `concat` behavior with empty strings).
    *   For rows where `data` had a value, `processed_data` is correctly transformed (e.g., `phone_A_processed`).
*   **Fail:** `NULL` values are handled unexpectedly (e.g., `NULL_processed` or an error occurs).

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-010
#### Purpose: Edge Case - Empty Source Data Handling
This test verifies that the job gracefully handles an empty source dataset without failing.

#### Setup:
1.  Same as Test Case 001.
2.  Create an empty source CSV file (only header row):
    ```csv
    id,data,status
    ```
3.  Upload this file to GCS at `gs://YOUR_BUCKET_NAME/input/empty_source.csv`.
4.  Modify the DAG to pass `source-path` to the PySpark job:
    ```python
    # ... inside pyspark_job args ...
    "--source-path", f"gs://{GCS_BUCKET}/input/empty_source.csv",
    # ...
    ```

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  Monitor the Dataproc job logs.
3.  Inspect the GCS output path.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   The Dataproc job completes successfully.
    *   Logs indicate `Loaded source data... Row count: 0`.
    *   An output directory (`temp_YYYYMMDDHHMMSS`) is created in GCS.
    *   A gzipped CSV file (`part-00000-*.csv.gz`) is present within the output directory, containing only the header row.
*   **Fail:** The job fails, or no output file is generated, or the output file contains unexpected data.

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-011
#### Purpose: Edge Case - Missing Configuration File Handling
This test verifies that the job handles a missing configuration file gracefully, as indicated by the `try-except` block in the PySpark code.

#### Setup:
1.  Same as Test Case 004 (with `sample_master_data.csv`).
2.  **Ensure no file exists** at `gs://YOUR_BUCKET_NAME/config/h_exis_apt_nna_daten.var`.

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  Monitor the Dataproc job logs.

#### Expected Result / Pass/Fail Criterion:
*   **Pass:**
    *   The Dataproc job completes successfully.
    *   Logs contain a warning message similar to `Could not load configuration from gs://YOUR_BUCKET_NAME/config/h_exis_apt_nna_daten.var: [Error details]`.
    *   The job does not fail due to the missing configuration file.
    *   The output file is generated, but without any `config_setting` column (as the config was not loaded).
*   **Fail:** The job fails due to the missing configuration file.

---

### Test Case ID: EXIS_SD_APT_NNA_DATA-012
#### Purpose: Verify Logging
This test ensures that key events and progress messages are logged correctly in the Dataproc job logs, replacing the legacy `DW.LESE_LOG` functionality.

#### Setup:
1.  Same as Test Case 004 (with `sample_master_data.csv` and a valid config file).

#### Action:
1.  Trigger the `dw_dwh_exis_sd_apt_nna_data` DAG.
2.  Access the Dataproc job driver logs (via GCP Console or `gcloud dataproc jobs describe`).

#### Expected Result / Pass/Fail Criterion:
*   **Pass:** The Dataproc job logs contain the following messages (or similar, with correct values):
    *   `Starting job: EXIS_SD_APT_NNA_DATA`
    *   `Derived MONAT_ID: YYYYMM`
    *   `Output timestamp (for filename): YYYYMMDDHHMMSS`
    *   `Loaded configuration from gs://YOUR_BUCKET_NAME/config/h_exis_apt_nna_daten.var. Content: [...]`
    *   `Loaded source data from gs://YOUR_BUCKET_NAME/input/sample_master_data.csv. Row count: 3`
    *   `Using config value: test_value_123` (if config used in transformation)
    *   `Writing final output to GCS path: gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/DWHM_APT_NNA_Daten_YYYYMMDDHHMMSS.csv.gz`
    *   `Output written to: gs://YOUR_BUCKET_NAME/output/dwhm_apt_nna_daten/temp_YYYYMMDDHHMMSS/part-00000-*.csv.gz. Expected logical filename: DWHM_APT_NNA_Daten_YYYYMMDDHHMMSS.csv.gz`
    *   `Job completed successfully: EXIS_SD_APT_NNA_DATA`
*   **Fail:** Critical log messages are missing, or error messages appear unexpectedly.

---