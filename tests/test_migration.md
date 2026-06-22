As a senior data-migration QA engineer, I've analyzed the `EXIS_SD_APT_RABATT` migration design and generated code. Below are comprehensive migration validation tests designed to ensure behavioral equivalence, data integrity, and correct functionality of the new GCP-based job.

---

# Migration Validation Tests: EXIS_SD_APT_RABATT

## Test Case 1: End-to-End Output Parity (Golden Record Comparison)

**Purpose:** To verify that the migrated job produces an identical output file (content and compression) to the legacy job when given the same input data. This is the ultimate behavioral equivalence test, covering the entire pipeline from data extraction to final file generation and compression.

**Setup:**
1.  **Controlled Legacy Data:** Identify or create a specific, controlled snapshot of the Oracle source tables (`RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`). This snapshot should represent a typical production scenario, including various data types, NULLs, and aggregated values.
2.  **Legacy Job Execution:** Run the legacy `EXIS_SD_APT_RABATT` job against this controlled Oracle data snapshot.
3.  **Golden Record Capture:** Retrieve the generated `DWHM_APT_RABATTREPORT_<timestamp_legacy>.csv.gz` file from the legacy SFTP target or archive. This file will serve as the "golden record."
4.  **Migrated Data Replication:** Replicate the *exact same snapshot* of Oracle source data into the BigQuery `ORACLE_DATA` tables. This step is critical for a fair comparison.
5.  **Migrated Job Configuration:** Ensure the Airflow DAG (`exis_sd_apt_rabatt_dag.py`), BigQuery SQL (`rabatt_data_extraction.sql`), and Python script (`post_process_rabatt_data.py`) are deployed and configured correctly in the GCP environment.
6.  **Timestamp Alignment (Optional but Recommended):** If possible, configure the Airflow DAG to generate a timestamp in the output filename and header/footer that matches the legacy job's timestamp, or ensure the comparison logic can handle timestamp differences. For this test, we will assume the comparison logic handles timestamp differences.

**Action:**
1.  Trigger the migrated `exis_sd_apt_rabatt_dag.py` Airflow DAG to run against the corresponding BigQuery `ORACLE_DATA` tables.
2.  Retrieve the generated `DWHM_APT_RABATTREPORT_<timestamp_migrated>.csv.gz` file from the `gs://<project_id>-apt-rabatt-export/work/` bucket (or the SFTP target if accessible for comparison).
3.  Decompress both the legacy "golden record" `.gz` file and the migrated `.gz` file.
4.  Compare the content of the two decompressed `.csv` files line by line. The comparison should ignore any differences in the timestamp values within the header and footer lines, as these are expected to vary based on execution time.

**Pass/Fail Criterion:**
*   **PASS:** The decompressed CSV content of the migrated job's output file is byte-for-byte identical to the decompressed CSV content of the legacy job's output file, after accounting for expected differences in timestamp values in the header/footer.
*   **FAIL:** Any difference in content, row count, or structure between the two decompressed CSV files.

**Runnable Test Code (Python):**
```python
import gzip
import re
import os

def compare_gzipped_csv_files(legacy_gz_path, migrated_gz_path):
    """
    Compares the content of two gzipped CSV files, ignoring timestamps
    in the header/footer lines.
    """
    def read_and_clean_content(gz_path):
        with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
            lines = f.readlines()

        cleaned_lines = []
        for line in lines:
            # Regex to replace YYYYMMDD parts in header/footer with placeholders
            # Header: X|filename|YYYYMMDD|NR|V_S_Rabattreport|YYYYMMDD
            # Footer: E|filename|YYYYMMDD|NR|V_S_Rabattreport|YYYYMMDD
            if line.startswith('X|') or line.startswith('E|'):
                parts = line.strip().split('|')
                if len(parts) >= 6:
                    parts[2] = "YYYYMMDD_FROM_PLACEHOLDER"
                    parts[5] = "YYYYMMDD_SYSDATE_PLACEHOLDER"
                cleaned_lines.append("|".join(parts) + "\n")
            else:
                cleaned_lines.append(line)
        return "".join(cleaned_lines)

    print(f"Comparing legacy: {legacy_gz_path} with migrated: {migrated_gz_path}")
    legacy_content = read_and_clean_content(legacy_gz_path)
    migrated_content = read_and_clean_content(migrated_gz_path)

    if legacy_content == migrated_content:
        print("PASS: Content of both files are identical (ignoring timestamps in header/footer).")
        return True
    else:
        print("FAIL: Content of files differ.")
        # Optional: write differences to files for manual inspection
        with open("legacy_content_cleaned.txt", "w") as f:
            f.write(legacy_content)
        with open("migrated_content_cleaned.txt", "w") as f:
            f.write(migrated_content)
        print("Differences written to 'legacy_content_cleaned.txt' and 'migrated_content_cleaned.txt'.")
        return False

# Example usage (replace with actual paths after job execution)
# legacy_output_file = "/path/to/legacy/DWHM_APT_RABATTREPORT_20231027120000.csv.gz"
# migrated_output_file = "/path/to/migrated/DWHM_APT_RABATTREPORT_20231027123000.csv.gz"
# assert compare_gzipped_csv_files(legacy_output_file, migrated_output_file)
```

---

## Test Case 2: SQL Transformation Correctness (BigQuery vs. Oracle)

**Purpose:** To verify that the BigQuery SQL query (`rabatt_data_extraction.sql`) produces the exact same result set as the original Oracle SQL query (`d_exis_apt_rabattdaten.sql`) when executed against equivalent data. This isolates the core data transformation logic, including joins, aggregations (`LISTAGG`/`STRING_AGG`), filters, and column selection.

**Setup:**
1.  **Data Consistency:** Ensure the BigQuery `ORACLE_DATA` tables contain data that is an *exact, point-in-time replica* of the Oracle source tables used for the legacy query.
2.  **Query Access:** Have access to both the original Oracle SQL query and the migrated BigQuery SQL query.

**Action:**
1.  Execute the original Oracle SQL query (`d_exis_apt_rabattdaten.sql`) against the Oracle source database. Export the results to a pipe-separated CSV file (e.g., `oracle_results.csv`).
2.  Execute the BigQuery SQL query (`rabatt_data_extraction.sql`) against the BigQuery `ORACLE_DATA` tables. Export the results to a pipe-separated CSV file (e.g., `bq_results.csv`).
3.  Compare the two exported CSV files. The comparison should verify:
    *   **Row Count:** Identical number of rows.
    *   **Column Count & Order:** Identical number of columns in the same order.
    *   **Data Content:** Each cell's value should match. Special attention should be paid to `BASISPRODUKTE` (from `LISTAGG`/`STRING_AGG`) for correct aggregation and ordering, and `RABATTHOEHE` for potential floating-point precision differences.

**Pass/Fail Criterion:**
*   **PASS:** The result set from the BigQuery query is identical to the result set from the Oracle query in terms of row count, column count, column order, and data content (allowing for minor, acceptable floating-point precision differences if applicable).
*   **FAIL:** Any discrepancy in row count, column count, column order, or data values.

**Runnable Test Code (Python with Pandas):**
```python
import pandas as pd
import io

def compare_sql_results_csv(oracle_csv_path, bigquery_csv_path, separator='|'):
    """
    Compares two CSV files (from Oracle and BigQuery) for content equivalence.
    Assumes column order is the same.
    """
    try:
        df_oracle = pd.read_csv(oracle_csv_path, sep=separator, dtype=str, keep_default_na=False)
        df_bigquery = pd.read_csv(bigquery_csv_path, sep=separator, dtype=str, keep_default_na=False)

        # Sort both DataFrames to ensure row order doesn't affect comparison
        # Use all columns as a stable sort key
        sort_cols = df_oracle.columns.tolist()
        df_oracle_sorted = df_oracle.sort_values(by=sort_cols).reset_index(drop=True)
        df_bigquery_sorted = df_bigquery.sort_values(by=sort_cols).reset_index(drop=True)

        # Compare shapes (row and column counts)
        if df_oracle_sorted.shape != df_bigquery_sorted.shape:
            print(f"FAIL: Row/column count mismatch. Oracle: {df_oracle_sorted.shape}, BigQuery: {df_bigquery_sorted.shape}")
            return False

        # Compare column names (should be identical if exported consistently)
        if not df_oracle_sorted.columns.equals(df_bigquery_sorted.columns):
            print(f"FAIL: Column name mismatch. Oracle: {df_oracle_sorted.columns.tolist()}, BigQuery: {df_bigquery_sorted.columns.tolist()}")
            return False
        
        # Compare content using .equals() for exact DataFrame comparison
        if df_oracle_sorted.equals(df_bigquery_sorted):
            print("PASS: Oracle and BigQuery SQL results are identical.")
            return True
        else:
            print("FAIL: Oracle and BigQuery SQL results differ.")
            # Find and print differences for debugging
            diff = df_oracle_sorted.compare(df_bigquery_sorted)
            print("Differences found:")
            print(diff)
            return False
    except Exception as e:
        print(f"An error occurred during SQL results comparison: {e}")
        return False

# Example usage:
# Assuming 'oracle_results.csv' and 'bq_results.csv' are generated and available locally
# assert compare_sql_results_csv("oracle_results.csv", "bq_results.csv")
```

---

## Test Case 3: Post-processing Logic (Header/Footer, Row Count, Compression)

**Purpose:** To verify that the Python post-processing script (`post_process_rabatt_data.py`) correctly applies the `nawk`-like header/footer logic, accurately counts data rows, uses the specified separator, and compresses the output correctly. This tests the transformation correctness of the Python component.

**Setup:**
1.  **Sample Input Data:** Create a sample CSV file (without header/footer) that mimics the output of the BigQuery SQL query. Include various data types, NULLs, and edge cases for `BASISPRODUKTE` (e.g., single value, multiple values, NULL).
    *   `sample_input.csv`:
        ```
        RV1|T1|TG1|RP1|RPI1|100.50|BPR1,BPR2
        RV2|T2|TG2|RP2|RPI2|200.00|BPR3
        RV3|T3|TG3|RP3|RPI3|50.25|
        RV4|T4|TG4|RP4|RPI4|NULL|BPR4
        ```
2.  **Expected Output:** Manually construct the expected gzipped output file's decompressed content, including the correct header, footer, and row count. The `output_filename` in the header/footer will be `DWHM_APT_RABATTREPORT_<timestamp>.csv.gz`. The `NR` will be the count of data rows (4 in this example).

**Action:**
1.  Upload `sample_input.csv` to a temporary GCS location (e.g., `gs://<project_id>-apt-rabatt-export/temp/sample_input.csv`).
2.  Execute the `post_process_and_compress` Python function (or run the script via a `PythonOperator` in a test DAG) with `input_gcs_path` pointing to the sample input and `output_gcs_path` to a test output location (e.g., `gs://<project_id>-apt-rabatt-export/temp/test_output.csv.gz`).
3.  Download the generated `test_output.csv.gz` from GCS.
4.  Decompress the downloaded file.
5.  Compare the decompressed content with the manually constructed expected output (after replacing timestamp placeholders).
6.  Verify the `content_type` of the uploaded GCS object is `application/gzip`.

**Pass/Fail Criterion:**
*   **PASS:** The decompressed output file's content (header, data rows, footer) exactly matches the expected content, including the correct data row count (`df.shape[0]`) in the header/footer. The file is successfully compressed in `gzip` format, and the separator (`|`) is correctly used.
*   **FAIL:** Any mismatch in content, incorrect row count in header/footer, wrong separator, or compression failure.

**Runnable Test Code (Pytest with Mock GCS):**
```python
import pytest
import io
import gzip
from datetime import datetime
from unittest.mock import MagicMock, patch

# Assuming post_process_rabatt_data.py is in 'python/' directory relative to test file
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Adjust path if needed
from python.post_process_rabatt_data import post_process_and_compress

@pytest.fixture
def mock_gcs_client():
    with patch('google.cloud.storage.Client') as mock_client:
        yield mock_client.return_value

def test_post_process_and_compress_basic_case(mock_gcs_client):
    input_csv_content = """RV1|T1|TG1|RP1|RPI1|100.50|BPR1,BPR2
RV2|T2|TG2|RP2|RPI2|200.00|BPR3
RV3|T3|TG3|RP3|RPI3|50.25|
RV4|T4|TG4|RP4|RPI4|NULL|BPR4
"""
    expected_row_count = 4
    current_date_str = datetime.now().strftime("%Y%m%d")
    output_filename = "DWHM_APT_RABATTREPORT_20231027123456.csv.gz" # Example filename

    # Mock GCS behavior for input and output blobs
    mock_input_blob = MagicMock()
    mock_input_blob.download_as_bytes.return_value = input_csv_content.encode('utf-8')
    
    mock_output_blob = MagicMock()
    mock_gcs_client.bucket.return_value.blob.side_effect = [mock_input_blob, mock_output_blob]

    input_gcs_path = "gs://test-bucket/temp/sample_input.csv"
    output_gcs_path = f"gs://test-bucket/work/{output_filename}"
    project_id = "test-project"

    # Action
    post_process_and_compress(input_gcs_path, output_gcs_path, project_id)

    # Assertions
    mock_output_blob.upload_from_string.assert_called_once()
    uploaded_data = mock_output_blob.upload_from_string.call_args[0][0]
    
    # Decompress and check content
    decompressed_content = gzip.decompress(uploaded_data).decode('utf-8')

    expected_header = f"X|{output_filename}|{current_date_str}|{expected_row_count}|V_S_Rabattreport|{current_date_str}\n"
    expected_footer = f"E|{output_filename}|{current_date_str}|{expected_row_count}|V_S_Rabattreport|{current_date_str}"
    
    expected_full_content = expected_header + input_csv_content + expected_footer

    assert decompressed_content == expected_full_content, "Decompressed content does not match expected."
    assert mock_output_blob.upload_from_string.call_args[1]['content_type'] == 'application/gzip'
    print("PASS: Basic post-processing and compression test successful.")

def test_post_process_and_compress_empty_data(mock_gcs_client):
    input_csv_content = ""
    expected_row_count = 0
    current_date_str = datetime.now().strftime("%Y%m%d")
    output_filename = "DWHM_APT_RABATTREPORT_EMPTY.csv.gz"

    mock_input_blob = MagicMock()
    mock_input_blob.download_as_bytes.return_value = input_csv_content.encode('utf-8')
    
    mock_output_blob = MagicMock()
    mock_gcs_client.bucket.return_value.blob.side_effect = [mock_input_blob, mock_output_blob]

    input_gcs_path = "gs://test-bucket/temp/empty_input.csv"
    output_gcs_path = f"gs://test-bucket/work/{output_filename}"
    project_id = "test-project"

    post_process_and_compress(input_gcs_path, output_gcs_path, project_id)

    uploaded_data = mock_output_blob.upload_from_string.call_args[0][0]
    decompressed_content = gzip.decompress(uploaded_data).decode('utf-8')

    expected_header = f"X|{output_filename}|{current_date_str}|{expected_row_count}|V_S_Rabattreport|{current_date_str}\n"
    expected_footer = f"E|{output_filename}|{current_date_str}|{expected_row_count}|V_S_Rabattreport|{current_date_str}"
    expected_full_content = expected_header + input_csv_content + expected_footer

    assert decompressed_content == expected_full_content, "Empty data content does not match expected."
    print("PASS: Empty data post-processing and compression test successful.")

# To run these tests:
# 1. Ensure `python/post_process_rabatt_data.py` exists relative to your test file.
# 2. Install pytest, google-cloud-storage, and pandas: `pip install pytest google-cloud-storage pandas`
# 3. Run from your terminal: `pytest your_test_file.py`
```

---

## Test Case 4: External System Replacement (SFTP Distribution)

**Purpose:** To verify that the Airflow DAG correctly transfers the final compressed file to the external SFTP server as specified in the design, replacing the legacy SFTP mechanism.

**Setup:**
1.  **SFTP Server:** Ensure a test SFTP server is accessible from the Airflow environment with the configured credentials (`sftp_apt_rabatt_conn`). The target directory (`SFTP_REMOTE_PATH`) should be empty or contain only test files.
2.  **Airflow Configuration:**
    *   `sftp_apt_rabatt_conn` Airflow Connection is correctly set up with host, port, username, and password/key.
    *   `sftp_remote_path` Airflow Variable is set to a known test directory on the SFTP server.
3.  **Test File:** A dummy `.csv.gz` file (e.g., `DWHM_APT_RABATTREPORT_SFTP_TEST.csv.gz`) is manually uploaded to the `gs://<project_id>-apt-rabatt-export/work/` bucket, mimicking the output of the `post_process_and_compress` task.

**Action:**
1.  Trigger a test run of the Airflow DAG, ensuring the `gcs_download_for_sftp` and `distribute_to_sftp_corrected` tasks execute. This might involve running a partial DAG or mocking upstream tasks if a full end-to-end run is not desired for this specific test.
2.  Monitor the Airflow task logs for `distribute_to_sftp_corrected` to ensure no errors.
3.  Connect to the SFTP server (using a separate SFTP client like `sftp` or `WinSCP`) and verify the presence and integrity of the transferred file in the `SFTP_REMOTE_PATH`.
4.  Compare the content of the file on the SFTP server with the original file in GCS.

**Pass/Fail Criterion:**
*   **PASS:** The `DWHM_APT_RABATTREPORT_SFTP_TEST.csv.gz` file is successfully transferred to the specified `SFTP_REMOTE_PATH` on the external SFTP server, and its content is byte-for-byte identical to the source file in GCS.
*   **FAIL:** The file is not found on the SFTP server, the transfer fails, or the file is corrupted.

**Runnable Test Code (Conceptual Bash/SFTP client):**
```bash
# --- Manual Setup Steps ---
# 1. Create a dummy test file locally
echo "Test data for SFTP transfer" | gzip > DWHM_APT_RABATTREPORT_SFTP_TEST.csv.gz

# 2. Upload to GCS work bucket
gsutil cp DWHM_APT_RABATTREPORT_SFTP_TEST.csv.gz gs://your-gcp-project-id-apt-rabatt-export/work/

# 3. Trigger Airflow DAG (or specific tasks)
#    Example: Trigger the DAG via Airflow UI or CLI, ensuring it processes the dummy file.
#    (You might need to adjust the DAG's timestamp logic or filename to match the dummy file for a targeted test)

# --- Verification Steps (after DAG run) ---
SFTP_HOST="your.sftp.host"
SFTP_USER="your_sftp_user"
SFTP_REMOTE_PATH="/path/to/sftp/target/dir/" # From Airflow Variable sftp_remote_path
SFTP_FILE="DWHM_APT_RABATTREPORT_SFTP_TEST.csv.gz"
LOCAL_DOWNLOAD_PATH="/tmp/sftp_downloaded_file.gz"
GCS_SOURCE_PATH="gs://your-gcp-project-id-apt-rabatt-export/work/${SFTP_FILE}"
LOCAL_GCS_ORIGINAL_PATH="/tmp/gcs_original_file.gz"

echo "Attempting to download file from SFTP..."
sftp -o StrictHostKeyChecking=no ${SFTP_USER}@${SFTP_HOST} <<EOF
get ${SFTP_REMOTE_PATH}${SFTP_FILE} ${LOCAL_DOWNLOAD_PATH}
bye
EOF

if [ $? -ne 0 ]; then
    echo "FAIL: SFTP download failed. File might not have been transferred."
    exit 1
fi

echo "Downloading original file from GCS..."
gsutil cp "${GCS_SOURCE_PATH}" "${LOCAL_GCS_ORIGINAL_PATH}"

# 3. Compare the two files
if cmp -s "${LOCAL_DOWNLOAD_PATH}" "${LOCAL_GCS_ORIGINAL_PATH}"; then
    echo "PASS: SFTP transferred file is identical to GCS source."
else
    echo "FAIL: SFTP transferred file differs from GCS source."
    echo "Differences (decompressed):"
    diff -u <(gzip -d < "${LOCAL_DOWNLOAD_PATH}") <(gzip -d < "${LOCAL_GCS_ORIGINAL_PATH}")
fi

# 4. Clean up (optional)
rm -f DWHM_APT_RABATTREPORT_SFTP_TEST.csv.gz
rm -f "${LOCAL_DOWNLOAD_PATH}"
rm -f "${LOCAL_GCS_ORIGINAL_PATH}"
gsutil rm "${GCS_SOURCE_PATH}"
sftp -o StrictHostKeyChecking=no ${SFTP_USER}@${SFTP_HOST} <<EOF
rm ${SFTP_REMOTE_PATH}${SFTP_FILE}
bye
EOF
```

---

## Test Case 5: External System Replacement (GCS Archiving)

**Purpose:** To verify that the Airflow DAG correctly moves the processed file from the `work` bucket to the `archive` bucket after successful distribution, replacing the legacy local archiving mechanism.

**Setup:**
1.  **GCS Buckets:** Ensure `gs://<project_id>-apt-rabatt-export/work/` and `gs://<project_id>-apt-rabatt-export/archive/` exist and are accessible by the Airflow service account.
2.  **Test File:** A dummy `.csv.gz` file (e.g., `DWHM_APT_RABATTREPORT_ARCHIVE_TEST.csv.gz`) is manually uploaded to the `gs://<project_id>-apt-rabatt-export/work/` bucket.

**Action:**
1.  Trigger a test run of the Airflow DAG, ensuring the `archive_processed_file` task executes. This task should be configured to run *after* the SFTP distribution task.
2.  Monitor Airflow task logs for `archive_processed_file` to confirm successful execution.
3.  Verify that the file is no longer present in the `gs://<project_id>-apt-rabatt-export/work/` bucket.
4.  Verify that the file is present in the `gs://<project_id>-apt-rabatt-export/archive/` bucket.

**Pass/Fail Criterion:**
*   **PASS:** The `DWHM_APT_RABATTREPORT_ARCHIVE_TEST.csv.gz` file is successfully moved from the `work` bucket to the `archive` bucket.
*   **FAIL:** The file remains in the `work` bucket, is not found in the `archive` bucket, or the move operation fails.

**Runnable Test Code (Conceptual Bash/gsutil):**
```bash
# --- Manual Setup Steps ---
# 1. Create a dummy test file locally
echo "Test data for GCS archiving" | gzip > DWHM_APT_RABATTREPORT_ARCHIVE_TEST.csv.gz

# 2. Upload to GCS work bucket
GCS_WORK_BUCKET="gs://your-gcp-project-id-apt-rabatt-export/work/"
GCS_ARCHIVE_BUCKET="gs://your-gcp-project-id-apt-rabatt-export/archive/"
TEST_FILE="DWHM_APT_RABATTREPORT_ARCHIVE_TEST.csv.gz"

gsutil cp "${TEST_FILE}" "${GCS_WORK_BUCKET}${TEST_FILE}"

# 3. Trigger Airflow DAG (or specific tasks)
#    Ensure the archive_processed_file task runs.

# --- Verification Steps (after DAG run) ---

echo "Checking if file exists in work bucket..."
if gsutil ls "${GCS_WORK_BUCKET}${TEST_FILE}" &> /dev/null; then
    echo "FAIL: File ${TEST_FILE} still exists in work bucket."
    WORK_BUCKET_CHECK="FAIL"
else
    echo "PASS: File ${TEST_FILE} is no longer in work bucket."
    WORK_BUCKET_CHECK="PASS"
fi

echo "Checking if file exists in archive bucket..."
if gsutil ls "${GCS_ARCHIVE_BUCKET}${TEST_FILE}" &> /dev/null; then
    echo "PASS: File ${TEST_FILE} is present in archive bucket."
    ARCHIVE_BUCKET_CHECK="PASS"
else
    echo "FAIL: File ${TEST_FILE} is NOT present in archive bucket."
    ARCHIVE_BUCKET_CHECK="FAIL"
fi

if [ "${WORK_BUCKET_CHECK}" == "PASS" ] && [ "${ARCHIVE_BUCKET_CHECK}" == "PASS" ]; then
    echo "Overall PASS: GCS archiving successful."
else
    echo "Overall FAIL: GCS archiving failed."
fi

# 4. Clean up (optional)
rm -f "${TEST_FILE}"
gsutil rm "${GCS_ARCHIVE_BUCKET}${TEST_FILE}" # Remove from archive for next test run
```

---

## Test Case 6: Data Quality - Row Count and Schema Assertions

**Purpose:** To verify that the final output file maintains the expected number of data rows and that the schema (number of columns, data types) is consistent with the design and legacy output. This is a sanity check for data integrity and transformation correctness.

**Setup:**
1.  **Migrated Output:** The Airflow DAG has successfully run and produced `DWHM_APT_RABATTREPORT_<timestamp>.csv.gz` in the `work` bucket.
2.  **Expected Metrics:** Have the expected data row count (from a legacy run or design spec) and the expected number of columns (7, as per the SQL query).

**Action:**
1.  Download and decompress the migrated output file from GCS.
2.  Parse the decompressed CSV file:
    *   Extract the reported data row count from the header and footer (the `NR` field).
    *   Count the actual data rows (excluding header and footer).
    *   Verify the number of columns in each data row.
    *   Perform basic data type checks on key columns (e.g., `RABATTHOEHE` should be numeric, `BASISPRODUKTE` should be string).
3.  Compare these metrics against the expected values.

**Pass/Fail Criterion:**
*   **PASS:**
    *   The `NR` field in the header/footer matches the actual number of data rows in the file.
    *   The actual number of data rows matches the expected row count from the legacy system (or a known baseline).
    *   The number of columns in each data row is consistent and matches the expected count (7 columns).
    *   Key data types are as expected (e.g., `RABATTHOEHE` is a valid float, `BASISPRODUKTE` is a string).
*   **FAIL:** Any discrepancy in row counts, inconsistent column numbers, or unexpected data type issues.

**Runnable Test Code (Python):**
```python
import gzip
import io
import pandas as pd
import re

def validate_output_file_data_quality(gzipped_file_path, expected_data_row_count=None):
    """
    Validates row count and basic schema of the decompressed CSV output.
    """
    try:
        with gzip.open(gzipped_file_path, 'rt', encoding='utf-8') as f:
            lines = f.readlines()

        if not lines:
            print("FAIL: File is empty.")
            return False

        header_line = lines[0]
        footer_line = lines[-1]
        data_lines = lines[1:-1] # Exclude header and footer

        # 1. Validate header/footer format and extracted row count
        header_parts = header_line.strip().split('|')
        footer_parts = footer_line.strip().split('|')

        if not (header_parts[0] == 'X' and footer_parts[0] == 'E'):
            print("FAIL: Header or footer prefix incorrect (expected X and E).")
            return False
        
        if not (len(header_parts) == 6 and len(footer_parts) == 6):
            print(f"FAIL: Header/footer has incorrect number of fields. Header: {len(header_parts)}, Footer: {len(footer_parts)}")
            return False

        try:
            header_reported_row_count = int(header_parts[3])
            footer_reported_row_count = int(footer_parts[3])
        except ValueError:
            print("FAIL: Row count in header/footer is not an integer.")
            return False

        actual_data_row_count = len(data_lines)

        if not (header_reported_row_count == actual_data_row_count and footer_reported_row_count == actual_data_row_count):
            print(f"FAIL: Reported row count in header/footer ({header_reported_row_count}/{footer_reported_row_count}) does not match actual data rows ({actual_data_row_count}).")
            return False
        print(f"PASS: Header/footer reported row count matches actual data rows ({actual_data_row_count}).")

        if expected_data_row_count is not None and actual_data_row_count != expected_data_row_count:
            print(f"FAIL: Actual data row count ({actual_data_row_count}) does not match expected ({expected_data_row_count}).")
            return False
        elif expected_data_row_count is not None:
            print(f"PASS: Actual data row count ({actual_data_row_count}) matches expected ({expected_data_row_count}).")

        # 2. Validate schema (number of columns)
        expected_columns = 7 # Based on the SQL query output
        for i, line in enumerate(data_lines):
            cols = line.strip().split('|')
            if len(cols) != expected_columns:
                print(f"FAIL: Data row {i+1} has {len(cols)} columns, expected {expected_columns}. Content: '{line.strip()}'")
                return False
        print(f"PASS: All data rows have the expected number of columns ({expected_columns}).")

        # 3. Basic data type check (e.g., RABATTHOEHE is numeric)
        if data_lines:
            df = pd.read_csv(io.StringIO("".join(data_lines)), sep='|', header=None,
                             names=['RAHMENVERTRAG_ID', 'TARIF_ID', 'DWH_TARIFGR_TEXT',
                                    'RABATTIERTE_RECH_POS', 'RABATTIERTE_RECH_POS_ID',
                                    'RABATTHOEHE', 'BASISPRODUKTE'],
                             keep_default_na=False) # Important for NULLs to be treated as empty strings

            # Check RABATTHOEHE can be converted to numeric (float64 in BQ)
            # Allow for 'NULL' string if it's explicitly in data, or empty string
            numeric_rabatthoehe = pd.to_numeric(df['RABATTHOEHE'], errors='coerce')
            if not numeric_rabatthoehe.notna().all() and not (df['RABATTHOEHE'] == 'NULL').all() and not (df['RABATTHOEHE'] == '').all():
                print("FAIL: 'RABATTHOEHE' column contains unexpected non-numeric values.")
                return False
            print("PASS: 'RABATTHOEHE' column is numeric or expected NULL representation.")

            # Check BASISPRODUKTE is string (and potentially comma-separated)
            if not df['BASISPRODUKTE'].apply(lambda x: isinstance(x, str)).all():
                print("FAIL: 'BASISPRODUKTE' column contains non-string values.")
                return False
            print("PASS: 'BASISPRODUKTE' column is string-like.")

        print("Overall PASS: Data quality checks passed.")
        return True

    except Exception as e:
        print(f"An error occurred during data quality validation: {e}")
        return False

# Example usage:
# After running the DAG and downloading the output file:
# migrated_output_file = "/tmp/DWHM_APT_RABATTREPORT_20231027123456.csv.gz"
# expected_legacy_row_count = 12345 # Obtain this from a legacy run or design spec
# assert validate_output_file_data_quality(migrated_output_file, expected_legacy_row_count)
```

---

## Test Case 7: NULL Handling in Aggregations (`STRING_AGG`)

**Purpose:** To verify that `STRING_AGG` in BigQuery handles NULL values in the aggregated column (`BPR_ID`) identically to Oracle's `LISTAGG`. By default, both functions ignore NULLs, but this should be explicitly confirmed.

**Setup:**
1.  **Controlled Data:** Prepare a small dataset in both Oracle and BigQuery `ORACLE_DATA` tables where `SOF_TA_BPR_OPTIONEN.BPR_ID` contains NULL values for some `CNTRCT_ID` groups that would be aggregated.
    *   **Example Scenario:**
        *   `RPT_TA_S_D1_VERTRAG`: `(RAHMENVERTRAG_ID='RV_NULL_TEST', SV_ID='SV_NULL', DWH_TARIFGR_TEXT='TG_NULL', VERTRAG_ID_CARMEN='C_NULL')`
        *   `RPT_TA_S_D1_DISCOUNT_RR`: `(CNTRCT_TEMPLATE_ID='SV_NULL', RABATTIERTE_RECH_POS='RP_NULL', DISC_INVOICE_ITEM_ID='RPI_NULL', RABATTHOEHE=10.0, CONTRACT_NUMBER='RV_NULL_TEST')`
        *   `SOF_TA_BPR_OPTIONEN`:
            *   `(BPR_ID='BPR1', CNTRCT_ID='C_NULL')`
            *   `(BPR_ID=NULL, CNTRCT_ID='C_NULL')`
            *   `(BPR_ID='BPR2', CNTRCT_ID='C_NULL')`
        *   `SOF_VI_L_OPTIONZUORDNUNG`: `(OPTION_ID='BPR1')`, `(OPTION_ID='BPR2')` (ensuring only non-NULL `BPR_ID`s join)

**Action:**
1.  Execute the original Oracle SQL query (`d_exis_apt_rabattdaten.sql`) against the controlled Oracle data.
2.  Execute the BigQuery SQL query (`rabatt_data_extraction.sql`) against the controlled BigQuery data.
3.  Compare the `BASISPRODUKTE` column for the `RAHMENVERTRAG_ID='RV_NULL_TEST'` in both result sets.

**Pass/Fail Criterion:**
*   **PASS:** The `BASISPRODUKTE` column in the BigQuery result for the test case is identical to the Oracle result (e.g., `BPR1,BPR2`, indicating that NULLs in `BPR_ID` were ignored during aggregation).
*   **FAIL:** The `BASISPRODUKTE` column differs (e.g., `BPR1,,BPR2` or `BPR1,NULL,BPR2`), indicating a different handling of NULLs in the aggregation.

**Runnable Test Code (Conceptual SQL for verification):**
```sql
-- Expected Oracle Output for 'RV_NULL_TEST' (assuming LISTAGG ignores NULLs)
-- RAHMENVERTRAG_ID | TARIF_ID | DWH_TARIFGR_TEXT | RABATTIERTE_RECH_POS | RABATTIERTE_RECH_POS_ID | RABATTHOEHE | BASISPRODUKTE
-- -----------------|----------|------------------|----------------------|-------------------------|-------------|----------------
-- RV_NULL_TEST     | SV_NULL  | TG_NULL          | RP_NULL              | RPI_NULL                | 10.0        | BPR1,BPR2

-- Expected BigQuery Output for 'RV_NULL_TEST' (should match Oracle)
-- RAHMENVERTRAG_ID | TARIF_ID | DWH_TARIFGR_TEXT | RABATTIERTE_RECH_POS | RABATTIERTE_RECH_POS_ID | RABATTHOEHE | BASISPRODUKTE
-- -----------------|----------|------------------|----------------------|-------------------------|-------------|----------------
-- RV_NULL_TEST     | SV_NULL  | TG_NULL          | RP_NULL              | RPI_NULL                | 10.0        | BPR1,BPR2

-- This specific check would be part of the broader "SQL Transformation Correctness" (Test Case 2)
-- by ensuring the `compare_sql_results_csv` function identifies no differences for this test scenario.
```

---

## Test Case 8: Empty Source Data

**Purpose:** To verify that the migrated job handles an empty source dataset gracefully, producing an output file with only the header and footer, and a row count of 0. This ensures robustness for scenarios where no data is available for export.

**Setup:**
1.  **Empty Data:** Ensure all BigQuery `ORACLE_DATA` tables are empty or contain no data that would satisfy the join conditions of `rabatt_data_extraction.sql`.
2.  **Airflow Configuration:** The DAG is deployed and configured.

**Action:**
1.  Trigger the `exis_sd_apt_rabatt_dag.py` Airflow DAG.
2.  Retrieve the generated `DWHM_APT_RABATTREPORT_<timestamp>.csv.gz` file from the `gs://<project_id>-apt-rabatt-export/work/` bucket.
3.  Decompress the file.
4.  Verify its content.

**Pass/Fail Criterion:**
*   **PASS:** The decompressed CSV file contains exactly two lines (header and footer), with the `NR` field in both lines correctly showing `0`. The file is properly gzipped.
*   **FAIL:** The job fails, produces an invalid file, or the row count in the header/footer is incorrect.

**Runnable Test Code (Python):**
```python
import gzip
import io
from datetime import datetime

def test_empty_source_data_output(gzipped_file_path):
    """
    Verifies output for empty source data.
    """
    try:
        with gzip.open(gzipped_file_path, 'rt', encoding='utf-8') as f:
            lines = f.readlines()

        assert len(lines) == 2, f"FAIL: Expected 2 lines (header+footer) for empty data, got {len(lines)}."

        header_line = lines[0]
        footer_line = lines[1]

        header_parts = header_line.strip().split('|')
        footer_parts = footer_line.strip().split('|')

        assert header_parts[0] == 'X', f"FAIL: Header prefix incorrect, got {header_parts[0]}."
        assert footer_parts[0] == 'E', f"FAIL: Footer prefix incorrect, got {footer_parts[0]}."
        assert int(header_parts[3]) == 0, f"FAIL: Header row count expected 0, got {header_parts[3]}."
        assert int(footer_parts[3]) == 0, f"FAIL: Footer row count expected 0, got {footer_parts[3]}."

        print("PASS: Empty source data handled correctly, output contains only header/footer with 0 rows.")
        return True

    except Exception as e:
        print(f"FAIL: An error occurred during empty data test: {e}")
        return False

# Example usage:
# After running the DAG with empty source data and downloading the output:
# empty_output_file = "/tmp/DWHM_APT_RABATTREPORT_EMPTY.csv.gz"
# assert test_empty_source_data_output(empty_output_file)
```