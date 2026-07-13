# Migration Validation Test Suite for `k_exis_ftp2.ksh`

This document contains the migration-validation tests to verify that the migrated Google Cloud Platform (GCP) components (`sp_exis_ftp2.sql` and `ftp_transfer_adapter/main.py`) are behaviorally equivalent to the legacy KornShell script (`k_exis_ftp2.ksh`).

---

## Test Case 1: End-to-End Happy Path (Successful Transfer and Rename)

### Purpose
To verify that a standard, successful file transfer executes exactly like the legacy script:
1. The file is uploaded to the target FTP server with a `.tmp` extension.
2. The `.tmp` file is successfully renamed to its final name.
3. The audit log table is populated with correct `DEBUG` and `INFO` traces.
4. The procedure returns a success code (`0`).

### Setup
1. **GCS Source File:** Create a dummy file in GCS at `gs://test-export-bucket/exports/test_data.csv` containing:
   ```csv
   id,value
   1,test_record
   ```
2. **Mock FTP Server:** Spin up a local or test FTP server (e.g., using `pyftpdlib` in a test container) with:
   * Host: `10.0.0.5`
   * User: `test_ftp_user`
   * Password: `super_secure_password` (stored in Secret Manager under the secret name `ftp-test-pass-key`)
   * Target Directory: `/remote/exports`
3. **Secret Manager:** Ensure `projects/your_project_id/secrets/ftp-test-pass-key/versions/latest` contains `super_secure_password`.
4. **Audit Log Table:** Clear any existing logs for the test run:
   ```sql
   DELETE FROM `your_project_id.isccr_exporter_dataset.ccr_audit_log` WHERE eintrags_nr = 'RUN_001';
   ```

### Action
Execute the BigQuery stored procedure:
```sql
DECLARE v_out_error INT64;

CALL `your_project_id.isccr_exporter_dataset.sp_exis_ftp2`(
  'RUN_001',
  'gs://test-export-bucket/exports/test_data.csv',
  '10.0.0.5',
  'test_ftp_user',
  'ftp-test-pass-key',
  '/remote/exports',
  'csv',
  1, -- Debug enabled
  v_out_error
);

SELECT v_out_error AS return_code;
```

### Pass/Fail Criterion
* **Pass:** 
  * The returned `return_code` is `0`.
  * The file `/remote/exports/test_data.csv` exists on the FTP server and matches the source content.
  * The temporary file `/remote/exports/test_data.csv.tmp` **does not** exist on the FTP server.
  * The `ccr_audit_log` table contains exactly 3 entries for `RUN_001` matching the expected sequence:
    1. `DEBUG` - "Uebertrage gs://test-export-bucket/exports/test_data.csv nach /remote/exports (Temp: gs://test-export-bucket/exports/test_data.csv.tmp)"
    2. `INFO` - "Transfer abgeschlossen fuer Datei: gs://test-export-bucket/exports/test_data.csv"
* **Fail:** Any return code other than `0`, missing remote file, leftover `.tmp` file, or missing audit logs.

---

## Test Case 2: Phase 1 Failure (Upload Interrupted / GCS File Missing)

### Purpose
To verify that if the source file does not exist in GCS, Phase 1 fails gracefully, the execution halts immediately without attempting a rename, and the procedure returns the legacy shell error code (`1`).

### Setup
1. **GCS Source File:** Ensure `gs://test-export-bucket/exports/non_existent_file.csv` **does not** exist.
2. **Audit Log Table:** Clear logs for the test run:
   ```sql
   DELETE FROM `your_project_id.isccr_exporter_dataset.ccr_audit_log` WHERE eintrags_nr = 'RUN_002';
   ```

### Action
Execute the BigQuery stored procedure:
```sql
DECLARE v_out_error INT64;

CALL `your_project_id.isccr_exporter_dataset.sp_exis_ftp2`(
  'RUN_002',
  'gs://test-export-bucket/exports/non_existent_file.csv',
  '10.0.0.5',
  'test_ftp_user',
  'ftp-test-pass-key',
  '/remote/exports',
  'csv',
  1,
  v_out_error
);

SELECT v_out_error AS return_code;
```

### Pass/Fail Criterion
* **Pass:**
  * The returned `return_code` is `1` (equivalent to legacy `k_FehlerShell`).
  * The `ccr_audit_log` table contains an `ERROR` entry containing the text: `"Konnte gs://test-export-bucket/exports/non_existent_file.csv nicht uebertragen. Details: Source file gs://test-export-bucket/exports/non_existent_file.csv not found in GCS."`
  * No rename action is triggered.
* **Fail:** The procedure returns `0`, or the error log is missing, or the adapter attempts to rename a non-existent file.

---

## Test Case 3: Phase 2 Failure (Rename Fails on Target Server)

### Purpose
To verify that if the file is successfully uploaded as `.tmp` but the remote FTP server prevents renaming (e.g., permission issues or file locks), the procedure logs the specific error, halts, and returns `1`.

### Setup
1. **GCS Source File:** Create a valid file at `gs://test-export-bucket/exports/locked_file.csv`.
2. **FTP Server Mocking:** Configure the mock FTP server to accept uploads but throw a `550 Permission Denied` error when a `RNTO` (Rename To) command is issued for `locked_file.csv`.
3. **Audit Log Table:** Clear logs for the test run:
   ```sql
   DELETE FROM `your_project_id.isccr_exporter_dataset.ccr_audit_log` WHERE eintrags_nr = 'RUN_003';
   ```

### Action
Execute the BigQuery stored procedure:
```sql
DECLARE v_out_error INT64;

CALL `your_project_id.isccr_exporter_dataset.sp_exis_ftp2`(
  'RUN_003',
  'gs://test-export-bucket/exports/locked_file.csv',
  '10.0.0.5',
  'test_ftp_user',
  'ftp-test-pass-key',
  '/remote/exports',
  'csv',
  1,
  v_out_error
);

SELECT v_out_error AS return_code;
```

### Pass/Fail Criterion
* **Pass:**
  * The returned `return_code` is `1`.
  * The temporary file `/remote/exports/locked_file.csv.tmp` exists on the FTP server (proving Phase 1 succeeded).
  * The final file `/remote/exports/locked_file.csv` **does not** exist.
  * The `ccr_audit_log` table contains an `ERROR` entry containing the text: `"Konnte gs://test-export-bucket/exports/locked_file.csv.tmp nicht umbenennen. Details: 550 Permission Denied"`.
* **Fail:** The procedure returns `0`, or the temporary file is missing, or the error log does not capture the rename failure details.

---

## Test Case 4: Debug Switch Behavior (Output Parity)

### Purpose
To verify that the `p_Debug` flag behaves exactly like the legacy script:
* When `p_Debug = 1`, `DEBUG` level messages are written to both the console stream and the `ccr_audit_log` table.
* When `p_Debug = 0`, `DEBUG` level messages are suppressed, but `INFO` and `ERROR` messages are still logged.

### Setup
1. **GCS Source File:** Create a valid file at `gs://test-export-bucket/exports/debug_test.csv`.
2. **Audit Log Table:** Clear logs for test runs `RUN_004_A` and `RUN_004_B`.

### Action
```sql
-- Run A: Debug Enabled (1)
DECLARE v_out_error_a INT64;
CALL `your_project_id.isccr_exporter_dataset.sp_exis_ftp2`(
  'RUN_004_A', 'gs://test-export-bucket/exports/debug_test.csv',
  '10.0.0.5', 'test_ftp_user', 'ftp-test-pass-key', '/remote/exports', 'csv',
  1, v_out_error_a
);

-- Run B: Debug Disabled (0)
DECLARE v_out_error_b INT64;
CALL `your_project_id.isccr_exporter_dataset.sp_exis_ftp2`(
  'RUN_004_B', 'gs://test-export-bucket/exports/debug_test.csv',
  '10.0.0.5', 'test_ftp_user', 'ftp-test-pass-key', '/remote/exports', 'csv',
  0, v_out_error_b
);
```

### Pass/Fail Criterion
* **Pass:**
  * For `RUN_004_A`: The audit log contains a `DEBUG` entry and an `INFO` entry.
  * For `RUN_004_B`: The audit log contains **only** the `INFO` entry. The `DEBUG` entry is completely absent.
* **Fail:** `DEBUG` logs are written during `RUN_004_B`, or `INFO` logs are missing from either run.

---

## Test Case 5: Automated Integration Test (Pytest)

### Purpose
An automated Python test suite using `pytest` to validate the Cloud Function's internal routing, Secret Manager integration, and FTP interactions. This ensures that the Python adapter behaves identically to the legacy `h_alis_ftp.ksh` shell functions.

```python
# test_ftp_transfer_adapter.py
import pytest
import json
from unittest.mock import MagicMock, patch
from main import ftp_transfer_adapter

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "test-project")

@patch("main.resolve_secret")
@patch("ftplib.FTP")
@patch("google.cloud.storage.Client")
def test_ftp_transfer_send_happy_path(mock_gcs_client, mock_ftp_class, mock_resolve_secret, mock_env):
    # Setup Mocks
    mock_resolve_secret.return_value = "decrypted_password"
    
    mock_ftp_instance = MagicMock()
    mock_ftp_class.return_value = mock_ftp_instance
    
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.open.return_value.__enter__.return_value = b"file_content"
    
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_gcs_client.return_value.bucket.return_value = mock_bucket

    # Construct BigQuery External Function payload
    request_data = {
        "calls": [
            [
                "gs://my-bucket/exports/data.csv",
                "data.csv.tmp",
                "ftp.example.com",
                "ftp_user",
                "secret-key-name",
                "/remote/dir",
                "SEND"
            ]
        ]
    }
    
    # Mock Flask Request Object
    mock_request = MagicMock()
    mock_request.get_json.return_value = request_data

    # Execute Cloud Function
    response_str, status_code = ftp_transfer_adapter(mock_request)
    response_json = json.loads(response_str)

    # Assertions
    assert status_code == 200
    assert response_json["replies"][0]["status"] == "SUCCESS"
    
    # Verify FTP commands executed in correct sequence
    mock_ftp_class.assert_called_once_with("ftp.example.com")
    mock_ftp_instance.login.assert_called_once_with(user="ftp_user", passwd="decrypted_password")
    mock_ftp_instance.cwd.assert_called_once_with("/remote/dir")
    mock_ftp_instance.storbinary.assert_called_once()
    mock_ftp_instance.quit.assert_called_once()

@patch("main.resolve_secret")
@patch("ftplib.FTP")
def test_ftp_transfer_rename_happy_path(mock_ftp_class, mock_resolve_secret, mock_env):
    # Setup Mocks
    mock_resolve_secret.return_value = "decrypted_password"
    mock_ftp_instance = MagicMock()
    mock_ftp_class.return_value = mock_ftp_instance

    request_data = {
        "calls": [
            [
                "data.csv.tmp",
                "data.csv",
                "ftp.example.com",
                "ftp_user",
                "secret-key-name",
                "/remote/dir",
                "RENAME"
            ]
        ]
    }
    
    mock_request = MagicMock()
    mock_request.get_json.return_value = request_data

    # Execute Cloud Function
    response_str, status_code = ftp_transfer_adapter(mock_request)
    response_json = json.loads(response_str)

    # Assertions
    assert status_code == 200
    assert response_json["replies"][0]["status"] == "SUCCESS"
    mock_ftp_instance.rename.assert_called_once_with("data.csv.tmp", "data.csv")
    mock_ftp_instance.quit.assert_called_once()
```

### Pass/Fail Criterion
* **Pass:** Running `pytest test_ftp_transfer_adapter.py` passes all assertions with `exit_code = 0`.
* **Fail:** Any assertion fails, indicating a mismatch in parameter mapping, FTP command sequencing, or error handling.