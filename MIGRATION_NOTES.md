# MIGRATION_NOTES.md — Job: k_exis_ftp2.ksh

This document provides comprehensive migration notes for transitioning the legacy KornShell data transfer utility `k_exis_ftp2.ksh` to Google Cloud Platform (GCP) using BigQuery and a serverless Cloud Function.

---

## 1. Summary

The legacy KornShell script `k_exis_ftp2.ksh` has been migrated from an on-premises UNIX environment to **Google Cloud Platform (GCP)**. 

*   **Source Platform:** On-premises UNIX/Linux executing KornShell (`ksh`), relying on local file systems and legacy FTP wrappers (`h_alis_ftp.ksh`).
*   **Target Platform:** Google Cloud Platform.
    *   **Orchestration & Logging:** BigQuery GoogleSQL Stored Procedures (`sp_exis_ftp2`, `sp_log_message`).
    *   **File Storage:** Google Cloud Storage (GCS) replacing the local UNIX filesystem.
    *   **Network Transfer Engine:** Python 3.11 HTTP Cloud Function (`ftp_transfer_adapter`) acting as a secure FTP/SFTP bridge.
*   **Functional Preservation:** The migration preserves the critical two-stage transactional delivery mechanism:
    1.  **Upload as Temporary:** Uploads the source file with a `.tmp` suffix.
    2.  **Rename to Final:** Renames the remote `.tmp` file to its final target name upon verified upload success.

---

## 2. Generated Artifacts

The migration process generated the following files, organized by their target runtime environments:

### A. BigQuery Database Layer (`gcp/procedures/`)
*   **`sp_exis_ftp2.sql`**
    *   **Role:** The primary orchestration stored procedure. It replaces the main logic of `k_exis_ftp2.ksh`. It manages the execution state, handles exceptions, and coordinates the two-stage transfer process.
    *   **`sp_log_message` (Sub-Procedure):** Replaces the legacy logging wrapper `h_alis_meldungen.ksh` (`CCRMSG_LogDebug`). It writes structured logs to the central audit table and outputs trace logs to the console.
    *   **`ext_ftp_transfer_handler` (External Function):** The BigQuery External Connection binding that allows BigQuery to securely invoke the Cloud Function via SQL.

### B. Serverless Integration Layer (`gcp/cloud_functions/ftp_transfer_adapter/`)
*   **`main.py`**
    *   **Role:** A Python 3.11 Google Cloud Function (conforming to the `functions-framework` HTTP specification). It handles the physical network connection, streams files directly from Google Cloud Storage to the target FTP server, and executes remote file renames.
*   **`requirements.txt`**
    *   **Role:** Defines the Python package dependencies required by the Cloud Function runtime (`google-cloud-storage`, `google-cloud-secret-manager`, and `functions-framework`).

---

## 3. Key Design Decisions

### A. Serverless File Streaming (No Local Disk Bottlenecks)
*   **Decision:** The Cloud Function streams data directly from Google Cloud Storage to the remote FTP server using Python's `ftplib` and GCS file-like object streams (`blob.open("rb")`).
*   **Reasoning:** This avoids downloading files to the Cloud Function's local `/tmp` disk space, eliminating memory/disk bottlenecks and allowing the transfer of very large export files.

### B. Externalization of Network Logic from BigQuery
*   **Decision:** BigQuery uses an `EXTERNAL FUNCTION` to trigger the transfer.
*   **Reasoning:** BigQuery is a data warehouse and cannot open raw TCP sockets or negotiate FTP/SFTP protocols. Delegating network operations to a Cloud Function keeps BigQuery focused on orchestration and metadata logging.

### C. Decoupled Logging Architecture
*   **Decision:** Created a dedicated sub-procedure `sp_log_message` writing to a centralized table `ccr_audit_log`.
*   **Reasoning:** This mimics the legacy `h_alis_meldungen.ksh` framework, ensuring that operational dashboards and support teams have a single, unified view of transfer statuses without modifying downstream log parsers.

### D. Security Hardening (B4 Redesign Item)
*   **Decision:** Replaced the legacy practice of passing raw passwords as positional parameters (`$5` / `p_Passwort`) with Google Cloud Secret Manager keys.
*   **Reasoning:** Passing raw passwords in shell scripts or SQL parameters exposes credentials in process lists, query histories, and logs. The migrated architecture passes a *Secret Key identifier*, which the Cloud Function resolves dynamically at runtime using IAM-secured API calls.

---

## 4. Manual Steps Before Go-Live

Before deploying and executing the migrated components, the following infrastructure and configuration steps must be completed:

### A. Schema & Dataset Creation
Ensure the target dataset exists in your designated region:
```sql
CREATE SCHEMA IF NOT EXISTS `your_project_id.isccr_exporter_dataset`
OPTIONS(location="us");
```

### B. IAM & Permissions
1.  **Cloud Function Service Account:** Create a dedicated Service Account for the Cloud Function (e.g., `cf-ftp-transfer@your_project_id.iam.gserviceaccount.com`).
2.  **Storage Permissions:** Grant this Service Account the **Storage Object Viewer** (`roles/storage.objectViewer`) role on the GCS bucket containing the export files.
3.  **Secret Manager Permissions:** Grant this Service Account the **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) role on the specific FTP password secret.
4.  **BigQuery Connection Service Account:** When you create the BigQuery External Connection, GCP automatically generates a system Service Account. Grant this system account the **Cloud Functions Invoker** (`roles/cloudfunctions.invoker`) role on the deployed Cloud Function.

### C. Secret Manager Setup
Store the target FTP password securely:
```bash
gcloud secrets create ftp-pass-key --replication-policy="automatic"
echo -n "YOUR_ACTUAL_FTP_PASSWORD" | gcloud secrets versions add ftp-pass-key --data-file=-
```
*Pass the string `"ftp-pass-key"` as the `p_SecretKey` parameter when calling `sp_exis_ftp2`.*

### D. BigQuery External Connection
Create the connection that links BigQuery to the Cloud Function:
```bash
gcloud beta biqquery connections create  \
    --connection_type=CLOUD_RESOURCE \
    --location=us \
    --project_id=your_project_id \
    ftp_connection
```

### E. Network & Firewall Routing (VPC Access)
If the target FTP server is behind a private firewall or requires IP whitelisting:
1.  Set up a **Serverless VPC Access Connector** in the Cloud Function's VPC.
2.  Route Cloud Function egress traffic through a **Cloud NAT** configured with a static external IP address.
3.  Provide this static IP address to the target FTP administrator for whitelisting.

---

## 5. Known Gaps & Unresolved References

During analysis, several legacy dependencies could not be resolved automatically. They have been addressed as follows:

| Legacy Reference | Description / Role | Migration Resolution |
| :--- | :--- | :--- |
| `~/.ccr_init` | Environment initialization script. | Replaced by GCP environment variables and BigQuery dataset-level configurations. |
| `h_alis_meldungen.ksh` | Legacy logging utility. | Replaced by the SQL sub-procedure `sp_log_message` writing to `ccr_audit_log`. |
| `h_alis_ftp.ksh` | Legacy FTP wrapper functions. | Replaced by the Python 3.11 Cloud Function (`ftp_transfer_adapter`). |
| Cleartext Passwords | Legacy parameter `$5` (`p_Passwort`). | **Redesign (B4):** Migrated to Google Cloud Secret Manager. The procedure now accepts a secret key name instead of a raw password. |

---

## 6. Validation

To validate the migration, execute a test run of the stored procedure and verify the outputs.

### A. How to Run the Test
Execute the following SQL block in the BigQuery console (replace placeholder values with your test environment details):

```sql
DECLARE v_test_error INT64;

CALL `your_project_id.isccr_exporter_dataset.sp_exis_ftp2`(
  'TEST_RUN_001',                               -- p_EintragsNr
  'gs://your-test-bucket/exports/test_data.csv', -- p_Datei (Source GCS URI)
  'ftp.yourpartner.com',                        -- p_Server
  'test_ftp_user',                              -- p_User
  'ftp-pass-key',                               -- p_SecretKey (Secret Manager ID)
  '/remote/target/dir',                         -- p_Verzeichnis
  'csv',                                        -- p_Endung
  1,                                            -- p_Debug (1 = Active Logging)
  v_test_error                                  -- OUT v_Error
);

SELECT v_test_error AS execution_result;
```

### B. What "Passing" Means
The migration is verified as successful when:
1.  **Execution Result:** The returned `execution_result` is `0` (`k_FertigOK`).
2.  **Remote Verification:** The file exists on the target FTP server under `/remote/target/dir/test_data.csv` and is **not** suffixed with `.tmp`.
3.  **Audit Logs:** Running the following query returns three distinct log entries (`DEBUG` trace, Phase 1/2 progress, and `INFO` completion):
    ```sql
    SELECT * FROM `your_project_id.isccr_exporter_dataset.ccr_audit_log` 
    WHERE eintrags_nr = 'TEST_RUN_001' 
    ORDER BY log_timestamp ASC;
    ```

---

## 7. Rollback Procedure

If a critical issue is discovered in production, revert to the legacy execution path using the following steps:

1.  **Disable Cloud Scheduler / Orchestrator:** Pause the Airflow DAG, Cloud Scheduler job, or Workflows trigger that invokes `sp_exis_ftp2`.
2.  **Re-enable Legacy Cron/Scheduler:** Un-comment or re-enable the legacy crontab entry or scheduling engine task that triggers `k_exis_ftp2.ksh` on the legacy UNIX host.
3.  **Verify Legacy Execution:** Monitor the legacy log files and the target FTP server to ensure files are being transferred and renamed correctly via the legacy `h_alis_ftp.ksh` wrappers.
4.  **Preserve Cloud Logs:** Do not delete the `ccr_audit_log` table or Cloud Function logs, as they will contain the traceback details needed to debug the failure.