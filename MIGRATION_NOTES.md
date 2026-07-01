# MIGRATION NOTES — JOB: EXIS

This document provides comprehensive technical notes for the migration of the legacy **EXIS** data exporter pipeline from an on-premise Oracle Data Warehouse to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy **EXIS** pipeline consists of four primary export tasks that extract master data, option associations, GPRS data, voice connections, and contract discounts from an Oracle Data Warehouse. Historically, these tasks were orchestrated by a UC4 (Automic) scheduler and executed via a heavy, custom, 83,000-line KornShell framework script (`r_exis_v2`) that relied on `SQL*Plus`, `nawk` formatting, `gzip` compression, and command-line SFTP.

This pipeline has been migrated to a **GCP-native, serverless architecture**:
*   **Orchestration**: Managed by Google Cloud Composer (Apache Airflow 2).
*   **Data Extraction**: Executed directly in Google BigQuery using standard SQL.
*   **Post-Processing & Formatting**: Handled by a lightweight, containerized Python script that streams data from Google Cloud Storage (GCS), appends legacy-compliant trailer records, and compresses the output.
*   **Secure Distribution**: Handled by the Airflow `SFTPOperator` using credentials stored securely in Google Secret Manager.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy KornShell framework and Oracle SQL queries:

| Relative Path | Language | Role / Description |
| :--- | :--- | :--- |
| `gcp_migration/dags/dw_dwh_exis_export_pipeline.py` | Python / Airflow | Consolidated Cloud Composer DAG that orchestrates the extraction, staging, post-processing, SFTP transfer, and cleanup for all four export pipelines. |
| `gcp_migration/exporter/apt/bin/add_trailer_and_compress.py` | Python | Auxiliary post-processing script. Streams raw CSV data from GCS, calculates row counts, appends the legacy-compliant trailer record, compresses the file to `.gz`, and writes it back to GCS. |
| `gcp_migration/exporter/apt/sql/d_exis_apt_bestandsdaten.sql` | BigQuery SQL | Re-engineered stock data query. Replaces Oracle-specific syntax (e.g., `LISTAGG`) with BigQuery-compatible functions (`STRING_AGG`). |
| `gcp_migration/exporter/apt/sql/d_exis_apt_nna_daten.sql` | BigQuery SQL | Re-engineered GPRS data query. Replaces Oracle string concatenation (`||`) and date formatting with standard BigQuery equivalents. |
| `gcp_migration/exporter/apt/sql/d_exis_apt_nna_voice.sql` | BigQuery SQL | Re-engineered voice data query. Modernizes date filters, integer division, and complex conditional logic. |
| `gcp_migration/exporter/apt/sql/d_exis_apt_rabattdaten.sql` | BigQuery SQL | Re-engineered discount reporting query. Strips Oracle parallel hints and converts implicit joins to explicit ANSI joins. |

---

## 3. Key Design Decisions

### Consolidation of Orchestration
*   **Decision**: Consolidate four separate UC4 jobs into a single Airflow DAG (`dw_dwh_exis_export_pipeline`) containing four parallel execution pipelines.
*   **Trade-off**: While consolidating simplifies monitoring and reduces DAG clutter, it combines daily and monthly tasks into one file. This is mitigated by using dynamic parameterization and can be further refined with conditional execution branches.

### Serverless Post-Processing
*   **Decision**: Replace the 83,000-line KSH framework (`r_exis_v2`) and `nawk` with a modular Python script (`add_trailer_and_compress.py`) executed via Airflow's `PythonOperator`.
*   **Reasoning**: This eliminates the need to maintain legacy shell scripts or provision persistent VM instances (like Compute Engine or Dataproc) simply to format and compress text files.

### GCS-to-Local Staging for SFTP
*   **Decision**: Use `GCSToLocalFilesystemOperator` to stage files in the Airflow worker's local `/tmp` directory before executing the `SFTPOperator`, followed by an explicit `BashOperator` cleanup task.
*   **Reasoning**: Airflow's native `SFTPOperator` requires a local file path for the `put` operation. Staging files locally in `/tmp` is highly performant but requires strict cleanup tasks to prevent worker disk exhaustion.

### Safe String Concatenation
*   **Decision**: Wrap all concatenated fields in `COALESCE(field, '')` within the BigQuery SQL scripts.
*   **Reasoning**: In BigQuery, concatenating a `NULL` value with other strings results in a `NULL` output for the entire expression. Coalescing fields ensures that missing values do not corrupt or blank out entire rows in the exported CSV.

---

## 4. Manual Steps Before Go-Live

Before enabling and running the migrated pipeline in production, the following setup steps must be completed:

### 1. BigQuery Dataset & Table Verification
Ensure that the target dataset (configured in Airflow variables as `bq_dataset_raw`, defaulting to `prod_dwh_raw_dataset`) exists and that all source tables/views are fully populated:
*   `RPT$TA_S_D1_VERTRAG`
*   `SOF$TA_BPR_OPTIONEN`
*   `SOF$VI_L_OPTIONZUORDNUNG`
*   `DWH$VI_L_MAP_FA_TARIF`
*   `BL_D_TARIF`
*   `DWH$VI_C_VERTRAG`
*   `DWH$TA_F_NNV_GPRS`
*   `DWH$VI_F_NNV_TVD_12_MONATE`
*   `DWH$VI_L_TVD_LEISTUNGSKLASSE`
*   `RPT$TA_S_D1_DISCOUNT_RR`

### 2. GCS Bucket Creation
Create the temporary and long-term storage buckets in the same region as your BigQuery dataset:
*   **Temp Bucket**: `prod-dwh-exporter-temp` (used for raw CSV spools)
*   **Store Bucket**: `prod-dwh-exporter-store` (used for final compressed `.gz` files)

### 3. IAM & Permissions
The Service Account running the Cloud Composer environment must be granted the following IAM roles:
*   `BigQuery Admin` (or `BigQuery Data Editor` + `BigQuery Job User`)
*   `Storage Object Admin` on both the temp and store GCS buckets.

### 4. Airflow Variables Configuration
Register the following variables in the Airflow UI (**Admin -> Variables**) or via Secret Manager:

```json
{
  "gcp_project_id": "prod-dwh-gcp-project",
  "bq_dataset_raw": "prod_dwh_raw_dataset",
  "gcs_temp_bucket": "prod-dwh-exporter-temp",
  "gcs_store_bucket": "prod-dwh-exporter-store",
  "sftp_connection_id": "ssh_sftp_apt_receiver",
  "sftp_remote_dir": "/incoming/apt_exports",
  "airflow_owner": "data_engineering_exports"
}
```

### 5. SFTP Connection Setup
In the Airflow UI, navigate to **Admin -> Connections** and create a connection with the ID `ssh_sftp_apt_receiver`:
*   **Conn Type**: `SFTP`
*   **Host**: Target SFTP server hostname or IP.
*   **Username**: SFTP system user.
*   **Password** or **Extra**: Provide the password or SSH private key (e.g., `{"key_file": "/home/airflow/gcs/data/sftp_private_key.pem"}`) to authenticate securely.

---

## 5. Known Gaps & Unresolved References

### 1. Scheduling Split (Redesign Item B4)
*   **Gap**: The legacy UC4 system runs the Stock (`BESTANDS`) and Discount (`RABATT`) exports **daily**, while the GPRS (`NNA_DATA`) and Voice (`NNA_VOIC`) exports run **monthly**. The consolidated Airflow DAG currently defines all four pipelines in a single file with `schedule_interval=None`.
*   **Resolution Plan**: 
    *   *Option A*: Split the single DAG file into two separate DAG files: `dw_dwh_exis_daily_pipeline.py` and `dw_dwh_exis_monthly_pipeline.py`.
    *   *Option B*: Implement an Airflow `BranchPythonOperator` at the start of the DAG to evaluate the execution date and skip the monthly pipelines if the run is not a designated monthly execution.

### 2. Local Disk Space Constraints
*   **Gap**: Staging very large files in the Airflow worker's `/tmp` directory before SFTP transfer carries a risk of disk exhaustion if multiple runs execute concurrently.
*   **Resolution Plan**: Monitor worker disk utilization. If file sizes exceed 5GB, replace the local staging and `SFTPOperator` steps with a custom Kubernetes Pod Operator or a Cloud Function that streams data directly from GCS to SFTP without local disk writes.

---

## 6. Validation

To validate the migrated pipeline, perform the following steps:

### How to Run the Tests
1. Upload the DAG and the auxiliary scripts to your Cloud Composer environment's DAGs folder.
2. Navigate to the Airflow UI and locate `dw_dwh_exis_export_pipeline`.
3. Trigger the DAG manually using **Trigger DAG w/ config**.

### What "Passing" Means
The migration is successful if all tasks in the DAG run complete with a `success` status, verifying the following criteria:
1.  **Data Extraction**: BigQuery jobs complete successfully, creating temporary tables in the raw dataset.
2.  **File Spooling**: Raw CSV files are written to the temp GCS bucket with `|` delimiters and no headers.
3.  **Post-Processing**: The Python post-processor successfully appends a trailer record matching the legacy format:
    `X|<filename>|<from_date>|<row_count>|<report_type>|<sysdate>`
    *(Note: The row count in the trailer must exactly equal the number of data rows, excluding the trailer itself).*
4.  **Compression**: The final file is correctly compressed using gzip (`.csv.gz`) and uploaded to the store bucket.
5.  **SFTP Delivery**: The compressed file is successfully transferred to the remote SFTP directory.
6.  **Cleanup**: The local `/tmp` file is successfully deleted from the Airflow worker.

---

## 7. Rollback Procedure

If critical errors are encountered in production, execute the following rollback steps:

1.  **Pause the Airflow DAG**: Navigate to the Airflow UI and toggle the switch for `dw_dwh_exis_export_pipeline` to **Off**.
2.  **Re-enable Legacy UC4 Jobs**: In the UC4 (Automic) scheduler, re-activate the following jobs:
    *   `DW.DWH_EXIS_SD_APT_NNA_DATA`
    *   `DW.DWH_EXIS_SD_APT_NNA_VOIC`
    *   `DW.DWH_EXIS_SD_APT_BESTANDS`
    *   `DW.DWH_EXIS_SD_APT_RABATT`
3.  **Verify Legacy Infrastructure**: Ensure that the legacy Unix execution host and the Oracle Data Warehouse connections are fully operational.
4.  **Target Directory Cleanup**: Connect to the target SFTP server and remove any partially transferred or corrupted files generated by the failed GCP run to prevent downstream processing errors.