# Migration Notes: DW.DWH_ALL_TYPES_MASTER

These migration notes detail the transition of the legacy UC4 job `DW.DWH_ALL_TYPES_MASTER` and its associated scripts (KSH, AWK, Oracle SQL) to Google Cloud Platform (GCP) using Apache Airflow (Cloud Composer), Dataproc Serverless (PySpark), and Google BigQuery.

---

## 1. Summary

The legacy UC4 job `DW.DWH_ALL_TYPES_MASTER` has been migrated from an on-premises Unix/Oracle environment to **Google Cloud Platform (GCP)**. 

This job is a showcase pipeline that combines Ab Initio graph execution, environment initialization, custom AWK data validation/transformation, and Oracle SQL database operations. In the target architecture:
*   **Orchestration** is handled by **Apache Airflow (Cloud Composer)**.
*   **Data Processing and Shell Logic** are executed via **Python 3** running on **Google Cloud Dataproc**.
*   **Database Operations** are executed natively in **Google BigQuery** using BigQuery Scripting.

---

## 2. Generated Artifacts

The migration process generated the following artifacts, preserving the original directory structure where applicable:

| Generated File Path | Target Language / Format | Role / Description |
| :--- | :--- | :--- |
| `DWH_ALL_TYPES_JOB/dw_dwh_all_types_master.py` | Python (Airflow DAG) | The master orchestrator DAG that defines the sequential execution chain, task dependencies, and environment configurations. |
| `dw_init.py` | Python 3 | Replaces the legacy `.dw_init` shell script. Dynamically maps legacy local directory paths to Google Cloud Storage (GCS) bucket paths. |
| `isall/aufbereitung/awk/k_all_types_transform.py` | Python 3 | Replaces `k_all_types_transform.awk`. Validates that each record has exactly 12 fields, prepends `"D;"` to valid records, and raises process-level exit codes (`exit 2`) on failure. |
| `isall/aufbereitung/bin/r_all_types_master.py` | Python 3 | Replaces `r_all_types_master.ksh`. Acts as a checkpoint logging harness to maintain legacy log formats and operational audit trails. |
| `isall/aufbereitung/sql/d_all_types.sql` | BigQuery SQL (Scripting) | Replaces the Oracle SQL script. Implements table truncation, conditional record insertion, transaction control, and error handling. |

---

## 3. Key Design Decisions

### Decoupled Orchestration (Airflow-Native Flow)
*   **Decision:** The legacy KSH script (`r_all_types_master.ksh`) originally invoked SQL\*Plus and AWK as subprocesses. In the migrated architecture, these steps have been decoupled into independent Airflow tasks (`BigQueryInsertJobOperator` and `DataprocSubmitJobOperator`).
*   **Reasoning:** Decoupling these steps provides granular task-level retries, clear logging boundaries in the Airflow UI, and eliminates the need to maintain heavy client binaries (like SQL\*Plus or AWK) inside the execution container.
*   **Trade-off:** The Python script `r_all_types_master.py` now serves solely as a checkpoint logging harness to preserve legacy log outputs, while the actual execution control is delegated to Airflow.

### AWK to Python Conversion
*   **Decision:** Converted `k_all_types_transform.awk` into a standalone Python script (`k_all_types_transform.py`) rather than attempting to model the logic in BigQuery SQL.
*   **Reasoning:** The AWK script acts as a strict data-quality gate. If a row fails validation (field count $\neq$ 12), it must immediately abort the pipeline with exit code `2`. BigQuery SQL is a set-based query engine and cannot raise custom OS-level exit codes mid-stream to fail an external orchestrator. Python preserves this exact procedural control flow.

### BigQuery Scripting for Transaction Control
*   **Decision:** Wrapped the BigQuery SQL statements in `d_all_types.sql` inside a scripting block (`BEGIN...EXCEPTION`) using explicit transactions (`BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`).
*   **Reasoning:** This replicates the Oracle SQL\*Plus error handling directives:
    *   `WHENEVER SQLERROR CONTINUE` for the `TRUNCATE` step is emulated by wrapping the truncate statement in an isolated exception block that swallows errors.
    *   `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` for the `INSERT` step is emulated by wrapping the insert in a transaction block that rolls back and raises an explicit `ERROR()` on failure.

### Environment Path Abstraction
*   **Decision:** Modified the environment initialization logic (`dw_init.py`) to dynamically prefix paths with `gs://{GCS_BUCKET}` if the `GCS_BUCKET` environment variable is present, falling back to local paths otherwise.
*   **Reasoning:** This allows the same initialization module to support both local containerized testing and cloud-native execution on GCS.

---

## 4. Manual Steps Before Go-Live

Before deploying the migrated pipeline to production, the following manual setup steps must be completed:

### 1. Schema and Table Creation
Ensure the target BigQuery tables exist in your target dataset:
*   **Source Raw Table:** `cds$ta_all_types_raw`
*   **Target Staging Table:** `sof$ta_all_types`

### 2. IAM Permissions
The service account running the Cloud Composer DAG and Dataproc Serverless workloads must be granted the following IAM roles:
*   `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target BigQuery datasets.
*   `roles/storage.objectAdmin` on the GCS bucket hosting the scripts and data.
*   `roles/dataproc.worker` to execute PySpark and Python tasks on Dataproc.

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:

```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "GCP_REGION": "your-gcp-region",
  "DATAPROC_CLUSTER": "your-dataproc-cluster-name",
  "GCS_BUCKET": "your-gcs-bucket-name",
  "ALL_TYPES_DIR_EXP_UTL": "your-export-utility-gcs-path"
}
```

### 4. Code Deployment
Upload the generated files to your Cloud Composer GCS bucket:
*   DAG file (`dw_dwh_all_types_master.py`) $\rightarrow$ `gs://{COMPOSER_BUCKET}/dags/`
*   SQL script (`d_all_types.sql`) $\rightarrow$ `gs://{COMPOSER_BUCKET}/dags/isall/aufbereitung/sql/`
*   Python scripts (`dw_init.py`, `k_all_types_transform.py`, `r_all_types_master.py`) $\rightarrow$ `gs://{GCS_BUCKET}/isall/aufbereitung/...` (matching the paths defined in the DAG).

---

## 5. Known Gaps & Unresolved References

*   **Unresolved Environment Files:** The legacy `.dw_init` script sourced `.dw_global` and `.dw_lokal`. These files were not supplied in the migration bundle and were marked as "no source needed" during human review. If these files contained critical environment variables used by downstream tasks, those variables must be manually added to the Airflow Variables configuration.
*   **Upstream PySpark Script:** The DAG task `all_types_master_graph` references `gs://{GCS_BUCKET}/pyspark_scripts/all_types_master.py` (the migrated Ab Initio graph). Ensure this script has been successfully deployed as part of the upstream migration (PR #883 / PR #884).

---

## 6. Validation

To validate the migrated pipeline, perform the following steps:

### How to Run the Tests
1.  Locate the DAG `dw_dwh_all_types_master` in the Airflow Web UI.
2.  Trigger the DAG manually by clicking **Trigger DAG**.
3.  Monitor the execution of the four sequential tasks:
    `all_types_master_graph` $\rightarrow$ `r_all_types_master` $\rightarrow$ `k_all_types_transform` $\rightarrow$ `d_all_types`.

### What "Passing" Means
The migration is considered successful and passing when:
1.  **`all_types_master_graph`** completes successfully, generating the raw export file in GCS.
2.  **`r_all_types_master`** completes with status `SUCCESS` and writes the checkpoint log headers to the daily log file in GCS.
3.  **`k_all_types_transform`** processes the input CSV, verifies that every row contains exactly 12 fields, prepends `"D;"` to each row, and writes the output to `all_types_export.out` with exit code `0`.
4.  **`d_all_types`** executes successfully in BigQuery:
    *   The table `sof$ta_all_types` is truncated.
    *   Records with `status = 'READY'` are successfully copied from `cds$ta_all_types_raw` to `sof$ta_all_types` with the current timestamp.
    *   The transaction commits successfully.

---

## 7. Rollback Procedure

In the event of a failure or unexpected behavior during go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch for `dw_dwh_all_types_master` to **Off** (Paused) to prevent any further automated or manual executions.
2.  **Database Rollback:**
    If the database migration needs to be reverted, run the following BigQuery command to restore the target table to its state prior to the run (or truncate it if it was empty):
    ```sql
    TRUNCATE TABLE `your-gcp-project-id.your-dataset-id.sof$ta_all_types`;
    ```
3.  **Remove Generated Artifacts:**
    Delete or archive the deployed DAG and Python scripts from the GCS buckets to ensure they are not executed accidentally.
4.  **Re-enable Legacy Scheduling:**
    If applicable, reactivate the legacy UC4 job `DW.DWH_ALL_TYPES_MASTER` in the on-premises environment to resume legacy operations.