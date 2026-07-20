# MIGRATION NOTES

## 1. Summary
This document details the migration of the legacy UC4 Job Plan (`JOBP`) **`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`** to **Google Cloud Composer (Airflow 2.x)**. 

The purpose of this workflow is the monthly consolidation of revenue data (`UMSATZ`) across all corporate entities. It orchestrates the execution of a legacy data transformation process (originally an Ab Initio graph) now targeted to run as a PySpark application on Google Cloud Dataproc.

### Legacy Metadata & Documentation
*   **German Title:** `"Monatliche Konsolidierung der Umsatzdaten (UMSATZ) ueber alle Konzerngesellschaften"`
*   **German Description:** `"Jobplan zur monatlichen Konsolidierung der Umsatzdaten ueber alle Konzerngesellschaften. Ruft ein Legacy-Ab-Initio-Graph auf, das noch aus der Erstmigration stammt."`
*   **Original Schedule:** Monthly execution on the 1st of every month at midnight (`0 0 1 * *`).

---

## 2. Generated Artifacts
The migration process generated the following target files:

1.  **Airflow DAG File:**
    *   **Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_jp.py`
    *   **Role:** Orchestrates the entire monthly workflow. It defines the execution schedule, manages cross-DAG dependencies using sensors, submits the core processing job to Google Cloud Dataproc, and handles execution failures.
2.  **PySpark Script Target (Placeholder/Reference):**
    *   **Path:** `gs://[GCS_BUCKET]/pyspark_scripts/dw_dwh_umsatz_konsolidierung_monatlich_js.py`
    *   **Role:** Executes the actual revenue consolidation logic. This replaces the legacy Ab Initio graph (`umsatz_konsolidierung.mp`) and its corresponding KSH wrapper (`r_umsatz_konsolidierung_monatlich.ksh`).

---

## 3. Key Design Decisions

*   **Folder Integrity Rule:** The target DAG file is placed in `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/` to mirror the legacy UC4 folder structure exactly, ensuring maintainability and clear ownership.
*   **Decoupled Dependency Management:** Instead of merging all upstream tasks into a single monolithic DAG, we implemented **`ExternalTaskSensor`** tasks. This allows the monthly consolidation DAG to remain modular and only run when its independent upstream prerequisites have completed successfully.
*   **Dataproc Serverless / Cluster Submission:** The legacy UNIX job execution is mapped to a **`DataprocSubmitJobOperator`**. This submits a PySpark job to a managed Dataproc cluster, passing the execution date (`{{ ds }}`) dynamically.
*   **Strict Environment Isolation (No Prose Placeholders):** In compliance with migration rules, all environment-specific configurations (GCP Project, Region, Cluster Name, and GCS Bucket) are fetched dynamically at runtime using Airflow Variables (`Variable.get()`). No hardcoded environment placeholders exist in the code.
*   **Concurrency Control:** Set `max_active_runs=1` to replicate the UC4 queue/sync behavior, preventing multiple monthly runs from executing concurrently and causing data state conflicts.

---

## 4. Manual Steps Before Go-Live

Before enabling this DAG in production, the following configuration and infrastructure steps must be completed:

### 1. Airflow Variables Setup
Ensure the following Airflow variables are configured in the target Cloud Composer environment:
*   `GCP_PROJECT`: The ID of your Google Cloud Project.
*   `GCP_REGION`: The GCP region where Dataproc resources are located (e.g., `europe-west3`).
*   `DATAPROC_CLUSTER`: The name of the active Dataproc cluster.
*   `GCS_BUCKET`: The GCS bucket name where PySpark scripts and assets are stored (without the `gs://` prefix).

### 2. IAM & Permissions
The Cloud Composer environment's service account must have the following IAM roles:
*   `roles/dataproc.editor` (to submit jobs to the Dataproc cluster)
*   `roles/storage.objectViewer` (to read the PySpark script from GCS)
*   `roles/bigquery.admin` or equivalent dataset-level write permissions (to write consolidated data to BigQuery)

### 3. Code Deployment
*   Deploy the DAG file to the Composer DAGs bucket: `gs://[COMPOSER_DAGS_BUCKET]/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_jp.py`.
*   Upload the finalized PySpark consolidation script to GCS: `gs://[GCS_BUCKET]/pyspark_scripts/dw_dwh_umsatz_konsolidierung_monatlich_js.py`.

### 4. Upstream DAG Verification
Verify that the following upstream DAGs are deployed and active:
*   `dw_dwh_abrechnung_reformat_js`
*   `dw_dwh_kunde_abgl_woechentlich_js`
*   `dw_dwh_rechnung_export_taeglich_js`
*   `dw_dwh_tarifhist_scd_monatlich_js`

---

## 5. Known Gaps & Unresolved References

*   **Missing Child Job Definition (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS`):**
    The source XML did not contain the definition for the underlying UNIX job. The PySpark script target, arguments, and execution parameters have been modeled based on standard migration patterns. This script must be thoroughly tested and verified against the legacy Ab Initio graph logic.
*   **Unmigrated Upstream Dependencies:**
    The four upstream jobs monitored by the `ExternalTaskSensor` tasks are not yet migrated to Airflow. If this DAG is enabled before those upstream DAGs exist, the sensors will time out and fail. 
    *   *Workaround for early testing:* Temporarily comment out or mock the sensor tasks in the dependency chain.
*   **ENDED_SKIPPED Behavior Gap:**
    UC4's native "ENDED_SKIPPED" pass-through behavior has no direct equivalent in Airflow without introducing complex custom trigger rules. Standard `all_success` trigger rules are used. If an upstream DAG is skipped, the corresponding sensor will wait until timeout unless explicit skip-propagation logic is added.

---

## 6. Validation

To validate the migration, perform the following tests in a non-production environment:

### 1. DAG Syntax & Parsing Test
Run a local syntax check on the DAG file to ensure there are no import or structural errors:
```bash
python dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_jp.py
```
*Passing criteria:* The command completes with exit code `0` and outputs no errors.

### 2. Airflow UI Validation
*   Navigate to the Airflow UI.
*   Verify that the DAG `dw_dwh_umsatz_konsolidierung_monatlich_jp` appears in the DAGs list without import errors.
*   Verify that the DAG Graph View matches the expected dependency structure:
    `start` -> `[sensors...]` -> `dw_dwh_umsatz_konsolidierung_monatlich_js` -> `end`.

### 3. Dry-Run / Mock Integration Test
1.  Temporarily set the `ExternalTaskSensor` tasks to `allowed_states=["success", "skipped"]` or mock their success.
2.  Trigger the DAG manually via the Airflow UI.
3.  *Passing criteria:* 
    *   The DAG starts successfully.
    *   The `DataprocSubmitJobOperator` successfully submits the job to the Dataproc cluster.
    *   The PySpark job executes and completes successfully.
    *   The DAG execution status transitions to `Success`.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, follow these rollback steps:

1.  **Pause the Airflow DAG:**
    Immediately pause the DAG `dw_dwh_umsatz_konsolidierung_monatlich_jp` in the Airflow UI to prevent any further scheduled executions.
2.  **Remove the DAG File:**
    Delete the DAG file from the Composer DAGs bucket to completely remove it from the scheduler:
    ```bash
    gsutil rm gs://[COMPOSER_DAGS_BUCKET]/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_jp.py
    ```
3.  **Clean Up Partial Data (If Applicable):**
    If the Dataproc job failed mid-execution, inspect the target BigQuery tables and revert/delete any partially loaded or corrupted monthly consolidation data for the affected execution date.
4.  **Re-enable Legacy Scheduling:**
    Re-activate the legacy UC4 Job Plan `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP` in the UC4 production client to resume legacy operations.