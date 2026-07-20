# MIGRATION_NOTES.md

**Migration Target:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow)  
**Source Workload:** UC4 / Automic Job Plan (`JOBP`)  
**Job Name:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml`  
**Target DAG ID:** `dw_dwh_stamm_knzb_abgl_jp`  

---

## 1. Summary

The legacy UC4 Job Plan `DW.DWH_STAMM_KNZB_ABGL_JP` has been migrated to a native Cloud Composer (Apache Airflow) DAG. This workflow coordinates the daily master data reconciliation of customer numbers and basic access credentials (referred to as **KNZB** / *Kundennummer-/Basiszugangs-Stammdaten*) between the source system **ISTNS** and the Core DWH Layer (*DWH-Kernschicht*).

The migration shifts orchestration from the legacy UC4 engine to Cloud Composer, utilizing a modular, trigger-based execution pattern to run the underlying child tasks as independent DAGs.

---

## 2. Generated Artifacts

The migration process produced the following target file:

*   **`dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/dw_dwh_stamm_knzb_abgl_jp.py`**  
    The primary orchestration DAG. It manages the sequential execution flow of the KNZB master data reconciliation by triggering downstream child DAGs and monitoring their completion.

### Retired / Separately Migrated Files
The following source files are accounted for but are not directly embedded in this orchestration DAG:
*   `DW.DWH_STAMM_KNZB_ABGL_START_JS.xml` (Retired from this file context; migrated as its own autonomous child DAG `dw_dwh_stamm_knzb_abgl_start_js`).
*   `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml` (Retired from this file context; migrated as its own autonomous child DAG `dw_dwh_stamm_knzb_abgl_ende_js`).
*   `includes/DW.HOLE_PFAD_KNZB.xml` & `includes/DW.LESE_LOG_KNZB.xml` (Retired; migrated separately as shared utility functions within the target Python environment).

---

## 3. Key Design Decisions

### Decoupled Orchestration via `TriggerDagRunOperator`
Instead of consolidating all Dataproc/Spark tasks into a single monolithic DAG, we preserved the 1:1 structural modularity of the legacy UC4 workspace. The parent DAG acts strictly as an orchestrator, using `TriggerDagRunOperator` to execute the child DAGs (`dw_dwh_stamm_knzb_abgl_start_js` and `dw_dwh_stamm_knzb_abgl_ende_js`). This approach offers several benefits:
*   **Modularity:** Child jobs can be run, tested, and maintained independently.
*   **Clear Boundaries:** Isolates failures to specific operational phases.
*   **Synchronous Control:** Configured with `wait_for_completion=True` and `poke_interval=60` to guarantee strict sequential execution.

### Concurrency and Safety Guards
*   **`max_active_runs=1`:** Configured to prevent overlapping daily schedules. This acts as an implicit queue guard, protecting the core target tables from concurrent write operations and potential data corruption.
*   **`catchup=False`:** Prevents backfilling historical runs when the DAG is first deployed or unpaused.

### Environment Variable Isolation
To comply with strict environment isolation policies, all infrastructure-level configurations (such as `GCP_PROJECT` and `GCP_REGION`) are fetched dynamically at runtime using Airflow Variables (`Variable.get()`). No environment-specific values are hardcoded.

### Preservation of Legacy Metadata
In accordance with the *Output/Print Literal Rule*, original German descriptions, titles, and logging structures have been preserved character-for-character within the DAG metadata to maintain operational continuity for the support teams.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the orchestration DAG, the following manual setup steps must be completed in the target Cloud Composer environment:

### 1. Airflow Variables Configuration
Ensure the following variables are defined in the Airflow Metadata Database (via the Airflow UI or CLI):
*   `GCP_PROJECT`: The ID of the Google Cloud Project hosting the Dataproc clusters and BigQuery datasets.
*   `GCP_REGION`: The GCP region where the workloads execute (e.g., `europe-west3`).

### 2. IAM & Permissions
The Cloud Composer Service Account (typically the GKE worker service account) must possess the following IAM roles:
*   **Composer User / Worker** (`roles/composer.worker`)
*   **Airflow RBAC Permissions:** Ensure the service account has permissions to trigger and monitor DAG runs (`TriggerDagRunOperator` requires appropriate DAG-level access if fine-grained access control is enabled).

### 3. Downstream DAG Deployment
The orchestration DAG cannot succeed without its child DAGs. Ensure that the following DAGs are deployed and active in the Composer environment *before* enabling the parent DAG:
1.  `dw_dwh_stamm_knzb_abgl_start_js`
2.  `dw_dwh_stamm_knzb_abgl_ende_js`

### 4. Scheduling Verification
The DAG is configured with a daily cron schedule of `0 3 * * *` (03:00 UTC) based on business requirements. Verify this execution window against the legacy operations database to ensure it does not conflict with upstream data availability from the `ISTNS` source system.

---

## 5. Known Gaps & Unresolved References

*   **Missing Scheduler Definition (`EVNT_TIME`):** The original UC4 scheduler file was not provided. The daily schedule (`0 3 * * *`) is a best-effort placeholder and must be verified during the integration phase.
*   **Dependency on External DAGs:** The orchestration DAG has hardcoded references to `trigger_dag_id='dw_dwh_stamm_knzb_abgl_start_js'` and `trigger_dag_id='dw_dwh_stamm_knzb_abgl_ende_js'`. Any changes to the IDs of the child DAGs will break this orchestrator.
*   **Shared Includes:** Legacy include scripts (`DW.HOLE_PFAD_KNZB` and `DW.LESE_LOG_KNZB`) are assumed to be handled internally by the child DAGs or packaged as common Python modules. If these utilities require global variables, they must be added to the Airflow Variable store.

---

## 6. Validation

To validate the migration, perform the following tests in a non-production environment:

### 1. DAG Parsing Test
Run a local syntax and import check on the DAG file:
```bash
python dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/dw_dwh_stamm_knzb_abgl_jp.py
```
*Passing Criteria:* The command terminates with exit code `0` and outputs no errors or traceback logs.

### 2. Airflow CLI Validation
Verify that the Airflow scheduler can successfully parse and list the DAG:
```bash
airflow dags list | grep dw_dwh_stamm_knzb_abgl_jp
```
*Passing Criteria:* The DAG ID `dw_dwh_stamm_knzb_abgl_jp` is displayed in the output.

### 3. End-to-End Execution Test
Manually trigger the DAG via the Airflow UI or CLI:
```bash
airflow dags trigger dw_dwh_stamm_knzb_abgl_jp
```
*Passing Criteria:*
1.  The `start` task completes immediately.
2.  The `dw_dwh_stamm_knzb_abgl_start_js` task triggers the child DAG, polls its status, and completes successfully once the child DAG finishes.
3.  The `dw_dwh_stamm_knzb_abgl_ende_js` task triggers its respective child DAG, polls its status, and completes successfully.
4.  The `end` task completes, and the overall DAG run status is marked as `SUCCESS`.
5.  If any task fails, the `on_failure_alarm` callback executes and prints the failure context to the task logs.

---

## 7. Rollback Procedure

In the event of an operational failure or data inconsistency during go-live, execute the following rollback steps:

1.  **Pause the Orchestration DAG:**
    Immediately pause the parent DAG in the Airflow UI or via the CLI to prevent subsequent scheduled runs:
    ```bash
    airflow dags pause dw_dwh_stamm_knzb_abgl_jp
    ```
2.  **Pause Child DAGs:**
    If necessary, pause the child DAGs to prevent manual or accidental triggers:
    ```bash
    airflow dags pause dw_dwh_stamm_knzb_abgl_start_js
    airflow dags pause dw_dwh_stamm_knzb_abgl_ende_js
    ```
3.  **Remove DAG Files (Optional):**
    If a complete code rollback is required, delete the DAG file from the Cloud Composer GCS bucket:
    ```bash
    gsutil rm gs://<composer-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/dw_dwh_stamm_knzb_abgl_jp.py
    ```
4.  **Re-enable Legacy Scheduling:**
    Re-activate the corresponding Job Plan (`DW.DWH_STAMM_KNZB_ABGL_JP`) in the UC4/Automic system to resume legacy operations.
5.  **Data Cleanup:**
    If the rollback is due to corrupted data in the Core DWH Layer, coordinate with the database administration team to restore the affected KNZB tables to their pre-execution state.