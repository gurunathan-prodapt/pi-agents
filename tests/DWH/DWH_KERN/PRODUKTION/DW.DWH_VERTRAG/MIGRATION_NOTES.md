# Migration Notes: DW.DWH_VERTRAG_TARIF_SYNC_JP

## 1. Summary
The legacy UC4 Job Plan (`JOBP`) `DW.DWH_VERTRAG_TARIF_SYNC_JP` has been migrated to an Apache Airflow DAG on Google Cloud Composer (GCP). 

This workflow orchestrates the weekly reconciliation (*Abgleich*) of contract and tariff assignments (*Vertrags-/Tarifzuordnung*) between the source Master Data system (*Stammdaten*) and the Core Data Warehouse layer (`DWH_KERN`). 

The migration transitions the orchestration from legacy UC4 scheduling to modern, cloud-native Airflow DAG triggering, while preserving the sequential execution of the underlying synchronization tasks.

---

## 2. Generated Artifacts
The migration process generated the following file:

* **Target DAG File:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py`
  * **Role:** Primary orchestration DAG. It defines the weekly schedule, sets up logical boundaries, and sequentially triggers the child task DAGs using the `TriggerDagRunOperator`.
  * **Legacy Source:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml`

---

## 3. Key Design Decisions

### Orchestration-Only Pattern (Decoupled Execution)
* **Decision:** Instead of embedding Dataproc/PySpark execution logic directly within this master DAG, we utilize the `TriggerDagRunOperator` to call separate, dedicated DAGs for the start and end sync tasks (`dw_dwh_vertrag_tarif_sync_start_js` and `dw_dwh_vertrag_tarif_sync_ende_js`).
* **Reasoning:** The underlying UC4 tasks are migrated as independent components. Decoupling them prevents code duplication, maintains a single source of truth for each job's execution logic, and allows developers to run or test individual sync phases independently.
* **Trade-offs:** This introduces cross-DAG dependencies. To ensure strict sequential execution, `wait_for_completion=True` and `poke_interval=60` are configured on the trigger operators.

### Environment Isolation
* **Decision:** Hardcoded environment variables (such as GCP Project IDs and GCS Bucket names) have been completely avoided.
* **Reasoning:** The DAG dynamically retrieves these values from the Airflow Variable store (`GCP_PROJECT` and `GCS_BUCKET`). This ensures the exact same DAG file can be promoted across Development, UAT, and Production environments without modification.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Configuration
Ensure the following variables are defined in your target Cloud Composer environment's Variable store:
* `GCP_PROJECT`: The GCP Project ID hosting your Dataproc clusters and GCS resources.
* `GCS_BUCKET`: The primary GCS bucket where PySpark scripts and logs are stored.

### 2. IAM & Permissions
* Ensure the Cloud Composer environment's service account has the necessary permissions to trigger other DAGs (typically covered by the `roles/composer.worker` role within the same environment).

### 3. Child DAG Deployment
* The two downstream child DAGs must be deployed and active in the same Airflow environment before enabling this master DAG:
  1. `dw_dwh_vertrag_tarif_sync_start_js`
  2. `dw_dwh_vertrag_tarif_sync_ende_js`

### 4. Scheduling Alignment
* The legacy weekly schedule has been translated to `0 3 * * 0` (Sundays at 03:00 AM UTC). Verify with business stakeholders that this execution window aligns with upstream database availability and downstream reporting requirements.

---

## 5. Known Gaps & Unresolved References

### Upstream Unmigrated Status
* **Status:** The child DAGs (`dw_dwh_vertrag_tarif_sync_start_js` and `dw_dwh_vertrag_tarif_sync_ende_js`) are currently undergoing migration and validation.
* **Impact:** This master orchestration DAG (`dw_dwh_vertrag_tarif_sync_jp`) cannot be successfully run in production until both child DAGs are fully deployed and functional. Attempting to run this DAG without them will result in a failure at the `TriggerDagRunOperator` tasks.

### Legacy Include Files
* Legacy include files (`DW.HOLE_PFAD_VTRG.xml` and `DW.LESE_LOG_VTRG.xml`) are referenced in the legacy metadata. These are assumed to be handled internally by the child PySpark scripts and do not require direct integration within this orchestration DAG.

---

## 6. Validation

### DAG Parsing Test
To verify that the DAG is syntactically correct and can be loaded by the Airflow scheduler, run the following command in your local development or CI/CD environment:
```bash
python dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py
```
* **Passing Criteria:** The command exits with code `0` without throwing any import or syntax errors.

### End-to-End Execution Test
1. Upload the DAG to the Cloud Composer DAGs bucket.
2. Unpause the DAG in the Airflow UI.
3. Trigger a manual run of `dw_dwh_vertrag_tarif_sync_jp`.
4. **Passing Criteria:**
   * The `start` task completes instantly.
   * `trigger_dw_dwh_vertrag_tarif_sync_start_js` successfully triggers `dw_dwh_vertrag_tarif_sync_start_js`, waits for its successful completion, and marks itself as `success`.
   * `trigger_dw_dwh_vertrag_tarif_sync_ende_js` successfully triggers `dw_dwh_vertrag_tarif_sync_ende_js`, waits for its successful completion, and marks itself as `success`.
   * The `end` task completes successfully.
   * If any step fails, the `on_failure_alarm` callback executes and prints the critical failure log.

---

## 7. Rollback Procedure

In the event of an operational failure or unexpected behavior post-deployment:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dw_dwh_vertrag_tarif_sync_jp` to **Off** (Paused). This prevents any further scheduled executions.
2. **Remove the DAG File:**
   Delete the DAG file from the Cloud Composer GCS bucket:
   ```bash
   gcloud storage rm gs://<your-composer-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py
   ```
3. **Re-enable Legacy Scheduling:**
   Re-activate the legacy UC4 Job Plan `DW.DWH_VERTRAG_TARIF_SYNC_JP` to resume weekly contract/tariff synchronization via the legacy infrastructure.