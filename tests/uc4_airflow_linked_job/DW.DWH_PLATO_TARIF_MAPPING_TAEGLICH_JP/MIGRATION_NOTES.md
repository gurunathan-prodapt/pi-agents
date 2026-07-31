# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the UC4/Automic job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Apache Airflow on Google Cloud Platform (Cloud Composer).

---

## 1. Summary

The active standalone UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to a native Apache Airflow DAG. 

In the legacy environment, this job functioned as a placeholder or dummy task executing a basic UC4 script print statement (`:print Doing nothinig`) on the target UNIX host `DWHDWH1P` under the login credentials `DW.UNIX.ISTNS`. Because no parent Workflow (`JOBP`), Schedule (`JSCH`), or native trigger Script (`SCRI`) was supplied within the extraction bundle, this job has been converted into an independent, single-task Airflow DAG configured for manual or external triggering.

* **Source Platform:** UC4 / Automic (JOBS_UNIX)
* **Target Platform:** Apache Airflow (Cloud Composer / GCP)
* **Target DAG ID:** `dw_dwh_dummy_absd_plato_tarife`

---

## 2. Generated Artifacts

The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Airflow DAG | Python definition file containing the DAG and its single `EmptyOperator` task representing the dummy synchronization point. |

---

## 3. Key Design Decisions

* **Mapping to `EmptyOperator`:** The legacy job's execution body consisted of an internal UC4 script command (`:print Doing nothinig`) rather than a standard UNIX shell script. Because this is a non-operational dummy command, the task was mapped to an Airflow `EmptyOperator`. This avoids unnecessary resource consumption (such as spinning up GKE pods or SSH sessions) while preserving the task's structural role in the orchestration sequence.
* **Standalone DAG Structure:** Since the extraction bundle did not contain a parent workflow (`JOBP`), the job was wrapped in its own standalone DAG. 
* **Manual Scheduling (`schedule=None`):** No calendar-based trigger rules were present in the source metadata. The DAG is configured with `schedule=None` to prevent accidental scheduled runs, relying instead on manual execution or external triggers.
* **Concurrency Control:** Configured `max_active_runs=1` as a standard execution guard to prevent multiple concurrent instances of this dummy task from running simultaneously.
* **Preservation of Legacy Typo:** In accordance with strict lineage rules, the typo in the legacy print statement (`Doing nothinig`) has been preserved in the DAG's comments and documentation to maintain character-for-character fidelity with the legacy system.

---

## 4. Manual Steps Before Go-Live

Before deploying this DAG to production, the following administrative and configuration steps must be completed:

### Schema & Dataset Creation
* No BigQuery datasets, Cloud Storage buckets, or database schemas are required for this specific dummy task.

### IAM & Permissions
* Ensure that the Cloud Composer service account has the necessary permissions to parse and execute the DAG. 
* If this task is later redesigned to execute actual commands on a remote host, ensure the appropriate IAM roles and SSH keys are configured.

### Connection Strings & Secrets
* **Legacy Host (`DWHDWH1P`):** If remote execution is eventually restored, define an Airflow SSH Connection (e.g., `ssh_dwhdwh1p`) pointing to the target host.
* **Legacy Login (`DW.UNIX.ISTNS`):** Map these credentials to an Airflow Connection or Secret if remote authentication becomes necessary.

### Scheduling & Cross-DAG Linkage
* Because this DAG is un-scheduled (`schedule=None`), you must manually configure its integration with the downstream consumer `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` once that workflow is migrated. This can be achieved using:
  * A `TriggerDagRunOperator` in an upstream DAG.
  * An `ExternalTaskSensor` in the downstream DAG.

---

## 5. Known Gaps & Unresolved References

* **Downstream Dependency Gap:** The downstream workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` was not included in this migration bundle and is currently marked as *not yet migrated*. The cross-DAG dependency linkage cannot be finalized until that workflow is deployed to the Airflow environment.
* **Redesign / Deprecation Candidate (B4):** Because this job performs no functional work (it is a dummy task), system architects should review whether this DAG is still required in the target cloud environment. If its only purpose was legacy synchronization, it may be a candidate for complete deprecation.

---

## 6. Validation

To validate the migrated DAG, perform the following steps:

### How to Run the Tests
1. Copy the generated DAG file `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` to your Cloud Composer environment's DAGs folder (e.g., `gs://<composer-bucket>/dags/`).
2. Navigate to the Airflow UI and verify that the DAG `dw_dwh_dummy_absd_plato_tarife` appears in the DAGs list without any import errors.
3. Manually trigger the DAG by clicking the **Trigger DAG** button in the Airflow UI, or run the following CLI command:
   ```bash
   gcloud composer environments run <env-name> \
       --location <location> \
       dags trigger -- dw_dwh_dummy_absd_plato_tarife
   ```

### What "Passing" Means
* The DAG run must transition to a green `success` state.
* The task `dw_dwh_dummy_absd_plato_tarife` must complete successfully.
* Task logs must show clean execution with no errors or warnings.

---

## 7. Rollback Procedure

If issues arise after deploying this DAG, execute the following rollback steps:

1. **Pause the DAG:** In the Airflow UI, toggle the active switch for `dw_dwh_dummy_absd_plato_tarife` to **Off** to prevent any further manual or external executions.
2. **Remove the DAG File:** Delete the DAG file from the Cloud Composer GCS bucket:
   ```bash
   gsutil rm gs://<composer-bucket>/dags/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```
3. **Revert Git Repository:** Revert the commit that introduced the DAG file to your CI/CD deployment pipeline.
4. **Remove Dependencies:** If any temporary cross-DAG sensors or triggers were established in other active DAGs to reference this dummy task, disable or revert those changes to prevent broken dependency chains.