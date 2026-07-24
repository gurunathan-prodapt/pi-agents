# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## 1. Summary
The UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to an Apache Airflow DAG on Google Cloud Composer. This job originally functioned as a dummy/placeholder task within the `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` workflow, performing no operational database or data-lake tasks other than printing a status message.

## 2. Generated Artifacts
The following file was generated as part of this migration:

| Artifact Path | Type | Role |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Python (Airflow DAG) | Defines the Airflow DAG and executes the dummy task using a `BashOperator`. |

## 3. Key Design Decisions
* **Use of `BashOperator` over `DataprocSubmitJobOperator`:** Although the standard migration template often wraps tasks in PySpark jobs executed on Dataproc, doing so for a dummy task that only prints a message would be highly inefficient and costly. A lightweight `BashOperator` was chosen to execute a simple `echo` command, saving cluster resources and execution time.
* **Preservation of Legacy Logs:** To maintain parity with the legacy system, the exact string (including the typo) `Doing nothinig` is printed to the standard output.
* **Dynamic Environment Configuration:** Environment-specific variables (`GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET`) are retrieved dynamically from the Airflow Variable store to prevent hardcoding and ensure portability across environments (Dev, Test, Prod).

## 4. Manual Steps Before Go-Live
Before deploying and enabling this DAG in a production environment, the following setup must be completed:

1. **Airflow Variables:**
   Ensure the following variables are configured in the Airflow metadata database (via the Airflow UI or CLI):
   * `GCP_PROJECT`: The Google Cloud Project ID.
   * `GCP_REGION`: The default GCP region (e.g., `europe-west3`).
   * `GCS_BUCKET`: The GCS bucket used for environment storage.

2. **IAM & Permissions:**
   * Ensure the service account running the Cloud Composer environment has basic execution permissions (though this specific task only runs a local bash command and requires no external GCP resource access).

3. **Scheduling & Integration:**
   * This DAG is currently configured with `schedule_interval=None` because it is designed to be triggered as part of a larger parent workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). Once the parent workflow is migrated, establish the appropriate trigger mechanism (e.g., `TriggerDagRunOperator` or dataset-based scheduling).

## 5. Known Gaps & Unresolved References
* **Missing Parent Context:** Because only the individual `JOBS_UNIX` file was provided, the overall orchestration context (upstream triggers, downstream consumers, and specific execution schedules) must be configured once the parent Job Plan (`JOBP`) is migrated.

## 6. Validation
To validate the migration of this task:
1. **DAG Parse Test:** Ensure the DAG is successfully parsed by Airflow without syntax or import errors.
2. **Manual Execution:** Trigger the DAG manually from the Airflow UI.
3. **Log Verification:** 
   * Verify that the `dw_dwh_dummy_absd_plato_tarife` task completes with a `success` status.
   * Inspect the task logs and confirm that the line `Doing nothinig` is printed to the standard output.

## 7. Rollback Procedure
In the event of a failure or the need to revert to the legacy system:
1. **Pause the Airflow DAG:** Turn off the toggle switch for `dw_dwh_dummy_absd_plato_tarife` in the Airflow UI to prevent further executions.
2. **Re-enable UC4 Job:** Reactivate the `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` active flag in the UC4 environment.