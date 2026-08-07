# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## 1. Summary
This migration transfers the legacy UC4 (Automic) UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Apache Airflow (Cloud Composer) on Google Cloud Platform (GCP). 

The source job was a standalone utility/dummy task that executed a native UC4 scripting command (`:print Doing nothinig`) rather than a standard operating system command or database script. In the target Airflow environment, this has been converted into a standalone, single-task DAG (`dw_dwh_dummy_absd_plato_tarife`) configured to run on demand (`schedule=None`).

---

## 2. Generated Artifacts
The migration process generated the following file:

*   **Target File Path**: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`
    *   **Role**: Airflow DAG definition file.
    *   **Contents**: Defines the DAG `dw_dwh_dummy_absd_plato_tarife` and instantiates a `BashOperator` task to replicate the legacy print statement.

---

## 3. Key Design Decisions

### Operator Selection
*   **Decision**: Use `BashOperator` executing `echo 'Doing nothinig'` instead of an `EmptyOperator`.
*   **Reasoning**: While the initial design analysis suggested an `EmptyOperator` because `:print` is a native UC4 scripting command, using a `BashOperator` preserves the exact operational behavior and logging of the legacy job. This ensures that operators monitoring execution logs see the expected output (`Doing nothinig`), maintaining consistency with legacy runbooks.

### Standalone DAG Wrapper
*   **Decision**: Wrap the job in a standalone DAG with `schedule=None`.
*   **Reasoning**: No parent Jobplan (`JOBP`) or schedule triggers were provided in the extraction bundle. Treating this as an externally triggered standalone DAG prevents accidental scheduled runs while keeping the workflow deployable and testable.

### Retirement of Legacy Infrastructure
*   **Decision**: Retire the legacy Unix host `DWHDWH1P` and login credentials `DW.UNIX.ISTNS` for this task.
*   **Reasoning**: Because the task is executed natively within the Airflow worker environment via a basic bash command, remote SSH connections and OS-level user profiles are no longer required, reducing the security and maintenance footprint.

### Idempotency and Restartability
*   **Decision**: Set `depends_on_past=False` and allow standard task retries.
*   **Reasoning**: Legacy documentation explicitly states: *"Wiederanlauf ohne weitere Maßnahmen möglich"* (Restart possible without further measures). This maps directly to Airflow's default behavior of allowing clean task clearing and retries without historical execution dependencies.

---

## 4. Manual Steps Before Go-Live

### Environment Variables & Airflow Variables
Ensure the following global Airflow variables are configured in the target Cloud Composer environment:
*   `GCP_PROJECT`: The ID of the target GCP project.
*   `GCP_REGION`: The target GCP region (e.g., `europe-west3`).

### IAM & Permissions
No specialized IAM permissions or service accounts are required for this DAG, as it executes a local echo command within the Airflow worker container.

### Scheduling & Triggering
Because the DAG is configured with `schedule=None`, you must establish how this workflow will be triggered in production:
*   If triggered by an external system, configure the appropriate API service account permissions to allow calling the Airflow Trigger DAG Run API.
*   If this job is meant to be part of a larger orchestration chain, prepare to trigger it via a `TriggerDagRunOperator` from an upstream DAG.

---

## 5. Known Gaps & Unresolved References

### Downstream Dependency Gap
*   **Reference**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`
*   **Status**: Unmigrated.
*   **Gap**: The legacy job has a downstream relationship with `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`. Because the downstream target is not yet migrated, cross-DAG wiring cannot be finalized.
*   **Resolution**: Once the downstream workflow is migrated to Cloud Composer, establish the connection using either:
    1.  An `ExternalTaskSensor` in the downstream DAG monitoring this DAG.
    2.  A `TriggerDagRunOperator` at the end of this DAG to trigger the downstream workflow.

### Dummy Command Verification
*   **Gap**: Confirm with business and operations teams that the legacy `:print Doing nothinig` statement was purely a placeholder and did not trigger undocumented side effects (such as legacy file watchers or event triggers) in the source environment.

---

## 6. Validation

### Deployment Testing
1.  Upload `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` to the Cloud Composer DAGs folder in the target environment.
2.  Navigate to the Airflow UI and verify that the DAG `dw_dwh_dummy_absd_plato_tarife` appears and parses without syntax or import errors.

### Execution Testing
1.  Manually trigger the DAG via the Airflow UI or the Airflow CLI:
    ```bash
    gcloud composer environments run <ENVIRONMENT_NAME> \
        --location <LOCATION> \
        dags trigger -- dw_dwh_dummy_absd_plato_tarife
    ```
2.  Monitor the DAG run in the Airflow Grid View.

### Success Criteria
The validation is considered **passing** if:
1.  The DAG run status transitions to `Success`.
2.  The task `dwh_dummy_absd_plato_tarife_task` completes successfully.
3.  The task execution logs contain the following output:
    ```text
    [INFO] Running command: echo 'Doing nothinig'
    [INFO] Output:
    Doing nothinig
    [INFO] Command exited with return code 0
    ```

---

## 7. Rollback Procedure

In the event of an issue or deployment failure, perform the following steps to roll back:

1.  **Pause the Airflow DAG**: Navigate to the Airflow UI and toggle the DAG `dw_dwh_dummy_absd_plato_tarife` to **Off** (Paused).
2.  **Remove the DAG File**: Delete the DAG file from the Cloud Composer GCS bucket:
    ```bash
    gsutil rm gs://<COMPOSER_DAGS_BUCKET>/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
    ```
3.  **Re-enable Legacy Job**: If the legacy UC4 job was deactivated or blocked during the cutover, re-enable the job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` in the UC4 environment to resume legacy operations.