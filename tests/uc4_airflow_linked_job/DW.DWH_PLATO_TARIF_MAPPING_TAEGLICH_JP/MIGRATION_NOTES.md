# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Apache Airflow.

---

## 1. Summary

The legacy UC4 UNIX Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to **Apache Airflow** (Cloud Composer / Google Cloud Platform). 

In the legacy UC4 system, this job was a standalone UNIX task that executed a native UC4 script statement (`:print Doing nothinig`) rather than a standard shell script. Because no parent workflow (JOBP) or script trigger (SCRI) was supplied in the source extraction, this job has been wrapped in a standalone, single-task Airflow DAG. It serves as an operational placeholder with no internal data dependencies.

---

## 2. Generated Artifacts

The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | **Airflow DAG** | Python file defining the standalone Airflow DAG and its single `BashOperator` task. |

---

## 3. Key Design Decisions

### Standalone DAG Wrapper
Because the parent workflow container (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) was not provided in this extraction, the job was modeled as an independent Airflow DAG with `schedule=None`. This ensures the job can be deployed and unit-tested in isolation.

### Operator Selection (`BashOperator` vs. `EmptyOperator`)
While the initial design pseudocode suggested mapping this to an `EmptyOperator`, the final implementation uses a `BashOperator` executing `echo 'Doing nothinig'`. 
* **Reasoning**: This preserves the literal logging behavior of the legacy UC4 job, which printed `"Doing nothinig"` (including the original typo) to the standard output. 
* **Trade-off**: Using a `BashOperator` incurs a minor overhead compared to an `EmptyOperator`, but it guarantees 100% functional parity for operational logging audits.

### Local Execution vs. Remote SSH
The legacy job was registered to run on host `DWHDWH1P` under the login credentials `DW.UNIX.ISTNS`. Because the migrated task only performs a basic echo command, it is executed locally within the Airflow worker container. This avoids the need to configure remote SSH connections or Kubernetes execution contexts for a dummy task.

---

## 4. Manual Steps Before Go-Live

Before activating this DAG in a production environment, the following manual setup is required:

1. **DAG Deployment**: Copy the generated `.py` file to your Airflow environment's `dags/` folder (typically an execution path like `dags/dw_dwh_plato_tarif_mapping_taeglich_jp/`).
2. **IAM & Permissions**: Ensure the Airflow worker service account has standard execution permissions. No specialized GCP IAM roles are required for this local bash execution.
3. **Parent Workflow Integration**: 
   * If the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated later, this DAG should either be:
     * Consolidated as a task inside the parent DAG file.
     * Triggered downstream within the parent DAG using a `TriggerDagRunOperator`.

---

## 5. Known Gaps & Unresolved References

* **Missing Parent Container**: The downstream/parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not present in this migration bundle. Cross-job orchestration links cannot be validated until the parent container is established in Airflow.
* **Functional Verification**: The command `:print Doing nothinig` is a dummy action. The business team must confirm if this job is truly an operational placeholder or if it was historically used to trigger external, non-system events that need to be manually re-engineered.

---

## 6. Validation

To validate the migration of this job, perform the following steps:

### Execution Test
1. Upload the DAG to the Airflow environment.
2. Verify that the DAG parses successfully with no import errors in the Airflow UI.
3. Manually trigger the DAG `dw_dwh_dummy_absd_plato_tarife` via the Airflow UI or CLI:
   ```bash
   airflow dags trigger dw_dwh_dummy_absd_plato_tarife
   ```

### Success Criteria
The validation is considered **passing** if:
1. The DAG run status transitions to `Success`.
2. The task `dummy_absd_plato_tarife` completes successfully.
3. The task execution logs contain the exact output:
   ```text
   Running command: echo 'Doing nothinig'
   Doing nothinig
   Command exited with return code 0
   ```

---

## 7. Rollback Procedure

In the event of an issue or deployment failure, perform the following rollback steps:

1. **Pause the DAG**: In the Airflow UI, toggle the DAG `dw_dwh_dummy_absd_plato_tarife` to **Off** (paused).
2. **Delete the Artifact**: Remove the DAG file from the Airflow environment:
   ```bash
   rm gs://<your-composer-bucket>/dags/dw_dwh_plato_tarif_mapping_taeglich_jp/dw_dwh_dummy_absd_plato_tarife.py
   ```
3. **Re-enable Legacy Job**: If a fallback to the legacy environment is required, ensure the UC4 UNIX Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is set to active (`active=1`) in the Automic/UC4 console.