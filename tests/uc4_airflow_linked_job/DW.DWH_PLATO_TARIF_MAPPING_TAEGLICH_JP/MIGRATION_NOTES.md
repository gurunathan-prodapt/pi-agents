# MIGRATION NOTES: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## 1. Summary
This document details the migration of the UC4 Unix Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Google Cloud Composer (Airflow). 

* **Source Object**: `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` (UC4 `JOBS_UNIX` object)
* **Target Platform**: Google Cloud Composer (Airflow)
* **Migration Pattern**: `UC4_ONLY` (Orchestration Migration)
* **Functional Description**: In the legacy system, this job functions as an operational dummy/placeholder task. It performs no business logic or data transformations; its sole action is printing a log statement (`Doing nothinig`).

---

## 2. Generated Artifacts
The migration process generated the following file:

| Target File Path | Role / Description |
| :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` | The production-ready Airflow DAG file containing the orchestration logic, environment variable retrieval, and task definitions. |

---

## 3. Key Design Decisions

### BashOperator for Legacy Log Preservation
To strictly adhere to the **Output/Print Literal Rule**, the legacy command `:print Doing nothinig` was migrated to an Airflow `BashOperator` executing `echo "Doing nothinig"`. This preserves the exact character-for-character legacy log output, including the original typographical error ("nothinig").

### Dynamic Environment Configuration
To comply with the **Environment Values Policy** and enforce the ban on hardcoded prose placeholders, all environment-specific variables are retrieved dynamically from the Airflow Variable store:
* `GCP_PROJECT`
* `GCP_REGION`
* `DATAPROC_CLUSTER`
* `GCS_BUCKET`

These variables are prepared in the DAG header to facilitate future upgrades if this dummy task is ever converted into an active PySpark computation task.

### Manual Scheduling (`None`)
Because the source export was limited to a single Unix Job file without its parent Job Plan (`JOBP`) or Schedule (`EVNT_TIME`), the DAG's schedule is set to `None`. This prevents accidental or unscheduled executions in the target environment.

### Preservation of Recovery Documentation
The legacy recovery comment (*"Wiederanlauf ohne weitere Maßnahmen möglich"*) has been preserved verbatim and embedded directly into the DAG's markdown documentation (`dag.doc_md`) to maintain operational continuity for support teams.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variable Configuration
Ensure that the following Airflow variables are configured in the target Cloud Composer environment:

```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "GCP_REGION": "your-gcp-region",
  "DATAPROC_CLUSTER": "your-dataproc-cluster-name",
  "GCS_BUCKET": "your-gcs-bucket-name"
}
```

### 2. IAM & Permissions
Although this is currently a dummy task running a basic `echo` command, the Composer Worker Service Account should have the standard IAM roles configured if this job is expanded in the future:
* `roles/composer.worker`
* `roles/dataproc.editor` (for future PySpark execution)
* `roles/storage.objectAdmin` (for future script/log access)

### 3. Scheduling & Integration
Keep the DAG schedule set to `None` until the downstream parent workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) is migrated.

---

## 5. Known Gaps & Unresolved References

### Missing Orchestration Context
The source UC4 export did not contain the parent `JOBP` or `EVNT_TIME` files. Consequently, this DAG is currently configured as a standalone, manually triggered pipeline.

### Unresolved Downstream Dependency
* **Downstream Target**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` (Not yet migrated).
* **Resolution Plan**: Once the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated, this DAG should either:
  1. Be integrated directly as a task node inside the parent DAG.
  2. Be triggered via a `TriggerDagRunOperator` from the parent DAG.
  3. Be sensed using an `ExternalTaskSensor` in the downstream DAG.

---

## 6. Validation

### DAG Parsing Test
Verify that the DAG is syntactically correct and can be parsed by Airflow without errors:

```bash
python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py
```
* **Passing Criteria**: The command exits with code `0` and outputs no syntax or import errors.

### Execution Test
1. Upload the DAG file to the Cloud Composer DAGs bucket: `gs://<composer-dag-bucket>/dags/`.
2. Navigate to the Airflow UI and locate the DAG `dw_dwh_dummy_absd_plato_tarife`.
3. Trigger the DAG manually.
4. Verify the task execution logs.

* **Passing Criteria**:
  * The DAG run completes with a `success` status.
  * The task `dwh_dummy_absd_plato_tarife` outputs `Doing nothinig` in its standard output log.
  * The DAG documentation tab in the Airflow UI displays the recovery note: *"Wiederanlauf ohne weitere Maßnahmen möglich"*.

---

## 7. Rollback Procedure
In the event of an issue or deployment failure, execute the following rollback steps:

1. **Pause the DAG**: Locate `dw_dwh_dummy_absd_plato_tarife` in the Airflow UI and toggle the switch to **Off** (Paused).
2. **Remove the Artifact**: Delete the migrated DAG file from the Cloud Composer DAGs bucket:
   ```bash
   gcloud storage rm gs://<composer-dag-bucket>/dags/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```
3. **Re-enable Legacy Job**: If the legacy UC4 system has been deactivated, re-enable the `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` job in the UC4 console to resume legacy orchestration.