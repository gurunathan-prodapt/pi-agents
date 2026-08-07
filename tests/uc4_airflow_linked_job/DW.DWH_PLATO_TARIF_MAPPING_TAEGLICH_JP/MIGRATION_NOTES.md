# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the legacy UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Apache Airflow (Google Cloud Composer).

---

## 1. Summary
The legacy UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to a standalone Apache Airflow DAG. 

* **Source Object**: `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` (JOBS_UNIX)
* **Target Platform**: Google Cloud Composer (Apache Airflow)
* **Functional Description**: This is a dummy synchronization/placeholder task that historically executed a native UC4 print command (`:print Doing nothinig`). It is designed to run as part of the daily mapping workflow sequence `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.

---

## 2. Generated Artifacts
The migration process generated the following file:

| Target File Path | Language | Role |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Python (Airflow DAG) | Defines the Airflow DAG and executes the dummy task using a `BashOperator`. |

---

## 3. Key Design Decisions

### Operator Selection
* **Decision**: Migrated the task using a `BashOperator` executing `echo 'Doing nothinig'` instead of an `EmptyOperator`.
* **Rationale**: While functionally a dummy task, using a `BashOperator` preserves the exact execution output of the legacy UC4 `:print` command, ensuring log parity and compliance with automated log-parsing tools.

### Output/Print Literal Rule Compliance
* **Decision**: Retained the spelling typo `"nothinig"` in the bash command (`echo 'Doing nothinig'`).
* **Rationale**: Strict adherence to the *Output/Print Literal Rule* ensures that downstream log parsers or verification scripts looking for this exact string do not fail.

### Scheduling and Concurrency
* **Decision**: Configured the DAG with `schedule=None` and `max_active_runs=1`.
* **Rationale**: The legacy job has no independent calendar schedule and is triggered externally or by a parent workflow. Setting `max_active_runs=1` prevents concurrent execution conflicts if triggered multiple times in rapid succession.

### Metadata Retention
* **Decision**: Embedded legacy metadata and German operational notes directly into the DAG's module-level docstring.
* **Rationale**: Retaining the note *"Wiederanlauf ohne weitere Maßnahmen möglich"* (Restart is possible without further actions) ensures the Cloud Operations team has immediate access to recovery instructions.

---

## 4. Manual Steps Before Go-Live

### 1. IAM and Permissions
* **Legacy Login**: `DW.UNIX.ISTNS`
* **Action Required**: Ensure the Google Cloud Service Account (GSA) associated with the Cloud Composer environment has the necessary permissions to execute tasks in the GKE tenant. Since this is a dummy bash command, no external GCP resource permissions (e.g., BigQuery, GCS) are required for this specific task.

### 2. DAG Deployment
* Copy the generated DAG file to your Cloud Composer DAGs bucket:
  ```bash
  gsutil cp uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py gs://<your-composer-bucket>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/
  ```

### 3. Connection Strings & Variables
* No Airflow Connections or Variables are required for this DAG.

---

## 5. Known Gaps & Unresolved References

### Parent Workflow Integration (Downstream Dependency)
* **Gap**: The parent JobPlan/workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` has **not yet been migrated**.
* **Resolution**: 
  * Currently, this DAG is configured to run standalone (`schedule=None`).
  * Once the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated to Airflow, this DAG should either be:
    1. Integrated directly as a task node within the parent DAG.
    2. Triggered downstream via a `TriggerDagRunOperator` from the parent DAG.

---

## 6. Validation

### Local/Dev Environment Validation
To validate the DAG structure and execution locally or in a development Composer environment, run the following commands:

1. **Syntax and Import Check**:
   Verify that Airflow can parse the DAG without errors:
   ```bash
   python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
   ```

2. **Task Unit Test**:
   Test the execution of the dummy task locally:
   ```bash
   airflow tasks test dw_dwh_dummy_absd_plato_tarife dwh_dummy_absd_plato_tarife 2023-01-01
   ```

### Definition of "Passing"
The validation is successful if:
* The command exits with code `0`.
* The task logs output exactly:
  ```text
  Running command: ['/bin/bash', '-c', "echo 'Doing nothinig'"]
  ...
  Doing nothinig
  Command exited with return code 0
  ```

---

## 7. Rollback Procedure

If issues arise post-deployment, execute the following steps to roll back:

1. **Pause the Airflow DAG**:
   Disable the DAG in the Airflow UI or via the CLI:
   ```bash
   airflow dags pause dw_dwh_dummy_absd_plato_tarife
   ```

2. **Remove the DAG File**:
   Delete the DAG file from the Cloud Composer GCS bucket to prevent it from being parsed:
   ```bash
   gsutil rm gs://<your-composer-bucket>/dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
   ```

3. **Re-enable Legacy Job**:
   If the legacy UC4 environment is still active, ensure the job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is set to active (`active=1`) to resume legacy operations.