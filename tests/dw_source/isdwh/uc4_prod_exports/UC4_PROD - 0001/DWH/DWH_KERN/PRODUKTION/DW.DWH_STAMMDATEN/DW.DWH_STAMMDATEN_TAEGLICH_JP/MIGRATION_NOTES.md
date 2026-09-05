# Migration Notes

**Migration Target:** Apache Airflow (Google Cloud Composer)  
**Source Job:** `DW.DWH_IPSD_DWH_MORPU_LID` (UC4 UNIX Job)  
**Target DAG ID:** `dw_dwh_ipsd_dwh_morpu_lid`  
**Target File Path:** `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/DW.DWH_IPSD_DWH_MORPU_LID.py`

---

## 1. Summary
This migration transitions the standalone UC4 UNIX job `DW.DWH_IPSD_DWH_MORPU_LID` to an Apache Airflow DAG. The primary function of this job is to import invoice services ("Rechnungsleistungen") to support the MORPU calculation process within the data warehouse. 

In the legacy environment, this job executed a custom binary script (`r_ipis`) on the host `dwhdwh1p` under the user profile `DW.UNIX.ISTNS`. Because no parent workflow (JOBP) or schedule (EVNT_TIME) was supplied in this extraction bundle, the job has been migrated as an externally-triggered, standalone Airflow DAG (`schedule=None`).

---

## 2. Generated Artifacts
The migration process generated the following file:

* **`DW.DWH_IPSD_DWH_MORPU_LID.py`**
  * **Role:** Apache Airflow DAG definition file.
  * **Location:** `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/`
  * **Description:** Contains the DAG configuration, default arguments, environment variable lookups, and a placeholder task (`EmptyOperator`) representing the legacy binary execution.

---

## 3. Key Design Decisions

### Standalone DAG with Dynamic Triggering (`schedule=None`)
Since no parent workflow (JOBP) or cron schedule was provided in the source UC4 export, the DAG is configured with `schedule=None`. It is designed to be triggered dynamically by upstream orchestration processes (e.g., via `TriggerDagRunOperator`) or manually via the Airflow UI/CLI.

### Folder Integrity Rule
To maintain strict traceability and align with the existing repository structure, the target Python file is placed in the exact directory path matching the source XML export, changing only the file extension from `.xml` to `.py`.

### Placeholder Operator for Binary Execution
The legacy job executed a local binary:
```bash
$HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid
```
Because the target execution architecture (e.g., migrating the binary to a Docker container vs. executing it on an on-premises VM via SSH) is not yet finalized, the task is implemented using an `EmptyOperator` with detailed metadata. This acts as a safe placeholder that developers can easily swap for an `SSHOperator`, `BashOperator`, or `GKEStartPodOperator`.

### Concurrency and Restartability
* **`max_active_runs=1`** is enforced at the DAG level to prevent parallel executions of the import binary, which could lead to race conditions or data corruption.
* **Restartability:** Legacy operational notes indicate that this process can be safely restarted or rerun after a failure without requiring manual cleanup or state restoration. This behavior is preserved.

---

## 4. Manual Steps Before Go-Live

To successfully deploy and run this DAG in production, the following manual setup steps must be completed:

### Schema & Dataset Creation
* Ensure that the target database tables and schemas associated with the `morpu_map_lid` context are fully provisioned in the target data warehouse environment.

### IAM & Permissions
* If executing via **SSH**: Ensure the Airflow worker service account has SSH access to the target host `dwhdwh1p` using the legacy user credentials (`DW.UNIX.ISTNS`).
* If executing via **Kubernetes/GKE**: Ensure the Airflow service account has permissions to launch pods in the target GKE cluster and that the container image containing the `r_ipis` binary is accessible in the container registry.

### Connection Strings & Secrets
Configure the following Airflow Connections and Variables in the Airflow Metadata Database:
1. **Airflow Variables:**
   * `GCP_PROJECT`: The target Google Cloud Project ID.
   * `GCS_BUCKET`: The GCS bucket used for staging or logging (if applicable).
   * `SSH_CONN_ID`: The connection ID for SSH execution (defaults to `ssh_default`).
2. **Airflow Connections:**
   * Create an SSH Connection matching the `SSH_CONN_ID` variable with:
     * **Host:** `dwhdwh1p`
     * **Username:** `DW.UNIX.ISTNS`
     * **Authentication:** SSH Key (stored securely in Secret Manager or Airflow Connections).

### Scheduling & Orchestration
* Since this DAG is non-scheduled (`schedule=None`), you must configure the upstream parent DAGs (once migrated) to trigger this DAG using the `TriggerDagRunOperator`.

---

## 5. Known Gaps & Unresolved References

### Missing Source Includes
The following UC4 include scripts and environment profiles were referenced in the source code but were not supplied in the migration bundle:
* `DW.HOLE_PFAD` (Path resolution script)
* `DW.LESE_LOG` (Log reading/parsing utility)
* `.dw_init` (UNIX environment initialization profile)

**Resolution Required:** The logic contained within these scripts must be replaced by native Airflow configurations (e.g., Airflow Connections, Environment Variables) or baked directly into the execution container/wrapper script.

### Binary Execution Strategy (B4 Redesign Item)
The execution of the compiled binary `$HOME/aktuell/import/is/bin/r_ipis` must be finalized. 
* *Option A (Lift-and-Shift):* Configure `SSHOperator` to run the command on the legacy VM `dwhdwh1p`.
* *Option B (Cloud-Native Redesign):* Containerize the `r_ipis` binary and execute it using `GKEStartPodOperator` or `KnativePodOperator`.

### Unmigrated Downstream Dependencies
The following downstream jobs consume this job's output but have not yet been migrated to Airflow:
* `DW.DWH_IPSD_DWH_MORPU_LID`
* `DW.DWH_MORPU_MONATLICH_JP`
* `DW.DWH_RUN_MORPU_MONATLICH_JP_EVT`
* `DW.DWH_STAMMDATEN_TAEGLICH_JP`
* `DW.DWH_START_RUN_MORPU_MONATLICH_JP_EVT`
* `DW.DWH_TVD_AK2_MONATLICH_JP`

Cross-DAG dependencies and execution triggers cannot be fully validated until these downstream workflows are deployed.

---

## 6. Validation

To validate the migrated DAG, perform the following tests:

### DAG Parse Test
Verify that the DAG is syntactically correct and can be loaded by the Airflow parser:
```bash
python3 dw_source/isdwh/uc4_prod_exports/UC4_PROD\ -\ 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMMDATEN/DW.DWH_STAMMDATEN_TAEGLICH_JP/DW.DWH_IPSD_DWH_MORPU_LID.py
```
* **Passing Criteria:** The command completes with exit code `0` and outputs no syntax or import errors.

### Airflow CLI Unit Test
Run a local test execution of the task:
```bash
airflow tasks test dw_dwh_ipsd_dwh_morpu_lid dw_dwh_ipsd_dwh_morpu_lid_task 2023-01-01
```
* **Passing Criteria:** The task execution completes successfully. *(Note: While the task is configured as an `EmptyOperator`, this test validates DAG structure and variable loading).*

### Integration Test (Post-Operator Resolution)
Once the `EmptyOperator` is replaced with the final execution operator (e.g., `SSHOperator` or `GKEStartPodOperator`):
1. Trigger the DAG manually via the Airflow UI.
2. Monitor the task logs to ensure the `r_ipis` binary executes, connects to the `dwh` database, and processes the `morpu_map_lid` context.
3. Verify that the target database tables are updated with the imported invoice services.

---

## 7. Rollback Procedure

In the event of a critical failure or data anomaly during the deployment of the migrated DAG, execute the following rollback steps:

1. **Pause the Airflow DAG:**
   Disable the DAG in the Airflow UI or via the CLI to prevent further executions:
   ```bash
   airflow dags pause dw_dwh_ipsd_dwh_morpu_lid
   ```
2. **Re-enable the Legacy UC4 Job:**
   * Open the UC4/Automic UI.
   * Locate the job `DW.DWH_IPSD_DWH_MORPU_LID`.
   * Set the active flag to `1` (Active).
3. **Verify Legacy Execution:**
   * Ensure the UC4 agent on host `dwhdwh1p` is active and ready to process triggers.
   * Run a test execution in UC4 to confirm the legacy pipeline is operational.
4. **Data Cleanup:**
   * Because the process is fully idempotent and safe to rerun without manual cleanup (as per legacy operational notes), no database rollback scripts or manual table purges are required before restarting the legacy job.