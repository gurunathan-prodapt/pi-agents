# Migration Notes: DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE

This document details the migration of the UC4/Automic native Unix job `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE` to Apache Airflow.

---

## 1. Summary
The UC4 native Unix job `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE` has been migrated to **Apache Airflow**. 

In the source UC4 system, this job was configured as a "DUMMY" execution step containing a simple placeholder command (`:print mach nix` — German colloquialism for "do nothing"). It performs no operational system tasks, database modifications, or file transfers. It has been migrated as a standalone, on-demand Airflow DAG containing a single `EmptyOperator` task, serving as a structural placeholder for future workflow integration.

---

## 2. Generated Artifacts
The migration process generated the following file:

* **DAG File:** `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_TVD_AKTIVITAETSRATE_MONATLICH_JP/DW.DWH_TVD_AK2_MONATLICH_JP/DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP/DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE.py`
  * **Role:** Defines the Airflow DAG `dw_dwh_dummy_vdgd_nvr_imvt_pre` and its single task `dwh_dummy_vdgd_nvr_imvt_pre`. It imports standard environment variables and sets up the execution metadata.

---

## 3. Key Design Decisions

### Mapping to `EmptyOperator`
The original UC4 script body contains only `:print mach nix`. Because this command has no functional operational impact, mapping it to a heavy execution operator (such as `BashOperator` or `SSHOperator`) would introduce unnecessary overhead and connection dependencies. The `EmptyOperator` was selected as the cleanest, most resource-efficient representation of a legacy "no-op" or dummy step.

### Scheduling and Triggering
No scheduling metadata (`EVNT_TIME`, `JOBP` parent workflow, or `SCRI` triggers) was provided in the source extraction bundle. 
* **Decision:** The DAG is configured with `schedule=None`. 
* **Rationale:** This prevents accidental automated runs. The DAG must be triggered manually or programmatically via an upstream orchestrator once the parent workflow is migrated.

### Retention of GCP Variable Imports
The generated DAG includes standard lookups for Airflow Variables (`GCP_PROJECT`, `DATAPROC_REGION`, etc.). Although these are not utilized by the `EmptyOperator`, they are retained to maintain structural consistency across all migrated DAGs in the repository, facilitating automated CI/CD parsing and environment promotion.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this DAG in a production Airflow environment, complete the following steps:

### 1. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in the target Airflow environment (even if unused by this specific dummy task, they prevent import-level failures if strict schema validation is enabled):
* `GCP_PROJECT`
* `DATAPROC_REGION`
* `DATAPROC_CLUSTER_NAME`
* `GCS_BUCKET`

### 2. IAM & Permissions
No specific service accounts or cloud permissions are required to execute this `EmptyOperator` task. Standard Airflow worker execution permissions apply.

### 3. Connection Strings & Secrets
No external connections (SSH, database, or cloud providers) are required for this DAG.

### 4. Scheduling & Upstream Integration
Because this job is designed to run as a pre-requisite step within a larger sequence, you must manually coordinate its execution with the upstream trigger mechanism once the orchestrating workflow is established.

---

## 5. Known Gaps & Unresolved References

### Downstream Dependency Gap
* **Unresolved Reference:** `DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP`
* **Details:** This downstream job/workflow is marked as **not yet migrated**. 
* **Impact:** Cross-DAG dependency wiring (e.g., via `ExternalTaskSensor` or `TriggerDagRunOperator`) cannot be finalized. 
* **Resolution:** Once `DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP` is migrated to Airflow, update either this DAG or the downstream DAG to establish the correct execution sequence.

### Unresolved UC4 Login and Host
* **Details:** The UC4 metadata referenced host `|DWHDWH1P|HOST` and login credentials `DW.UNIX.ISTNS`. 
* **Impact:** These have been ignored because the task is mapped to an `EmptyOperator`.
* **Redesign (B4) Item:** If future business requirements dictate that this dummy step must be replaced with actual shell scripts or database calls on that host, you must:
  1. Create an Airflow SSH Connection (e.g., `ssh_dwh_host`).
  2. Replace `EmptyOperator` with `SSHOperator`.
  3. Retrieve credentials securely via Airflow Connections/Secrets Manager.

---

## 6. Validation

To validate the migrated DAG, perform the following tests:

### 1. DAG Syntax and Import Test
Run a local Python compilation check to ensure there are no syntax errors or missing imports:
```bash
python3 dw_source/isdwh/uc4_prod_exports/UC4_PROD\ -\ 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_TVD_AKTIVITAETSRATE_MONATLICH_JP/DW.DWH_TVD_AK2_MONATLICH_JP/DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP/DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE.py
```
* **Passing Criteria:** The command exits with code `0` and outputs no errors.

### 2. Airflow DB Registration Test
Verify that the Airflow metadata database successfully parses and registers the DAG:
```bash
airflow dags report
# OR
airflow dags list | grep dw_dwh_dummy_vdgd_nvr_imvt_pre
```
* **Passing Criteria:** The DAG ID `dw_dwh_dummy_vdgd_nvr_imvt_pre` appears in the list without import errors.

### 3. Manual Execution Test
Trigger the DAG manually via the Airflow CLI or UI:
```bash
airflow dags trigger dw_dwh_dummy_vdgd_nvr_imvt_pre
```
* **Passing Criteria:** The DAG run starts immediately, the task `dwh_dummy_vdgd_nvr_imvt_pre` transitions to `success` within seconds, and the DAG run finishes successfully.

---

## 7. Rollback Procedure

In the event of an issue or deployment rollback:

1. **Pause the Airflow DAG:**
   Disable the DAG in the Airflow UI or via the CLI to prevent manual or accidental triggers:
   ```bash
   airflow dags pause dw_dwh_dummy_vdgd_nvr_imvt_pre
   ```
2. **Re-enable UC4 Job:**
   If the legacy UC4 system is still active, ensure the active flag for `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE` is set to `1` (Active) within the Automic UI.
3. **Remove Airflow Artifacts (Optional):**
   Delete the generated `.py` file from the Airflow DAGs folder to completely remove it from the UI.