# Migration Notes: DW.DWH_EXIS_CPDW_DIRECT

This document outlines the migration details, design decisions, manual steps, and validation procedures for transitioning the legacy UC4 job `DW.DWH_EXIS_CPDW_DIRECT` to Apache Airflow on Google Cloud Platform (GCP).

---

## 1. Summary
The legacy UC4 job `DW.DWH_EXIS_CPDW_DIRECT` (a standalone `JOBS_UNIX` object) has been migrated to an Apache Airflow DAG on GCP. 

* **Source Platform:** UC4 (Automic) running on legacy host `dwhdwh1p` under the login profile `DW.UNIX.ISTNS`.
* **Target Platform:** Apache Airflow (Cloud Composer) on Google Cloud Platform (GCP).
* **Functional Purpose:** This job exports lookup data to the CPDW system using an SFTP direct transfer protocol via a legacy binary launcher (`r_exis`).

---

## 2. Generated Artifacts
The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_DIRECT.py` | Airflow DAG | Python script defining the Airflow DAG `dw_dwh_exis_cpdw_direct`. It encapsulates the legacy execution logic and parameters. |

---

## 3. Key Design Decisions

### Standalone DAG Wrapping
Because the source UC4 object was extracted without an orchestrating parent workflow (`JOBP`), it has been wrapped in its own standalone Airflow DAG (`dw_dwh_exis_cpdw_direct`). 
* **Trade-off:** This ensures the job is immediately runnable and deployable in Airflow, but it requires manual triggering or downstream cross-DAG configuration until parent workflows are migrated.

### Placeholder Operator for Unrecognized Launcher
The legacy script executes a custom binary:
```bash
$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp
```
Because this custom binary cannot be automatically mapped to a native GCP operator, the task is currently defined using an `EmptyOperator` placeholder.
* **Decision:** This prevents DAG compilation failures during initial deployment. The developer must replace this placeholder with a concrete operator (e.g., `SSHOperator` or a containerized `BashOperator`) once the target execution environment is finalized.

### Parameterization
The legacy UC4 variable `&DWH_JOB_KENNUNG` (set to `'EXIS_CPDW_DIRECT'`) has been mapped to the Airflow DAG's `params` dictionary to maintain metadata lineage and operational context.

---

## 4. Manual Steps Before Go-Live

Before activating this DAG in a production environment, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure the following global Airflow variables are defined in your Cloud Composer environment:
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `DATAPROC_REGION`: The region where Dataproc/Compute resources reside.
* `DATAPROC_CLUSTER`: The name of the active Dataproc cluster (if applicable).
* `GCS_BUCKET`: The primary Google Cloud Storage bucket for DWH staging.

### 2. Connection Strings & Secrets
The legacy job relies on SFTP credentials to transfer data to CPDW.
* **SFTP Connection:** Create an Airflow Connection (e.g., `cpdw_sftp_conn`) containing the target host, port, username, and private key/password.
* **SSH Connection (If executing on a VM):** If the `r_exis` binary must still run on a legacy VM, set up an SSH Connection in Airflow to allow the `SSHOperator` to connect to `dwhdwh1p`.

### 3. IAM & Permissions
* Ensure the Airflow worker service account has the necessary IAM roles to read secrets from Google Secret Manager (if storing SFTP keys there).
* If migrating the SFTP transfer to a native Python operator, ensure the service account has read access to the source data GCS buckets.

### 4. Scheduling & Downstream Alignment
* The DAG is currently configured with `schedule=None`. Keep this setting until the parent workflows (e.g., `DW.DWH_CPDW_EXP_MORPU_JP`) are migrated.
* Once the parent workflows are migrated, configure cross-DAG dependencies using `TriggerDagRunOperator` or `ExternalTaskSensor`.

---

## 5. Known Gaps & Unresolved References

The following items have been flagged for manual follow-up and redesign (B4 items):

### 1. Unrecognized Launcher (`r_exis`)
* **Gap:** The custom binary `$HOME/aktuell/exporter/is/bin/r_exis` is not natively supported on GCP.
* **Resolution Options:**
  * **Option A (Lift & Shift):** Replace `EmptyOperator` with an `SSHOperator` that connects to the legacy host `dwhdwh1p` and runs the original shell commands.
  * **Option B (Cloud Native Redesign):** Re-implement the SFTP export logic using Airflow's `SFTPHook` or `LocalFilesystemToSFTPOperator`, pulling data directly from Google Cloud Storage or BigQuery.

### 2. Missing UC4 Includes
* The legacy script references `:inc DW.HOLE_PFAD` and `:inc DW.LESE_LOG`, as well as sourcing `$HOME/.dw_init`.
* **Gap:** These files were not provided in the migration bundle.
* **Resolution:** Verify if these scripts perform critical environment setup or log parsing. If so, their logic must be manually ported to the Airflow task execution environment.

---

## 6. Validation

To validate the migrated DAG, perform the following steps:

### Step 1: DAG Syntax and Parsing Test
Run a local syntax check to ensure the DAG compiles without errors:
```bash
python3 dw_source/isdwh/uc4_prod_exports/UC4_PROD\ -\ 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/DW.DWH_EXIS_CPDW_DIRECT.py
```
*Alternatively, verify that the DAG appears in the Airflow UI without import errors.*

### Step 2: Integration Testing (Dry Run)
1. Replace the `EmptyOperator` placeholder with the chosen execution operator (e.g., `SSHOperator` or a custom Python SFTP task).
2. Trigger a manual run of the DAG `dw_dwh_exis_cpdw_direct` from the Airflow UI.
3. Monitor the task logs.

### What "Passing" Means:
* The task executes successfully without throwing exceptions.
* Connection to the CPDW SFTP server is established successfully.
* The expected lookup data files are verified as successfully received on the target CPDW SFTP directory.
* Task execution logs confirm that the environment variables (such as `DWH_JOB_KENNUNG`) were correctly evaluated.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or execution in the target environment, follow this rollback procedure:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the DAG `dw_dwh_exis_cpdw_direct` to **Off** (Paused) to prevent any automated or accidental manual triggers.
   
2. **Re-enable the Legacy UC4 Job:**
   * Log into the UC4 client.
   * Locate the object `DW.DWH_EXIS_CPDW_DIRECT`.
   * Ensure the active flag is set to `1` (Active).
   * If any parent workflows were modified to point to Airflow, revert them to point back to the UC4 `JOBS_UNIX` object.

3. **Verify Legacy Execution:**
   Trigger a test run of the job in UC4 and verify the legacy execution logs on host `dwhdwh1p` to ensure the export pipeline is functioning in its original state.