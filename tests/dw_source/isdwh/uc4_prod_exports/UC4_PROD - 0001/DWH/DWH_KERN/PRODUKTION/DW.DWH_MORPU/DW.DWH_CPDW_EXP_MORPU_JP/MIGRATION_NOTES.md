# Migration Notes: DW.DWH_EXIS_CPDW_LOC

This document details the migration of the UC4 job **DW.DWH_EXIS_CPDW_LOC** to Apache Airflow (Cloud Composer).

---

## 1. Summary
The UC4 active UNIX job `DW.DWH_EXIS_CPDW_LOC` has been migrated to a standalone Apache Airflow DAG. 

* **Source Platform:** UC4 (Automic) Engine
* **Target Platform:** Apache Airflow / Google Cloud Composer
* **Functionality:** Exports lookup data to the CPDW target system via SFTP using an internal command-line exporter utility (`r_exis`) hosted on the remote host `dwhdwh1p`.

---

## 2. Generated Artifacts
The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_MORPU/DW.DWH_CPDW_EXP_MORPU_JP/dw_dwh_exis_cpdw_loc.py` | Airflow DAG | Python definition file containing the DAG and the `SSHOperator` task to execute the remote export command. |

---

## 3. Key Design Decisions

### Remote Execution via SSH
* **Decision:** Use `SSHOperator` instead of a local `BashOperator`.
* **Reasoning:** The export utility (`r_exis`) and its environment initialization files (`.dw_init`) reside on the external Unix host `dwhdwh1p`. Executing this locally on the Airflow worker is not feasible without migrating the entire exporter codebase and configuring SFTP keys on the Airflow worker itself.

### Dynamic Connection Resolution
* **Decision:** Retrieve the SSH Connection ID dynamically from Airflow Variables (`SSH_CONN_DWHDWH1P`) with a fallback default (`ssh_dwhdwh1p_default`).
* **Reasoning:** This avoids hardcoding environment-specific connection parameters in the DAG code, allowing seamless promotion across Development, Test, and Production environments.

### Standalone DAG Configuration
* **Decision:** The DAG is configured with `schedule=None` (manual or external trigger only).
* **Reasoning:** No scheduling objects (`EVNT_TIME`, `JSCH`) were associated with this job in the source UC4 extraction bundle. It is designed to be triggered externally or integrated into a parent workflow at a later stage.

---

## 4. Manual Steps Before Go-Live

Before activating this DAG in production, the following infrastructure and configuration steps must be completed:

### 1. Connection Strings & Secrets
* Create an SSH Connection in Airflow with the following details:
  * **Conn ID:** `ssh_dwhdwh1p_default` (or the custom name defined in your Airflow variables).
  * **Conn Type:** `SSH`
  * **Host:** IP address or FQDN of `dwhdwh1p`.
  * **Username:** Credentials associated with the UC4 login `DW.UNIX.ISTNS`.
  * **Authentication:** Configure the SSH Private Key corresponding to the authorized user on `dwhdwh1p`.

### 2. Airflow Variables
* Define the following variable in the Airflow UI (**Admin -> Variables**):
  * **Key:** `SSH_CONN_DWHDWH1P`
  * **Value:** `ssh_dwhdwh1p_default` (or your environment-specific connection ID).

### 3. IAM & Network Permissions
* Ensure that the Cloud Composer GKE worker nodes have network access (firewall rules allowed) to connect to the target host `dwhdwh1p` on port `22` (SSH).

### 4. Target Host Verification
* Verify that the target host `dwhdwh1p` has:
  * The initialization profile `$HOME/.dw_init` configured and accessible.
  * The exporter utility `$HOME/aktuell/exporter/is/bin/r_exis` compiled and runnable.
  * Passwordless SFTP key-based authentication established between the target host and the `cpdw` target system.

---

## 5. Known Gaps & Unresolved References

### Downstream Integration
* **Gap:** The downstream consumer workflow `DW.DWH_CPDW_EXP_MORPU_JP` has not yet been migrated.
* **Resolution:** Once the downstream workflow is migrated, configure an `ExternalTaskSensor` or direct cross-DAG triggering mechanism to link this export task to its downstream consumers.

### UC4 Inclusions
* **Gap:** The UC4 script included `:inc DW.HOLE_PFAD` and `:inc DW.LESE_LOG`. These were flagged as "NO SOURCE NEEDED" during the migration assessment.
* **Resolution:** Confirm that any path resolution or log parsing previously handled by these inclusions is either obsolete or successfully encapsulated within the remote host's `.dw_init` profile or the `r_exis` binary itself.

---

## 6. Validation

To validate the migrated workflow:

1. **Trigger the DAG:** Manually trigger the `dw_dwh_exis_cpdw_loc` DAG from the Airflow Web UI.
2. **Monitor Execution:**
   * Verify that the task `dwh_exis_cpdw_loc` transitions to `running` and then `success`.
   * Inspect the Airflow task logs to ensure the SSH connection was established successfully and the remote command output does not contain errors.
3. **Verify Target State:**
   * Confirm that the lookup data has been successfully transferred to the `cpdw` target system via SFTP.
   * Check the timestamp of the exported files on the target system to ensure they match the execution time.

---

## 7. Rollback Procedure

If issues arise post-deployment, perform the following steps to roll back:

1. **Pause the Airflow DAG:**
   * Navigate to the Airflow UI and toggle the active switch for `dw_dwh_exis_cpdw_loc` to **Off** (Paused).
2. **Re-enable UC4 Job:**
   * Reactivate the original `DW.DWH_EXIS_CPDW_LOC` job in the UC4 environment.
   * Ensure any scheduling or external triggers in UC4 are restored to their original state.