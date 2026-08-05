# Migration Notes: DW.EXTTEST_LEGACY_DWH

## 1. Summary
This document details the migration of the UC4 UNIX Job object `DW.EXTTEST_LEGACY_DWH` (Title: `legacy_ksh_dwh`) to Apache Airflow on Google Cloud Platform (Cloud Composer). 

The legacy job was responsible for executing a shell script (`r_legacy_ksh_dwh`) on an external host (`dwhdwh2p`) under the UNIX login `DW.UNIX.ISXTST`. Because no parent workflow (`JOBP`) or schedule (`JSCH`) was provided in the extraction bundle, this job has been migrated as a standalone, externally triggered Airflow DAG (`schedule=None`).

---

## 2. Generated Artifacts
The migration process generated the following file:

* **`vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/dw_exttest_legacy_dwh.py`**
  * **Role**: The Apache Airflow DAG definition file. It defines the DAG metadata, default arguments, environment-wide variable lookups (`GCP_PROJECT`, `GCP_REGION`, `HOME_DIR`), and contains the task placeholder representing the legacy execution.

---

## 3. Key Design Decisions
* **Standalone DAG Modeling**: Since no scheduling or parent workflow context was extracted, the DAG is configured with `schedule=None`. It must be triggered manually or via an external orchestration mechanism.
* **EmptyOperator Stub (B4 Redesign)**: The automated migration tool encountered a `NO_MCP_TOOL` error because the custom shell launcher pattern (`&HOME/scripts/r_legacy_ksh_dwh`) was unrecognized. To prevent a hard failure of the migration pipeline, the task was mapped to an `EmptyOperator` stub. This serves as a placeholder for manual implementation.
* **Retirement of Legacy Includes**: The UC4 include blocks `:inc DW.EXTTEST_HOLE_PFAD` and `:inc DW.EXTTEST_LESE_LOG` were flagged during human review as retired/not needed. They have been omitted from the target DAG, simplifying the environment setup.
* **Concurrency Control**: `max_active_runs=1` is enforced at the DAG level to prevent concurrent executions of the legacy script, mimicking the safe-concurrency behavior of the legacy system.

---

## 4. Manual Steps Before Go-Live
Before this DAG can be run in a production environment, the following manual steps must be completed:

1. **Replace the Task Operator**:
   * Replace the `EmptyOperator` (`dw_exttest_legacy_dwh_task`) in `dw_exttest_legacy_dwh.py` with an operational execution operator.
   * *Option A (SSH Execution)*: Use `SSHOperator` if the script must continue to run on the legacy host `dwhdwh2p`.
   * *Option B (Containerized Execution)*: Use `GKEStartPodOperator` or `KubernetesPodOperator` if the script has been containerized.
2. **Deploy the Target Script**:
   * Ensure that the shell script `r_legacy_ksh_dwh` is migrated and deployed to the target execution environment (VM or container image) at the path resolved by `$HOME/scripts/r_legacy_ksh_dwh`.
3. **Configure Airflow Connections & Secrets**:
   * Create an Airflow Connection (e.g., SSH connection) for host `dwhdwh2p` using the credentials associated with `DW.UNIX.ISXTST`.
4. **Define Airflow Variables**:
   * Ensure the following Airflow Variables are defined in the target environment:
     * `GCP_PROJECT`
     * `GCP_REGION`
     * `HOME_DIR` (defaults to the system `$HOME` if not set)
5. **Downstream Integration**:
   * Once the downstream consumer `DW.EXTTEST_ABLAUFSTEUERUNG` is migrated, configure a cross-DAG dependency (e.g., using `TriggerDagRunOperator` or `ExternalTaskSensor`) to link these workflows.

---

## 5. Known Gaps & Unresolved References
* **Design Failure (B4 Item)**: The automated migration failed to resolve the execution operator due to the unrecognized launcher pattern (`&HOME/scripts/r_legacy_ksh_dwh`). Manual code intervention is required to replace the `EmptyOperator` stub.
* **Unmigrated Downstream Dependency**: `DW.EXTTEST_ABLAUFSTEUERUNG` is currently unmigrated. The downstream execution chain is broken until this dependency is resolved and wired.
* **Verification of Retired Includes**: Although `DW.EXTTEST_HOLE_PFAD` and `DW.EXTTEST_LESE_LOG` are marked as retired, developers must verify that the target execution environment natively handles path resolution and log parsing without these legacy components.

---

## 6. Validation
To validate the migrated DAG:

1. **DAG Syntax & Compilation Check**:
   Run a local syntax check to ensure the DAG is parsed correctly by Airflow:
   ```bash
   python vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/dw_exttest_legacy_dwh.py
   ```
   *Passing criteria*: The command exits with code `0` and no syntax or import errors are output.

2. **Airflow UI Import**:
   Place the DAG file in the Airflow `dags/` folder and verify it appears in the Airflow UI without import errors.

3. **Manual Execution Test**:
   Once the `EmptyOperator` is replaced with the active execution operator (e.g., `SSHOperator`), trigger the DAG manually from the Airflow UI.
   *Passing criteria*:
   * The task connects successfully to the execution environment.
   * The environment variable `DWH_JOB_KENNUNG='EXTTEST_LEGACY_DWH'` is successfully exported.
   * The script `r_legacy_ksh_dwh` executes and completes with exit code `0`.

---

## 7. Rollback Procedure
In the event of a deployment failure or critical runtime issue in the target environment, perform the following rollback steps:

1. **Pause the Airflow DAG**:
   Disable the DAG in the Airflow UI or via the CLI:
   ```bash
   airflow dags pause dw_exttest_legacy_dwh
   ```
2. **Re-enable the UC4 Job**:
   * Log into the UC4/Automic interface.
   * Locate the job `DW.EXTTEST_LEGACY_DWH`.
   * Ensure the active flag is set to `1` (Active).
   * If any downstream triggers were modified, revert them to point back to the UC4 job.
3. **Verify Legacy Execution**:
   Monitor the next scheduled or manual run in UC4 to ensure the legacy shell script executes successfully on host `dwhdwh2p`.