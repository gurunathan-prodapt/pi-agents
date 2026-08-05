# MIGRATION NOTES: DW.DWH_VVTN_IAR_BGF_GUTSCHR

This document provides the migration details, design decisions, manual setup steps, known gaps, validation procedures, and rollback strategies for migrating the UC4 job `DW.DWH_VVTN_IAR_BGF_GUTSCHR` to Apache Airflow (Cloud Composer).

---

## 1. Summary
The legacy UC4 Unix job `DW.DWH_VVTN_IAR_BGF_GUTSCHR` has been migrated to an Apache Airflow DAG running on Cloud Composer. 

* **Source Workload:** A standalone UC4 `JOBS_UNIX` task that sets environment variables (such as the job identifier and the previous calendar month's ID) and executes a local shell script (`r_vvtn_iar_bgf_gutschrift`) to transform credit ("Gutschrift") files into a single unified CSV file.
* **Target Platform:** Apache Airflow (Cloud Composer) / Google Cloud Platform (GCP).
* **Scheduling:** The legacy job had no self-contained UC4 calendar schedule or time-based event triggers. Consequently, the migrated Airflow DAG is configured as an externally triggered workflow (`schedule=None`).

---

## 2. Generated Artifacts
The migration process generated the following orchestration file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py` | Airflow DAG | Orchestrates the execution environment setup, dynamically calculates the target month, and triggers the processing script via a `BashOperator`. |

---

## 3. Key Design Decisions

### Lift-and-Shift Bash Execution
* **Decision:** Use a `BashOperator` to execute the legacy script `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` rather than a complete Python refactor of the shell script.
* **Reasoning:** The underlying processing script and its associated AWK dependencies (`k_vvtn_iar_bgf_gutschrift.awk` and `k_vvtn_iar_bgf_gutsch_foot.awk`) failed automated conversion due to missing translation tools (see [Section 5: Known Gaps](#5-known-gaps--unresolved-references)). A hybrid lift-and-shift approach ensures operational continuity while the underlying scripts are manually refactored or pending tool updates.

### Dynamic Date Calculation via Jinja
* **Decision:** Replaced the UC4 variable `&LASTMONTH_YYYYMM` with an Airflow Jinja macro expression:
  ```python
  {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}
  ```
* **Reasoning:** This guarantees that the execution context is deterministic and tied to the DAG's execution date, allowing safe historical backfills.

### Externalized Environment Variables
* **Decision:** Sourced global infrastructure variables (`GCP_PROJECT`, `GCS_BUCKET`, `BQ_DATASET`) dynamically using `Airflow Variable` lookups with safe fallbacks.
* **Reasoning:** Prevents hardcoding environment-specific values (Dev/UAT/Prod) directly into the DAG code.

---

## 4. Manual Steps Before Go-Live

Before enabling and executing this DAG in a production environment, the following manual setup steps must be completed:

### 1. Airflow Variables Setup
Ensure the following Airflow Variables are configured in the target Cloud Composer environment:
* `GCP_PROJECT`: The ID of your Google Cloud Project.
* `GCS_BUCKET`: The GCS bucket acting as the landing zone/working directory for incoming data files (replacing `$HOME/aktuell`).
* `BQ_DATASET`: The target BigQuery dataset where the processed Gutschrift tables will reside.

### 2. IAM & Permissions
* The Cloud Composer worker service account must have:
  * Read/Write permissions (`roles/storage.objectAdmin`) on the configured `GCS_BUCKET`.
  * BigQuery Data Editor (`roles/bigquery.dataEditor`) and Job User (`roles/bigquery.jobUser`) roles on the `BQ_DATASET` (if downstream steps load this data into BigQuery).
  * SSH/Execution permissions on the target VM host if the script is executed remotely (if transitioning from local Bash to `SSHOperator`).

### 3. Execution Environment & Path Alignment
* The legacy script relies on `.dw_init` and the path structure `$HOME/aktuell/...`. 
* Ensure that the target execution environment (whether a persistent worker VM, a container, or a mounted file share) has the `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` script and its dependent AWK files deployed in the correct directories.
* Verify that `.dw_init` is present in the execution user's home directory and correctly configures the environment.

---

## 5. Known Gaps & Unresolved References

### 1. Sibling Migration Failures (Redesign B4 Items)
The automated conversion for the core processing scripts failed during the migration pass:
* **Failed Modules:** 
  * `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift` (Shell script)
  * `k_vvtn_iar_bgf_gutschrift.awk` (AWK processing logic)
  * `k_vvtn_iar_bgf_gutsch_foot.awk` (AWK footer processing logic)
* **Reason:** The required migration tool `awk_design_bqsql_python` was not available on the migration servers.
* **Mitigation/Follow-up:** The current DAG relies on the legacy shell script and AWK files remaining intact on the target execution host. A manual refactoring of these AWK scripts into native Python/BigQuery SQL is highly recommended for a complete cloud-native architecture.

### 2. Host Execution Context
* The legacy job ran on host `|DWHDWH1P|HOST` using login `DW.UNIX.ISTNS`. 
* The generated DAG uses a local `BashOperator`. If Cloud Composer workers do not have local access to the legacy file system or script paths, this task must be updated to use the `SSHOperator` targeting the legacy host, utilizing Airflow Connection credentials mapped from `DW.UNIX.ISTNS`.

---

## 6. Validation

To validate the migration of this workflow, perform the following steps:

### Test Execution
1. Trigger the DAG manually in the Airflow UI using **Trigger DAG w/ config**.
2. Pass a specific execution date in the configuration to test historical backfilling (e.g., `{"execution_date": "2023-10-15T00:00:00+00:00"}`).

### Verification of "Passing" Status
* **Log Verification:**
  * Open the task logs for `dw_dwh_vvtn_iar_bgf_gutschr_task`.
  * Verify that the log outputs: `Lastmonth is 202309` (confirming the Jinja macro correctly calculated the previous month relative to the execution date).
  * Confirm that `. dw_init` loaded successfully without environment errors.
* **Data Verification:**
  * Verify that the shell script executed to completion (Exit Code `0`).
  * Confirm that the unified CSV file was successfully generated in the target directory (or GCS bucket) and matches the schema and record count of a baseline run from the legacy UC4 system.

---

## 7. Rollback Procedure

If issues are encountered during go-live or validation, follow this rollback protocol:

1. **Pause the Airflow DAG:**
   * Navigate to the Airflow UI and toggle the switch for `dw_dwh_vvtn_iar_bgf_gutschr` to **Off** (Paused).
2. **Re-enable the UC4 Job:**
   * Log into the UC4 Automic interface.
   * Locate the job `DW.DWH_VVTN_IAR_BGF_GUTSCHR`.
   * Ensure the active flag is set to `Active (1)` and any associated parent schedules or external triggers are reactivated.
3. **Cleanup Target Environment:**
   * If the failed Airflow run generated partial or corrupted CSV files in the target directory or GCS bucket, manually delete those artifacts to prevent downstream processing errors.