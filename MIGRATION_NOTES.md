```markdown
# MIGRATION_NOTES.md: r_ausd_bp_ta_bcp_iccid.ksh

This document outlines the migration of the KornShell script `r_ausd_bp_ta_bcp_iccid.ksh` to Google Cloud Platform, specifically BigQuery Stored Procedures.

---

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh` has been migrated. This script served as an orchestration layer, handling input parameters (snapshot date and restart value), setting up the execution environment, performing initial parameter validation, and managing logging and error handling. It delegated the core data processing logic to a separate "kernel script" named `k_ausd_bp_ta_bcp_iccid.ksh`.

The migration target is Google Cloud Platform, with the orchestration logic translated into a BigQuery Stored Procedure named `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`. The invocation of the original kernel script is replaced by a call to another BigQuery Stored Procedure, `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid` (currently a stub requiring full implementation). Logging and auditing functionalities are now handled by dedicated BigQuery tables.

---

## 2. Generated Artifacts

The following BigQuery DDL and Stored Procedure files were generated as part of this migration:

*   **`ddl/job_audit.sql`**
    *   **Role**: Defines the BigQuery table `my_project.my_dataset.job_audit`. This table serves as the central audit log for job executions, storing high-level metadata such as job ID, run ID, start/end timestamps, overall status, and key parameters (`stichtag`, `wiederanlauf_wert`). It replaces the audit-related functionalities of the legacy `DWMSG_*` functions.

*   **`ddl/job_log.sql`**
    *   **Role**: Defines the BigQuery table `my_project.my_dataset.job_log`. This table captures detailed, chronological log messages generated during job execution, including timestamps, log levels (INFO, ERROR), the procedure name, and the message content. It replaces the detailed logging functionalities of the legacy `DWMSG_*` functions and shell script output.

*   **`ddl/job_error_log.sql`**
    *   **Role**: Defines the BigQuery table `my_project.my_dataset.job_error_log`. This table is specifically designed to store detailed information about errors encountered during job runs, including error codes, messages, stack traces, and the parameters active at the time of the error. It replaces the error reporting aspects of the legacy `DWMSG_*` functions and shell error handling.

*   **`sprocs/ausd_bp_ta_ibcp_ccid.sql`**
    *   **Role**: Defines the BigQuery Stored Procedure `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`. This procedure is the direct replacement for `r_ausd_bp_ta_bcp_iccid.ksh`. It handles:
        *   Parsing and defaulting input parameters (`p_stichtag_str`, `p_wiederanlaufWert_in`).
        *   Parameter validation.
        *   Logging job start, progress, and completion status to `job_audit` and `job_log`.
        *   Invoking the kernel stored procedure `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid`.
        *   Robust error handling using `BEGIN...EXCEPTION` blocks, logging errors to `job_error_log`.

*   **`sprocs/k_ausd_bp_ta_bcp_iccid.sql`**
    *   **Role**: Defines a placeholder BigQuery Stored Procedure `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid`. This procedure is intended to replace the core data processing logic originally found in `k_ausd_bp_ta_bcp_iccid.ksh`. **It is currently a stub and requires detailed analysis and implementation of the actual business logic.** It accepts `p_stichtag` and `p_wiederanlaufWert` as inputs.

---

## 3. Key Design Decisions

The migration strategy focused on translating the orchestration and control flow aspects of the KornShell script into BigQuery's native capabilities, while maintaining a clear separation between orchestration and core business logic.

*   **Orchestration to BigQuery Stored Procedure**: The primary decision was to translate the shell script's orchestration logic (parameter handling, logging, error trapping, kernel invocation) into a BigQuery Stored Procedure (`ausd_bp_ta_ibcp_ccid`). This centralizes the job's control flow within BigQuery, leveraging its native SQL capabilities for data manipulation and execution management. This approach aligns with a cloud-native data warehousing paradigm.
*   **Dedicated Logging and Auditing Tables**: Instead of relying on shell-based log files and custom `DWMSG_*` functions, a structured logging and auditing framework was implemented using three dedicated BigQuery tables (`job_audit`, `job_log`, `job_error_log`). This provides:
    *   **Centralized Observability**: All job execution details are stored within BigQuery, making them easily queryable and analyzable.
    *   **Structured Data**: Logs are stored in a structured format, enabling easier reporting, monitoring, and debugging compared to plain text files.
    *   **Improved Error Handling**: Specific error details, including BigQuery error codes and stack traces, are captured in `job_error_log`, facilitating quicker issue resolution.
*   **Kernel Script as Separate Stored Procedure**: The original script's design of invoking a separate "kernel script" (`k_ausd_bp_ta_bcp_iccid.ksh`) was preserved by creating a distinct BigQuery Stored Procedure (`k_ausd_bp_ta_bcp_iccid`). This maintains the separation of concerns, allowing the core data processing logic to be developed, tested, and maintained independently from the orchestration layer.
*   **Leveraging Native BigQuery Features**: Shell-specific constructs and helper scripts were replaced by BigQuery SQL equivalents:
    *   `trap` for error handling was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks.
    *   Date utility functions (e.g., `h_alis_date.ksh`) were replaced by BigQuery's native date functions (`CURRENT_DATE()`, `FORMAT_DATE`, `PARSE_DATE`).
    *   Parameter defaulting and validation logic was implemented using `COALESCE`, `IF` statements, and `RAISE` for explicit error signaling.

**Notable Trade-offs:**

*   **Loss of Direct File System Access**: BigQuery Stored Procedures operate within the BigQuery execution environment and do not have direct access to the file system. Any file I/O operations or external command executions present in the original `k_ausd_bp_ta_bcp_iccid.ksh` would require significant redesign, potentially involving Google Cloud Storage (GCS) or other GCP services.
*   **Re-implementation of Helper Logic**: The functionality of sourced shell helper scripts (`$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) had to be re-implemented directly in BigQuery SQL. While feasible, this can sometimes be more verbose or less flexible than shell scripting for certain types of tasks.
*   **Dependency on Kernel Implementation**: The core data processing logic resides in the `k_ausd_bp_ta_bcp_iccid.sql` stub. The overall migration is incomplete and cannot be fully validated until this kernel stored procedure is thoroughly analyzed and implemented.

---

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`my_project.my_dataset`) exists in your GCP project. If not, create it.
    *   `bq mk --dataset my_project:my_dataset`

2.  **IAM Permissions**:
    *   Grant the necessary IAM roles to the service account or user that will be executing these BigQuery Stored Procedures. At a minimum, this typically includes:
        *   `BigQuery Data Editor` (or more granular `BigQuery Data Viewer` + `BigQuery Data Editor` on specific tables) for the `my_dataset` to allow writing to logging tables.
        *   `BigQuery Job User` to allow execution of BigQuery jobs and stored procedures.

3.  **Execute DDLs for Logging Tables**:
    *   Run the DDL scripts to create the `job_audit`, `job_log`, and `job_error_log` tables in your target dataset.
    *   `bq query --use_legacy_sql=false < ddl/job_audit.sql`
    *   `bq query --use_legacy_sql=false < ddl/job_log.sql`
    *   `bq query --use_legacy_sql=false < ddl/job_error_log.sql`

4.  **Deploy Stored Procedures**:
    *   Execute the SQL scripts to create or replace the orchestrator and kernel (stub) stored procedures in your target dataset.
    *   `bq query --use_legacy_sql=false < sprocs/ausd_bp_ta_ibcp_ccid.sql`
    *   `bq query --use_legacy_sql=false < sprocs/k_ausd_bp_ta_bcp_iccid.sql`

5.  **Implement Kernel Stored Procedure (`k_ausd_bp_ta_bcp_iccid.sql`)**:
    *   **CRITICAL**: The `sprocs/k_ausd_bp_ta_bcp_iccid.sql` file currently contains only a placeholder. A detailed analysis of the original `k_ausd_bp_ta_bcp_iccid.ksh` is required to translate its core data processing logic into BigQuery SQL. This fully implemented procedure must be deployed before the job can perform its intended function.

6.  **Orchestration Setup**:
    *   Configure the new orchestration tool (e.g., a Cloud Composer DAG, Cloud Workflows, or a scheduled query in BigQuery) to invoke the `my_project.my_dataset.ausd_bp_ta_ibcp_ccid` stored procedure.
    *   Ensure the orchestrator passes the `p_stichtag_str` and `p_wiederanlaufWert_in` parameters correctly.
    *   Set up the desired scheduling frequency and dependencies.

7.  **Notification Setup (if applicable)**:
    *   If the original `DWMSG_*` functions included email notifications, design and implement a corresponding GCP notification mechanism (e.g., a Cloud Function triggered by Pub/Sub messages from BigQuery logs, or Cloud Monitoring alerts) to send alerts on job failures.

---

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up and represent potential risks or incomplete aspects of the migration:

*   **Unknown Kernel Script Logic (B4 Item)**: The most significant unresolved item is the content and functionality of `k_ausd_bp_ta_bcp_iccid.ksh`. This script holds the core data processing, and its migration design is critical and currently undefined. A separate, detailed analysis of `k_ausd_bp_ta_bcp_iccid.ksh` is mandatory to fully implement `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid`.
*   **Complex Date Logic**: While `h_alis_date.ksh` is replaced by BigQuery functions, the original script comments mention `MIN(sysdate,maxladedatum)` and `FOSHoleLadedatum "DWH$TA_C_VERTRAG" v_ladedatum`. If the "maxladedatum" logic was critical and involved complex lookups (e.g., from `h_alis_fos_date.ksh`), its exact implementation needs to be understood and replicated in BigQuery. The current migration only infers `p_stichtag=$v_sysdate;`.
*   **Dynamic Script Execution**: The original script constructs the kernel script name dynamically using `${BERT_DIR_ROOT}`. This implies potential variations in the invoked script. The BigQuery target assumes a single, fixed kernel stored procedure. If `BERT_DIR_ROOT` dynamically altered the script's behavior or pointed to different scripts, this design might need re-evaluation.
*   **Orchestration Environment Differences**: The original shell script ran in a Linux environment with access to a filesystem and other binaries. The BigQuery stored procedure environment is more restricted. Any file I/O or system commands performed by the original kernel script will need complete redesign (e.g., using GCS for file operations, or Cloud Functions for external calls).
*   **UC4 Job Integration**: The current UC4 scheduler (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) invokes the original shell script. The migration of this UC4 job to a GCP orchestrator (e.g., Cloud Composer) needs to be planned and executed to ensure the correct scheduling and invocation of the new BigQuery stored procedure.

---

## 6. Validation

Validation involves testing the orchestrator stored procedure and verifying its interaction with the logging tables and the kernel stub.

**How to Run Tests:**

1.  **Execute the Orchestrator SP**:
    *   Use the BigQuery UI, `bq` command-line tool, or a client library to call `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`.
    *   **Test Cases**:
        *   **Successful Run (Default Parameters)**: `CALL my_project.my_dataset.ausd_bp_ta_ibcp_ccid(NULL, NULL);` (should default `p_stichtag` to `CURRENT_DATE()` and `p_wiederanlaufWert` to 0).
        *   **Successful Run (Explicit Parameters)**: `CALL my_project.my_dataset.ausd_bp_ta_ibcp_ccid('01012023', 12345);`
        *   **Invalid `p_stichtag_str`**: `CALL my_project.my_dataset.ausd_bp_ta_ibcp_ccid('2023-01-01', NULL);` (should raise an error due to invalid format).
        *   **Simulated Kernel Failure**: Temporarily modify `sprocs/k_ausd_bp_ta_bcp_iccid.sql` to include a `RAISE` statement (e.g., `RAISE USING MESSAGE 'Simulated kernel error';`) and then call the orchestrator SP. This tests the error handling path.

2.  **Query Logging Tables**:
    *   After each test run, query the `my_project.my_dataset.job_audit`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log` tables to inspect the recorded entries.

**What "Passing" Means:**

*   **Successful Runs**:
    *   `job_audit`: A new entry exists for the run with `status = 'OK'`, `start_timestamp` and `end_timestamp` populated, and `message = 'Job completed successfully'`. The `stichtag` and `wiederanlauf_wert` should match the input or defaulted values.
    *   `job_log`: Multiple `INFO` level entries should be present, detailing the job start, parameter values, kernel call, and successful completion.
    *   `job_error_log`: No new entries should be present for successful runs.
    *   The `k_ausd_bp_ta_bcp_iccid` stub will print a status message; once implemented, this procedure should execute its core logic without error.

*   **Failed Runs (e.g., Invalid Parameter, Simulated Kernel Failure)**:
    *   `job_audit`: A new entry exists for the run with `status = 'ERROR'`, `start_timestamp` and `end_timestamp` populated, and `message` indicating a failure.
    *   `job_log`: `ERROR` level entries should be present, detailing the failure.
    *   `job_error_log`: A new entry should be present, containing the `run_id`, `error_timestamp`, `procedure_name` (either `ausd_bp_ta_ibcp_ccid` or `k_ausd_bp_ta_bcp_iccid`), `error_code`, `error_message`, and `error_stack_trace`. The `stichtag` and `wiederanlauf_wert` should reflect the parameters at the time of the error.
    *   The orchestrator SP call itself should return an error, indicating the job failure.

---

## 7. Rollback Procedure

In case of issues or unexpected behavior after go-live, the following steps outline the procedure to roll back to the original KornShell script execution:

1.  **Stop New Orchestration**:
    *   Immediately halt or pause the new GCP orchestration mechanism (e.g., disable the Cloud Composer DAG, delete the scheduled query) that invokes `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`. This prevents any further execution of the migrated job.

2.  **Re-enable Original Scheduling**:
    *   Re-enable the original UC4 job (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) to resume invoking the `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh` script.
    *   Verify that the original job's schedule and parameters are correctly restored.

3.  **Monitor Original Job**:
    *   Closely monitor the first few runs of the original KornShell script to ensure it executes successfully, processes data as expected, and produces the correct output. Check its logs and any downstream dependencies.

4.  **Data Consistency Check**:
    *   If the new BigQuery job processed any data before the rollback, assess the state of the target tables. Determine if any partial data needs to be cleaned up, reverted, or if the original job can safely pick up from the last consistent state. This step is highly dependent on the specific logic within the `k_ausd_bp_ta_bcp_iccid` procedure.

5.  **Optional: Remove BigQuery Artifacts**:
    *   If a full revert is desired and the BigQuery artifacts are no longer needed, you may drop the created stored procedures and logging tables. This is typically not necessary for a quick rollback but might be considered for a permanent revert or cleanup.
        *   `DROP PROCEDURE IF EXISTS my_project.my_dataset.ausd_bp_ta_ibcp_ccid;`
        *   `DROP PROCEDURE IF EXISTS my_project.my_dataset.k_ausd_bp_ta_bcp_iccid;`
        *   `DROP TABLE IF EXISTS my_project.my_dataset.job_audit;`
        *   `DROP TABLE IF EXISTS my_project.my_dataset.job_log;`
        *   `DROP TABLE IF EXISTS my_project.my_dataset.job_error_log;`

---
```