# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` from a legacy Unix/KornShell environment to Google BigQuery.

The original script served as an orchestration and wrapper script for contract data reconciliation related to the `ta_period` table. It handled environment setup, parameter parsing, logging, and error trapping, before invoking a core processing script (`k_ausd_v_ta_period.ksh`).

The migration re-implements this orchestration and error handling logic as a BigQuery Stored Procedure, leveraging BigQuery's native capabilities for logging and error management.

## 2. Generated Artifacts

The migration process generates the following artifacts in the target BigQuery environment:

*   **BigQuery Stored Procedure:**
    *   `project.dataset.sp_vertragsdatenabgleich`: This procedure directly replaces the functionality of `r_ausd_v_ta_period.ksh`, managing orchestration, parameter handling, logging, and error trapping.
*   **BigQuery Tables (for Logging and Status Tracking):**
    *   `project.dataset.dw_job_registry`: Stores job entry numbers and metadata.
    *   `project.dataset.dw_job_log`: Detailed log entries for job execution.
    *   `project.dataset.dw_job_status`: Tracks the overall status (OK/ERR) of job runs.
    *   `project.dataset.dw_job_error`: Records specific error details when failures occur.
    *   `project.dataset.dw_job_stichtag`: Stores reference date information relevant to job execution.
*   **Invoked BigQuery Stored Procedure (Assumed):**
    *   `project.dataset.sp_k_ausd_v_ta_period`: This is the migrated version of the core processing script `k_ausd_v_ta_period.ksh`, which `sp_vertragsdatenabgleich` will call. Its generation is a prerequisite for this migration.
*   **Deployment Scripts:**
    *   Bash/Python scripts utilizing the `bq` CLI or BigQuery client libraries to deploy the DDL for tables and the SQL for stored procedures.
*   **Orchestration Configuration (Optional):**
    *   A Cloud Scheduler job or Cloud Composer DAG definition (YAML/Python) if external scheduling of `sp_vertragsdatenabgleich` is required.

## 3. Key Design Decisions

The following key design decisions were made to transition from the KornShell environment to BigQuery:

*   **Orchestration Logic as BigQuery Stored Procedure:** The KornShell wrapper script `r_ausd_v_ta_period.ksh` is re-implemented as a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This centralizes the orchestration logic within the BigQuery ecosystem, eliminating external script dependencies and leveraging BigQuery's native execution environment.
*   **Centralized Logging and Status Tracking in BigQuery Tables:** File-based logging and status updates from the original script are replaced by inserts into dedicated BigQuery tables (`dw_job_log`, `dw_job_status`, `dw_job_error`, `dw_job_registry`, `dw_job_stichtag`). This provides a queryable, centralized, and structured repository for all job execution metadata, improving auditability and monitoring capabilities.
*   **Native BigQuery Error Handling:** The `trap` mechanisms in KornShell are replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` blocks. This provides structured error handling within the SQL procedure, allowing for graceful failure, error logging, and propagation of failure status.
*   **Parameterization via Stored Procedure Arguments:** Command-line argument parsing (`getopts`) is replaced by direct input parameters to the BigQuery Stored Procedure. This aligns with BigQuery's procedure invocation model and provides clear, type-safe parameter definitions.
*   **Invocation of Core Logic via `CALL` Statement:** The execution of the core script (`k_ausd_v_ta_period.ksh`) is translated into a `CALL` statement to its migrated BigQuery Stored Procedure counterpart (`sp_k_ausd_v_ta_period`). This maintains modularity and allows the orchestration layer to trigger the business logic within the same BigQuery environment.
*   **Elimination of Filesystem and OS Dependencies:** All dependencies on shell utilities (`date`, `tee`), environment files (`.dw_init`), and custom shell functions (`DWMSG_*`) are replaced by BigQuery SQL functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`), direct SQL logic, or the new logging/status tables. This removes external system dependencies and simplifies deployment.

**Notable Trade-offs:**

*   **Dependency on Core Script Migration:** The successful functioning of `sp_vertragsdatenabgleich` is entirely dependent on the prior or concurrent migration of `k_ausd_v_ta_period.ksh` to `sp_k_ausd_v_ta_period`. Any issues with the core script's migration will directly impact this wrapper.
*   **Loss of Direct Console Output:** While the original script could print messages to the console/log file, the BigQuery Stored Procedure primarily logs to tables. Direct real-time console output for monitoring during execution is not a primary feature, though logs can be queried immediately.

## 4. Manual Steps Before Go-Live

Before the migrated solution can go live, the following manual steps or prerequisites must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **IAM Permissions Configuration:**
    *   Grant appropriate IAM roles to the service account or user that will execute the BigQuery Stored Procedure. This includes permissions to:
        *   Execute stored procedures (`bigquery.routines.call`).
        *   Read/Write to the `dw_job_registry`, `dw_job_log`, `dw_job_status`, `dw_job_error`, and `dw_job_stichtag` tables (`bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.insertData`).
        *   Call `project.dataset.sp_k_ausd_v_ta_period` (`bigquery.routines.call`).
3.  **Core Script Migration and Deployment:** The BigQuery Stored Procedure `project.dataset.sp_k_ausd_v_ta_period` (migrated from `k_ausd_v_ta_period.ksh`) must be deployed and functional before `sp_vertragsdatenabgleich` can be used.
4.  **Initial Data Population (if applicable):** If `dw_job_registry` requires initial seeding for job entry numbers, this should be performed.
5.  **Secrets Management (if applicable):** If the original `.dw_init` file contained any sensitive information, these must be securely managed (e.g., using Google Secret Manager) and passed as parameters or configured appropriately within the BigQuery environment.
6.  **Scheduling Configuration (if applicable):** If the job is to be scheduled, configure the Cloud Scheduler job or Cloud Composer DAG to invoke `project.dataset.sp_vertragsdatenabgleich` with the necessary parameters.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent known limitations/risks:

*   **Unresolved Core Logic (`k_ausd_v_ta_period.ksh`):** The actual data reconciliation logic resides in the core script. Its successful migration to `project.dataset.sp_k_ausd_v_ta_period` is critical and is a prerequisite for this wrapper's functionality. The details of that migration are outside the scope of this document.
*   **Missing Complexity Data:** The absence of complexity analysis for the original script means potential hidden complexities or edge cases might not have been fully accounted for in the migration design.
*   **Custom Shell Functions (`DWMSG_*`):** The exact implementation of the custom `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` functions was not available. The migration assumes standard logging and error handling patterns. A deeper review of their original logic might be necessary if unexpected behavior arises.
*   **Environment Initialization (`. $HOME/.dw_init`):** The contents of this environment file are unknown. Any critical environment variables, paths, or configurations defined within it need to be identified and explicitly replicated as BigQuery procedure parameters or BigQuery project/dataset configurations.
*   **Parameter List (`ParamList="s:l:"`):** The original script uses `getopts` with `-s` and `-l` parameters, but the script itself does not explicitly handle them. It's unclear if these parameters are passed through to `k_ausd_v_ta_period.ksh` or are remnants. Their purpose and usage by the core script need to be confirmed and incorporated into `sp_k_ausd_v_ta_period` if still relevant.

## 6. Validation

To validate the successful migration and functionality of `sp_vertragsdatenabgleich`:

1.  **Execute the Stored Procedure:**
    *   Manually execute `CALL project.dataset.sp_vertragsdatenabgleich();` (or with relevant parameters if any are introduced beyond `-h`).
    *   Execute `CALL project.dataset.sp_vertragsdatenabgleich(p_help => '-h');` to test the help message functionality.
2.  **Check BigQuery Log Tables:**
    *   Query `project.dataset.dw_job_log` to ensure detailed log entries are recorded correctly, including start/end messages and any intermediate steps.
    *   Query `project.dataset.dw_job_status` to verify the final status of the job run (expected 'OK' for successful runs).
    *   Query `project.dataset.dw_job_registry` to confirm new job entry numbers are generated and recorded.
    *   Query `project.dataset.dw_job_stichtag` to ensure date information is stored correctly.
3.  **Verify Core Script Invocation:**
    *   Confirm that `project.dataset.sp_k_ausd_v_ta_period` was successfully called by inspecting the logs in `dw_job_log` or by checking its own logging/output.
4.  **Test Error Handling:**
    *   Introduce a controlled error within `sp_k_ausd_v_ta_period` (e.g., by raising an explicit error or causing a division by zero) and execute `sp_vertragsdatenabgleich`.
    *   Verify that `dw_job_error` contains the error details and `dw_job_status` shows 'ERR'.
    *   Confirm that the `sp_vertragsdatenabgleich` procedure itself terminates with an error, signaling failure to the caller.

**"Passing" Criteria:**

*   `sp_vertragsdatenabgleich` executes without unhandled errors.
*   All relevant BigQuery logging and status tables (`dw_job_log`, `dw_job_status`, `dw_job_registry`, `dw_job_stichtag`) are populated correctly for both success and failure scenarios.
*   The final status in `dw_job_status` accurately reflects the outcome of the execution ('OK' for success, 'ERR' for failure).
*   The help message (`-h` parameter) is displayed correctly.
*   The core stored procedure `sp_k_ausd_v_ta_period` is successfully invoked.

## 7. Rollback Procedure

In the event of critical issues or unforeseen problems after go-live, the following rollback procedure can be initiated:

1.  **Halt New Executions:** Stop any scheduled invocations (e.g., Cloud Scheduler, Cloud Composer DAGs) of `project.dataset.sp_vertragsdatenabgleich`.
2.  **Revert to Original Script:** Re-enable and resume execution of the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` in the legacy environment. Ensure all its dependencies are functional.
3.  **Delete Migrated Artifacts (Optional, for cleanup):**
    *   Delete the BigQuery Stored Procedure: `DROP PROCEDURE IF EXISTS project.dataset.sp_vertragsdatenabgleich;`
    *   Delete the associated logging and status tables:
        *   `DROP TABLE IF EXISTS project.dataset.dw_job_registry;`
        *   `DROP TABLE IF EXISTS project.dataset.dw_job_log;`
        *   `DROP TABLE IF EXISTS project.dataset.dw_job_status;`
        *   `DROP TABLE IF EXISTS project.dataset.dw_job_error;`
        *   `DROP TABLE IF EXISTS project.dataset.dw_job_stichtag;`
    *   (Note: The core script `sp_k_ausd_v_ta_period` and its related artifacts would need its own rollback procedure.)
4.  **Review and Rectify:** Analyze the reasons for the rollback, address the identified issues, and update the migration design or implementation as necessary before attempting re-migration.