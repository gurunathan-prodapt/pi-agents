```markdown
# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh`. The script's primary function is to handle parameter parsing, date defaulting, error management, and logging, before invoking a core business logic script.

The migration target platform is **Google BigQuery**. The orchestration logic has been re-implemented as a BigQuery Stored Procedure, `ausd_bp_ta_bpr_optionen_wrapper`. Supporting logging and status tracking functionalities have been migrated to dedicated BigQuery tables and helper procedures.

**Important Note:** The core business logic, originally residing in `k_ausd_bp_ta_bpr_optionen.ksh`, has been represented by a placeholder BigQuery Stored Procedure (`k_ausd_bp_ta_bpr_optionen`). Its detailed migration is a separate, subsequent effort and is not covered by this document beyond its invocation by the wrapper.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`ddl/create_job_log_table.sql`**
    *   **Role:** Defines the schema for the `job_log` BigQuery table. This table serves as the central repository for all job execution logs, replacing the file-based logging of the original KornShell script. It captures details such as job run ID, timestamp, log level, message, `Stichtag`, `Wiederanlaufwert`, and error information.
*   **`ddl/create_job_status_table.sql`**
    *   **Role:** Defines the schema for the `job_status` BigQuery table. This table tracks the overall lifecycle and status (RUNNING, SUCCESS, FAILED) of each job run, including start/end timestamps and key parameters.
*   **`procedures/f_alis_log_message.sql`**
    *   **Role:** A BigQuery Stored Procedure that encapsulates the logic for inserting log entries into the `job_log` table. It standardizes logging across all migrated BigQuery procedures, replacing the `DWMSG_ErzeugeEintrag` function and direct `print` statements from the original script.
*   **`procedures/f_alis_update_job_status.sql`**
    *   **Role:** A BigQuery Stored Procedure responsible for creating and updating entries in the `job_status` table. It handles setting the initial `RUNNING` status and updating to `SUCCESS` or `FAILED` upon job completion or error, replacing functionalities like `DWMSG_SetzeStatusOK`.
*   **`procedures/k_ausd_bp_ta_bpr_optionen.sql`**
    *   **Role:** A **placeholder** BigQuery Stored Procedure for the core business logic. This procedure is invoked by the `ausd_bp_ta_bpr_optionen_wrapper` and represents the future migration target for `k_ausd_bp_ta_bpr_optionen.ksh`. Currently, it only logs its invocation.
*   **`procedures/ausd_bp_ta_bpr_optionen_wrapper.sql`**
    *   **Role:** The main BigQuery Stored Procedure that replaces the `r_ausd_bp_ta_bpr_optionen.ksh` KornShell script. It handles:
        *   Parsing and validating input parameters (`p_stichtag_raw`, `p_wiederanlaufwert_raw`).
        *   Defaulting `Stichtag` to the current date and `Wiederanlaufwert` to `0` if not provided.
        *   Initializing and updating job status in `job_status` table.
        *   Comprehensive logging of job events (start, parameter processing, success, failure) to `job_log` table.
        *   Invoking the core business logic procedure (`k_ausd_bp_ta_bpr_optionen`).
        *   Implementing robust error handling using `EXCEPTION WHEN ERROR THEN` blocks, logging errors, and re-raising them to the orchestrator.

## 3. Key Design Decisions

*   **Target Platform Choice (BigQuery Stored Procedures):** BigQuery Stored Procedures were chosen to leverage BigQuery's native capabilities for data processing, scalability, and integration within the Google Cloud ecosystem. This allows for a direct translation of procedural logic (parameter handling, control flow) from KornShell to SQL scripting, minimizing the need for external compute resources for orchestration.
*   **Separation of Concerns (Wrapper vs. Core Logic):** The decision to migrate the `r_ausd_bp_ta_bpr_optionen.ksh` (wrapper) and `k_ausd_bp_ta_bpr_optionen.ksh` (core logic) into separate BigQuery Stored Procedures (`ausd_bp_ta_bpr_optionen_wrapper` and `k_ausd_bp_ta_bpr_optionen`) maintains the original script's architectural separation. This modularity simplifies development, testing, and future maintenance, especially given the unknown complexity of the core logic.
*   **Centralized Logging and Status Tracking:** Instead of file-based logging, dedicated BigQuery tables (`job_log`, `job_status`) were introduced. This provides a structured, queryable, and scalable solution for monitoring job executions, enabling easier analysis, alerting, and integration with other GCP monitoring tools. Helper procedures (`f_alis_log_message`, `f_alis_update_job_status`) abstract the table interaction.
*   **Parameter Handling Translation:** KornShell's `getopts` and variable assignments are replaced by `IN` parameters in the BigQuery Stored Procedure. Defaulting logic uses `IFNULL`, `NULLIF`, `DECLARE`, and `SET` statements, along with `PARSE_DATE` and `CAST` for type conversion. This ensures strong typing and validation within BigQuery.
*   **Error Handling Paradigm Shift:** The `set -e` and `trap` mechanisms of KornShell are replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. This provides structured error capture, allowing for logging of error details (`@@error.message`, `@@error.stack_trace`) and explicit error signaling (`SIGNAL SQLSTATE`) to the calling orchestrator.
*   **Re-implementation of Helper Functions:** Generic helper script functionalities (e.g., date manipulation, parameter validation, logging utilities) are either directly translated into BigQuery SQL functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`) or reimplemented as BigQuery helper procedures (`f_alis_log_message`, `f_alis_update_job_status`). This avoids external dependencies and keeps the solution entirely within BigQuery.

**Notable Trade-offs:**
*   **Loss of Direct Shell Environment Access:** Environment variables and sourced scripts (`.dw_init`) need explicit translation to BigQuery parameters, configuration tables, or hardcoded values, which might require more upfront analysis.
*   **Explicit Error Handling:** While more robust, BigQuery's `EXCEPTION` blocks require more verbose coding compared to the implicit `set -e` or generic `trap` of shell scripts.
*   **Placeholder for Core Logic:** The current solution is incomplete without the full migration of `k_ausd_bp_ta_bpr_optionen.ksh`, which represents a significant dependency.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_bq_dataset` in the generated code) exists in your GCP project (`your_gcp_project`). If not, create it:
        ```bash
        bq mk --dataset your_gcp_project:your_bq_dataset
        ```
    *   **Action:** Replace `your_gcp_project` and `your_bq_dataset` placeholders in all generated `.sql` files with your actual project ID and dataset name.
2.  **IAM Permissions:**
    *   The service account or user identity that will execute the `ausd_bp_ta_bpr_optionen_wrapper` procedure must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on `your_gcp_project.your_bq_dataset` to create/update tables and insert/update data into `job_log` and `job_status`.
        *   `BigQuery Data Viewer` on any source tables that `k_ausd_bp_ta_bpr_optionen` (once implemented) might read from.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
    *   **Action:** Grant necessary IAM roles to the execution identity.
3.  **Deploy DDLs:**
    *   Execute `ddl/create_job_log_table.sql` to create the `job_log` table.
    *   Execute `ddl/create_job_status_table.sql` to create the `job_status` table.
    *   **Action:** Run these DDLs in BigQuery.
4.  **Deploy Helper Procedures:**
    *   Execute `procedures/f_alis_log_message.sql` to create the logging helper procedure.
    *   Execute `procedures/f_alis_update_job_status.sql` to create the status update helper procedure.
    *   **Action:** Run these procedure creation scripts in BigQuery.
5.  **Deploy Core Logic Placeholder:**
    *   Execute `procedures/k_ausd_bp_ta_bpr_optionen.sql` to create the placeholder procedure for the core business logic.
    *   **Action:** Run this procedure creation script in BigQuery.
6.  **Deploy Wrapper Procedure:**
    *   Execute `procedures/ausd_bp_ta_bpr_optionen_wrapper.sql` to create the main orchestration wrapper procedure.
    *   **Action:** Run this procedure creation script in BigQuery.
7.  **Scheduling/Orchestration Configuration:**
    *   Configure your chosen orchestrator (e.g., Cloud Composer/Airflow, Google Cloud Workflows, or BigQuery Scheduled Queries) to invoke the `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper` procedure.
    *   Ensure the orchestrator passes the `p_stichtag_raw` and `p_wiederanlaufwert_raw` parameters as needed, or relies on the procedure's defaults.
    *   **Action:** Set up the external orchestrator.
8.  **Secrets/Configuration:**
    *   Review the original `.dw_init` file and any other environment variables used by the KornShell script. If any contained sensitive information or critical configurations, these must be securely managed (e.g., Google Secret Manager) and passed as parameters or retrieved within the BigQuery environment.
    *   **Action:** Analyze and migrate any critical configurations or secrets.

## 5. Known Gaps & Unresolved References

*   **Core Business Logic (`k_ausd_bp_ta_bpr_optionen.ksh`) Migration (B4 Item):** This is the most significant gap. The `k_ausd_bp_ta_bpr_optionen` BigQuery Stored Procedure is currently a placeholder. Its full design, implementation, and testing are required for the complete job functionality. This is a **critical dependency** for the overall job migration.
*   **Custom Shell Function Fidelity:** The exact behavior of all custom KornShell functions (e.g., `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, and the various `DWMSG_*` functions) from the sourced helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) has been translated to generic BigQuery logging and status updates. Specific nuances or complex logic within these original functions might require further analysis and more detailed BigQuery UDFs or procedures to achieve exact parity.
*   **`.dw_init` Environment Variables:** The full content and impact of the `$HOME/.dw_init` script and other environment variables (`$BERT_DIR_ROOT`) have not been exhaustively analyzed. Any critical configurations or paths defined there might need to be explicitly managed within BigQuery (e.g., as procedure parameters, BigQuery lookup tables, or constants).
*   **Error Handling Granularity:** While BigQuery's `EXCEPTION` blocks provide robust error handling, replicating the precise behavior of all possible KornShell `trap` signals (e.g., `INT`, `STOP`, `CONT`) might not be directly achievable or necessary. The current implementation focuses on catching `ERROR` conditions within the SQL execution.
*   **`line_number` in Error Logging:** The `@@error.statement_text_start` used for `line_number` in error logging provides the starting line of the statement that caused the error. This is an approximation and might not correspond to the exact logical line number in the original script or the BigQuery procedure for complex statements.

## 6. Validation

To validate the migrated `ausd_bp_ta_bpr_optionen_wrapper` procedure, perform the following steps:

1.  **Manual Execution (BigQuery Console/CLI):**
    *   **Successful Run (Default Parameters):**
        ```sql
        CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper`(NULL, NULL);
        ```
    *   **Successful Run (Explicit Parameters):**
        ```sql
        CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper`('2023-10-26', '1');
        ```
    *   **Invalid `Stichtag`:**
        ```sql
        CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper`('2023/10/26', NULL);
        ```
    *   **Invalid `Wiederanlaufwert`:**
        ```sql
        CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper`('2023-10-26', 'abc');
        ```
    *   **Simulate Core Logic Failure:** Temporarily modify `procedures/k_ausd_bp_ta_bpr_optionen.sql` to include an error (e.g., uncomment `SELECT 1/0;`) and re-deploy it. Then run the wrapper:
        ```sql
        CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper`(NULL, NULL);
        ```
        Remember to revert the change in `k_ausd_bp_ta_bpr_optionen.sql` after testing.

2.  **Check `job_log` Table:**
    *   Query `your_gcp_project.your_bq_dataset.job_log` after each test run.
    *   **Passing Criteria:**
        *   For successful runs, expect `INFO` messages detailing parameter parsing, job start, core logic invocation, and job completion. No `ERROR` messages should be present.
        *   For runs with invalid parameters, expect `ERROR` messages indicating the parsing failure and the job terminating early.
        *   For simulated core logic failure, expect `ERROR` messages from the wrapper indicating the failure of the `k_ausd_bp_ta_bpr_optionen` procedure.
        *   `job_run_id` should be consistent for all entries related to a single execution.
        *   `stichtag` and `wiederanlaufwert` should reflect the parameters passed or defaulted.

3.  **Check `job_status` Table:**
    *   Query `your_gcp_project.your_bq_dataset.job_status` after each test run.
    *   **Passing Criteria:**
        *   For successful runs, an entry should exist with `status = 'SUCCESS'`, `start_timestamp` and `end_timestamp` populated.
        *   For runs with invalid parameters or simulated core logic failure, an entry should exist with `status = 'FAILED'`, `start_timestamp` and `end_timestamp` populated.
        *   The `job_run_id` should match the corresponding entries in `job_log`.

4.  **Orchestrator Integration Test:**
    *   Once the manual tests pass, configure the external orchestrator (e.g., Airflow DAG) to call the wrapper procedure.
    *   Trigger the orchestrator and verify its logs and the BigQuery `job_log`/`job_status` tables for correct behavior.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following steps outline the rollback procedure:

1.  **Stop New Invocations:**
    *   Immediately disable or pause the external orchestrator (e.g., Cloud Composer DAG, Google Cloud Workflow, BigQuery Scheduled Query) that invokes the `ausd_bp_ta_bpr_optionen_wrapper` procedure.
2.  **Revert Orchestrator Configuration:**
    *   If the original KornShell script was scheduled by an orchestrator, re-enable its original configuration.
3.  **Delete BigQuery Procedures:**
    *   Delete the migrated BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.k_ausd_bp_ta_bpr_optionen`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.f_alis_log_message`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.f_alis_update_job_status`;
        ```
4.  **Delete BigQuery Tables (Optional, Data Retention Policy Dependent):**
    *   If the `job_log` and `job_status` tables are not needed for historical analysis of the migrated job's runs, they can be dropped. **Exercise caution as this deletes data.**
        ```sql
        DROP TABLE IF EXISTS `your_gcp_project.your_bq_dataset.job_log`;
        DROP TABLE IF EXISTS `your_gcp_project.your_bq_dataset.job_status`;
        ```
5.  **Re-enable Original KornShell Script:**
    *   Ensure the original `r_ausd_bp_ta_bpr_optionen.ksh` script is in place and executable on its legacy environment.
    *   Re-enable its original scheduling mechanism.
6.  **Verify Legacy Job:**
    *   Monitor the re-enabled legacy job to ensure it is running correctly and processing data as expected.

This rollback procedure aims to quickly restore the previous working state by reverting to the original KornShell script and removing the newly deployed BigQuery components.
```