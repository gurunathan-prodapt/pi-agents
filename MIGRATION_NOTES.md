# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_period.ksh` from its legacy environment to Google Cloud BigQuery.

The original script served as an orchestration wrapper for a contract data reconciliation job related to the `ta_period` table. Its primary responsibilities included environment initialization, command-line parameter validation, custom logging and error handling via a `DWMSG_` framework, and invoking a core business logic script (`k_ausd_v_ta_period.ksh`).

The migration targets Google Cloud BigQuery, where the wrapper logic has been transformed into a BigQuery Stored Procedure (`sp_bert_v_ta_period`). The custom logging framework has been replaced by dedicated BigQuery tables (`job_log`, `job_status`, `job_control`). The core business logic, originally in `k_ausd_v_ta_period.ksh`, is represented by a placeholder BigQuery Stored Procedure (`sp_k_ausd_v_ta_period`), awaiting detailed analysis and migration.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_log` table. This table serves as the central repository for all job-related log messages, replacing the custom `DWMSG_` logging framework and file-based logging of the original KornShell script. It captures details such as job name, entry number, log level, messages, and timestamps.
*   **`sql/ddl/job_status.sql`**
    *   **Role:** Defines the DDL for the `job_status` table. This table tracks the current status of each job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED'), providing a structured way to monitor job execution, replacing implicit status tracking and exit codes from the shell script.
*   **`sql/ddl/job_control.sql`**
    *   **Role:** Defines the DDL for the `job_control` table. This table stores control-related information for each job run, such as the business date (`stichtag`), which was previously managed through environment variables or parameters in the legacy system.
*   **`sql/procedures/sp_k_ausd_v_ta_period.sql`**
    *   **Role:** This is a **placeholder** BigQuery Stored Procedure. It is intended to house the migrated core business logic that was originally contained within `k_ausd_v_ta_period.ksh`. Currently, it includes basic logging for its start and end, and accepts parameters that would be passed from the wrapper. Its actual data transformation logic needs to be developed.
*   **`sql/procedures/sp_bert_v_ta_period.sql`**
    *   **Role:** This is the main BigQuery Stored Procedure that replaces the `r_ausd_v_ta_period.ksh` wrapper script. It handles:
        *   Parsing input parameters (`p_help`, `p_param_s`, `p_param_l`).
        *   Simulating the `DWMSG_` framework by interacting with the `job_log`, `job_status`, and `job_control` tables for logging and status updates.
        *   Orchestrating the execution by calling the `sp_k_ausd_v_ta_period` (core logic placeholder).
        *   Implementing error handling via BigQuery's `EXCEPTION WHEN ERROR` block, mimicking the `trap` functionality of the shell script.
        *   Providing a usage message similar to the original script's help output.

## 3. Key design decisions

The migration strategy focused on translating the orchestration and control flow aspects of the KornShell script into native BigQuery constructs, while acknowledging the placeholder nature of the core business logic.

*   **BigQuery Stored Procedures for Orchestration:** The wrapper script's control flow, parameter parsing, and invocation of the core logic were directly translated into a BigQuery Stored Procedure (`sp_bert_v_ta_period`). This leverages BigQuery's native capabilities for procedural logic, transaction management, and direct interaction with data.
*   **Structured Logging and Status Management:** The custom `DWMSG_` framework, which relied on shell functions and file-based logging, was replaced by dedicated BigQuery tables (`job_log`, `job_status`, `job_control`). This provides a structured, queryable, and scalable logging solution, aligning with cloud-native best practices for observability.
*   **Error Handling via `EXCEPTION` Blocks:** The shell script's `trap` mechanism for signal handling (e.g., `INT`, `ERR`) was re-implemented using BigQuery SQL's `EXCEPTION WHEN ERROR` blocks. This allows for robust error capture, logging, and status updates within the BigQuery environment.
*   **Parameter Mapping:** Shell script `getopts` parameter parsing was directly mapped to BigQuery Stored Procedure input parameters (e.g., `-h` to `p_help`, `-s` to `p_param_s`). This maintains the external interface of the job.
*   **Placeholder for Core Logic:** Recognizing that `k_ausd_v_ta_period.ksh` contains the actual data transformation, a placeholder stored procedure (`sp_k_ausd_v_ta_period`) was created. This allows for the independent migration of the wrapper while clearly delineating the remaining work for the core logic.
*   **Environment Initialization Replacement:** The `$HOME/.dw_init` and sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were replaced by explicit parameter passing, BigQuery project/dataset configurations, and the logic embedded directly within the stored procedures or the new logging tables. This eliminates shell-specific environment dependencies.
*   **Trade-offs:**
    *   **Loss of Direct File System Interaction:** The ability to `tee -a` output to a physical log file is replaced by structured inserts into `job_log`. While more scalable and queryable, it changes the immediate access pattern to logs.
    *   **Shell-Specific Features:** Full emulation of all shell features (e.g., complex `trap` scenarios, external process execution) within pure BigQuery SQL is not always feasible. For highly complex orchestration or external system interactions, an additional orchestration layer like Cloud Composer (Apache Airflow) or Cloud Workflows might be necessary, though not strictly required for this wrapper's current scope.
    *   **Dependency on Core Script Migration:** The full functionality of the migrated job is contingent on the complete and correct migration of `k_ausd_v_ta_period.ksh`.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`your_bq_dataset` in the generated code) exists within `your_gcp_project`. If not, create it:
    ```bash
    bq mk --dataset your_gcp_project:your_bq_dataset
    ```
2.  **IAM Permissions:**
    *   The service account or user identity that will deploy and execute these BigQuery procedures must have appropriate IAM roles.
    *   Minimum roles typically include `BigQuery Data Editor` (for creating tables and inserting data) and `BigQuery Job User` (for running jobs/procedures) on the target dataset.
    *   If the procedures interact with other datasets or projects, corresponding permissions will be required.
3.  **Deploy DDLs:** Execute the DDL scripts to create the logging and control tables:
    ```bash
    bq query --use_legacy_sql=false < sql/ddl/job_log.sql
    bq query --use_legacy_sql=false < sql/ddl/job_status.sql
    bq query --use_legacy_sql=false < sql/ddl/job_control.sql
    ```
4.  **Deploy Stored Procedures:** Execute the stored procedure creation scripts:
    ```bash
    bq query --use_legacy_sql=false < sql/procedures/sp_k_ausd_v_ta_period.sql
    bq query --use_legacy_sql=false < sql/procedures/sp_bert_v_ta_period.sql
    ```
5.  **Update Placeholders:** Replace `your_gcp_project` and `your_bq_dataset` placeholders in the generated SQL files with your actual GCP project ID and BigQuery dataset ID before deployment.
6.  **Scheduling Configuration:**
    *   Determine how `sp_bert_v_ta_period` will be scheduled. Options include:
        *   **Cloud Scheduler:** For simple, time-based scheduling.
        *   **Cloud Composer (Apache Airflow):** For complex workflows, dependency management, and integration with other GCP services. A Python DAG would be created to call the BigQuery Stored Procedure.
        *   **Cloud Workflows:** For event-driven or sequential orchestration of GCP services.
        *   **Manual Execution:** For ad-hoc runs or testing.
    *   Configure the chosen scheduler to invoke `CALL your_gcp_project.your_bq_dataset.sp_bert_v_ta_period(...)` with the necessary parameters.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up, including potential redesign (B4) items:

*   **`k_ausd_v_ta_period.ksh` Core Logic Migration:** The most significant gap is the actual business logic within `k_ausd_v_ta_period.ksh`. The `sp_k_ausd_v_ta_period` is a placeholder. A detailed analysis of the original script's data transformation, sources, and targets is required to fully migrate this component into BigQuery SQL.
*   **`DWMSG_` Framework Implementation Details:** While the logging tables provide a structured replacement, the exact logic and error codes of all `DWMSG_` functions (e.g., `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Fehlerbehandlung`) were not fully reverse-engineered. The current implementation provides a functional equivalent but might need fine-tuning if specific error codes or message formats are critical.
*   **Shell-Specific Feature Emulation:**
    *   The `trap` command's full behavior, especially for signals beyond `ERR` (like `INT` for graceful shutdown), is approximated by BigQuery's `EXCEPTION` block. For more advanced signal handling or external process control, an orchestration layer (e.g., Cloud Composer) would be necessary.
    *   The sourcing of utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) is replaced by direct BigQuery SQL logic. If these utilities contained complex, reusable functions, they might warrant separate BigQuery functions or helper procedures.
*   **Parameter `s:` and `l:` Usage:** The original `getopts` definition included `-s` and `-l` parameters, but their direct usage within `r_ausd_v_ta_period.ksh` was not evident. It's assumed they are passed to the core script (`k_ausd_v_ta_period.ksh`). Their exact purpose and impact on the core logic need to be clarified during the `k_ausd_v_ta_period.ksh` migration.
*   **Lineage Completeness:** The design document noted a gap in automated lineage discovery for orchestrator scripts. While the migration explicitly links the wrapper to the core logic, a thorough manual review of the end-to-end data flow, especially once `sp_k_ausd_v_ta_period` is fully implemented, is recommended to ensure complete data lineage.
*   **Secrets Management:** If the original `k_ausd_v_ta_period.ksh` or any of its dependencies accessed sensitive credentials, a robust secrets management solution (e.g., Google Secret Manager) should be integrated into the core logic's migration.

## 6. Validation

To validate the migrated `sp_bert_v_ta_period` and its supporting DDLs, perform the following steps:

1.  **Prerequisites:** Ensure all DDLs and stored procedures are deployed to the target BigQuery dataset as per the "Manual steps before go-live" section.

2.  **Test Cases:**

    *   **Help Message (`p_help = TRUE`):**
        ```sql
        CALL `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period`(p_help => TRUE);
        ```
        *   **Expected "Passing" Result:** The query should return a single row with a formatted help message containing program name, version, and parameter descriptions. No entries should be made in `job_log`, `job_status`, or `job_control`.

    *   **Successful Execution (Default Parameters):**
        ```sql
        CALL `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period`();
        ```
        *   **Expected "Passing" Result:**
            *   The procedure should complete successfully without errors.
            *   Verify entries in `your_gcp_project.your_bq_dataset.job_log`:
                *   An 'I' (Info) entry for job start.
                *   An 'I' entry for core script start (from `sp_k_ausd_v_ta_period`).
                *   An 'I' entry for core script end (from `sp_k_ausd_v_ta_period`).
                *   An 'S' (Success) entry for wrapper completion.
                *   All entries should have the correct `job_name`, `job_entry_nr`, and `business_date`.
            *   Verify entry in `your_gcp_project.your_bq_dataset.job_status`:
                *   One entry for the `JobKennung` with `status = 'SUCCESS'` and a recent `updated_at` timestamp.
            *   Verify entry in `your_gcp_project.your_bq_dataset.job_control`:
                *   One entry for the `JobKennung` with the correct `stichtag` (current date).
            *   The `job_header_info` should be returned as a result set.

    *   **Successful Execution (with `p_param_s` and `p_param_l`):**
        ```sql
        CALL `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period`(p_param_s => 'test_s_value', p_param_l => 'test_l_value');
        ```
        *   **Expected "Passing" Result:** Similar to the default successful execution, but verify that the `p_param_s` and `p_param_l` values are correctly logged in the `job_log` entries from `sp_k_ausd_v_ta_period`.

    *   **Error Handling (Simulated Failure in Core Script):**
        *   **Step 1: Modify `sp_k_ausd_v_ta_period` to raise an error.** Temporarily add `RAISE 'Simulated error in core script';` inside `sp_k_ausd_v_ta_period`'s `BEGIN...END` block, before the final `INSERT` statement.
        *   **Step 2: Re-deploy the modified `sp_k_ausd_v_ta_period`.**
        *   **Step 3: Execute the wrapper:**
            ```sql
            CALL `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period`();
            ```
        *   **Expected "Passing" Result:**
            *   The `CALL` statement should fail and return an error message (e.g., "Simulated error in core script").
            *   Verify entries in `your_gcp_project.your_bq_dataset.job_log`:
                *   Entries for job start and core script start.
                *   An 'E' (Error) entry from the wrapper's `EXCEPTION` block, containing the error message and `SQLSTATE`.
            *   Verify entry in `your_gcp_project.your_bq_dataset.job_status`:
                *   One entry for the `JobKennung` with `status = 'FAILED'` and a recent `updated_at` timestamp.
        *   **Step 4: Revert `sp_k_ausd_v_ta_period` to its original placeholder state and re-deploy.**

3.  **Monitoring:** After successful execution, review BigQuery job history and Cloud Logging for any unexpected errors or warnings.

## 7. Rollback procedure

In case of issues or if the migration needs to be reverted, follow these steps:

1.  **Stop New Scheduling:** Immediately disable or remove any new scheduling configurations (e.g., Cloud Scheduler jobs, Cloud Composer DAGs, Cloud Workflows) that invoke `sp_bert_v_ta_period`.
2.  **Revert to Original Scheduling:** Re-enable or restore the original scheduling mechanism for `r_ausd_v_ta_period.ksh` in its legacy environment. Ensure the original script and its dependencies are fully functional.
3.  **Delete BigQuery Stored Procedures:** Remove the migrated stored procedures from BigQuery:
    ```sql
    DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.sp_bert_v_ta_period`;
    DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.sp_k_ausd_v_ta_period`;
    ```
4.  **Handle BigQuery DDL Tables:**
    *   **Option A (Recommended for clean rollback):** If the `job_log`, `job_status`, and `job_control` tables contain no critical data or if their data is not needed for historical analysis, delete them:
        ```sql
        DROP TABLE IF EXISTS `your_gcp_project.your_bq_dataset.job_log`;
        DROP TABLE IF EXISTS `your_gcp_project.your_bq_dataset.job_status`;
        DROP TABLE IF EXISTS `your_gcp_project.your_bq_dataset.job_control`;
        ```
    *   **Option B (If data needs to be preserved):** If the data in these tables is valuable for post-mortem analysis or auditing, rename them (e.g., `job_log_rolled_back_YYYYMMDD`) or move them to an archive dataset instead of dropping them.
5.  **Verify Original System:** Confirm that the original `r_ausd_v_ta_period.ksh` job is running as expected in its legacy environment.