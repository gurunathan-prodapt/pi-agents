# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh` has been migrated. This script, originally responsible for environment setup, parameter parsing, logging, error handling, and orchestrating a core data reconciliation process, has been re-implemented.

The target platform for this migration is **Google Cloud BigQuery**. The KornShell wrapper has been converted into a **BigQuery Script**, leveraging BigQuery's native scripting capabilities and stored procedures for modularity, logging, and error management. The core processing logic, originally in `k_ausd_v_ta_vvl_dwh.ksh`, is now represented as a placeholder BigQuery Stored Procedure, `k_ausd_v_ta_vvl_dwh_sp`, which is a separate, dependent migration task.

## 2. Generated artifacts

The migration produced the following BigQuery-native artifacts:

*   **`sql/ddl/job_log_ddl.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_log` table in BigQuery. This table serves as the central repository for all job execution logs, replacing the file-based logging of the original KornShell script. It captures job status, messages, timestamps, and other relevant metadata.
*   **`sql/stored_procedures/DWMSG_ErmittleNr_SP.sql`**
    *   **Role:** A BigQuery Stored Procedure that generates or retrieves a unique job entry number (`DW_EintragsNr`). This replaces the logic in the original script that determined a unique identifier for each job run.
*   **`sql/stored_procedures/DWMSG_Logdateiname_SP.sql`**
    *   **Role:** A BigQuery Stored Procedure that generates a logical identifier for grouping log entries related to a single job execution. This conceptually replaces the `LogDatei` variable from the KornShell script, providing a way to associate all log messages with a specific run.
*   **`sql/stored_procedures/DWMSG_ErzeugeEintrag_SP.sql`**
    *   **Role:** A BigQuery Stored Procedure responsible for creating the initial log entry in the `job_log` table when a job starts. It records basic job information and sets the initial status to 'RUNNING'.
*   **`sql/stored_procedures/DWMSG_SetzeStichtagInfo_SP.sql`**
    *   **Role:** A BigQuery Stored Procedure that updates the `job_log` table to record a specific reference date (`StichtagInfo`) for a given job. This is used to store important date context for the job's execution.
*   **`sql/stored_procedures/DWMSG_Fehlerbehandlung_SP.sql`**
    *   **Role:** A BigQuery Stored Procedure that handles error conditions. When an error occurs, it updates the job's status to 'FAILED' in the `job_log` table, records the error message, and can optionally signal a SQLSTATE to propagate the error. This replaces the `DWMSG_MeldeFehler` and `DWMSG_Fehlerbehandlung` functions from the original script.
*   **`sql/stored_procedures/DWMSG_SetzeStatusOK_SP.sql`**
    *   **Role:** A BigQuery Stored Procedure that marks a job as successfully completed in the `job_log` table. It updates the job's status to 'SUCCESS' and logs a completion message.
*   **`sql/stored_procedures/k_ausd_v_ta_vvl_dwh_sp.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure representing the migrated core business logic from the original `k_ausd_v_ta_vvl_dwh.ksh` script. This procedure is invoked by the main orchestration script and will contain the actual data transformation and reconciliation steps once migrated.
*   **`sql/r_ausd_v_ta_vvl_dwh_bq.sql`**
    *   **Role:** The main BigQuery Script that directly replaces the original `r_ausd_v_ta_vvl_dwh.ksh` KornShell script. It handles parameter declaration, job initialization, calls to the `DWMSG_*` utility stored procedures for logging and status management, and orchestrates the execution of the core processing logic (`k_ausd_v_ta_vvl_dwh_sp`). It also implements BigQuery's native error handling (`BEGIN...EXCEPTION`).

## 3. Key design decisions

*   **BigQuery Script for Orchestration:** The KornShell wrapper's orchestration role was directly translated to a BigQuery Script. This choice leverages BigQuery's native capabilities for sequential execution, variable declaration, conditional logic, and error handling within the data warehouse environment. It avoids the overhead and complexity of external orchestration tools for this specific wrapper's scope, providing a serverless and scalable solution.
*   **BigQuery Stored Procedures for Utility Functions (`DWMSG_*`):** The various `DWMSG_*` functions and sourced shell libraries were re-implemented as BigQuery Stored Procedures. This promotes modularity, reusability, and maintainability. Each procedure encapsulates a specific logging or status management task, making the main orchestration script cleaner and easier to understand. It also ensures that logging and status updates are transactional and consistent within BigQuery.
*   **BigQuery Stored Procedure for Core Processing Logic (`k_ausd_v_ta_vvl_dwh_sp`):** The core business logic, though a placeholder in this migration, is designed to be a BigQuery Stored Procedure. This decision aligns with the goal of moving all data processing logic into BigQuery for performance, scalability, and to fully utilize BigQuery's SQL-native capabilities.
*   **Centralized `job_log` Table for Logging:** All logging and status updates are directed to a single, structured `job_log` BigQuery table. This replaces disparate file-based logs, offering a unified, queryable, and auditable record of job executions. This significantly improves monitoring and troubleshooting capabilities.
*   **`BEGIN...EXCEPTION` for Error Handling:** BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` block was used to translate the KornShell's `trap ERR` mechanism. This provides robust, structured error handling within the BigQuery Script, allowing for graceful failure, logging of error details, and setting appropriate job statuses.
*   **Parameter Handling:** Command-line parameters (`-h`, `-s`, `-l`) are translated to `DECLARE` statements with `DEFAULT` values in the BigQuery Script. This allows parameters to be passed during execution (e.g., via `bq query --parameter` or scheduling tools) while maintaining the original script's flexibility.

**Notable Trade-offs:**

*   **Loss of Direct File System Access:** The migration to BigQuery means losing direct access to the file system for logging or temporary files. This is mitigated by using BigQuery tables for logging and temporary data storage.
*   **Dependency on Core Script Migration:** The full functionality of the migrated wrapper is dependent on the successful and accurate migration of the `k_ausd_v_ta_vvl_dwh.ksh` script into `k_ausd_v_ta_vvl_dwh_sp`. This is a significant external dependency.
*   **`FORMAT_BQM_TEXT` Placeholder:** The generated code uses `FORMAT_BQM_TEXT` as a placeholder for BigQuery's `FORMAT` function. This needs to be replaced with `FORMAT` during deployment.

## 4. Manual steps before go-live

Before the migrated BigQuery job can be run, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset `your_project.your_dataset` exists. If not, create it:
        ```bash
        bq mk --dataset --default_table_expiration 3600 --default_partition_expiration 3600 your_project:your_dataset
        ```
    *   **Action:** Replace `your_project` and `your_dataset` with actual project and dataset IDs.

2.  **IAM/Permissions:**
    *   The service account or user executing the BigQuery Script and Stored Procedures must have appropriate IAM roles. At a minimum, this includes:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on `your_project.your_dataset` to create tables, stored procedures, and insert/update data into `job_log`.
        *   `BigQuery Job User` to run BigQuery queries and scripts.
    *   **Action:** Verify and assign necessary IAM roles to the execution identity.

3.  **Deploy DDL and Stored Procedures:**
    *   Execute the `job_log_ddl.sql` to create the logging table:
        ```bash
        bq query --use_legacy_sql=false < sql/ddl/job_log_ddl.sql
        ```
    *   Deploy all `DWMSG_*_SP.sql` and `k_ausd_v_ta_vvl_dwh_sp.sql` stored procedures:
        ```bash
        bq query --use_legacy_sql=false < sql/stored_procedures/DWMSG_ErmittleNr_SP.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/DWMSG_Logdateiname_SP.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/DWMSG_ErzeugeEintrag_SP.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/DWMSG_SetzeStichtagInfo_SP.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/DWMSG_Fehlerbehandlung_SP.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/DWMSG_SetzeStatusOK_SP.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/k_ausd_v_ta_vvl_dwh_sp.sql
        ```
    *   **Action:** Replace `your_project.your_dataset` placeholders within these files with the actual project and dataset IDs before deployment.
    *   **Action:** Replace `FORMAT_BQM_TEXT` with `FORMAT` in all generated SQL files.

4.  **Configuration Management:**
    *   The `BERT_DIR_ROOT` variable in `r_ausd_v_ta_vvl_dwh_bq.sql` is a placeholder. If it represents a configuration value, consider:
        *   Hardcoding the actual dataset name if it's static.
        *   Using a BigQuery configuration table to store such values, and querying it at the start of the script.
    *   **Action:** Define and implement the strategy for `BERT_DIR_ROOT` and any other configuration parameters.

5.  **Scheduling:**
    *   The BigQuery Script needs to be scheduled. Options include:
        *   **Cloud Scheduler:** For simple time-based scheduling, triggering a Cloud Function or Pub/Sub message that executes the BigQuery Script.
        *   **Cloud Composer (Apache Airflow):** For complex workflows, dependency management, and more robust scheduling. An Airflow DAG would be created to execute the BigQuery Script.
    *   **Action:** Set up the chosen scheduling mechanism to invoke `r_ausd_v_ta_vvl_dwh_bq.sql`.

## 5. Known gaps & unresolved references

*   **Core Script Migration (`k_ausd_v_ta_vvl_dwh.ksh`):** The `k_ausd_v_ta_vvl_dwh_sp.sql` is currently a placeholder. The actual business logic from the original KornShell script needs to be fully translated into BigQuery SQL and implemented within this stored procedure. This is the most critical dependency for the end-to-end functionality.
*   **`DWMSG_*` Functions Semantics:** While the `DWMSG_*` stored procedures provide a functional equivalent, their exact behavior (e.g., how `DW_EintragsNr` is truly generated in the legacy system, the full context of `LogDatei` naming beyond the identifier) might require further investigation to ensure 100% fidelity with the original system's logging and status management.
*   **Generic Parameters (`-s`, `-l`):** The parameters `-s` and `-l` are accepted by the BigQuery Script, but their specific usage or impact on the core processing logic (within `k_ausd_v_ta_vvl_dwh_sp`) is not defined in this migration. Their intended purpose needs to be clarified and implemented in the core stored procedure if they influence its behavior.
*   **Sourced KornShell Libraries:** The full content and functionality of sourced libraries like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` were not fully analyzed. Their specific utility functions (beyond basic parameter parsing and date formatting) might need to be replicated as additional BigQuery UDFs or stored procedures if they contain complex, reusable logic.
*   **`BERT_DIR_ROOT` Placeholder:** The `BERT_DIR_ROOT` variable is currently a string placeholder (`'your_project.your_dataset'`). Its actual meaning in the legacy system (e.g., a base directory for other scripts or configuration files) needs to be fully understood and translated into an appropriate BigQuery context (e.g., a dataset prefix for common utility procedures or configuration tables).
*   **`FORMAT_BQM_TEXT`:** This is a placeholder for BigQuery's `FORMAT` function. It must be replaced in all generated SQL files before deployment.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deploy all artifacts:** Ensure all DDL and Stored Procedures (including the placeholder `k_ausd_v_ta_vvl_dwh_sp`) are deployed to `your_project.your_dataset`.
2.  **Execute the main script:** Run the `r_ausd_v_ta_vvl_dwh_bq.sql` script using the `bq query` command-line tool or through your chosen scheduling mechanism (e.g., Cloud Composer).
    *   **Example execution (without optional parameters):**
        ```bash
        bq query --project_id=your_project --dataset_id=your_dataset --use_legacy_sql=false < sql/r_ausd_v_ta_vvl_dwh_bq.sql
        ```
    *   **Example execution (with optional parameters):**
        ```bash
        bq query --project_id=your_project --dataset_id=your_dataset --use_legacy_sql=false \
            --parameter='p_s:STRING:test_s_value' \
            --parameter='p_l:STRING:test_l_value' \
            < sql/r_ausd_v_ta_vvl_dwh_bq.sql
        ```
    *   **Example execution (with help flag):**
        ```bash
        bq query --project_id=your_project --dataset_id=your_dataset --use_legacy_sql=false \
            --parameter='p_h:BOOL:TRUE' \
            < sql/r_ausd_v_ta_vvl_dwh_bq.sql
        ```
3.  **Monitor `job_log` table:** Query the `your_project.your_dataset.job_log` table to observe the entries created by the script.

**What "passing" means:**

*   **Script Completion:** The `bq query` command (or the scheduler job) completes without reporting a BigQuery job error.
*   **Successful Status:** For the executed job, there should be an entry in `your_project.your_dataset.job_log` with `status = 'SUCCESS'`.
*   **Expected Log Messages:** Verify that all expected log messages (start, parameter values, core processing start/end, success message) are present in the `job_log` table for the corresponding `job_nr`.
*   **Error Handling Test:**
    *   To test error handling, temporarily modify `k_ausd_v_ta_vvl_dwh_sp.sql` to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in core logic.';` and re-deploy.
    *   Run the main script again.
    *   "Passing" for error handling means the `job_log` table shows `status = 'FAILED'` and an `ERROR` level log message containing the simulated error. The BigQuery job itself should also report a failure.
*   **Core Logic Validation (once implemented):** Once `k_ausd_v_ta_vvl_dwh_sp` is fully migrated, "passing" will also include verifying that the data transformations and reconciliation steps performed by the stored procedure produce the correct output in the target `ta_vvl_dwh` table.

## 7. Rollback procedure

In case of issues or unexpected behavior with the migrated BigQuery job, the following rollback procedure can be executed:

1.  **Stop BigQuery Job Execution:** Immediately halt any scheduled or ongoing executions of `r_ausd_v_ta_vvl_dwh_bq.sql` in Cloud Scheduler, Cloud Composer, or any other orchestration tool.
2.  **Revert to Original KornShell Script:** Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh` script in its legacy scheduling environment.
3.  **Clean Up BigQuery Artifacts (Optional but Recommended):**
    *   **Drop Stored Procedures:**
        ```bash
        bq rm -f your_project:your_dataset.DWMSG_ErmittleNr_SP
        bq rm -f your_project:your_dataset.DWMSG_Logdateiname_SP
        bq rm -f your_project:your_dataset.DWMSG_ErzeugeEintrag_SP
        bq rm -f your_project:your_dataset.DWMSG_SetzeStichtagInfo_SP
        bq rm -f your_project:your_dataset.DWMSG_Fehlerbehandlung_SP
        bq rm -f your_project:your_dataset.DWMSG_SetzeStatusOK_SP
        bq rm -f your_project:your_dataset.k_ausd_v_ta_vvl_dwh_sp
        ```
    *   **Drop `job_log` Table:**
        ```bash
        bq rm -f your_project:your_dataset.job_log
        ```
        *Note: If historical log data is important, consider archiving or renaming the `job_log` table instead of dropping it.*
4.  **Verify Legacy System Functionality:** Confirm that the original KornShell script is running as expected and producing correct results.

**Important Considerations for Rollback:**

*   **Data Consistency:** Ensure that the BigQuery version of the core logic (`k_ausd_v_ta_vvl_dwh_sp`) has not made any irreversible or partial data changes that would conflict with the legacy system. If it has, a data reconciliation or cleanup step might be necessary before reverting.
*   **Monitoring:** Closely monitor the legacy system after rollback to ensure full operational stability.