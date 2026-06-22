# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh`. This script, originally serving as an orchestration wrapper for a contract data reconciliation job (`ta_barrier` table), has been re-implemented to leverage Google Cloud's BigQuery environment. The migration transforms the shell-based orchestration, parameter handling, logging, and error management into BigQuery Stored Procedures and dedicated BigQuery tables.

## 2. Generated artifacts

The migration process generated the following BigQuery assets:

*   **`project/dataset/job_control.sql`**
    *   **Role**: Defines a BigQuery table (`project.dataset.job_control`) used to store metadata and status for each job execution. This replaces the file-based job control and status updates previously managed by the KornShell script.
*   **`project/dataset/job_log.sql`**
    *   **Role**: Defines a BigQuery table (`project.dataset.job_log`) for storing detailed log messages generated during job execution. This replaces the file-based logging (`LogDatei`) and `print`/`tee` operations in the original script.
*   **`project/dataset/job_error_log.sql`**
    *   **Role**: Defines a BigQuery table (`project.dataset.job_error_log`) dedicated to recording error incidents encountered during job execution. This replaces the error reporting mechanisms and `trap` handling of the original script.
*   **`project/dataset/Vertragsdatenabgleich.sql`**
    *   **Role**: Defines a BigQuery Stored Procedure (`project.dataset.Vertragsdatenabgleich`) that serves as the direct replacement for the original `r_ausd_v_ta_barrier.ksh` KornShell script. It encapsulates the orchestration logic, parameter handling, logging, error management, and invokes the core data reconciliation logic (expected to be migrated to `project.dataset.k_ausd_v_ta_barrier`).

## 3. Key design decisions

The migration to BigQuery was guided by the following key design decisions:

*   **BigQuery Stored Procedures for Orchestration**: The KornShell wrapper's primary function was orchestration. BigQuery Stored Procedures provide a native, scalable, and managed environment for procedural logic within BigQuery, making them a natural fit for replacing shell scripts that orchestrate SQL operations. This centralizes data processing and control within the data warehouse.
*   **Table-based Logging and Job Control**: Instead of file-based logging and ad-hoc status updates, dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`) were chosen. This provides structured, queryable, and centralized logging and auditing capabilities, significantly improving observability and troubleshooting compared to parsing flat files.
*   **Translation of Shell Constructs to BigQuery SQL**:
    *   **Parameter Handling**: The `getopts` mechanism was replaced by standard BigQuery Stored Procedure input parameters and `IF` statements for validation.
    *   **Environment Initialization**: Sourcing of `.dw_init` and utility scripts was replaced by `DECLARE` variables for configuration and direct incorporation of logic (e.g., date formatting) using BigQuery SQL functions.
    *   **Error Handling**: Shell `set -eu` and `trap` mechanisms were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and `SIGNAL SQLSTATE` for robust error management and propagation.
    *   **Core Logic Invocation**: The execution of the dependent `k_ausd_v_ta_barrier.ksh` script was translated into a `CALL` statement to its BigQuery Stored Procedure counterpart (`project.dataset.k_ausd_v_ta_barrier`), maintaining the modularity.
*   **Trade-offs**:
    *   **Loss of direct file system interaction**: The ability to write to arbitrary log files or interact with the local file system is lost. This is mitigated by the benefits of structured, queryable logging in BigQuery tables.
    *   **Dependency on BigQuery ecosystem**: The solution is tightly coupled with BigQuery, which is acceptable given the target platform.
    *   **Complexity of utility script migration**: Common shell utility functions (e.g., `DWMSG_*`) require careful re-implementation or inlining in BigQuery SQL, which can be more verbose than shell scripting for simple tasks.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `project.dataset` exists in your Google Cloud project. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
2.  **IAM Permissions**:
    *   The service account or user executing the BigQuery Stored Procedure must have appropriate IAM roles. At a minimum, this includes:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on the `project.dataset` to create/update tables and stored procedures, and to insert/update data into the `job_control`, `job_log`, and `job_error_log` tables.
        *   `BigQuery Job User` to run BigQuery jobs.
3.  **Core Logic Migration (Prerequisite)**:
    *   **Crucially**, the core data reconciliation logic from `k_ausd_v_ta_barrier.ksh` **must be migrated and deployed** as the BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_barrier` before this wrapper procedure can function correctly. This is a hard dependency.
4.  **Deployment of BigQuery Tables**:
    *   Execute the `CREATE TABLE` statements for `job_control`, `job_log`, and `job_error_log` in BigQuery.
    *   `project/dataset/job_control.sql`
    *   `project/dataset/job_log.sql`
    *   `project/dataset/job_error_log.sql`
5.  **Deployment of BigQuery Stored Procedure**:
    *   Execute the `CREATE OR REPLACE PROCEDURE` statement for `project.dataset.Vertragsdatenabgleich` in BigQuery.
    *   `project/dataset/Vertragsdatenabgleich.sql`
6.  **Scheduling (if applicable)**:
    *   If the job requires scheduled execution, configure a Cloud Composer DAG or Cloud Workflows definition to `CALL` the `project.dataset.Vertragsdatenabgleich` stored procedure. Ensure the orchestrator has the necessary BigQuery permissions.

## 5. Known gaps & unresolved references

*   **Core Logic Migration (`k_ausd_v_ta_barrier.ksh`)**: This is the most significant unresolved item. The current design only covers the wrapper script. The actual data reconciliation logic must be migrated and deployed as `project.dataset.k_ausd_v_ta_barrier` for this wrapper to function. Its design, implementation, and validation are outside the scope of this document.
*   **Utility Script Re-implementation**: The original script sourced several utility KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). While basic parameter handling and date formatting are covered, a comprehensive review is needed to ensure all functionalities from these utilities are either:
    *   Re-implemented as BigQuery UDFs or helper stored procedures.
    *   Inlined directly into `Vertragsdatenabgleich` where appropriate.
    *   Deemed unnecessary in the BigQuery context.
*   **Parameter Validation Logic**: The provided pseudocode for `Vertragsdatenabgleich` initializes `ErrNr` to 0 and then immediately checks `IF ErrNr != 0`. This implies that actual parameter validation logic (beyond `-h`) that would set `ErrNr` and `ErrArg` is missing from the pseudocode and needs to be fully implemented based on the original script's `getopts` and validation rules.
*   **Missing Source Metadata**: The absence of `file_complexity` and `automation_rate` data for the source script means the precise effort and automation tier (`B0-B4`) for this migration were not formally assessed.
*   **Secrets Management**: The original script did not explicitly show handling of sensitive information. If any parameters or environment variables contained secrets, a robust secrets management solution (e.g., Google Secret Manager) should be integrated.

## 6. Validation

To validate the migrated BigQuery Stored Procedure:

1.  **Execute the Stored Procedure**:
    *   Call the procedure from the BigQuery console or via the `bq query` command-line tool.
    *   **Successful run example**:
        ```sql
        CALL `project.dataset.Vertragsdatenabgleich`(NULL, NULL, NULL); -- Assuming no specific parameters needed for a basic run
        ```
    *   **Help parameter example**:
        ```sql
        CALL `project.dataset.Vertragsdatenabgleich`('-h', NULL, NULL);
        ```
        This should return the `usage_text` and `LEAVE` the procedure without further execution.
    *   **Error scenario example**: (Once parameter validation is fully implemented, test with invalid parameters to trigger the `SIGNAL SQLSTATE`.)
        ```sql
        -- Example for an invalid parameter (assuming 'p_s' is mandatory and not provided)
        -- This would require adding validation logic to the SP first.
        -- CALL `project.dataset.Vertragsdatenabgleich`(NULL, NULL, NULL);
        ```

2.  **Check BigQuery Tables for "Passing" Criteria**:
    *   **`project.dataset.job_control`**:
        *   A new entry should exist for the executed job (`JobKennung = 'BERT_V_TA_BARRIER'`).
        *   For a successful run, the `status` column should be `'OK'` and `finished_at` should be populated.
        *   For a failed run, the `status` column should be `'ERROR'`.
    *   **`project.dataset.job_log`**:
        *   Entries corresponding to the `DW_EintragsNr` of the executed job should be present.
        *   For a successful run, the final message should be `'Die Abarbeitung wurde ohne erkennbare Fehler beendet'` with `log_level = 'INFO'`.
        *   For a failed run, an `AppError: Abbruch` message with `log_level = 'ERROR'` should be present.
    *   **`project.dataset.job_error_log`**:
        *   For a successful run, no new entries should be present for the executed job.
        *   For a failed run (e.g., due to parameter errors or errors in the called core procedure), an entry with `job_kennung`, `eintrags_nr`, `err_nr`, and `err_arg` should be recorded.
    *   **Core Logic Validation**: The most critical part of "passing" is that the *called* `project.dataset.k_ausd_v_ta_barrier` procedure successfully executes its data reconciliation logic and produces the expected output or state changes in the target data. This requires separate validation specific to the core logic.

## 7. Rollback procedure

In case of issues or a need to revert the migration, follow these steps:

1.  **Disable New Orchestration**:
    *   If scheduled via Cloud Composer or Cloud Workflows, disable or delete the DAG/workflow that calls `project.dataset.Vertragsdatenabgleich`.
2.  **Delete BigQuery Stored Procedure**:
    *   Drop the migrated stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.Vertragsdatenabgleich`;
        ```
3.  **Revert BigQuery Tables (Optional/Conditional)**:
    *   If the `job_control`, `job_log`, or `job_error_log` tables were created solely for this migration and contain no other critical data, they can be dropped:
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_control`;
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        ```
    *   If these tables are shared or contain data that needs to be preserved, simply leave them.
4.  **Re-enable Original Script**:
    *   Re-enable the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh` in its original execution environment.
5.  **Rollback Core Logic (if applicable)**:
    *   If the core logic `project.dataset.k_ausd_v_ta_barrier` was also migrated and deployed, its rollback procedure must be followed, which would typically involve dropping its BigQuery Stored Procedure and reverting to the original `k_ausd_v_ta_barrier.ksh` script.