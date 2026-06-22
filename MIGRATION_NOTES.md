```markdown
# MIGRATION_NOTES for r_ausd_v_ta_discount_rr.ksh

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_discount_rr.ksh` from its legacy environment to Google Cloud Platform (GCP), specifically utilizing BigQuery for data warehousing and orchestration.

The original script acted as a wrapper, orchestrating the execution of a core data reconciliation process for the `ta_discount_rr` table. Its functions included environment setup, parameter parsing, logging, error handling, and invoking a core reconciliation script (`k_ausd_v_ta_discount_rr.ksh`).

The migrated solution transforms this orchestration logic into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich_ta_discount_rr`). Logging and job control, previously file-based, are now managed by dedicated BigQuery tables (`job_log`, `job_control`). The core reconciliation logic, originally in `k_ausd_v_ta_discount_rr.ksh`, is represented by a placeholder BigQuery Stored Procedure (`sp_k_ausd_v_ta_discount_rr`), which requires separate, detailed migration.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`bigquery/ddl/job_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_log` table in BigQuery. This table replaces the file-based logging mechanism of the original KornShell script, storing detailed log messages, job start/end events, and error information for all migrated jobs.
*   **`bigquery/ddl/job_control.sql`**
    *   **Role:** Defines the DDL for the `job_control` table in BigQuery. This table replaces the job status tracking functionality, maintaining overall status and metadata (e.g., `Stichtag`) for each job execution.
*   **`bigquery/procedures/sp_k_ausd_v_ta_discount_rr_stub.sql`**
    *   **Role:** Provides a stub (placeholder) BigQuery Stored Procedure for the core reconciliation logic originally found in `k_ausd_v_ta_discount_rr.ksh`. This procedure accepts job identification parameters and logs its invocation. It serves as an integration point for the wrapper procedure and must be fully implemented based on the detailed analysis of the original core script.
*   **`bigquery/procedures/sp_vertragsdatenabgleich_ta_discount_rr.sql`**
    *   **Role:** Implements the main wrapper/orchestration logic of the original `r_ausd_v_ta_discount_rr.ksh` script as a BigQuery Stored Procedure. It handles parameter parsing, initializes logging, calls the core reconciliation stub, and manages error handling and status updates using the `job_log` and `job_control` tables.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration:** The wrapper script's logic was migrated to a BigQuery Stored Procedure (`sp_vertragsdatenabgleich_ta_discount_rr`). This centralizes the orchestration within the data warehouse, leveraging BigQuery's native capabilities for SQL-based logic, parameter handling, and error management. This approach minimizes external dependencies for the orchestration layer itself.
*   **Dedicated BigQuery Tables for Logging and Control:** Instead of file-based logging, two BigQuery tables (`job_log`, `job_control`) were introduced. This provides a centralized, queryable, and scalable logging solution, enabling easier monitoring, auditing, and analysis of job executions directly within BigQuery.
*   **`BEGIN...EXCEPTION WHEN ERROR THEN ... END` for Error Handling:** BigQuery's native error handling blocks replace the KornShell `trap` mechanism. This provides robust error capture and allows for structured logging of failures within the `job_log` and `job_control` tables, ensuring job status is accurately reflected.
*   **Stub for Core Logic:** The core reconciliation script (`k_ausd_v_ta_discount_rr.ksh`) was identified as a separate, complex migration. A stub BigQuery Stored Procedure (`sp_k_ausd_v_ta_discount_rr_stub`) was created to allow the wrapper to be migrated and tested independently, deferring the detailed analysis and migration of the core business logic.
*   **Parameterization over Environment Variables:** Shell script environment variables and sourced utility scripts are replaced by explicit input parameters to the BigQuery Stored Procedure or by direct SQL constructs. This enhances clarity, testability, and reduces reliance on external environment configurations.

**Notable Trade-offs:**
*   **Loss of `trap` granularity:** BigQuery's `EXCEPTION` blocks handle SQL errors effectively, but the fine-grained signal trapping (e.g., `INT` for user interruption) of shell scripts is not directly replicable. External orchestration (e.g., Cloud Composer) would need to manage job cancellation and retries.
*   **Dependency on Core Script Migration:** The full functionality of the migrated wrapper is contingent on the complete and correct migration of the core `k_ausd_v_ta_discount_rr.ksh` script. This introduces a critical dependency and potential for delays.

## 4. Manual steps before go-live

Before the migrated solution can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` as used in the generated code) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
2.  **IAM Permissions:**
    *   Grant appropriate IAM roles to the service account or user that will execute the BigQuery stored procedures. This typically includes:
        *   `BigQuery Data Editor` (for `job_log`, `job_control` tables and any tables modified by `sp_k_ausd_v_ta_discount_rr`)
        *   `BigQuery Job User` (to run queries and stored procedures)
        *   `BigQuery Data Viewer` (for reading data from source tables)
3.  **Connection Strings / Configuration:**
    *   If an external orchestrator (e.g., Cloud Composer, Cloud Functions) is used to invoke `sp_vertragsdatenabgleich_ta_discount_rr`, ensure it has the necessary BigQuery connection details and authentication configured.
4.  **Secrets Management:**
    *   No explicit secrets are identified in the wrapper script. If the core script (`k_ausd_v_ta_discount_rr.ksh`) or any of its dependencies involve sensitive credentials, these must be securely managed (e.g., using Google Secret Manager) and passed to the BigQuery procedures or the external orchestrator.
5.  **Scheduling:**
    *   Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer DAG, or a custom cron job on a Compute Engine instance) to trigger the execution of `sp_vertragsdatenabgleich_ta_discount_rr` at the desired frequency and time. The scheduler should pass any required parameters (e.g., `p_s`, `p_l`).
6.  **Core Table Migration:**
    *   Ensure the `ta_discount_rr` table (and any other tables interacted with by `k_ausd_v_ta_discount_rr.ksh`) has been migrated to BigQuery (e.g., `project.dataset.ta_discount_rr`) and is populated with the necessary data.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up and represent potential risks or incomplete aspects of the migration:

*   **Core Script Migration (`k_ausd_v_ta_discount_rr.ksh`):** The most significant gap. The generated `sp_k_ausd_v_ta_discount_rr_stub.sql` is a placeholder. The actual logic of `k_ausd_v_ta_discount_rr.ksh` must be thoroughly analyzed and migrated. This could involve:
    *   Converting SQL-heavy logic into a full BigQuery Stored Procedure.
    *   If it contains complex non-SQL logic, file operations, or external system interactions, it might require migration to a Python-based Cloud Function or Cloud Run service, introducing inter-service communication and orchestration considerations. This is a **B4 (Redesign) item**.
*   **Framework Function Reimplementation:** The exact logic of original KornShell utility functions like `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` needs to be fully understood from their source `.ksh` files. While the generated code provides a reasonable simulation, a precise re-implementation might require further refinement to match all original behaviors (e.g., specific log message formats, error codes).
*   **Parameter `s` and `l` Usage:** The original `r_ausd_v_ta_discount_rr.ksh` script parses parameters `-s` and `-l` but their specific usage within the script itself or the invoked `k_ausd_v_ta_discount_rr.ksh` is not fully detailed. It's assumed they are passed to the core script. Their exact purpose and validation rules need to be confirmed to ensure correct handling in the BigQuery procedures.
*   **Signal Trapping Equivalence:** The `trap` mechanism in KornShell for signals like `INT` (interrupt) cannot be perfectly replicated in BigQuery SQL. While `BEGIN...EXCEPTION` handles SQL errors, external job interruptions would need to be managed by the external orchestrator (e.g., Cloud Composer's task retry/failure handling).
*   **Environment Variable Resolution:** Dynamic path resolution (e.g., `${BERT_DIR_ROOT}`) has been replaced by hardcoded dataset/procedure names or parameters. If these paths were dynamic in the original system, this change might need review.

## 6. Validation

To validate the migrated solution, follow these steps:

1.  **Deploy Generated Artifacts:**
    *   Execute `bigquery/ddl/job_log.sql` to create the `job_log` table.
    *   Execute `bigquery/ddl/job_control.sql` to create the `job_control` table.
    *   Execute `bigquery/procedures/sp_k_ausd_v_ta_discount_rr_stub.sql` to create the stub core procedure.
    *   Execute `bigquery/procedures/sp_vertragsdatenabgleich_ta_discount_rr.sql` to create the wrapper procedure.
    *   *(Ensure these are deployed to the correct `project.dataset`)*

2.  **Run the Stored Procedure:**
    *   Invoke the main wrapper procedure from the BigQuery console or via the `bq` command-line tool:
        ```sql
        CALL `project.dataset.sp_vertragsdatenabgleich_ta_discount_rr`(FALSE, 'param_s_value', 'param_l_value');
        ```
        *   Replace `project.dataset` with your actual project and dataset.
        *   Replace `'param_s_value'` and `'param_l_value'` with appropriate test values.
        *   To test the help message: `CALL `project.dataset.sp_vertragsdatenabgleich_ta_discount_rr`(TRUE, NULL, NULL);`
        *   To test parameter error: `CALL `project.dataset.sp_vertragsdatenabgleich_ta_discount_rr`(FALSE, NULL, 'param_l_value');`

3.  **Check Logging Tables:**
    *   **Passing Criteria:**
        *   **Successful Execution:**
            *   Query `project.dataset.job_control` for the latest entry for `BERT_V_TA_DISCOUNT_RR`. The `status` column should be 'OK'.
            *   Query `project.dataset.job_log` for entries associated with the `job_entry_nr` from the `job_control` table. You should see:
                *   An 'I' (Info) level message for "Job start: Vertragsdatenabgleich".
                *   An 'I' level message indicating the core script stub was called.
                *   An 'I' level message "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
        *   **Parameter Error Execution:**
            *   Query `project.dataset.job_control` for the latest entry. The `status` column should be 'ERROR'.
            *   Query `project.dataset.job_log`. You should see an 'E' (Error) level message like "Parameterfehler: s" (or 'l').
        *   **Core Script Stub Error (simulated):** To test the error handling for the core script, you would temporarily modify `sp_k_ausd_v_ta_discount_rr_stub` to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error from core script';`. Then, run the wrapper and check `job_control` for 'ERROR' status and `job_log` for the error message.

4.  **Data Validation (after core script migration):** Once `sp_k_ausd_v_ta_discount_rr` is fully implemented, a critical validation step will be to compare the output or state of `ta_discount_rr` (or related tables) in BigQuery with the legacy system after running both. This ensures data integrity and functional equivalence.

## 7. Rollback procedure

In case of issues or a decision to revert the migration, follow these steps:

1.  **Stop New Executions:**
    *   Disable or remove any scheduled jobs (e.g., Cloud Scheduler, Cloud Composer DAGs) that invoke `sp_vertragsdatenabgleich_ta_discount_rr`.
2.  **Revert to Legacy Script:**
    *   Ensure the original `r_ausd_v_ta_discount_rr.ksh` script and its dependencies are fully operational in the legacy environment.
    *   Re-enable any legacy scheduling mechanisms for `r_ausd_v_ta_discount_rr.ksh`.
3.  **Drop BigQuery Objects (Optional, but recommended for clean rollback):**
    *   **Drop the wrapper stored procedure:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_vertragsdatenabgleich_ta_discount_rr`;
        ```
    *   **Drop the core script stub stored procedure:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_v_ta_discount_rr`;
        ```
    *   **Drop logging and control tables:**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.job_control`;
        ```
    *   **Note:** If the `ta_discount_rr` table was migrated and modified, its rollback would depend on its specific migration strategy (e.g., restoring from backup, reverting changes). This is outside the scope of this wrapper script's rollback.
4.  **Verify Legacy System:**
    *   Confirm that the original `r_ausd_v_ta_discount_rr.ksh` script is running as expected and producing correct results in the legacy environment.

This rollback procedure ensures a clean reversion to the previous state, minimizing disruption.
```