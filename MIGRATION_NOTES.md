# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `r_ausd_v_ta_vertrag_tmp.ksh` from a legacy Unix/KornShell environment to Google Cloud Platform (GCP). The script, originally responsible for orchestrating a contract data reconciliation job, has been re-implemented as a BigQuery Stored Procedure.

The migration targets:
*   **Source**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh` (KornShell)
*   **Target Platform**: Google BigQuery
*   **Target Components**: BigQuery Stored Procedures for orchestration and core logic, and a BigQuery table for centralized audit logging.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`ddl/job_audit_log.sql`**
    *   **Role**: This SQL Data Definition Language (DDL) script creates the `job_audit_log` table in BigQuery. This table serves as the centralized logging mechanism, replacing the filesystem-based logging of the original KornShell script. It captures job execution details, status, messages, and errors for all migrated jobs, including `sp_vertragsdatenabgleich` and its sub-procedures.
*   **`procedures/sp_k_ausd_v_ta_vertrag_tmp.sql`**
    *   **Role**: This BigQuery Stored Procedure is a placeholder for the core data transformation logic originally contained within `k_ausd_v_ta_vertrag_tmp.ksh`. It is designed to be called by the wrapper procedure `sp_vertragsdatenabgleich`. Currently, it only logs its start and successful completion, with error handling in place for future implementation of the actual business logic.
*   **`procedures/sp_vertragsdatenabgleich.sql`**
    *   **Role**: This BigQuery Stored Procedure is the direct replacement for the `r_ausd_v_ta_vertrag_tmp.ksh` wrapper script. It handles environment setup, command-line parameter parsing (now as BigQuery procedure parameters), logging to `job_audit_log`, and orchestrates the execution of the core processing logic by calling `sp_k_ausd_v_ta_vertrag_tmp`. It also implements robust error handling using BigQuery's `BEGIN...EXCEPTION...END` blocks.

## 3. Key design decisions

The following key design decisions were made during this migration:

*   **Orchestration as BigQuery Stored Procedure**: The KornShell wrapper script's orchestration logic (parameter handling, logging, invoking core logic) was directly translated into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This keeps the orchestration close to the data processing, leveraging BigQuery's native capabilities.
*   **Centralized Audit Logging**: Instead of disparate filesystem logs, a dedicated BigQuery table (`job_audit_log`) was created. This provides a structured, queryable, and centralized repository for all job execution metadata, status, and error messages, significantly improving observability and troubleshooting.
*   **BigQuery Stored Procedure for Core Logic**: The core processing script (`k_ausd_v_ta_vertrag_tmp.ksh`) is planned to be migrated into its own BigQuery Stored Procedure (`sp_k_ausd_v_ta_vertrag_tmp`). This modular approach promotes reusability and clear separation of concerns.
*   **Native BigQuery Error Handling**: Shell `trap` mechanisms were replaced with BigQuery's `BEGIN...EXCEPTION...END` blocks and `SIGNAL SQLSTATE` for robust error management within the stored procedures.
*   **Parameter Mapping**: KornShell `getopts` parameters were mapped to BigQuery Stored Procedure input parameters, providing clear interfaces and type safety. Unused parameters (`-s`, `-l`) from the original script were noted but not explicitly migrated, maintaining functional equivalence.
*   **Unique Job Run Identifiers**: `GENERATE_UUID()` is used to create a unique `job_run_id` for each execution, facilitating tracking and correlation of log entries.
*   **Date Function Translation**: Shell date commands (e.g., `date +%d%m%Y`) were replaced with BigQuery's `FORMAT_DATE()` and `CURRENT_DATE()` functions.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (e.g., `your_gcp_project.your_dataset_name`) exists. If not, create it using the GCP Console or `bq mk` command.
2.  **IAM Permissions**:
    *   The service account or user executing these procedures must have appropriate BigQuery IAM roles:
        *   `BigQuery Data Editor` (or equivalent) on the target dataset for `INSERT` operations into `job_audit_log` and any tables modified by `sp_k_ausd_v_ta_vertrag_tmp`.
        *   `BigQuery Job User` for running BigQuery jobs and stored procedures.
        *   `BigQuery Data Viewer` (or equivalent) on any source tables read by `sp_k_ausd_v_ta_vertrag_tmp`.
3.  **Replace Placeholders**:
    *   In all generated SQL files (`ddl/job_audit_log.sql`, `procedures/sp_k_ausd_v_ta_vertrag_tmp.sql`, `procedures/sp_vertragsdatenabgleich.sql`), replace `project.dataset` with your actual GCP project ID and BigQuery dataset name.
4.  **Deploy DDL**:
    *   Execute `ddl/job_audit_log.sql` to create the `job_audit_log` table.
    *   `bq query --use_legacy_sql=false < ddl/job_audit_log.sql`
5.  **Deploy Stored Procedures**:
    *   Execute `procedures/sp_k_ausd_v_ta_vertrag_tmp.sql` to create the core placeholder procedure.
    *   `bq query --use_legacy_sql=false < procedures/sp_k_ausd_v_ta_vertrag_tmp.sql`
    *   Execute `procedures/sp_vertragsdatenabgleich.sql` to create the wrapper procedure.
    *   `bq query --use_legacy_sql=false < procedures/sp_vertragsdatenabgleich.sql`
6.  **Scheduling (if applicable)**:
    *   If external orchestration (e.g., Cloud Composer/Airflow, Cloud Workflows) is required, configure the DAG or workflow to call `sp_vertragsdatenabgleich` with the necessary parameters.
    *   If direct execution is sufficient, document the `CALL` statement for manual or scheduled execution.

## 5. Known gaps & unresolved references

*   **Core Script Migration Pending**: The most significant gap is that `procedures/sp_k_ausd_v_ta_vertrag_tmp.sql` is currently a placeholder. The actual data transformation logic from `k_ausd_v_ta_vertrag_tmp.ksh` still needs to be fully migrated and implemented within this procedure. This will be a separate, subsequent migration task.
*   **Dynamic Invocation**: The original script's dynamic invocation of `k_ausd_v_ta_vertrag_tmp.ksh` via a variable (`Name_Kernskript`) was a challenge for static analysis. The migration addresses this by explicitly calling the target BigQuery Stored Procedure, but it highlights a potential blind spot in automated lineage analysis for similar legacy scripts.
*   **Shell-Specific Feature Mapping**: Direct equivalents for shell features like `trap` (for signal handling) or `tee` (for simultaneous console and file output) do not exist in BigQuery SQL. These have been addressed by mapping to BigQuery's `BEGIN...EXCEPTION...END` for error handling and the `job_audit_log` table for centralized logging, which is a functional replacement but not a direct syntactic one.
*   **Unused Parameters**: The original KornShell script declared `-s` and `-l` as valid `getopts` parameters but did not use them. This behavior is preserved in the migration by not including them in the BigQuery Stored Procedure's signature, acknowledging their original unused status.

## 6. Validation

To validate the successful migration and functionality of the `sp_vertragsdatenabgleich` procedure:

1.  **Execute the Stored Procedure**:
    *   Open the BigQuery console or use the `bq query` command.
    *   Execute the wrapper procedure:
        ```sql
        CALL `your_gcp_project.your_dataset_name.sp_vertragsdatenabgleich`(
            'BERT_V_TA_VERTRAG_TMP', -- p_job_name_param
            CURRENT_DATE()           -- p_stichtag_param (or a specific date like '2023-10-26')
        );
        ```
    *   Test with different `p_stichtag_param` values.
    *   Test without `p_stichtag_param` to ensure `CURRENT_DATE()` default works.
2.  **Verify `job_audit_log` entries**:
    *   Query the `job_audit_log` table immediately after execution:
        ```sql
        SELECT *
        FROM `your_gcp_project.your_dataset_name.job_audit_log`
        WHERE job_name = 'BERT_V_TA_VERTRAG_TMP'
        ORDER BY start_time DESC
        LIMIT 5;
        ```
    *   **Passing Criteria**:
        *   The `CALL` statement completes successfully without raising an error.
        *   The `job_audit_log` table contains at least two entries for the latest `job_run_id` of `BERT_V_TA_VERTRAG_TMP`:
            *   One entry with `status = 'RUNNING'` and `message = 'Wrapper procedure sp_vertragsdatenabgleich started.'`.
            *   One entry with `status = 'SUCCESS'` and `message = 'Wrapper procedure sp_vertragsdatenabgleich completed successfully. Core script finished.'`.
            *   The `start_time`, `end_time`, and `stichtag` columns should reflect the execution time and passed parameters accurately.
        *   The `job_audit_log` should also contain entries from the *placeholder* core procedure `sp_k_ausd_v_ta_vertrag_tmp` indicating its start and successful completion.
        *   (Once `sp_k_ausd_v_ta_vertrag_tmp` is fully implemented): Verify that the expected data transformations or outputs from the core logic are correctly produced.
3.  **Error Handling Validation**:
    *   To test error handling, temporarily modify `sp_k_ausd_v_ta_vertrag_tmp` to `RAISE` an error (e.g., `RAISE USING MESSAGE = 'Simulated error in core procedure';`).
    *   Re-run `sp_vertragsdatenabgleich`.
    *   **Passing Criteria**:
        *   The `CALL` statement should fail and raise the error message.
        *   The `job_audit_log` should contain entries for `sp_vertragsdatenabgleich` with `status = 'FAILED'` and `error_message` reflecting the simulated error.

## 7. Rollback procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Stop New Executions**: Immediately halt any scheduled or automated executions of `sp_vertragsdatenabgleich` (e.g., disable the Cloud Composer DAG or Cloud Workflow).
2.  **Delete BigQuery Stored Procedures**:
    *   Drop the migrated procedures from BigQuery:
        ```sql
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_dataset_name.sp_vertragsdatenabgleich`;
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_dataset_name.sp_k_ausd_v_ta_vertrag_tmp`;
        ```
3.  **Revert Scheduling**:
    *   If the job was scheduled via Cloud Composer or Cloud Workflows, revert to the previous scheduling mechanism (e.g., re-enable the original cron job or legacy scheduler).
4.  **Verify Original Script Functionality**:
    *   Ensure the original KornShell script `r_ausd_v_ta_vertrag_tmp.ksh` is still present, executable, and functioning correctly in the legacy environment.
5.  **Audit Log Table (Optional)**:
    *   The `job_audit_log` table can be retained for historical logging purposes. If it must be removed, ensure no other migrated jobs are relying on it.
    *   To delete: `DROP TABLE IF EXISTS `your_gcp_project.your_dataset_name.job_audit_log`;`