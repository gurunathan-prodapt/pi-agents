# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `k_ausd_v_ta_apn_ve.ksh` from its legacy environment to Google Cloud's BigQuery platform.

The original script served as an orchestration layer, handling parameter parsing, validation, job control (registration, deactivation), error logging, and the execution of a core SQL script (`d_ausd_v_ta_apn_ve.sql`). It also captured processed record counts.

The migration re-implements this orchestration logic using BigQuery Stored Procedures. The core SQL logic from `d_ausd_v_ta_apn_ve.sql` is expected to be translated into a separate BigQuery Stored Procedure. Logging and job control mechanisms are replaced by dedicated BigQuery tables.

**What was migrated:** The orchestration logic, parameter handling, error reporting, and record count capture from `k_ausd_v_ta_apn_ve.ksh`.
**To which target platform:** Google BigQuery, utilizing BigQuery Stored Procedures and tables for logging.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL scripts:

1.  **`project.dataset.error_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `error_log` table in BigQuery. This table is used to capture and store detailed error messages and context whenever an error occurs during the execution of the migrated BigQuery stored procedures, replacing the shell-based `DWMSG_MeldeFehler` and `echo` statements.

2.  **`project.dataset.job_log.sql`**
    *   **Role:** Defines the DDL for the `job_log` table in BigQuery. This table records the status and metadata of each job execution, including `job_kennung`, `eintrags_nr`, target table, and final status ('ENDE'). It replaces the implicit job registration and status tracking of the original KornShell script.

3.  **`project.dataset.record_count_log.sql`**
    *   **Role:** Defines the DDL for the `record_count_log` table in BigQuery. This table stores the number of records processed by each job, identified by `job_kennung` and `eintrags_nr`. It replaces the mechanism of writing record counts to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp`) and reading it back.

4.  **`project.dataset.d_ausd_v_ta_apn_ve_sp.sql`**
    *   **Role:** Defines a BigQuery Stored Procedure that serves as a placeholder for the core data transformation logic originally contained in `d_ausd_v_ta_apn_ve.sql`. This procedure is designed to accept `p_EintragsNr` and `p_JobKennung` as inputs. **Note:** The actual SQL content for data manipulation needs to be populated here after a separate analysis and conversion of the original `d_ausd_v_ta_apn_ve.sql` script.

5.  **`project.dataset.control_r_ausd_vertrag.sql`**
    *   **Role:** Defines the main BigQuery Stored Procedure that orchestrates the entire process. This procedure is the direct equivalent of the original `k_ausd_v_ta_apn_ve.ksh` script. It handles:
        *   Parameter validation for `p_JobKennung` and `p_EintragsNr`.
        *   Error handling and logging to `project.dataset.error_log`.
        *   Calling the `project.dataset.d_ausd_v_ta_apn_ve_sp` to execute the core data transformation.
        *   Logging job completion status to `project.dataset.job_log`.
        *   Calculating and logging the number of processed records to `project.dataset.record_count_log`.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **BigQuery Stored Procedures for Orchestration:** The entire orchestration logic, previously managed by KornShell, is re-implemented as a BigQuery Stored Procedure (`control_r_ausd_vertrag`). This centralizes the job execution within the BigQuery environment, leveraging its native capabilities for parameter handling, control flow, and error management, eliminating the need for external shell scripts.
*   **Dedicated BigQuery Tables for Logging:** Instead of relying on shell-based logging, temporary files, and implicit job tables, dedicated BigQuery tables (`error_log`, `job_log`, `record_count_log`) are introduced. This provides a structured, queryable, and scalable solution for auditing, monitoring, and debugging job executions directly within BigQuery.
*   **BigQuery-Native Parameter and Error Handling:** The `getopts` and `pruefeParameterGesetzt` functions from the KornShell script are replaced by BigQuery Stored Procedure input parameters and `IF` statements for validation. Shell-based error reporting (`DWMSG_MeldeFehler`) is replaced by BigQuery's `RAISE USING MESSAGE` and structured logging to the `error_log` table, providing clearer error propagation and traceability.
*   **Direct Record Count Retrieval:** The original method of writing record counts to a temporary file and reading it back is replaced by a direct `SELECT COUNT(*)` query against the target BigQuery table (`project.dataset.ta_apn_ve`). This is more efficient and BigQuery-native.
*   **Modularization of Core SQL Logic:** The core data transformation logic from `d_ausd_v_ta_apn_ve.sql` is encapsulated in its own BigQuery Stored Procedure (`d_ausd_v_ta_apn_ve_sp`). This promotes modularity, reusability, and easier maintenance, allowing the orchestration layer to focus solely on control flow.
*   **Trade-offs:**
    *   **Dependency on `d_ausd_v_ta_apn_ve.sql` conversion:** The success of this migration heavily depends on the accurate and complete conversion of the underlying `d_ausd_v_ta_apn_ve.sql` logic to BigQuery SQL. This was not part of the automated generation and remains a manual effort.
    *   **Loss of direct file system interaction:** Operations like sourcing environment variables (`. $HOME/.dw_init`) and temporary file handling are fundamentally different in a serverless BigQuery environment. These have been re-architected into BigQuery-native constructs (e.g., dataset/table names, `DECLARE` variables).
    *   **Complexity of `starteSQLSkript`:** The full complexity of the original `starteSQLSkript` (e.g., job activation/deactivation, retry logic) was not fully detailed. The current migration provides a basic call to the core SQL procedure. If `starteSQLSkript` had more sophisticated logic, further refinement of `control_r_ausd_vertrag` might be needed.

## 4. Manual steps before go-live

Before the migrated BigQuery stored procedures can be deployed and run, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`
        OPTIONS(
            location="YOUR_REGION",
            description="Dataset for migrated ISBERT processes."
        );
        ```

2.  **IAM Permissions:**
    *   The service account or user executing these BigQuery stored procedures must have the following IAM roles:
        *   `BigQuery Data Editor` or `BigQuery Data Owner` on `project.dataset` to create/update tables and stored procedures, and insert/update data into logging tables and `ta_apn_ve`.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
        *   If external orchestration (e.g., Cloud Composer/Airflow) is used, ensure the associated service account has these permissions.

3.  **Connection Strings / Secrets:**
    *   BigQuery stored procedures do not use traditional connection strings. All interactions are within BigQuery.
    *   If the original `d_ausd_v_ta_apn_ve.sql` connected to other external databases, those connections would need to be re-evaluated for BigQuery (e.g., using federated queries, external tables, or data transfer services). This migration assumes all data is within BigQuery.

4.  **Core SQL Logic Implementation (`d_ausd_v_ta_apn_ve_sp`):**
    *   The `project.dataset.d_ausd_v_ta_apn_ve_sp.sql` file is currently a placeholder. The actual SQL logic from the original `d_ausd_v_ta_apn_ve.sql` must be manually translated and implemented within this stored procedure. This includes:
        *   Converting Oracle (or other source RDBMS) SQL syntax to BigQuery SQL.
        *   Ensuring all source and target tables referenced in the original SQL exist in BigQuery with compatible schemas.
        *   Validating data types and functions.

5.  **Target Table `ta_apn_ve` Creation:**
    *   The target table `project.dataset.ta_apn_ve` (or whatever the actual target table name is) must be created in BigQuery with the correct schema, matching the expectations of the `d_ausd_v_ta_apn_ve_sp` procedure.

6.  **Scheduling:**
    *   The original KornShell script was likely triggered by a scheduler. The migrated `control_r_ausd_vertrag` BigQuery stored procedure will need to be scheduled using a suitable BigQuery-compatible orchestrator, such as:
        *   **Cloud Composer (Airflow):** Create a DAG to call the BigQuery stored procedure.
        *   **Cloud Workflows:** Define a workflow to execute the stored procedure.
        *   **Cloud Scheduler + Cloud Functions:** A Cloud Scheduler job can trigger a Cloud Function, which in turn calls the BigQuery stored procedure.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up or represent areas where further information/design is required:

*   **SQL Script `d_ausd_v_ta_apn_ve.sql` Content:** The most significant gap. The actual data transformation logic from this file was not available for analysis. Its migration (syntax conversion from source RDBMS, likely Oracle, to BigQuery SQL) is critical and represents the core business logic transformation. This requires a separate, dedicated analysis and conversion effort. The `d_ausd_v_ta_apn_ve_sp` is a placeholder.
*   **Exact Logic of `starteSQLSkript`:** While a basic call to the core SQL procedure is implemented, the full logic of the original `starteSQLSkript` shell function (e.g., how it handles active jobs, job registration, deactivation, complex error handling around SQL*Plus execution, retry mechanisms) is not fully known. If it contained sophisticated logic beyond simple execution, the `control_r_ausd_vertrag` procedure might need further refinement.
*   **`DW_DIR_UTL` and `BERT_DIR_ROOT`:** These environment variables defined paths in the legacy system. Their values and how they were used (e.g., for temporary files, script locations) have been re-architected into BigQuery dataset/table names or `DECLARE` variables. Any other implicit dependencies on these paths need to be identified and addressed.
*   **`r_ausd_vertrag.ksh` Context:** The original script is described as a "Kontrollscript zu r_ausd_vertrag.ksh". The larger context, dependencies, and scheduling of `r_ausd_vertrag.ksh` are not detailed. Migrating `r_ausd_vertrag.ksh` might introduce further dependencies or orchestration requirements that could impact this migration.
*   **`ta_apn_ve` Table Schema:** The schema for the target table `project.dataset.ta_apn_ve` is assumed but not explicitly defined. Its schema must be correctly established in BigQuery to match the operations performed by `d_ausd_v_ta_apn_ve_sp`. The `eintrags_nr` column used for `COUNT(*)` is an assumption based on the parameter name.

## 6. Validation

Validation ensures the migrated process functions correctly and produces the expected output.

**How to run the tests:**

1.  **Deploy DDLs:** Execute the `project.dataset.error_log.sql`, `project.dataset.job_log.sql`, and `project.dataset.record_count_log.sql` files to create the logging tables in your BigQuery dataset.
2.  **Deploy Core SQL SP (Placeholder):** Deploy the `project.dataset.d_ausd_v_ta_apn_ve_sp.sql` (after implementing the actual SQL logic).
3.  **Deploy Orchestration SP:** Deploy the `project.dataset.control_r_ausd_vertrag.sql` file.
4.  **Prepare Test Data:** Ensure that any source tables required by `d_ausd_v_ta_apn_ve_sp` contain appropriate test data. Also, ensure the target table `project.dataset.ta_apn_ve` exists and is in a known state.
5.  **Execute the Main Stored Procedure:** Call the `control_r_ausd_vertrag` stored procedure with example parameters from the BigQuery console or a client tool:
    ```sql
    CALL `project.dataset.control_r_ausd_vertrag`('TEST_JOB_001', 'ENTRY_001');
    ```
    Test with various valid and invalid parameter combinations (e.g., `NULL` or empty `p_JobKennung`, `p_EintragsNr`) to verify error handling.

**What "passing" means:**

*   **Successful Execution:** The `CALL` statement for `control_r_ausd_vertrag` completes without raising an unhandled exception.
*   **Job Log Entry:** A new record exists in `project.dataset.job_log` for the executed job, with `job_kennung = 'TEST_JOB_001'`, `eintrags_nr = 'ENTRY_001'`, and `status = 'ENDE'`.
*   **Record Count Log Entry:** A new record exists in `project.dataset.record_count_log` for the executed job, with `job_kennung = 'TEST_JOB_001'`, `eintrags_nr = 'ENTRY_001'`, and `record_count` reflecting the actual number of records processed/affected by `d_ausd_v_ta_apn_ve_sp` for `ENTRY_001`.
*   **No Error Log Entries (for successful runs):** For successful executions, there should be no corresponding entries in `project.dataset.error_log`.
*   **Correct Error Log Entries (for failed runs):** For executions with invalid parameters or other simulated failures, appropriate error messages should be logged in `project.dataset.error_log`, and the `CALL` statement should `RAISE` an error.
*   **Data Integrity:** The data in the target table `project.dataset.ta_apn_ve` (and any other tables modified by `d_ausd_v_ta_apn_ve_sp`) should be as expected, reflecting the correct application of the business logic from the original `d_ausd_v_ta_apn_ve.sql`. This requires a detailed understanding of the original SQL's behavior.

## 7. Rollback procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Stop New Executions:** Immediately halt any scheduled or manual executions of the `project.dataset.control_r_ausd_vertrag` BigQuery stored procedure.
2.  **Delete BigQuery Stored Procedures:**
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.control_r_ausd_vertrag`;
    DROP PROCEDURE IF EXISTS `project.dataset.d_ausd_v_ta_apn_ve_sp`;
    ```
3.  **Delete BigQuery Logging Tables (Optional, but recommended for clean rollback):**
    *   If the logging tables are not used by other processes, delete them. If they contain valuable audit data, consider renaming or archiving them instead of deleting.
    ```sql
    DROP TABLE IF EXISTS `project.dataset.error_log`;
    DROP TABLE IF EXISTS `project.dataset.job_log`;
    DROP TABLE IF EXISTS `project.dataset.record_count_log`;
    ```
4.  **Revert Data Changes (Conditional):**
    *   The `d_ausd_v_ta_apn_ve_sp` procedure modifies data in `project.dataset.ta_apn_ve`. Depending on the nature of these modifications (e.g., `INSERT`, `UPDATE`, `DELETE`), a specific data rollback strategy might be necessary. This could involve:
        *   Restoring `ta_apn_ve` from a snapshot or backup taken before the migration.
        *   Running inverse SQL statements to undo the changes.
        *   This step is highly dependent on the actual logic of `d_ausd_v_ta_apn_ve.sql` and the data retention/recovery policies.
5.  **Reactivate Original Process:** Re-enable the original `k_ausd_v_ta_apn_ve.ksh` script in its legacy environment and resume its scheduling.
6.  **Monitor:** Closely monitor the re-activated legacy process to ensure it functions as expected after the rollback.