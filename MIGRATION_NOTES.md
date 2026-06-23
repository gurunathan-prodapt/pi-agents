# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_apn_ve.ksh` from its legacy environment to Google BigQuery.

The original script served as an orchestration layer, handling parameter validation, job status management, and invoking a core SQL script (`d_ausd_v_ta_apn_ve.sql`) for data processing. The migration re-implements this control flow and data interaction using BigQuery-native constructs.

The primary target platform is **Google BigQuery**, leveraging its Stored Procedures for orchestration and data processing, and BigQuery tables for logging and data storage.

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **`project/dataset/ddl/ta_apn_ve.sql`**
    *   **Role**: This DDL script defines the schema for the `ta_apn_ve` table in BigQuery. This table is the primary data target referenced by the original script and the migrated procedures. It serves as the destination for the data processed by `d_ausd_v_ta_apn_ve.sql` (once migrated).
*   **`project/dataset/ddl/job_error_log.sql`**
    *   **Role**: This DDL script creates a dedicated BigQuery table (`job_error_log`) to capture and store error details from job executions. It replaces the legacy shell script's error reporting mechanisms (`DWMSG_MeldeFehler`, `echo "FEHLER..."`).
*   **`project/dataset/ddl/job_run_log.sql`**
    *   **Role**: This DDL script creates a BigQuery table (`job_run_log`) to log successful job execution metadata, including job identifiers, processed record counts, and timestamps. It replaces the legacy script's job registration and completion reporting.
*   **`project/dataset/procedures/d_ausd_v_ta_apn_ve.sql`**
    *   **Role**: This is a placeholder BigQuery Stored Procedure. It is intended to house the translated business logic from the original `d_ausd_v_ta_apn_ve.sql` script. This procedure will perform the actual data transformations, manage job states (active/deactivate), and update the `ta_apn_ve` table. **Note: This procedure currently contains placeholder logic and requires full implementation.**
*   **`project/dataset/procedures/r_ausd_vertrag_control.sql`**
    *   **Role**: This BigQuery Stored Procedure is the direct migration of the `k_ausd_v_ta_apn_ve.ksh` script. It handles parameter validation, error logging, invokes the `d_ausd_v_ta_apn_ve` procedure, and logs job run details, including processed record counts. It serves as the main entry point for the migrated job.

## 3. Key design decisions

The migration strategy focused on leveraging BigQuery's native capabilities to re-implement the KornShell script's functionality.

*   **BigQuery Stored Procedures for Orchestration and Data Processing**:
    *   **Why**: BigQuery Stored Procedures offer a robust, scalable, and BigQuery-native way to encapsulate complex SQL logic, control flow (IF/ELSE, loops), and parameter handling. This eliminates the need for external shell scripts to orchestrate BigQuery operations, reducing cross-platform dependencies and simplifying deployment.
    *   **Trade-offs**: While powerful, BigQuery Stored Procedures have limitations compared to a full-fledged programming language (e.g., KornShell). Complex file system operations or interactions with diverse external systems are not directly supported, necessitating a shift to BigQuery-native equivalents (e.g., logging tables instead of log files).
*   **Parameter Handling via Stored Procedure Parameters**:
    *   **Why**: The original script used command-line arguments (`-j`, `-f`) parsed by `getopts`. This was directly translated to `IN` parameters for the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`), providing a clear and type-safe interface.
    *   **Trade-offs**: This approach is BigQuery-specific. If the procedure needs to be called from an external orchestrator (e.g., Cloud Composer), the orchestrator must be configured to pass these parameters correctly.
*   **Dedicated BigQuery Tables for Logging**:
    *   **Why**: Instead of writing to flat files or relying on shell-based error reporting (`DWMSG_MeldeFehler`), dedicated BigQuery tables (`job_error_log`, `job_run_log`) were created. This centralizes logging, makes it queryable, and integrates seamlessly with BigQuery's ecosystem for monitoring and analysis.
    *   **Trade-offs**: Requires DDL creation and `INSERT` statements within the procedures, adding a small overhead compared to simple `echo` commands.
*   **Direct `SELECT COUNT(*)` for Record Counts**:
    *   **Why**: The original script used a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp`) to store and retrieve the processed record count. In BigQuery, this is replaced by a direct `SELECT COUNT(*)` query on the target table (`project.dataset.ta_apn_ve`) after the data processing, storing the result in a `DECLARE`d variable. This is more efficient and BigQuery-native.
    *   **Trade-offs**: Assumes the `eintrags_nr` column in `ta_apn_ve` is sufficient to identify records processed by a specific run. If the original temporary file contained a more complex count, this might need adjustment.
*   **Error Handling with `SIGNAL SQLSTATE` and `EXCEPTION WHEN ERROR`**:
    *   **Why**: BigQuery's scripting language provides robust error handling mechanisms. `SIGNAL SQLSTATE` is used to explicitly raise errors and terminate procedure execution, while `EXCEPTION WHEN ERROR` blocks catch unexpected errors during execution, allowing for centralized error logging before re-raising. This replaces the shell's `exit $ErrNr` and provides more structured error reporting.
    *   **Trade-offs**: Requires careful mapping of legacy error codes/messages to BigQuery's error handling paradigm.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
2.  **Table and Procedure Deployment**:
    *   Execute the DDL scripts to create the necessary tables:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/ddl/ta_apn_ve.sql
        bq query --use_legacy_sql=false < project/dataset/ddl/job_error_log.sql
        bq query --use_legacy_sql=false < project/dataset/ddl/job_run_log.sql
        ```
    *   Deploy the BigQuery Stored Procedures:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/procedures/d_ausd_v_ta_apn_ve.sql
        bq query --use_legacy_sql=false < project/dataset/procedures/r_ausd_vertrag_control.sql
        ```
3.  **IAM/Permissions**:
    *   The service account or user identity that will execute the `r_ausd_vertrag_control` stored procedure must have appropriate BigQuery IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` to `INSERT` into log tables and `ta_apn_ve`, and to `CALL` procedures.
        *   `BigQuery Data Viewer` on `project.dataset` to `SELECT` from `ta_apn_ve`.
        *   `BigQuery Job User` to run BigQuery jobs.
4.  **Connection Strings/Configuration**:
    *   No explicit connection strings are needed within BigQuery Stored Procedures. However, if an external orchestrator (e.g., Cloud Composer) is used to call `r_ausd_vertrag_control`, ensure it has the necessary BigQuery connection configured.
5.  **Secrets Management**:
    *   The original script did not explicitly mention secrets. If `d_ausd_v_ta_apn_ve.sql` (once implemented) requires any sensitive credentials, these must be managed securely (e.g., using Google Secret Manager) and passed as parameters or accessed via secure BigQuery functions if applicable.
6.  **Scheduling**:
    *   If the job was previously scheduled (e.g., via cron), the new BigQuery Stored Procedure needs to be integrated into a new scheduling mechanism. This could involve:
        *   **Cloud Composer (Apache Airflow)**: Create a DAG to call `project.dataset.r_ausd_vertrag_control` at the desired frequency.
        *   **Cloud Workflows**: Define a workflow to execute the procedure.
        *   **Cloud Scheduler + Cloud Functions/Run**: A simpler setup for direct calls.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up and represent areas that require further attention or are currently placeholders:

*   **Core SQL Script (`d_ausd_v_ta_apn_ve.sql`) Implementation**: The `project/dataset/procedures/d_ausd_v_ta_apn_ve.sql` procedure is currently a placeholder. The complete business logic from the original `d_ausd_v_ta_apn_ve.sql` script, including data transformations, job table updates, and deactivation of older jobs, **must be fully translated and implemented** within this BigQuery Stored Procedure. This is the most critical unresolved item.
*   **`ta_apn_ve` Table Schema**: The DDL for `project.dataset.ddl/ta_apn_ve.sql` is generic. The full schema (all columns and their types) for `ta_apn_ve` needs to be derived from the original `d_ausd_v_ta_apn_ve.sql` script and applied to the DDL.
*   **Full Functionality of Sourced Utility Scripts**: The original KornShell script sourced several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While common functionalities like parameter parsing and error logging have been addressed, a thorough review of these scripts is needed to ensure *all* their functionalities (e.g., specific date formats, complex SQLPlus interactions, environment variable setups) are correctly replicated or replaced by BigQuery equivalents.
*   **`starteSQLSkript` Function Logic**: The `starteSQLSkript` function (from sourced utilities) is responsible for active job checks, SQL execution, job table updates, and deactivating old jobs. The migration assumes this logic will be fully incorporated into the `project.dataset.d_ausd_v_ta_apn_ve` procedure. The exact details of this function's implementation need to be understood and translated.
*   **`DWMSG_MeldeFehler` Specifics**: The `DWMSG_MeldeFehler` function's exact behavior (e.g., error codes, message formatting, additional logging destinations) needs to be fully understood to ensure the `job_error_log` table captures equivalent information.
*   **Parameter Validation Logic (`ErrNr` handling)**: In `r_ausd_vertrag_control.sql`, the parameter validation sets `ErrNr = 0` on error, then checks `IF ErrNr = 0 AND (p_JobKennung IS NULL OR p_JobKennung = '' OR p_EintragsNr IS NULL OR p_EintragsNr = '') THEN`. This logic, while a direct translation of the provided pseudocode, might be counter-intuitive if `ErrNr` is typically expected to be non-zero for an error. It should be reviewed to ensure it correctly captures and signals parameter validation failures as intended by the original script.

## 6. Validation

Validation ensures the migrated job functions correctly and produces expected outputs.

**How to Run Tests:**

1.  **Prerequisites**: Ensure all DDLs and procedures from Section 2 have been deployed to your BigQuery project and dataset.
2.  **Test Data**: Populate the `ta_apn_ve` table with sample data, and potentially other tables that `d_ausd_v_ta_apn_ve` might interact with (once implemented).
3.  **Execute the Main Procedure**: Call the `r_ausd_vertrag_control` stored procedure from the BigQuery console or via a client library.

    *   **Successful Run (Valid Parameters)**:
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', 'ENTRY_123');
        ```
    *   **Failed Run (Missing Parameters)**:
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`(NULL, 'ENTRY_123');
        -- or
        CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', '');
        ```
    *   **Failed Run (Simulated Internal Error)**: This would require modifying the `d_ausd_v_ta_apn_ve` placeholder to explicitly raise an error, or testing with data that causes an error in the fully implemented `d_ausd_v_ta_apn_ve`.

**What "Passing" Means:**

*   **Successful Execution**:
    *   The `CALL` statement completes without BigQuery-level errors.
    *   A `SELECT` statement after the call should show a completion message, e.g., `Job completed successfully. Processed X records for EintragsNr: Y`.
    *   A new record is inserted into `project.dataset.job_run_log` with the correct `job_kennung`, `eintrags_nr`, `tab_name`, `records` (matching the count from `ta_apn_ve`), and `run_ts`.
    *   The `ta_apn_ve` table is updated as expected by the (fully implemented) `d_ausd_v_ta_apn_ve` procedure.
*   **Error Handling (Parameter Validation)**:
    *   When called with invalid or missing parameters, the procedure should terminate with an error message like `Bitte ueber Rahmenscript aufrufen`.
    *   A new record is inserted into `project.dataset.job_error_log` with the relevant `job_kennung`, `eintrags_nr`, `error_nr` (0 in the current implementation), `error_arg`, and `error_ts`.
*   **Error Handling (Internal SQL Execution Failure)**:
    *   If an error occurs within `d_ausd_v_ta_apn_ve`, the `r_ausd_vertrag_control` procedure should catch it and terminate with an error message like `SQL execution failed`.
    *   A new record is inserted into `project.dataset.job_error_log` with `error_nr = 1` and `error_arg = 'SQL execution failed'`.

## 7. Rollback procedure

In case of critical issues or if the migrated job does not perform as expected, the following steps outline the rollback procedure to revert to the original KornShell script execution:

1.  **Halt New Deployments**: Stop any automated deployment pipelines for the BigQuery artifacts.
2.  **Disable BigQuery Scheduling**: If the BigQuery job was integrated into an external orchestrator (e.g., Cloud Composer, Cloud Workflows), disable or delete the corresponding DAG/workflow.
3.  **Re-enable Legacy Scheduling**: Re-activate the original scheduling mechanism (e.g., cron job) for `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh`.
4.  **Verify Legacy Job**: Monitor the re-enabled legacy job to ensure it is running correctly and processing data as expected.
5.  **Optional: Clean Up BigQuery Artifacts**: Once the legacy system is stable, the BigQuery artifacts can be removed.
    *   Drop the BigQuery Stored Procedures:
        ```bash
        bq rm -f -r project.dataset.r_ausd_vertrag_control
        bq rm -f -r project.dataset.d_ausd_v_ta_apn_ve
        ```
    *   Drop the BigQuery tables (use caution, as this deletes data):
        ```bash
        bq rm -f project.dataset.ta_apn_ve
        bq rm -f project.dataset.job_error_log
        bq rm -f project.dataset.job_run_log
        ```
    *   If the dataset was created specifically for this migration, it can also be removed:
        ```bash
        bq rm -f -r project.dataset
        ```