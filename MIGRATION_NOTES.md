# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `f_alis_msgerr.ksh`, a utility library for error management and logging within an ETL framework, has been migrated from its on-premises Oracle-based environment to Google Cloud Platform. The migration targets BigQuery, leveraging BigQuery Stored Procedures for procedural logic and BigQuery tables for persistent storage. This utility provides standardized functions for error handling, status management, log entry generation, and log file naming, previously interacting with an Oracle database via `sqlplus` calls to the `BERT_MELDUNG` package.

## 2. Generated artifacts

The migration process has generated the following BigQuery SQL files:

*   **`bq_dataset_ddl.sql`**:
    *   **Role**: Defines and creates the BigQuery dataset (`project_id.dataset_name`) that will house the migrated logging and error management components. This serves as the foundational container for all subsequent BigQuery objects.
*   **`bq_message_table_ddl.sql`**:
    *   **Role**: Defines and creates the `message_table` within the designated BigQuery dataset. This table replaces the original Oracle message table and is used to store log entries, job statuses, error details, and other relevant metadata.
*   **`bq_sp_dwmsg_fehlerbehandlung.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_Fehlerbehandlung`. This procedure encapsulates the logic for handling errors, including calling `DWMSG_MeldeFehler` and `DWMSG_SetzeStatusAbbruch`, replicating the original KornShell function's behavior.
*   **`bq_sp_dwmsg_setzestatusok.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_SetzeStatusOK`. This procedure updates the `status` column in the `message_table` to 'OK' for a given `entry_nr`, mirroring the Oracle `BERT_MELDUNG.SetzeStatusOk` functionality.
*   **`bq_sp_dwmsg_setzestatusabbruch.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_SetzeStatusAbbruch`. This procedure updates the `status` column in the `message_table` to 'ABBRUCH' for a given `entry_nr`, replicating the Oracle `BERT_MELDUNG.SetzeStatusAbbruch` functionality.
*   **`bq_sp_dwmsg_ermittlenr.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_ErmittleNr`. This procedure generates a unique identifier (UUID) for log entries and returns it via an `OUT` parameter, replacing the original method of generating numbers via Oracle and temporary files.
*   **`bq_sp_dwmsg_erzeugeeintrag.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_ErzeugeEintrag`. This procedure inserts a new row into the `message_table` with initial job details, replicating the Oracle `BERT_MELDUNG.Erzeuge_Eintrag` functionality.
*   **`bq_sp_dwmsg_meldefehler.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_MeldeFehler`. This procedure updates error-related columns (`fehler_typ`, `fehler_nr`, `zusatz1`, `zusatz2`) in the `message_table`, replicating the Oracle `BERT_MELDUNG.Fehler` functionality.
*   **`bq_sp_dwmsg_logdateiname.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_Logdateiname`. This procedure constructs a log file name based on job details and current timestamp, returning it via an `OUT` parameter. This replaces the KornShell's string concatenation and `date` command usage.
*   **`bq_sp_dwmsg_setzestichtaginfo.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_SetzeStichtagInfo`. This procedure updates the `zusatzinfos` column in the `message_table` with parsed date information, replicating the Oracle `BERT_MELDUNG.SetzeZusatzInfos` functionality for specific date inputs.
*   **`bq_sp_dwmsg_appendtiminginfos.sql`**:
    *   **Role**: Creates the BigQuery Stored Procedure `DWMSG_AppendTimingInfos`. This procedure appends timing-related information to the `zusatzinfos` column in the `message_table`, replicating the Oracle `BERT_MELDUNG.SetzeZusatzInfos` functionality for appending timestamps.

## 3. Key design decisions

The migration strategy for `f_alis_msgerr.ksh` was guided by the following key design decisions:

*   **BigQuery Stored Procedures for Functional Equivalence**: Each significant KornShell function (`DWMSG_...`) that interacted with the database has been translated into a dedicated BigQuery Stored Procedure. This approach encapsulates the logic directly within BigQuery, leveraging its native scripting capabilities and eliminating the need for external shell execution or `sqlplus` calls.
*   **BigQuery Table as Central Message Store**: The Oracle message table has been replaced by a BigQuery table (`message_table`). This provides a scalable, cloud-native, and cost-effective solution for storing logging and error information, fully integrated with the BigQuery ecosystem.
*   **Elimination of `sqlplus` and Temporary Files**: The original script's reliance on `sqlplus` for database interaction and temporary files (`/tmp`) for data exchange has been removed. BigQuery Stored Procedures interact directly with BigQuery tables, simplifying the architecture, improving performance, and removing inter-process communication overhead.
*   **UUID for Unique Identifiers**: Instead of relying on Oracle sequences or custom number generation for `entry_nr`, the `DWMSG_ErmittleNr` procedure now uses `GENERATE_UUID()`. This provides a simple, globally unique, and BigQuery-native mechanism for generating identifiers.
*   **BigQuery Scripting for Procedural Logic**: Shell scripting constructs like conditional logic (`if`, `test`), variable assignment, and basic error handling are now handled directly within BigQuery Stored Procedures using BigQuery Scripting. This keeps the logic co-located with the data operations and improves maintainability.
*   **Orchestration Layer for `trap ERR` and Job Flow**: The exact behavior of KornShell's `trap ERR` for comprehensive error handling across an entire job is shifted to the calling orchestration layer (e.g., Cloud Composer/Airflow). BigQuery Stored Procedures handle errors within their scope using `RAISE` statements, but the overarching job failure and subsequent `DWMSG_Fehlerbehandlung` invocation will be managed by the orchestrator. This acknowledges that BigQuery SPs are atomic units and complex job flow is best managed by dedicated orchestration tools.
*   **Standard BigQuery Functions for Utilities**: Unix utilities (`date`, `cat`, `tr`, `rm`) are replaced by equivalent BigQuery native functions for string manipulation, timestamp formatting (`FORMAT_TIMESTAMP`, `CURRENT_TIMESTAMP()`), and data handling.

## 4. Manual steps before go-live

Before the migrated components can be used in a production environment, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Execute the `bq_dataset_ddl.sql` script to create the target BigQuery dataset.
    *   **Action**: `bq query --use_legacy_sql=false --file=bq_dataset_ddl.sql` (replace `project_id` and `dataset_name` placeholders).
2.  **BigQuery Message Table Creation**:
    *   Execute the `bq_message_table_ddl.sql` script to create the `message_table` within the newly created dataset.
    *   **Action**: `bq query --use_legacy_sql=false --file=bq_message_table_ddl.sql` (replace `project_id` and `dataset_name` placeholders).
3.  **BigQuery Stored Procedure Deployment**:
    *   Deploy all generated `bq_sp_dwmsg_*.sql` files by executing them in BigQuery.
    *   **Action**: For each `bq_sp_dwmsg_*.sql` file: `bq query --use_legacy_sql=false --file=bq_sp_dwmsg_*.sql` (replace `project_id` and `dataset_name` placeholders).
4.  **IAM Permissions Configuration**:
    *   Ensure the Google Cloud service account(s) that will execute these BigQuery Stored Procedures (e.g., from Cloud Composer, Cloud Workflows, or other BigQuery jobs) have the necessary IAM roles.
    *   **Required Roles**:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on the target dataset for `INSERT`, `UPDATE`, `SELECT` operations on `message_table` and for calling stored procedures.
        *   `BigQuery Job User` for running BigQuery queries and jobs.
5.  **Environment Variable Mapping / Parameterization**:
    *   The original script relied on environment variables like `DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`. These need to be mapped to parameters for the BigQuery Stored Procedures or configured as environment variables/secrets within the orchestration layer (e.g., Airflow Variables, GCP Secret Manager, or passed as BigQuery job parameters).
    *   Specifically, the `logdatei` path in `DWMSG_Logdateiname` needs to be reviewed and potentially updated to reflect Cloud Storage URIs or other cloud-native logging destinations.
6.  **Orchestration Layer Integration**:
    *   The calling jobs (which previously sourced `f_alis_msgerr.ksh`) must be migrated to an orchestration tool (e.g., Cloud Composer/Airflow, Cloud Workflows).
    *   These migrated calling jobs must be updated to invoke the new BigQuery Stored Procedures using appropriate BigQuery operators (e.g., `BigQueryExecuteQueryOperator` in Airflow).

## 5. Known gaps & unresolved references

The following items have been identified as known gaps or require further attention:

*   **Absence of `file_purpose` in analysis**: While the script's utility/logging purpose was clear from manual review, the initial `file_analysis` table lacked a `file_purpose` entry. This highlights a potential risk of incomplete automated analysis, though mitigated by detailed manual review in this case.
*   **Exact `trap ERR` behavior replication**: The KornShell `trap ERR` mechanism provides a global error handler. In BigQuery, error handling is typically localized within `BEGIN...EXCEPTION` blocks or managed by the calling orchestration layer. The current migration shifts the responsibility for comprehensive job-level error trapping and `DWMSG_Fehlerbehandlung` invocation to the orchestrator. This requires careful implementation in each calling job.
*   **`eval` usage replacement**: The original script used `eval` for dynamic variable assignment. This has been replaced by explicit `SET` statements for BigQuery variables or by returning values directly to the calling orchestration layer. This change is a simplification but requires verification of all dynamic assignments.
*   **Environment Variables Handling**: The original script's reliance on `DW_ORAUSER`, `DW_DIR_ROOT`, and `DW_DIR_PROT` needs to be fully addressed. These should be passed as explicit parameters to the BigQuery Stored Procedures or managed as configuration in the orchestration layer.
*   **Oracle `TO_DATE` and Date Format Compatibility**: Oracle's `TO_DATE` function has specific parsing behaviors. While BigQuery's `PARSE_TIMESTAMP` is used, thorough testing is required to ensure all original date formats (`stichtag_fmt`) are correctly interpreted and handled, especially considering potential implicit conversions or locale differences.
*   **Log File Path Convention**: The `DWMSG_Logdateiname` procedure currently generates a path like `/protocol/...`. This path needs to be re-evaluated for Cloud Storage integration. The `logdatei` column in `message_table` should either store a Cloud Storage URI or the actual log content, depending on the chosen logging strategy.
*   **Interfacing with other migrated components**: This utility script is called by other ETL jobs. The successful migration and integration of this component are dependent on the migration strategy and implementation of those calling jobs. Consistent API calls to the new BigQuery Stored Procedures must be ensured.

## 6. Validation

Validation of the migrated `f_alis_msgerr.ksh` functionality involves unit and integration testing:

*   **Unit Tests (BigQuery Stored Procedures)**:
    *   **How to run**: Execute each BigQuery Stored Procedure individually with various input parameters, including valid, invalid, and edge cases (e.g., `NULL` or empty strings for required parameters).
    *   **What "passing" means**:
        *   Procedures execute without BigQuery errors.
        *   `INSERT` and `UPDATE` operations on `message_table` result in the correct data being stored (e.g., `status`, `fehler_typ`, `zusatz1`, `zusatz2`, `created_ts`, `updated_ts`).
        *   `OUT` parameters (e.g., `var_name` from `DWMSG_ErmittleNr` and `DWMSG_Logdateiname`) return the expected values.
        *   `RAISE` statements are triggered correctly for invalid inputs, preventing erroneous operations.
*   **Integration Tests (Simulated Calling Job)**:
    *   **How to run**: Create a simple BigQuery Script or an Airflow DAG that simulates a calling job. This script/DAG should invoke multiple `DWMSG_` procedures in a sequence that mimics a typical ETL job flow (e.g., `DWMSG_ErmittleNr` -> `DWMSG_ErzeugeEintrag` -> `DWMSG_SetzeStatusOK` or `DWMSG_Fehlerbehandlung`). Include scenarios for both successful execution and simulated failures.
    *   **What "passing" means**:
        *   The simulated job completes successfully or fails gracefully as expected.
        *   The `message_table` accurately reflects the entire lifecycle of the simulated job, including initial entry, status updates (OK/ABBRUCH), and error details if a failure occurred.
        *   All parameters are passed correctly between the calling context and the BigQuery Stored Procedures.
        *   Timestamps (`created_ts`, `updated_ts`) are correctly recorded.

## 7. Rollback procedure

In the event that the migrated `f_alis_msgerr.ksh` components exhibit critical issues after go-live, the following rollback procedure can be followed:

1.  **Stop New Invocations**: Immediately halt any new executions of jobs that rely on the migrated BigQuery Stored Procedures. This typically involves pausing or disabling the relevant DAGs/workflows in the orchestration layer (e.g., Cloud Composer).
2.  **Revert Calling Jobs**: Revert the calling jobs (DAGs/workflows) in the orchestration layer to their previous versions that utilize the original KornShell script `f_alis_msgerr.ksh` and interact with the Oracle database.
3.  **Verify Original System Functionality**: Confirm that the original KornShell-based logging and error management system is fully operational and processing jobs as expected.
4.  **Optional: BigQuery Cleanup**: If necessary for a clean slate or to resolve resource conflicts, the deployed BigQuery Stored Procedures and the `message_table` can be dropped.
    *   **Action**: `DROP PROCEDURE IF EXISTS project_id.dataset_name.DWMSG_Fehlerbehandlung;` (repeat for all SPs)
    *   **Action**: `DROP TABLE IF EXISTS project_id.dataset_name.message_table;`
    *   **Action**: `DROP SCHEMA IF EXISTS project_id.dataset_name;` (only if the dataset is no longer needed and empty)
5.  **Root Cause Analysis**: Investigate the issues that necessitated the rollback, address them in the migration design and implementation, and plan for a re-migration.