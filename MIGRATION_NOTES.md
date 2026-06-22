# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `f_alis_msgerr.ksh`, originally a utility library for error management and logging within the Information Services platform, has been migrated. This script previously standardized error handling by interacting with an Oracle database via `sqlplus` to manage a message table.

The migration re-implements this functionality to the Google Cloud Platform (GCP). The target architecture leverages **BigQuery** for data persistence, replacing the Oracle database, and **BigQuery Stored Procedures** for procedural logic, replacing the KornShell functions and Oracle PL/SQL interactions. An external orchestration layer (e.g., Cloud Composer/Apache Airflow, Cloud Workflows) is intended to invoke these BigQuery procedures, taking over the role of the original shell script as the primary executor.

## 2. Generated artifacts

The migration process generated the following BigQuery DDL and Stored Procedure scripts:

*   **`ddl/message_table.sql`**
    *   **Role:** BigQuery DDL script to create the `dw_is_error_management.message_table`. This table replaces the original Oracle message table (`BERT_MELDUNG`) and is designed to store job status, error details, and additional information for job executions.
*   **`ddl/message_log.sql`**
    *   **Role:** BigQuery DDL script to create the `dw_is_error_management.message_log` table. This table is intended to store detailed error log entries, providing a granular history of issues.
*   **`ddl/message_sequence.sql`**
    *   **Role:** BigQuery DDL script to create and initialize the `dw_is_error_management.message_sequence` table. This table simulates the functionality of an Oracle sequence, providing unique `eintragsnr` values for job entries.
*   **`stored_procedures/f_alis_msgerr_ErmittleNr.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_ErmittleNr` function. It is responsible for generating and returning a unique entry number (`EintragsNr`) for a new job execution using the `message_sequence` table.
*   **`stored_procedures/f_alis_msgerr_ErzeugeEintrag.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_ErzeugeEintrag` function. It creates a new entry in the `message_table` with initial job metadata and sets its status to 'LAEUFT'.
*   **`stored_procedures/f_alis_msgerr_SetzeStatusOK.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_SetzeStatusOK` function. It updates the status of a specified job entry in the `message_table` to 'OK'.
*   **`stored_procedures/f_alis_msgerr_SetzeStatusAbbruch.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_SetzeStatusAbbruch` function. It updates the status of a specified job entry in the `message_table` to 'ABBRUCH'.
*   **`stored_procedures/f_alis_msgerr_MeldeFehler.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_MeldeFehler` function. It logs specific error details to the `message_log` table and updates the last error information in the `message_table` for a given job entry.
*   **`stored_procedures/f_alis_msgerr_Fehlerbehandlung.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_Fehlerbehandlung` function. This is the central error handling routine, which calls `DWMSG_MeldeFehler` to log the error and then `DWMSG_SetzeStatusAbbruch` to mark the job as aborted. It expects error details (SQLCODE, SQLERRM) to be passed from the calling context.
*   **`stored_procedures/f_alis_msgerr_Logdateiname.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_Logdateiname` function. It generates a log file name string based on job identifier, timestamp, and entry number, incorporating a configurable base path.
*   **`stored_procedures/f_alis_msgerr_SetzeStichtagInfo.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_SetzeStichtagInfo` function. It parses a given date string and updates the `zusatzinfos_date` column in the `message_table`.
*   **`stored_procedures/f_alis_msgerr_AppendTimingInfos.sql`**
    *   **Role:** BigQuery Stored Procedure that re-implements the `DWMSG_AppendTimingInfos` function. It appends timing-related information to the `zusatzinfos_text` column in the `message_table`.

## 3. Key design decisions

The following key design decisions guided the migration of `f_alis_msgerr.ksh` to BigQuery:

*   **BigQuery as the Central Data Store**: The Oracle database, previously used for the message table, has been replaced by BigQuery. This decision leverages BigQuery's strengths in scalability, cost-effectiveness, and integration within the GCP ecosystem for data warehousing and analytics.
*   **Re-implementation as BigQuery Stored Procedures**: Each distinct function (`DWMSG_...`) within the original KornShell script has been translated into a dedicated BigQuery Stored Procedure. This approach keeps the procedural logic close to the data, allowing for efficient DML operations and minimizing external dependencies for core data manipulation.
*   **Simulated Sequence Generation**: Since BigQuery does not have native `SEQUENCE` objects like Oracle, a dedicated `message_sequence` table is used. This table is updated and queried to generate unique `eintragsnr` values, effectively simulating the Oracle sequence functionality.
*   **External Orchestration Layer**: The original KornShell script acted as both a utility library and an orchestrator. In the migrated architecture, the orchestration responsibility is explicitly delegated to an external tool (e.g., Cloud Composer/Airflow, Cloud Workflows). This separation of concerns allows BigQuery to focus on data operations while the orchestration layer manages workflow, parameter passing, and top-level error handling.
*   **Parameterization of External Paths**: Hardcoded paths like `DW_DIR_PROT` for log files have been replaced with configurable parameters (e.g., `p_LogBasePath` in `DWMSG_Logdateiname`). This enhances flexibility and decouples the BigQuery procedures from specific filesystem structures, aligning with cloud-native practices (e.g., using Cloud Storage buckets for logs).
*   **Adaptation of Error Handling**: The KornShell `trap ERR` mechanism, a system-level feature, has been adapted. BigQuery Stored Procedures utilize `EXCEPTION WHEN ERROR` blocks for internal error handling. For the central `DWMSG_Fehlerbehandlung` procedure, it is designed to receive `SQLCODE` and `SQLERRM` from the calling orchestration layer's error handling context, requiring careful integration.
*   **Data Type Mapping**: Oracle/KornShell data types (e.g., `NUMBER`, `VARCHAR2`) have been mapped to their appropriate BigQuery equivalents (`INT64`, `STRING`, `TIMESTAMP`, `DATE`), ensuring data integrity and compatibility within the new environment.

## 4. Manual steps before go-live

Before the migrated solution can go live, the following manual steps must be performed:

1.  **BigQuery Dataset Creation**:
    *   Create the BigQuery dataset `dw_is_error_management` in your target GCP project. This dataset will host all the migrated tables and stored procedures.
    *   Command example: `bq mk --dataset --default_table_expiration 365 dw_is_error_management`

2.  **BigQuery Table and Sequence Deployment (DDL)**:
    *   Execute the DDL scripts in the following order to create the necessary tables and initialize the sequence:
        *   `ddl/message_table.sql`
        *   `ddl/message_log.sql`
        *   `ddl/message_sequence.sql`
    *   Verify that the `message_sequence` table has been initialized with an entry for `eintragsnr_seq` and `next_val = 1`.

3.  **BigQuery Stored Procedure Deployment**:
    *   Execute each `CREATE OR REPLACE PROCEDURE` script located in the `stored_procedures/` directory. This will deploy all the `DWMSG_` procedures into the `dw_is_error_management` dataset.

4.  **IAM/Permissions Configuration**:
    *   **For Deployment**: The user or service account deploying the DDL and stored procedures must have `BigQuery Data Editor` (or more granular permissions like `bigquery.tables.create`, `bigquery.routines.create`, `bigquery.tables.updateData`) on the target project/dataset.
    *   **For Execution**: The service account used by the orchestration layer (e.g., Cloud Composer, Cloud Workflows) to invoke these procedures must be granted:
        *   `BigQuery Job User` role to run BigQuery jobs.
        *   `BigQuery Data Editor` (or granular `bigquery.tables.updateData`, `bigquery.tables.insertData`, `bigquery.tables.getData`) on the `dw_is_error_management` dataset to allow the procedures to modify table data.

5.  **Orchestration Layer Development and Deployment**:
    *   Develop the orchestration workflow (e.g., Airflow DAGs, Cloud Workflows) that will call the BigQuery Stored Procedures. This layer is responsible for:
        *   Defining the sequence of calls to the `DWMSG_` procedures.
        *   Passing required parameters (e.g., `p_JobKennung`, `p_Programmname`, `p_LogBasePath`).
        *   Implementing top-level error handling to catch exceptions and pass `SQLCODE`/`SQLERRM` to `DWMSG_Fehlerbehandlung`.
    *   Deploy this orchestration layer to your chosen GCP service (e.g., Cloud Composer environment, Cloud Workflows).

6.  **Configuration of External Parameters**:
    *   Configure the `p_LogBasePath` parameter within your orchestration layer. This parameter specifies the base path for log files (e.g., `gs://your-project-logs-bucket/is_error_logs/`).
    *   If any other environment-specific parameters were used in the original script, ensure they are properly configured in the new orchestration environment.

## 5. Known gaps & unresolved references

The following items have been identified as known gaps or require further follow-up:

*   **Missing Metadata**: The original `file_complexity` and `automation_rate` metadata for `f_alis_msgerr.ksh` was unavailable. This implies that the complexity assessment and migration effort might require additional manual validation.
*   **Oracle PL/SQL Logic Translation**: The most significant gap is the lack of direct access to the source code for the Oracle PL/SQL package `BERT_MELDUNG`. The BigQuery Stored Procedures were developed based on the *assumed* functionality and parameters derived from the KornShell script's calls. A thorough analysis of the `BERT_MELDUNG` package's internal logic, table structures, and any specific business rules or custom error codes is **crucial** to ensure complete functional parity and accurate replication in BigQuery.
*   **Error Handling Granularity**: The KornShell `trap ERR` mechanism is a system-level feature. While BigQuery Scripting offers `EXCEPTION WHEN ERROR` blocks, the exact behavior and any custom error codes from the original Oracle PL/SQL need careful mapping. The `DWMSG_Fehlerbehandlung` procedure relies on the calling orchestration to provide `SQLCODE` and `SQLERRM`, which requires robust error handling implementation in the orchestration layer.
*   **Dynamic SQL/Parameter Quoting**: The original `DWMSG_MeldeFehler` function dynamically constructed `sqlplus` commands, including specific quoting for `Zusatz1` and `Zusatz2`. While BigQuery Stored Procedures handle parameters more directly, it's important to verify that any special characters in string parameters are handled correctly to prevent SQL injection or unexpected behavior.
*   **External Notifications (Robomon)**: The design document mentions "Robomonbenachrichtigung" as a potential action in `DWMSG_Fehlerbehandlung`. If this refers to an active external notification system, it needs to be identified and integrated with a GCP equivalent (e.g., Cloud Monitoring alerts, Pub/Sub triggered Cloud Functions/Workflows) to maintain existing alerting capabilities.
*   **`log_id` Generation in `DWMSG_MeldeFehler`**: The generated `DWMSG_MeldeFehler` procedure uses `GENERATE_UUID()` as a placeholder for `log_id`. If `log_id` is intended to be an `INT64` and sequential, a separate sequence mechanism (similar to `eintragsnr`) or a different approach for generating unique `INT64` IDs will be required.

## 6. Validation

Validation of the migrated components involves both unit testing of individual BigQuery Stored Procedures and integration testing with the orchestration layer.

### Unit Tests (BigQuery Stored Procedures)

1.  **Test Setup**:
    *   Ensure the `dw_is_error_management` dataset and all DDL components (`message_table`, `message_log`, `message_sequence`) are deployed and initialized.
    *   Ensure all `DWMSG_` stored procedures are deployed.
2.  **Execution**:
    *   For each `DWMSG_` procedure, write and execute BigQuery SQL scripts that call the procedure with a variety of test cases:
        *   **Valid Inputs**: Call with expected parameters and verify the resulting state of `message_table`, `message_log`, and `message_sequence`.
        *   **Invalid/NULL Inputs**: Test scenarios where required parameters are `NULL` or malformed (e.g., invalid date format for `DWMSG_SetzeStichtagInfo`). Verify that the procedure raises a `SIGNAL SQLSTATE` error with an appropriate message.
        *   **Edge Cases**: Test `DWMSG_AppendTimingInfos` with an empty `zusatzinfos_text` field, or `DWMSG_SetzeStatusOK` with a non-existent `EintragsNr`.
3.  **Verification**:
    *   After each procedure call, query the affected BigQuery tables (`dw_is_error_management.message_table`, `dw_is_error_management.message_log`, `dw_is_error_management.message_sequence`) to confirm that data has been inserted, updated, or deleted as expected.
    *   Check the `status`, `last_error_type`, `zusatzinfos_date`, `zusatzinfos_text` fields in `message_table` and the content of `message_log`.
    *   For `DWMSG_ErmittleNr`, verify that `next_val` in `message_sequence` increments correctly.
    *   For procedures that return values (e.g., `DWMSG_Logdateiname`), verify the output string.
4.  **Passing Criteria**: All unit tests execute without unhandled BigQuery errors, and the state of the BigQuery tables accurately reflects the expected outcomes for all valid and invalid test cases.

### Integration Tests (Orchestration Layer)

1.  **Test Setup**:
    *   Deploy a test orchestration workflow (e.g., a simple Airflow DAG or Cloud Workflow) that mimics a typical job execution flow.
    *   This workflow should include calls to multiple `DWMSG_` procedures in a realistic sequence.
2.  **Execution**:
    *   Run the test orchestration workflow.
    *   Include scenarios for both successful job completion and simulated job failures (e.g., by intentionally causing an error in a step or calling `DWMSG_Fehlerbehandlung` directly).
3.  **Verification**:
    *   Monitor the execution logs of the orchestration tool for any errors or unexpected behavior.
    *   Query the `dw_is_error_management.message_table` and `dw_is_error_management.message_log` tables to ensure:
        *   New job entries are created with the correct initial status.
        *   Status updates (`OK`, `ABBRUCH`) are correctly applied.
        *   Error details are logged in `message_log` and updated in `message_table` during failure scenarios.
        *   `zusatzinfos_date` and `zusatzinfos_text` are updated as expected.
    *   Verify that the `eintragsnr` values are unique and incrementing.
4.  **Passing Criteria**: The orchestration workflow completes successfully for both success and failure paths, and all BigQuery table entries accurately reflect the simulated job states and logged information, demonstrating end-to-end functionality.

## 7. Rollback procedure

In the event that the migrated solution needs to be rolled back, follow these steps:

1.  **Halt New Deployments**: Immediately stop any new deployments or executions of the migrated BigQuery-based error management system.

2.  **Revert Orchestration Layer**:
    *   Undeploy or disable the new orchestration workflows (e.g., Airflow DAGs, Cloud Workflows) that invoke the BigQuery Stored Procedures.
    *   Re-enable or redeploy the original calling processes that relied on the `f_alis_msgerr.ksh` script.

3.  **Re-enable Original System**:
    *   Ensure the original KornShell script `f_alis_msgerr.ksh` and its dependencies (Oracle database, `sqlplus`, `BERT_MELDUNG` package) are fully operational and accessible to the original calling processes.
    *   Verify that the original system can resume error handling and logging without issues.

4.  **Clean Up BigQuery Assets (Optional but Recommended)**:
    *   **Delete BigQuery Stored Procedures**:
        ```sql
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_ErmittleNr;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_ErzeugeEintrag;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_SetzeStatusOK;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_SetzeStatusAbbruch;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_MeldeFehler;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_Fehlerbehandlung;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_Logdateiname;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_SetzeStichtagInfo;
        DROP PROCEDURE IF EXISTS dw_is_error_management.DWMSG_AppendTimingInfos;
        ```
    *   **Delete BigQuery Tables**:
        ```sql
        DROP TABLE IF EXISTS dw_is_error_management.message_table;
        DROP TABLE IF EXISTS dw_is_error_management.message_log;
        DROP TABLE IF EXISTS dw_is_error_management.message_sequence;
        ```
    *   **Delete BigQuery Dataset**:
        ```bash
        bq rm -r -f dw_is_error_management
        ```
        (Use with caution, `-r` deletes all tables and routines within the dataset, `-f` forces deletion without confirmation.)

5.  **Verify Rollback**: Confirm that the original error management and logging system is fully functional and that no new data is being written to the BigQuery tables.