# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell utility script `f_alis_msgerr.ksh` from its legacy environment (interacting with an Oracle database via `sqlplus`) to Google Cloud Platform (GCP).

The original script provided standardized error handling, logging, and status management functionalities, acting as a wrapper for Oracle PL/SQL procedures. The migration re-implements these functionalities using:

*   **Google BigQuery:** For all data storage (replacing Oracle tables) and procedural logic (replacing Oracle PL/SQL functions).
*   **Python (Orchestration Layer):** To handle shell-specific behaviors, environment variable management, and to serve as the interface for calling the new BigQuery Stored Procedures. This layer can be deployed on services like Cloud Functions, Cloud Run, or Cloud Composer.

The migration aims to modernize the error handling and logging infrastructure, leveraging GCP's scalable and managed services.

## 2. Generated artifacts

The migration process generated the following files:

*   **`sql/ddl/bert_meldung_schema.sql`**
    *   **Role:** Defines the BigQuery Data Definition Language (DDL) for the `bert_meldung` and `bert_meldung_fehler` tables. These tables are the BigQuery equivalents of the legacy Oracle `BERT_MELDUNG` table (and potentially a separate error logging table). They store all message, log, and error entries.

*   **`sql/procedures/dwmsg_ermittle_nr.sql`**
    *   **Role:** A BigQuery Stored Procedure that generates a unique entry number (UUID) for log and message entries. This replaces the Oracle PL/SQL logic that generated unique numbers and the temporary file mechanism used in the KSH script.

*   **`sql/procedures/dwmsg_erzeuge_eintrag.sql`**
    *   **Role:** A BigQuery Stored Procedure responsible for inserting a new message/log entry into the `bert_meldung` table with an initial status of 'IN_PROGRESS'.

*   **`sql/procedures/dwmsg_setze_status_ok.sql`**
    *   **Role:** A BigQuery Stored Procedure that updates the status of a specific entry in the `bert_meldung` table to 'OK', indicating successful completion.

*   **`sql/procedures/dwmsg_setze_status_abbruch.sql`**
    *   **Role:** A BigQuery Stored Procedure that updates the status of a specific entry in the `bert_meldung` table to 'ABORTED', indicating a failure or termination.

*   **`sql/procedures/dwmsg_melde_fehler.sql`**
    *   **Role:** A BigQuery Stored Procedure that records detailed error information into the `bert_meldung_fehler` table and optionally updates the main `bert_meldung` entry's status to 'ERROR'. This replaces the `sqlplus` calls to `d_alis_spaufruf_pX.sql` for error logging.

*   **`sql/procedures/dwmsg_logdateiname.sql`**
    *   **Role:** A BigQuery Stored Procedure that constructs and returns a standardized log filename string based on job and program identifiers.

*   **`sql/procedures/dwmsg_setze_stichtag_info.sql`**
    *   **Role:** A BigQuery Stored Procedure that updates the `zusatz_infos` field in the `bert_meldung` table with key date (Stichtag) information. It attempts to maintain `zusatz_infos` as a JSON string.

*   **`sql/procedures/dwmsg_append_timing_infos.sql`**
    *   **Role:** A BigQuery Stored Procedure that appends timing-related information to the `zusatz_infos` field of a `bert_meldung` entry. It also attempts to maintain `zusatz_infos` as a JSON string.

*   **`sql/procedures/dwmsg_fehlerbehandlung.sql`**
    *   **Role:** A BigQuery Stored Procedure that encapsulates the generic error handling logic. It calls `dwmsg_melde_fehler` to log the error and `dwmsg_setze_status_abbruch` to update the main entry's status. It also signals an SQLSTATE error to the caller.

*   **`python/orchestration_example.py`**
    *   **Role:** An example Python script demonstrating how to interact with the BigQuery Stored Procedures. It serves as a blueprint for the new orchestration layer, showing how to call the procedures, pass parameters, and handle return values and errors.

## 3. Key design decisions

The migration strategy involved several key design decisions to transition from a KornShell/Oracle environment to GCP:

*   **BigQuery as the Central Data Store:** The Oracle `BERT_MELDUNG` table and related error logging mechanisms are fully migrated to BigQuery tables (`bert_meldung`, `bert_meldung_fehler`). This leverages BigQuery's scalability, analytical capabilities, and managed service benefits.
*   **BigQuery Stored Procedures for Logic Re-implementation:** Each distinct function within the original `f_alis_msgerr.ksh` script that involved database interaction (e.g., `DWMSG_ErmittleNr`, `DWMSG_ErzeugeEintrag`, `DWMSG_MeldeFehler`) has been re-implemented as a BigQuery Stored Procedure. This encapsulates the logic directly within the database layer, promoting reusability and maintainability.
*   **Python-based Orchestration Layer:** A lightweight Python layer (exemplified by `orchestration_example.py`) is introduced to replace the shell's role in:
    *   Calling database functions (now BigQuery Stored Procedures).
    *   Managing job-level parameters and environment variables (passed as function arguments or configuration).
    *   Handling shell-specific constructs like `eval` and temporary files, which are now replaced by direct parameter passing and return values from BigQuery procedures.
    *   Providing a robust interface for other GCP services (e.g., Cloud Functions, Cloud Run, Cloud Composer) to interact with the logging and error handling system.
*   **Native BigQuery Features for Unique ID Generation:** The legacy method of generating unique numbers (via Oracle PL/SQL and temporary files) is replaced by BigQuery's `GENERATE_UUID()` function, simplifying the process and eliminating filesystem dependencies.
*   **Standardized Error Handling within BigQuery:** The shell's `trap ERR` mechanism is replaced by BigQuery's `BEGIN...EXCEPTION...END` blocks and `SIGNAL SQLSTATE` within stored procedures, providing more structured error management. The orchestration layer is responsible for catching and reacting to these signals.
*   **Flexible `ZusatzInfos` Handling:** The `zusatz_infos` field in `bert_meldung` is designed to store additional information, attempting to maintain a JSON structure for better queryability. However, it includes a fallback to string concatenation if the existing content is not valid JSON, providing robustness for potentially inconsistent legacy data.
*   **Timezone Consideration for Timestamps:** `FORMAT_TIMESTAMP` in BigQuery procedures explicitly uses a timezone (`'Europe/Berlin'`) for consistency, which is a best practice for global operations.

## 4. Manual steps before go-live

Before the migrated `f_alis_msgerr.ksh` functionality can go live, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure a GCP project is active and billing is enabled.
    *   Create the target BigQuery dataset (e.g., `my_dataset`) within your project (e.g., `my_project`). This dataset will host the `bert_meldung` and `bert_meldung_fehler` tables and all stored procedures.
    *   **Action:** `bq mk --dataset my_project:my_dataset`

2.  **Deploy BigQuery Table Schemas:**
    *   Execute the DDL script `sql/ddl/bert_meldung_schema.sql` to create the `bert_meldung` and `bert_meldung_fehler` tables in your target BigQuery dataset.
    *   **Action:** `bq query --use_legacy_sql=false < sql/ddl/bert_meldung_schema.sql` (after replacing `my_project.my_dataset` placeholders).

3.  **Migrate Historical Data (if applicable):**
    *   If historical log/message data from the Oracle `BERT_MELDUNG` table needs to be preserved, establish and execute a data migration process to transfer this data to the new BigQuery `bert_meldung` table. This could involve:
        *   Exporting Oracle data to CSV/JSON and loading into BigQuery using `bq load`.
        *   Using Dataflow for more complex transformations or ongoing synchronization.
        *   Using Database Migration Service (DMS) for a managed migration.
    *   **Action:** Plan and execute data migration strategy.

4.  **Deploy BigQuery Stored Procedures:**
    *   Deploy all `.sql` files located in the `sql/procedures/` directory to your BigQuery dataset. Each file represents a BigQuery Stored Procedure.
    *   **Action:** For each `.sql` file in `sql/procedures/`, execute: `bq query --use_legacy_sql=false < sql/procedures/dwmsg_procedure_name.sql` (after replacing `my_project.my_dataset` placeholders).

5.  **IAM Permissions Configuration:**
    *   Configure appropriate Identity and Access Management (IAM) roles for the service account(s) that will execute the Python orchestration layer and interact with BigQuery.
    *   Minimum required roles typically include:
        *   `BigQuery Data Editor` (to insert/update data in `bert_meldung` and `bert_meldung_fehler`).
        *   `BigQuery Job User` (to run queries and stored procedures).
    *   **Action:** Grant necessary IAM roles to the service account(s) on the BigQuery project or dataset.

6.  **Deploy Orchestration Layer:**
    *   Deploy the Python orchestration code (based on `python/orchestration_example.py`) to your chosen GCP service (e.g., Cloud Functions, Cloud Run, Cloud Composer).
    *   Ensure the `PROJECT_ID` and `DATASET_ID` placeholders in the Python code are updated to reflect your actual environment.
    *   **Action:** Follow deployment instructions for your chosen GCP service (e.g., `gcloud functions deploy`, `gcloud run deploy`, or Airflow DAG deployment).

7.  **Update Calling Jobs:**
    *   **Crucial Step:** Identify all existing legacy jobs and scripts that currently call `f_alis_msgerr.ksh`.
    *   Modify these calling jobs to invoke the new Python orchestration layer (or directly call BigQuery Stored Procedures if the orchestration layer is not used) instead of the original KornShell script. This involves updating their execution commands and parameter passing mechanisms.
    *   **Action:** Analyze legacy job dependencies, update scripts/configurations to use the new GCP-based logging/error handling.

## 5. Known gaps & unresolved references

The following items are identified as known gaps, potential risks, or areas requiring further follow-up (B4 items):

*   **Comprehensive Oracle PL/SQL Complexity Assessment:** The current design assumes a relatively straightforward conversion of Oracle PL/SQL to BigQuery SQL. If the underlying Oracle `BERT_MELDUNG` package contains highly complex business logic, advanced PL/SQL features (e.g., complex cursors, custom data types, extensive procedural loops) that are not easily translatable to BigQuery SQL, a more detailed analysis and potential re-architecture (e.g., using Dataflow or Cloud Functions for procedural logic) might be required.
*   **Identification and Migration of All Consumers:** The most significant unresolved reference is the complete mapping and subsequent migration of all legacy jobs and scripts that currently rely on `f_alis_msgerr.ksh`. This script is a utility, meaning its value is derived from its callers. A thorough "reverse dependency" analysis is critical to ensure a smooth cutover and prevent disruption to other processes. This is a **B4 item** requiring dedicated effort.
*   **`ZusatzInfos` JSON Consistency:** While the `dwmsg_setze_stichtag_info` and `dwmsg_append_timing_infos` procedures attempt to maintain `zusatz_infos` as a JSON string, they include a fallback to string concatenation if the existing content is not valid JSON. If strict JSON format is required for downstream analytics or processing, a data cleansing or migration step might be necessary to ensure all historical `zusatz_infos` data conforms to JSON.
*   **Error Context from External Systems:** The original `DWMSG_Fehlerbehandlung` relied on shell exit codes (`$?`). The new `dwmsg_fehlerbehandlung` procedure in BigQuery expects explicit error codes and messages as parameters. The orchestration layer (or the calling job itself) is responsible for capturing errors from external commands/processes and translating them into the expected parameters for `dwmsg_fehlerbehandlung`. This translation mechanism needs to be robustly implemented in each calling job.

## 6. Validation

Validation ensures that the migrated components function correctly and meet the requirements of the original script.

### How to Run Tests:

1.  **BigQuery DDL and Stored Procedure Deployment Verification:**
    *   After deploying the DDL and stored procedures, verify their existence and syntax.
    *   **Action:** Use `bq show --schema my_project:my_dataset.bert_meldung` and `bq show --schema my_project:my_dataset.bert_meldung_fehler` to check table schemas. Use `bq show --routine my_project:my_dataset.dwmsg_ermittle_nr` (and similar for other procedures) to verify procedure deployment.

2.  **Individual Stored Procedure Unit Tests:**
    *   Execute each BigQuery Stored Procedure independently using the BigQuery console or `bq query` command-line tool, providing sample input parameters.
    *   **Action (Example for `dwmsg_ermittle_nr`):**
        ```sql
        DECLARE entry_id STRING;
        CALL `my_project.my_dataset.dwmsg_ermittle_nr`(entry_id);
        SELECT entry_id;
        ```
    *   **Action (Example for `dwmsg_erzeuge_eintrag`):**
        ```sql
        CALL `my_project.my_dataset.dwmsg_erzeuge_eintrag`('test_entry_123', 'TEST_JOB', 'test_prog.py', '/tmp/test.log', 'INFO');
        SELECT * FROM `my_project.my_dataset.bert_meldung` WHERE eintrags_nr = 'test_entry_123';
        ```
    *   Test error conditions (e.g., calling with `NULL` `p_eintrags_nr` where not allowed) to ensure `SIGNAL SQLSTATE` is triggered.

3.  **Orchestration Layer Integration Tests:**
    *   Run the `python/orchestration_example.py` script (or your refined orchestration script) locally or in a test environment.
    *   **Action:** Execute the Python script with various scenarios (success, expected errors, unexpected errors).
    *   Ensure the script correctly initializes the `BigQueryErrorLogger`, calls the procedures, and handles both successful responses and exceptions (e.g., from `dwmsg_fehlerbehandlung`).

4.  **End-to-End Scenario Tests:**
    *   Integrate the new logging/error handling into a representative test job that mimics a real-world workload.
    *   **Action:** Run the test job and observe the entries in `my_project.my_dataset.bert_meldung` and `my_project.my_dataset.bert_meldung_fehler`. Verify that statuses, error details, and `zusatz_infos` are correctly recorded.

### What "Passing" Means:

*   **Schema Integrity:** `bert_meldung` and `bert_meldung_fehler` tables are created with the exact schema defined in `bert_meldung_schema.sql`.
*   **Procedure Execution:** All BigQuery Stored Procedures execute without syntax errors or runtime exceptions when called with valid parameters.
*   **Unique ID Generation:** `dwmsg_ermittle_nr` consistently returns valid, unique identifiers (UUIDs).
*   **Entry Creation:** `dwmsg_erzeuge_eintrag` successfully inserts new rows into `bert_meldung` with the correct initial status and metadata.
*   **Status Updates:** `dwmsg_setze_status_ok` and `dwmsg_setze_status_abbruch` correctly update the `status` and `last_update_timestamp` fields for existing entries.
*   **Error Logging:** `dwmsg_melde_fehler` successfully inserts detailed error records into `bert_meldung_fehler` and updates the main entry's status in `bert_meldung` to 'ERROR' (or 'ABORTED' via `dwmsg_fehlerbehandlung`).
*   **Log Filename Generation:** `dwmsg_logdateiname` returns a correctly formatted and plausible log filename string.
*   **`ZusatzInfos` Management:** `dwmsg_setze_stichtag_info` and `dwmsg_append_timing_infos` correctly update the `zusatz_infos` field, ideally maintaining a valid JSON structure, or at least appending information correctly in a readable format.
*   **Error Handling Flow:** `dwmsg_fehlerbehandlung` correctly orchestrates error logging and status updates, and the orchestration layer successfully catches the `SIGNAL SQLSTATE` from BigQuery.
*   **Data Accuracy:** All data inserted or updated in BigQuery tables matches the expected values based on the input parameters and transformation logic.
*   **Performance:** The BigQuery operations complete within acceptable timeframes for the expected workload.

## 7. Rollback procedure

In the event that the migrated `f_alis_msgerr.ksh` functionality needs to be rolled back, follow these steps:

1.  **Immediate Action: Revert Calling Jobs:**
    *   The most critical and immediate step is to revert all jobs and scripts that were modified to use the new GCP-based logging/error handling back to their original state, where they call the legacy `f_alis_msgerr.ksh` script.
    *   **Impact:** This will immediately restore the original logging and error handling behavior for all dependent processes.

2.  **Data Consistency Check (if applicable):**
    *   If any new data was generated in the BigQuery `bert_meldung` or `bert_meldung_fehler` tables that is critical for ongoing operations and was *not* also written to the legacy Oracle `BERT_MELDUNG` table (e.g., during a phased cutover), this data would need to be manually reconciled or migrated back to Oracle if a full data rollback is required.
    *   **Recommendation:** During initial testing and phased rollout, ensure that the Oracle system remains the source of truth for a defined period, or that a clear data synchronization strategy is in place.

3.  **Decommission GCP Components:**
    *   **Delete BigQuery Stored Procedures:** Remove all deployed BigQuery Stored Procedures from the target dataset.
        *   **Action:** For each procedure, `bq rm --routine my_project:my_dataset.dwmsg_procedure_name`
    *   **Delete BigQuery Tables:** Remove the `bert_meldung` and `bert_meldung_fehler` tables from the target dataset.
        *   **Action:** `bq rm --table my_project:my_dataset.bert_meldung` and `bq rm --table my_project:my_dataset.bert_meldung_fehler`
    *   **Remove Orchestration Layer Deployment:** Delete the deployed Python orchestration code from Cloud Functions, Cloud Run, Cloud Composer, or wherever it was deployed.
        *   **Action:** Follow the specific deletion instructions for the respective GCP service (e.g., `gcloud functions delete`, `gcloud run services delete`).

4.  **IAM Permissions Reversal:**
    *   Revoke any specific IAM permissions granted to service accounts solely for the purpose of accessing the new BigQuery logging/error handling system.

**Considerations for Rollback:**

*   **Data Loss:** Any new log/error entries created exclusively in the BigQuery tables *after* the cutover and *before* the rollback will be lost if not explicitly migrated back to the legacy system.
*   **Downtime:** The rollback process will likely involve some downtime for the affected jobs as they are reconfigured and redeployed to use the legacy system.
*   **Communication:** Clear communication with all stakeholders and dependent teams is essential during a rollback scenario.