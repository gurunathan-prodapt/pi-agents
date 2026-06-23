# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_bp_ta_cntrct_evn.ksh` KornShell job, responsible for orchestrating the preparation and provisioning of basic product contract data for the BERT system. The job, along with its core business logic residing in the downstream `k_ausd_bp_ta_cntrct_evn.ksh` kernel script, has been migrated from a legacy KornShell environment to Google Cloud Platform (GCP).

The target platform for this migration is **BigQuery**, utilizing BigQuery Stored Procedures for both orchestration and core data processing, and BigQuery Scheduled Queries for job scheduling. Logging has been centralized into a dedicated BigQuery audit table.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL files:

*   **`sql/ddl/job_log.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `job_log` BigQuery table. This table serves as the centralized audit and logging mechanism for job execution, replacing the legacy file-based logging (`DWMSG_*` functions). It captures job name, log level, messages, and timestamps for each run.

*   **`sql/procedures/process_contract_data.sql`**
    *   **Role**: Contains the BigQuery Stored Procedure (`process_contract_data`) that encapsulates the core business logic previously found in `k_ausd_bp_ta_cntrct_evn.ksh`. This procedure is responsible for selecting, transforming, and loading contract event data into the `sof_ta_cntrct_evn` table, handling both full refreshes and incremental updates based on the `p_wiederanlaufWert` parameter.

*   **`sql/procedures/ausd_bp_ta_cntrct_evn.sql`**
    *   **Role**: Defines the main BigQuery Stored Procedure (`ausd_bp_ta_cntrct_evn`) that orchestrates the entire job. This procedure is the direct migration of `r_ausd_bp_ta_cntrct_evn.ksh`. It handles parameter parsing, defaulting (`p_stichtag`, `p_wiederanlaufWert`), date determination, validation, logging job start/end/errors to the `job_log` table, and invoking the `process_contract_data` procedure.

*   **`sql/orchestration/scheduled_query.sql`**
    *   **Role**: Provides the SQL content for a BigQuery Scheduled Query. This query is configured to periodically call the main orchestration procedure (`ausd_bp_ta_cntrct_evn`), effectively replacing the legacy job scheduler. It demonstrates how to pass parameters (or use defaults) to the main procedure.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **BigQuery Stored Procedures for Logic Encapsulation**: Both the orchestration logic (from `r_ausd_bp_ta_cntrct_evn.ksh`) and the core business logic (from `k_ausd_bp_ta_cntrct_evn.ksh`) were translated into separate BigQuery Stored Procedures.
    *   **Why**: This approach leverages BigQuery's native capabilities for data processing, keeping computation close to the data. It reduces external dependencies, simplifies deployment, and improves performance by minimizing data movement.
    *   **Trade-offs**: Required a complete translation of shell scripting constructs (e.g., parameter parsing, date manipulation, error handling) into BigQuery SQL. Direct OS-level functionalities (like file system operations or specific signal traps) are not directly replicable and are handled at the orchestration layer or via BigQuery's error handling.

*   **Centralized BigQuery Table for Logging**: The custom `DWMSG_*` logging framework was replaced by direct `INSERT` statements into a dedicated `job_log` BigQuery table.
    *   **Why**: Provides centralized, queryable logs within the data platform, allowing for easier monitoring, auditing, and debugging. It integrates well with GCP's logging ecosystem.
    *   **Trade-offs**: Requires explicit SQL `INSERT` statements for each log entry, rather than simple shell `echo` commands.

*   **BigQuery Scheduled Queries for Orchestration**: The job's scheduling and invocation are managed via a BigQuery Scheduled Query.
    *   **Why**: Offers a simple, native, and cost-effective way to schedule BigQuery operations without requiring a separate orchestration service for this specific job's complexity.
    *   **Trade-offs**: BigQuery Scheduled Queries have limitations compared to more robust orchestrators like Cloud Composer (Apache Airflow). They offer less flexibility for complex dependencies, external system calls, or highly dynamic parameter generation beyond `CURRENT_DATE()`. For future, more complex requirements, Cloud Composer would be a suitable alternative.

*   **Native BigQuery Functions for Parameter and Date Handling**: Shell script's parameter parsing, defaulting, and date manipulation logic were translated using BigQuery's built-in functions (`IFNULL`, `NULLIF`, `CURRENT_DATE()`, `FORMAT_DATE()`).
    *   **Why**: Utilizes native SQL capabilities, ensuring consistency and performance within the BigQuery environment.
    *   **Trade-offs**: Requires careful mapping of legacy shell variable types and default behaviors to BigQuery SQL types and logic.

*   **BigQuery `EXCEPTION WHEN ERROR` for Error Handling**: The legacy `trap` commands for signal handling were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` blocks and `RAISE` statements.
    *   **Why**: Provides a structured and standard way to handle errors within BigQuery Stored Procedures, allowing for logging of failures to the `job_log` table and propagating errors.
    *   **Trade-offs**: Represents a different paradigm than OS-level signal handling; specific OS-level recovery actions are not directly translated.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and Billing**: Ensure a Google Cloud Project is set up and has billing enabled.
2.  **BigQuery Dataset Creation**: Create the target BigQuery dataset (e.g., `your_dataset_id`) where the tables and procedures will reside.
    *   `bq mk --dataset your_project_id:your_dataset_id`
3.  **IAM Permissions**:
    *   **Service Account**: Create a dedicated service account for running the BigQuery Scheduled Query. This service account will need:
        *   `BigQuery Data Editor` role on the target dataset (`your_project_id.your_dataset_id`) to read/write data and execute procedures.
        *   `BigQuery Job User` role on the project (`your_project_id`) to run BigQuery jobs.
    *   **Deployment Users**: Users or groups responsible for deploying these resources will need appropriate `BigQuery Admin` or `BigQuery Data Editor` roles.
4.  **Deploy DDL for `job_log` table**: Execute the SQL from `sql/ddl/job_log.sql` in BigQuery to create the audit log table.
    *   Remember to replace `your_project_id.your_dataset_id` with your actual project and dataset IDs.
5.  **Deploy Stored Procedures**: Execute the SQL from `sql/procedures/process_contract_data.sql` and `sql/procedures/ausd_bp_ta_cntrct_evn.sql` in BigQuery to create the procedures.
    *   Remember to replace `your_project_id.your_dataset_id` with your actual project and dataset IDs.
6.  **Source Data Tables**: Ensure the source tables (`sof_ta_bpr_evn`) and target tables (`sof_ta_cntrct_evn`) exist in `your_project_id.your_dataset_id` with the correct schemas and data types, matching the expectations of the `process_contract_data` procedure.
7.  **Configure BigQuery Scheduled Query**:
    *   Navigate to BigQuery in the GCP Console.
    *   Go to "Scheduled queries" and click "CREATE NEW SCHEDULED QUERY".
    *   Provide a name (e.g., `r_ausd_bp_ta_cntrct_evn_scheduler`).
    *   Paste the content of `sql/orchestration/scheduled_query.sql` into the query editor.
    *   Replace `your_project_id.your_dataset_id` with your actual project and dataset IDs.
    *   Configure the desired schedule (e.g., daily, hourly).
    *   Select the service account created in step 3.
    *   Specify the destination dataset for query results (can be a temporary dataset or the same target dataset).
    *   Enable the scheduled query.
8.  **Parameterization Review**: Decide if the scheduled query should pass explicit values for `p_stichtag` and `p_wiederanlaufWert` or rely on the stored procedure's internal defaults (passing `NULL` for both).

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps, unresolved references, or areas for potential follow-up:

*   **Missing Complexity and Automation Rate**: The original analysis lacked detailed complexity and automation rate metrics. This means the effort estimation for this and similar migrations might be less precise than desired.
*   **Kernel Script Logic Completeness**: The migration of `k_ausd_bp_ta_cntrct_evn.ksh` to `process_contract_data` assumes its logic is purely SQL-translatable. If the original kernel script contained complex non-SQL logic (e.g., external system calls, intricate file manipulations, or advanced string processing not covered by the `CASE` statement), these aspects are not explicitly migrated and would require further analysis (e.g., UDFs, Cloud Functions, or Python scripts in Cloud Composer).
*   **`p_stichtag` Usage in `process_contract_data`**: The `p_stichtag` parameter is passed to the `process_contract_data` procedure, but its current BigQuery SQL implementation does not explicitly use it for data filtering (e.g., `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`). If the original `k_ausd_bp_ta_cntrct_evn.ksh` used `Stichtag` to filter the data being processed, this functionality is currently missing in the migrated `process_contract_data` and needs to be added.
*   **Exact Date Synchronization (`MIN(sysdate, maxladedatum)`)**: The design document noted a commented-out section in the legacy script indicating a potential historical requirement for `MIN(sysdate, maxladedatum)`. The current migration relies solely on `CURRENT_DATE()` for `Stichtag` defaulting. If the `MIN(sysdate, maxladedatum)` logic is a current or future requirement, it needs to be explicitly implemented in BigQuery SQL using `LEAST()` and appropriate table lookups.
*   **`DWH_VERTRAG_ID` Data Type Confirmation**: The `p_wiederanlaufWert` parameter is used to filter based on `cntrct_id`. The migration assumes `cntrct_id` in `sof_ta_bpr_evn` and `sof_ta_cntrct_evn` is the equivalent of `DWH_VERTRAG_ID` and is compatible with `INT64`. This data type mapping needs explicit confirmation to prevent potential type casting errors or incorrect comparisons.
*   **Security Context of Init Files (`. $HOME/.dw_init`)**: The contents and implications of the `.dw_init` file (e.g., environment variables, paths, security settings) are not directly migrated. Any critical configurations from this file must be explicitly re-established in the GCP environment, potentially using IAM roles, service account environment variables, or Cloud Secret Manager.

## 6. Validation

Validation of the migrated job involves several stages:

### Deployment Validation
1.  **BigQuery Resources**: Verify that the `job_log` table and both `process_contract_data` and `ausd_bp_ta_cntrct_evn` stored procedures exist in the target BigQuery dataset (`your_project_id.your_dataset_id`).
2.  **Scheduled Query**: Confirm that the BigQuery Scheduled Query is created, enabled, and configured with the correct schedule and service account.

### Functional Validation
Execute the main orchestration procedure (`ausd_bp_ta_cntrct_evn`) with various parameter combinations:

1.  **Test Case 1 (Full Refresh)**:
    *   **Action**: Manually execute `CALL your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn(NULL, NULL);` (or `CALL your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn('DDMMYYYY', 0);` for a specific date).
    *   **Expected Result**:
        *   The `sof_ta_cntrct_evn` table should be truncated and then fully repopulated with data derived from `sof_ta_bpr_evn` based on the `CASE` logic in `process_contract_data`.
        *   The `job_log` table should contain two 'INFO' entries for this run: one for job start and one for successful completion.

2.  **Test Case 2 (Incremental Update)**:
    *   **Action**: Manually execute `CALL your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn(NULL, <a_valid_DWH_VERTRAG_ID_value>);` where `<a_valid_DWH_VERTRAG_ID_value>` is an `INT64` representing a `cntrct_id`.
    *   **Expected Result**:
        *   Rows in `sof_ta_cntrct_evn` with `cntrct_id >= <value>` should be deleted.
        *   New/updated rows with `cntrct_id > <value>` should be inserted into `sof_ta_cntrct_evn`.
        *   The `job_log` table should contain 'INFO' entries for job start and successful completion.

3.  **Test Case 3 (Parameter Validation / Error Handling)**:
    *   **Action**: Manually execute `CALL your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn('', NULL);` (or any other input that would trigger the `RAISE` for a missing `Stichtag`).
    *   **Expected Result**:
        *   The procedure should terminate with an error, and a `RAISE` message should be visible in the BigQuery UI/logs.
        *   The `job_log` table should contain an 'INFO' entry for job start and an 'ERROR' entry indicating job failure with the error message.

### Data Validation
1.  **Row Counts and Checksums**: After a full refresh (Test Case 1), compare the row count and, if possible, data checksums of the `sof_ta_cntrct_evn` table in BigQuery against the output of the legacy system for the same cutoff date.
2.  **Spot Checks**: Perform detailed spot checks on a representative sample of `cntrct_id` and their corresponding `evn` values to ensure the `CASE` logic and data transformations are correctly applied.

### "Passing" Means
*   All BigQuery Stored Procedure calls complete successfully without unhandled errors.
*   The `job_log` table accurately records the start, successful completion, or specific error messages for each job run.
*   The data in `your_project_id.your_dataset_id.sof_ta_cntrct_evn` precisely matches the expected output from the legacy system for equivalent input parameters and source data.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Disable BigQuery Scheduled Query**: Immediately pause or delete the BigQuery Scheduled Query configured for this job to prevent further execution of the migrated code.
2.  **Revert BigQuery Objects**:
    *   Drop the deployed BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.ausd_bp_ta_cntrct_evn`;
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.process_contract_data`;
        ```
    *   (Optional) If the `job_log` table was created solely for this migration and is not shared, it can also be dropped:
        ```sql
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_log`;
        ```
3.  **Restore Legacy Job**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh` job in the legacy environment.
4.  **Data Rollback**:
    *   If the `sof_ta_cntrct_evn` table in BigQuery was modified by the migrated job, restore it from a backup taken immediately before the migration cutover.
    *   Alternatively, if the legacy system can regenerate the data, run the original `r_ausd_bp_ta_cntrct_evn.ksh` job to repopulate the target table in the legacy environment, and then re-ingest that data into BigQuery if necessary.