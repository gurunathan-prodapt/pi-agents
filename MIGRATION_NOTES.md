# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh` has been migrated. This script, responsible for orchestrating the initial provisioning of selected basic products for the BERT system and generating a snapshot of contract cache data from the Data Warehouse (DWH) for a downstream Forderungsscoring (FOS) process, has been re-platformed to **Google BigQuery**.

The original script's functionality, including parameter handling, date determination, error handling, and the invocation of its core business logic, has been encapsulated within BigQuery Stored Procedures and supporting BigQuery tables.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`ddl/dwh_ta_c_vertrag.sql`**
    *   **Role:** Data Definition Language (DDL) script to create the BigQuery table `project.dataset.DWH_TA_C_VERTRAG`. This table serves as the target for the `DWH$TA_C_VERTRAG` source table from the legacy DWH system. It includes placeholder columns (`column1`, `column2`) that must be replaced with the actual schema of the source table.
*   **`ddl/fos_table.sql`**
    *   **Role:** DDL script to create the BigQuery table `project.dataset.FOS_TABLE`. This table is the target for the processed data, corresponding to the `FOS_TABLE` in the legacy environment. It also includes placeholder columns (`column1`, `column2`) that must be replaced with the actual schema of the target table.
*   **`ddl/job_log.sql`**
    *   **Role:** DDL script to create the BigQuery table `project.dataset.job_log`. This table is designed for general-purpose logging of job execution messages, particularly for error reporting.
*   **`ddl/job_audit.sql`**
    *   **Role:** DDL script to create the BigQuery table `project.dataset.job_audit`. This table serves as an audit trail for each execution of the main stored procedure, capturing parameters, status, and timestamps for tracking job lifecycle.
*   **`sprocs/process_contract_cache_data.sql`**
    *   **Role:** BigQuery Stored Procedure `project.dataset.process_contract_cache_data`. This procedure encapsulates the core business logic previously found in `k_ausd_bp_ta_bpr_beschr.ksh`. It handles the conditional deletion for restart scenarios and the main `INSERT INTO ... SELECT` operation from `DWH_TA_C_VERTRAG` to `FOS_TABLE`, applying the necessary date and ID filters.
*   **`sprocs/ausd_bp_ta_bpr_beschr.sql`**
    *   **Role:** BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_beschr`. This is the main orchestration procedure, directly replacing `r_ausd_bp_ta_bpr_beschr.ksh`. It handles parameter parsing, date defaulting, validation, logging to `job_audit`, and invokes `process_contract_cache_data` for the core data processing. It also includes robust error handling.

## 3. Key design decisions

*   **Orchestration to BigQuery Stored Procedure:** The KornShell orchestration script (`r_ausd_bp_ta_bpr_beschr.ksh`) was directly translated into a BigQuery Stored Procedure (`ausd_bp_ta_bpr_beschr`). This decision leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, eliminating the need for external shell environments.
*   **Core Logic Encapsulation:** The business logic from the invoked KornShell script (`k_ausd_bp_ta_bpr_beschr.ksh`) was migrated into a separate BigQuery Stored Procedure (`process_contract_cache_data`). This promotes modularity and reusability, allowing the orchestration layer to focus on control flow and auditing, while the core procedure handles data manipulation.
*   **Native SQL for Utilities:** Functions previously provided by helper shell scripts (e.g., `h_alis_date.ksh` for date handling, `h_alis_parameter.ksh` for parameter parsing) are re-implemented using standard BigQuery SQL functions and constructs (e.g., `FORMAT_DATE`, `PARSE_DATE`, `IFNULL`, `DECLARE`, `IF`). This reduces external dependencies and keeps the entire solution within the BigQuery ecosystem.
*   **Dedicated Logging and Auditing Tables:** Instead of file-based logging, two BigQuery tables (`job_log` and `job_audit`) were introduced. This centralizes logging, provides structured and queryable audit trails, and enhances observability of job executions and errors directly within BigQuery.
*   **Restart Mechanism (`p_wiederanlaufWert`) in SQL:** The restart logic, which involves conditional deletion of data based on a `p_wiederanlaufWert`, is directly implemented within the `process_contract_cache_data` stored procedure using a `DELETE` statement. This ensures the restart functionality is native to the data processing layer.
*   **Trade-offs:**
    *   **Increased SQL Complexity:** The migration results in more complex SQL code within stored procedures compared to simple shell script orchestration. This requires a deeper understanding of BigQuery SQL procedural language.
    *   **Dependency on BigQuery Ecosystem:** The solution is now tightly coupled with BigQuery, which is beneficial for performance and integration within GCP, but reduces portability to other database systems.
    *   **Placeholder Schemas:** Due to the lack of full source schema details for `DWH$TA_C_VERTRAG` and `FOS_TABLE`, placeholder columns were used. This requires manual refinement post-generation.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **Schema Refinement:**
    *   **`ddl/dwh_ta_c_vertrag.sql`**: Update the `CREATE TABLE` statement to accurately reflect the full schema (all columns and their data types) of the source `DWH$TA_C_VERTRAG` table.
    *   **`ddl/fos_table.sql`**: Update the `CREATE TABLE` statement to accurately reflect the full schema (all columns and their data types) of the target `FOS_TABLE`.
    *   **`sprocs/process_contract_cache_data.sql`**: Update the `INSERT INTO` statement to list all actual column names for `FOS_TABLE` and ensure the `SELECT` clause retrieves the corresponding columns from `DWH_TA_C_VERTRAG`.
3.  **Deploy DDLs:** Execute the DDL scripts (`dwh_ta_c_vertrag.sql`, `fos_table.sql`, `job_log.sql`, `job_audit.sql`) in BigQuery to create the necessary tables.
4.  **Deploy Stored Procedures:** Execute the stored procedure scripts (`process_contract_cache_data.sql`, `ausd_bp_ta_bpr_beschr.sql`) in BigQuery to create the procedures.
5.  **IAM/Permissions:**
    *   Grant the service account or user executing the BigQuery stored procedures the necessary IAM roles. This typically includes `BigQuery Data Editor` (for `project.dataset.DWH_TA_C_VERTRAG`, `project.dataset.FOS_TABLE`, `project.dataset.job_log`, `project.dataset.job_audit`) and `BigQuery Job User` (to run queries and procedures).
6.  **Source Data Ingestion:** Ensure that the `project.dataset.DWH_TA_C_VERTRAG` table is populated with the relevant source data from the legacy DWH system. This might involve a separate data ingestion pipeline (e.g., using Dataflow, Cloud Storage, or BigQuery Data Transfer Service).
7.  **Scheduling:** Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer/Airflow DAG) to invoke the main stored procedure `project.dataset.ausd_bp_ta_bpr_beschr` with the required parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`).

## 5. Known gaps & unresolved references

The following items have been identified as gaps or require further attention:

*   **Full `k_ausd_bp_ta_bpr_beschr.ksh` Analysis:** The core data transformation logic from `k_ausd_bp_ta_bpr_beschr.ksh` was not fully analyzed during this migration. While a placeholder stored procedure (`process_contract_cache_data`) has been created, its exact SQL queries, transformations, and control flow need to be thoroughly reviewed and implemented based on the original script's detailed logic. This includes identifying all source tables beyond `DWH$TA_C_VERTRAG` and any complex business rules.
*   **Detailed Logging Strategy:** While `job_log` and `job_audit` tables are provided, a more comprehensive logging strategy, including detailed error messages, execution traces, and potentially metrics, should be designed and implemented for production environments.
*   **Environment Variables:** The original script uses environment variables like `BERT_DIR_ROOT`. These implicit dependencies need to be either externalized as parameters to the BigQuery stored procedure, configured in a BigQuery configuration table, or hardcoded if their values are static and known.
*   **`MIN(sysdate, maxladedatum)` Logic:** The design document noted a commented-out section in the original script suggesting a `MIN(sysdate, maxladedatum)` logic for `p_stichtag`. If this logic is a business requirement, it must be explicitly re-introduced and implemented in the BigQuery stored procedure.
*   **Placeholder Column Names:** The generated DDLs and the `process_contract_cache_data` stored procedure use generic `column1`, `column2` placeholders. These *must* be replaced with the actual, correct column names and data types from the source and target schemas.
*   **Data Type Mapping:** Ensure that the data types chosen for the BigQuery tables (`DWH_TA_C_VERTRAG`, `FOS_TABLE`) accurately reflect the data types from the legacy DWH system to prevent data loss or conversion errors.

## 6. Validation

To ensure the successful migration and correct functionality of the BigQuery solution, the following validation steps should be performed:

1.  **Unit Testing of Stored Procedures:**
    *   Individually test `process_contract_cache_data` with various `p_stichtag` and `p_restart_value` inputs to verify correct data manipulation (DELETE and INSERT logic).
    *   Test `ausd_bp_ta_bpr_beschr` with valid and invalid `p_stichtag_in` and `p_wiederanlaufWert_in` parameters to confirm correct parameter parsing, defaulting, and error handling.
2.  **Integration Testing:**
    *   Execute the main stored procedure `project.dataset.ausd_bp_ta_bpr_beschr` with a representative set of test data in `DWH_TA_C_VERTRAG`.
    *   Verify that `FOS_TABLE` is populated correctly according to the expected business logic.
    *   Test restart scenarios by running the procedure with `p_wiederanlaufWert_in > 0` and verifying that previous data is correctly handled (deleted/overwritten) before new data is inserted.
3.  **Data Comparison (Reconciliation):**
    *   Run the original KornShell script with a specific set of input parameters and capture its output in `FOS_TABLE`.
    *   Run the migrated BigQuery stored procedure with the *exact same* input parameters and source data.
    *   Compare the data in the BigQuery `FOS_TABLE` with the data produced by the legacy system. A row-by-row comparison or aggregate checks (e.g., row counts, sum of key columns) should be performed to ensure data parity.
4.  **Logging and Auditing Verification:**
    *   After each test run, query `project.dataset.job_log` and `project.dataset.job_audit` to ensure that job status, parameters, start/end times, and any error messages are accurately recorded.
5.  **Performance Testing:**
    *   Measure the execution time of the BigQuery stored procedure and compare it against the legacy script's runtime to ensure performance meets expectations.

**"Passing" Criteria:**

*   All BigQuery stored procedures execute without unhandled errors.
*   The `project.dataset.FOS_TABLE` contains data that is functionally identical to the output produced by the original `r_ausd_bp_ta_bpr_beschr.ksh` script for the same input.
*   The `project.dataset.job_audit` table accurately reflects the execution status (STARTED, OK, ERROR) and details for each run.
*   Performance metrics are within acceptable thresholds.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after deployment, the following rollback procedure can be initiated:

1.  **Stop New Executions:** Immediately halt any scheduled executions of the BigQuery stored procedure (`project.dataset.ausd_bp_ta_bpr_beschr`) by disabling or pausing the Cloud Scheduler job or Airflow DAG.
2.  **Revert Scheduling:** Reconfigure the scheduling system to point back to and execute the original KornShell script (`r_ausd_bp_ta_bpr_beschr.ksh`) in the legacy environment.
3.  **Data Restoration (if necessary):**
    *   If the `project.dataset.FOS_TABLE` was modified or corrupted by the migrated job, restore it to its state prior to the problematic BigQuery execution. This requires a robust backup strategy for `FOS_TABLE` (e.g., BigQuery table snapshots, point-in-time recovery, or a full table export).
    *   Alternatively, if the impact is limited, the original KornShell script can be run with appropriate parameters to re-generate the correct data in the `FOS_TABLE`.
4.  **Deactivate BigQuery Components (Optional):** If the rollback is intended to be long-term, the BigQuery stored procedures (`ausd_bp_ta_bpr_beschr`, `process_contract_cache_data`) can be dropped or renamed to prevent accidental invocation. The `FOS_TABLE` and other DDL-created tables can also be dropped if they are not shared with other processes and were created solely for this migration.
5.  **Investigation:** Analyze the `project.dataset.job_log` and `project.dataset.job_audit` tables for error messages and execution details to diagnose the root cause of the issue.