# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the data processing job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh` and its associated Oracle SQL script `d_ausd_bp_ta_iccid_einzeln.sql`.

The original job, implemented as a KornShell script, was responsible for:
*   Parsing and validating input parameters (job identifier, entry number, key date, restart value).
*   Deriving "today" and "yesterday" dates.
*   Orchestrating the execution of an Oracle SQL script.
*   Capturing record counts from the SQL execution.

The underlying Oracle SQL script performed the core business logic, reading data from `DWTK_MELDUNGEN` and `SOF$TA_BPR_BASIS` tables and writing processed data into the `SOF$TA_ICCID_EINZELN` table.

The entire workflow has been migrated to **Google Cloud BigQuery**. The shell scripting logic (parameter handling, validation, date derivation, and orchestration) has been re-implemented as a BigQuery Stored Procedure. The Oracle SQL logic has been translated into BigQuery SQL and embedded within this stored procedure. All source and target tables have been mapped to BigQuery tables.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`sql/ddl/dwtk_meldungen.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the BigQuery table `project.dataset.dwtk_meldungen`. This table serves as the migrated counterpart to the original Oracle `DWTK_MELDUNGEN` source table.
*   **`sql/ddl/sof_ta_bpr_basis.sql`**
    *   **Role**: Defines the DDL for the BigQuery table `project.dataset.sof_ta_bpr_basis`. This table serves as the migrated counterpart to the original Oracle `SOF$TA_BPR_BASIS` source table.
*   **`sql/ddl/sof_ta_iccid_einzeln.sql`**
    *   **Role**: Defines the DDL for the BigQuery table `project.dataset.sof_ta_iccid_einzeln`. This table serves as the migrated counterpart to the original Oracle `SOF$TA_ICCID_EINZELN` target table.
*   **`sql/ddl/job_error_log.sql`**
    *   **Role**: Defines the DDL for the BigQuery table `project.dataset.job_error_log`. This table is used for centralized logging of errors encountered during the execution of the migrated stored procedure.
*   **`sql/ddl/job_run_result.sql`**
    *   **Role**: Defines the DDL for the BigQuery table `project.dataset.job_run_result`. This table is used for centralized logging of job run metadata, including input parameters, derived dates, and the final record count.
*   **`sql/stored_procedures/d_ausd_bp_ta_iccid_einzeln_proc.sql`**
    *   **Role**: Contains the BigQuery SQL code for the stored procedure `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`. This procedure encapsulates the entire migrated logic, including parameter validation, date derivation, data transformation, and insertion into the target table. It replaces both the original KornShell script and the Oracle SQL script.

## 3. Key design decisions

*   **Consolidated Logic into BigQuery Stored Procedure**: The control flow from the original KornShell script (`k_ausd_bp_ta_iccid_einzeln.ksh`) and the core data processing logic from the Oracle SQL script (`d_ausd_bp_ta_iccid_einzeln.sql`) have been combined into a single BigQuery Stored Procedure (`d_ausd_bp_ta_iccid_einzeln_proc`).
    *   **Rationale**: This approach leverages BigQuery's native capabilities for orchestration and data manipulation, reducing cross-platform dependencies and simplifying deployment and monitoring within the Google Cloud ecosystem. It promotes a "data-centric" approach where logic resides closer to the data.
    *   **Trade-offs**: Required re-implementation of shell-specific utilities (e.g., `getopts`, date functions, error handling) using BigQuery SQL scripting constructs. Oracle-specific SQL syntax and functions also needed careful translation.
*   **Native BigQuery Logging**: The original job's logging to console and temporary files has been replaced with inserts into dedicated BigQuery logging tables (`job_error_log`, `job_run_result`).
    *   **Rationale**: Provides centralized, queryable, and structured logging within BigQuery, enhancing observability, auditing, and troubleshooting capabilities.
*   **Elimination of Temporary Files**: The temporary file used to capture record counts has been removed. The record count is now obtained directly via BigQuery SQL (`SELECT COUNT(*)`) and logged to `job_run_result`.
    *   **Rationale**: Simplifies the workflow, removes file system dependencies, and aligns with BigQuery's serverless and managed service paradigm.
*   **Direct Schema Mapping**: Oracle source and target tables have been directly mapped to BigQuery tables with corresponding DDLs.
    *   **Rationale**: Maintains the existing data structure and relationships, simplifying the migration effort.
    *   **Trade-offs**: Assumes compatibility of data types between Oracle and BigQuery; minor adjustments were made where necessary (e.g., `TIMESTAMP` for `DATE` in `dwtk_meldungen` to capture `timecreated` more accurately, `DATE` for `valid_to`).
*   **Oracle-to-BigQuery SQL Translation**: Oracle-specific functions and syntax (e.g., `NVL`, `DECODE`, `TO_CHAR` formats, `SQL*Plus` commands) were converted to their BigQuery SQL equivalents (`COALESCE`, `CASE` statements, `FORMAT_DATE`, `PARSE_DATE`).
    *   **Rationale**: Ensures functional equivalence of the data transformation logic within the new BigQuery environment.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed in the target Google Cloud Project:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `project.dataset` (replace `project` and `dataset` with your actual project ID and dataset name) exists. If not, create it.
2.  **Table DDL Deployment**:
    *   Execute the DDL scripts for all required tables in the `project.dataset` BigQuery dataset:
        *   `sql/ddl/dwtk_meldungen.sql`
        *   `sql/ddl/sof_ta_bpr_basis.sql`
        *   `sql/ddl/sof_ta_iccid_einzeln.sql`
        *   `sql/ddl/job_error_log.sql`
        *   `sql/ddl/job_run_result.sql`
3.  **Initial Data Ingestion**:
    *   Migrate historical and any necessary ongoing data from the original Oracle `DWTK_MELDUNGEN` and `SOF$TA_BPR_BASIS` tables into their respective BigQuery counterparts (`project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_bpr_basis`). This typically involves using a data transfer service like BigQuery Data Transfer Service, Dataflow, or custom ETL processes.
4.  **Stored Procedure Deployment**:
    *   Deploy the BigQuery Stored Procedure by executing the `sql/stored_procedures/d_ausd_bp_ta_iccid_einzeln_proc.sql` script in the `project.dataset` BigQuery dataset.
5.  **IAM Permissions Configuration**:
    *   Grant the necessary Identity and Access Management (IAM) permissions to the Google Cloud service account or user that will be responsible for executing this stored procedure. Required permissions typically include:
        *   `bigquery.dataEditor` on `project.dataset` (or more granular `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.create` for specific tables).
        *   `bigquery.routines.get` and `bigquery.routines.execute` for the stored procedure.
        *   `bigquery.jobs.create` to run BigQuery jobs.
6.  **Scheduling Configuration**:
    *   Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer/Apache Airflow, or Google Cloud Workflows) to invoke the `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc` stored procedure with the required `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` parameters at the desired frequency.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and implementation as potential gaps or areas requiring further investigation/follow-up:

*   **Dynamic SQL in `DWPA_UTIL_SKRIPT.runstatement`**: The original Oracle SQL script referenced `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`. The exact functionality of this Oracle package call, especially if it executes complex dynamic SQL, was not fully detailed. The migrated BigQuery Stored Procedure assumes its core function was to execute the `INSERT ... SELECT` statement, which has been directly embedded. If `runstatement` performed other dynamic or complex operations, these would need to be identified and separately migrated or re-implemented in BigQuery.
*   **`v_TabName` and `p_wiederanlaufWert` Usage Context**: While `v_TabName` and `p_wiederanlaufWert` are now parameters and logged in the BigQuery Stored Procedure, their full original usage context within the `starteSQLSkript` function or other parts of the Oracle environment was not entirely clear from the provided source. The current migration assumes their primary role was for logging and parameter passing. Confirmation that no other critical logic depended on these variables in the original system is recommended.
*   **`gestern.ksh` Complexities**: The `gestern.ksh` script was used for date derivation. The migration assumes a straightforward "today" and "yesterday" calculation using `CURRENT_DATE()` and `DATE_SUB`. If `gestern.ksh` contained more complex business rules for date determination (e.g., skipping weekends, holidays, or specific business day logic), these rules were not captured and would need to be explicitly added to the BigQuery Stored Procedure.
*   **`trace.sql.cfg` Configuration**: The original Oracle SQL script referenced `start ../trace.sql.cfg`. The contents and purpose of this configuration file were not provided. While general logging has been migrated to BigQuery tables, any specific tracing or configuration defined in `trace.sql.cfg` would need to be reviewed and potentially replicated using BigQuery's native logging or other Google Cloud monitoring tools.
*   **Detailed Schema Mapping Validation**: Although DDLs are provided, a thorough review of data types and constraints between the original Oracle tables and the new BigQuery tables is crucial to ensure full functional equivalence and prevent data integrity issues. This includes checking for `NULL` vs. `NOT NULL` constraints, default values, and character set considerations.

## 6. Validation

To ensure the successful migration and correct functionality of the BigQuery job, the following validation steps should be performed:

1.  **Unit Testing (Stored Procedure Logic)**:
    *   **Parameter Validation**:
        *   Invoke `d_ausd_bp_ta_iccid_einzeln_proc` with missing required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`). Verify that the procedure returns an error message (e.g., `FEHLER: 0 E 193 Jobkennung`) and logs an entry to `project.dataset.job_error_log` with the correct error code and argument.
        *   Invoke with an invalid `p_Stichtag` format (e.g., '2023-01-01' instead of '01012023'). Verify error message and `job_error_log` entry.
    *   **Date Derivation**:
        *   Execute the procedure and inspect the `datum_heute` and `datum_gestern` columns in `project.dataset.job_run_result` to confirm they reflect the current date and previous day correctly.
    *   **`v_max_timecreated_datum` Derivation**:
        *   Populate `project.dataset.dwtk_meldungen` with test data, including a `job_kennung = 'BERT_DROP_TEMP_TABLE'` entry with a known `timecreated`. Execute the procedure and verify that `v_max_timecreated_datum` (implicitly used in the `CASE` statements for `STATUS`) is correctly derived.
2.  **Integration Testing (Data Transformation)**:
    *   **Prepare Test Data**: Load a representative sample of data from the original Oracle `DWTK_MELDUNGEN` and `SOF$TA_BPR_BASIS` tables into their BigQuery counterparts (`project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_bpr_basis`). Ensure this data covers various scenarios, including different `bpr_id` values and `valid_to` dates relative to `v_max_timecreated_datum`.
    *   **Execute Migrated Job**: Call the `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc` stored procedure with valid parameters.
    *   **Verify Target Data**:
        *   Query `project.dataset.sof_ta_iccid_einzeln` and compare its contents with the expected output generated by the original Oracle job using the *exact same input data*.
        *   Pay close attention to the `CASE` statement logic for `TN_STATUS`, `TC_STATUS`, `TB_STATUS`, and `MSx_STATUS` to ensure `L` (Lapsed) and `A` (Active) statuses are correctly assigned based on `valid_to` and `v_max_timecreated_datum`.
        *   Verify the `DECODE` to `CASE` conversions for `MSx_ICCID` and related fields are accurate.
    *   **Verify Run Results**:
        *   Check `project.dataset.job_run_result` for a successful entry corresponding to the execution.
        *   Confirm that the `record_count` in `job_run_result` matches the number of rows inserted into `project.dataset.sof_ta_iccid_einzeln` and, more importantly, matches the record count from the original Oracle job.

**"Passing" Criteria**:

A successful migration and validation means:
*   The `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc` stored procedure completes without any unhandled BigQuery errors.
*   For valid inputs, no entries are logged in `project.dataset.job_error_log`.
*   For invalid inputs, appropriate error messages are returned, and corresponding entries are correctly logged in `project.dataset.job_error_log`.
*   An entry is successfully recorded in `project.dataset.job_run_result` for each execution, containing accurate metadata and the correct `record_count`.
*   The data generated in `project.dataset.sof_ta_iccid_einzeln` is functionally identical to the data produced by the original Oracle job for the same input data, considering any necessary data type adjustments.

## 7. Rollback procedure

In the event that the migrated BigQuery job needs to be reverted to the original Oracle/KornShell implementation, follow these steps:

1.  **Halt BigQuery Job Execution**:
    *   Immediately stop any scheduled or manual executions of the `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc` stored procedure. Disable or delete the scheduler (e.g., Cloud Scheduler job, Cloud Composer DAG) that invokes the BigQuery procedure.
2.  **Re-enable Original Scheduling**:
    *   Re-enable the original scheduling mechanism for the `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh` script in the legacy environment.
3.  **Data Restoration (if necessary)**:
    *   Since the BigQuery job performs a `TRUNCATE TABLE` followed by an `INSERT`, the `project.dataset.sof_ta_iccid_einzeln` table is completely overwritten each run. If the BigQuery job ran and produced incorrect data that was consumed by downstream systems, you may need to:
        *   Restore the `SOF$TA_ICCID_EINZELN` table in the Oracle database from a backup taken *before* the BigQuery job's first production run.
        *   Communicate with downstream consumers of `SOF$TA_ICCID_EINZELN` to ensure they are aware of any data discrepancies and can revert to the correct state.
    *   If the BigQuery job only ran in a test environment or did not impact production data, this step might be less critical.
4.  **Decommission BigQuery Artifacts (Optional)**:
    *   If the rollback is deemed permanent, consider deleting the migrated BigQuery artifacts to avoid confusion and resource consumption:
        *   Drop the `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc` stored procedure.
        *   Drop the `project.dataset.sof_ta_iccid_einzeln`, `project.dataset.job_error_log`, and `project.dataset.job_run_result` tables.
        *   The source tables (`project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_bpr_basis`) should only be dropped if they are not used by any other migrated jobs or processes.