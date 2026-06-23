# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL workflow associated with the KornShell script `k_ausd_v_ta_discount_rr.ksh` and its dependent Oracle SQL script `d_ausd_v_ta_discount_rr.sql`. The original job's purpose is to prepare discount-related data for reporting by populating the `sof$ta_discount_rr` table from various `cds$` source tables within an Oracle environment.

The entire workflow has been re-platformed to **Google Cloud's BigQuery**. The KornShell orchestration logic has been translated into a BigQuery Stored Procedure, and the Oracle SQL transformation logic has been converted to BigQuery SQL. The target table `sof$ta_discount_rr` is now `curated_rpt.sof_ta_discount_rr` in BigQuery.

## 2. Generated Artifacts

The migration process generated the following BigQuery-compliant artifacts:

*   **`sql/ddl/raw_isbert.dwtk_meldungen.sql`**: BigQuery DDL for the `dwtk_meldungen` table, used to determine the processing date. This table resides in the `raw_isbert` dataset.
*   **`sql/ddl/raw_isbert.cds_ta_discount_bc_assoc.sql`**: BigQuery DDL for the `cds_ta_discount_bc_assoc` source table in the `raw_isbert` dataset.
*   **`sql/ddl/raw_isbert.cds_ta_discount.sql`**: BigQuery DDL for the `cds_ta_discount` source table in the `raw_isbert` dataset.
*   **`sql/ddl/raw_isbert.cds_ta_care_description.sql`**: BigQuery DDL for the `cds_ta_care_description` source table in the `raw_isbert` dataset.
*   **`sql/ddl/raw_isbert.cds_ta_disc_vector.sql`**: BigQuery DDL for the `cds_ta_disc_vector` source table in the `raw_isbert` dataset.
*   **`sql/ddl/raw_isbert.cds_ta_disc_invoice_item.sql`**: BigQuery DDL for the `cds_ta_disc_invoice_item` source table in the `raw_isbert` dataset.
*   **`sql/ddl/curated_rpt.sof_ta_discount_rr.sql`**: BigQuery DDL for the target table `sof_ta_discount_rr`, residing in the `curated_rpt` dataset.
*   **`sql/ddl/control_tables.job_table.sql`**: BigQuery DDL for a control table (`job_table`) used to track the execution status, start/end times, and record counts of the migrated job. This table resides in a designated `your_dataset` for control tables.
*   **`sql/ddl/control_tables.job_error_log.sql`**: BigQuery DDL for an error logging table (`job_error_log`) to capture any errors or warnings during job execution. This table resides in a designated `your_dataset` for control tables.
*   **`sql/d_ausd_v_ta_discount_rr_bq.sql`**: This file contains the core BigQuery SQL `INSERT INTO ... SELECT` statement, translated from the original Oracle SQL. It is designed to be embedded or called from within the main control stored procedure.
*   **`sql/sprocs/control_ausd_v_ta_discount_rr.sql`**: The main BigQuery Stored Procedure, `control_ausd_v_ta_discount_rr`, which encapsulates the entire orchestration logic previously handled by `k_ausd_v_ta_discount_rr.ksh`. This includes parameter handling, job status updates, date determination, target table truncation, execution of the core transformation logic, record counting, and error logging.

## 3. Key Design Decisions

*   **Orchestration Re-platforming**: The KornShell script's orchestration logic was migrated to a BigQuery Stored Procedure (`control_ausd_v_ta_discount_rr`). This centralizes the job control within BigQuery, leveraging its native procedural capabilities and eliminating external shell dependencies.
*   **Direct SQL Translation**: The core Oracle SQL transformation logic was directly translated into BigQuery SQL. This approach minimizes changes to the business logic, ensuring functional equivalence while benefiting from BigQuery's performance and scalability.
*   **BigQuery Native Job Control & Logging**: Instead of relying on shell scripts for logging and job status management, dedicated BigQuery tables (`job_table`, `job_error_log`) were introduced. This provides a centralized, queryable, and BigQuery-native mechanism for monitoring job execution and errors.
*   **Data Layer Separation**: Source Oracle tables are ingested into a `raw_isbert` dataset in BigQuery, preserving their original schema. The target table resides in a `curated_rpt` dataset, clearly delineating raw ingested data from processed, curated data.
*   **Elimination of Temporary Files**: The original KornShell script used temporary files to capture record counts. In BigQuery, this is replaced by direct `COUNT(*)` queries and variable assignments within the stored procedure, simplifying the workflow and improving reliability.
*   **Oracle-Specific Feature Replacement**:
    *   Oracle `NVL` was replaced with BigQuery `IFNULL`.
    *   Oracle `TO_DATE` and `TO_CHAR` date formatting functions were replaced with BigQuery `PARSE_TIMESTAMP` and `FORMAT_DATE`.
    *   Oracle `TRUNCATE TABLE` via `DWPA_UTIL_SKRIPT` was replaced with a direct BigQuery `TRUNCATE TABLE` statement.
    *   Oracle `DEFINE` variables and `COLUMN ... NEW_VALUE` were replaced by `DECLARE` and `SET` statements for variables within the stored procedure.
    *   Oracle SQL*Plus specific commands (`SPOOL`, `SET TIMING ON`, `WHENEVER SQLERROR`) were removed, as BigQuery's execution environment handles these aspects differently (e.g., Cloud Logging for execution details).
*   **Explicit Joins**: Oracle's implicit join syntax was converted to explicit `INNER JOIN` clauses for better readability and maintainability in BigQuery.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Creation**:
    *   Ensure a Google Cloud Project is set up.
    *   Create the following BigQuery datasets (replace `your_project` and `your_dataset` with actual project and dataset IDs):
        *   `your_project.raw_isbert`
        *   `your_project.curated_rpt`
        *   `your_project.your_dataset` (for control tables)

2.  **BigQuery Table Creation (DDL Deployment)**:
    *   Execute all DDL scripts provided in the `sql/ddl/` directory to create the necessary source, target, and control tables in their respective datasets.
        *   `raw_isbert.dwtk_meldungen`
        *   `raw_isbert.cds_ta_discount_bc_assoc`
        *   `raw_isbert.cds_ta_discount`
        *   `raw_isbert.cds_ta_care_description`
        *   `raw_isbert.cds_ta_disc_vector`
        *   `raw_isbert.cds_ta_disc_invoice_item`
        *   `curated_rpt.sof_ta_discount_rr`
        *   `your_dataset.job_table`
        *   `your_dataset.job_error_log`

3.  **Initial Data Ingestion for `raw_isbert` Tables**:
    *   Set up and execute a data ingestion pipeline (e.g., using Cloud Data Fusion, Data Transfer Service, or custom ETL) to load historical and ongoing data from the Oracle source tables into their corresponding BigQuery `raw_isbert` tables. This is a critical prerequisite for the job to function correctly.

4.  **BigQuery Stored Procedure Deployment**:
    *   Execute the `sql/sprocs/control_ausd_v_ta_discount_rr.sql` script to create the main control stored procedure in the `your_project.your_dataset` dataset.

5.  **IAM Permissions Configuration**:
    *   Ensure the service account or user executing the BigQuery stored procedure has the following IAM roles:
        *   `BigQuery Data Editor` on `your_project.raw_isbert` dataset (to read source tables).
        *   `BigQuery Data Editor` on `your_project.curated_rpt` dataset (to truncate and insert into the target table).
        *   `BigQuery Data Editor` on `your_project.your_dataset` dataset (to insert/update control tables and execute the stored procedure).
        *   `BigQuery Job User` (to run BigQuery jobs).

6.  **Scheduling Configuration**:
    *   If the job is part of a larger workflow, configure the external scheduler (e.g., Cloud Composer DAG, Cloud Workflows, or BigQuery Scheduled Queries) to invoke the `your_project.your_dataset.control_ausd_v_ta_discount_rr` stored procedure with the required parameters (`p_JobKennung`, `p_EintragsNr`).

## 5. Known Gaps & Unresolved References

*   **Unclear `ZWISCHEN` and `VIA` Table Usage**: The original lineage analysis indicated reads from `TABLE:ZWISCHEN` and writes to `TABLE:VIA` for `d_ausd_v_ta_discount_rr.sql`, but these tables were not found in the provided SQL code. Their purpose and whether they are temporary, views, or external dependencies remain unclear. Further investigation is required to ensure no data loss or incorrect processing.
*   **Full Scope of `isbert_schema.DWPA_UTIL_SKRIPT`**: Only the `TRUNCATE TABLE` functionality of this Oracle package was explicitly handled. If `DWPA_UTIL_SKRIPT` contains other critical or complex logic, it needs to be fully analyzed and re-implemented in BigQuery.
*   **Complex `f_alis_msgerr.ksh` Logic**: The original error reporting and handling in `f_alis_msgerr.ksh` might have intricate logic beyond basic logging. The current BigQuery error handling provides basic error messages and logging to `job_error_log`. A detailed comparison might be needed if specific error codes or advanced reporting mechanisms are required.
*   **External Orchestration (`r_ausd_v_ta_discount_rr.ksh`)**: The migration focused on the immediate job. If `r_ausd_v_ta_discount_rr.ksh` or any other parent job invokes this script, that parent job will also need to be migrated or adapted to call the new BigQuery stored procedure.
*   **Placeholder Project/Dataset Names**: The generated code uses `your_project` and `your_dataset` as placeholders. These must be replaced with the actual Google Cloud Project ID and BigQuery Dataset IDs during deployment.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Execute the Stored Procedure**:
    *   Manually execute the `control_ausd_v_ta_discount_rr` stored procedure in the BigQuery console or via a BigQuery client, providing sample `p_JobKennung` and `p_EintragsNr` parameters.
    *   If using Cloud Composer, trigger the corresponding DAG.

2.  **Check Job Status and Logs**:
    *   **Passing Criteria**:
        *   The stored procedure execution completes without errors.
        *   Query `your_project.your_dataset.job_table` for the `run_id` of the execution. The `status` column should be `'COMPLETED'`, and `record_count` should reflect the number of rows inserted into `curated_rpt.sof_ta_discount_rr` (and ideally be greater than 0).
        *   Query `your_project.your_dataset.job_error_log` for the `run_id`. There should be no entries for successful runs, or only expected `WARNING` messages (e.g., for default date values).

3.  **Data Validation**:
    *   **Passing Criteria**:
        *   Compare the data in `your_project.curated_rpt.sof_ta_discount_rr` with the expected output from the original Oracle `sof$ta_discount_rr` table for the same input data and processing date.
        *   Verify data types, column values, and row counts match between source and target.
        *   Perform a row-by-row comparison or aggregate checks (e.g., `SUM`, `COUNT`, `AVG`) on key columns.

4.  **Performance Testing**:
    *   Monitor the execution time and BigQuery slot consumption of the stored procedure. Ensure it meets or exceeds the performance of the original Oracle job.

## 7. Rollback Procedure

In case of critical issues or failure to meet validation criteria, the following rollback procedure can be initiated:

1.  **Stop New Job Execution**:
    *   Immediately disable or pause any scheduled executions of the `control_ausd_v_ta_discount_rr` BigQuery stored procedure (e.g., disable the Cloud Composer DAG or BigQuery Scheduled Query).

2.  **Reactivate Original Job**:
    *   Re-enable the original KornShell script (`k_ausd_v_ta_discount_rr.ksh`) in the Oracle environment. Ensure its scheduler is reactivated.

3.  **Data Cleanup (Optional but Recommended)**:
    *   If the BigQuery target table `your_project.curated_rpt.sof_ta_discount_rr` contains erroneous data from the failed migration, truncate it:
        ```sql
        TRUNCATE TABLE `your_project.curated_rpt.sof_ta_discount_rr`;
        ```
    *   If the `raw_isbert` tables were affected or need to be reset, re-ingest data from the Oracle sources.

4.  **Revert BigQuery Artifacts (Optional)**:
    *   If the migration is deemed permanently unsuccessful or requires a complete redesign, the deployed BigQuery DDLs and stored procedures can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project.your_dataset.control_ausd_v_ta_discount_rr`;
        DROP TABLE IF EXISTS `your_project.curated_rpt.sof_ta_discount_rr`;
        -- ... and other DDLs if necessary, but usually only target and control tables are dropped.
        ```
    *   This step should only be performed if there's no intention to re-attempt the migration with the current artifacts.