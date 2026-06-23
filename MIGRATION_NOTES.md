# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job identified by `run_id 5af228f1-3847-4cc6-9310-ed82ed19407c` and `seed_name vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh`.

The original job, consisting of a KornShell script (`k_ausd_v_ta_inv_def.ksh`) orchestrating an Oracle SQL script (`d_ausd_v_ta_inv_def.sql`), has been re-architected and migrated to Google BigQuery. The KornShell orchestration logic, including parameter handling, job management, and logging, has been translated into a BigQuery Stored Procedure. The Oracle SQL data transformation logic has also been converted into a separate BigQuery Stored Procedure. All associated data storage and logging mechanisms have been adapted for the BigQuery environment.

## 2. Generated artifacts

The migration process has generated the following BigQuery-specific artifacts:

*   **`ddl/isrpt_isbert_tables.sql`**
    *   **Role**: This DDL script defines the schema for all necessary tables in BigQuery. This includes:
        *   `job_status_log`: A new control table to track the execution status, parameters, and outcomes of BigQuery jobs, replacing the legacy shell-based logging and job management.
        *   `sof_ta_inv_def`: The target table for the main data transformation, migrated from `SOF$TA_INV_DEF`.
        *   `via`: A placeholder target table, whose full schema needs to be completed based on the original Oracle `MERGE` statement.
        *   `dwtk_meldungen`, `cds_ta_inv_definition`, `cds_ta_inv_cont_config`, `cds_ta_care_description`: Schemas for the source tables, assuming their data will be ingested into BigQuery.

*   **`sp/isrpt_isbert/sp_d_ausd_v_ta_inv_def.sql`**
    *   **Role**: This BigQuery Stored Procedure encapsulates the data transformation logic originally found in `d_ausd_v_ta_inv_def.sql`. It performs the `TRUNCATE` and `INSERT INTO sof_ta_inv_def` operations, translating Oracle-specific SQL constructs (e.g., `NVL`, date functions, `/*+ ... */` hints) to BigQuery SQL. It also includes a placeholder for the `MERGE` operation into the `via` table. It returns the number of records processed.

*   **`sp/isrpt_isbert/sp_k_ausd_v_ta_inv_def.sql`**
    *   **Role**: This BigQuery Stored Procedure serves as the primary orchestrator, replacing the `k_ausd_v_ta_inv_def.ksh` KornShell script. Its responsibilities include:
        *   Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`).
        *   Implementing job control logic (ignoring active jobs, deactivating old active jobs) using the `job_status_log` table.
        *   Deriving the `v_datum_str` from `dwtk_meldungen`.
        *   Calling the `sp_d_ausd_v_ta_inv_def` stored procedure to execute the core data transformation.
        *   Handling error conditions and logging job status, messages, and processed record counts to the `job_status_log` table.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration and Transformation**: The decision was made to split the KornShell script's orchestration logic and the Oracle SQL script's transformation logic into two distinct BigQuery Stored Procedures (`sp_k_ausd_v_ta_inv_def` and `sp_d_ausd_v_ta_inv_def`). This promotes modularity, reusability, and leverages BigQuery's native scripting capabilities for procedural logic.
    *   **Trade-off**: While a Cloud Composer (Airflow) DAG could have orchestrated the entire workflow, using BigQuery Stored Procedures directly simplifies deployment for jobs that are self-contained or part of a simpler scheduling mechanism. If this job becomes part of a complex, multi-step workflow, a Cloud Composer DAG can easily wrap the call to `sp_k_ausd_v_ta_inv_def`.

*   **Dedicated BigQuery Job Control Table**: A new `job_status_log` table was introduced in BigQuery to manage job state, logging, and job concurrency (ignoring/deactivating jobs).
    *   **Trade-off**: This replaces disparate shell-based logging, temporary files, and custom job status tracking with a centralized, queryable, and auditable mechanism within BigQuery, enhancing observability and maintainability.

*   **Data Ingestion for Oracle Sources**: Instead of using BigQuery external tables directly linked to Oracle (which can have performance and connectivity challenges for production ETL), the design assumes that all necessary Oracle source tables (`DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, `CDS$TA_CARE_DESCRIPTION`) will be replicated or ingested into BigQuery using dedicated data pipelines (e.g., DataStream, Dataflow).
    *   **Trade-off**: This adds an upstream dependency for data replication but provides better performance, reliability, and separation of concerns for the BigQuery transformation job.

*   **BigQuery-Native Error Handling**: The legacy `set -e` and `f_alis_msgerr.ksh` error handling are replaced by BigQuery's `RAISE ERROR` statements, `EXCEPTION WHEN ERROR` blocks, and detailed logging to the `job_status_log` table.
    *   **Trade-off**: This aligns with BigQuery's scripting paradigm, providing structured error reporting and allowing for robust recovery or notification mechanisms.

*   **Removal of Oracle-Specific Features**: Oracle-specific SQL hints (`/*+ full(id) ... parallel(id,4) */`), packages (`DWPA_UTIL_SKRIPT`, `ICC`), and control statements (`START`, `SPOOL`, `WHENEVER SQLERROR`, `COMMIT`) have been removed or replaced with BigQuery equivalents.
    *   **Trade-off**: This ensures full compatibility with BigQuery SQL but requires careful re-implementation of any business logic embedded within the Oracle packages.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Create BigQuery Dataset**: Ensure the target BigQuery dataset `my_gcp_project.isrpt_isbert` exists. If not, create it.
    ```bash
    bq mk --dataset my_gcp_project:isrpt_isbert
    ```
2.  **Deploy DDLs**: Execute the `ddl/isrpt_isbert_tables.sql` script to create the `job_status_log`, `sof_ta_inv_def`, `via`, and source tables in the `my_gcp_project.isrpt_isbert` dataset.
    ```bash
    bq query --use_legacy_sql=false < ddl/isrpt_isbert_tables.sql
    ```
3.  **Deploy Stored Procedures**: Deploy `sp/isrpt_isbert/sp_d_ausd_v_ta_inv_def.sql` and `sp/isrpt_isbert/sp_k_ausd_v_ta_inv_def.sql` to the `my_gcp_project.isrpt_isbert` dataset.
    ```bash
    bq query --use_legacy_sql=false < sp/isrpt_isbert/sp_d_ausd_v_ta_inv_def.sql
    bq query --use_legacy_sql=false < sp/isrpt_isbert/sp_k_ausd_v_ta_inv_def.sql
    ```
4.  **Establish Data Ingestion Pipelines**: Set up and verify continuous data ingestion pipelines (e.g., using Google Cloud DataStream, Dataflow, or Fivetran) to replicate data from the Oracle source tables (`DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, `CDS$TA_CARE_DESCRIPTION`) into their corresponding BigQuery tables in `my_gcp_project.isrpt_isbert`. Ensure data freshness and completeness.
5.  **IAM Permissions**: Grant appropriate BigQuery IAM roles to the service account or user that will execute the `sp_k_ausd_v_ta_inv_def` stored procedure. This typically includes:
    *   `BigQuery Data Editor` on `my_gcp_project.isrpt_isbert` (for `INSERT`, `UPDATE`, `TRUNCATE` operations).
    *   `BigQuery Job User` (to run queries and stored procedures).
6.  **Complete `VIA` Table Schema**: The `via` table DDL is currently a placeholder. The full schema must be derived from the original Oracle `MERGE` statement in `d_ausd_v_ta_inv_def.sql` and updated in `ddl/isrpt_isbert_tables.sql`.
7.  **Re-implement Oracle Package Logic**: Any business logic embedded within the Oracle packages `DWPA_UTIL_SKRIPT` and `ICC` that is critical for the job's functionality must be identified and re-implemented in BigQuery SQL or Python, and integrated into the relevant stored procedures or an orchestrating Airflow DAG.
8.  **Scheduling**: Configure a scheduler (e.g., BigQuery Scheduled Queries, Cloud Scheduler, or Cloud Composer/Airflow) to invoke `CALL my_gcp_project.isrpt_isbert.sp_k_ausd_v_ta_inv_def('YOUR_JOB_KENNUNG', 'YOUR_ENTRY_NR')` at the desired frequency.

## 5. Known gaps & unresolved references

The following items require further attention or are currently placeholders:

*   **`VIA` Table `MERGE` Logic**: The exact `MERGE` statement for the `VIA` table from `d_ausd_v_ta_inv_def.sql` was not fully provided in the source inventory. The `sp_d_ausd_v_ta_inv_def` stored procedure contains a `TODO` placeholder for this. This must be completed and verified.
*   **Complete Source Table Schemas**: The DDLs for `dwtk_meldungen`, `cds_ta_inv_definition`, `cds_ta_inv_cont_config`, and `cds_ta_care_description` include `TODO` comments to add all columns from their original Oracle counterparts. While the critical columns for this specific job are present, a complete schema is necessary for full data fidelity and future use.
*   **Oracle Package Functionality**: The full scope and business logic of Oracle packages `DWPA_UTIL_SKRIPT` and `ICC` are not fully known. Any critical functions within these packages that affect the data transformation or job control must be thoroughly analyzed and re-implemented in BigQuery or Python.
*   **Upstream Invoker Migration**: The upstream script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh` was identified as invoking the original KornShell script. This upstream invoker must also be analyzed and migrated to call the new BigQuery stored procedure, ensuring the end-to-end workflow is fully transitioned.
*   **Utility Script Logic**: While basic functionalities of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` have been addressed, a comprehensive review of these legacy utility scripts is recommended to ensure no subtle business logic or side effects were missed during the migration.
*   **Performance Tuning**: Oracle-specific query hints were removed. While BigQuery is highly optimized, post-migration performance testing and potential BigQuery-native query optimization (e.g., clustering, partitioning, appropriate data types) may be required.
*   **`is_production` Field Type**: The `is_production` field in `cds_ta_inv_definition` and `cds_ta_inv_cont_config` was assumed to be a `BOOL` in BigQuery. This should be verified against the original Oracle data type (e.g., `NUMBER(1)` for 0/1) and adjusted if necessary.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Deployment Verification**:
    *   Confirm that the `my_gcp_project.isrpt_isbert` dataset exists.
    *   Verify that all tables (`job_status_log`, `sof_ta_inv_def`, `via`, and source tables) are created with the correct schemas using `bq show --schema my_gcp_project:isrpt_isbert.<table_name>`.
    *   Confirm that `sp_d_ausd_v_ta_inv_def` and `sp_k_ausd_v_ta_inv_def` stored procedures are deployed using `bq show --routine my_gcp_project:isrpt_isbert.sp_k_ausd_v_ta_inv_def`.

2.  **Data Ingestion Verification**:
    *   Ensure that the BigQuery source tables (`dwtk_meldungen`, `cds_ta_inv_definition`, etc.) are populated with up-to-date data from Oracle. Check row counts and a sample of data against the Oracle source.

3.  **Execution and Logging Test**:
    *   Execute the main orchestration stored procedure with sample parameters:
        ```sql
        CALL `my_gcp_project.isrpt_isbert.sp_k_ausd_v_ta_inv_def`('TEST_JOB_001', 'ENTRY_001');
        ```
    *   Immediately after, attempt to run it again with the same parameters to test the "ignore active jobs" logic.
        ```sql
        CALL `my_gcp_project.isrpt_isbert.sp_k_ausd_v_ta_inv_def`('TEST_JOB_001', 'ENTRY_002'); -- Should be ignored
        ```
    *   Query the `job_status_log` table to observe the execution status:
        ```sql
        SELECT * FROM `my_gcp_project.isrpt_isbert.job_status_log` ORDER BY start_time DESC LIMIT 5;
        ```

4.  **Data Output Verification**:
    *   Query the target table `sof_ta_inv_def` and `via` (once its `MERGE` logic is complete) to verify the transformed data.
    *   Compare row counts and a statistically significant sample of data points in `sof_ta_inv_def` against the output of the legacy Oracle job for the same input data.
    *   Verify that the `rechn_inh_konfig_text` column is correctly populated from the joined `cds_ta_care_description`.

**"Passing" means**:

*   The `job_status_log` table shows the job `TEST_JOB_001` with `ENTRY_001` completed with `status = 'SUCCESS'` and a non-zero `records_processed` count (unless expected to be zero).
*   The subsequent run with `ENTRY_002` for `TEST_JOB_001` should have `status = 'IGNORED'` in the `job_status_log`.
*   The data in `my_gcp_project.isrpt_isbert.sof_ta_inv_def` (and `via`) is identical to the output produced by the legacy Oracle job for the same input data, both in terms of row count and data content.
*   Error scenarios (e.g., invalid parameters, data issues) are correctly caught, logged as `FAILED` in `job_status_log`, and the `message` field contains relevant error details.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop BigQuery Job Execution**: Immediately disable or remove any scheduled queries or Cloud Composer DAGs that invoke `sp_k_ausd_v_ta_inv_def`.
2.  **Revert Scheduling**: Reconfigure the legacy scheduling system (e.g., cron, enterprise scheduler) to resume execution of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh` script.
3.  **Data Reversion (if necessary)**:
    *   If the BigQuery job has modified data in `sof_ta_inv_def` or `via` that cannot be tolerated, and the legacy system needs to re-process the data from a clean state, a data rollback might be required. This could involve:
        *   Restoring `sof_ta_inv_def` and `via` tables in BigQuery from a snapshot or backup taken just before the BigQuery job's first execution.
        *   Alternatively, if the legacy system is capable of idempotent re-processing, simply letting the legacy job run might correct the data.
    *   **Crucially, assess the impact of the BigQuery job's data modifications before deciding on data reversion.** If the BigQuery job only writes to new tables or partitions, data reversion might not be necessary.
4.  **Monitor Legacy System**: Verify that the legacy job is running correctly and producing expected output after the rollback.
5.  **Optional: Clean Up BigQuery Artifacts**: If the migration is deemed unsuccessful and a re-migration is planned, or if the BigQuery artifacts are no longer needed, they can be dropped:
    ```bash
    bq rm --routine my_gcp_project:isrpt_isbert.sp_k_ausd_v_ta_inv_def
    bq rm --routine my_gcp_project:isrpt_isbert.sp_d_ausd_v_ta_inv_def
    bq rm my_gcp_project:isrpt_isbert.sof_ta_inv_def
    bq rm my_gcp_project:isrpt_isbert.via
    bq rm my_gcp_project:isrpt_isbert.job_status_log
    -- Only drop source tables if they are not used by other BigQuery processes
    bq rm my_gcp_project:isrpt_isbert.dwtk_meldungen
    bq rm my_gcp_project:isrpt_isbert.cds_ta_inv_definition
    bq rm my_gcp_project:isrpt_isbert.cds_ta_inv_cont_config
    bq rm my_gcp_project:isrpt_isbert.cds_ta_care_description
    ```