# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh`. This script, responsible for orchestrating the initial provisioning of the "Vertrags-Cache" for the "Forderungsscoring" (FOS) system, has been migrated from a shell-based environment to Google Cloud's BigQuery platform.

The original KornShell orchestrator and its invoked core processing script (`k_ausd_rechempf.ksh`) have been re-implemented as BigQuery Stored Procedures. File-based logging and error handling have been replaced with BigQuery-native logging tables.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **DDL for Logging and Audit Tables (Manual Step)**
    *   `project.dataset.job_log`: Table to store job execution metadata (start/end times, status, parameters).
    *   `project.dataset.job_error_log`: Table to log detailed error information, including messages and stack traces.
    *   `project.dataset.job_log_messages`: Table for general informational and success messages generated during job execution.

*   **`ddl/project.dataset.sof_ta_means_of_pay.sql`**
    *   **Role**: DDL for creating the `sof_ta_means_of_pay` table. This table serves as an intermediate staging area, derived from the `bpd_ta_means_of_payment` source, used within the core processing logic.

*   **`ddl/project.dataset.sof_ta_bank.sql`**
    *   **Role**: DDL for creating the `sof_ta_bank` table. This table is another intermediate staging area, combining data from `bpd_ta_bank` and `bpd_ta_bank_international`, used within the core processing logic.

*   **`ddl/project.dataset.sof_ta_bank_verb.sql`**
    *   **Role**: DDL for creating the `sof_ta_bank_verb` table. This is an intermediate table that joins `sof_ta_means_of_pay` and `sof_ta_bank` to enrich means of payment data with bank details.

*   **`ddl/project.dataset.sof_ta_bank_zuord.sql`**
    *   **Role**: DDL for creating the `sof_ta_bank_zuord` table. This intermediate table further refines bank assignment data by joining `sof_ta_bank_verb` with `sof_ta_e_regulierer`.

*   **`ddl/project.dataset.sof_ta_p_rech_empf.sql`**
    *   **Role**: DDL for creating the `sof_ta_p_rech_empf` table. This is a primary target table for the migrated job, storing the processed "Rechnungsempfänger" (invoice recipient) data.

*   **`ddl/project.dataset.sof_ta_p_d1_vpn.sql`**
    *   **Role**: DDL for creating the `sof_ta_p_d1_vpn` table. This is another target table, storing "Vertrags-ID" (contract ID) to "VPN-ID" mappings for specific product types.

*   **`stored_procedures/sp_k_ausd_rechempf.sql`**
    *   **Role**: BigQuery Stored Procedure that encapsulates the core data processing logic originally found in `k_ausd_rechempf.ksh`. It performs data extraction, transformation, and loading into the `sof_ta_p_rech_empf` and `sof_ta_p_d1_vpn` target tables, utilizing the intermediate tables.

*   **`stored_procedures/sp_initial_befuellung_vertrags_cache_fos.sql`**
    *   **Role**: BigQuery Stored Procedure that replaces the `r_ausd_rechempf.ksh` orchestrator. It handles parameter parsing, defaulting, logging, error trapping, and orchestrates the execution of `sp_k_ausd_rechempf`.

## 3. Key design decisions

The migration to BigQuery involved several key design decisions:

*   **Orchestrator Re-implementation as BigQuery Stored Procedure**:
    *   **Decision**: The `r_ausd_rechempf.ksh` KornShell script was directly translated into a BigQuery Stored Procedure (`sp_initial_befuellung_vertrags_cache_fos`).
    *   **Rationale**: This approach leverages BigQuery's native procedural capabilities for parameter handling, conditional logic, and error management, eliminating the need for an external shell environment. It simplifies deployment and integrates seamlessly with BigQuery's execution model.
    *   **Trade-offs**: Increased reliance on BigQuery SQL for control flow, which can be less flexible than shell scripting for certain system-level operations (e.g., file manipulation, external process calls).

*   **Core Processing Logic as Separate BigQuery Stored Procedure**:
    *   **Decision**: The logic of the invoked `k_ausd_rechempf.ksh` script was encapsulated in a separate BigQuery Stored Procedure (`sp_k_ausd_rechempf`).
    *   **Rationale**: This modular approach separates orchestration from core data processing, improving readability, maintainability, and reusability. It allows the data-intensive operations to run entirely within BigQuery, benefiting from its performance and scalability.

*   **BigQuery-Native Logging and Error Handling**:
    *   **Decision**: File-based logging and shell `trap` mechanisms were replaced by `INSERT` statements into dedicated BigQuery tables (`job_log`, `job_error_log`, `job_log_messages`) and BigQuery's `EXCEPTION WHEN ERROR` blocks.
    *   **Rationale**: Centralizes logging, making it queryable, auditable, and easily integrated with GCP monitoring tools (e.g., Cloud Logging, Cloud Monitoring). Provides structured error information, including stack traces, which is superior to simple shell error codes.

*   **Shell Helper Scripts to Inline Logic/UDFs**:
    *   **Decision**: Functionality from shell helper scripts (e.g., `h_alis_date.ksh`, `h_alis_parameter.ksh`) was translated into BigQuery's built-in functions, inline SQL logic, or procedural statements within the stored procedures.
    *   **Rationale**: Eliminates external dependencies on shell scripts, making the solution self-contained within BigQuery. Simplifies the deployment and execution environment.

*   **Parameter Handling**:
    *   **Decision**: Shell `getopts` and environment variable parsing were replaced by standard BigQuery Stored Procedure input parameters.
    *   **Rationale**: Provides a clear, type-safe interface for job execution, consistent with BigQuery's procedural language.

*   **Intermediate Tables for Staging**:
    *   **Decision**: The migration uses persistent BigQuery tables (`sof_ta_means_of_pay`, `sof_ta_bank`, `sof_ta_bank_verb`, `sof_ta_bank_zuord`) as intermediate staging areas.
    *   **Rationale**: While BigQuery supports temporary tables, using persistent tables for intermediate steps can aid in debugging, allow for inspection of intermediate results, and potentially be more robust for complex multi-step transformations, especially if the original script implied similar staging.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it.
    *   Ensure the source BigQuery dataset (`source_project.source_dataset`) exists and contains the necessary source tables.

2.  **IAM Permissions**:
    *   The service account or user executing the BigQuery Stored Procedures must have appropriate IAM roles and permissions. This typically includes:
        *   `bigquery.jobs.create` (to run queries and procedures)
        *   `bigquery.tables.create` (to create new tables, if not pre-created)
        *   `bigquery.tables.updateData` (to insert/update data in target and log tables)
        *   `bigquery.tables.getData` (to read from source and intermediate tables)
        *   `bigquery.routines.create` (to create stored procedures)
        *   `bigquery.routines.update` (to update stored procedures)

3.  **Logging and Audit Tables DDL Deployment**:
    *   Execute the DDL statements for the `project.dataset.job_log`, `project.dataset.job_error_log`, and `project.dataset.job_log_messages` tables in the target BigQuery dataset. These are crucial for monitoring and debugging the migrated job.

4.  **Source Data Availability**:
    *   Verify that all source tables referenced in `sp_k_ausd_rechempf` (e.g., `source_project.source_dataset.bpd_ta_means_of_payment`, `bpd_ta_bank`, `bpd_ta_bank_international`, `sof_ta_e_regulierer`, `sof_ta_e_reach_re`, `sof_ta_e_business_re`, `dwh_vi_s_ibasisprodukt`) exist in the `source_project.source_dataset` and are populated with the expected data. Any schema differences between the original Oracle sources and their BigQuery counterparts must be reconciled.

5.  **Deployment of Generated DDLs and Stored Procedures**:
    *   Execute all `CREATE TABLE IF NOT EXISTS` statements for the intermediate and target tables (`sof_ta_means_of_pay`, `sof_ta_bank`, `sof_ta_bank_verb`, `sof_ta_bank_zuord`, `sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`).
    *   Execute the `CREATE OR REPLACE PROCEDURE` statements for `sp_k_ausd_rechempf` and `sp_initial_befuellung_vertrags_cache_fos`.

6.  **Scheduling Configuration**:
    *   If the job is to be scheduled, configure a new scheduler (e.g., Google Cloud Composer/Apache Airflow, Cloud Workflows, or BigQuery Scheduled Queries) to invoke `project.dataset.sp_initial_befuellung_vertrags_cache_fos` at the desired frequency and with the correct parameters.

## 5. Known gaps & unresolved references

*   **`k_ausd_rechempf.ksh` Logic Analysis (B4 Item)**: The migration design document explicitly flagged the core data processing logic of `k_ausd_rechempf.ksh` as a critical unresolved component. While `sp_k_ausd_rechempf` has been generated based on an assumed translation, a thorough review of the original `k_ausd_rechempf.ksh` script is required to ensure all nuances of data sources, transformation logic, and target table interactions are correctly captured in the BigQuery Stored Procedure. This is the highest priority for follow-up.
*   **Legacy Data Sources**: The exact schemas and data types of the original Oracle source tables (e.g., `bpd_ta_means_of_payment`, `bpd_ta_bank`, `sof_ta_e_regulierer`, `dwh_vi_s_ibasisprodukt`) are assumed in the generated DDLs and SQL. A detailed comparison and validation against the actual source system schemas are necessary to prevent data type mismatches or missing columns.
*   **`BERT_DROP_TEMP_TABLE` Reference**: In `sp_k_ausd_rechempf`, the logic for `v_datum_from_log` references `project.dataset.job_log` with `job_id = 'BERT_DROP_TEMP_TABLE'`. This appears to be a placeholder or a legacy concept from the original environment. It needs clarification on what `BERT_DROP_TEMP_TABLE` represents in the BigQuery context and if this logic is still relevant for determining a cutoff date. If it's meant to track the last successful run of *this* job, the `job_id` condition should be adjusted accordingly (e.g., `t2.job_kennung = p_job_kennung`).
*   **Historical `MIN(sysdate,maxladedatum)` Logic**: The original design document mentioned a commented-out logic for deriving `p_stichtag` from `MIN(sysdate, maxladedatum)`. This logic is not implemented in the generated BigQuery procedures. If this was a business requirement that was previously dormant but is now desired, it needs to be explicitly added to `sp_initial_befuellung_vertrags_cache_fos`.
*   **Environment Variables (`BERT_DIR_ROOT`)**: The original script relied on environment variables for path resolution. In the BigQuery context, these have been implicitly handled by direct references to BigQuery datasets and tables. If any other environment-specific configurations were present, they need to be translated into BigQuery constants, parameters, or configuration tables.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`**: The original script used this Oracle-specific utility for truncating tables. This has been replaced by direct `TRUNCATE TABLE` statements in BigQuery. This change is generally acceptable but highlights the need to ensure all Oracle-specific DDL/DML operations have been correctly translated.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Execute the Main Orchestration Procedure**:
    *   Manually trigger the main stored procedure in BigQuery:
        ```sql
        CALL project.dataset.sp_initial_befuellung_vertrags_cache_fos(NULL, NULL);
        ```
    *   To test specific scenarios, provide parameters:
        ```sql
        CALL project.dataset.sp_initial_befuellung_vertrags_cache_fos('01012023', 12345);
        ```
        (where '01012023' is `p_stichtag` and 12345 is `p_wiederanlauf_wert`)

2.  **Monitor Logging Tables**:
    *   Query `project.dataset.job_log` to check the overall job status.
    *   Query `project.dataset.job_log_messages` for informational messages and progress.
    *   Query `project.dataset.job_error_log` to identify any errors.

3.  **Verify Target Data**:
    *   After successful execution, query the target tables:
        *   `SELECT COUNT(*) FROM project.dataset.sof_ta_p_rech_empf;`
        *   `SELECT COUNT(*) FROM project.dataset.sof_ta_p_d1_vpn;`
    *   Perform data validation by comparing a sample of records or aggregate counts from the BigQuery target tables against the output generated by the original `r_ausd_rechempf.ksh` script in the legacy environment. This is the most critical validation step.
    *   Ensure data types, formats, and content match expectations.

**"Passing" Criteria**:

*   The `project.dataset.job_log` table shows an entry for the executed job with `status = 'OK'` and a valid `end_time`.
*   There are no corresponding entries for the `job_id` in `project.dataset.job_error_log`.
*   The target tables (`project.dataset.sof_ta_p_rech_empf` and `project.dataset.sof_ta_p_d1_vpn`) are populated with data.
*   The data in the target tables is accurate and consistent with the output of the legacy system for the same input parameters and source data state. This includes record counts, specific field values, and overall data integrity.

## 7. Rollback procedure

In case of issues or failure during go-live, the following rollback procedure should be followed:

1.  **Stop New Executions**:
    *   Immediately disable or remove any new scheduling configurations (e.g., Cloud Composer DAGs, Cloud Workflows, BigQuery Scheduled Queries) that trigger `sp_initial_befuellung_vertrags_cache_fos`.

2.  **Revert Scheduling to Legacy System**:
    *   Re-enable or restore the original scheduling mechanism for `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh` in the legacy environment.

3.  **Data Restoration (if necessary)**:
    *   If the BigQuery target tables (`sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`) were intended to replace existing production tables and data corruption occurred, restore these tables from the most recent backup.
    *   *Note*: In this migration, new tables are created. If the legacy system continues to use its original target tables, no data restoration is needed for the legacy system. However, any downstream systems that might have started consuming data from the new BigQuery tables would need to be reverted to their original data sources.

4.  **Clean Up BigQuery Artifacts (Optional but Recommended)**:
    *   Drop the newly created BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.sp_initial_befuellung_vertrags_cache_fos;
        DROP PROCEDURE IF EXISTS project.dataset.sp_k_ausd_rechempf;
        ```
    *   Drop the newly created intermediate and target tables:
        ```sql
        DROP TABLE IF EXISTS project.dataset.sof_ta_p_rech_empf;
        DROP TABLE IF EXISTS project.dataset.sof_ta_p_d1_vpn;
        DROP TABLE IF EXISTS project.dataset.sof_ta_bank_zuord;
        DROP TABLE IF EXISTS project.dataset.sof_ta_bank_verb;
        DROP TABLE IF EXISTS project.dataset.sof_ta_bank;
        DROP TABLE IF EXISTS project.dataset.sof_ta_means_of_pay;
        ```
    *   (Optional) Drop the logging tables if they are not used by other migrated jobs:
        ```sql
        DROP TABLE IF EXISTS project.dataset.job_log;
        DROP TABLE IF EXISTS project.dataset.job_error_log;
        DROP TABLE IF EXISTS project.dataset.job_log_messages;
        ```

5.  **Root Cause Analysis**:
    *   Investigate the cause of the failure using the `job_error_log` and `job_log_messages` tables, BigQuery job history, and any other available logs. Rectify the issues before attempting re-migration or re-deployment.