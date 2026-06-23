# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_bp_ta_rn_da_vda_tk.ksh` job, originally composed of three KornShell and Oracle SQL scripts, from its legacy Oracle/KornShell environment to Google BigQuery.

The original job was responsible for provisioning selected basic product data for the BERT system, extracting contract cache data from the Data Warehouse (DWH) based on a snapshot date, and preparing it for "Forderungsscoring" (claims scoring).

The migration targets Google BigQuery, where the orchestration logic (previously in KornShell) is refactored into BigQuery stored procedures, and the core data processing logic (previously in Oracle SQL) is directly translated into BigQuery SQL within a stored procedure. All associated Oracle tables are migrated to native BigQuery tables.

## 2. Generated Artifacts

The migration process generated the following BigQuery DDL and stored procedure files:

*   **`ddl/dwtk_meldungen.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `dwtk_meldungen` table. This table serves as the BigQuery equivalent of the Oracle `isbert_schema.dwtk_meldungen` table, used for determining date variables.
*   **`ddl/sof_ta_rn_einzeln.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_rn_einzeln` table. This table is the BigQuery equivalent of the Oracle `sof$ta_rn_einzeln` table, which acts as the primary source for data selection.
*   **`ddl/sof_ta_rn_da_vda_tk.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_rn_da_vda_tk` table. This table is the BigQuery equivalent of the Oracle `sof$ta_rn_da_vda_tk` table, serving as the target for the processed data. Its schema is designed to match `sof_ta_rn_einzeln`.
*   **`ddl/job_audit_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_audit_log` table. This table is a new artifact designed to centralize logging and auditing of job executions, replacing the custom shell-based logging mechanisms.
*   **`sp/sp_ausd_bp_ta_rn_da_vda_tk.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the core data processing logic. It directly translates the Oracle SQL from `d_ausd_bp_ta_rn_da_vda_tk.sql`, including determining a date, truncating the target table, and inserting filtered data.
*   **`sp/sp_bereitstellung_basisprodukte_bert.sql`**
    *   **Role:** BigQuery Stored Procedure that serves as the top-level orchestrator. It replaces the functionality of `r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh`, handling parameter validation, defaulting, job audit logging, and invoking the core data processing procedure (`sp_ausd_bp_ta_rn_da_vda_tk`).

## 3. Key Design Decisions

The migration strategy focused on leveraging BigQuery's native capabilities for data processing and orchestration, aiming for a cloud-native, scalable, and maintainable solution.

*   **BigQuery Stored Procedures for Orchestration:** The original KornShell scripts (`r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh`) were refactored into a single BigQuery stored procedure (`sp_bereitstellung_basisprodukte_bert`). This decision centralizes the job's control flow within BigQuery, eliminating external script dependencies and enabling direct integration with BigQuery's execution environment. It allows for robust parameter handling, error management, and logging using BigQuery Scripting.
*   **Direct SQL Translation to BigQuery Stored Procedure:** The core Oracle SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) was directly converted into BigQuery SQL within a dedicated stored procedure (`sp_ausd_bp_ta_rn_da_vda_tk`). This approach minimizes logical changes to the data transformation, ensuring functional equivalence while benefiting from BigQuery's optimized query engine. Oracle-specific syntax (e.g., `NVL`, `TO_CHAR`, `TRUNCATE` via `DWPA_UTIL_SKRIPT`, SQL*Plus commands, hints) was translated to BigQuery equivalents (`COALESCE`, `FORMAT_DATE`, `TRUNCATE TABLE` DDL, removal of hints).
*   **Centralized Audit Logging:** The custom shell-based `DWMSG_*` logging functions were replaced by inserts into a new `job_audit_log` BigQuery table. This provides a standardized, queryable, and persistent record of job executions, statuses, and messages, significantly improving observability and troubleshooting capabilities compared to file-based logs.
*   **Robust Error Handling:** BigQuery Scripting's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks were implemented to replace the shell script's `trap` mechanisms. This ensures that errors are caught, logged to the `job_audit_log` table, and re-raised to inform the caller, providing a more structured and reliable error management framework.
*   **Parameter Handling and Defaults:** Input parameters (`p_stichtag`, `p_wiederanlaufWert`) are explicitly defined in the stored procedure signatures. Defaulting logic (e.g., using `CURRENT_DATE()` for `p_stichtag` if not provided) is implemented using `IFNULL(NULLIF(...), ...)`, mirroring the original script's behavior.
*   **Trade-offs:**
    *   **Loss of Shell Script Flexibility:** The migration to BigQuery stored procedures means that any file system operations or complex external command orchestrations (like the commented-out `sed`, `sort`, `join` commands) would require a different approach (e.g., Cloud Storage with Dataflow or Cloud Functions) if they were to become active. For this job, which is primarily database-centric, this trade-off is acceptable.
    *   **Oracle-Specific Optimizations:** Oracle hints (e.g., `/*+ full(rp) parallel(rp,4) */`) were removed as BigQuery's query optimizer automatically handles parallelism and execution plans, making such hints unnecessary and potentially counterproductive.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure the target Google Cloud Project (`my_project`) and BigQuery Dataset (`my_dataset`) exist. If not, create them.
2.  **IAM Permissions:**
    *   Grant the service account that will execute the BigQuery stored procedures the necessary IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` (to create/truncate/insert into tables).
        *   `BigQuery Job User` on `my_project` (to run BigQuery jobs, including stored procedures).
3.  **Deploy DDLs:**
    *   Execute the DDL scripts to create the necessary tables in BigQuery:
        *   `ddl/dwtk_meldungen.sql`
        *   `ddl/sof_ta_rn_einzeln.sql`
        *   `ddl/sof_ta_rn_da_vda_tk.sql`
        *   `ddl/job_audit_log.sql`
    *   *Note:* The DDLs use `CREATE TABLE IF NOT EXISTS`, so re-running them will not cause errors if tables already exist.
4.  **Deploy Stored Procedures:**
    *   Execute the stored procedure scripts to create or replace the procedures in BigQuery:
        *   `sp/sp_ausd_bp_ta_rn_da_vda_tk.sql`
        *   `sp/sp_bereitstellung_basisprodukte_bert.sql`
    *   *Note:* The procedures use `CREATE OR REPLACE PROCEDURE`, so re-running them will update existing procedures.
5.  **Initial Data Ingestion:**
    *   Perform an initial, full historical load of data from the Oracle source tables (`isbert_schema.dwtk_meldungen` and `sof$ta_rn_einzeln`) into their respective BigQuery target tables (`my_project.my_dataset.dwtk_meldungen` and `my_project.my_dataset.sof_ta_rn_einzeln`). This can be done using tools like BigQuery Data Transfer Service, Dataflow, or custom ETL scripts.
6.  **Scheduling Configuration:**
    *   Set up a scheduler (e.g., Cloud Composer/Airflow, Cloud Workflows, or Dataform) to invoke the top-level orchestration stored procedure `my_project.my_dataset.sp_bereitstellung_basisprodukte_bert` at the required frequency. Ensure parameters (`p_stichtag`, `p_wiederanlaufWert`) are passed correctly.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration and require further consideration or are noted as potential areas for future work:

*   **Case Sensitivity:** While BigQuery is generally case-sensitive for identifiers, the migration assumed consistent naming or handled explicit quoting where necessary. If the original Oracle schema had mixed-case identifiers referenced inconsistently, this could lead to issues. This should be verified during testing.
*   **Commented-out File Processing:** The original `k_ausd_bp_ta_rn_da_vda_tk.ksh` script contained commented-out sections for file-based data reformatting and joining (`sed`, `sort`, `join`). These were not migrated as they are inactive. If these functionalities become active in the future, they would require a separate migration effort, likely involving Cloud Storage and services like Dataflow or Cloud Functions.
*   **`v_carmen` Variable:** The Oracle `DEFINE v_carmen = "@pcrs1"` was removed as it's an SQL*Plus specific definition. If `v_carmen` represented a connection string or schema that was dynamically used in the Oracle SQL in ways not captured by the direct table references, its absence might be a gap. Based on the provided SQL, it did not appear to be actively used in the core query.
*   **Placeholder Parameters in `sp_ausd_bp_ta_rn_da_vda_tk`:** The parameters `p_entry_number` and `p_restart_threshold` were included in `sp_ausd_bp_ta_rn_da_vda_tk` as placeholders. Their actual logic from `k_ausd_bp_ta_rn_da_vda_tk.ksh` (if it involved more than just passing values) was not fully migrated. Currently, `p_restart_threshold` is passed with a default of `0`. If these parameters are intended to drive specific logic within the core data processing, that logic needs to be implemented.
*   **`DWPA_UTIL_SKRIPT.runstatement` Functionality:** The Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` was used for `TRUNCATE TABLE`. While the `TRUNCATE TABLE` functionality was directly translated, if this Oracle utility had other, more complex functions that were implicitly relied upon, those functionalities are not migrated.

## 6. Validation

To ensure the successful migration and correct functioning of the BigQuery job, follow these validation steps:

1.  **Execute the Orchestration Procedure:**
    *   Manually execute the top-level BigQuery stored procedure `my_project.my_dataset.sp_bereitstellung_basisprodukte_bert` with representative parameters.
    *   Example:
        ```sql
        CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert(
            p_stichtag => '20231026', -- Or NULL to use current date
            p_wiederanlaufWert => '0'  -- Or NULL to use default '0'
        );
        ```
2.  **Monitor Job Audit Log:**
    *   Query the `my_project.my_dataset.job_audit_log` table for the `job_run_id` generated by the execution.
    *   Verify that the `status` for the `sp_bereitstellung_basisprodukte_bert` entry is `SUCCESS` and that there are no `FAILED` entries for any sub-steps.
    *   Check the `message` column for any unexpected warnings or errors.
3.  **Verify Target Table Data:**
    *   Query the target table `my_project.my_dataset.sof_ta_rn_da_vda_tk` to inspect the loaded data.
    *   Check for data types, nullability, and data integrity.
    *   Compare a sample of the loaded data with the expected output from the original Oracle job for the same input parameters.
4.  **Row Count Verification:**
    *   Compare the number of rows inserted into `my_project.my_dataset.sof_ta_rn_da_vda_tk` with the number of rows that would have been generated by the original Oracle job for the same reference date and source data. This is a critical check for functional equivalence.
    *   The row count should match the count from `my_project.my_dataset.sof_ta_rn_einzeln` where `DA_RN_msisdn IS NOT NULL OR VDA_RN_msisdn IS NOT NULL OR TK_RN_msisdn IS NOT NULL`.

**"Passing" Criteria:**

A successful migration is confirmed when:
*   The `my_project.my_dataset.sp_bereitstellung_basisprodukte_bert` stored procedure completes execution without raising any unhandled errors.
*   The `job_audit_log` table records a `SUCCESS` status for the corresponding job run.
*   The `my_project.my_dataset.sof_ta_rn_da_vda_tk` table contains the expected data, matching the filtering logic of the original Oracle SQL.
*   The row count in `my_project.my_dataset.sof_ta_rn_da_vda_tk` is identical to the row count produced by the original Oracle job for the same input data and parameters.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Job Execution:**
    *   Immediately disable or pause the scheduler (e.g., Cloud Composer DAG, Cloud Workflow) that invokes `my_project.my_dataset.sp_bereitstellung_basisprodukte_bert`.
2.  **Re-enable Original Job:**
    *   Re-enable the original KornShell job (`r_ausd_bp_ta_rn_da_vda_tk.ksh`) in its legacy environment.
3.  **Data Reconciliation (if necessary):**
    *   Since this job performs a `TRUNCATE` and `INSERT`, the target table `sof_ta_rn_da_vda_tk` is completely overwritten each run. If the BigQuery job ran incorrectly and produced bad data, the next successful run of the original Oracle job will overwrite it.
    *   If data integrity is paramount and the BigQuery job's output was consumed by downstream systems before rollback, a data restoration of `sof_ta_rn_da_vda_tk` to its state before the BigQuery job run might be necessary. This would typically involve restoring from a BigQuery table snapshot or a backup.
4.  **Revert BigQuery Objects (Optional, for clean-up/re-deployment):**
    *   If the BigQuery stored procedures or DDL changes are deemed problematic, they can be reverted.
        *   For stored procedures, re-deploying a previous version (if available in version control) using `CREATE OR REPLACE PROCEDURE` would revert the code.
        *   For tables, `DROP TABLE IF EXISTS` can be used, followed by re-creation if needed. However, this is usually not required unless the DDL itself was flawed.
5.  **Investigation and Remediation:**
    *   Analyze the `job_audit_log` and BigQuery job logs to identify the root cause of the issue.
    *   Address the identified problems in the BigQuery code or configuration.
    *   Once resolved, re-validate and re-deploy the BigQuery job.