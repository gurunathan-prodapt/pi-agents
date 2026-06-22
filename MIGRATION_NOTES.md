# MIGRATION_NOTES: `r_ausd_bp_ta_rn_vertrag.ksh`

## 1. Summary

This document outlines the migration of the `r_ausd_bp_ta_rn_vertrag.ksh` job, an initial provisioning process for BERT system base products. The original job, written in KornShell and Oracle SQL, generates a daily snapshot of contract data (Vertrags-Cache) for credit scoring.

The job has been migrated from its original Oracle/KornShell environment to Google BigQuery. The KornShell orchestration logic has been re-engineered into BigQuery Stored Procedures, and the core Oracle SQL transformation has been translated into BigQuery SQL within a dedicated Stored Procedure. Logging has been centralized to a BigQuery audit table.

**Target Platform:** Google BigQuery

## 2. Generated Artifacts

The migration process generated the following BigQuery artifacts:

*   **`sql/ddl/job_audit.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_audit` table in BigQuery. This table serves as a centralized logging mechanism for all job executions, capturing status, messages, and execution details, replacing the original shell-based logging and SQL*Plus spooling.
*   **`sql/ddl/sof_ta_rn_vertrag.sql`**
    *   **Role:** Defines the DDL for the `sof_ta_rn_vertrag` table in BigQuery. This is the target table for the aggregated contract data, replacing the Oracle `sof$ta_rn_vertrag` table. Its schema is derived from the `INSERT` statement in the original SQL.
*   **`sql/procedures/d_ausd_bp_ta_rn_vertrag.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `project.dataset.d_ausd_bp_ta_rn_vertrag`. This procedure encapsulates the core data transformation logic, including truncating the target table and inserting aggregated data from `sof_ta_rn_einzeln` into `sof_ta_rn_vertrag`. It directly replaces the functionality of the original `d_ausd_bp_ta_rn_vertrag.sql` Oracle script.
*   **`sql/procedures/k_ausd_bp_ta_rn_vertrag.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `project.dataset.k_ausd_bp_ta_rn_vertrag`. This procedure handles internal control logic such as parameter validation, date calculations, and orchestrates the call to `d_ausd_bp_ta_rn_vertrag`. It replaces the functionality of the original `k_ausd_bp_ta_rn_vertrag.ksh` KornShell script.
*   **`sql/procedures/ausd_bp_ta_rn_vertrag.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_vertrag`. This is the main entry point for the job, responsible for top-level parameter handling, initializing logging, and invoking the `k_ausd_bp_ta_rn_vertrag` procedure. It replaces the functionality of the original `r_ausd_bp_ta_rn_vertrag.ksh` KornShell script.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration:** The original KornShell scripts (`r_ausd_bp_ta_rn_vertrag.ksh`, `k_ausd_bp_ta_rn_vertrag.ksh`) were re-engineered into nested BigQuery Stored Procedures. This decision leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, eliminating the need for external shell environments or complex orchestration tools for this specific job's complexity.
*   **Direct BigQuery SQL for Data Transformation:** The core Oracle SQL (`d_ausd_bp_ta_rn_vertrag.sql`) was translated directly into BigQuery SQL within a Stored Procedure. This maintains the data transformation logic close to the data, benefiting from BigQuery's performance and scalability.
*   **Centralized `job_audit` Table for Logging:** All logging, including job start/end, status, errors, and record counts, is directed to a dedicated `project.dataset.job_audit` BigQuery table. This replaces disparate shell-based log files and SQL*Plus spooling, providing a structured, queryable, and centralized audit trail.
*   **BigQuery `TRUNCATE TABLE`:** The Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call for truncation was replaced with a direct `TRUNCATE TABLE` DML statement in BigQuery. This simplifies the operation, as BigQuery handles table truncation natively and efficiently.
*   **Atomic DML Operations:** BigQuery DML operations are atomic, meaning an explicit `COMMIT;` statement (present in the original Oracle SQL) is not required. This simplifies the transaction management logic.
*   **Built-in BigQuery Date Functions:** Shell utilities like `gestern.ksh` and custom date handling logic were replaced by BigQuery's native date and time functions (e.g., `CURRENT_DATE()`, `DATE_SUB`, `FORMAT_DATE`, `PARSE_DATE`). This reduces external dependencies and improves maintainability.
*   **Oracle Hint Removal:** Oracle-specific hints (e.g., `/*+ full(rp) parallel(rp,4) */`) were removed as they are not applicable to BigQuery and its query optimizer. BigQuery's optimizer will automatically determine the most efficient execution plan.
*   **Parameter Handling within Stored Procedures:** Parameters like `Stichtag` and `Wiederanlaufwert` are passed directly as arguments to the BigQuery Stored Procedures, with validation and default value assignment handled within the procedures themselves. This replaces the shell script's parameter parsing logic.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it in the Google Cloud Console or using `bq mk --dataset project:dataset`.
2.  **Source Table Creation and Data Ingestion:**
    *   Create the BigQuery tables for the source data: `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_rn_einzeln`.
    *   **Crucially, the schemas for these tables must accurately reflect their original Oracle counterparts.** Placeholder DDLs are provided in the generated code comments, but the actual schemas need to be derived from the source system.
    *   Ingest historical and ongoing data into these BigQuery source tables. This typically involves a separate data migration process (e.g., using Dataflow, BigQuery Data Transfer Service, or custom scripts).
3.  **Target Table Creation:**
    *   Execute the DDL from `sql/ddl/sof_ta_rn_vertrag.sql` to create the `project.dataset.sof_ta_rn_vertrag` table.
4.  **Audit Table Creation:**
    *   Execute the DDL from `sql/ddl/job_audit.sql` to create the `project.dataset.job_audit` table.
5.  **Stored Procedure Deployment:**
    *   Deploy the generated BigQuery Stored Procedures by executing the DDL from:
        *   `sql/procedures/d_ausd_bp_ta_rn_vertrag.sql`
        *   `sql/procedures/k_ausd_bp_ta_rn_vertrag.sql`
        *   `sql/procedures/ausd_bp_ta_rn_vertrag.sql`
6.  **IAM Permissions:**
    *   Ensure the service account or user that will execute the BigQuery Stored Procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on `project.dataset` to create/truncate/insert into tables and create/call procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `BigQuery Data Viewer` on any source datasets if they reside in a different project/dataset.
7.  **Scheduling:**
    *   Configure a scheduling mechanism (e.g., Google Cloud Scheduler, Cloud Composer/Airflow) to call the main stored procedure `project.dataset.ausd_bp_ta_rn_vertrag` at the required frequency (e.g., daily).
    *   The scheduler should pass the necessary parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`) to the main procedure.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or areas requiring further investigation/design:

*   **`v_datum` Variable Usage:** In the original `d_ausd_bp_ta_rn_vertrag.sql`, the `v_datum` variable is derived from `dwtk_meldungen` but appears unused in the subsequent SQL logic. Its intended purpose (e.g., for filtering, historization, or as a cutoff date) needs to be clarified. If it's a functional requirement, the corresponding logic must be explicitly added to the BigQuery transformation.
*   **Restart Logic (`p_wiederanlaufWert`):** While the `p_wiederanlaufWert` parameter is passed through the BigQuery Stored Procedures, the provided SQL body (`d_ausd_bp_ta_rn_vertrag.sql`) does not explicitly implement the restart logic (e.g., filtering `DWH_VERTRAG_ID > Wiederanlaufwert` or deleting prior entries). If this is a functional requirement for partial processing or error recovery, it must be designed and implemented within the BigQuery DML.
*   **Post-processing Scripts (Commented Out):** The original `k_ausd_bp_ta_rn_vertrag.ksh` contained commented-out `sed`, `sort`, and `join` commands. These suggest potential post-processing or file manipulation steps. If these steps are functionally required for the target system, they will need to be re-implemented in BigQuery (e.g., using SQL transformations, array functions, or potentially external tools like Dataflow if file-based operations are truly necessary). Currently, they are not part of the migrated BigQuery solution.
*   **`trace.sql.cfg` and Environment Files:** The exact content and impact of `../trace.sql.cfg` and `.dw_init` from the original environment are not fully known. While assumptions were made about their roles (e.g., environment setup, logging configuration), a deeper analysis might be needed if they contain complex logic that affects job execution or data, beyond what's covered by BigQuery's native features.
*   **`DW_DIR_UTL` Variable and Temporary Files:** The original script used a temporary file (`tmpFile=\"$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp\"`). This local file system interaction has been replaced by in-memory operations within BigQuery procedures. If any complex intermediate data storage or exchange was implicitly handled by these temporary files, it needs to be explicitly addressed in BigQuery (e.g., using temporary tables or Common Table Expressions).

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data:**
    *   Ensure `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_rn_einzeln` contain representative test data that mirrors the production environment, including edge cases.
    *   Optionally, back up the `project.dataset.sof_ta_rn_vertrag` table if it contains existing data that should not be truncated during testing.
2.  **Execute the Main Stored Procedure:**
    *   Call the main BigQuery Stored Procedure:
        ```sql
        CALL `project.dataset.ausd_bp_ta_rn_vertrag`(
            p_stichtag_in => 'DDMMYYYY', -- e.g., '25102023'
            p_wiederanlaufWert_in => 0    -- or a specific restart value
        );
        ```
    *   Replace `DDMMYYYY` with an appropriate test date.
3.  **Monitor Job Execution:**
    *   Observe the BigQuery job history in the Google Cloud Console for the execution of the stored procedures.
    *   Query the `project.dataset.job_audit` table to track the job's progress and status messages.
4.  **Verify Data in Target Table:**
    *   Query `project.dataset.sof_ta_rn_vertrag` to inspect the inserted data.
    *   Compare the record count in `sof_ta_rn_vertrag` with the `record_count` logged in `job_audit` for the `d_ausd_bp_ta_rn_vertrag` step.
5.  **Compare with Source System Output:**
    *   Run the original `r_ausd_bp_ta_rn_vertrag.ksh` job in the source environment with the *exact same input parameters* and source data state.
    *   Extract the output from the original job's target table (`sof$ta_rn_vertrag`).
    *   Perform a row-by-row comparison (e.g., using `EXCEPT DISTINCT` in BigQuery or external tools) between the data in `project.dataset.sof_ta_rn_vertrag` and the output from the original system.

**"Passing" Criteria:**

*   The call to `project.dataset.ausd_bp_ta_rn_vertrag` completes without errors.
*   The `job_audit` table shows a `status` of 'COMPLETED' for the main job and 'SUCCESS' for its sub-procedures (`k_ausd_bp_ta_rn_vertrag`, `d_ausd_bp_ta_rn_vertrag`).
*   The `record_count` in `job_audit` for `d_ausd_bp_ta_rn_vertrag` matches the number of rows inserted into `project.dataset.sof_ta_rn_vertrag`.
*   The data in `project.dataset.sof_ta_rn_vertrag` is identical to the output produced by the original Oracle job when run with the same input data and parameters. This includes matching record counts, column values, and data types.

## 7. Rollback Procedure

In case of critical issues or failure during the go-live, the following rollback procedure can be executed:

1.  **Stop New Executions:**
    *   Immediately disable or pause any scheduled executions of the BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_rn_vertrag`) in Cloud Scheduler or Cloud Composer.
2.  **Revert to Original System:**
    *   Re-enable or restart the original `r_ausd_bp_ta_rn_vertrag.ksh` job in the source environment.
    *   Ensure the original job's scheduling is restored.
3.  **Data Restoration (if necessary):**
    *   If the BigQuery job corrupted or incorrectly modified `project.dataset.sof_ta_rn_vertrag` and this data is critical for downstream processes, restore the table from the last known good backup. This might involve:
        *   Using BigQuery's time travel feature to restore the table to a point before the erroneous run.
        *   Loading data from a snapshot or a previously exported backup.
    *   **Note:** Since this job truncates and re-inserts, a full restore might not always be necessary if the next successful run of the original job can simply overwrite the BigQuery table. However, if downstream systems depend on the BigQuery table *before* the next successful run, a restore is crucial.
4.  **Delete BigQuery Artifacts (Optional, for clean slate):**
    *   If the migration is deemed unsuccessful and a complete re-migration is planned, the deployed BigQuery Stored Procedures can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_rn_vertrag`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_rn_vertrag`;
        DROP PROCEDURE IF EXISTS `project.dataset.d_ausd_bp_ta_rn_vertrag`;
        ```
    *   The `project.dataset.sof_ta_rn_vertrag` and `project.dataset.job_audit` tables can also be truncated or dropped if desired, but retaining `job_audit` for post-mortem analysis is often beneficial.
5.  **Root Cause Analysis:**
    *   Analyze the `job_audit` table and BigQuery job logs to identify the root cause of the failure before attempting re-migration or re-deployment.